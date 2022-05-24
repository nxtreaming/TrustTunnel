use std::io;
use std::io::ErrorKind;
use std::sync::{Arc, Mutex};
use tokio::time;
use crate::authentication::Status;
use crate::downstream::{AuthorizedRequest, Downstream, PendingDatagramMultiplexerRequest, PendingTcpConnectRequest};
use crate::forwarder::Forwarder;
use crate::{core, datagram_pipe, downstream, log_id, log_utils, pipe, udp_pipe};
use crate::pipe::DuplexPipe;


pub(crate) struct Tunnel {
    context: Arc<core::Context>,
    downstream: Box<dyn Downstream>,
    forwarder: Arc<Mutex<Box<dyn Forwarder>>>,
    id: log_utils::IdChain<u64>,
}


impl Tunnel {
    pub fn new(
        context: Arc<core::Context>,
        downstream: Box<dyn Downstream>,
        forwarder: Box<dyn Forwarder>,
        id: log_utils::IdChain<u64>,
    ) -> Self {
        Self {
            context,
            downstream,
            forwarder: Arc::new(Mutex::new(forwarder)),
            id,
        }
    }

    pub async fn listen(&mut self) -> io::Result<()> {
        let (mut shutdown_notification, _shutdown_completion) = {
            let shutdown = self.context.shutdown.lock().unwrap();
            (shutdown.notification_handler(), shutdown.completion_guard())
        };
        tokio::select! {
            x = shutdown_notification.wait() => {
                match x {
                    Ok(_) => self.downstream.graceful_shutdown().await,
                    Err(e) => Err(io::Error::new(ErrorKind::Other, format!("{}", e))),
                }
            }
            x = self.listen_inner() => x,
        }
    }

    async fn listen_inner(&mut self) -> io::Result<()> {
        loop {
            let request = match tokio::time::timeout(
                self.context.settings.client_listener_timeout, self.downstream.listen()
            ).await {
                Ok(Ok(None)) => {
                    log_id!(debug, self.id, "Tunnel closed gracefully");
                    return Ok(());
                }
                Ok(Ok(Some(r))) => r,
                Ok(Err(e)) => return Err(e),
                Err(_) => return Err(io::Error::from(ErrorKind::TimedOut)),
            };

            let context = self.context.clone();
            let forwarder = self.forwarder.clone();
            let request_id = request.id();
            let log_id = self.id.clone();
            let update_metrics = {
                let metrics = context.metrics.clone();
                let protocol = self.downstream.protocol();
                move |direction, n| {
                    match direction {
                        pipe::SimplexDirection::Incoming => metrics.add_inbound_bytes(protocol, n),
                        pipe::SimplexDirection::Outgoing => metrics.add_outbound_bytes(protocol, n),
                    }
                }
            };

            tokio::spawn(async move {
                let info = match request.auth_info() {
                    Ok(x) => x,
                    Err(e) => {
                        log_id!(debug, request_id, "Failed to get auth info: {}", e);
                        request.fail_request();
                        return;
                    }
                };

                match context.settings.authenticator.authenticate(info, &log_id).await {
                    Status::Pass => (),
                    Status::Reject => {
                        log_id!(debug, request_id, "Authorization failed");
                        request.fail_request();
                        return;
                    }
                }

                match request.succeed_request() {
                    Ok(None) => (),
                    Ok(Some(AuthorizedRequest::TcpConnect(request))) => {
                        Tunnel::on_tcp_connect_request(context, forwarder, request, update_metrics).await
                    }
                    Ok(Some(AuthorizedRequest::DatagramMultiplexer(request))) => {
                        Tunnel::on_datagram_mux_request(context, forwarder, request, update_metrics).await
                    }
                    Err(e) => {
                        log_id!(debug, request_id, "Failed to complete request: {}", e);
                    }
                }
            });
        }
    }

    async fn on_tcp_connect_request<F: Fn(pipe::SimplexDirection, usize) + Send + Clone>(
        context: Arc<core::Context>,
        forwarder: Arc<Mutex<Box<dyn Forwarder>>>,
        request: Box<dyn PendingTcpConnectRequest>,
        update_metrics: F,
    ) {
        let request_id = request.id();
        let destination = match request.destination() {
            Ok(d) => d,
            Err(e) => {
                log_id!(debug, request_id, "Failed to get destination: {}", e);
                let _ = request.fail_request(e);
                return;
            }
        };

        log_id!(trace, request_id, "Connecting to peer: {:?}", destination);
        let connector =
            match forwarder.lock().unwrap().tcp_connector(request_id.clone(), destination) {
                Ok(c) => c,
                Err(e) => {
                    log_id!(debug, request_id, "Failed to start connection: {}", e);
                    let _ = request.fail_request(e);
                    return;
                }
            };

        let (fwd_rx, fwd_tx) =
            match time::timeout(context.settings.tcp_connections_timeout, connector.connect()).await
                .unwrap_or_else(|_| Err(io::Error::from(ErrorKind::TimedOut)))
            {
                Ok(x) => x,
                Err(e) => {
                    log_id!(debug, request_id, "Connection to peer failed: {}", e);
                    let _ = request.fail_request(e);
                    return;
                }
            };

        log_id!(trace, request_id, "Successfully connected to peer");
        let (dstr_rx, dstr_tx) =
            match request.succeed_request() {
                Ok(x) => x,
                Err(e) => {
                    log_id!(debug, request_id, "Failed to complete request: {}", e);
                    return;
                }
            };

        let mut pipe = DuplexPipe::new(
            (pipe::SimplexDirection::Outgoing, dstr_rx, fwd_tx),
            (pipe::SimplexDirection::Incoming, fwd_rx, dstr_tx),
            update_metrics,
        );

        match pipe.exchange(context.settings.tcp_connections_timeout).await {
            Ok(_) => { log_id!(trace, request_id, "Both ends closed gracefully"); }
            Err(e) => { log_id!(debug, request_id, "Error on pipe: {}", e); }
        }
    }

    async fn on_datagram_mux_request<F: Fn(pipe::SimplexDirection, usize) + Send + Clone + Sync>(
        context: Arc<core::Context>,
        forwarder: Arc<Mutex<Box<dyn Forwarder>>>,
        request: Box<dyn PendingDatagramMultiplexerRequest>,
        update_metrics: F,
    ) {
        let request_id = request.id();
        let mut pipe: Box<dyn datagram_pipe::DuplexPipe> = match request.succeed_request() {
            Ok(downstream::DatagramPipeHalves::Udp(dstr_source, dstr_sink)) => {
                let (fwd_shared, fwd_source, fwd_sink) =
                    match forwarder.lock().unwrap().make_udp_datagram_multiplexer(request_id.clone()) {
                        Ok(x) => x,
                        Err(e) => {
                            log_id!(debug, request_id, "Failed to create datagram multiplexer: {}", e);
                            return;
                        }
                    };

                Box::new(udp_pipe::DuplexPipe::new(
                    (dstr_source, dstr_sink),
                    (fwd_shared, fwd_source, fwd_sink),
                    update_metrics,
                    context.settings.udp_connections_timeout,
                ))
            }
            Ok(downstream::DatagramPipeHalves::Icmp(dstr_source, dstr_sink)) => {
                let (fwd_source, fwd_sink) =
                    match forwarder.lock().unwrap().make_icmp_datagram_multiplexer(request_id.clone()) {
                        Ok(x) => x,
                        Err(e) => {
                            log_id!(debug, request_id, "Failed to create datagram multiplexer: {}", e);
                            return;
                        }
                    };

                Box::new(datagram_pipe::GenericDuplexPipe::new(
                    (
                        pipe::SimplexDirection::Outgoing,
                        dstr_source,
                        fwd_sink,
                    ),
                    (
                        pipe::SimplexDirection::Incoming,
                        fwd_source,
                        dstr_sink,
                    ),
                    update_metrics,
                ))
            }
            Err(e) => {
                log_id!(debug, request_id, "Failed to respond for datagram multiplexer request: {}", e);
                return;
            }
        };

        match pipe.exchange().await {
            Ok(_) => log_id!(trace, request_id, "Datagram multiplexer gracefully closed"),
            Err(e) => log_id!(debug, request_id, "Datagram multiplexer closed with error: {}", e),
        }
    }
}
