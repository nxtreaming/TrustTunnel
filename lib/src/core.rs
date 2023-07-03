use std::io;
use std::io::ErrorKind;
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, UdpSocket};
use crate::direct_forwarder::DirectForwarder;
use crate::{authentication, http_ping_handler, http_speedtest_handler, log_id, log_utils, metrics, net_utils, reverse_proxy, settings, tls_demultiplexer, tunnel};
use crate::tls_demultiplexer::TlsDemux;
use crate::forwarder::Forwarder;
use crate::http1_codec::Http1Codec;
use crate::http2_codec::Http2Codec;
use crate::http3_codec::Http3Codec;
use crate::http_codec::HttpCodec;
use crate::http_downstream::HttpDownstream;
use crate::icmp_forwarder::IcmpForwarder;
use crate::metrics::Metrics;
use crate::quic_multiplexer::{QuicMultiplexer, QuicSocket};
use crate::settings::{ForwardProtocolSettings, Settings};
use crate::shutdown::Shutdown;
use crate::socks5_forwarder::Socks5Forwarder;
use crate::tls_listener::{TlsAcceptor, TlsListener};
use crate::tunnel::Tunnel;


#[derive(Debug)]
pub enum Error {
    /// Passed settings did not pass the validation
    SettingsValidation(settings::ValidationError),
    /// TLS demultiplexer initialization failed
    TlsDemultiplexer(String),
    /// Metrics module initialization failed
    Metrics(String),
}

pub struct Core {
    context: Arc<Context>,
}


pub(crate) struct Context {
    pub settings: Arc<Settings>,
    tls_demux: Arc<RwLock<TlsDemux>>,
    pub icmp_forwarder: Option<Arc<IcmpForwarder>>,
    pub shutdown: Arc<Mutex<Shutdown>>,
    pub metrics: Arc<Metrics>,
    next_client_id: Arc<AtomicU64>,
    next_tunnel_id: Arc<AtomicU64>,
}

impl Core {
    pub fn new(
        settings: Settings,
        tls_hosts_settings: settings::TlsHostsSettings,
        shutdown: Arc<Mutex<Shutdown>>,
    ) -> Result<Self, Error> {
        if !settings.is_built() {
            settings.validate().map_err(Error::SettingsValidation)?;
        }
        if !tls_hosts_settings.is_built() {
            tls_hosts_settings.validate().map_err(Error::SettingsValidation)?;
        }

        let settings = Arc::new(settings);

        Ok(Self {
            context: Arc::new(Context {
                settings: settings.clone(),
                tls_demux: Arc::new(RwLock::new(
                    TlsDemux::new(&settings, &tls_hosts_settings)
                        .map_err(|e| Error::TlsDemultiplexer(e.to_string()))?
                )),
                icmp_forwarder: if settings.icmp.is_none() {
                    None
                } else {
                    Some(Arc::new(IcmpForwarder::new(settings)))
                },
                shutdown,
                metrics: Metrics::new().map_err(|e| Error::Metrics(e.to_string()))?,
                next_client_id: Default::default(),
                next_tunnel_id: Default::default(),
            }),
        })
    }

    /// Run an endpoint instance inside the caller provided asynchronous runtime.
    pub async fn listen(&self) -> io::Result<()> {
        let listen_tcp = async {
            self.listen_tcp().await
                .map_err(|e| io::Error::new(e.kind(), format!("TCP listener failure: {}", e)))
        };

        let listen_udp = async {
            self.listen_udp().await
                .map_err(|e| io::Error::new(e.kind(), format!("UDP listener failure: {}", e)))
        };

        let listen_icmp = async {
            self.listen_icmp().await
                .map_err(|e| io::Error::new(e.kind(), format!("ICMP listener failure: {}", e)))
        };

        let listen_metrics = async {
            metrics::listen(self.context.clone(), log_utils::IdChain::empty()).await
                .map_err(|e| io::Error::new(e.kind(), format!("Metrics listener failure: {}", e)))
        };

        let (mut shutdown_notification, _shutdown_completion) = {
            let shutdown = self.context.shutdown.lock().unwrap();
            (
                shutdown.notification_handler(),
                shutdown.completion_guard()
                    .ok_or_else(|| io::Error::new(ErrorKind::Other, "Shutdown is already submitted"))?
            )
        };

        tokio::select! {
            x = shutdown_notification.wait() => {
                x.map_err(|e| io::Error::new(ErrorKind::Other, format!("{}", e)))
            },
            x = futures::future::try_join4(
                listen_tcp,
                listen_udp,
                listen_icmp,
                listen_metrics,
            ) => x.map(|_| ()),
        }
    }

    /// Reload the TLS hosts settings
    pub fn reload_tls_hosts_settings(&self, settings: settings::TlsHostsSettings) -> io::Result<()> {
        let mut demux = self.context.tls_demux.write().unwrap();

        if !settings.is_built() {
            settings.validate()
                .map_err(|e| io::Error::new(
                    ErrorKind::Other, format!("Settings validation failure: {:?}", e),
                ))?;
        }

        *demux = TlsDemux::new(&self.context.settings, &settings)?;
        Ok(())
    }

    async fn listen_tcp(&self) -> io::Result<()> {
        let settings = self.context.settings.clone();
        let has_tcp_based_codec = settings.listen_protocols.http1.is_some()
            || settings.listen_protocols.http2.is_some();

        let tcp_listener = TcpListener::bind(settings.listen_address).await?;
        info!("Listening to TCP {}", settings.listen_address);

        let tls_listener = Arc::new(TlsListener::new());
        loop {
            let client_id = log_utils::IdChain::from(log_utils::IdItem::new(
                log_utils::CLIENT_ID_FMT, self.context.next_client_id.fetch_add(1, Ordering::Relaxed),
            ));
            let stream = match tcp_listener.accept().await
                .and_then(|(s, a)| {
                    s.set_nodelay(true)?;
                    Ok((s, a))
                })
            {
                Ok((stream, addr)) => if has_tcp_based_codec {
                    log_id!(debug, client_id, "New TCP client: {}", addr);
                    stream
                } else {
                    continue; // accept just for pings
                }
                Err(e) => {
                    log_id!(debug, client_id, "TCP connection failed: {}", e);
                    continue;
                }
            };

            tokio::spawn({
                let context = self.context.clone();
                let tls_listener = tls_listener.clone();
                async move {
                    let handshake_timeout = context.settings.tls_handshake_timeout;
                    match tokio::time::timeout(handshake_timeout, tls_listener.listen(stream))
                        .await
                        .unwrap_or_else(|_| Err(io::Error::from(ErrorKind::TimedOut)))
                    {
                        Ok(stream) =>
                            if let Err((client_id, message)) = Core::on_new_tls_connection(
                                context.clone(), stream, client_id,
                            ).await {
                                log_id!(debug, client_id, "{}", message);
                            },
                        Err(e) => log_id!(trace, client_id, "TLS handshake failed: {}", e),
                    }
                }
            });
        }
    }

    async fn listen_udp(&self) -> io::Result<()> {
        let settings = self.context.settings.clone();
        if settings.listen_protocols.quic.is_none() {
            return Ok(());
        }

        let socket = UdpSocket::bind(settings.listen_address).await?;
        info!("Listening to UDP {}", settings.listen_address);

        let mut quic_listener = QuicMultiplexer::new(
            settings,
            socket,
            self.context.tls_demux.clone(),
            self.context.next_client_id.clone(),
        );

        loop {
            let socket = quic_listener.listen().await?;

            tokio::spawn({
                let context = self.context.clone();
                let socket_id = socket.id();
                async move {
                    log_id!(debug, socket_id, "New QUIC connection");
                    Self::on_new_quic_connection(context, socket, socket_id).await;
                }
            });
        }
    }

    async fn listen_icmp(&self) -> io::Result<()> {
        let forwarder = match &self.context.icmp_forwarder {
            None => return Ok(()),
            Some(x) => x.clone(),
        };

        forwarder.listen().await
    }

    async fn on_new_tls_connection(
        context: Arc<Context>,
        acceptor: TlsAcceptor,
        client_id: log_utils::IdChain<u64>,
    ) -> Result<(), (log_utils::IdChain<u64>, String)> {
        let sni = match acceptor.sni() {
            Some(s) => s,
            None => return Err((client_id, "Drop TLS connection due to absence of SNI".to_string())),
        };

        let core_settings = context.settings.clone();
        let tls_connection_meta =
            match context.tls_demux.read().unwrap().select(acceptor.alpn().iter().map(Vec::as_slice), sni) {
                Ok(x) if x.protocol == tls_demultiplexer::Protocol::Http3 =>
                    return Err((client_id, format!("Dropping connection due to unexpected protocol: {:?}", x))),
                Ok(x) => x,
                Err(e) => return Err((client_id, format!("Dropping connection due to error: {}", e))),
            };
        log_id!(debug, client_id, "Connection meta: {:?}", tls_connection_meta);

        let stream = match acceptor.accept(
            tls_connection_meta.protocol,
            tls_connection_meta.cert_chain,
            tls_connection_meta.key,
            &client_id,
        ).await {
            Ok(s) => {
                log_id!(debug, client_id, "New TLS client: {:?}", s);
                s
            }
            Err(e) => {
                return Err((client_id, format!("TLS connection failed: {}", e)));
            }
        };

        match tls_connection_meta.channel {
            net_utils::Channel::Tunnel => {
                let tunnel_id = client_id.extended(log_utils::IdItem::new(
                    log_utils::TUNNEL_ID_FMT, context.next_tunnel_id.fetch_add(1, Ordering::Relaxed),
                ));

                Self::on_tunnel_request(
                    context,
                    tls_connection_meta.protocol,
                    match Self::make_tcp_http_codec(
                        tls_connection_meta.protocol, core_settings, stream, tunnel_id.clone(),
                    ) {
                        Ok(x) => x,
                        Err(e) => return Err((client_id, format!("Failed to create HTTP codec: {}", e))),
                    },
                    tls_connection_meta.sni,
                    tls_connection_meta.sni_auth_creds,
                    tunnel_id,
                ).await
            }
            net_utils::Channel::Ping => http_ping_handler::listen(
                context.shutdown.clone(),
                match Self::make_tcp_http_codec(
                    tls_connection_meta.protocol, core_settings, stream, client_id.clone(),
                ) {
                    Ok(x) => x,
                    Err(e) => return Err((client_id, format!("Failed to create HTTP codec: {}", e))),
                },
                context.settings.tls_handshake_timeout,
                client_id,
            ).await,
            net_utils::Channel::Speedtest => http_speedtest_handler::listen(
                context.shutdown.clone(),
                match Self::make_tcp_http_codec(
                    tls_connection_meta.protocol, core_settings, stream, client_id.clone(),
                ) {
                    Ok(x) => x,
                    Err(e) => return Err((client_id, format!("Failed to create HTTP codec: {}", e))),
                },
                context.settings.tls_handshake_timeout,
                client_id,
            ).await,
            net_utils::Channel::ReverseProxy => reverse_proxy::listen(
                core_settings.clone(),
                context.shutdown.clone(),
                match Self::make_tcp_http_codec(
                    tls_connection_meta.protocol, core_settings, stream, client_id.clone(),
                ) {
                    Ok(x) => x,
                    Err(e) => return Err((client_id, format!("Failed to create HTTP codec: {}", e))),
                },
                tls_connection_meta.sni,
                client_id,
            ).await,
        }

        Ok(())
    }

    async fn on_new_quic_connection(
        context: Arc<Context>,
        socket: QuicSocket,
        client_id: log_utils::IdChain<u64>,
    ) {
        let tls_connection_meta = socket.tls_connection_meta();
        log_id!(debug, client_id, "Connection meta: {:?}", tls_connection_meta);

        match tls_connection_meta.channel {
            net_utils::Channel::Tunnel => {
                let tunnel_id = client_id.extended(log_utils::IdItem::new(
                    log_utils::TUNNEL_ID_FMT, context.next_tunnel_id.fetch_add(1, Ordering::Relaxed),
                ));

                let sni = tls_connection_meta.sni.clone();
                let sni_auth_creds = tls_connection_meta.sni_auth_creds.clone();

                Self::on_tunnel_request(
                    context,
                    tls_connection_meta.protocol,
                    Box::new(Http3Codec::new(socket, tunnel_id.clone())),
                    sni,
                    sni_auth_creds,
                    tunnel_id,
                ).await
            }
            net_utils::Channel::Ping => http_ping_handler::listen(
                context.shutdown.clone(),
                Box::new(Http3Codec::new(socket, client_id.clone())),
                context.settings.tls_handshake_timeout,
                client_id,
            ).await,
            net_utils::Channel::Speedtest => http_speedtest_handler::listen(
                context.shutdown.clone(),
                Box::new(Http3Codec::new(socket, client_id.clone())),
                context.settings.tls_handshake_timeout,
                client_id,
            ).await,
            net_utils::Channel::ReverseProxy => {
                let sni = tls_connection_meta.sni.clone();

                reverse_proxy::listen(
                    context.settings.clone(),
                    context.shutdown.clone(),
                    Box::new(Http3Codec::new(socket, client_id.clone())),
                    sni,
                    client_id,
                ).await
            }
        }
    }

    async fn on_tunnel_request(
        context: Arc<Context>,
        protocol: tls_demultiplexer::Protocol,
        codec: Box<dyn HttpCodec>,
        server_name: String,
        sni_auth_creds: Option<String>,
        tunnel_id: log_utils::IdChain<u64>,
    ) {
        let _metrics_guard = Metrics::client_sessions_counter(context.metrics.clone(), protocol);

        let authentication_policy = match context.settings.authenticator.as_ref().zip(sni_auth_creds) {
            None => tunnel::AuthenticationPolicy::Default,
            Some((authenticator, credentials)) => {
                let auth = authentication::Source::Sni(credentials.into());
                match authenticator.authenticate(&auth, &tunnel_id) {
                    authentication::Status::Pass => tunnel::AuthenticationPolicy::Authenticated(auth),
                    authentication::Status::Reject => {
                        log_id!(debug, tunnel_id, "SNI authentication failed");
                        return;
                    }
                }
            }
        };

        log_id!(debug, tunnel_id, "New tunnel for client");
        let mut tunnel = Tunnel::new(
            context.clone(),
            Box::new(HttpDownstream::new(
                context.settings.clone(),
                context.shutdown.clone(),
                codec,
                server_name,
            )),
            Self::make_forwarder(context),
            authentication_policy,
            tunnel_id.clone(),
        );

        log_id!(trace, tunnel_id, "Listening for client tunnel");
        match tunnel.listen().await {
            Ok(_) => log_id!(debug, tunnel_id, "Tunnel stopped gracefully"),
            Err(e) => log_id!(debug, tunnel_id, "Tunnel stopped with error: {}", e),
        }
    }

    fn make_tcp_http_codec<IO>(
        protocol: tls_demultiplexer::Protocol,
        core_settings: Arc<Settings>,
        io: IO,
        log_id: log_utils::IdChain<u64>,
    ) -> io::Result<Box<dyn HttpCodec>>
        where IO: 'static + AsyncRead + AsyncWrite + Unpin + Send + net_utils::PeerAddr
    {
        match protocol {
            tls_demultiplexer::Protocol::Http1 => Ok(Box::new(Http1Codec::new(
                core_settings, io, log_id,
            ))),
            tls_demultiplexer::Protocol::Http2 => Ok(Box::new(Http2Codec::new(
                core_settings, io, log_id,
            )?)),
            tls_demultiplexer::Protocol::Http3 => unreachable!(),
        }
    }

    fn make_forwarder(context: Arc<Context>) -> Box<dyn Forwarder> {
        match &context.settings.forward_protocol {
            ForwardProtocolSettings::Direct(_) => Box::new(DirectForwarder::new(context)),
            ForwardProtocolSettings::Socks5(_) => Box::new(Socks5Forwarder::new(context)),
        }
    }
}

#[cfg(test)]
impl Default for Context {
    fn default() -> Self {
        let settings = Arc::new(Settings::default());
        Self {
            settings: settings.clone(),
            tls_demux: Arc::new(RwLock::new(
                TlsDemux::new(&settings, &settings::TlsHostsSettings::default()).unwrap()
            )),
            icmp_forwarder: None,
            shutdown: Shutdown::new(),
            metrics: Metrics::new().unwrap(),
            next_client_id: Default::default(),
            next_tunnel_id: Default::default(),
        }
    }
}
