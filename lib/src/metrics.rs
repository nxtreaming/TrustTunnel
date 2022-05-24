use std::io;
use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use bytes::Bytes;
use prometheus::Encoder;
use tokio::net::{TcpListener, TcpStream};
use crate::{core, http_codec, log_id, log_utils};
use crate::downstream_protocol_selector::TunnelProtocol;
use crate::http1_codec::Http1Codec;
use crate::http_codec::HttpCodec;


const LOG_FMT: &str = "METRICS={}";
const HEALTH_CHECK_PATH: &str = "/health-check";
const METRICS_PATH: &str = "/metrics";


pub(crate) struct Metrics {
    client_sessions: prometheus::IntGaugeVec,
    inbound_traffic: prometheus::IntCounterVec,
    outbound_traffic: prometheus::IntCounterVec,
}

pub(crate) struct ClientSessionsCounter {
    metrics: Arc<Metrics>,
    protocol: TunnelProtocol,
}


impl Metrics {
    pub fn new() -> io::Result<Arc<Self>> {
        Ok(Arc::new(Self {
            client_sessions: prometheus::register_int_gauge_vec!(
                "client_sessions",
                "Number of active client sessions",
                &["protocol_type"]
            ).map_err(prometheus_to_io_error)?,
            inbound_traffic: prometheus::register_int_counter_vec!(
                "inbound_traffic_bytes",
                "Total number of bytes uploaded by clients",
                &["protocol_type"]
            ).map_err(prometheus_to_io_error)?,
            outbound_traffic: prometheus::register_int_counter_vec!(
                "outbound_traffic_bytes",
                "Total number of bytes downloaded by clients",
                &["protocol_type"]
            ).map_err(prometheus_to_io_error)?,
        }))
    }

    pub fn client_sessions_counter(self: Arc<Self>, protocol: TunnelProtocol) -> ClientSessionsCounter {
        ClientSessionsCounter::new(self, protocol)
    }

    pub fn add_inbound_bytes(&self, protocol: TunnelProtocol, n: usize) {
        self.inbound_traffic.with_label_values(&[protocol.to_str()]).inc_by(n as u64);
    }

    pub fn add_outbound_bytes(&self, protocol: TunnelProtocol, n: usize) {
        self.outbound_traffic.with_label_values(&[protocol.to_str()]).inc_by(n as u64);
    }

    fn collect(&self) -> (String, Bytes) {
        let encoder = prometheus::TextEncoder::new();

        let metric_families = prometheus::gather();
        let mut buffer = vec![];
        encoder.encode(&metric_families, &mut buffer).unwrap();

        (encoder.format_type().to_string(), Bytes::from(buffer))
    }
}

impl ClientSessionsCounter {
    fn new(metrics: Arc<Metrics>, protocol: TunnelProtocol) -> Self {
        metrics.client_sessions.with_label_values(&[protocol.to_str()]).inc();

        Self {
            metrics,
            protocol,
        }
    }
}

impl Drop for ClientSessionsCounter {
    fn drop(&mut self) {
        self.metrics.client_sessions.with_label_values(&[self.protocol.to_str()]).dec();
    }
}

pub(crate) async fn listen(
    context: Arc<core::Context>,
    log_chain: log_utils::IdChain<u64>,
) -> io::Result<()> {
    let (mut shutdown_notification, _shutdown_completion) = {
        let shutdown = context.shutdown.lock().unwrap();
        (shutdown.notification_handler(), shutdown.completion_guard())
    };

    tokio::select! {
        x = shutdown_notification.wait() => {
            match x {
                Ok(_) => Ok(()),
                Err(e) => Err(io::Error::new(ErrorKind::Other, format!("{}", e))),
            }
        }
        x = listen_inner(context, log_chain) => x,
    }
}

async fn listen_inner(
    context: Arc<core::Context>,
    log_chain: log_utils::IdChain<u64>,
) -> io::Result<()> {
    let settings = context.settings.metrics.as_ref();
    if settings.is_none() {
        return Ok(());
    }

    let next_id = AtomicU64::default();
    let listener = TcpListener::bind(settings.unwrap().address).await?;

    loop {
        let (stream, peer) = listener.accept().await?;
        let log_id = log_chain.extended(log_utils::IdItem::new(
            LOG_FMT, next_id.fetch_add(1, Ordering::Relaxed)
        ));
        log_id!(trace, log_id, "New connection from {}", peer);
        let context = context.clone();
        tokio::spawn(async move {
            handle_request(context, stream, log_id).await
        });
    }
}

async fn handle_request(context: Arc<core::Context>, io: TcpStream, log_id: log_utils::IdChain<u64>) {
    let mut codec = Http1Codec::new(context.settings.clone(), io, log_id.clone());
    let timeout = context.settings.metrics.as_ref().unwrap().request_timeout;
    let stream = match tokio::time::timeout(timeout, codec.listen()).await {
        Ok(Ok(Some(x))) => {
            log_id!(trace, log_id, "Got request: {:?}", x.request().request());
            x
        }
        Ok(Ok(None)) => {
            log_id!(debug, log_id, "Connection closed immediately");
            return;
        }
        Ok(Err(e)) => {
            log_id!(debug, log_id, "Listen failed: {}", e);
            return;
        }
        Err(_elapsed) => {
            log_id!(debug, log_id, "Didn't receive any request during configured period");
            return;
        }
    };

    let dispatch = async {
        match codec.listen().await {
            Ok(Some(x)) => log_id!(
                    debug, log_id,
                    "Got unexpected request while processing previous: {:?}",
                    x.request().request(),
                ),
            Ok(None) => (),
            Err(e) => log_id!(debug, log_id, "IO error during processing: {}", e),
        }
    };

    let handle = async {
        let path = stream.request().request().uri.path();
        let result = match path {
            HEALTH_CHECK_PATH => handle_health_check(stream),
            METRICS_PATH => handle_metrics_collect(&context.metrics, stream).await,
            x => {
                log_id!(debug, log_id, "Unexpected path: {}", x);
                let respond = stream.split().1;
                if let Err(e) = respond.send_bad_response(http::status::StatusCode::BAD_REQUEST, vec![]) {
                    log_id!(debug, log_id, "Failed to send response: {}", e);
                }
                return;
            }
        };

        if let Err(e) = result {
            log_id!(debug, log_id, "Failed to handle request: {}", e);
        }
    };

    tokio::select! {
        _ = dispatch => (),
        _ = handle => (),
    }

    if let Err(e) = codec.graceful_shutdown().await {
        log_id!(debug, log_id, "Failed to shutdown HTTP session: {}", e);
    }
}

fn handle_health_check(stream: Box<dyn http_codec::Stream>) -> io::Result<()> {
    stream.split().1.send_ok_response(true).map(|_| ())
}

async fn handle_metrics_collect(
    metrics: &Metrics,
    stream: Box<dyn http_codec::Stream>,
) -> io::Result<()> {
    let (content_type, mut content) = metrics.collect();
    let response = http::Response::builder()
        .version(stream.request().request().version)
        .status(http::status::StatusCode::OK)
        .header(http::header::CONTENT_TYPE, content_type)
        .header(http::header::CONTENT_LENGTH, content.len())
        .body(())
        .unwrap()
        .into_parts().0;

    let mut sink = stream.split().1
        .send_response(response, false)?
        .into_pipe_sink();

    while !content.is_empty() {
        content = sink.write(content)?;
        sink.wait_writable().await?;
    }

    sink.eof()
}


fn prometheus_to_io_error(e: prometheus::Error) -> io::Error {
    match e {
        prometheus::Error::Io(e) => e,
        e => io::Error::new(ErrorKind::Other, e.to_string()),
    }
}
