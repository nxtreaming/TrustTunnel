use std::fmt::{Display, Formatter};
use std::io;
use std::io::ErrorKind;
use std::time::Duration;
use async_trait::async_trait;
use bytes::Bytes;
use future::Either;
use futures::{future, FutureExt};
use tokio::time::Instant;
use crate::{log_id, log_utils};


macro_rules! log_dir {
    ($lvl:ident, $id_chain:expr, $direction:expr, $msg:expr) => {
        log_id!($lvl, $id_chain, std::concat!("{} ", $msg), $direction)
    };
    ($lvl:ident, $id_chain:expr, $direction:expr, $fmt:expr, $($arg:tt)*) => {
        log_id!($lvl, $id_chain, std::concat!("{} ", $fmt), $direction, $($arg)*)
    };
}


pub(crate) enum Data {
    /// Data chunk
    Chunk(Bytes),
    /// No more data will be transmitted in that direction
    Eof,
}

/// An abstract interface for a receiver implementation
#[async_trait]
pub(crate) trait Source: Send {
    /// Get the request ID for logging
    fn id(&self) -> log_utils::IdChain<u64>;

    /// Listen for incoming data on the connection.
    async fn read(&mut self) -> io::Result<Data>;

    /// Slide receive window on the connection. Must be called by a caller after a portion of
    /// received data is processed.
    fn consume(&mut self, size: usize) -> io::Result<()>;
}

/// An abstract interface for a transmitter implementation
#[async_trait]
pub(crate) trait Sink: Send {
    /// Get the request ID for logging
    fn id(&self) -> log_utils::IdChain<u64>;

    /// Write a data chunk to the connection.
    ///
    /// # Return
    ///
    /// An unsent portion of `data` due to flow control limits. It must be sent later by a caller.
    fn write(&mut self, data: Bytes) -> io::Result<Bytes>;

    /// Indicate that no more data will be sent to the sink
    fn eof(&mut self) -> io::Result<()>;

    /// Wait for the connection to be writable. Should be called if [`Self::write()`] return non-empty
    /// buffer.
    async fn wait_writable(&mut self) -> io::Result<()>;
}

#[derive(Copy, Clone, PartialEq)]
pub(crate) enum SimplexDirection {
    /// Packet goes from a peer to a client
    Incoming,
    /// Packet goes from a client to a peer
    Outgoing,
}

/// Feeds packets received from [`Source`] to [`Sink`] doing some flow control
pub(crate) struct SimplexPipe<F> {
    source: Box<dyn Source>,
    sink: Box<dyn Sink>,
    update_metrics: F,
    pending_chunk: Option<Data>,
    direction: SimplexDirection,
    last_activity: Instant,
}

pub(crate) struct Error<T> {
    pub id: T,
    pub io: io::Error,
}


impl Display for Data {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Data::Chunk(x) => write!(f, "Chunk({} bytes)", x.len()),
            Data::Eof => write!(f, "Eof"),
        }
    }
}

impl Display for SimplexDirection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SimplexDirection::Incoming => write!(f, "<--"),
            SimplexDirection::Outgoing => write!(f, "-->"),
        }
    }
}

impl<F: Fn(SimplexDirection, usize) + Send> SimplexPipe<F> {
    pub fn new(
        source: Box<dyn Source>,
        sink: Box<dyn Sink>,
        update_metrics: F,
        direction: SimplexDirection,
    ) -> Self {
        Self {
            source,
            sink,
            update_metrics,
            pending_chunk: Default::default(),
            direction,
            last_activity: Instant::now(),
        }
    }

    /// Initiate data exchange until the `Source` is closed or some error happened
    pub async fn exchange<T: Copy>(&mut self, id: T, timeout: Duration) -> Result<T, Error<T>> {
        loop {
            self.last_activity = Instant::now();

            let future = match self.pending_chunk.take() {
                None => async {
                    let x = self.source.read().await?;
                    log_dir!(trace, self.source.id(), self.direction, "TCP data: {}", x);
                    Ok(x)
                }.boxed(),
                Some(x) => async {
                    self.sink.wait_writable().await?;
                    log_dir!(trace, self.sink.id(), self.direction, "Sending unsent");
                    Ok(x)
                }.boxed(),
            };

            let data = tokio::time::timeout(timeout, future).await
                .unwrap_or_else(|_| Err(io::Error::from(ErrorKind::TimedOut)))
                .map_err(|e| io_to_pipe_error(id, e))?;

            match data {
                Data::Chunk(bytes) => {
                    let data_len = bytes.len();
                    let unsent_data = self.sink.write(bytes)
                        .map_err(|e| io_to_pipe_error(id, e))?;
                    let sent = data_len - unsent_data.len();
                    (self.update_metrics)(self.direction, sent);
                    self.source.consume(sent)
                        .map_err(|e| io_to_pipe_error(id, e))?;
                    if !unsent_data.is_empty() {
                        log_dir!(trace, self.source.id(), self.direction, "Unsent: {} bytes", unsent_data.len());
                        self.pending_chunk = Some(Data::Chunk(unsent_data));
                    }
                }
                Data::Eof => return self.sink.eof()
                    .map_err(|e| io_to_pipe_error(id, e))
                    .map(|_| id),
            }
        }
    }
}

pub(crate) struct DuplexPipe<F> {
    left_pipe: SimplexPipe<F>,
    right_pipe: SimplexPipe<F>,
}

impl<F: Fn(SimplexDirection, usize) + Send + Clone> DuplexPipe<F> {
    pub fn new(
        (dir1, source1, sink1): (SimplexDirection, Box<dyn Source>, Box<dyn Sink>),
        (dir2, source2, sink2): (SimplexDirection, Box<dyn Source>, Box<dyn Sink>),
        update_metrics: F,
    ) -> Self {
        Self {
            left_pipe: SimplexPipe::new(source1, sink1, update_metrics.clone(), dir1),
            right_pipe: SimplexPipe::new(source2, sink2, update_metrics, dir2),
        }
    }

    pub async fn exchange(&mut self, timeout: Duration) -> io::Result<()> {
        loop {
            match self.exchange_once(timeout).await {
                Err(e) if e.kind() == ErrorKind::TimedOut => {
                    let last_unexpired_timestamp = Instant::now() - timeout;
                    if self.left_pipe.last_activity < last_unexpired_timestamp
                        && self.right_pipe.last_activity < last_unexpired_timestamp
                    {
                        return Err(e);
                    }
                    // it is ok if only one of them timed out
                }
                x => return x,
            }
        }
    }

    async fn exchange_once(&mut self, timeout: Duration) -> io::Result<()> {
        let id = self.left_pipe.source.id();
        let f1 = self.left_pipe.exchange(self.left_pipe.direction, timeout);
        futures::pin_mut!(f1);
        let f2 = self.right_pipe.exchange(self.right_pipe.direction, timeout);
        futures::pin_mut!(f2);

        match future::try_select(f1, f2).await {
            Ok(Either::Left((dir, another)))
            | Ok(Either::Right((dir, another))) => {
                log_dir!(trace, id, dir, "Pipe gracefully closed");
                another.await
                    .map(|_| ())
                    .map_err(|e| {
                        log_dir!(debug, id, e.id, "Error on pipe: {}", e.io);
                        e.io
                    })
            }
            Err(Either::Left((e, _))) | Err(Either::Right((e, _))) => {
                if e.io.kind() != ErrorKind::WouldBlock {
                    log_dir!(debug, id, e.id, "Error on pipe: {}", e.io);
                }
                Err(e.io)
            }
        }
    }
}


fn io_to_pipe_error<T>(id: T, io: io::Error) -> Error<T> {
    Error { id, io, }
}
