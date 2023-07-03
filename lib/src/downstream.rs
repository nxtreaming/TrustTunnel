use std::fmt::{Debug, Formatter};
use std::io;
use std::net::{IpAddr, SocketAddr};
use async_trait::async_trait;
use bytes::Bytes;
use crate::{authentication, datagram_pipe, forwarder, icmp_utils, log_utils, pipe, tunnel};
use crate::tls_demultiplexer::Protocol;
use crate::net_utils::TcpDestination;


#[derive(Debug, Hash, PartialEq, Eq)]
pub(crate) struct UdpDatagramMeta {
    pub source: SocketAddr,
    pub destination: SocketAddr,
    pub app_name: Option<String>,
}

pub(crate) struct UdpDatagram {
    pub meta: UdpDatagramMeta,
    pub payload: Bytes,
}

#[derive(Debug, Hash, PartialEq, Eq)]
pub(crate) struct IcmpDatagramMeta {
    pub peer: IpAddr,
}

#[derive(Debug)]
pub(crate) struct IcmpDatagram {
    pub meta: IcmpDatagramMeta,
    pub message: icmp_utils::Message,
    pub ttl: u8,
}

pub(crate) trait StreamId {
    /// Get the request ID for logging
    fn id(&self) -> log_utils::IdChain<u64>;
}

/// An abstract interface for a still-not-responded request
pub(crate) trait PendingRequest: StreamId + Send {
    type NextState;

    /// Proceed the request to the next state
    fn promote_to_next_state(self: Box<Self>) -> io::Result<Self::NextState>;

    /// Notify a client of a multiplexer open failure
    fn fail_request(self: Box<Self>, error: tunnel::ConnectionError);
}

/// An abstract interface for a pre-demultiplexed request
pub(crate) trait PendingMultiplexedRequest:
    StreamId
    + PendingRequest<NextState = Option<PendingDemultiplexedRequest>>
    + Send
{
    /// Get the authorization info
    fn auth_info(&self) -> io::Result<Option<authentication::Source<'_>>>;
}

pub(crate) enum PendingDemultiplexedRequest {
    TcpConnect(Box<dyn PendingTcpConnectRequest>),
    DatagramMultiplexer(Box<dyn PendingDatagramMultiplexerRequest>),
}

/// An abstract interface for a TCP connection request implementation
pub(crate) trait PendingTcpConnectRequest:
    StreamId
    + PendingRequest<NextState = (Box<dyn pipe::Source>, Box<dyn pipe::Sink>)>
    + Send
{
    /// Get the address of a VPN client made the connection request
    fn client_address(&self) -> io::Result<IpAddr>;

    /// Get the target host
    fn destination(&self) -> io::Result<TcpDestination>;

    /// Get the user agent
    fn user_agent(&self) -> Option<String>;
}

pub(crate) enum DatagramPipeHalves {
    Udp(Box<dyn datagram_pipe::Source<Output = UdpDatagram>>, Box<dyn datagram_pipe::Sink<Input = forwarder::UdpDatagram>>),
    Icmp(Box<dyn datagram_pipe::Source<Output = IcmpDatagram>>, Box<dyn datagram_pipe::Sink<Input = forwarder::IcmpDatagram>>),
}

/// An abstract interface for a datagram multiplexer open request implementation
pub(crate) trait PendingDatagramMultiplexerRequest:
    StreamId
    + PendingRequest<NextState = DatagramPipeHalves>
    + Send
{
    /// Get the address of a VPN client made the connection request
    fn client_address(&self) -> io::Result<IpAddr>;

    /// Get the user agent
    fn user_agent(&self) -> Option<String>;
}

/// An abstract interface for a downstream implementation which communicates with a client
#[async_trait]
pub(crate) trait Downstream: Send {
    /// Listen to events on the client-side.
    /// Returns `None` in case the listening finished gracefully and should not be continued,
    /// `Some` in case the downstream encountered the new authorization request which should be
    /// processed and listening should be continued.
    async fn listen(&mut self) -> io::Result<Option<Box<dyn PendingMultiplexedRequest>>>;

    /// Shut down the downstream connection gracefully
    async fn graceful_shutdown(&mut self) -> io::Result<()>;

    /// Get the downstream protocol
    fn protocol(&self) -> Protocol;

    /// Get the domain name used for TLS session (SNI)
    fn tls_domain(&self) -> &str;
}

impl Debug for UdpDatagram {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "meta={:?}, payload={}B", self.meta, self.payload.len())
    }
}

impl datagram_pipe::Datagram for IcmpDatagram {
    fn len(&self) -> usize {
        self.message.len()
    }
}
