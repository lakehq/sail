use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tonic::transport::server::Connected;

/// This state belongs to one accepted connection, even when a later connection
/// reuses the same peer address. Requests may also identify a session explicitly.
#[derive(Clone, Default)]
pub(super) struct FlightConnectionInfo {
    pub session_id: Arc<Mutex<Option<String>>>,
}

pub(super) struct FlightConnection {
    stream: TcpStream,
    info: FlightConnectionInfo,
}

impl FlightConnection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            info: FlightConnectionInfo::default(),
        }
    }
}

impl Connected for FlightConnection {
    type ConnectInfo = FlightConnectionInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.info.clone()
    }
}

impl AsyncRead for FlightConnection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for FlightConnection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}
