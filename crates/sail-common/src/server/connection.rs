// SPDX-License-Identifier: Apache-2.0

use std::io::{self, IoSlice};
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};
use tonic::transport::server::{Connected, TcpConnectInfo};

/// Interrupts transport I/O even when HTTP/2 flow control has stopped polling a response body.
pub(super) struct ServerConnection {
    stream: TcpStream,
    closed: Pin<Box<WaitForCancellationFutureOwned>>,
}

impl ServerConnection {
    pub(super) fn new(stream: TcpStream, closed: CancellationToken) -> Self {
        Self {
            stream,
            closed: Box::pin(closed.cancelled_owned()),
        }
    }

    fn io(&mut self, cx: &mut Context<'_>) -> io::Result<Pin<&mut TcpStream>> {
        if self.closed.as_mut().poll(cx).is_ready() {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "server shutdown grace period elapsed",
            ));
        }
        Ok(Pin::new(&mut self.stream))
    }
}

impl Connected for ServerConnection {
    type ConnectInfo = TcpConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.stream.connect_info()
    }
}

impl AsyncRead for ServerConnection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.io(cx)?.poll_read(cx, buf)
    }
}

impl AsyncWrite for ServerConnection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.io(cx)?.poll_write(cx, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.io(cx)?.poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.stream.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.io(cx)?.poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.io(cx)?.poll_shutdown(cx)
    }
}
