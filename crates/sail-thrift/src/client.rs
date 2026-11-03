//! Async Thrift RPC runtime using Apache Thrift codecs over bounded memory buffers.

use std::future::poll_fn;
use std::io::Cursor;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::task::Poll;
use std::time::Duration;

use thrift::protocol::{
    TBinaryInputProtocol, TBinaryOutputProtocol, TInputProtocol, TMessageIdentifier, TMessageType,
    TOutputProtocol,
};
use thrift::{ApplicationError, ApplicationErrorKind};
use tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite, BufReader};
use tokio::sync::{Mutex, Semaphore};
use tokio::time::Instant;

use crate::codec::invalid_data;
use crate::transport::Transport;

const IDLE_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_CONCURRENT_CALLS: usize = 8;

pub type ReadHalf = Box<dyn AsyncRead + Unpin + Send>;
pub type WriteHalf = Box<dyn AsyncWrite + Unpin + Send>;

/// Callers supply connection setup and authentication, independently of generated code.
#[async_trait::async_trait]
pub trait Connector: Send + Sync {
    async fn connect(&self) -> std::io::Result<(ReadHalf, WriteHalf)>;
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("transport error: {0}")]
    Transport(#[source] std::io::Error),
    #[error("{0}")]
    Thrift(#[from] thrift::Error),
}

impl ClientError {
    fn io(error: std::io::Error) -> Self {
        if error.kind() == std::io::ErrorKind::InvalidData {
            Self::Thrift(invalid_data(&error.to_string()))
        } else {
            Self::Transport(error)
        }
    }

    pub fn is_unknown_method(&self) -> bool {
        matches!(self, Self::Thrift(thrift::Error::Application(e)) if e.kind == ApplicationErrorKind::UnknownMethod)
    }
}

/// Declared service exceptions remain typed, independently of transport/protocol errors.
#[derive(Debug, PartialEq)]
pub enum MaybeException<T, E> {
    Ok(T),
    Exception(E),
}

#[derive(Clone)]
pub struct Client(Arc<ClientInner>);

struct ClientInner {
    connector: Arc<dyn Connector>,
    transport: Transport,
    idle: Mutex<IdlePool>,
    permits: Semaphore,
}

#[derive(Default)]
struct IdlePool {
    connections: Vec<IdleConnection>,
    expiring: bool,
}

struct Connection {
    reader: BufReader<ReadHalf>,
    writer: WriteHalf,
    sequence: i32,
}

struct IdleConnection {
    connection: Connection,
    since: Instant,
}

impl Client {
    pub fn new(connector: impl Connector + 'static, transport: Transport) -> Self {
        Self(Arc::new(ClientInner {
            connector: Arc::new(connector),
            transport,
            idle: Mutex::new(IdlePool::default()),
            permits: Semaphore::new(MAX_CONCURRENT_CALLS),
        }))
    }

    async fn take_idle_connection(&self) -> Option<Connection> {
        loop {
            let idle = {
                let mut idle = self.0.idle.lock().await;
                idle.connections
                    .retain(|idle| idle.since.elapsed() < IDLE_TIMEOUT);
                idle.connections.pop()
            };
            let mut connection = idle?.connection;
            // No response is outstanding while idle. EOF, an error, or unsolicited
            // bytes makes the connection unusable. Poll once so a healthy peer
            // waiting for the next request never blocks checkout.
            let reusable = poll_fn(|cx| {
                Poll::Ready(
                    Pin::new(&mut connection.reader)
                        .poll_fill_buf(cx)
                        .is_pending(),
                )
            })
            .await;
            if reusable {
                return Some(connection);
            }
        }
    }

    async fn put_idle_connection(&self, connection: Connection) {
        let mut idle = self.0.idle.lock().await;
        idle.connections.push(IdleConnection {
            connection,
            since: Instant::now(),
        });
        if !idle.expiring {
            idle.expiring = true;
            tokio::spawn(Self::expire_idle_connections(Arc::downgrade(&self.0)));
        }
    }

    async fn expire_idle_connections(inner: Weak<ClientInner>) {
        loop {
            // Do not keep the client or its sockets alive while waiting for expiration.
            let deadline = {
                let Some(inner) = inner.upgrade() else {
                    return;
                };
                let mut idle = inner.idle.lock().await;
                idle.connections
                    .retain(|idle| idle.since.elapsed() < IDLE_TIMEOUT);
                // Connections are appended in order of their return to the pool.
                match idle.connections.first() {
                    Some(connection) => connection.since + IDLE_TIMEOUT,
                    None => {
                        idle.expiring = false;
                        return;
                    }
                }
            };
            tokio::time::sleep_until(deadline).await;
        }
    }

    /// Invoke a generated RPC using synchronous codecs and asynchronous network I/O.
    pub async fn call<T>(
        &self,
        method: &str,
        encode: impl FnOnce(&mut dyn TOutputProtocol) -> thrift::Result<()>,
        decode: impl FnOnce(&mut dyn TInputProtocol) -> thrift::Result<T>,
    ) -> Result<T, ClientError> {
        let _permit = self
            .0
            .permits
            .acquire()
            .await
            .map_err(|e| ClientError::Transport(std::io::Error::other(e.to_string())))?;
        let idle = self.take_idle_connection().await;
        // A call owns its connection across every await. Cancellation or any error
        // drops it, so partially written/read messages can never enter the idle pool.
        let mut connection = match idle {
            Some(connection) => connection,
            None => {
                let (reader, writer) = self
                    .0
                    .connector
                    .connect()
                    .await
                    .map_err(ClientError::Transport)?;
                Connection {
                    reader: BufReader::new(reader),
                    writer,
                    sequence: 0,
                }
            }
        };

        connection.sequence = connection.sequence.wrapping_add(1);
        let sequence = connection.sequence;
        let mut request = Vec::new();
        {
            let mut protocol = TBinaryOutputProtocol::new(&mut request, true);
            protocol.write_message_begin(&TMessageIdentifier::new(
                method,
                TMessageType::Call,
                sequence,
            ))?;
            encode(&mut protocol)?;
            protocol.write_message_end()?;
        }

        crate::transport::write_message(&mut connection.writer, self.0.transport, &request)
            .await
            .map_err(ClientError::io)?;
        let response = crate::transport::read_message(&mut connection.reader, self.0.transport)
            .await
            .map_err(ClientError::io)?;
        let mut cursor = Cursor::new(response.as_slice());
        let result = {
            let config = thrift::TConfiguration::builder()
                .max_message_size(Some(response.len()))
                .max_frame_size(Some(response.len()))
                .max_string_size(Some(response.len()))
                .max_container_size(Some(response.len()))
                .build()?;
            let mut protocol = TBinaryInputProtocol::with_config(&mut cursor, false, config);
            let header = protocol.read_message_begin()?;
            if header.sequence_number != sequence {
                return Err(ApplicationError::new(
                    ApplicationErrorKind::BadSequenceId,
                    "Thrift reply sequence mismatch",
                )
                .into());
            }

            if header.name != method {
                return Err(ApplicationError::new(
                    ApplicationErrorKind::WrongMethodName,
                    "Thrift reply method mismatch",
                )
                .into());
            }

            match header.message_type {
                TMessageType::Reply => {
                    let value = decode(&mut protocol)?;
                    protocol.read_message_end()?;
                    value
                }
                TMessageType::Exception => {
                    let error =
                        thrift::Error::read_application_error_from_in_protocol(&mut protocol)?;
                    protocol.read_message_end()?;
                    return Err(thrift::Error::Application(error).into());
                }
                _ => {
                    return Err(ApplicationError::new(
                        ApplicationErrorKind::InvalidMessageType,
                        "invalid Thrift reply type",
                    )
                    .into());
                }
            }
        };

        if cursor.position() != response.len() as u64 {
            return Err(invalid_data("trailing bytes in Thrift reply").into());
        }

        self.put_idle_connection(connection).await;
        Ok(result)
    }
}

impl From<ApplicationError> for ClientError {
    fn from(error: ApplicationError) -> Self {
        Self::Thrift(error.into())
    }
}
