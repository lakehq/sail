//! Async binary-message boundary detection. Typed decoding uses Apache Thrift.
//!
//! Wire layouts follow the [Thrift Binary protocol] and [framed transport] specifications.
//! The message size, container size, and nesting limits below are local safety policies.
//!
//! [Thrift Binary protocol]: https://github.com/apache/thrift/blob/master/doc/specs/thrift-binary-protocol.md
//! [framed transport]: https://github.com/apache/thrift/blob/master/doc/specs/thrift-rpc.md#framed-vs-unframed-transport

use std::io;

use num_enum::TryFromPrimitive;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};

/// Bound complete messages, including unframed messages, before typed decoding.
pub(crate) const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;
const MAX_DEPTH: usize = 64;
/// Bound scanner work independently of the message byte limit.
const MAX_CONTAINER_ELEMENTS: usize = 32 * 1024 * 1024;

const BINARY_VERSION_MASK: u32 = 0xffff_0000;
const BINARY_VERSION_1: u32 = 0x8001_0000;
const UUID_BYTE_LEN: usize = 16;

/// Field type identifiers in the Binary protocol, including the struct terminator.
#[derive(Clone, Copy, Debug, PartialEq, Eq, TryFromPrimitive)]
#[repr(u8)]
enum BinaryType {
    Stop = 0,
    Bool = 2,
    Byte = 3,
    Double = 4,
    I16 = 6,
    I32 = 8,
    I64 = 10,
    String = 11,
    Struct = 12,
    Map = 13,
    Set = 14,
    List = 15,
    Uuid = 16,
}

#[derive(Clone, Copy, Debug)]
pub enum Transport {
    Buffered,
    Framed,
}

/// Write and flush one binary message with the selected transport framing.
pub async fn write_message<W: AsyncWrite + Unpin + ?Sized>(
    writer: &mut W,
    transport: Transport,
    message: &[u8],
) -> io::Result<()> {
    if message.len() > MAX_MESSAGE_SIZE {
        return Err(invalid("Thrift message exceeds size limit"));
    }

    if matches!(transport, Transport::Framed) {
        writer
            .write_all(&(message.len() as i32).to_be_bytes())
            .await?;
    }
    writer.write_all(message).await?;
    writer.flush().await
}

/// Read one bounded binary message, retaining buffered bytes for subsequent messages.
pub async fn read_message<R: AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
    transport: Transport,
) -> io::Result<Vec<u8>> {
    if matches!(transport, Transport::Framed) {
        let size = reader.read_i32().await?;
        if size <= 0 || size as usize > MAX_MESSAGE_SIZE {
            return Err(invalid("invalid Thrift frame length"));
        }

        let mut message = vec![0; size as usize];
        reader.read_exact(&mut message).await?;
        return Ok(message);
    }

    let mut scanner = Scanner {
        reader,
        message: Vec::new(),
    };

    let header = scanner.i32().await?;
    if header < 0 {
        if (header as u32 & BINARY_VERSION_MASK) != BINARY_VERSION_1 {
            return Err(invalid("invalid Thrift binary protocol version"));
        }

        let size = scanner.i32().await?;
        scanner.bytes(checked_size(size)?).await?;
    } else {
        // Legacy non-versioned binary message header.
        scanner.bytes(checked_size(header)?).await?;
        scanner.bytes(size_of::<u8>()).await?; // message type
    }

    scanner.bytes(size_of::<i32>()).await?; // sequence number
    // Explicit tasks keep nested/container scanning linear without recursive futures.
    let mut tasks = vec![Task::Value(BinaryType::Struct, 0)];
    while let Some(task) = tasks.pop() {
        match task {
            Task::Struct(depth) => {
                let ty = scanner.field_type().await?;
                if ty != BinaryType::Stop {
                    scanner.bytes(size_of::<i16>()).await?; // field ID
                    tasks.push(Task::Struct(depth));
                    tasks.push(Task::Value(ty, depth));
                }
            }
            Task::Repeated {
                first,
                second,
                remaining,
                depth,
            } => {
                if remaining > 0 {
                    tasks.push(Task::Repeated {
                        first,
                        second,
                        remaining: remaining - 1,
                        depth,
                    });
                    if let Some(ty) = second {
                        tasks.push(Task::Value(ty, depth));
                    }
                    tasks.push(Task::Value(first, depth));
                }
            }
            Task::Value(ty, depth) => {
                if depth >= MAX_DEPTH {
                    return Err(invalid("Thrift nesting exceeds depth limit"));
                }

                match ty {
                    BinaryType::Bool | BinaryType::Byte => scanner.bytes(size_of::<u8>()).await?,
                    BinaryType::I16 => scanner.bytes(size_of::<i16>()).await?,
                    BinaryType::I32 => scanner.bytes(size_of::<i32>()).await?,
                    BinaryType::I64 => scanner.bytes(size_of::<i64>()).await?,
                    BinaryType::Double => scanner.bytes(size_of::<f64>()).await?,
                    BinaryType::Uuid => scanner.bytes(UUID_BYTE_LEN).await?,
                    BinaryType::String => {
                        let size = scanner.i32().await?;
                        scanner.bytes(checked_size(size)?).await?;
                    }
                    BinaryType::Struct => tasks.push(Task::Struct(depth + 1)),
                    BinaryType::Map | BinaryType::Set | BinaryType::List => {
                        let first = scanner.field_type().await?;
                        let second = if ty == BinaryType::Map {
                            Some(scanner.field_type().await?)
                        } else {
                            None
                        };

                        if first == BinaryType::Stop || second == Some(BinaryType::Stop) {
                            return Err(invalid("invalid Thrift container type"));
                        }

                        let size = scanner.i32().await?;
                        let size = checked_size(size)?;
                        if size > MAX_CONTAINER_ELEMENTS {
                            return Err(invalid("Thrift container exceeds size limit"));
                        }
                        tasks.push(Task::Repeated {
                            first,
                            second,
                            remaining: size,
                            depth: depth + 1,
                        });
                    }
                    BinaryType::Stop => return Err(invalid("unexpected Thrift stop field")),
                }
            }
        }
    }

    Ok(scanner.message)
}

fn checked_size(size: i32) -> io::Result<usize> {
    if size < 0 || size as usize > MAX_MESSAGE_SIZE {
        Err(invalid("invalid Thrift length"))
    } else {
        Ok(size as usize)
    }
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

enum Task {
    Value(BinaryType, usize),
    Struct(usize),
    Repeated {
        first: BinaryType,
        second: Option<BinaryType>,
        remaining: usize,
        depth: usize,
    },
}

struct Scanner<'a, R> {
    reader: &'a mut R,
    message: Vec<u8>,
}

impl<R: AsyncRead + Unpin> Scanner<'_, R> {
    async fn bytes(&mut self, count: usize) -> io::Result<()> {
        let start = self.message.len();
        let end = start
            .checked_add(count)
            .filter(|end| *end <= MAX_MESSAGE_SIZE)
            .ok_or_else(|| invalid("Thrift message exceeds size limit"))?;
        self.message.resize(end, 0);
        self.reader.read_exact(&mut self.message[start..]).await?;
        Ok(())
    }

    async fn byte(&mut self) -> io::Result<u8> {
        self.bytes(size_of::<u8>()).await?;
        Ok(self.message[self.message.len() - 1])
    }

    async fn field_type(&mut self) -> io::Result<BinaryType> {
        BinaryType::try_from(self.byte().await?)
            .map_err(|_| invalid("unsupported Thrift binary field type"))
    }

    async fn i32(&mut self) -> io::Result<i32> {
        self.bytes(size_of::<i32>()).await?;
        let start = self.message.len() - size_of::<i32>();
        let bytes = self.message[start..]
            .try_into()
            .map_err(|_| invalid("invalid i32"))?;
        Ok(i32::from_be_bytes(bytes))
    }
}
