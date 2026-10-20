//! Async binary-message boundary detection. Typed decoding uses Apache Thrift.

use std::io;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};

/// Bound complete messages, including unframed messages, before typed decoding.
pub(crate) const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;
const MAX_DEPTH: usize = 64;

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
        if (header as u32 & 0xffff_0000) != 0x8001_0000 {
            return Err(invalid("invalid Thrift binary protocol version"));
        }

        let size = scanner.i32().await?;
        scanner.bytes(checked_size(size)?).await?;
    } else {
        // Legacy non-versioned binary message header.
        scanner.bytes(checked_size(header)?).await?;
        scanner.bytes(1).await?;
    }

    scanner.bytes(4).await?; // sequence number
    // Explicit tasks keep nested/container scanning linear without recursive futures.
    let mut tasks = vec![Task::Value(12, 0)];
    while let Some(task) = tasks.pop() {
        match task {
            Task::Struct(depth) => {
                let ty = scanner.byte().await?;
                if ty != 0 {
                    scanner.bytes(2).await?; // field ID
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
                    2 | 3 => scanner.bytes(1).await?,
                    6 => scanner.bytes(2).await?,
                    8 => scanner.bytes(4).await?,
                    4 | 10 => scanner.bytes(8).await?,
                    16 => scanner.bytes(16).await?,
                    11 => {
                        let size = scanner.i32().await?;
                        scanner.bytes(checked_size(size)?).await?;
                    }
                    12 => tasks.push(Task::Struct(depth + 1)),
                    13..=15 => {
                        let first = scanner.byte().await?;
                        let second = if ty == 13 {
                            Some(scanner.byte().await?)
                        } else {
                            None
                        };

                        if !valid_type(first) || second.is_some_and(|t| !valid_type(t)) {
                            return Err(invalid("invalid Thrift container type"));
                        }

                        let size = scanner.i32().await?;
                        let size = checked_size(size)?;
                        if size > MAX_MESSAGE_SIZE / 2 {
                            return Err(invalid("Thrift container exceeds size limit"));
                        }
                        tasks.push(Task::Repeated {
                            first,
                            second,
                            remaining: size,
                            depth: depth + 1,
                        });
                    }
                    _ => return Err(invalid("unsupported Thrift binary field type")),
                }
            }
        }
    }

    Ok(scanner.message)
}

fn valid_type(ty: u8) -> bool {
    matches!(ty, 2 | 3 | 4 | 6 | 8 | 10 | 11 | 12 | 13 | 14 | 15 | 16)
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
    Value(u8, usize),
    Struct(usize),
    Repeated {
        first: u8,
        second: Option<u8>,
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
        self.bytes(1).await?;
        Ok(self.message[self.message.len() - 1])
    }

    async fn i32(&mut self) -> io::Result<i32> {
        self.bytes(4).await?;
        let start = self.message.len() - 4;
        let bytes = self.message[start..]
            .try_into()
            .map_err(|_| invalid("invalid i32"))?;
        Ok(i32::from_be_bytes(bytes))
    }
}
