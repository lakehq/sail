use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use num_enum::{IntoPrimitive, TryFromPrimitive};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, MutexGuard};

use crate::error::{CelebornError, CelebornResult};
use crate::protocol::StatusCode;
use crate::protocol::proto::MessageType;

/// Netty transport frame types, mirroring the `Message.Type` identifiers in Java.
#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
enum TransportFrameType {
    ChunkFetchRequest = 0,
    ChunkFetchSuccess = 1,
    ChunkFetchFailure = 2,
    RpcRequest = 3,
    RpcResponse = 4,
    RpcFailure = 5,
    OpenStream = 6,
    StreamHandle = 7,
    OneWayMessage = 9,
    PushData = 11,
    PushMergedData = 12,
    RegionStart = 13,
    RegionFinish = 14,
    PushDataHandshake = 15,
    ReadAddCredit = 16,
    ReadData = 17,
    OpenStreamWithCredit = 18,
    BacklogAnnouncement = 19,
    TransportableError = 20,
    BufferStreamEnd = 21,
    Heartbeat = 22,
    SegmentStart = 23,
    NotifyRequiredSegment = 24,
    SubpartitionReadData = 25,
}

/// A fully decoded Netty transport frame.
///
/// Celeborn's frame decoder reads the fixed prefix first, then consumes `header` and `body`
/// separately. Keeping that split is important: failures put their error string in the encoded
/// header, whereas successful RPCs put their payload in the body.
#[derive(Debug)]
struct TransportFrame<'a> {
    pub frame_type: TransportFrameType,
    pub header: Vec<u8>,
    pub body: TransportBody<'a>,
}

#[derive(Debug)]
enum TransportBody<'a> {
    Owned(Vec<u8>),
    PrefixAndData { prefix: Vec<u8>, data: &'a [u8] },
}

impl TransportBody<'_> {
    fn len(&self) -> usize {
        match self {
            Self::Owned(body) => body.len(),
            Self::PrefixAndData { prefix, data } => prefix.len() + data.len(),
        }
    }

    async fn write<W>(&self, writer: &mut W) -> std::io::Result<()>
    where
        W: AsyncWriteExt + Unpin,
    {
        match self {
            Self::Owned(body) => writer.write_all(body).await,
            Self::PrefixAndData { prefix, data } => {
                writer.write_all(prefix).await?;
                writer.write_all(data).await
            }
        }
    }

    fn into_owned(self) -> Vec<u8> {
        match self {
            Self::Owned(body) => body,
            Self::PrefixAndData { mut prefix, data } => {
                prefix.extend_from_slice(data);
                prefix
            }
        }
    }
}

impl TransportFrame<'static> {
    pub fn new(frame_type: TransportFrameType, header: Vec<u8>, body: Vec<u8>) -> Self {
        Self {
            frame_type,
            header,
            body: TransportBody::Owned(body),
        }
    }

    fn rpc_request(request_id: i64, body: Vec<u8>) -> CelebornResult<Self> {
        let body_length = i32::try_from(body.len())
            .map_err(|_| CelebornError::Protocol("transport body is too large".to_string()))?;
        let mut header = Vec::with_capacity(12);
        header.extend_from_slice(&request_id.to_be_bytes());
        header.extend_from_slice(&body_length.to_be_bytes());
        Ok(Self::new(TransportFrameType::RpcRequest, header, body))
    }

    pub async fn read<R>(reader: &mut R) -> CelebornResult<Self>
    where
        R: AsyncReadExt + Unpin,
    {
        let header_length = usize::try_from(reader.read_i32().await?)
            .map_err(|_| CelebornError::Protocol("invalid transport header length".to_string()))?;
        let frame_type = TransportFrameType::try_from(reader.read_u8().await?)
            .map_err(|error| CelebornError::Protocol(error.to_string()))?;
        let body_length = usize::try_from(reader.read_i32().await?)
            .map_err(|_| CelebornError::Protocol("invalid transport body length".to_string()))?;
        let mut header = vec![0; header_length];
        let mut body = vec![0; body_length];
        reader.read_exact(&mut header).await?;
        reader.read_exact(&mut body).await?;
        Ok(Self::new(frame_type, header, body))
    }
}

/// A fully validated reply. A remote failure completes the exchange on the socket.
enum TransportReply {
    Success(Vec<u8>),
    Failure(String),
}

impl TransportReply {
    /// Both RPC and chunk failures encode a length-prefixed error string in the header.
    fn failure(header: &[u8], body: &TransportBody<'_>) -> CelebornResult<Self> {
        let (length, message) = header
            .split_first_chunk::<4>()
            .ok_or_else(|| CelebornError::Protocol("missing failure message length".to_string()))?;
        if body.len() != 0
            || usize::try_from(i32::from_be_bytes(*length)).ok() != Some(message.len())
        {
            return Err(CelebornError::Protocol(
                "invalid failure message length".to_string(),
            ));
        }
        Ok(Self::Failure(String::from_utf8_lossy(message).into_owned()))
    }
}

impl TransportFrame<'_> {
    /// Returns a matching reply, or skips a well-formed RPC reply to a different request.
    fn reply_to(self, request: &TransportFrame<'_>) -> CelebornResult<Option<TransportReply>> {
        match self.frame_type {
            TransportFrameType::RpcResponse | TransportFrameType::RpcFailure => {
                self.rpc_reply_to(request)
            }
            TransportFrameType::ChunkFetchSuccess | TransportFrameType::ChunkFetchFailure
                if request.frame_type == TransportFrameType::ChunkFetchRequest =>
            {
                // Celeborn echoes the requested StreamChunkSlice, including the requested
                // length when the last chunk contains fewer bytes.
                let header = self
                    .header
                    .strip_prefix(request.header.as_slice())
                    .ok_or_else(|| {
                        CelebornError::Protocol("chunk response does not match request".to_string())
                    })?;
                let reply = if self.frame_type == TransportFrameType::ChunkFetchFailure {
                    TransportReply::failure(header, &self.body)?
                } else if header.is_empty() {
                    TransportReply::Success(self.body.into_owned())
                } else {
                    return Err(CelebornError::Protocol(
                        "invalid chunk response header length".to_string(),
                    ));
                };
                Ok(Some(reply))
            }
            frame_type => Err(CelebornError::Protocol(format!(
                "unexpected {frame_type:?} reply to {:?}",
                request.frame_type
            ))),
        }
    }

    fn rpc_reply_to(self, request: &TransportFrame<'_>) -> CelebornResult<Option<TransportReply>> {
        let (request_id, header) = self
            .header
            .split_first_chunk::<8>()
            .ok_or_else(|| CelebornError::Protocol("invalid RPC reply ID".to_string()))?;
        let reply = if self.frame_type == TransportFrameType::RpcFailure {
            TransportReply::failure(header, &self.body)?
        } else {
            let length: [u8; 4] = header.try_into().map_err(|_| {
                CelebornError::Protocol("invalid RPC response header length".to_string())
            })?;
            if usize::try_from(i32::from_be_bytes(length)).ok() != Some(self.body.len()) {
                return Err(CelebornError::Protocol(
                    "invalid RPC response length".to_string(),
                ));
            }
            TransportReply::Success(self.body.into_owned())
        };

        // RPC and push requests both start with their ID. A chunk fetch has no RPC ID.
        let matches = request.frame_type != TransportFrameType::ChunkFetchRequest
            && request.header.starts_with(request_id);
        Ok(matches.then_some(reply))
    }

    pub async fn write<W>(&self, writer: &mut W) -> CelebornResult<()>
    where
        W: AsyncWriteExt + Unpin,
    {
        let header_length = i32::try_from(self.header.len())
            .map_err(|_| CelebornError::Protocol("transport header is too large".to_string()))?;
        let body_length = i32::try_from(self.body.len())
            .map_err(|_| CelebornError::Protocol("transport body is too large".to_string()))?;
        writer.write_i32(header_length).await?;
        writer.write_u8(self.frame_type.into()).await?;
        writer.write_i32(body_length).await?;
        writer.write_all(&self.header).await?;
        self.body.write(writer).await?;
        writer.flush().await?;
        Ok(())
    }
}

/// A shared, serialized TCP connection to one Celeborn Netty endpoint.
///
/// Netty permits multiple outstanding RPCs, but a small client only needs one transaction at a
/// time. The mutex gives clones a single reusable connection while retaining exact request/reply
/// ordering. A transaction that does not complete (an I/O, timeout, or framing error, or a
/// caller that stops waiting for the reply) invalidates the connection before it is reused.
#[derive(Debug, Clone)]
pub struct TransportConnection {
    host: String,
    port: u16,
    timeout: Duration,
    stream: Arc<Mutex<Option<TcpStream>>>,
    next_request_id: Arc<AtomicI64>,
}

impl TransportConnection {
    pub fn new(host: impl Into<String>, port: u16, timeout: Duration) -> Self {
        Self {
            host: host.into(),
            port,
            timeout,
            stream: Arc::new(Mutex::new(None)),
            next_request_id: Arc::new(AtomicI64::new(0)),
        }
    }

    #[cfg(test)]
    pub(crate) fn shares_stream_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.stream, &other.stream)
    }

    fn next_request_id(&self) -> i64 {
        self.next_request_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Send a Netty RPC request and return its response payload.
    pub(crate) async fn send_rpc(&self, body: Vec<u8>) -> CelebornResult<Vec<u8>> {
        self.send_rpc_with_id(self.next_request_id(), body).await
    }

    /// Send an RPC with a caller-assigned ID unique to this connection.
    pub async fn send_rpc_with_id(
        &self,
        request_id: i64,
        body: Vec<u8>,
    ) -> CelebornResult<Vec<u8>> {
        self.request(TransportFrame::rpc_request(request_id, body)?)
            .await
    }

    /// Push one map-data batch using Celeborn's `PushData` transport frame.
    pub(crate) async fn push_data(
        &self,
        shuffle_key: &str,
        partition_unique_id: &str,
        map_id: i32,
        attempt_id: i32,
        batch_id: i32,
        data: &[u8],
    ) -> CelebornResult<()> {
        let request_id = self.next_request_id();
        let shuffle_key = shuffle_key.as_bytes();
        let partition_unique_id = partition_unique_id.as_bytes();
        let header_length = 8 + 1 + 4 + shuffle_key.len() + 4 + partition_unique_id.len();
        let header_length = i32::try_from(header_length)
            .map_err(|_| CelebornError::Protocol("push header is too large".to_string()))?;
        let data_length = i32::try_from(data.len())
            .map_err(|_| CelebornError::Protocol("push data is too large".to_string()))?;
        let mut header = Vec::with_capacity(header_length as usize);
        header.extend_from_slice(&request_id.to_be_bytes());
        header.push(0);
        write_bytes(&mut header, shuffle_key)?;
        write_bytes(&mut header, partition_unique_id)?;
        let prefix = batch_header(map_id, attempt_id, batch_id, data_length);

        let frame = TransportFrame {
            frame_type: TransportFrameType::PushData,
            header,
            body: TransportBody::PrefixAndData { prefix, data },
        };
        let response = self.request(frame).await?;
        match response.first().copied() {
            None => Ok(()),
            Some(status)
                if status == StatusCode::Success as u8
                    || status == StatusCode::MapEnded as u8
                    || status == StatusCode::PushDataSuccessPrimaryCongested as u8
                    || status == StatusCode::PushDataSuccessReplicaCongested as u8 =>
            {
                Ok(())
            }
            Some(status) => Err(CelebornError::Worker {
                status: i32::from(status),
            }),
        }
    }

    /// Fetch a slice of a shuffle chunk using Celeborn's `ChunkFetchRequest` frame.
    pub(crate) async fn fetch_chunk(
        &self,
        stream_id: i64,
        chunk_index: i32,
        offset: i32,
        length: i32,
    ) -> CelebornResult<Vec<u8>> {
        let mut header = Vec::with_capacity(20);
        header.extend_from_slice(&stream_id.to_be_bytes());
        header.extend_from_slice(&chunk_index.to_be_bytes());
        header.extend_from_slice(&offset.to_be_bytes());
        header.extend_from_slice(&length.to_be_bytes());
        self.request(TransportFrame::new(
            TransportFrameType::ChunkFetchRequest,
            header,
            Vec::new(),
        ))
        .await
    }

    async fn request(&self, frame: TransportFrame<'_>) -> CelebornResult<Vec<u8>> {
        // Keep the lock outside the I/O timeout. A caller queued behind another transaction must
        // not time out and clear the connection that the active transaction is using.
        let mut transaction = Transaction {
            stream: self.stream.lock().await,
            completed: false,
        };
        let reply = tokio::time::timeout(self.timeout, async {
            let stream = &mut *transaction.stream;
            if stream.is_none() {
                *stream = Some(TcpStream::connect((&*self.host, self.port)).await?);
            }
            let stream = stream.as_mut().ok_or_else(|| {
                CelebornError::Protocol("connection was not initialized".to_string())
            })?;
            frame.write(stream).await?;
            loop {
                let response = TransportFrame::read(stream).await?;
                if let Some(reply) = response.reply_to(&frame)? {
                    return Ok::<_, CelebornError>(reply);
                }
            }
        })
        .await
        .map_err(|_| CelebornError::Timeout)??;
        transaction.completed = true;
        match reply {
            TransportReply::Success(data) => Ok(data),
            TransportReply::Failure(message) => Err(CelebornError::Protocol(message)),
        }
    }
}

/// A transaction on a connection, which discards the connection unless it completes.
///
/// A transaction that fails, times out, or is dropped by its caller after its request is
/// written can leave unread bytes in the socket. Starting fresh is safer than allowing the
/// next request to interpret those bytes as its reply.
struct Transaction<'a> {
    stream: MutexGuard<'a, Option<TcpStream>>,
    completed: bool,
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if !self.completed {
            *self.stream = None;
        }
    }
}

/// Returns the header of a batch of pushed data, which the shuffle service reads when it
/// sorts a shuffle file by map index. The fields use native byte order to match
/// `Platform.putInt` and `Platform.getInt` in Celeborn. Clients and workers must have
/// the same endianness.
pub(crate) fn batch_header(map_id: i32, attempt_id: i32, batch_id: i32, length: i32) -> Vec<u8> {
    let mut header = Vec::with_capacity(16);
    header.extend_from_slice(&map_id.to_ne_bytes());
    header.extend_from_slice(&attempt_id.to_ne_bytes());
    header.extend_from_slice(&batch_id.to_ne_bytes());
    header.extend_from_slice(&length.to_ne_bytes());
    header
}

fn write_bytes(buffer: &mut Vec<u8>, bytes: &[u8]) -> CelebornResult<()> {
    buffer.extend_from_slice(
        &i32::try_from(bytes.len())
            .map_err(|_| CelebornError::Protocol("byte array is too large".to_string()))?
            .to_be_bytes(),
    );
    buffer.extend_from_slice(bytes);
    Ok(())
}

/// A Celeborn control message, independent of whether it is sent or received.
pub(crate) struct TransportMessage {
    pub message_type: MessageType,
    pub payload: Vec<u8>,
}

impl TransportMessage {
    pub(crate) fn new(message_type: MessageType, payload: Vec<u8>) -> Self {
        Self {
            message_type,
            payload,
        }
    }

    /// Encode Celeborn's native `TransportMessage` layout: message type followed by its payload.
    pub(crate) fn encode(self) -> CelebornResult<Vec<u8>> {
        let payload_length = i32::try_from(self.payload.len())
            .map_err(|_| CelebornError::Protocol("transport payload is too large".to_string()))?;
        let mut message = Vec::with_capacity(self.payload.len() + 8);
        message.extend_from_slice(&(self.message_type as i32).to_be_bytes());
        message.extend_from_slice(&payload_length.to_be_bytes());
        message.extend_from_slice(&self.payload);
        Ok(message)
    }

    pub(crate) fn decode(bytes: &[u8]) -> CelebornResult<Self> {
        const HEADER_LENGTH: usize = 8;

        if bytes.len() < HEADER_LENGTH {
            return Err(CelebornError::Protocol(
                "truncated transport message".to_string(),
            ));
        }
        let message_type =
            i32::from_be_bytes(bytes[..4].try_into().map_err(|_| {
                CelebornError::Protocol("invalid transport message type".to_string())
            })?);
        let message_type = MessageType::try_from(message_type).map_err(|error| {
            CelebornError::Protocol(format!("invalid transport message type: {error}"))
        })?;
        let payload_length = i32::from_be_bytes(bytes[4..8].try_into().map_err(|_| {
            CelebornError::Protocol("invalid transport message payload length".to_string())
        })?);
        let payload_length = usize::try_from(payload_length).map_err(|_| {
            CelebornError::Protocol("invalid transport message payload length".to_string())
        })?;
        if bytes.len() != payload_length + HEADER_LENGTH {
            return Err(CelebornError::Protocol(
                "invalid transport message payload length".to_string(),
            ));
        }
        Ok(Self::new(message_type, bytes[HEADER_LENGTH..].to_vec()))
    }

    /// Decode Java serialization's stable `TransportMessage` layout used by Celeborn master replies.
    /// The V1 wrapper contains the message type followed by a serialized `byte[]` payload.
    pub(crate) fn decode_java(bytes: &[u8]) -> CelebornResult<Self> {
        let mut reader = JavaReader::new(bytes);
        if reader.read_exact(4)? != [0xac, 0xed, 0x00, 0x05] || reader.read_u8()? != 0x73 {
            return Err(CelebornError::Protocol(
                "expected Java-serialized Celeborn transport response".to_string(),
            ));
        }
        reader.read_class_description()?;
        let message_type = MessageType::try_from(reader.read_i32()?).map_err(|error| {
            CelebornError::Protocol(format!("invalid transport message type: {error}"))
        })?;
        let payload = reader.read_byte_array()?;
        Ok(Self::new(message_type, payload))
    }

    pub(crate) fn into_rpc_envelope<'a>(
        self,
        sender_host: &'a str,
        receiver_host: &'a str,
        receiver_port: u16,
        endpoint: &'a str,
    ) -> RpcEnvelope<'a> {
        RpcEnvelope {
            sender_host,
            receiver_host,
            receiver_port,
            endpoint,
            message: self,
        }
    }
}

/// A Netty RPC request addressed to a Celeborn endpoint.
pub(crate) struct RpcEnvelope<'a> {
    sender_host: &'a str,
    receiver_host: &'a str,
    receiver_port: u16,
    endpoint: &'a str,
    message: TransportMessage,
}

impl RpcEnvelope<'_> {
    const NATIVE_TRANSPORT_MARKER: u8 = 0xff;

    pub(crate) fn encode(self) -> CelebornResult<Vec<u8>> {
        let sender_host = self.sender_host.as_bytes();
        let receiver_host = self.receiver_host.as_bytes();
        let endpoint = self.endpoint.as_bytes();
        let sender_host_length = u16::try_from(sender_host.len())
            .map_err(|_| CelebornError::Protocol("sender host is too long".to_string()))?;
        let receiver_host_length = u16::try_from(receiver_host.len())
            .map_err(|_| CelebornError::Protocol("receiver host is too long".to_string()))?;
        let endpoint_length = u16::try_from(endpoint.len())
            .map_err(|_| CelebornError::Protocol("endpoint name is too long".to_string()))?;
        let payload_length = i32::try_from(self.message.payload.len())
            .map_err(|_| CelebornError::Protocol("request payload is too large".to_string()))?;
        let mut bytes = Vec::with_capacity(
            sender_host.len()
                + receiver_host.len()
                + endpoint.len()
                + self.message.payload.len()
                + 24,
        );
        bytes.push(1);
        bytes.extend_from_slice(&sender_host_length.to_be_bytes());
        bytes.extend_from_slice(sender_host);
        bytes.extend_from_slice(&0_i32.to_be_bytes());
        bytes.push(1);
        bytes.extend_from_slice(&receiver_host_length.to_be_bytes());
        bytes.extend_from_slice(receiver_host);
        bytes.extend_from_slice(&i32::from(self.receiver_port).to_be_bytes());
        bytes.extend_from_slice(&endpoint_length.to_be_bytes());
        bytes.extend_from_slice(endpoint);
        bytes.push(Self::NATIVE_TRANSPORT_MARKER);
        bytes.extend_from_slice(&(self.message.message_type as i32).to_be_bytes());
        bytes.extend_from_slice(&payload_length.to_be_bytes());
        bytes.extend_from_slice(&self.message.payload);
        Ok(bytes)
    }
}

struct JavaReader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> JavaReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn read_exact(&mut self, length: usize) -> CelebornResult<&'a [u8]> {
        let end = self.position.checked_add(length).ok_or_else(|| {
            CelebornError::Protocol("invalid Java serialization length".to_string())
        })?;
        let bytes = self.bytes.get(self.position..end).ok_or_else(|| {
            CelebornError::Protocol("truncated Java-serialized response".to_string())
        })?;
        self.position = end;
        Ok(bytes)
    }

    fn read_u8(&mut self) -> CelebornResult<u8> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u16(&mut self) -> CelebornResult<u16> {
        let bytes = self.read_exact(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn read_i32(&mut self) -> CelebornResult<i32> {
        let bytes = self.read_exact(4)?;
        Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_class_description(&mut self) -> CelebornResult<()> {
        if self.read_u8()? != 0x72 {
            return Err(CelebornError::Protocol(
                "expected Java class description".to_string(),
            ));
        }
        let class_name_length = usize::from(self.read_u16()?);
        let class_name = self.read_exact(class_name_length)?;
        if class_name != b"org.apache.celeborn.common.network.protocol.TransportMessage" {
            return Err(CelebornError::Protocol(
                "unexpected Java response class".to_string(),
            ));
        }
        self.read_exact(8)?; // serialVersionUID
        self.read_exact(1)?; // class flags
        let field_count = usize::from(self.read_u16()?);
        for _ in 0..field_count {
            let field_type = self.read_u8()?;
            let field_name_length = usize::from(self.read_u16()?);
            self.read_exact(field_name_length)?;
            if field_type == b'L' || field_type == b'[' {
                self.read_string()?;
            }
        }
        if self.read_u8()? != 0x78 || self.read_u8()? != 0x70 {
            return Err(CelebornError::Protocol(
                "unsupported Java response class hierarchy".to_string(),
            ));
        }
        Ok(())
    }

    fn read_string(&mut self) -> CelebornResult<()> {
        match self.read_u8()? {
            0x74 => {
                let length = usize::from(self.read_u16()?);
                self.read_exact(length)?;
                Ok(())
            }
            0x71 => {
                self.read_exact(4)?;
                Ok(())
            }
            _ => Err(CelebornError::Protocol(
                "unsupported Java class field descriptor".to_string(),
            )),
        }
    }

    fn read_byte_array(&mut self) -> CelebornResult<Vec<u8>> {
        let token = self.read_u8()?;
        if token == 0x70 {
            return Ok(Vec::new());
        }
        if token != 0x75 {
            return Err(CelebornError::Protocol(format!(
                "expected Java byte array payload but got token {token:#x}"
            )));
        }
        self.read_array_description()?;
        let length = usize::try_from(self.read_i32()?)
            .map_err(|_| CelebornError::Protocol("invalid Java byte array length".to_string()))?;
        Ok(self.read_exact(length)?.to_vec())
    }

    fn read_array_description(&mut self) -> CelebornResult<()> {
        if self.read_u8()? != 0x72 {
            return Err(CelebornError::Protocol(
                "expected Java byte array class description".to_string(),
            ));
        }
        let name_length = usize::from(self.read_u16()?);
        if self.read_exact(name_length)? != b"[B" {
            return Err(CelebornError::Protocol(
                "expected Java byte array class".to_string(),
            ));
        }
        self.read_exact(8)?;
        self.read_exact(1)?;
        if self.read_u16()? != 0 || self.read_u8()? != 0x78 || self.read_u8()? != 0x70 {
            return Err(CelebornError::Protocol(
                "unsupported Java byte array hierarchy".to_string(),
            ));
        }
        Ok(())
    }
}
