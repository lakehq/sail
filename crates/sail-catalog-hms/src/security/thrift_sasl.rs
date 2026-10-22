use std::io;

use sail_catalog::error::{CatalogError, CatalogResult};

/// Maximum allowed SASL frame size (16 MB).
/// Matches the negotiated max buffer from the SASL security layer exchange
/// (`0x00FF_FFFF` cap in `select_security_layer`).
const MAX_SASL_FRAME_SIZE: usize = 0x00FF_FFFF;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::tcp;

use super::gssapi::{GssapiFrameProtector, GssapiSession, SaslQop};

fn kerb_trace_enabled() -> bool {
    std::env::var_os("SAIL_HMS_KRB_TRACE").is_some()
}

pub(crate) trait SaslClientSession {
    fn mechanism(&self) -> &'static str;

    fn step(&mut self, token: Option<&[u8]>) -> CatalogResult<(Vec<u8>, bool)>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NegotiationStatus {
    Start = 0x01,
    Ok = 0x02,
    Bad = 0x03,
    Error = 0x04,
    Complete = 0x05,
}

impl NegotiationStatus {
    fn from_byte(value: u8) -> CatalogResult<Self> {
        match value {
            0x01 => Ok(Self::Start),
            0x02 => Ok(Self::Ok),
            0x03 => Ok(Self::Bad),
            0x04 => Ok(Self::Error),
            0x05 => Ok(Self::Complete),
            other => Err(CatalogError::External(format!(
                "Received invalid Thrift SASL negotiation status byte {other}"
            ))),
        }
    }
}

/// Upgrade an already connected Tokio stream, preserving the negotiated SASL layer.
pub(crate) async fn kerberos_transport(
    mut stream: tokio::net::TcpStream,
    target: &str,
    qop_min: SaslQop,
) -> io::Result<(SaslReadHalf, SaslWriteHalf)> {
    let mut session =
        GssapiSession::new(target, qop_min).map_err(|error| io::Error::other(error.to_string()))?;
    perform_thrift_sasl_handshake(&mut stream, &mut session)
        .await
        .map_err(|error| io::Error::other(error.to_string()))?;
    let protector = session
        .take_frame_protector()
        .map_err(|error| io::Error::other(error.to_string()))?;
    let (reader, writer) = stream.into_split();
    Ok((
        SaslReadHalf::new(reader, protector.clone()),
        SaslWriteHalf::new(writer, protector),
    ))
}

pub(crate) struct SaslReadHalf<R = tcp::OwnedReadHalf> {
    inner: R,
    frame_protector: Option<GssapiFrameProtector>,
    state: ReadState,
}

enum ReadState {
    Header { buf: [u8; 4], read: usize },
    Frame { buf: Vec<u8>, read: usize },
    Ready { buf: Vec<u8>, offset: usize },
}

impl<R> SaslReadHalf<R> {
    fn new(inner: R, frame_protector: Option<GssapiFrameProtector>) -> Self {
        Self {
            inner,
            frame_protector,
            state: ReadState::Header {
                buf: [0; 4],
                read: 0,
            },
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for SaslReadHalf<R> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return std::task::Poll::Ready(Ok(()));
        }
        loop {
            let state = std::mem::replace(
                &mut self.state,
                ReadState::Header {
                    buf: [0; 4],
                    read: 0,
                },
            );
            match state {
                ReadState::Ready {
                    buf: frame,
                    mut offset,
                } => {
                    if offset >= frame.len() {
                        continue;
                    }
                    let remaining = &frame[offset..];
                    let to_copy = remaining.len().min(buf.remaining());
                    buf.put_slice(&remaining[..to_copy]);
                    offset += to_copy;
                    self.state = ReadState::Ready { buf: frame, offset };
                    return std::task::Poll::Ready(Ok(()));
                }
                ReadState::Header { mut buf, mut read } => {
                    let mut read_buf = tokio::io::ReadBuf::new(&mut buf[read..]);
                    match std::pin::Pin::new(&mut self.inner).poll_read(cx, &mut read_buf) {
                        std::task::Poll::Ready(Ok(())) => {
                            let filled = read_buf.filled().len();
                            if filled == 0 {
                                return if read == 0 {
                                    std::task::Poll::Ready(Ok(()))
                                } else {
                                    std::task::Poll::Ready(Err(io::Error::new(
                                        io::ErrorKind::UnexpectedEof,
                                        "truncated SASL frame header",
                                    )))
                                };
                            }
                            read += filled;
                            if read == buf.len() {
                                let frame_len = u32::from_be_bytes(buf) as usize;
                                if kerb_trace_enabled() {
                                    eprintln!(
                                        "kerberos thrift sasl data read frame_len={frame_len}"
                                    );
                                }
                                if frame_len > MAX_SASL_FRAME_SIZE {
                                    return std::task::Poll::Ready(Err(io::Error::new(
                                        io::ErrorKind::InvalidData,
                                        format!(
                                            "SASL frame too large: {frame_len} bytes (max {MAX_SASL_FRAME_SIZE})"
                                        ),
                                    )));
                                }
                                self.state = ReadState::Frame {
                                    buf: vec![0; frame_len],
                                    read: 0,
                                };
                            } else {
                                self.state = ReadState::Header { buf, read };
                            }
                        }
                        std::task::Poll::Ready(Err(error)) => {
                            return std::task::Poll::Ready(Err(error));
                        }
                        std::task::Poll::Pending => {
                            self.state = ReadState::Header { buf, read };
                            return std::task::Poll::Pending;
                        }
                    }
                }
                ReadState::Frame { mut buf, mut read } => {
                    if buf.is_empty() {
                        continue;
                    }
                    let mut read_buf = tokio::io::ReadBuf::new(&mut buf[read..]);
                    match std::pin::Pin::new(&mut self.inner).poll_read(cx, &mut read_buf) {
                        std::task::Poll::Ready(Ok(())) => {
                            let filled = read_buf.filled().len();
                            if filled == 0 {
                                return std::task::Poll::Ready(Err(io::Error::new(
                                    io::ErrorKind::UnexpectedEof,
                                    "truncated SASL frame payload",
                                )));
                            }
                            read += filled;
                            if read == buf.len() {
                                let payload = if let Some(frame_protector) = &self.frame_protector {
                                    frame_protector
                                        .unwrap(&buf)
                                        .map_err(|error| io::Error::other(error.to_string()))?
                                } else {
                                    buf
                                };
                                self.state = ReadState::Ready {
                                    buf: payload,
                                    offset: 0,
                                };
                            } else {
                                self.state = ReadState::Frame { buf, read };
                            }
                        }
                        std::task::Poll::Ready(Err(error)) => {
                            return std::task::Poll::Ready(Err(error));
                        }
                        std::task::Poll::Pending => {
                            self.state = ReadState::Frame { buf, read };
                            return std::task::Poll::Pending;
                        }
                    }
                }
            }
        }
    }
}

pub(crate) struct SaslWriteHalf<W = tcp::OwnedWriteHalf> {
    inner: W,
    frame_protector: Option<GssapiFrameProtector>,
    buffered: Vec<u8>,
    frame: Option<(Vec<u8>, usize)>,
    flushing_inner: bool,
}

impl<W> SaslWriteHalf<W> {
    fn new(inner: W, frame_protector: Option<GssapiFrameProtector>) -> Self {
        Self {
            inner,
            frame_protector,
            buffered: Vec::new(),
            frame: None,
            flushing_inner: false,
        }
    }

    fn prepare_frame(&mut self) -> io::Result<()> {
        if self.frame.is_some() || self.buffered.is_empty() {
            return Ok(());
        }

        let buffered = std::mem::take(&mut self.buffered);
        let payload = if let Some(frame_protector) = &self.frame_protector {
            frame_protector
                .wrap(&buffered)
                .map_err(|error| io::Error::other(error.to_string()))?
        } else {
            buffered
        };
        if payload.len() > MAX_SASL_FRAME_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "SASL frame exceeds size limit",
            ));
        }
        let payload_len = payload.len() as u32;
        if kerb_trace_enabled() {
            eprintln!("kerberos thrift sasl data write frame_len={payload_len}");
        }
        let mut frame = Vec::with_capacity(4 + payload.len());
        frame.extend_from_slice(&payload_len.to_be_bytes());
        frame.extend_from_slice(&payload);
        self.frame = Some((frame, 0));
        Ok(())
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for SaslWriteHalf<W> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, io::Error>> {
        self.buffered.extend_from_slice(buf);
        std::task::Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
        loop {
            if let Err(error) = self.prepare_frame() {
                return std::task::Poll::Ready(Err(error));
            }
            if let Some((frame, mut written)) = self.frame.take() {
                while written < frame.len() {
                    match std::pin::Pin::new(&mut self.inner).poll_write(cx, &frame[written..]) {
                        std::task::Poll::Ready(Ok(0)) => {
                            return std::task::Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::WriteZero,
                                "failed to write SASL-framed HMS payload",
                            )));
                        }
                        std::task::Poll::Ready(Ok(n)) => written += n,
                        std::task::Poll::Ready(Err(error)) => {
                            return std::task::Poll::Ready(Err(error));
                        }
                        std::task::Poll::Pending => {
                            self.frame = Some((frame, written));
                            return std::task::Poll::Pending;
                        }
                    }
                }
                self.flushing_inner = true;
            }

            if self.flushing_inner {
                match std::pin::Pin::new(&mut self.inner).poll_flush(cx) {
                    std::task::Poll::Ready(Ok(())) => {
                        self.flushing_inner = false;
                    }
                    other => return other,
                }
            }

            if self.frame.is_none() && self.buffered.is_empty() && !self.flushing_inner {
                return std::task::Poll::Ready(Ok(()));
            }
        }
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
        match self.as_mut().poll_flush(cx) {
            std::task::Poll::Ready(Ok(())) => std::pin::Pin::new(&mut self.inner).poll_shutdown(cx),
            std::task::Poll::Ready(Err(error)) => std::task::Poll::Ready(Err(error)),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

pub(crate) async fn perform_thrift_sasl_handshake<S, Session>(
    stream: &mut S,
    session: &mut Session,
) -> CatalogResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
    Session: SaslClientSession,
{
    let (initial_response, mut complete) = session.step(None)?;
    write_message(
        stream,
        NegotiationStatus::Start,
        session.mechanism().as_bytes(),
    )
    .await?;
    write_message(
        stream,
        if complete {
            NegotiationStatus::Complete
        } else {
            NegotiationStatus::Ok
        },
        &initial_response,
    )
    .await?;

    let mut last_server_status = NegotiationStatus::Ok;

    while !complete {
        let (status, payload) = read_message(stream).await?;
        match status {
            NegotiationStatus::Ok | NegotiationStatus::Complete => {}
            other => {
                return Err(CatalogError::External(format!(
                    "Expected Thrift SASL OK or COMPLETE from HMS, got {other:?}"
                )));
            }
        }

        last_server_status = status;
        let (response, step_complete) = session.step(Some(&payload))?;
        complete = step_complete;

        if status == NegotiationStatus::Complete {
            if !complete {
                return Err(CatalogError::External(
                    "HMS marked Thrift SASL negotiation complete before the client finished"
                        .to_string(),
                ));
            }
            break;
        }

        write_message(
            stream,
            if complete {
                NegotiationStatus::Complete
            } else {
                NegotiationStatus::Ok
            },
            &response,
        )
        .await?;
    }

    if last_server_status != NegotiationStatus::Complete {
        let (status, _) = read_message(stream).await?;
        if status != NegotiationStatus::Complete {
            return Err(CatalogError::External(format!(
                "Expected final Thrift SASL COMPLETE from HMS, got {status:?}"
            )));
        }
    }

    Ok(())
}

async fn write_message<S>(
    stream: &mut S,
    status: NegotiationStatus,
    payload: &[u8],
) -> CatalogResult<()>
where
    S: AsyncWrite + Unpin,
{
    if kerb_trace_enabled() {
        eprintln!(
            "kerberos thrift sasl write: status={status:?} payload_len={}",
            payload.len()
        );
    }
    stream.write_u8(status as u8).await.map_err(|error| {
        CatalogError::External(format!("Failed to write Thrift SASL status: {error}"))
    })?;
    stream
        .write_u32(payload.len() as u32)
        .await
        .map_err(|error| {
            CatalogError::External(format!(
                "Failed to write Thrift SASL payload length: {error}"
            ))
        })?;
    stream.write_all(payload).await.map_err(|error| {
        CatalogError::External(format!("Failed to write Thrift SASL payload: {error}"))
    })?;
    stream.flush().await.map_err(|error| {
        CatalogError::External(format!("Failed to flush Thrift SASL payload: {error}"))
    })?;
    Ok(())
}

async fn read_message<S>(stream: &mut S) -> CatalogResult<(NegotiationStatus, Vec<u8>)>
where
    S: AsyncRead + Unpin,
{
    let status = stream.read_u8().await.map_err(|error| {
        CatalogError::External(format!("Failed to read Thrift SASL status: {error}"))
    })?;
    let payload_len = stream.read_u32().await.map_err(|error| {
        CatalogError::External(format!(
            "Failed to read Thrift SASL payload length: {error}"
        ))
    })?;
    if payload_len as usize > MAX_SASL_FRAME_SIZE {
        return Err(CatalogError::External(
            "SASL negotiation frame exceeds size limit".into(),
        ));
    }
    let mut payload = vec![0; payload_len as usize];
    stream.read_exact(&mut payload).await.map_err(|error| {
        CatalogError::External(format!("Failed to read Thrift SASL payload: {error}"))
    })?;

    let status = NegotiationStatus::from_byte(status)?;
    if kerb_trace_enabled() {
        eprintln!(
            "kerberos thrift sasl read: status={status:?} payload_len={}",
            payload.len()
        );
    }
    match status {
        NegotiationStatus::Bad | NegotiationStatus::Error => Err(CatalogError::External(format!(
            "Hive Metastore rejected Kerberos SASL negotiation: {}",
            String::from_utf8_lossy(&payload)
        ))),
        other => Ok((other, payload)),
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used)]

    use std::sync::{Arc, Mutex};

    use tokio::io::{AsyncRead, AsyncWrite, duplex};
    use tokio::time::{Duration, timeout};

    use super::{
        AsyncReadExt, AsyncWriteExt, CatalogError, CatalogResult, NegotiationStatus,
        SaslClientSession, perform_thrift_sasl_handshake,
    };

    #[derive(Debug, Clone)]
    struct FakeSession {
        mechanism: &'static str,
        steps: Vec<(Option<Vec<u8>>, Vec<u8>, bool)>,
        calls: Arc<Mutex<Vec<Option<Vec<u8>>>>>,
    }

    impl FakeSession {
        fn new(mechanism: &'static str, steps: Vec<(Option<Vec<u8>>, Vec<u8>, bool)>) -> Self {
            Self {
                mechanism,
                steps,
                calls: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl SaslClientSession for FakeSession {
        fn mechanism(&self) -> &'static str {
            self.mechanism
        }

        fn step(&mut self, token: Option<&[u8]>) -> CatalogResult<(Vec<u8>, bool)> {
            self.calls
                .lock()
                .unwrap()
                .push(token.map(|value| value.to_vec()));
            let (expected, response, complete) = self.steps.remove(0);
            assert_eq!(expected, token.map(|value| value.to_vec()));
            Ok((response, complete))
        }
    }

    async fn read_message<S>(stream: &mut S) -> (NegotiationStatus, Vec<u8>)
    where
        S: AsyncRead + Unpin,
    {
        let status = stream.read_u8().await.unwrap();
        let payload_len = stream.read_u32().await.unwrap() as usize;
        let mut payload = vec![0; payload_len];
        stream.read_exact(&mut payload).await.unwrap();
        (NegotiationStatus::from_byte(status).unwrap(), payload)
    }

    async fn write_message<S>(stream: &mut S, status: NegotiationStatus, payload: &[u8])
    where
        S: AsyncWrite + Unpin,
    {
        stream.write_u8(status as u8).await.unwrap();
        stream.write_u32(payload.len() as u32).await.unwrap();
        stream.write_all(payload).await.unwrap();
        stream.flush().await.unwrap();
    }

    #[tokio::test]
    async fn test_handshake_sends_start_and_completes_multi_step_exchange() {
        let (mut client, mut server) = duplex(1024);
        let mut session = FakeSession::new(
            "FAKE",
            vec![
                (None, b"init".to_vec(), false),
                (Some(b"challenge-1".to_vec()), b"response-1".to_vec(), false),
                (
                    Some(b"layer-token".to_vec()),
                    b"wrapped-auth-only".to_vec(),
                    true,
                ),
            ],
        );

        let server_task = tokio::spawn(async move {
            timeout(Duration::from_secs(1), async {
                assert_eq!(
                    read_message(&mut server).await,
                    (NegotiationStatus::Start, b"FAKE".to_vec())
                );
                assert_eq!(
                    read_message(&mut server).await,
                    (NegotiationStatus::Ok, b"init".to_vec())
                );
                write_message(&mut server, NegotiationStatus::Ok, b"challenge-1").await;
                assert_eq!(
                    read_message(&mut server).await,
                    (NegotiationStatus::Ok, b"response-1".to_vec())
                );
                write_message(&mut server, NegotiationStatus::Ok, b"layer-token").await;
                assert_eq!(
                    read_message(&mut server).await,
                    (NegotiationStatus::Complete, b"wrapped-auth-only".to_vec())
                );
                write_message(&mut server, NegotiationStatus::Complete, b"").await;
            })
            .await
            .unwrap();
        });

        let result = perform_thrift_sasl_handshake(&mut client, &mut session).await;

        server_task.await.unwrap();
        assert!(result.is_ok(), "{result:?}");
    }

    #[tokio::test]
    async fn test_handshake_does_not_reply_after_server_complete() {
        let (mut client, mut server) = duplex(1024);
        let mut session = FakeSession::new(
            "FAKE",
            vec![
                (None, b"init".to_vec(), false),
                (
                    Some(b"server-final".to_vec()),
                    b"would-have-been-sent".to_vec(),
                    true,
                ),
            ],
        );

        let server_task = tokio::spawn(async move {
            timeout(Duration::from_secs(1), async {
                assert_eq!(
                    read_message(&mut server).await,
                    (NegotiationStatus::Start, b"FAKE".to_vec())
                );
                assert_eq!(
                    read_message(&mut server).await,
                    (NegotiationStatus::Ok, b"init".to_vec())
                );
                write_message(&mut server, NegotiationStatus::Complete, b"server-final").await;
                let mut extra = [0_u8; 1];
                let read = server.read(&mut extra).await.unwrap();
                assert_eq!(read, 0);
            })
            .await
            .unwrap();
        });

        let result = perform_thrift_sasl_handshake(&mut client, &mut session).await;
        drop(client);

        server_task.await.unwrap();
        assert!(result.is_ok(), "{result:?}");
    }

    #[tokio::test]
    async fn test_handshake_surfaces_server_error_status() {
        let (mut client, mut server) = duplex(1024);
        let mut session = FakeSession::new("FAKE", vec![(None, b"init".to_vec(), false)]);

        let server_task = tokio::spawn(async move {
            timeout(Duration::from_secs(1), async {
                assert_eq!(
                    read_message(&mut server).await,
                    (NegotiationStatus::Start, b"FAKE".to_vec())
                );
                assert_eq!(
                    read_message(&mut server).await,
                    (NegotiationStatus::Ok, b"init".to_vec())
                );
                write_message(&mut server, NegotiationStatus::Error, b"boom").await;
            })
            .await
            .unwrap();
        });

        let error = perform_thrift_sasl_handshake(&mut client, &mut session)
            .await
            .unwrap_err();

        server_task.await.unwrap();
        assert!(matches!(error, CatalogError::External(_)));
        assert!(error.to_string().contains("boom"));
    }
    #[tokio::test]
    async fn test_sasl_data_frames_handle_fragmentation_and_truncation() {
        let (client, mut peer) = duplex(32);
        let mut reader = super::SaslReadHalf::new(client, None);
        let server = tokio::spawn(async move {
            for byte in [0, 0, 0, 3, b'a', b'b', b'c', 0, 0, 0, 2, b'd', b'e'] {
                peer.write_all(&[byte]).await.unwrap();
                tokio::task::yield_now().await;
            }
        });
        let mut payload = [0; 5];
        reader.read_exact(&mut payload).await.unwrap();
        assert_eq!(&payload, b"abcde");
        server.await.unwrap();
        for wire in [vec![0, 0], vec![0, 0, 0, 3, b'a']] {
            let mut reader = super::SaslReadHalf::new(wire.as_slice(), None);
            let error = reader.read_to_end(&mut Vec::new()).await.unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
        }
    }

    #[tokio::test]
    async fn test_sasl_writer_flushes_one_frame_across_partial_writes() {
        let (client, mut peer) = duplex(2);
        let mut writer = super::SaslWriteHalf::new(client, None);
        let server = tokio::spawn(async move {
            let mut wire = Vec::new();
            peer.read_to_end(&mut wire).await.unwrap();
            wire
        });
        writer.write_all(b"abc").await.unwrap();
        writer.write_all(b"de").await.unwrap();
        writer.flush().await.unwrap();
        writer.shutdown().await.unwrap();
        assert_eq!(server.await.unwrap(), b"\0\0\0\x05abcde");
    }
}
