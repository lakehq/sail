use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio_util::task::AbortOnDropHandle;

use super::*;

/// A byte-level HTTP/2 peer: /invalid forces a library-initiated client reset and
/// /disconnect closes the connection. No Arrow data or query execution is involved.
async fn server() -> Result<
    (
        ClientOptions,
        Arc<AtomicUsize>,
        AbortOnDropHandle<Result<(), TransportError>>,
    ),
    TransportError,
> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address: SocketAddr = listener.local_addr()?;
    let connections = Arc::new(AtomicUsize::new(0));
    let count = connections.clone();
    let server = tokio::spawn(async move {
        let mut tasks = JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (socket, _) = accepted?;
                    let connection_id = count.fetch_add(1, Ordering::SeqCst);
                    tasks.spawn(async move {
                        let mut connection = h2::server::handshake(socket).await?;
                        while let Some(request) = connection.accept().await {
                            let (request, mut respond) = request?;
                            if request.uri().path() == "/disconnect" {
                                connection.abrupt_shutdown(h2::Reason::NO_ERROR);
                                continue;
                            }
                            // END_STREAM with a nonzero content-length is malformed. Receiving
                            // it increments the client's internal reset counter, unlike CANCEL.
                            let length = if request.uri().path() == "/invalid" { "1" } else { "0" };
                            let response = Response::builder()
                                .header("content-length", length)
                                .header("connection-id", connection_id)
                                .body(())?;
                            respond.send_response(response, true)?;
                        }
                        Ok::<_, TransportError>(())
                    });
                }
                Some(result) = tasks.join_next() => { result??; }
            }
        }
    });
    Ok((
        ClientOptions {
            enable_tls: false,
            host: address.ip().to_string(),
            port: address.port(),
            flight_connection_count: NonZeroUsize::MIN,
            flight_initial_stream_window_size: None,
            flight_initial_connection_window_size: None,
        },
        connections,
        AbortOnDropHandle::new(server),
    ))
}

fn request(transport: &FlightTransport, path: &str) -> Result<Request<Body>, TransportError> {
    let mut parts = transport.inner.origin.clone().into_parts();
    parts.path_and_query = Some(path.parse()?);
    Ok(Request::builder()
        .uri(Uri::from_parts(parts)?)
        .body(Body::empty())?)
}

#[tokio::test]
async fn internal_resets_do_not_close_shared_connection() -> Result<(), TransportError> {
    tokio::time::timeout(Duration::from_secs(15), async {
        let (options, connections, _server) = server().await?;
        let transport = FlightTransport::connect(&options).await?;
        for _ in 0..1100 {
            let response = transport
                .clone()
                .oneshot(request(&transport, "/invalid")?)
                .await;
            assert!(
                response.is_err(),
                "malformed response must fail its request"
            );
        }
        let response = transport
            .clone()
            .oneshot(request(&transport, "/ok")?)
            .await?;
        assert_eq!(response.status(), 200);
        assert_eq!(connections.load(Ordering::SeqCst), 1);
        Ok::<_, TransportError>(())
    })
    .await?
}

#[tokio::test]
async fn clones_share_reconnection_after_disconnect() -> Result<(), TransportError> {
    tokio::time::timeout(Duration::from_secs(15), async {
        let (options, connections, _server) = server().await?;
        let transport = FlightTransport::connect(&options).await?;
        let mut sender = transport.sender(0).await?;
        let _ = transport
            .clone()
            .oneshot(request(&transport, "/disconnect")?)
            .await;
        // Wait for Hyper to observe shutdown before issuing new requests. Dispatch errors
        // during a concurrent shutdown are intentionally not retried by the transport.
        while !sender.is_closed() {
            tokio::task::yield_now().await;
        }
        assert!(sender.ready().await.is_err());
        let requests =
            (0..32).map(|_| async { transport.clone().oneshot(request(&transport, "/ok")?).await });
        for response in futures::future::try_join_all(requests).await? {
            assert_eq!(response.status(), 200);
        }
        assert_eq!(connections.load(Ordering::SeqCst), 2);
        Ok::<_, TransportError>(())
    })
    .await?
}

#[tokio::test]
async fn clones_share_connection_pool_and_reconnect_one_slot() -> Result<(), TransportError> {
    tokio::time::timeout(Duration::from_secs(15), async {
        let (mut options, connections, _server) = server().await?;
        options.flight_connection_count = NonZeroUsize::new(3).ok_or("invalid connection count")?;
        let transport = FlightTransport::connect(&options).await?;
        // The remaining slots are connected on demand, and all clones share the cursor.
        for index in 0..9 {
            let response = transport
                .clone()
                .oneshot(request(&transport, "/ok")?)
                .await?;
            assert_eq!(response.headers()["connection-id"], (index % 3).to_string());
            if index == 0 {
                assert_eq!(connections.load(Ordering::SeqCst), 1);
            }
        }
        assert_eq!(connections.load(Ordering::SeqCst), 3);

        let sender = transport.sender(0).await?;
        let _ = transport
            .clone()
            .oneshot(request(&transport, "/disconnect")?)
            .await;
        while !sender.is_closed() {
            tokio::task::yield_now().await;
        }
        let requests =
            (0..30).map(|_| async { transport.clone().oneshot(request(&transport, "/ok")?).await });
        let mut counts = [0; 4];
        for response in futures::future::try_join_all(requests).await? {
            let id: usize = response.headers()["connection-id"].to_str()?.parse()?;
            counts[id] += 1;
        }
        assert_eq!(counts, [0, 10, 10, 10]);
        assert_eq!(connections.load(Ordering::SeqCst), 4);
        Ok::<_, TransportError>(())
    })
    .await?
}

#[tokio::test]
async fn custom_window_sizes_are_advertised_for_streams_and_connection()
-> Result<(), TransportError> {
    tokio::time::timeout(Duration::from_secs(15), async {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let stream_window_size: u32 = 4 * 1024 * 1024;
        let connection_window_size: u32 = 16 * 1024 * 1024;
        // Inspect the HTTP/2 preface directly so this verifies both flow-control levels.
        let server = AbortOnDropHandle::new(tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let mut preface = [0; 24];
            socket.read_exact(&mut preface).await?;
            assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
            // Send an empty server SETTINGS frame.
            socket.write_all(&[0, 0, 0, 4, 0, 0, 0, 0, 0]).await?;
            let mut stream_window = None;
            let mut connection_window = 65535;
            while stream_window.is_none() || connection_window == 65535 {
                let mut header = [0; 9];
                socket.read_exact(&mut header).await?;
                let length = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
                let mut payload = vec![0; length];
                socket.read_exact(&mut payload).await?;
                match header[3] {
                    4 if header[4] == 0 => {
                        for setting in payload.chunks_exact(6) {
                            if setting[..2] == [0, 4] {
                                stream_window = Some(u32::from_be_bytes(setting[2..].try_into()?));
                            }
                        }
                        socket.write_all(&[0, 0, 0, 4, 1, 0, 0, 0, 0]).await?;
                    }
                    8 if header[5..] == [0, 0, 0, 0] => {
                        connection_window +=
                            u32::from_be_bytes(payload.as_slice().try_into()?) & 0x7fff_ffff;
                    }
                    _ => {}
                }
            }
            assert_eq!(stream_window, Some(stream_window_size));
            assert_eq!(connection_window, connection_window_size);
            Ok::<_, TransportError>(())
        }));
        let options = ClientOptions {
            enable_tls: false,
            host: address.ip().to_string(),
            port: address.port(),
            flight_connection_count: NonZeroUsize::MIN,
            flight_initial_stream_window_size: Some(stream_window_size),
            flight_initial_connection_window_size: Some(connection_window_size),
        };
        let _transport = FlightTransport::connect(&options).await?;
        server.await??;
        Ok::<_, TransportError>(())
    })
    .await?
}

#[tokio::test]
async fn connect_rejects_invalid_window_sizes_before_opening_connection()
-> Result<(), TransportError> {
    for (name, stream_window, connection_window) in [
        ("stream", Some(1 << 31), None),
        ("connection", None, Some(1 << 31)),
    ] {
        let options = ClientOptions {
            enable_tls: false,
            host: "invalid host".to_string(),
            port: 0,
            flight_connection_count: NonZeroUsize::MIN,
            flight_initial_stream_window_size: stream_window,
            flight_initial_connection_window_size: connection_window,
        };
        let error = FlightTransport::connect(&options)
            .await
            .err()
            .ok_or("invalid window size must fail")?;
        let error = error.downcast::<std::io::Error>()?;
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(
            error.to_string(),
            format!("Flight initial {name} window size must not exceed 2147483647 bytes")
        );
    }
    Ok(())
}
