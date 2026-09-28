use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

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
                    count.fetch_add(1, Ordering::SeqCst);
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
        let mut sender = transport.sender().await?;
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
