use std::future::Future;
use std::sync::Arc;

use arrow_flight::flight_service_client::FlightServiceClient;
use tokio::sync::{oneshot, Mutex, MutexGuard, OnceCell};
use tokio::task::JoinHandle;
use tonic::transport::Channel;

use crate::driver::DriverServiceClient;
use crate::error::{ExecutionError, ExecutionResult};
use crate::worker::WorkerServiceClient;

pub enum ServerMonitor {
    Stopped,
    Pending {
        handle: JoinHandle<ExecutionResult<()>>,
    },
    Running {
        /// The shutdown signal to send to the server,
        /// or `None` if the server is not running.
        signal: oneshot::Sender<()>,
        /// The join handle of the server task.
        handle: JoinHandle<ExecutionResult<()>>,
        /// The server port.
        port: u16,
    },
}

impl Default for ServerMonitor {
    fn default() -> Self {
        Self::new()
    }
}

impl ServerMonitor {
    pub fn new() -> Self {
        Self::Stopped
    }

    pub async fn start(
        self,
        f: impl Future<Output = ExecutionResult<()>> + Send + 'static,
    ) -> Self {
        self.stop().await;
        Self::Pending {
            handle: tokio::spawn(f),
        }
    }

    pub fn ready(self, signal: oneshot::Sender<()>, port: u16) -> ExecutionResult<Self> {
        match self {
            Self::Pending { handle } => Ok(Self::Running {
                signal,
                handle,
                port,
            }),
            _ => Err(ExecutionError::InternalError(
                "the server must be in pending state before it can be ready".to_string(),
            )),
        }
    }

    pub async fn stop(self) {
        match self {
            Self::Stopped => {}
            Self::Pending { handle } => {
                handle.abort();
            }
            Self::Running {
                signal,
                handle,
                port: _,
            } => {
                let _ = signal.send(());
                let _ = handle.await;
            }
        }
    }

    pub fn port(&self) -> Option<u16> {
        match self {
            Self::Running { port, .. } => Some(*port),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClientOptions {
    pub enable_tls: bool,
    pub host: String,
    pub port: u16,
}

impl ClientOptions {
    pub fn to_url_string(&self) -> String {
        let scheme = if self.enable_tls { "https" } else { "http" };
        format!("{}://{}:{}", scheme, self.host, self.port)
    }
}

#[tonic::async_trait]
pub trait ClientBuilder: Sized {
    async fn connect(options: &ClientOptions) -> ExecutionResult<Self>;
}

macro_rules! impl_client_builder {
    ($client_type:ty) => {
        #[tonic::async_trait]
        impl ClientBuilder for $client_type {
            async fn connect(options: &ClientOptions) -> ExecutionResult<Self> {
                Ok(<$client_type>::connect(options.to_url_string()).await?)
            }
        }
    };
}

impl_client_builder!(DriverServiceClient<Channel>);
impl_client_builder!(WorkerServiceClient<Channel>);
impl_client_builder!(FlightServiceClient<Channel>);

/// A handle to a gRPC client to support connection reuse.
/// The handle can be cheaply cloned and the underlying connection is shared.
#[derive(Debug, Clone)]
pub struct ClientHandle<T> {
    /// The client options.
    options: Arc<ClientOptions>,
    /// The shared gRPC client which is lazily initialized.
    /// Note that this must be `Arc<OnceCell<Mutex<T>>>` instead of `OnceCell<Arc<Mutex<T>>>`.
    /// If we use the latter, when the client is not initialized, an empty `OnceCell` would be
    /// cloned and later initialized independently, resulting in multiple connections.
    /// This could then easily overwhelm the server, and the client would see the
    /// "connection refused" Tonic transport error.
    inner: Arc<OnceCell<Mutex<T>>>,
}

impl<T: ClientBuilder> ClientHandle<T> {
    pub fn new(options: ClientOptions) -> Self {
        Self {
            options: Arc::new(options),
            inner: Arc::new(OnceCell::new()),
        }
    }

    async fn init(options: Arc<ClientOptions>) -> ExecutionResult<Mutex<T>> {
        let client = T::connect(&options).await?;
        Ok(Mutex::new(client))
    }

    async fn get(&self) -> ExecutionResult<&Mutex<T>> {
        let options = Arc::clone(&self.options);
        self.inner.get_or_try_init(|| Self::init(options)).await
    }

    pub async fn lock(&self) -> ExecutionResult<MutexGuard<T>> {
        Ok(self.get().await?.lock().await)
    }
}
