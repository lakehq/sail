use std::sync::Arc;

use arrow_flight::flight_service_client::FlightServiceClient;
use tokio::sync::{oneshot, Mutex, MutexGuard, OnceCell};
use tonic::transport::Channel;

use crate::driver::DriverServiceClient;
use crate::error::ExecutionResult;
use crate::worker::WorkerServiceClient;

pub struct ServerMonitor {
    /// The shutdown signal to send to the server,
    /// or `None` if the server is not running.
    signal: Option<oneshot::Sender<()>>,
}

impl ServerMonitor {
    pub fn idle() -> Self {
        Self { signal: None }
    }

    pub fn running(signal: oneshot::Sender<()>) -> Self {
        Self {
            signal: Some(signal),
        }
    }

    pub fn is_running(&self) -> bool {
        self.signal.is_some()
    }

    pub fn stop(self) {
        if let Some(signal) = self.signal {
            let _ = signal.send(());
        }
    }
}

impl Default for ServerMonitor {
    fn default() -> Self {
        Self::idle()
    }
}

#[derive(Clone)]
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

#[derive(Clone)]
pub struct ClientHandle<T> {
    options: Arc<ClientOptions>,
    inner: OnceCell<Arc<Mutex<T>>>,
}

impl<T: ClientBuilder> ClientHandle<T> {
    pub fn new(options: ClientOptions) -> Self {
        Self {
            options: Arc::new(options),
            inner: OnceCell::new(),
        }
    }

    async fn init(options: Arc<ClientOptions>) -> ExecutionResult<Arc<Mutex<T>>> {
        let client = T::connect(&options).await?;
        Ok(Arc::new(Mutex::new(client)))
    }

    async fn get(&self) -> ExecutionResult<&Mutex<T>> {
        let options = Arc::clone(&self.options);
        Ok(self
            .inner
            .get_or_try_init(|| Self::init(options))
            .await?
            .as_ref())
    }

    pub async fn lock(&self) -> ExecutionResult<MutexGuard<T>> {
        Ok(self.get().await?.lock().await)
    }
}
