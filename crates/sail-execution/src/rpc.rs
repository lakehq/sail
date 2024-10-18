use std::sync::Arc;

use tokio::sync::{oneshot, Mutex, MutexGuard, OnceCell};

use crate::error::ExecutionResult;

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
        match self.signal {
            Some(signal) => {
                let _ = signal.send(());
            }
            None => {}
        }
    }
}

impl Default for ServerMonitor {
    fn default() -> Self {
        Self::idle()
    }
}

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
        let options = self.options.clone();
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
