// SPDX-License-Identifier: Apache-2.0

use futures::StreamExt;
use futures::stream::BoxStream;
use tokio::runtime::{Handle, Runtime};
use tokio::sync::mpsc;
use tokio_util::task::AbortOnDropHandle;

use crate::config::RuntimeConfig;
use crate::error::{CommonError, CommonResult};

#[derive(Debug)]
pub struct RuntimeManager {
    // Drop compute first so I/O remains available while query tasks shut down.
    compute: Runtime,
    primary: Runtime,
}

impl RuntimeManager {
    /// Attempts to initialize a new RuntimeManager instance.
    ///
    /// # Arguments
    ///
    /// * `config` - The runtime configuration to use.
    ///
    /// # Errors
    ///
    /// Returns a `CommonError` if the runtime manager fails to initialize.
    pub fn try_new(config: &RuntimeConfig) -> CommonResult<Self> {
        let primary = Self::build_runtime("sail-primary", config.stack_size)?;
        let compute = Self::build_runtime("sail-compute", config.stack_size)?;
        Ok(Self { primary, compute })
    }

    pub fn handle(&self) -> RuntimeHandle {
        let primary = self.primary.handle().clone();
        let compute = self.compute.handle().clone();
        RuntimeHandle { primary, compute }
    }

    fn build_runtime(name: &'static str, stack_size: usize) -> CommonResult<Runtime> {
        tokio::runtime::Builder::new_multi_thread()
            .thread_name(name)
            .thread_stack_size(stack_size)
            .enable_all()
            .build()
            .map_err(|e| CommonError::internal(e.to_string()))
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeHandle {
    primary: Handle,
    compute: Handle,
}

impl RuntimeHandle {
    pub fn new(primary: Handle, compute: Handle) -> Self {
        Self { primary, compute }
    }

    /// Runs servers, I/O and control-plane tasks.
    pub fn primary(&self) -> &Handle {
        &self.primary
    }

    /// Runs DataFusion query planning and execution.
    pub fn compute(&self) -> &Handle {
        &self.compute
    }
}

/// Start polling a stream on its owning runtime at the first read. Dropping the
/// receiver cancels the producer, including when it is waiting for I/O.
pub fn spawn_stream_on<T, A, F>(handle: Handle, args: A, open: F) -> BoxStream<'static, T>
where
    T: Send + 'static,
    A: Send + 'static,
    F: FnOnce(&A) -> BoxStream<'_, T> + Send + 'static,
{
    async_stream::stream! {
        let (tx, mut rx) = mpsc::channel(1);
        let task = AbortOnDropHandle::new(handle.spawn(async move {
            let mut stream = open(&args);
            while let Some(item) = stream.next().await {
                if tx.send(item).await.is_err() {
                    break;
                }
            }
        }));
        while let Some(item) = rx.recv().await {
            yield item;
        }
        if let Err(error) = task.await
            && error.is_panic()
        {
            std::panic::resume_unwind(error.into_panic());
        }
    }
    .boxed()
}

/// Register shutdown signals before a standalone Sail process starts serving.
pub fn stop_signal() -> std::io::Result<impl Future<Output = ()>> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut interrupt = signal(SignalKind::interrupt())?;
        let mut terminate = signal(SignalKind::terminate())?;
        Ok(async move {
            tokio::select! {
                _ = interrupt.recv() => {}
                _ = terminate.recv() => {}
            }
        })
    }

    #[cfg(windows)]
    {
        let mut interrupt = tokio::signal::windows::ctrl_c()?;
        Ok(async move {
            interrupt.recv().await;
        })
    }
}
