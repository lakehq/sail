use tokio::runtime::{Handle, Runtime};

use crate::config::RuntimeConfig;
use crate::error::{CommonError, CommonResult};

#[derive(Debug)]
pub struct RuntimeManager {
    primary: Runtime,
    // FIXME:
    //  Ideally IO bound tasks should be run on the primary runtime,
    //  and CPU bound tasks on the secondary runtime.
    io: Runtime,
    io_runtime_for_object_store: bool,
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
        let primary = Self::build_runtime(config.stack_size)?;
        let io = Self::build_runtime(config.stack_size)?;
        Ok(Self {
            primary,
            io,
            io_runtime_for_object_store: config.enable_secondary,
        })
    }

    pub fn handle(&self) -> RuntimeHandle {
        let primary = self.primary.handle().clone();
        let io = self.io.handle().clone();
        let io_runtime_for_object_store = self.io_runtime_for_object_store;
        RuntimeHandle {
            primary,
            io,
            io_runtime_for_object_store,
        }
    }

    fn build_runtime(stack_size: usize) -> CommonResult<Runtime> {
        tokio::runtime::Builder::new_multi_thread()
            .thread_stack_size(stack_size)
            .enable_all()
            .build()
            .map_err(|e| CommonError::internal(e.to_string()))
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeHandle {
    primary: Handle,
    io: Handle,
    io_runtime_for_object_store: bool,
}

impl RuntimeHandle {
    pub fn new(primary: Handle, io: Handle, io_runtime_for_object_store: bool) -> Self {
        Self {
            primary,
            io,
            io_runtime_for_object_store,
        }
    }

    pub fn primary(&self) -> &Handle {
        &self.primary
    }

    pub fn io(&self) -> &Handle {
        &self.io
    }

    pub fn io_runtime_for_object_store(&self) -> bool {
        self.io_runtime_for_object_store
    }
}
