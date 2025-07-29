mod error;

use sail_common::config::RuntimeConfig;
use tokio::runtime::{Handle, Runtime};

use crate::error::{RuntimeError, RuntimeResult};

#[derive(Debug)]
pub struct RuntimeManager {
    primary: Runtime,
    cpu: Runtime,
}

impl RuntimeManager {
    pub fn try_new(config: &RuntimeConfig) -> RuntimeResult<Self> {
        let primary = Self::build_runtime(config.stack_size)?;
        let cpu = Self::build_cpu_runtime(config.stack_size)?;
        // let secondary = if config.enable_secondary {
        //     Some(Self::build_cpu_runtime(config.stack_size)?)
        // } else {
        //     None
        // };

        Ok(Self { primary, cpu })
    }

    pub fn handle(&self) -> RuntimeHandle {
        let primary = self.primary.handle().clone();
        let cpu = self.cpu.handle().clone();
        RuntimeHandle { primary, cpu }
    }

    fn build_runtime(stack_size: usize) -> RuntimeResult<Runtime> {
        tokio::runtime::Builder::new_multi_thread()
            .thread_stack_size(stack_size)
            .enable_all()
            .build()
            .map_err(|e| RuntimeError::internal(e.to_string()))
    }

    fn build_cpu_runtime(stack_size: usize) -> RuntimeResult<Runtime> {
        tokio::runtime::Builder::new_multi_thread()
            .thread_stack_size(stack_size)
            .enable_time()
            .build()
            .map_err(|e| RuntimeError::internal(e.to_string()))
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeHandle {
    primary: Handle,
    cpu: Handle,
}

impl RuntimeHandle {
    pub fn primary(&self) -> &Handle {
        &self.primary
    }

    pub fn cpu(&self) -> &Handle {
        &self.cpu
    }
}
