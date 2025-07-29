mod error;
pub mod executor;
pub mod io;

use sail_common::config::RuntimeConfig;
use tokio::runtime::{Handle, Runtime};

use crate::error::{RuntimeError, RuntimeResult};

#[derive(Debug)]
pub struct RuntimeManager {
    primary: Runtime,
    dedicated_executor: executor::DedicatedExecutor,
}

impl RuntimeManager {
    pub fn try_new(config: &RuntimeConfig, executor_name: &str) -> RuntimeResult<Self> {
        let primary = runtime_builder(config)
            .build()
            .map_err(|e| RuntimeError::internal(e.to_string()))?;
        let dedicated_executor =
            executor::DedicatedExecutor::new(executor_name, runtime_builder(config));
        Ok(Self {
            primary,
            dedicated_executor,
        })
    }

    pub fn handle(&self) -> RuntimeHandle {
        let primary = self.primary.handle().clone();
        let dedicated_executor = self.dedicated_executor.clone();
        RuntimeHandle {
            primary,
            dedicated_executor,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeHandle {
    primary: Handle,
    dedicated_executor: executor::DedicatedExecutor,
}

impl RuntimeHandle {
    pub fn primary(&self) -> &Handle {
        &self.primary
    }

    pub fn dedicated_executor(&self) -> &executor::DedicatedExecutor {
        &self.dedicated_executor
    }
}

pub fn runtime_builder(config: &RuntimeConfig) -> tokio::runtime::Builder {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.thread_stack_size(config.stack_size);
    builder.enable_all();
    builder
}
