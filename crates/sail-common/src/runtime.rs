use tokio::runtime::{Handle, Runtime};

use crate::config::RuntimeConfig;
use crate::error::{CommonError, CommonResult};

#[derive(Debug)]
pub struct RuntimeManager {
    primary: Runtime,
    secondary: Option<Runtime>,
}

impl RuntimeManager {
    pub fn try_new(config: &RuntimeConfig) -> CommonResult<Self> {
        let primary = Self::build_runtime(config.stack_size)?;
        let secondary = if config.enable_secondary {
            Some(Self::build_runtime(config.stack_size)?)
        } else {
            None
        };

        Ok(Self { primary, secondary })
    }

    pub fn handle(&self) -> RuntimeHandle {
        let primary = self.primary.handle().clone();
        let secondary = self.secondary.as_ref().map(|r| r.handle().clone());
        RuntimeHandle { primary, secondary }
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
    secondary: Option<Handle>,
}

impl RuntimeHandle {
    pub fn primary(&self) -> &Handle {
        &self.primary
    }

    pub fn secondary(&self) -> Option<&Handle> {
        self.secondary.as_ref()
    }
}
