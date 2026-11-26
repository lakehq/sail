// SPDX-License-Identifier: Apache-2.0

use std::any::Any;

use datafusion_common::config::{ConfigEntry, ConfigExtension, ConfigOptions, ExtensionOptions};
use datafusion_common::{Result, config_err};
use tokio::runtime::Handle;
use tokio_util::task::AbortOnDropHandle;

#[derive(Clone, Debug)]
pub struct IoRuntime(pub Handle);

impl IoRuntime {
    /// Standalone DataFusion contexts use the runtime of the caller.
    pub fn from_options(options: &ConfigOptions) -> Self {
        options
            .extensions
            .get::<Self>()
            .cloned()
            .unwrap_or_else(|| Self(Handle::current()))
    }

    pub fn spawn<F>(&self, future: F) -> AbortOnDropHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        AbortOnDropHandle::new(self.0.spawn(future))
    }
}

impl ConfigExtension for IoRuntime {
    const PREFIX: &'static str = "sail_io_runtime";
}

impl ExtensionOptions for IoRuntime {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, _value: &str) -> Result<()> {
        config_err!("unknown I/O runtime option: {key}")
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        Vec::new()
    }
}
