use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::{SessionState, TaskContext};
use datafusion::prelude::SessionContext;
use datafusion_common::{internal_datafusion_err, Result};

pub trait SessionExtension: Send + Sync + 'static {
    fn name() -> &'static str;
}

pub trait SessionExtensionAccessor {
    fn extension<T: SessionExtension>(&self) -> Result<Arc<T>>;
    fn runtime_env(&self) -> Arc<RuntimeEnv>;
}

impl SessionExtensionAccessor for SessionContext {
    fn extension<T: SessionExtension>(&self) -> Result<Arc<T>> {
        self.state_ref()
            .read()
            .config()
            .get_extension::<T>()
            .ok_or_else(|| internal_datafusion_err!("session extension not found: {}", T::name()))
    }

    fn runtime_env(&self) -> Arc<RuntimeEnv> {
        self.state_ref().read().runtime_env().clone()
    }
}

impl SessionExtensionAccessor for SessionState {
    fn extension<T: SessionExtension>(&self) -> Result<Arc<T>> {
        self.config()
            .get_extension::<T>()
            .ok_or_else(|| internal_datafusion_err!("session extension not found: {}", T::name()))
    }

    fn runtime_env(&self) -> Arc<RuntimeEnv> {
        self.runtime_env().clone()
    }
}

impl SessionExtensionAccessor for &dyn Session {
    fn extension<T: SessionExtension>(&self) -> Result<Arc<T>> {
        self.config()
            .get_extension::<T>()
            .ok_or_else(|| internal_datafusion_err!("session extension not found: {}", T::name()))
    }

    fn runtime_env(&self) -> Arc<RuntimeEnv> {
        Session::runtime_env(*self).clone()
    }
}

impl SessionExtensionAccessor for TaskContext {
    fn extension<T: SessionExtension>(&self) -> Result<Arc<T>> {
        self.session_config()
            .get_extension::<T>()
            .ok_or_else(|| internal_datafusion_err!("session extension not found: {}", T::name()))
    }

    fn runtime_env(&self) -> Arc<RuntimeEnv> {
        self.runtime_env().clone()
    }
}
