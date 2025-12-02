use std::sync::Arc;

use datafusion::execution::SessionState;
use datafusion::prelude::SessionContext;
use datafusion_common::{internal_datafusion_err, Result};

pub trait SessionExtension: Send + Sync + 'static {
    fn name() -> &'static str;
}

pub trait SessionExtensionAccessor {
    fn extension<T: SessionExtension>(&self) -> Result<Arc<T>>;
}

impl SessionExtensionAccessor for SessionContext {
    fn extension<T: SessionExtension>(&self) -> Result<Arc<T>> {
        self.state_ref()
            .read()
            .config()
            .get_extension::<T>()
            .ok_or_else(|| internal_datafusion_err!("session extension not found: {}", T::name()))
    }
}

impl SessionExtensionAccessor for SessionState {
    fn extension<T: SessionExtension>(&self) -> Result<Arc<T>> {
        self.config()
            .get_extension::<T>()
            .ok_or_else(|| internal_datafusion_err!("session extension not found: {}", T::name()))
    }
}
