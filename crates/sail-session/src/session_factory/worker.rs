use std::sync::Arc;

use datafusion::common::Result;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use sail_common::runtime::RuntimeHandle;
use sail_object_store::DynamicObjectStoreRegistry;

use crate::session_factory::SessionFactory;

pub struct WorkerSessionFactory<'a> {
    runtime: &'a RuntimeHandle,
}

impl<'a> WorkerSessionFactory<'a> {
    pub fn new(runtime: &'a RuntimeHandle) -> Self {
        Self { runtime }
    }
}

impl<'a> SessionFactory<()> for WorkerSessionFactory<'a> {
    fn create(&mut self, _info: ()) -> Result<SessionContext> {
        let runtime = {
            let registry = DynamicObjectStoreRegistry::new(self.runtime.clone());
            let builder =
                RuntimeEnvBuilder::default().with_object_store_registry(Arc::new(registry));
            Arc::new(builder.build()?)
        };
        let config = SessionConfig::default();
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_default_features()
            .build();
        let session = SessionContext::new_with_state(state);
        Ok(session)
    }
}
