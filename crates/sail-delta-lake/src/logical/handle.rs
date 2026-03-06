use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;

use crate::datasource::DeltaScanConfig;
use crate::storage::LogStoreRef;
use crate::table::DeltaTableState;

#[derive(Clone, Debug)]
pub struct DeltaTableHandle(Arc<DeltaTableHandleInner>);

pub struct DeltaTableHandleInner {
    pub snapshot: DeltaTableState,
    pub log_store: LogStoreRef,
    pub config: DeltaScanConfig,
    pub schema: SchemaRef,
}

impl std::fmt::Debug for DeltaTableHandleInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaTableHandleInner")
            .field("snapshot_version", &self.snapshot.version())
            .field("config", &self.config)
            .field(
                "schema_fields",
                &self
                    .schema
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl DeltaTableHandle {
    pub fn new(inner: DeltaTableHandleInner) -> Self {
        Self(Arc::new(inner))
    }

    pub fn inner(&self) -> &Arc<DeltaTableHandleInner> {
        &self.0
    }
}

impl PartialEq for DeltaTableHandle {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for DeltaTableHandle {}

impl Hash for DeltaTableHandle {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (Arc::as_ptr(&self.0) as usize).hash(state);
    }
}

impl PartialOrd for DeltaTableHandle {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DeltaTableHandle {
    fn cmp(&self, other: &Self) -> Ordering {
        (Arc::as_ptr(&self.0) as usize).cmp(&(Arc::as_ptr(&other.0) as usize))
    }
}
