use std::sync::Arc;

use datafusion::execution::SessionState;
use iceberg_datafusion::IcebergTableProviderFactory;

pub fn register_table_factories(state: &mut SessionState) {
    state.table_factories_mut().insert(
        "ICEBERG".to_string(),
        Arc::new(IcebergTableProviderFactory::new()),
    );
}
