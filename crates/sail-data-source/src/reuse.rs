use std::sync::Arc;

use datafusion::datasource::{TableProvider, source_as_provider};
use datafusion::logical_expr::TableSource;
use sail_common_datafusion::rename::table_provider::RenameTableProvider;
use sail_common_datafusion::streaming::source::StreamSourceTableProvider;

use crate::formats::python::PythonTableProvider;

/// Returns whether repeated reads of the source within a query are interchangeable,
/// so that table references can share the source and equivalent scalar subqueries
/// over it can run once. Python data sources are read once for each reference, as in
/// Spark, and streaming sources are not reusable.
pub fn is_reusable_source(source: &Arc<dyn TableSource>) -> bool {
    fn is_reusable_provider(provider: &dyn TableProvider) -> bool {
        if let Some(rename) = provider.downcast_ref::<RenameTableProvider>() {
            is_reusable_provider(rename.inner().as_ref())
        } else {
            !(provider.is::<PythonTableProvider>() || provider.is::<StreamSourceTableProvider>())
        }
    }
    match source_as_provider(source) {
        Ok(provider) => is_reusable_provider(provider.as_ref()),
        Err(_) => true,
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::datasource::provider_as_source;

    use super::*;
    use crate::formats::python::InProcessExecutor;

    #[test]
    fn test_python_data_sources_are_not_reusable() -> datafusion_common::Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let python: Arc<dyn TableProvider> = Arc::new(PythonTableProvider::new(
            Arc::new(InProcessExecutor::new()),
            vec![],
            Arc::clone(&schema),
        ));
        let renamed = Arc::new(RenameTableProvider::try_new(
            Arc::clone(&python),
            vec!["y".to_string()],
        )?);
        assert!(!is_reusable_source(&provider_as_source(python)));
        assert!(!is_reusable_source(&provider_as_source(renamed)));
        assert!(is_reusable_source(&provider_as_source(Arc::new(
            EmptyTable::new(schema)
        ))));
        Ok(())
    }
}
