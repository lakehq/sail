use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::plan_datafusion_err;
use datafusion::logical_expr::{LogicalPlan, TableSource};
use datafusion_common::Result;

use crate::datasource::{SinkInfo, SourceInfo};
use crate::extension::SessionExtension;

#[derive(Debug, Clone)]
pub struct DataSourceMetadata {
    pub schema: SchemaRef,
    pub properties: Vec<(String, String)>,
}

#[async_trait]
pub trait DataSourceFormat: Send + Sync {
    fn name(&self) -> &str;

    async fn create_source(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableSource>>;

    async fn infer_schema(&self, ctx: &dyn Session, info: SourceInfo) -> Result<SchemaRef> {
        Ok(self.create_source(ctx, info).await?.schema())
    }

    async fn infer_metadata(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<DataSourceMetadata> {
        Ok(DataSourceMetadata {
            schema: self.infer_schema(ctx, info).await?,
            properties: vec![],
        })
    }

    async fn create_writer(&self, ctx: &dyn Session, info: SinkInfo) -> Result<LogicalPlan>;
}

#[derive(Default)]
pub struct DataSourceFormatRegistry {
    formats: RwLock<HashMap<String, Arc<dyn DataSourceFormat>>>,
}

impl DataSourceFormatRegistry {
    pub fn new() -> Self {
        Self {
            formats: RwLock::new(HashMap::new()),
        }
    }

    pub fn register(&self, format: Arc<dyn DataSourceFormat>) -> Result<()> {
        let mut formats = self
            .formats
            .write()
            .map_err(|_| plan_datafusion_err!("data source format registry poisoned"))?;
        formats.insert(format.name().to_lowercase(), format);
        Ok(())
    }

    pub fn get(&self, name: &str) -> Result<Arc<dyn DataSourceFormat>> {
        self.get_optional(name)?
            .ok_or_else(|| missing_data_source_format_error(name))
    }

    pub fn get_optional(&self, name: &str) -> Result<Option<Arc<dyn DataSourceFormat>>> {
        let formats = self
            .formats
            .read()
            .map_err(|_| plan_datafusion_err!("data source format registry poisoned"))?;
        Ok(formats.get(&name.to_lowercase()).cloned())
    }
}

fn missing_data_source_format_error(name: &str) -> datafusion::common::DataFusionError {
    if name.eq_ignore_ascii_case("jdbc") {
        plan_datafusion_err!(
            "No data source format found for: {name}. \
             The JDBC data source is provided by pysail and must be registered before use: \
             `from pysail.spark.datasource.jdbc import JdbcDataSource`; \
             `spark.dataSource.register(JdbcDataSource)`"
        )
    } else {
        plan_datafusion_err!("No data source format found for: {name}")
    }
}

impl SessionExtension for DataSourceFormatRegistry {
    fn name() -> &'static str {
        "DataSourceFormatRegistry"
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::not_impl_err;

    use super::*;

    struct TestDataSourceFormat;

    #[async_trait]
    impl DataSourceFormat for TestDataSourceFormat {
        fn name(&self) -> &str {
            "test"
        }

        async fn create_source(
            &self,
            _ctx: &dyn Session,
            _info: SourceInfo,
        ) -> Result<Arc<dyn TableSource>> {
            not_impl_err!("test format does not create sources")
        }

        async fn create_writer(&self, _ctx: &dyn Session, _info: SinkInfo) -> Result<LogicalPlan> {
            not_impl_err!("test format does not create writers")
        }
    }

    #[test]
    fn missing_jdbc_data_source_format_error_includes_registration_hint()
    -> std::result::Result<(), String> {
        let registry = DataSourceFormatRegistry::new();
        let error = match registry.get("jdbc") {
            Ok(_) => return Err("expected missing jdbc data source format error".to_string()),
            Err(error) => error.to_string(),
        };

        assert!(error.contains("No data source format found for: jdbc"));
        assert!(error.contains("from pysail.spark.datasource.jdbc import JdbcDataSource"));
        assert!(error.contains("spark.dataSource.register(JdbcDataSource)"));
        Ok(())
    }

    #[test]
    fn missing_non_jdbc_data_source_format_error_stays_generic() -> std::result::Result<(), String>
    {
        let registry = DataSourceFormatRegistry::new();
        let error = match registry.get("unknown") {
            Ok(_) => return Err("expected missing unknown data source format error".to_string()),
            Err(error) => error.to_string(),
        };

        assert_eq!(
            error,
            "Error during planning: No data source format found for: unknown"
        );
        Ok(())
    }

    #[test]
    fn registry_is_case_insensitive() -> Result<()> {
        let registry = DataSourceFormatRegistry::new();
        registry.register(Arc::new(TestDataSourceFormat))?;
        assert_eq!(registry.get("TEST")?.name(), "test");
        Ok(())
    }
}
