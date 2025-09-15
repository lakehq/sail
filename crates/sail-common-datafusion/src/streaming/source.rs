use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{internal_err, Constraints, Result};

/// A streaming data source.
/// The streaming logical plan rewriter will identify such data sources
/// that are wrapped in the [`TableProvider`] and rewrite the logical plan.
#[async_trait::async_trait]
pub trait StreamSource: Send + Sync + fmt::Debug {
    /// The data schema for the data source.
    /// This is the schema without flow event fields.
    fn data_schema(&self) -> SchemaRef;

    fn constraints(&self) -> Option<&Constraints> {
        None
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    /// Creates an execution plan that will scan the source.
    /// An encoded flow event stream is returned from execution.
    /// The schema of the scan is the flow event schema derived from the data schema.
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}

///A [`TableProvider`] implementation for a streaming source.
#[derive(Debug)]
pub struct StreamSourceTableProvider {
    source: Arc<dyn StreamSource>,
}

impl StreamSourceTableProvider {
    pub fn new(source: Arc<dyn StreamSource>) -> Self {
        Self { source }
    }

    pub fn source(&self) -> &Arc<dyn StreamSource> {
        &self.source
    }
}

#[async_trait::async_trait]
impl TableProvider for StreamSourceTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        // The schema of the table provider is the data schema of the source
        // since this table provider only exists before streaming logical plan rewriting.
        self.source.data_schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.source.constraints()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        internal_err!("stream source should be rewritten during logical planning")
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.source.supports_filters_pushdown(filters)
    }
}
