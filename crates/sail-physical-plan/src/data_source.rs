use std::fmt;
use std::sync::Arc;

use datafusion::datasource::source::{DataSource, DataSourceExec};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::{exec_err, internal_err, Result};

/// A [`DataSourceExec`] wrapper for Sail remote codec.
///
/// DataFusion's built-in physical plan codec can currently mishandle file
/// compression metadata for data sources in cluster mode. We wrap
/// [`DataSourceExec`] before remote plan encoding so our extension codec handles
/// every supported data source explicitly. The wrapper is not present
/// in decoded remote plans so it would never be executed.
#[derive(Clone, Debug)]
pub struct RemoteDataSourceExec {
    data_source: Arc<dyn DataSource>,
    properties: Arc<PlanProperties>,
}

impl RemoteDataSourceExec {
    pub fn new(node: &DataSourceExec) -> Self {
        Self {
            data_source: Arc::clone(node.data_source()),
            properties: Arc::clone(node.properties()),
        }
    }

    pub fn data_source(&self) -> &Arc<dyn DataSource> {
        &self.data_source
    }
}

impl DisplayAs for RemoteDataSourceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl ExecutionPlan for RemoteDataSourceExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return internal_err!("{} should not have children", self.name());
        }
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        exec_err!("{} cannot be executed", self.name())
    }
}
