use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::common::{exec_err, plan_err, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::system::catalog::SystemTable;

use crate::service::SystemTableService;

#[derive(Debug)]
pub struct SystemTableExec {
    table: SystemTable,
    projection: Option<Vec<usize>>,
    filters: Vec<Arc<dyn PhysicalExpr>>,
    fetch: Option<usize>,
    properties: PlanProperties,
}

impl SystemTableExec {
    pub fn try_new(
        table: SystemTable,
        projection: Option<Vec<usize>>,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        fetch: Option<usize>,
    ) -> Result<Self> {
        let schema = if let Some(projection) = &projection {
            Arc::new(table.schema().project(projection)?)
        } else {
            table.schema()
        };
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Ok(Self {
            table,
            projection,
            filters,
            fetch,
            properties,
        })
    }

    pub fn table(&self) -> SystemTable {
        self.table
    }

    pub fn projection(&self) -> Option<&[usize]> {
        self.projection.as_deref()
    }

    pub fn filters(&self) -> &[Arc<dyn PhysicalExpr>] {
        self.filters.as_slice()
    }

    pub fn fetch(&self) -> Option<usize> {
        self.fetch
    }
}

impl DisplayAs for SystemTableExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "SystemTableExec: table={}, projection={:?}, filters={:?}, fetch={:?}",
            self.table.name(),
            self.projection,
            self.filters,
            self.fetch,
        )
    }
}

impl ExecutionPlan for SystemTableExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return plan_err!("{} should not have children", self.name());
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return exec_err!(
                "{} expects only partition 0 but got {}",
                self.name(),
                partition
            );
        }
        let table = self.table;
        let projection = self.projection.clone();
        let filters = self.filters.clone();
        let fetch = self.fetch;
        let stream = futures::stream::once(async move {
            context
                .extension::<SystemTableService>()?
                .read(table, projection, filters, fetch)
                .await
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}
