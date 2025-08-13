use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use datafusion_common::{not_impl_err, plan_err, Result};
use deltalake::arrow::datatypes::{Field, Schema};
use sail_common_datafusion::datasource::{SinkInfo, SourceInfo, TableFormat};

#[derive(Debug)]
pub struct RateTableFormat;

#[async_trait]
impl TableFormat for RateTableFormat {
    fn name(&self) -> &str {
        "rate"
    }

    async fn create_provider(
        &self,
        _ctx: &dyn Session,
        _info: SourceInfo,
    ) -> Result<Arc<dyn TableProvider>> {
        Ok(Arc::new(RateTableProvider::new()))
    }

    async fn create_writer(
        &self,
        _ctx: &dyn Session,
        _info: SinkInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan_err!("the rate table format does not support writing")
    }
}

#[derive(Debug, Clone)]
struct RateTableProvider {
    schema: SchemaRef,
}

impl RateTableProvider {
    pub fn new() -> Self {
        Self {
            schema: Arc::new(Schema::new(vec![Arc::new(Field::new(
                "value",
                DataType::Utf8,
                false,
            ))])),
        }
    }
}

#[async_trait]
impl TableProvider for RateTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
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
        Ok(Arc::new(RateExec::new(Arc::clone(&self.schema))))
    }
}

#[derive(Debug)]
struct RateExec {
    #[expect(unused)]
    schema: SchemaRef,
    properties: PlanProperties,
}

impl RateExec {
    pub fn new(schema: SchemaRef) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Both,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        );
        Self { schema, properties }
    }
}

impl DisplayAs for RateExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl ExecutionPlan for RateExec {
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
            plan_err!("{} cannot have children", self.name())
        } else {
            Ok(self)
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return plan_err!("{} only supports a single partition", self.name());
        }
        not_impl_err!("rate is not implemented yet")
    }
}
