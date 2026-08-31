use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::{Result, exec_err, internal_err};
use sail_common_datafusion::lakeprocedure::LakeProcedureCall;
use sail_physical_plan::lake_procedure::prepare_lake_procedure_execution;

use crate::procedure::execute_iceberg_procedure;

/// Current coordinator-local implementation of Iceberg procedures.
///
/// This is one provider-owned implementation plan. It can be replaced per procedure with a
/// distributed plan without changing the engine-owned procedure boundary.
#[derive(Debug, Clone)]
pub struct IcebergProcedureExec {
    call: LakeProcedureCall,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl IcebergProcedureExec {
    pub fn new(call: LakeProcedureCall) -> Self {
        let schema = call.invocation.procedure.schema();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            call,
            schema,
            properties,
        }
    }

    pub fn call(&self) -> &LakeProcedureCall {
        &self.call
    }

    pub fn validate(&self) -> Result<()> {
        self.call.validate()?;
        if self.schema.as_ref() != self.call.invocation.procedure.schema().as_ref() {
            return internal_err!("Iceberg procedure schema does not match its descriptor");
        }
        Ok(())
    }
}

impl DisplayAs for IcebergProcedureExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "IcebergProcedureExec: procedure={}, invocation_id={}",
            self.call.invocation.procedure.name, self.call.invocation_id.0
        )
    }
}

impl ExecutionPlan for IcebergProcedureExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    #[expect(deprecated)]
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: datafusion::physical_plan::ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_new_children(children)
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
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.validate()?;
        if partition != 0 {
            return exec_err!(
                "{} expects only partition 0 but got {}",
                self.name(),
                partition
            );
        }
        let call = self.call.clone();
        let schema = self.schema.clone();
        let stream = futures::stream::once(async move {
            let target = prepare_lake_procedure_execution(context.as_ref(), &call).await?;
            let batch =
                execute_iceberg_procedure(context.as_ref(), target, call.invocation).await?;
            Ok(batch)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
