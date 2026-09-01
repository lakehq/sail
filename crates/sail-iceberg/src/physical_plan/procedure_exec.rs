use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::{Result, exec_err, internal_err};
use sail_common_datafusion::lakeprocedure::{LakeProcedureAccess, LakeProcedureCall};
use sail_physical_plan::lake_procedure::prepare_lake_procedure_execution;

use crate::procedure::execute_iceberg_procedure;
use crate::procedure::table::ProcedureTable;

/// Provider-owned leaf implementation of Iceberg procedures.
///
/// Metadata reads carry a planned table to workers. Metadata commits omit it and reacquire the
/// table in the coordinator immediately before execution.
#[derive(Debug, Clone)]
pub struct IcebergProcedureExec {
    call: LakeProcedureCall,
    planned_table: Option<ProcedureTable>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl IcebergProcedureExec {
    pub(crate) fn try_new(
        call: LakeProcedureCall,
        planned_table: Option<ProcedureTable>,
    ) -> Result<Self> {
        call.validate()?;
        let schema = call.invocation.procedure.schema();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        let procedure = Self {
            call,
            planned_table,
            schema,
            properties,
        };
        procedure.validate()?;
        Ok(procedure)
    }

    pub fn try_new_from_serialized_table(
        call: LakeProcedureCall,
        planned_table: &str,
    ) -> Result<Self> {
        let planned_table = if planned_table.is_empty() {
            None
        } else {
            Some(serde_json::from_str(planned_table).map_err(|error| {
                datafusion_common::DataFusionError::Plan(format!(
                    "failed to decode Iceberg procedure table: {error}"
                ))
            })?)
        };
        Self::try_new(call, planned_table)
    }

    pub fn serialized_table(&self) -> Result<String> {
        self.planned_table
            .as_ref()
            .map(|table| {
                serde_json::to_string(table).map_err(|error| {
                    datafusion_common::DataFusionError::Plan(format!(
                        "failed to encode Iceberg procedure table: {error}"
                    ))
                })
            })
            .unwrap_or_else(|| Ok(String::new()))
    }

    pub fn call(&self) -> &LakeProcedureCall {
        &self.call
    }

    pub fn validate(&self) -> Result<()> {
        self.call.validate()?;
        if self.schema.as_ref() != self.call.invocation.procedure.schema().as_ref() {
            return internal_err!("Iceberg procedure schema does not match its descriptor");
        }
        match (self.call.invocation.procedure.access, &self.planned_table) {
            (LakeProcedureAccess::MetadataRead, Some(table)) => {
                table.validate_for_call(&self.call)?;
            }
            (LakeProcedureAccess::MetadataRead, None) => {
                return internal_err!(
                    "distributed Iceberg metadata procedure is missing its planned table"
                );
            }
            (LakeProcedureAccess::MetadataCommit, None) => {}
            (LakeProcedureAccess::MetadataCommit, Some(_)) => {
                return internal_err!(
                    "Iceberg metadata commit must reacquire its table at execution time"
                );
            }
        }
        Ok(())
    }
}

pub fn validate_iceberg_procedure_call_identity(
    implementation: &Arc<dyn ExecutionPlan>,
    expected: &LakeProcedureCall,
) -> Result<()> {
    if let Some(procedure) = implementation.downcast_ref::<IcebergProcedureExec>()
        && procedure.call() != expected
    {
        return internal_err!(
            "Iceberg procedure implementation call does not match its engine boundary"
        );
    }
    for child in implementation.children() {
        validate_iceberg_procedure_call_identity(child, expected)?;
    }
    Ok(())
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
        let planned_table = self.planned_table.clone();
        let schema = self.schema.clone();
        let stream = futures::stream::once(async move {
            let table = match planned_table {
                Some(table) => table,
                None => {
                    let target = prepare_lake_procedure_execution(context.as_ref(), &call).await?;
                    ProcedureTable::from_execution_target(target).await?
                }
            };
            let batch = execute_iceberg_procedure(context.as_ref(), table, call.invocation).await?;
            Ok(batch)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
