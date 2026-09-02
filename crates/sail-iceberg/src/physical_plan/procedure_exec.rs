use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::{Result, exec_err, internal_err};
use futures::StreamExt;
use sail_common_datafusion::lakeprocedure::{LakeProcedureAccess, LakeProcedureCall};
use sail_physical_plan::lake_procedure::prepare_lake_procedure_execution;

use crate::operations::SnapshotUpdateKind;
use crate::physical_plan::commit::commit_exec::IcebergCommitExec;
use crate::procedure::RewriteDataFilesPlan;
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
    input: Option<Arc<dyn ExecutionPlan>>,
    rewrite_data_files: Option<RewriteDataFilesPlan>,
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
            input: None,
            rewrite_data_files: None,
            schema,
            properties,
        };
        procedure.validate()?;
        Ok(procedure)
    }

    pub(crate) fn try_new_rewrite_data_files(
        call: LakeProcedureCall,
        input: Arc<dyn ExecutionPlan>,
        rewrite_data_files: RewriteDataFilesPlan,
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
            planned_table: None,
            input: Some(input),
            rewrite_data_files: Some(rewrite_data_files),
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

    pub fn try_new_from_serialized(
        call: LakeProcedureCall,
        planned_table: &str,
        rewrite_data_files: &str,
        input: Option<Arc<dyn ExecutionPlan>>,
    ) -> Result<Self> {
        if rewrite_data_files.is_empty() {
            if input.is_some() {
                return internal_err!("leaf Iceberg procedure cannot have a physical input");
            }
            return Self::try_new_from_serialized_table(call, planned_table);
        }
        if !planned_table.is_empty() {
            return internal_err!("rewrite_data_files cannot carry a planned procedure table");
        }
        let rewrite_data_files = serde_json::from_str(rewrite_data_files).map_err(|error| {
            datafusion_common::DataFusionError::Plan(format!(
                "failed to decode Iceberg rewrite_data_files plan: {error}"
            ))
        })?;
        let input = input.ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(
                "rewrite_data_files is missing its physical input".to_string(),
            )
        })?;
        Self::try_new_rewrite_data_files(call, input, rewrite_data_files)
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

    pub fn serialized_rewrite_data_files(&self) -> Result<String> {
        self.rewrite_data_files
            .as_ref()
            .map(|plan| {
                serde_json::to_string(plan).map_err(|error| {
                    datafusion_common::DataFusionError::Plan(format!(
                        "failed to encode Iceberg rewrite_data_files plan: {error}"
                    ))
                })
            })
            .unwrap_or_else(|| Ok(String::new()))
    }

    pub fn input(&self) -> Option<&Arc<dyn ExecutionPlan>> {
        self.input.as_ref()
    }

    pub fn call(&self) -> &LakeProcedureCall {
        &self.call
    }

    pub fn validate(&self) -> Result<()> {
        self.call.validate()?;
        if self.schema.as_ref() != self.call.invocation.procedure.schema().as_ref() {
            return internal_err!("Iceberg procedure schema does not match its descriptor");
        }
        match (
            self.call.invocation.procedure.access,
            &self.planned_table,
            &self.input,
            &self.rewrite_data_files,
        ) {
            (LakeProcedureAccess::MetadataRead, Some(table), None, None) => {
                table.validate_for_call(&self.call)?;
            }
            (LakeProcedureAccess::MetadataRead, _, _, _) => {
                return internal_err!(
                    "distributed Iceberg metadata procedure is missing its planned table"
                );
            }
            (LakeProcedureAccess::MetadataCommit, None, None, None) => {}
            (LakeProcedureAccess::MetadataCommit, None, Some(_), Some(_))
                if self
                    .call
                    .invocation
                    .procedure
                    .name
                    .eq_ignore_ascii_case("rewrite_data_files") => {}
            (LakeProcedureAccess::MetadataCommit, Some(_), _, _) => {
                return internal_err!(
                    "Iceberg metadata commit must reacquire its table at execution time"
                );
            }
            (LakeProcedureAccess::MetadataCommit, None, _, _) => {
                return internal_err!(
                    "Iceberg procedure input does not match its commit operation"
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
        self.input.iter().collect()
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
        match (&self.input, children.as_slice()) {
            (None, []) => Ok(self),
            (Some(_), [input]) => Ok(Arc::new(Self::try_new_rewrite_data_files(
                self.call.clone(),
                Arc::clone(input),
                self.rewrite_data_files.clone().ok_or_else(|| {
                    datafusion_common::DataFusionError::Internal(
                        "Iceberg procedure child is missing rewrite state".to_string(),
                    )
                })?,
            )?)),
            _ => internal_err!("{} child count does not match its operation", self.name()),
        }
    }

    fn required_input_distribution(&self) -> Vec<datafusion::physical_expr::Distribution> {
        self.input
            .as_ref()
            .map(|_| vec![datafusion::physical_expr::Distribution::SinglePartition])
            .unwrap_or_default()
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
        let input = self.input.clone();
        let rewrite_data_files = self.rewrite_data_files.clone();
        let schema = self.schema.clone();
        let execution_schema = schema.clone();
        let stream = futures::stream::once(async move {
            let table = match planned_table {
                Some(table) => table,
                None => {
                    let target = prepare_lake_procedure_execution(context.as_ref(), &call).await?;
                    ProcedureTable::from_execution_target(target).await?
                }
            };
            table.validate_for_call(&call)?;
            if let Some(rewrite_data_files) = rewrite_data_files {
                let input = input.ok_or_else(|| {
                    datafusion_common::DataFusionError::Internal(
                        "rewrite_data_files is missing its writer input".to_string(),
                    )
                })?;
                let table_url = table.table_url().await?;
                let mut stream = IcebergCommitExec::new(
                    input,
                    table_url,
                    table.lakehouse_table().cloned(),
                    SnapshotUpdateKind::RewriteDataFiles,
                )
                .with_expected_snapshot_id(Some(rewrite_data_files.expected_snapshot_id()))
                .with_removed_data_file_paths(rewrite_data_files.removed_data_file_paths().to_vec())
                .with_rewrite_data_files_output(
                    execution_schema.clone(),
                    rewrite_data_files.rewritten_data_files_count(),
                    rewrite_data_files.rewritten_bytes_count(),
                )
                .execute(0, Arc::clone(&context))?;
                let batch = stream.next().await.ok_or_else(|| {
                    datafusion_common::DataFusionError::Execution(
                        "rewrite_data_files commit returned no result".to_string(),
                    )
                })??;
                if stream.next().await.is_some() {
                    return internal_err!("rewrite_data_files commit returned multiple batches");
                }
                return Ok(batch);
            }
            let batch = execute_iceberg_procedure(context.as_ref(), table, call.invocation).await?;
            Ok(batch)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
