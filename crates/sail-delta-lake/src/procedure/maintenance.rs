use std::collections::BTreeMap;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::{DFSchema, DFSchemaRef, Result, internal_err, not_impl_err, plan_err};
use datafusion::execution::{SessionStateBuilder, TaskContext};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::tree_node::TreeNodeRecursion;
use educe::Educe;
use futures::TryStreamExt;
use sail_common_datafusion::catalog::LakehouseExecutionContext;
use sail_common_datafusion::datasource::{OptionLayer, SourceInfo};
use sail_common_datafusion::lakeprocedure::{
    LakeProcedureCall, LakeProcedureExecutionTarget, LakeProcedurePredicate,
};
use sail_data_source::options::ResolveOptions;
use sail_physical_plan::lake_procedure::prepare_lake_procedure_execution;

use super::optimize::DeltaOptimizePlan;
use super::{optimize, vacuum};
use crate::DeltaLakeSource;
use crate::options::r#gen::DeltaReadOptions;
use crate::spec::TableFeature;
use crate::table::load_delta_read_state;
use crate::transaction::PROTOCOL;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub(crate) struct DeltaMaintenanceNode {
    pub call: LakeProcedureCall,
    pub options: BTreeMap<String, String>,
    pub paths: Vec<String>,
    pub read_options: Vec<OptionLayer>,
    pub lakehouse_table: Option<LakehouseExecutionContext>,
    #[educe(PartialOrd(ignore))]
    pub predicate: Option<LakeProcedurePredicate>,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
}

impl DeltaMaintenanceNode {
    pub fn try_new(
        call: LakeProcedureCall,
        info: SourceInfo,
        mut predicates: Vec<LakeProcedurePredicate>,
    ) -> Result<Self> {
        if predicates.len() > 1 {
            return plan_err!("Delta maintenance accepts one predicate");
        }
        let predicate = predicates.pop();
        call.validate()?;
        let options = info
            .options
            .clone()
            .into_iter()
            .flat_map(|layer| layer.into_opaque_options())
            .collect();
        let schema = Arc::new(DFSchema::try_from(call.invocation.procedure.schema())?);
        Ok(Self {
            call,
            options,
            paths: info.paths,
            read_options: info.options,
            lakehouse_table: info.lakehouse_table,
            predicate,
            schema,
        })
    }
}

impl UserDefinedLogicalNodeCore for DeltaMaintenanceNode {
    fn name(&self) -> &str {
        "DeltaMaintenance"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "DeltaMaintenance: procedure={}",
            self.call.invocation.procedure.name
        )
    }
    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if !exprs.is_empty() || !inputs.is_empty() {
            return internal_err!("DeltaMaintenance is a leaf");
        }
        Ok(self.clone())
    }
}

/// Reacquires catalog access on the coordinator before applying maintenance changes.
#[derive(Debug, Clone)]
pub struct DeltaMaintenanceExec {
    call: LakeProcedureCall,
    options: BTreeMap<String, String>,
    predicate: Option<DeltaMaintenancePredicate>,
    input: Option<Arc<dyn ExecutionPlan>>,
    optimize: Option<DeltaOptimizePlan>,
    properties: Arc<PlanProperties>,
}

impl DeltaMaintenanceExec {
    pub fn try_new(
        call: LakeProcedureCall,
        options: BTreeMap<String, String>,
        predicate: Option<DeltaMaintenancePredicate>,
    ) -> Result<Self> {
        call.validate()?;
        if !matches!(
            call.invocation.procedure.name.as_str(),
            "optimize" | "vacuum"
        ) {
            return plan_err!("Unknown Delta maintenance operation");
        }
        if super::optional_string(&call.invocation, "predicate")?.is_some() != predicate.is_some() {
            return plan_err!("Delta maintenance predicate does not match its invocation");
        }
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(call.invocation.procedure.schema()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Self {
            call,
            options,
            predicate,
            input: None,
            optimize: None,
            properties,
        })
    }

    pub fn with_optimize(
        mut self,
        input: Arc<dyn ExecutionPlan>,
        plan: DeltaOptimizePlan,
    ) -> Result<Self> {
        if self.call.invocation.procedure.name != "optimize"
            || input.schema() != crate::physical_plan::delta_action_schema()?
            || plan.version < 0
            || plan.parallelism == 0
            || plan.considered < plan.removed.len()
        {
            return plan_err!("Invalid Delta OPTIMIZE rewrite plan");
        }
        self.input = Some(input);
        self.optimize = Some(plan);
        Ok(self)
    }

    pub fn input(&self) -> Option<&Arc<dyn ExecutionPlan>> {
        self.input.as_ref()
    }

    pub fn optimize_plan(&self) -> Option<&DeltaOptimizePlan> {
        self.optimize.as_ref()
    }
    pub fn call(&self) -> &LakeProcedureCall {
        &self.call
    }
    pub fn options(&self) -> &BTreeMap<String, String> {
        &self.options
    }
    pub fn predicate(&self) -> Option<&DeltaMaintenancePredicate> {
        self.predicate.as_ref()
    }
}

impl DisplayAs for DeltaMaintenanceExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "DeltaMaintenanceExec: procedure={}",
            self.call.invocation.procedure.name
        )
    }
}

impl ExecutionPlan for DeltaMaintenanceExec {
    fn name(&self) -> &'static str {
        "DeltaMaintenanceExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.input.iter().collect()
    }
    fn required_input_distribution(&self) -> Vec<datafusion::physical_expr::Distribution> {
        self.input
            .iter()
            .map(|_| datafusion::physical_expr::Distribution::SinglePartition)
            .collect()
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        self.input.iter().map(|_| false).collect()
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    #[expect(deprecated)]
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _: datafusion::physical_plan::ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_new_children(children)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match (self.optimize.as_ref(), children.as_slice()) {
            (Some(plan), [input]) => Ok(Arc::new(
                Self::try_new(
                    self.call.clone(),
                    self.options.clone(),
                    self.predicate.clone(),
                )?
                .with_optimize(input.clone(), plan.clone())?,
            )),
            (None, []) => Ok(self),
            _ => internal_err!("Delta maintenance child count does not match its operation"),
        }
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("Delta maintenance expects partition zero");
        }
        let call = self.call.clone();
        let options = self.options.clone();
        let input = self.input.clone();
        let optimize = self.optimize.clone();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::once(
                async move { execute(context, call, options, input, optimize).await },
            ),
        )))
    }
}

async fn execute(
    context: Arc<TaskContext>,
    call: LakeProcedureCall,
    options: BTreeMap<String, String>,
    input: Option<Arc<dyn ExecutionPlan>>,
    optimize: Option<DeltaOptimizePlan>,
) -> Result<RecordBatch> {
    let started = chrono::Utc::now().timestamp_millis();
    let LakeProcedureExecutionTarget::Table(info) =
        prepare_lake_procedure_execution(context.as_ref(), &call).await?
    else {
        return plan_err!("Delta maintenance requires a table");
    };
    let session = SessionStateBuilder::new()
        .with_default_features()
        .with_config(context.session_config().clone())
        .with_runtime_env(context.runtime_env())
        .with_scalar_functions(context.scalar_functions().values().cloned().collect())
        .build();
    let table_url = DeltaLakeSource::parse_table_url(&session, info.paths).await?;
    let read_options = resolve_maintenance_read_options(
        &session,
        info.options,
        optimize.as_ref().map(|plan| plan.version),
    )?;
    if let Some(plan) = &optimize
        && table_url.as_str() != plan.table_url
    {
        return plan_err!("OPTIMIZE table location changed after planning");
    }
    let (snapshot, log_store, _) = load_delta_read_state(
        &session,
        table_url,
        None,
        read_options,
        false,
        info.lakehouse_table.clone(),
    )
    .await?;
    validate_snapshot(&snapshot, &call.invocation)?;
    match call.invocation.procedure.name.as_str() {
        "optimize" => {
            let (Some(input), Some(plan)) = (input, optimize) else {
                return internal_err!("OPTIMIZE is missing its rewrite plan");
            };
            if snapshot.metadata().id() != plan.table_id {
                return plan_err!("OPTIMIZE table identity changed after planning");
            }
            let mut output = input.execute(0, context)?;
            let mut added = Vec::new();
            while let Some(batch) = output.try_next().await? {
                added.extend(crate::physical_plan::decode_adds_from_batch(&batch)?);
            }
            optimize::finish(snapshot, log_store, &call.invocation, &plan, added, started).await
        }
        "vacuum" => {
            vacuum::execute(&session, snapshot, log_store, &call.invocation, &options).await
        }
        _ => internal_err!("Unknown Delta maintenance operation"),
    }
}

/// Maintenance needs complete file state, not a metadata-only, CDF, or historical read.
/// Only OPTIMIZE execution may pin the snapshot version selected during planning.
pub(super) fn resolve_maintenance_read_options(
    session: &dyn Session,
    options: Vec<OptionLayer>,
    planned_version: Option<i64>,
) -> Result<DeltaReadOptions> {
    let mut options = DeltaReadOptions::resolve(session, options)?;
    options.metadata_as_data_read = false;
    options.read_change_feed = false;
    options.version_as_of = planned_version;
    options.timestamp_as_of = None;
    Ok(options)
}

pub(super) fn validate_snapshot(
    snapshot: &crate::snapshot::DeltaSnapshot,
    invocation: &sail_common_datafusion::lakeprocedure::LakeProcedureInvocation,
) -> Result<()> {
    PROTOCOL
        .can_write_to_protocol(snapshot.protocol())
        .map_err(crate::spec::DeltaError::from)?;
    if snapshot
        .protocol()
        .has_writer_feature(&TableFeature::CatalogManaged)
    {
        // TODO: Integrate catalog-managed maintenance commits and file-lifecycle authorization.
        return not_impl_err!(
            "Delta {} is not supported for catalog-managed tables",
            invocation.procedure.name
        );
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct DeltaMaintenancePredicate {
    pub schema: SchemaRef,
    pub expr: Arc<dyn PhysicalExpr>,
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::options::DeltaLogReplayStrategy;

    #[test]
    fn maintenance_ignores_scan_modes_and_time_travel() -> Result<()> {
        let session = SessionContext::new().state();
        // CDF and time travel are read options, not catalog table properties.
        for (metadata, cdf) in [(false, false), (true, false), (false, true), (true, true)] {
            let layers = vec![OptionLayer::OptionList {
                items: vec![
                    ("metadataAsDataRead".to_string(), metadata.to_string()),
                    ("readChangeFeed".to_string(), cdf.to_string()),
                    ("versionAsOf".to_string(), "1".to_string()),
                    ("timestampAsOf".to_string(), "2020-01-01".to_string()),
                    ("deltaLogReplayStrategy".to_string(), "sort".to_string()),
                ],
            }];
            let normal = DeltaReadOptions::resolve(&session, layers.clone())?;
            assert_eq!(normal.metadata_as_data_read, metadata);
            assert_eq!(normal.read_change_feed, cdf);
            assert_eq!(normal.version_as_of, Some(1));
            assert_eq!(normal.timestamp_as_of.as_deref(), Some("2020-01-01"));
            for planned_version in [None, Some(7)] {
                let options =
                    resolve_maintenance_read_options(&session, layers.clone(), planned_version)?;
                assert!(!options.metadata_as_data_read);
                assert!(!options.read_change_feed);
                assert_eq!(options.version_as_of, planned_version);
                assert!(options.timestamp_as_of.is_none());
                assert_eq!(
                    options.delta_log_replay_strategy,
                    DeltaLogReplayStrategy::Sort
                );
            }
        }
        Ok(())
    }
}
