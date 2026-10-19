use std::collections::BTreeMap;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
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
use sail_common_datafusion::datasource::SourceInfo;
use sail_common_datafusion::lakeprocedure::{
    LakeProcedureCall, LakeProcedureExecutionTarget, LakeProcedurePredicate,
};
use sail_data_source::options::ResolveOptions;
use sail_physical_plan::lake_procedure::prepare_lake_procedure_execution;

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
            .into_iter()
            .flat_map(|layer| layer.into_opaque_options())
            .collect();
        let schema = Arc::new(DFSchema::try_from(call.invocation.procedure.schema())?);
        Ok(Self {
            call,
            options,
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

/// Mutations reacquire catalog access and current state on the coordinator at execution time.
#[derive(Debug, Clone)]
pub struct DeltaMaintenanceExec {
    call: LakeProcedureCall,
    options: BTreeMap<String, String>,
    predicate: Option<DeltaMaintenancePredicate>,
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
            properties,
        })
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
        vec![]
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
        if !children.is_empty() {
            return internal_err!("DeltaMaintenanceExec is a leaf");
        }
        Ok(self)
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
        let predicate = self.predicate.clone();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::once(async move { execute(context, call, options, predicate).await }),
        )))
    }
}

async fn execute(
    context: Arc<TaskContext>,
    call: LakeProcedureCall,
    options: BTreeMap<String, String>,
    predicate: Option<DeltaMaintenancePredicate>,
) -> Result<RecordBatch> {
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
    let read_options = DeltaReadOptions::resolve(&session, info.options)?;
    let (snapshot, log_store, scan_config) = load_delta_read_state(
        &session,
        table_url,
        None,
        read_options,
        false,
        info.lakehouse_table.clone(),
    )
    .await?;
    PROTOCOL
        .can_write_to_protocol(snapshot.protocol())
        .map_err(crate::spec::DeltaError::from)?;
    if snapshot
        .protocol()
        .has_writer_feature(&TableFeature::CatalogManaged)
    {
        return not_impl_err!(
            "Delta {} is not supported for catalog-managed tables",
            call.invocation.procedure.name
        );
    }
    match call.invocation.procedure.name.as_str() {
        "optimize" => {
            optimize::execute(
                &session,
                context,
                snapshot,
                log_store,
                scan_config,
                info.lakehouse_table,
                &call.invocation,
                &options,
                predicate.as_ref(),
            )
            .await
        }
        "vacuum" => {
            vacuum::execute(&session, snapshot, log_store, &call.invocation, &options).await
        }
        _ => internal_err!("Unknown Delta maintenance operation"),
    }
}

#[derive(Debug, Clone)]
pub struct DeltaMaintenancePredicate {
    pub schema: SchemaRef,
    pub expr: Arc<dyn PhysicalExpr>,
}
