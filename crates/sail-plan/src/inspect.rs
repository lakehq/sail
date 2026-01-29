use std::any::Any;
use std::cmp::Ordering;
use std::fmt::Formatter;
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::StringBuilder;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::MemTable;
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::provider_as_source;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    collect, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use datafusion::prelude::SessionContext;
use datafusion_common::DFSchemaRef;
use datafusion_expr::{TableScan, UNNAMED_TABLE};
use futures::StreamExt;
use sail_common::spec::InspectNodeOutputFormat;

use crate::execute_logical_plan;
use crate::explain::strip_explain_side_effect_nodes;
use crate::streaming::rewriter::{is_streaming_plan, rewrite_streaming_plan};

#[allow(clippy::derived_hash_with_manual_eq)]
#[derive(Clone, Debug, Eq, Hash)]
pub struct InspectNodeOutputNode {
    name: String,
    node_name: String,
    format: InspectNodeOutputFormat,
    schema: DFSchemaRef,
    plan: Arc<LogicalPlan>,
}

impl InspectNodeOutputNode {
    pub fn try_new(
        node_name: String,
        format: InspectNodeOutputFormat,
        schema: DFSchemaRef,
        plan: LogicalPlan,
    ) -> Result<Self> {
        Ok(Self {
            name: format!("InspectNodeOutput: {node_name}"),
            node_name,
            format,
            schema,
            plan: Arc::new(plan),
        })
    }

    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    pub fn format(&self) -> InspectNodeOutputFormat {
        self.format.clone()
    }

    pub fn plan(&self) -> &LogicalPlan {
        self.plan.as_ref()
    }

    pub fn pretty_schema() -> Result<DFSchemaRef> {
        let fields = vec![
            Arc::new(Field::new("node", DataType::Utf8, false)),
            Arc::new(Field::new("line", DataType::Utf8, false)),
        ];
        Ok(DFSchemaRef::new(
            datafusion_common::DFSchema::from_unqualified_fields(
                fields.into(),
                Default::default(),
            )?,
        ))
    }

    pub async fn execute(&self, ctx: &SessionContext) -> Result<LogicalPlan> {
        let plan = build_physical_plan_for_execute(ctx, (*self.plan).clone()).await?;
        let capture = CaptureState::new();
        let wrapped = wrap_capture_plan(plan, self.node_name(), &capture)?;
        capture.ensure_match_policy(self.format())?;

        let _ = collect(wrapped, ctx.task_ctx()).await?;

        let provider = match self.format() {
            InspectNodeOutputFormat::Raw => {
                let schema = capture.schema()?;
                let mut batches = capture.batches_for(0);
                if batches.is_empty() {
                    batches.push(RecordBatch::new_empty(schema.clone()));
                }
                MemTable::try_new(schema, vec![batches])?
            }
            InspectNodeOutputFormat::Pretty => {
                let schema = capture.pretty_schema();
                let batches = capture.batches_pretty()?;
                MemTable::try_new(schema, vec![batches])?
            }
        };
        Ok(LogicalPlan::TableScan(TableScan::try_new(
            UNNAMED_TABLE,
            provider_as_source(Arc::new(provider)),
            None,
            vec![],
            None,
        )?))
    }
}

impl UserDefinedLogicalNodeCore for InspectNodeOutputNode {
    fn name(&self) -> &str {
        &self.name
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
        write!(f, "{}", self.name)
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if !exprs.is_empty() || !inputs.is_empty() {
            return Err(DataFusionError::Plan(
                "InspectNodeOutput does not accept expressions or inputs".to_string(),
            ));
        }
        Ok(self.clone())
    }
}

impl PartialEq for InspectNodeOutputNode {
    fn eq(&self, other: &Self) -> bool {
        self.node_name == other.node_name
            && self.format == other.format
            && self.plan == other.plan
            && self.schema == other.schema
    }
}

#[derive(PartialEq, PartialOrd)]
struct InspectNodeOutputNodeOrd<'a> {
    node_name: &'a String,
    format: &'a InspectNodeOutputFormat,
    plan: &'a Arc<LogicalPlan>,
}

impl<'a> From<&'a InspectNodeOutputNode> for InspectNodeOutputNodeOrd<'a> {
    fn from(node: &'a InspectNodeOutputNode) -> Self {
        Self {
            node_name: &node.node_name,
            format: &node.format,
            plan: &node.plan,
        }
    }
}

impl PartialOrd for InspectNodeOutputNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        InspectNodeOutputNodeOrd::from(self).partial_cmp(&other.into())
    }
}

pub async fn infer_node_schema(
    ctx: &SessionContext,
    plan: LogicalPlan,
    node_name: &str,
) -> Result<SchemaRef> {
    let plan = build_physical_plan_for_schema(ctx, plan).await?;
    let mut schemas = Vec::new();
    collect_node_schemas(&plan, node_name, &mut schemas);
    match schemas.len() {
        0 => Err(DataFusionError::Plan(format!(
            "InspectNodeOutput: no physical node named '{node_name}'"
        ))),
        1 => Ok(schemas.remove(0)),
        count => Err(DataFusionError::Plan(format!(
            "InspectNodeOutput: expected one node named '{node_name}', found {count}"
        ))),
    }
}

async fn build_physical_plan_for_execute(
    ctx: &SessionContext,
    plan: LogicalPlan,
) -> Result<Arc<dyn ExecutionPlan>> {
    let df = execute_logical_plan(ctx, plan).await?;
    let (session_state, plan) = df.into_parts();
    let plan = session_state.optimize(&plan)?;
    let plan = if is_streaming_plan(&plan)? {
        rewrite_streaming_plan(plan)?
    } else {
        plan
    };
    session_state
        .query_planner()
        .create_physical_plan(&plan, &session_state)
        .await
}

async fn build_physical_plan_for_schema(
    ctx: &SessionContext,
    plan: LogicalPlan,
) -> Result<Arc<dyn ExecutionPlan>> {
    let plan =
        strip_explain_side_effect_nodes(plan).map_err(|e| DataFusionError::Plan(e.to_string()))?;
    let session_state = ctx.state();
    let plan = session_state.optimize(&plan)?;
    let plan = if is_streaming_plan(&plan)? {
        rewrite_streaming_plan(plan)?
    } else {
        plan
    };
    session_state
        .query_planner()
        .create_physical_plan(&plan, &session_state)
        .await
}

fn collect_node_schemas(
    plan: &Arc<dyn ExecutionPlan>,
    node_name: &str,
    schemas: &mut Vec<SchemaRef>,
) {
    if plan.name() == node_name {
        schemas.push(plan.schema());
    }
    for child in plan.children() {
        collect_node_schemas(child, node_name, schemas);
    }
}

#[derive(Clone, Debug)]
struct CaptureState {
    batches: Arc<Mutex<Vec<Vec<RecordBatch>>>>,
    node_names: Arc<Mutex<Vec<String>>>,
    schema: Arc<Mutex<Option<SchemaRef>>>,
    matches: Arc<Mutex<usize>>,
}

impl CaptureState {
    fn new() -> Self {
        Self {
            batches: Arc::new(Mutex::new(Vec::new())),
            node_names: Arc::new(Mutex::new(Vec::new())),
            schema: Arc::new(Mutex::new(None)),
            matches: Arc::new(Mutex::new(0)),
        }
    }

    fn record_match(&self, node_name: &str, schema: SchemaRef) -> Result<usize> {
        if let Ok(mut matches) = self.matches.lock() {
            *matches += 1;
        }
        if let Ok(mut stored) = self.schema.lock() {
            if stored.is_none() {
                *stored = Some(schema);
            }
        }
        let mut names = self
            .node_names
            .lock()
            .map_err(|_| DataFusionError::Execution("capture node names poisoned".to_string()))?;
        let mut batches = self
            .batches
            .lock()
            .map_err(|_| DataFusionError::Execution("capture batches poisoned".to_string()))?;
        names.push(node_name.to_string());
        batches.push(Vec::new());
        Ok(names.len() - 1)
    }

    fn ensure_match_policy(&self, format: InspectNodeOutputFormat) -> Result<()> {
        let count = self
            .matches
            .lock()
            .map_err(|_| DataFusionError::Execution("capture match count poisoned".to_string()))?;
        match *count {
            0 => Err(DataFusionError::Plan(
                "InspectNodeOutput: no matching node found".to_string(),
            )),
            1 => Ok(()),
            count => match format {
                InspectNodeOutputFormat::Raw => Err(DataFusionError::Plan(format!(
                    "InspectNodeOutput: expected one node for RAW output, found {count} (use AS PRETTY)"
                ))),
                InspectNodeOutputFormat::Pretty => Ok(()),
            },
        }
    }

    fn schema(&self) -> Result<SchemaRef> {
        let stored = self
            .schema
            .lock()
            .map_err(|_| DataFusionError::Execution("capture schema poisoned".to_string()))?;
        stored.clone().ok_or_else(|| {
            DataFusionError::Plan("InspectNodeOutput: missing captured schema".to_string())
        })
    }

    fn batches_for(&self, index: usize) -> Vec<RecordBatch> {
        self.batches
            .lock()
            .ok()
            .and_then(|batches| batches.get(index).cloned())
            .unwrap_or_default()
    }

    fn pretty_schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("node", DataType::Utf8, false),
            Field::new("line", DataType::Utf8, false),
        ]))
    }

    fn batches_pretty(&self) -> Result<Vec<RecordBatch>> {
        let (names, batches) = {
            let names = self
                .node_names
                .lock()
                .map_err(|_| DataFusionError::Execution("capture node names poisoned".to_string()))?
                .clone();
            let batches = self
                .batches
                .lock()
                .map_err(|_| DataFusionError::Execution("capture batches poisoned".to_string()))?
                .clone();
            (names, batches)
        };
        let mut out = Vec::new();
        let schema = self.pretty_schema();
        for (node, node_batches) in names.into_iter().zip(batches.into_iter()) {
            let lines = pretty_lines(&node_batches)?;
            let mut node_col = StringBuilder::new();
            let mut line_col = StringBuilder::new();
            if lines.is_empty() {
                node_col.append_value(&node);
                line_col.append_value("<empty>");
            } else {
                for (idx, line) in lines.into_iter().enumerate() {
                    if idx == 0 {
                        node_col.append_value(&node);
                    } else {
                        node_col.append_value("");
                    }
                    line_col.append_value(line);
                }
            }
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(node_col.finish()) as _,
                    Arc::new(line_col.finish()) as _,
                ],
            )?;
            out.push(batch);
        }
        Ok(out)
    }
}

fn wrap_capture_plan(
    plan: Arc<dyn ExecutionPlan>,
    node_name: &str,
    capture: &CaptureState,
) -> Result<Arc<dyn ExecutionPlan>> {
    let children = plan.children();
    let mut children_changed = false;
    let mut new_children = Vec::with_capacity(children.len());
    for child in children {
        let new_child = wrap_capture_plan(child.clone(), node_name, capture)?;
        if !Arc::ptr_eq(child, &new_child) {
            children_changed = true;
        }
        new_children.push(new_child);
    }
    let plan = if children_changed {
        plan.with_new_children(new_children)?
    } else {
        plan
    };

    if plan.name() == node_name {
        let index = capture.record_match(node_name, plan.schema())?;
        return Ok(Arc::new(CaptureExec::new(
            plan,
            capture.batches.clone(),
            index,
        )));
    }

    Ok(plan)
}

#[derive(Debug)]
struct CaptureExec {
    input: Arc<dyn ExecutionPlan>,
    batches: Arc<Mutex<Vec<Vec<RecordBatch>>>>,
    index: usize,
    properties: PlanProperties,
}

impl CaptureExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        batches: Arc<Mutex<Vec<Vec<RecordBatch>>>>,
        index: usize,
    ) -> Self {
        let properties = input.properties().clone();
        Self {
            input,
            batches,
            index,
            properties,
        }
    }
}

impl DisplayAs for CaptureExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CaptureExec")
    }
}

impl ExecutionPlan for CaptureExec {
    fn name(&self) -> &str {
        "CaptureExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "CaptureExec expects exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.batches.clone(),
            self.index,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = self.input.execute(partition, context)?;
        let schema = stream.schema();
        let batches = self.batches.clone();
        let index = self.index;
        let stream = stream.map(move |batch: Result<RecordBatch>| {
            if let Ok(batch) = &batch {
                if let Ok(mut out) = batches.lock() {
                    if let Some(node_batches) = out.get_mut(index) {
                        node_batches.push(batch.clone());
                    }
                }
            }
            batch
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

fn pretty_lines(batches: &[RecordBatch]) -> Result<Vec<String>> {
    let formatted = datafusion::arrow::util::pretty::pretty_format_batches(batches)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
    Ok(formatted
        .to_string()
        .trim()
        .lines()
        .map(|line| line.to_string())
        .collect())
}
