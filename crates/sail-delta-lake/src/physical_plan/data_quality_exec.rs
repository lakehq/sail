use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::catalog::Session;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{ColumnarValue, ExprSchemable};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result, ToDFSchema};
use datafusion_physical_expr::{Distribution, EquivalenceProperties};
use futures::TryStreamExt;
use sail_plan::resolver::PlanResolver;

use crate::schema::{
    extract_invariants, invariant_path_support, DeltaInvariant, InvariantPathSupport,
};
use crate::spec::{Action, ColumnMappingMode, StructType, TableFeature};
use crate::table::DeltaTable;

#[derive(Clone)]
pub struct CompiledInvariant {
    field_path: String,
    sql: String,
    expr: Arc<dyn PhysicalExpr>,
}

impl fmt::Debug for CompiledInvariant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompiledInvariant")
            .field("field_path", &self.field_path)
            .field("sql", &self.sql)
            .finish()
    }
}

impl CompiledInvariant {
    pub fn new(field_path: String, sql: String, expr: Arc<dyn PhysicalExpr>) -> Self {
        Self {
            field_path,
            sql,
            expr,
        }
    }

    pub fn field_path(&self) -> &str {
        &self.field_path
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }

    pub fn expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }
}

/// Check whether the table protocol supports column invariants.
///
/// Delta Lake's `invariants` is a **legacy writer feature** with `minWriterVersion = 2`.
/// Legacy protocols (writer versions 2–6 without explicit `writerFeatures`) implicitly
/// support invariants.  Protocol v7+ must explicitly list the feature in `writerFeatures`.
fn protocol_supports_invariants(protocol: &crate::spec::Protocol) -> bool {
    let v = protocol.min_writer_version();
    if v >= 7 {
        protocol.has_writer_feature(&TableFeature::Invariants)
    } else {
        v >= 2
    }
}

/// Compile invariants from the table metadata on the driver side.
///
/// This function is called during physical plan construction (driver),
/// NOT during execution (worker). All SQL parsing and expression resolution
/// happens here so workers only need to evaluate pre-compiled `PhysicalExpr`.
pub fn compile_data_quality_invariants(
    session: &dyn Session,
    table: Option<&DeltaTable>,
    schema_actions: &[Action],
    logical_schema: &SchemaRef,
    metadata_configuration: &HashMap<String, String>,
) -> Result<Vec<CompiledInvariant>> {
    // Determine the schema that carries invariant metadata.
    // Prefer the pending metadata action (schema evolution) over the existing snapshot.
    let pending_metadata = schema_actions.iter().rev().find_map(|action| match action {
        Action::Metadata(metadata) => Some(metadata.clone()),
        _ => None,
    });

    let invariant_schema = if let Some(ref metadata) = pending_metadata {
        metadata
            .parse_schema()
            .map_err(|e| DataFusionError::External(Box::new(e)))?
    } else if let Some(table) = table {
        table
            .snapshot()
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .metadata()
            .parse_schema()
            .map_err(|e| DataFusionError::External(Box::new(e)))?
    } else {
        let effective_mode = metadata_configuration
            .get("delta.columnMapping.mode")
            .and_then(|v| ColumnMappingMode::try_from(v.as_str()).ok())
            .unwrap_or_default();
        let kernel_schema = StructType::try_from(logical_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        if matches!(effective_mode, ColumnMappingMode::None) {
            kernel_schema
        } else {
            crate::schema::annotate_for_column_mapping(&kernel_schema)
        }
    };

    let invariants = extract_invariants(&invariant_schema)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    if invariants.is_empty() {
        return Ok(Vec::new());
    }

    // For existing tables whose protocol does not support invariants:
    // enforce them anyway to prevent silent data corruption, but reject schema evolution
    // that introduces new invariants without a protocol upgrade.
    if let Some(table) = table {
        let snapshot = table
            .snapshot()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        if !protocol_supports_invariants(snapshot.protocol()) && pending_metadata.is_some() {
            return Err(DataFusionError::Plan(
                "Schema evolution introduces delta.invariants but the table's protocol \
                 (writer version < 2) does not support invariants. \
                 Upgrade the table protocol before adding invariant constraints."
                    .to_string(),
            ));
        }
    }

    compile_invariants_inner(session, logical_schema, &invariant_schema)
}

fn compile_invariants_inner(
    session: &dyn Session,
    logical_schema: &SchemaRef,
    invariant_schema: &StructType,
) -> Result<Vec<CompiledInvariant>> {
    let invariants =
        extract_invariants(invariant_schema).map_err(|e| DataFusionError::External(Box::new(e)))?;
    if invariants.is_empty() {
        return Ok(Vec::new());
    }

    let df_schema = logical_schema
        .clone()
        .to_dfschema()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let mut compiled = Vec::with_capacity(invariants.len());
    for DeltaInvariant { field_path, sql } in invariants {
        if matches!(
            invariant_path_support(invariant_schema, &field_path)
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
            InvariantPathSupport::UnsupportedCollection
        ) {
            return Err(DataFusionError::Plan(format!(
                "delta.invariants on array/map nested fields is not yet supported: {field_path}"
            )));
        }

        let expr = PlanResolver::resolve_row_local_sql_expression_with_session(
            session,
            &sql,
            logical_schema.as_ref(),
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let expr_type = expr
            .get_type(&df_schema)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        if expr_type != DataType::Boolean {
            return Err(DataFusionError::Plan(format!(
                "delta.invariants expression must be boolean for field '{field_path}': {sql}"
            )));
        }
        let physical = session.create_physical_expr(expr, &df_schema)?;
        compiled.push(CompiledInvariant::new(field_path, sql, physical));
    }
    Ok(compiled)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataQualityPolicy {
    Strict,
}

#[derive(Debug, Clone)]
pub struct DeltaDataQualityExec {
    input: Arc<dyn ExecutionPlan>,
    invariants: Vec<CompiledInvariant>,
    policy: DataQualityPolicy,
    cache: Arc<PlanProperties>,
}

impl DeltaDataQualityExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        invariants: Vec<CompiledInvariant>,
        policy: DataQualityPolicy,
    ) -> Result<Self> {
        let schema = input.schema();
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            input.output_partitioning().clone(),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            invariants,
            policy,
            cache,
        })
    }

    fn apply_policy(&self, batch: RecordBatch) -> Result<Option<RecordBatch>> {
        if self.invariants.is_empty() {
            return Ok(Some(batch));
        }

        for invariant in &self.invariants {
            let mask = Self::evaluate_invariant_mask(&batch, invariant)?;
            let has_invalid = mask.iter().any(|value| !matches!(value, Some(true)));

            if has_invalid && matches!(self.policy, DataQualityPolicy::Strict) {
                let violation_mask = BooleanArray::from(
                    mask.iter()
                        .map(|value| !matches!(value, Some(true)))
                        .collect::<Vec<_>>(),
                );
                let row = Self::first_violating_row(&batch, &violation_mask)?;
                return Err(DataFusionError::Execution(format!(
                    "Delta invariant violated for field '{}': {}. First violating row: {}",
                    invariant.field_path(),
                    invariant.sql(),
                    row
                )));
            }
        }

        Ok(Some(batch))
    }

    fn evaluate_invariant_mask(
        batch: &RecordBatch,
        invariant: &CompiledInvariant,
    ) -> Result<BooleanArray> {
        let result = invariant.expr.evaluate(batch)?;
        Self::invariant_result_to_array(result, batch.num_rows())
    }

    fn invariant_result_to_array(result: ColumnarValue, num_rows: usize) -> Result<BooleanArray> {
        match result {
            ColumnarValue::Scalar(datafusion_common::ScalarValue::Boolean(value)) => {
                Ok(BooleanArray::from(vec![value; num_rows]))
            }
            ColumnarValue::Array(array) => array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .cloned()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "delta.invariants evaluation did not produce a boolean array".to_string(),
                    )
                }),
            other => Err(DataFusionError::Execution(format!(
                "delta.invariants evaluation did not produce a boolean result: {other:?}"
            ))),
        }
    }

    fn first_violating_row(batch: &RecordBatch, violation_mask: &BooleanArray) -> Result<String> {
        let invalid = filter_record_batch(batch, violation_mask)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        if invalid.num_rows() == 0 {
            return Ok("{}".to_string());
        }
        Self::format_row(&invalid, 0)
    }

    fn format_row(batch: &RecordBatch, row_idx: usize) -> Result<String> {
        let values = batch
            .schema()
            .fields()
            .iter()
            .zip(batch.columns())
            .map(|(field, column)| {
                let value = array_value_to_string(column.as_ref(), row_idx)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                Ok(format!("{}={value}", field.name()))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(format!("{{{}}}", values.join(", ")))
    }
}

#[async_trait]
impl ExecutionPlan for DeltaDataQualityExec {
    fn name(&self) -> &'static str {
        "DeltaDataQualityExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("DeltaDataQualityExec requires exactly one child");
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            self.invariants.clone(),
            self.policy,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, Arc::clone(&context))?;
        let schema = self.schema();
        let exec = self.clone();

        let stream = input_stream.try_filter_map(move |batch| {
            let exec = exec.clone();
            async move {
                if batch.num_rows() == 0 {
                    return Ok(None);
                }
                exec.apply_policy(batch)
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

impl DisplayAs for DeltaDataQualityExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let fields = self
            .invariants
            .iter()
            .map(|invariant| invariant.field_path())
            .collect::<Vec<_>>()
            .join(", ");
        let policy = match self.policy {
            DataQualityPolicy::Strict => "strict",
        };
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "DeltaDataQualityExec: [{fields}], policy={policy}")
            }
        }
    }
}
