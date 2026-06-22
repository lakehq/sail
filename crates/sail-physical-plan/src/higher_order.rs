use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::Result;
use datafusion::physical_expr::expressions::LambdaExpr;
use datafusion::physical_expr::{HigherOrderFunctionExpr, PhysicalExpr};
use datafusion::physical_plan::ColumnarValue;
use datafusion_common::arrow::datatypes::FieldRef;
use datafusion_common::datatype::FieldExt;
use datafusion_common::{plan_err, DataFusionError};
use datafusion_expr::{LambdaParametersProgress, ValueOrLambda};

/// A distribution-friendly wrapper around DataFusion's `HigherOrderFunctionExpr`.
///
/// DataFusion's higher-order function physical expression can only be built via
/// `HigherOrderFunctionExpr::try_new_with_schema`, which needs the input schema,
/// and neither `PhysicalExtensionCodec::try_encode_expr` nor `try_decode_expr`
/// exposes that schema. This wrapper carries the input schema alongside the inner
/// expression (captured at planning time) so the serialization codec can rebuild
/// the inner expression on a remote worker. All evaluation delegates to the inner
/// expression, so the wrapper is behaviorally transparent.
///
/// Mirrors the pattern of
/// [`sail_common_datafusion::schema_evolution::SchemaEvolutionCastColumnExpr`].
#[derive(Debug, Clone)]
pub struct DistributedHigherOrderExpr {
    /// The wrapped `HigherOrderFunctionExpr`.
    inner: Arc<dyn PhysicalExpr>,
    /// The input schema the inner expression was planned against. Serialized so
    /// the inner expression can be rebuilt during decoding.
    input_schema: SchemaRef,
}

impl DistributedHigherOrderExpr {
    pub fn new(inner: Arc<dyn PhysicalExpr>, input_schema: SchemaRef) -> Self {
        Self {
            inner,
            input_schema,
        }
    }

    pub fn inner(&self) -> &Arc<dyn PhysicalExpr> {
        &self.inner
    }

    pub fn input_schema(&self) -> &SchemaRef {
        &self.input_schema
    }
}

impl PartialEq for DistributedHigherOrderExpr {
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner) && self.input_schema.eq(&other.input_schema)
    }
}

impl Eq for DistributedHigherOrderExpr {}

impl Hash for DistributedHigherOrderExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
        self.input_schema.hash(state);
    }
}

impl std::fmt::Display for DistributedHigherOrderExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.inner, f)
    }
}

impl PhysicalExpr for DistributedHigherOrderExpr {
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.inner.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.inner.nullable(input_schema)
    }

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        self.inner.return_field(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        self.inner.evaluate(batch)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.inner.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        // The cached `input_schema` is preserved: `with_new_children` replaces
        // child expressions but not the schema they are evaluated against. The
        // wrapping rule runs late (just before the final sanity check), so no
        // later rule re-roots these children against a different schema.
        let inner = Arc::clone(&self.inner).with_new_children(children)?;
        Ok(Arc::new(Self::new(inner, Arc::clone(&self.input_schema))))
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt_sql(f)
    }
}

/// Wraps every `HigherOrderFunctionExpr` in `expr` with a
/// [`DistributedHigherOrderExpr`] that carries the schema each one was planned
/// against, so the serialization codec can rebuild them on a remote worker.
///
/// `schema` is the schema the top-level expression is evaluated against. When a
/// higher-order function contains a lambda, the lambda body is evaluated against
/// the schema extended with the lambda's parameter fields (matching the physical
/// planner), so any higher-order function nested inside a lambda body is wrapped
/// with that extended schema.
pub fn wrap_distributed_higher_order(
    expr: Arc<dyn PhysicalExpr>,
    schema: &SchemaRef,
) -> Result<Arc<dyn PhysicalExpr>> {
    if let Some(hof) = expr.downcast_ref::<HigherOrderFunctionExpr>() {
        let args = hof.args();
        let param_sets = lambda_parameter_fields(hof, schema)?;
        let mut lambda_index = 0;
        let mut new_args: Vec<Arc<dyn PhysicalExpr>> = Vec::with_capacity(args.len());
        for arg in args {
            if let Some(lambda) = arg.downcast_ref::<LambdaExpr>() {
                let params = param_sets.get(lambda_index).ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "missing lambda parameter fields for `{}`",
                        hof.name()
                    ))
                })?;
                lambda_index += 1;
                let extended = extend_schema(schema, lambda.params(), params);
                let body = wrap_distributed_higher_order(Arc::clone(lambda.body()), &extended)?;
                new_args.push(Arc::new(LambdaExpr::try_new(
                    lambda.params().to_vec(),
                    body,
                )?));
            } else {
                new_args.push(wrap_distributed_higher_order(Arc::clone(arg), schema)?);
            }
        }
        let inner = Arc::clone(&expr).with_new_children(new_args)?;
        Ok(Arc::new(DistributedHigherOrderExpr::new(
            inner,
            Arc::clone(schema),
        )))
    } else {
        let schema = Arc::clone(schema);
        expr.transform_down(|node| {
            if node.is::<HigherOrderFunctionExpr>() {
                // The recursive call wraps the entire higher-order subtree
                // (including nested lambdas), so stop descending into it.
                let wrapped = wrap_distributed_higher_order(node, &schema)?;
                Ok(Transformed::new(wrapped, true, TreeNodeRecursion::Jump))
            } else {
                Ok(Transformed::no(node))
            }
        })
        .data()
    }
}

/// Returns the lambda parameter fields for each lambda argument of `hof`, using
/// the higher-order UDF's `lambda_parameters` (the same hook the physical planner
/// uses), so the schema extension matches planning exactly.
fn lambda_parameter_fields(
    hof: &HigherOrderFunctionExpr,
    schema: &Schema,
) -> Result<Vec<Vec<FieldRef>>> {
    let fields = hof
        .args()
        .iter()
        .map(|arg| {
            if arg.is::<LambdaExpr>() {
                Ok(ValueOrLambda::Lambda(None))
            } else {
                Ok(ValueOrLambda::Value(arg.return_field(schema)?))
            }
        })
        .collect::<Result<Vec<_>>>()?;
    match hof.fun().lambda_parameters(0, &fields)? {
        LambdaParametersProgress::Complete(sets) => Ok(sets),
        LambdaParametersProgress::Partial(_) => {
            plan_err!("`{}` returned partial lambda parameters", hof.name())
        }
    }
}

/// Builds the schema a lambda body is evaluated against: the base schema followed
/// by the lambda's declared parameter fields (renamed to the parameter names),
/// mirroring the physical planner's lambda schema construction.
fn extend_schema(base: &Schema, params: &[String], param_fields: &[FieldRef]) -> SchemaRef {
    let mut fields: Vec<FieldRef> = base.fields().iter().map(Arc::clone).collect();
    for (name, field) in params.iter().zip(param_fields) {
        fields.push(Arc::clone(field).renamed(name.as_str()));
    }
    Arc::new(Schema::new(fields))
}
