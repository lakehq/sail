use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::scalar_subquery::ScalarSubqueryExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, TableReference, plan_err};
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::{
    AggregateUDF, AggregateUDFImpl, Expr, Extension, LogicalPlan, Projection, ScalarUDF, Subquery,
    TableSource, Union, UserDefinedLogicalNode, Volatility, WindowFunctionDefinition,
};
use datafusion_physical_expr::projection::ProjectionExpr;
use sail_common_datafusion::input_file::is_input_file_metadata_function;
use sail_common_datafusion::udf::StreamUDF;
use sail_data_source::reuse::is_reusable_source;
use sail_function::scalar::math::rand_poisson::RandPoisson;
use sail_function::scalar::math::randn::Randn;
use sail_function::scalar::math::random::Random;
use sail_function::scalar::math::spark_uniform::SparkUniform;
use sail_function::scalar::string::randstr::Randstr;
use sail_logical_plan::map_partitions::MapPartitionsNode;
use sail_logical_plan::monotonic_id::MonotonicIdNode;
use sail_logical_plan::range::{Range, RangeNode};
use sail_logical_plan::repartition::ExplicitRepartitionNode;
use sail_logical_plan::seeded_random::{SeededRandomColumn, SeededRandomNode};
use sail_logical_plan::sort::{RequiredSortNode, SortWithinPartitionsNode};
use sail_logical_plan::spark_partition_id::SparkPartitionIdNode;
use sail_python_udf::udf::pyspark_cogroup_map_udf::PySparkCoGroupMapUDF;
use sail_python_udf::udf::pyspark_group_map_udf::PySparkGroupMapUDF;
use sail_python_udf::udf::pyspark_scalar_iter_udf::PySparkScalarPandasIterUDF;
use sail_python_udf::udf::pyspark_udaf::PySparkGroupAggregateUDF;
use sail_python_udf::udf::pyspark_udf::PySparkUDF;

/// The prefix of the canonical names and qualifiers in a normalized subquery body.
const CANONICAL_PREFIX: &str = "#canonical:";

/// Rewrites a plan so that plans that differ only in their generated column names, in how
/// they name their relations, or in aliases and projections that only rename columns, become
/// equal.
///
/// Every alias and table scan gets a fresh canonical name, subquery aliases are removed, and
/// every column reference is replaced by the column at the same position of the normalized
/// input, as in Spark's canonicalized plans. The names of scanned columns are kept. The result
/// is only used for comparison, and an error means that the plan is not normalized.
struct NameNormalizer {
    next: usize,
}

/// Identifies what a scalar subquery computes, regardless of its output names.
#[derive(PartialEq, Eq, Hash)]
pub struct ScalarSubqueryKey {
    /// The unaliased expressions of the output projections, outermost first, with the
    /// column of a single-column input replaced by a placeholder.
    output: Vec<Expr>,
    /// The plan below the output projections.
    input: Arc<LogicalPlan>,
    /// Whether the generated names of the plan are normalized.
    normalized: bool,
    /// The scanned sources, since `TableScan` equality ignores the source, and scans
    /// of different data can share a table name such as `?table?`.
    sources: Vec<SourceKey>,
    /// The types of the literals, since literal equality ignores some type
    /// properties (e.g. the time zone of a timestamp).
    literal_types: Vec<DataType>,
    /// The output fields without their names, so that the shared body keeps the
    /// output types and metadata of every reference.
    fields: Vec<Field>,
}

/// Identifies the data of a scanned source.
#[derive(PartialEq, Eq, Hash)]
enum SourceKey {
    /// The source by identity.
    Identity(*const ()),
    /// A source that is equivalent to a range, by its range, since the `range` table
    /// function creates a new source for every reference.
    Range(RangeNode),
}

impl SourceKey {
    fn new(source: &Arc<dyn TableSource>) -> Self {
        if let Some(plan) = source.get_logical_plan()
            && let LogicalPlan::Extension(extension) = plan.as_ref()
            && let Some(range) = extension.node.as_any().downcast_ref::<RangeNode>()
        {
            return Self::Range(range.clone());
        }
        Self::Identity(Arc::as_ptr(source).cast::<()>())
    }
}

impl ScalarSubqueryKey {
    /// Returns `None` if the subquery must not be shared.
    pub fn try_new(subquery: &Subquery) -> datafusion_common::Result<Option<Self>> {
        if !subquery.outer_ref_columns.is_empty() {
            return Ok(None);
        }
        let mut sources = vec![];
        let mut literal_types = vec![];
        let mut shareable = true;
        subquery.subquery.apply_with_subqueries(|plan| {
            match plan {
                LogicalPlan::TableScan(scan) => {
                    if !is_reusable_source(&scan.source) {
                        shareable = false;
                        return Ok(TreeNodeRecursion::Stop);
                    }
                    sources.push(SourceKey::new(&scan.source));
                }
                LogicalPlan::Extension(extension)
                    if !is_shareable_extension(extension.node.as_ref()) =>
                {
                    shareable = false;
                    return Ok(TreeNodeRecursion::Stop);
                }
                _ => {}
            }
            plan.apply_expressions(|expr| {
                expr.apply(|expr| {
                    if let Expr::Literal(value, _) = expr {
                        literal_types.push(value.data_type());
                    }
                    let reusable = match expr {
                        Expr::ScalarFunction(function) => {
                            function.func.signature().volatility != Volatility::Volatile
                                || is_reusable_volatile_scalar(&function.func, &function.args)
                        }
                        Expr::AggregateFunction(function) => {
                            function.func.signature().volatility != Volatility::Volatile
                                || is_reusable_volatile_aggregate(function.func.inner().as_ref())
                        }
                        Expr::WindowFunction(function) => match &function.fun {
                            WindowFunctionDefinition::AggregateUDF(udaf) => {
                                udaf.signature().volatility != Volatility::Volatile
                                    || is_reusable_volatile_aggregate(udaf.inner().as_ref())
                            }
                            WindowFunctionDefinition::WindowUDF(udwf) => {
                                udwf.signature().volatility != Volatility::Volatile
                            }
                        },
                        _ => true,
                    };
                    if !reusable {
                        shareable = false;
                        return Ok(TreeNodeRecursion::Stop);
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
            })
        })?;
        if !shareable {
            return Ok(None);
        }
        // Each reference to a table, view or CTE gets fresh column names. A body that
        // cannot be normalized is compared as it is.
        let normalized = normalize_names(&subquery.subquery);
        let is_normalized = normalized.is_some();
        let body = normalized
            .map(Arc::new)
            .unwrap_or_else(|| Arc::clone(&subquery.subquery));
        // Skip the output names, which differ between the references to a CTE and
        // between repeated subqueries, including the renamed column of a single-column
        // input (e.g. under a cast added by type coercion).
        let mut input = &body;
        let mut output = vec![];
        loop {
            match &**input {
                LogicalPlan::SubqueryAlias(alias) => input = &alias.input,
                LogicalPlan::Projection(projection) => {
                    let [expr] = projection.expr.as_slice() else {
                        break;
                    };
                    let mut expr = expr.clone().unalias_nested().data;
                    input = &projection.input;
                    let single_column = input.schema().fields().len() == 1;
                    if single_column {
                        // A plain column only renames the single input column.
                        if matches!(expr, Expr::Column(_)) {
                            continue;
                        }
                        expr = expr
                            .transform(|expr| {
                                Ok(match expr {
                                    Expr::Column(_) => Transformed::yes(Expr::Column(
                                        datafusion_common::Column::new_unqualified("#input"),
                                    )),
                                    _ => Transformed::no(expr),
                                })
                            })
                            .data()?;
                    }
                    output.push(expr);
                    if !single_column {
                        break;
                    }
                }
                _ => break,
            }
        }
        let fields = subquery
            .subquery
            .schema()
            .fields()
            .iter()
            .map(|field| (**field).clone().with_name(""))
            .collect();
        Ok(Some(Self {
            output,
            input: Arc::clone(input),
            normalized: is_normalized,
            sources,
            literal_types,
            fields,
        }))
    }
}

impl NameNormalizer {
    fn fresh(&mut self) -> String {
        let name = format!("{CANONICAL_PREFIX}{}", self.next);
        self.next += 1;
        name
    }

    fn plan(&mut self, plan: &LogicalPlan) -> datafusion_common::Result<LogicalPlan> {
        if let LogicalPlan::TableScan(scan) = plan {
            let table_name = TableReference::bare(self.fresh());
            let mut scan = scan.clone();
            let filters = std::mem::take(&mut scan.filters);
            scan.filters = filters
                .into_iter()
                .map(|filter| {
                    filter
                        .transform(|expr| {
                            Ok(match expr {
                                Expr::Column(column) if column.relation.is_some() => {
                                    if column.relation.as_ref() != Some(&scan.table_name) {
                                        return plan_err!("unexpected filter qualifier");
                                    }
                                    Transformed::yes(Expr::Column(Column::new(
                                        Some(table_name.clone()),
                                        column.name,
                                    )))
                                }
                                _ => Transformed::no(expr),
                            })
                        })
                        .data()
                })
                .collect::<datafusion_common::Result<_>>()?;
            let schema = scan.projected_schema.as_ref().clone();
            scan.projected_schema = Arc::new(schema.replace_qualifier(table_name.clone()));
            scan.table_name = table_name;
            return Ok(LogicalPlan::TableScan(scan));
        }
        match plan {
            // Unnest refers to its columns by name outside its expressions.
            LogicalPlan::Unnest(_) => return plan_err!("unexpected unnest"),
            LogicalPlan::Subquery(subquery) if !subquery.outer_ref_columns.is_empty() => {
                return plan_err!("unexpected correlated subquery");
            }
            _ => {}
        }
        let inputs = plan.inputs();
        let new_inputs = inputs
            .iter()
            .map(|input| self.plan(input))
            .collect::<datafusion_common::Result<Vec<_>>>()?;
        for (input, new_input) in inputs.iter().zip(&new_inputs) {
            if input.schema().fields().len() != new_input.schema().fields().len() {
                return plan_err!("unexpected normalized schema");
            }
        }
        if let LogicalPlan::Extension(extension) = plan
            && let Some(node) = self.extension(extension.node.as_ref(), &new_inputs)?
        {
            return Ok(LogicalPlan::Extension(Extension { node }));
        }
        // Replaces a column with the column at the same position of the normalized inputs.
        // The column must match exactly one input column, as DataFusion resolves it.
        let resolve = |column: &Column| -> datafusion_common::Result<Column> {
            let mut matches = inputs
                .iter()
                .zip(&new_inputs)
                .flat_map(|(input, new_input)| {
                    input
                        .schema()
                        .iter()
                        .zip(new_input.schema().iter())
                        .filter(|((qualifier, field), _)| {
                            field.name() == &column.name
                                && match (&column.relation, qualifier) {
                                    (Some(relation), Some(qualifier)) => {
                                        relation.resolved_eq(qualifier)
                                    }
                                    (Some(_), None) => false,
                                    (None, _) => true,
                                }
                        })
                        .map(|(_, (qualifier, field))| {
                            Column::new(qualifier.cloned(), field.name())
                        })
                });
            match (matches.next(), matches.next()) {
                (Some(target), None) => Ok(target),
                _ => plan_err!("unresolved or ambiguous column"),
            }
        };
        if let LogicalPlan::Projection(projection) = plan
            && is_renaming_projection(projection)
            && let [input] = new_inputs.as_slice()
        {
            return Ok(input.clone());
        }
        let exprs = plan
            .expressions()
            .into_iter()
            .map(|expr| {
                expr.transform(|expr| {
                    Ok(match expr {
                        Expr::Column(column) => Transformed::yes(Expr::Column(resolve(&column)?)),
                        Expr::Alias(mut alias) => {
                            // The qualifier of an alias only names the output column, which
                            // the parent resolves by position.
                            alias.relation = None;
                            alias.name = self.fresh();
                            Transformed::yes(Expr::Alias(alias))
                        }
                        Expr::ScalarSubquery(subquery) => {
                            Transformed::yes(Expr::ScalarSubquery(self.subquery(subquery)?))
                        }
                        Expr::Exists(mut exists) => {
                            exists.subquery = self.subquery(exists.subquery)?;
                            Transformed::yes(Expr::Exists(exists))
                        }
                        Expr::InSubquery(mut in_subquery) => {
                            in_subquery.subquery = self.subquery(in_subquery.subquery)?;
                            Transformed::yes(Expr::InSubquery(in_subquery))
                        }
                        Expr::SetComparison(mut comparison) => {
                            comparison.subquery = self.subquery(comparison.subquery)?;
                            Transformed::yes(Expr::SetComparison(comparison))
                        }
                        Expr::OuterReferenceColumn(_, _) => {
                            return plan_err!("unexpected outer reference");
                        }
                        // The calls of a nondeterministic Python UDF are only equal to
                        // themselves, but equal subqueries are shared regardless of determinism.
                        Expr::ScalarFunction(mut function) => {
                            match shared_python_scalar_udf(&function.func) {
                                Some(func) => {
                                    function.func = Arc::new(func);
                                    Transformed::yes(Expr::ScalarFunction(function))
                                }
                                None => Transformed::no(Expr::ScalarFunction(function)),
                            }
                        }
                        Expr::AggregateFunction(mut function) => {
                            match shared_python_aggregate_udf(&function.func) {
                                Some(func) => {
                                    function.func = Arc::new(func);
                                    Transformed::yes(Expr::AggregateFunction(function))
                                }
                                None => Transformed::no(Expr::AggregateFunction(function)),
                            }
                        }
                        Expr::WindowFunction(mut function) => {
                            let shared = match &function.fun {
                                WindowFunctionDefinition::AggregateUDF(udaf) => {
                                    shared_python_aggregate_udf(udaf)
                                }
                                WindowFunctionDefinition::WindowUDF(_) => None,
                            };
                            match shared {
                                Some(func) => {
                                    function.fun =
                                        WindowFunctionDefinition::AggregateUDF(Arc::new(func));
                                    Transformed::yes(Expr::WindowFunction(function))
                                }
                                None => Transformed::no(Expr::WindowFunction(function)),
                            }
                        }
                        _ => Transformed::no(expr),
                    })
                })
                .data()
            })
            .collect::<datafusion_common::Result<Vec<_>>>()?;
        let plan = plan.with_new_exprs(exprs, new_inputs)?;
        Ok(match plan {
            // A subquery alias only renames the qualifier of its input columns, which the
            // parent resolves by position.
            LogicalPlan::SubqueryAlias(alias) => Arc::unwrap_or_clone(alias.input),
            // The schema of a union is kept when its inputs are rewritten.
            LogicalPlan::Union(union) => LogicalPlan::Union(Union::try_new(union.inputs)?),
            // A seeded random node names its output columns outside its expressions.
            LogicalPlan::Extension(extension) => {
                match extension.node.as_any().downcast_ref::<SeededRandomNode>() {
                    Some(random) => {
                        let columns = random
                            .columns()
                            .iter()
                            .map(|column| SeededRandomColumn {
                                name: self.fresh(),
                                ..column.clone()
                            })
                            .collect();
                        LogicalPlan::Extension(Extension {
                            node: Arc::new(SeededRandomNode::try_new(
                                Arc::clone(random.input()),
                                columns,
                            )?),
                        })
                    }
                    None => LogicalPlan::Extension(extension),
                }
            }
            plan => plan,
        })
    }

    /// Rebuilds an extension node that stores generated output names outside its
    /// expressions, with canonical names. Returns `None` for other nodes.
    fn extension(
        &mut self,
        node: &dyn UserDefinedLogicalNode,
        inputs: &[LogicalPlan],
    ) -> datafusion_common::Result<Option<Arc<dyn UserDefinedLogicalNode>>> {
        let node = node.as_any();
        if let Some(range) = node.downcast_ref::<RangeNode>() {
            let &Range { start, end, step } = range.range();
            let range = RangeNode::try_new(self.fresh(), start, end, step, range.num_partitions())?;
            return Ok(Some(Arc::new(range)));
        }
        let [input] = inputs else {
            return Ok(None);
        };
        let input = || Arc::new(input.clone());
        Ok(Some(if node.is::<MonotonicIdNode>() {
            Arc::new(MonotonicIdNode::try_new(input(), self.fresh())?)
        } else if node.is::<SparkPartitionIdNode>() {
            Arc::new(SparkPartitionIdNode::try_new(input(), self.fresh())?)
        } else if let Some(map) = node.downcast_ref::<MapPartitionsNode>() {
            let width = map.udf().output_schema().fields().len();
            let names = (0..width).map(|_| self.fresh()).collect();
            Arc::new(MapPartitionsNode::try_new(
                input(),
                names,
                vec![None; width],
                self.stream_udf(map.udf())?,
            )?)
        } else {
            return Ok(None);
        }))
    }

    /// Returns the UDF with canonical output names, since a scalar iterator UDF names its
    /// output after the generated names of its input.
    fn stream_udf(
        &mut self,
        udf: &Arc<dyn StreamUDF>,
    ) -> datafusion_common::Result<Arc<dyn StreamUDF>> {
        let any: &dyn Any = udf.as_ref();
        let Some(iter) = any.downcast_ref::<PySparkScalarPandasIterUDF>() else {
            return Ok(Arc::clone(udf));
        };
        let schema = iter.output_schema();
        let fields = schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone().with_name(self.fresh()))
            .collect::<Vec<_>>();
        Ok(Arc::new(PySparkScalarPandasIterUDF::try_new(
            iter.name().to_string(),
            iter.payload().to_vec(),
            Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
            Arc::clone(iter.config()),
        )?))
    }

    fn subquery(&mut self, subquery: Subquery) -> datafusion_common::Result<Subquery> {
        // The outer references of nested correlated subqueries are not normalized.
        if !subquery.outer_ref_columns.is_empty() {
            return plan_err!("unexpected correlated subquery");
        }
        Ok(Subquery {
            subquery: Arc::new(self.plan(&subquery.subquery)?),
            ..subquery
        })
    }
}

/// Returns whether the extension node holds no source or state, and its equality compares
/// everything that determines its output. Such nodes are shared even when their output is
/// nondeterministic (e.g. monotonically increasing IDs or Python UDFs), as Spark reuses
/// equal subqueries regardless of determinism. Other extension nodes may hold sources or
/// state (e.g. checkpoints or streams).
fn is_shareable_extension(node: &dyn UserDefinedLogicalNode) -> bool {
    let node = node.as_any();
    node.is::<RangeNode>()
        || node.is::<RequiredSortNode>()
        || node.is::<SortWithinPartitionsNode>()
        || node.is::<ExplicitRepartitionNode>()
        || node.is::<MonotonicIdNode>()
        || node.is::<SparkPartitionIdNode>()
        || node.is::<SeededRandomNode>()
        || node.is::<MapPartitionsNode>()
}

/// Returns whether calls to the volatile scalar function compute the same result for the
/// same input, as Spark assumes when it reuses equal subqueries. Python UDFs are reused even
/// when they are nondeterministic, and random functions only with an explicit seed (where a
/// null seed is zero), since Spark gives each random function call without a seed its own
/// seed. Input file functions are reused as well.
fn is_reusable_volatile_scalar(function: &ScalarUDF, args: &[Expr]) -> bool {
    if is_input_file_metadata_function(function) {
        return true;
    }
    let func = function.inner();
    let seeded = |position: usize| matches!(args.get(position), Some(Expr::Literal(_, _)));
    func.is::<PySparkUDF>()
        || func.is::<PySparkCoGroupMapUDF>()
        || ((func.is::<Random>() || func.is::<Randn>()) && seeded(0))
        || ((func.is::<Randstr>() || func.is::<RandPoisson>()) && seeded(1))
        || (func.is::<SparkUniform>() && seeded(2))
}

/// Returns whether calls to the volatile aggregate function may be reused, which holds for
/// Python UDFs as for [`is_reusable_volatile_scalar`].
fn is_reusable_volatile_aggregate(func: &dyn AggregateUDFImpl) -> bool {
    func.is::<PySparkGroupAggregateUDF>() || func.is::<PySparkGroupMapUDF>()
}

/// Returns the nondeterministic Python UDF with calls that are equal to the calls of all other
/// equal UDFs, or `None` for other functions.
fn shared_python_scalar_udf(func: &ScalarUDF) -> Option<ScalarUDF> {
    let inner = func.inner();
    if let Some(udf) = inner.downcast_ref::<PySparkUDF>()
        && !udf.deterministic()
    {
        Some(ScalarUDF::new_from_impl(udf.to_shared()))
    } else if let Some(udf) = inner.downcast_ref::<PySparkCoGroupMapUDF>()
        && !udf.deterministic()
    {
        Some(ScalarUDF::new_from_impl(udf.to_shared()))
    } else {
        None
    }
}

/// Returns the nondeterministic Python aggregate UDF with calls that are equal to the calls of
/// all other equal UDFs, or `None` for other functions.
fn shared_python_aggregate_udf(func: &AggregateUDF) -> Option<AggregateUDF> {
    let inner = func.inner();
    if let Some(udf) = inner.downcast_ref::<PySparkGroupAggregateUDF>()
        && !udf.deterministic()
    {
        Some(AggregateUDF::new_from_impl(udf.to_shared()))
    } else if let Some(udf) = inner.downcast_ref::<PySparkGroupMapUDF>()
        && !udf.deterministic()
    {
        Some(AggregateUDF::new_from_impl(udf.to_shared()))
    } else {
        None
    }
}

/// Returns whether the projection only renames the columns of its input, in order.
fn is_renaming_projection(projection: &Projection) -> bool {
    let schema = projection.input.schema();
    projection.expr.len() == schema.fields().len()
        && projection.expr.iter().enumerate().all(|(i, expr)| {
            // An alias with metadata changes the field, so it does not only rename it.
            let expr = match expr {
                Expr::Alias(alias) if alias.metadata.is_none() => alias.expr.as_ref(),
                Expr::Alias(_) => return false,
                expr => expr,
            };
            matches!(expr, Expr::Column(column) if schema.maybe_index_of_column(column) == Some(i))
        })
}

/// Returns the normalized plan, or `None` if the plan cannot be normalized.
fn normalize_names(plan: &LogicalPlan) -> Option<LogicalPlan> {
    let mut found_canonical = false;
    plan.apply_with_subqueries(|node| {
        found_canonical |= node.schema().iter().any(|(qualifier, field)| {
            field.name().starts_with(CANONICAL_PREFIX)
                || qualifier.is_some_and(|q| q.table().starts_with(CANONICAL_PREFIX))
        });
        Ok(TreeNodeRecursion::Continue)
    })
    .ok()?;
    if found_canonical {
        return None;
    }
    NameNormalizer { next: 0 }.plan(plan).ok()
}

/// Makes equivalent uncorrelated scalar subqueries share one body.
///
/// DataFusion plans and executes each distinct scalar subquery once, using exact
/// plan equality. Sail gives each subquery fresh output names, so a CTE referenced
/// by several scalar subqueries, or the same scalar subquery written several times,
/// would otherwise be executed once per occurrence.
#[allow(clippy::allow_attributes, clippy::mutable_key_type)]
pub fn share_equivalent_scalar_subqueries(
    plan: LogicalPlan,
) -> datafusion_common::Result<LogicalPlan> {
    // This scan avoids plan transformation for queries that cannot share a subquery.
    let mut candidates = 0;
    plan.apply_with_subqueries(|node| {
        node.apply_expressions(|expr| {
            expr.apply(|expr| {
                if let Expr::ScalarSubquery(subquery) = expr
                    && subquery.outer_ref_columns.is_empty()
                {
                    candidates += 1;
                }
                Ok(if candidates < 2 {
                    TreeNodeRecursion::Continue
                } else {
                    TreeNodeRecursion::Stop
                })
            })
        })
    })?;
    if candidates < 2 {
        return Ok(plan);
    }
    let mut bodies = HashMap::new();
    plan.transform_up_with_subqueries(|node| {
        let names = NamePreserver::new(&node);
        node.map_expressions(|expr| {
            // A grouping set cannot be aliased to preserve its name.
            if matches!(expr, Expr::GroupingSet(_)) || !expr.contains_scalar_subquery() {
                return Ok(Transformed::no(expr));
            }
            let name = names.save(&expr);
            let expr = expr.transform_up(|expr| {
                let Expr::ScalarSubquery(mut subquery) = expr else {
                    return Ok(Transformed::no(expr));
                };
                let Some(key) = ScalarSubqueryKey::try_new(&subquery)? else {
                    return Ok(Transformed::no(Expr::ScalarSubquery(subquery)));
                };
                let body = bodies
                    .entry(key)
                    .or_insert_with(|| Arc::clone(&subquery.subquery));
                if Arc::ptr_eq(body, &subquery.subquery) {
                    return Ok(Transformed::no(Expr::ScalarSubquery(subquery)));
                }
                subquery.subquery = Arc::clone(body);
                Ok(Transformed::yes(Expr::ScalarSubquery(subquery)))
            })?;
            Ok(expr.update_data(|expr| name.restore(expr)))
        })
    })
    .data()
}

#[expect(deprecated)]
pub fn ensure_scalar_subquery_nullability(
    plan: Arc<dyn ExecutionPlan>,
) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
    plan.transform_up(|plan| {
        let Some(projection) = plan.downcast_ref::<ProjectionExec>() else {
            return Ok(Transformed::no(plan));
        };

        let mut changed = false;
        let expressions = projection
            .expr()
            .iter()
            .map(|projection_expr| {
                let transformed = Arc::clone(&projection_expr.expr).transform_up(|expression| {
                    let Some(scalar) = expression.downcast_ref::<ScalarSubqueryExpr>() else {
                        return Ok(Transformed::no(expression));
                    };
                    if scalar.nullable() {
                        return Ok(Transformed::no(expression));
                    }
                    Ok(Transformed::yes(Arc::new(ScalarSubqueryExpr::new(
                        scalar.data_type().clone(),
                        true,
                        scalar.index(),
                        scalar.results().clone(),
                    ))
                        as Arc<dyn PhysicalExpr>))
                })?;
                changed |= transformed.transformed;
                Ok(ProjectionExpr::new(
                    transformed.data,
                    projection_expr.alias.clone(),
                ))
            })
            .collect::<datafusion_common::Result<Vec<_>>>()?;

        if changed {
            Ok(Transformed::yes(Arc::new(ProjectionExec::try_new(
                expressions,
                Arc::clone(projection.input()),
            )?) as Arc<dyn ExecutionPlan>))
        } else {
            Ok(Transformed::no(plan))
        }
    })
    .data()
}
