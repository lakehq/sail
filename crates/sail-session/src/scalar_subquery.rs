use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::scalar_subquery::ScalarSubqueryExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, TableReference, plan_err};
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::{Expr, LogicalPlan, Projection, Subquery, Union, Volatility};
use datafusion_physical_expr::projection::ProjectionExpr;
use sail_data_source::reuse::is_reusable_source;

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
    /// The scanned sources by identity, since `TableScan` equality ignores the
    /// source, and scans of different data can share a table name such as `?table?`.
    sources: Vec<*const ()>,
    /// The types of the literals, since literal equality ignores some type
    /// properties (e.g. the time zone of a timestamp).
    literal_types: Vec<DataType>,
    /// The output fields without their names, so that the shared body keeps the
    /// output types and metadata of every reference.
    fields: Vec<Field>,
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
                    sources.push(Arc::as_ptr(&scan.source).cast::<()>());
                }
                // Extension node equality may ignore sources or nondeterminism.
                // TODO: share bodies with extension nodes whose equality is known to be exact.
                LogicalPlan::Extension(_) => {
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
                    let volatility = match expr {
                        Expr::ScalarFunction(function) => function.func.signature().volatility,
                        Expr::AggregateFunction(function) => function.func.signature().volatility,
                        Expr::WindowFunction(function) => function.fun.signature().volatility,
                        _ => Volatility::Immutable,
                    };
                    if volatility == Volatility::Volatile {
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
            plan => plan,
        })
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
