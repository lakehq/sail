use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::datasource::{TableProvider, source_as_provider};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::scalar_subquery::ScalarSubqueryExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::{Expr, LogicalPlan, Subquery, Volatility};
use datafusion_physical_expr::projection::ProjectionExpr;
use sail_common_datafusion::rename::table_provider::RenameTableProvider;
use sail_data_source::formats::python::PythonTableProvider;

/// Identifies what a scalar subquery computes, regardless of its output names.
#[derive(PartialEq, Eq, Hash)]
pub struct ScalarSubqueryKey {
    /// The unaliased expressions of the output projections, outermost first, with the
    /// column of a single-column input replaced by a placeholder.
    output: Vec<Expr>,
    /// The plan below the output projections.
    input: Arc<LogicalPlan>,
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
                    // Python data sources are read once for each reference, as in Spark.
                    if source_as_provider(&scan.source)
                        .is_ok_and(|provider| is_python_table_provider(provider.as_ref()))
                    {
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
        // Skip the output names, which differ between the references to a CTE and
        // between repeated subqueries, including the renamed column of a single-column
        // input (e.g. under a cast added by type coercion).
        let mut input = &subquery.subquery;
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
            sources,
            literal_types,
            fields,
        }))
    }
}

fn is_python_table_provider(provider: &dyn TableProvider) -> bool {
    if provider.is::<PythonTableProvider>() {
        true
    } else if let Some(rename) = provider.downcast_ref::<RenameTableProvider>() {
        is_python_table_provider(rename.inner().as_ref())
    } else {
        false
    }
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
