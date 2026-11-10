use std::collections::HashMap;
use std::mem;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter};
use datafusion::common::{DataFusionError, Result};
use datafusion_expr::expr::{BinaryExpr, Case, Placeholder, ScalarFunction};
use datafusion_expr::{Expr, Extension, LogicalPlan, Projection, SubqueryAlias, ident};
use sail_common_datafusion::input_file::is_input_file_metadata_function;
use sail_function::scalar::explode::Explode;
use sail_function::scalar::math::partition_random::{
    LazyRightOperand, PartitionSeed, is_row_argument, lazy_right_operand, seed_argument_position,
};
use sail_function::scalar::multi_expr::MultiExpr;
use sail_logical_plan::seeded_random::{
    SeededRandomColumn, SeededRandomNode, is_reproducible_seeded_random_call,
};
use sail_python_udf::udf::pyspark_udf::{PySparkUDF, PySparkUdfKind};

use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;
use crate::resolver::tree::exists::ExistsRewriter;
use crate::resolver::tree::{PlanRewriter, empty_logical_plan};
use crate::scalar_subquery_key::ScalarSubqueryKey;

/// How the calls of random functions with a seed in an expression are evaluated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SeededRandomScope {
    /// The whole expression is evaluated by a [`SeededRandomNode`], so that each call
    /// advances its generator only for the rows that evaluate it. Spark evaluates
    /// nondeterministic expressions in projections and filters this way.
    Expression,
    /// Each call is evaluated by a [`SeededRandomNode`] for every row. Spark pulls the calls
    /// out of the other operators (`PullOutNondeterministic`), such as sorts, grouping
    /// expressions, and repartitioning.
    Call,
}

/// Replaces the expressions with calls of random functions with a seed by columns of a
/// [`SeededRandomNode`], which seeds each call once per partition as Spark does.
///
/// In the [`SeededRandomScope::Expression`] scope, the largest subexpressions that the node
/// can evaluate are replaced. An expression with a window function is evaluated around the
/// window, so only the arguments and the window specification are replaced, and the rest is
/// replaced when the rewriter is applied again after the window functions are planned.
/// Predicate subqueries, correlated scalar subqueries, input file metadata, and pandas
/// iterator UDFs are evaluated for every row below the node, as Spark evaluates them before
/// the projection or filter, so that the node can evaluate the expression around them. An
/// expression that the node still cannot evaluate, such as one with a generator or with such
/// a part whose arguments call a random function with a seed, has its calls replaced
/// individually, except for the calls in children that are not evaluated for every row, which
/// stay scalar functions.
pub(crate) struct SeededRandomRewriter<'s> {
    plan: LogicalPlan,
    state: &'s mut PlanResolverState,
    scope: SeededRandomScope,
    /// Whether the plan is the aggregate of a correlated subquery, possibly with its filter,
    /// which DataFusion requires below only projections in a correlated scalar subquery, so
    /// the calls above it stay scalar functions. It is found when an expression first has a
    /// call to replace.
    correlated_aggregate: Option<bool>,
    /// Whether the input of the node has outer references, which a part evaluated below the
    /// node would separate from the operators above it, so that the subquery could not be
    /// decorrelated. Staging does not add outer references, so it is found once.
    correlated_input: Option<bool>,
    input: Option<LogicalPlan>,
    columns: Vec<SeededRandomColumn>,
    /// The names of the columns whose expressions can be reused.
    reusable_columns: HashMap<ColumnKey, String>,
}

impl<'s> PlanRewriter<'s> for SeededRandomRewriter<'s> {
    fn new_from_plan(plan: LogicalPlan, state: &'s mut PlanResolverState) -> Self {
        Self::new(plan, state, SeededRandomScope::Expression)
    }

    fn into_plan(self) -> Result<LogicalPlan> {
        let Some(input) = self.input else {
            return Ok(self.plan);
        };
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(SeededRandomNode::try_new(Arc::new(input), self.columns)?),
        }))
    }
}

impl TreeNodeRewriter for SeededRandomRewriter<'_> {
    type Node = Expr;

    fn f_down(&mut self, node: Expr) -> Result<Transformed<Expr>> {
        if contains_seeded_random_call(&node) && self.is_correlated_aggregate() {
            return Ok(Transformed::new(node, false, TreeNodeRecursion::Jump));
        }
        let Transformed {
            data, transformed, ..
        } = match self.scope {
            SeededRandomScope::Expression => self.rewrite_expression(node)?,
            SeededRandomScope::Call => self.rewrite_calls(node)?,
        };
        Ok(Transformed::new(data, transformed, TreeNodeRecursion::Jump))
    }
}

impl<'s> SeededRandomRewriter<'s> {
    pub(crate) fn new(
        plan: LogicalPlan,
        state: &'s mut PlanResolverState,
        scope: SeededRandomScope,
    ) -> Self {
        Self {
            plan,
            state,
            scope,
            correlated_aggregate: None,
            correlated_input: None,
            input: None,
            columns: vec![],
            reusable_columns: HashMap::new(),
        }
    }

    fn is_correlated_aggregate(&mut self) -> bool {
        // The plan is not replaced by the node before the first call is replaced.
        *self.correlated_aggregate.get_or_insert_with(|| {
            let mut plan = &self.plan;
            while let LogicalPlan::Projection(Projection { input, .. })
            | LogicalPlan::SubqueryAlias(SubqueryAlias { input, .. }) = plan
            {
                plan = input.as_ref();
            }
            let aggregate = match plan {
                LogicalPlan::Aggregate(_) => true,
                LogicalPlan::Filter(filter) => {
                    matches!(filter.input.as_ref(), LogicalPlan::Aggregate(_))
                }
                _ => false,
            };
            aggregate
                && self
                    .plan
                    .exists(|plan| Ok(plan.contains_outer_reference()))
                    .unwrap_or(true)
        })
    }

    fn rewrite_expression(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        if !contains_seeded_random_call(&expr) {
            return Ok(Transformed::no(expr));
        }
        if can_evaluate_in_node(&expr) {
            return self.materialize(expr);
        }
        match expr {
            Expr::WindowFunction(_) => expr.map_children(|child| self.rewrite_expression(child)),
            expr if contains_window_function(&expr) => expr.map_children(|child| {
                if contains_window_function(&child) {
                    self.rewrite_expression(child)
                } else {
                    Ok(Transformed::no(child))
                }
            }),
            // Staging keeps the children that are not evaluated for every row whole, so it is
            // only needed for the calls in such children. Other calls are replaced individually.
            expr if has_lazy_seeded_random_call(&expr)
                && can_evaluate_after_staging(&expr)
                && !self.is_input_correlated() =>
            {
                match self.stage(expr.clone())? {
                    Some(staged) => self.materialize(staged),
                    None => self.rewrite_lazily(expr),
                }
            }
            expr => self.rewrite_lazily(expr),
        }
    }

    fn rewrite_lazily(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        // The calls in the children that are not evaluated for every row stay
        // scalar functions, so that they advance their generators only for the rows
        // that evaluate them.
        // TODO: Such calls are not seeded per partition, as Spark does.
        let lazy = lazy_children(&expr);
        let mut index = 0;
        let result = expr.map_children(|child| {
            let is_lazy = lazy.get(index).copied().unwrap_or(false);
            index += 1;
            if is_lazy {
                Ok(Transformed::no(child))
            } else {
                self.rewrite_expression(child)
            }
        });
        debug_assert!(result.is_err() || index == lazy.len());
        result
    }

    /// Returns the input of the node, which is prepared from the plan for the first column.
    fn take_input(&mut self) -> Result<LogicalPlan> {
        match self.input.take() {
            Some(input) => Ok(input),
            None => {
                let plan = mem::replace(&mut self.plan, empty_logical_plan());
                // The random values depend on the user's input order even when the
                // consumer does not.
                let (plan, _, _) = PlanResolver::require_input_sort_inner(plan)
                    .map_err(|error| DataFusionError::External(Box::new(error)))?;
                Ok(plan)
            }
        }
    }

    fn is_input_correlated(&mut self) -> bool {
        let plan = self.input.as_ref().unwrap_or(&self.plan);
        *self.correlated_input.get_or_insert_with(|| {
            plan.exists(|plan| Ok(plan.contains_outer_reference()))
                .unwrap_or(true)
        })
    }

    /// Replaces the parts of the expression that the node cannot evaluate by columns
    /// computed for every row below the node, and updates the input of the node. Returns
    /// `None` without changing the input if a subquery cannot be evaluated below the node.
    fn stage(&mut self, expr: Expr) -> Result<Option<Expr>> {
        let mut input = match &self.input {
            Some(input) => input.clone(),
            None => {
                // The random values depend on the user's input order even when the
                // consumer does not.
                let (plan, _, _) = PlanResolver::require_input_sort_inner(self.plan.clone())
                    .map_err(|error| DataFusionError::External(Box::new(error)))?;
                plan
            }
        };
        let expr = self.stage_into(expr, &mut input)?;
        if expr.is_some() {
            self.input = Some(input);
        }
        Ok(expr)
    }

    fn stage_into(&mut self, expr: Expr, input: &mut LogicalPlan) -> Result<Option<Expr>> {
        let mut staged = true;
        let expr = expr.transform_down(|expr| {
            if !is_stageable(&expr) {
                return Ok(Transformed::no(expr));
            }
            let plan = mem::replace(input, empty_logical_plan());
            let expr = if matches!(expr, Expr::Exists(_) | Expr::InSubquery(_)) {
                let mut rewriter = ExistsRewriter::new_from_plan(plan, self.state);
                // Some subqueries cannot be rewritten as joins, so their expressions keep
                // the subqueries, which are planned after the node.
                let Ok(result) = expr.clone().rewrite(&mut rewriter) else {
                    staged = false;
                    return Ok(Transformed::new(expr, false, TreeNodeRecursion::Stop));
                };
                *input = rewriter.into_plan()?;
                // The result may have a correlated scalar subquery to stage.
                let Some(expr) = self.stage_into(result.data, input)? else {
                    staged = false;
                    return Ok(Transformed::new(expr, false, TreeNodeRecursion::Stop));
                };
                expr
            } else {
                let name = self.state.next_field_id();
                let exprs = plan
                    .schema()
                    .columns()
                    .into_iter()
                    .map(Expr::Column)
                    .chain([expr.alias(&name)])
                    .collect();
                *input = LogicalPlan::Projection(Projection::try_new(exprs, Arc::new(plan))?);
                ident(name)
            };
            Ok(Transformed::new(expr, true, TreeNodeRecursion::Jump))
        })?;
        Ok(staged.then_some(expr.data))
    }

    fn rewrite_calls(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        expr.transform_up(|expr| {
            if is_seeded_random_call(&expr) {
                self.materialize(expr)
            } else {
                Ok(Transformed::no(expr))
            }
        })
    }

    /// Replaces an expression by a column of the node.
    fn materialize(&mut self, node: Expr) -> Result<Transformed<Expr>> {
        // Equal expressions over the same rows produce the same values, unless they contain
        // other volatile calls, which are evaluated independently.
        let reusable = !node
            .exists(|e| {
                Ok(e.is_volatile_node()
                    && !(is_seeded_random_call(e) && is_reproducible_seeded_random_call(e)))
            })
            .unwrap_or(true);
        let key = if reusable {
            ColumnKey::try_new(&node)?
        } else {
            None
        };
        let existing = key.as_ref().and_then(|key| self.reusable_columns.get(key));
        let name = match existing {
            Some(name) => name.clone(),
            None => {
                let input = self.take_input()?;
                self.input = Some(input);
                // Create an internal-only field ID without a referenceable name.
                let name = self.state.next_field_id();
                if let Some(key) = key {
                    self.reusable_columns.insert(key, name.clone());
                }
                self.columns.push(SeededRandomColumn {
                    name: name.clone(),
                    expr: node,
                    seed: PartitionSeed::Offset,
                });
                name
            }
        };
        Ok(Transformed::yes(ident(&name).alias(&name)))
    }
}

/// Identifies the values of a column of the node, regardless of the aliases and of the
/// generated names in equivalent uncorrelated scalar subqueries.
#[derive(PartialEq, Eq, Hash)]
struct ColumnKey {
    /// The expression with each shareable scalar subquery replaced by a placeholder.
    expr: Expr,
    /// The keys of the replaced scalar subqueries in order.
    subqueries: Vec<ScalarSubqueryKey>,
    /// Literal equality can ignore type properties such as timestamp time zones.
    literal_types: Vec<DataType>,
}

impl ColumnKey {
    fn try_new(expr: &Expr) -> Result<Option<Self>> {
        let mut expr = expr;
        while let Expr::Alias(alias) = expr
            && alias.metadata.is_none()
        {
            expr = alias.expr.as_ref();
        }
        let mut subqueries = vec![];
        let mut literal_types = vec![];
        let mut shareable = true;
        let expr = expr
            .clone()
            .transform_down(|expr| {
                if let Expr::Literal(value, _) = &expr {
                    literal_types.push(value.data_type());
                }
                let Expr::ScalarSubquery(subquery) = &expr else {
                    return Ok(Transformed::no(expr));
                };
                let Some(key) = ScalarSubqueryKey::try_new(subquery)? else {
                    shareable = false;
                    return Ok(Transformed::new(expr, false, TreeNodeRecursion::Jump));
                };
                subqueries.push(key);
                Ok(Transformed::new(
                    Expr::Placeholder(Placeholder::new_with_field("#subquery".to_string(), None)),
                    true,
                    TreeNodeRecursion::Jump,
                ))
            })?
            .data;
        Ok(shareable.then_some(Self {
            expr,
            subqueries,
            literal_types,
        }))
    }
}

/// Returns whether the expression is a call of a random function with a seed whose arguments
/// are constant, as Spark requires, except for the arguments evaluated for each row, which the
/// node must be able to evaluate. Other calls are evaluated as scalar functions.
fn is_seeded_random_call(expr: &Expr) -> bool {
    let Expr::ScalarFunction(ScalarFunction { func, args }) = expr else {
        return false;
    };
    seed_argument_position(func, args.len()).is_some()
        && args.iter().enumerate().all(|(i, arg)| {
            (if is_row_argument(func, i) {
                can_evaluate_in_node(arg)
            } else {
                arg.column_refs().is_empty() && !arg.is_volatile()
            }) && !arg
                .exists(|e| Ok(matches!(e, Expr::LambdaVariable(_))))
                .unwrap_or(true)
        })
}

pub(crate) fn contains_seeded_random_call(expr: &Expr) -> bool {
    expr.exists(|e| Ok(is_seeded_random_call(e)))
        .unwrap_or(false)
}

/// Returns, for each child of the expression in order, whether the child may not be
/// evaluated for every row, as in the common subexpression elimination of DataFusion.
fn lazy_children(expr: &Expr) -> Vec<bool> {
    let lazy: Vec<&Expr> = match expr {
        Expr::ScalarFunction(ScalarFunction { func, args }) => func
            .conditional_arguments(args)
            .map(|(_, lazy)| lazy)
            .unwrap_or_default(),
        Expr::BinaryExpr(BinaryExpr { op, right, .. })
            if matches!(
                lazy_right_operand(*op),
                Some(LazyRightOperand::And | LazyRightOperand::Or)
            ) =>
        {
            vec![right.as_ref()]
        }
        Expr::Case(Case {
            when_then_expr,
            else_expr,
            ..
        }) => when_then_expr
            .iter()
            .take(1)
            .map(|(_, then)| then.as_ref())
            .chain(
                when_then_expr
                    .iter()
                    .skip(1)
                    .flat_map(|(when, then)| [when.as_ref(), then.as_ref()]),
            )
            .chain(else_expr.iter().map(|e| e.as_ref()))
            .collect(),
        _ => vec![],
    };
    let mut children = vec![];
    let _ = expr.apply_children(|child| {
        children.push(lazy.iter().any(|e| std::ptr::eq(*e, child)));
        Ok(TreeNodeRecursion::Continue)
    });
    children
}

/// Returns, for each child of the expression in order, whether Spark may not evaluate the
/// child for every row. In addition to the children in [`lazy_children`], Spark evaluates
/// the right operand of an operator whose result is null for a null operand only if the left
/// operand is not null, and the node evaluates it in the same way.
fn spark_lazy_children(expr: &Expr) -> Vec<bool> {
    let mut lazy = lazy_children(expr);
    if let Expr::BinaryExpr(BinaryExpr { op, .. }) = expr
        && lazy_right_operand(*op).is_some()
        && let Some(right) = lazy.get_mut(1)
    {
        *right = true;
    }
    lazy
}

/// Returns whether a call of a random function with a seed in the expression is in a child
/// that may not be evaluated for every row.
fn has_lazy_seeded_random_call(expr: &Expr) -> bool {
    let lazy = spark_lazy_children(expr);
    let mut index = 0;
    let mut found = false;
    let _ = expr.apply_children(|child| {
        let is_lazy = lazy.get(index).copied().unwrap_or(false);
        index += 1;
        found = if is_lazy {
            contains_seeded_random_call(child)
        } else {
            has_lazy_seeded_random_call(child)
        };
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    });
    found
}

/// Returns whether the expression contains a generator, which is planned separately.
pub(crate) fn contains_generator(expr: &Expr) -> bool {
    expr.exists(|e| {
        Ok(
            matches!(e, Expr::ScalarFunction(ScalarFunction { func, .. })
            if func.inner().is::<Explode>()),
        )
    })
    .unwrap_or(false)
}

fn contains_window_function(expr: &Expr) -> bool {
    expr.exists(|e| Ok(matches!(e, Expr::WindowFunction(_))))
        .unwrap_or(false)
}

/// Returns whether the node can evaluate the expression, which must not contain operators
/// or functions that are planned separately.
fn can_evaluate_in_node(expr: &Expr) -> bool {
    !expr.exists(|e| Ok(is_unevaluable(e))).unwrap_or(true)
}

/// Returns whether the node can evaluate the expression after its stageable parts are
/// replaced by columns computed below the node.
fn can_evaluate_after_staging(expr: &Expr) -> bool {
    let mut evaluable = true;
    let _ = expr.apply(|e| {
        if is_stageable(e) {
            Ok(TreeNodeRecursion::Jump)
        } else if is_unevaluable(e) {
            evaluable = false;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    });
    evaluable
}

/// Returns whether the expression can be evaluated for every row below the node, which is
/// how Spark evaluates predicate subqueries, correlated scalar subqueries, input file
/// metadata, and Python UDFs in projections and filters.
fn is_stageable(expr: &Expr) -> bool {
    let stageable = match expr {
        Expr::Exists(_) | Expr::InSubquery(_) => true,
        Expr::ScalarSubquery(subquery) => !subquery.outer_ref_columns.is_empty(),
        Expr::ScalarFunction(ScalarFunction { func, .. }) => {
            is_input_file_metadata_function(func)
                || func
                    .inner()
                    .downcast_ref::<PySparkUDF>()
                    .is_some_and(|udf| udf.kind() == PySparkUdfKind::ScalarPandasIter)
        }
        _ => false,
    };
    stageable
        && !expr
            .exists(|e| {
                Ok(is_seeded_random_call(e)
                    || matches!(
                        e,
                        Expr::LambdaVariable(_) | Expr::OuterReferenceColumn(_, _)
                    ))
            })
            .unwrap_or(true)
}

/// Returns whether the expression itself is an operator or a function that the node cannot
/// evaluate.
fn is_unevaluable(e: &Expr) -> bool {
    match e {
        Expr::AggregateFunction(_)
        | Expr::WindowFunction(_)
        | Expr::Exists(_)
        | Expr::InSubquery(_)
        | Expr::SetComparison(_)
        | Expr::OuterReferenceColumn(_, _)
        | Expr::Placeholder(_)
        | Expr::ScalarVariable(_, _)
        | Expr::GroupingSet(_)
        | Expr::Unnest(_) => true,
        // An uncorrelated scalar subquery is evaluated before the node as a constant.
        Expr::ScalarSubquery(subquery) => !subquery.outer_ref_columns.is_empty(),
        Expr::ScalarFunction(ScalarFunction { func, .. }) => {
            func.as_async().is_some()
                || is_input_file_metadata_function(func)
                || func.inner().is::<Explode>()
                || func.inner().is::<MultiExpr>()
                || func
                    .inner()
                    .downcast_ref::<PySparkUDF>()
                    .is_some_and(|udf| udf.kind() == PySparkUdfKind::ScalarPandasIter)
        }
        _ => false,
    }
}

/// Returns the calls of random functions with a seed in the expression, which Spark
/// evaluates for every row of operators other than projections and filters.
pub(crate) fn seeded_random_calls(expr: &Expr) -> Vec<&Expr> {
    let mut calls = vec![];
    let _ = expr.apply(|e| {
        if is_seeded_random_call(e) {
            calls.push(e);
            return Ok(TreeNodeRecursion::Jump);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    calls
}

#[cfg(test)]
mod tests {
    use datafusion_expr::{LogicalPlanBuilder, Subquery, col, lit, when};

    use super::*;

    fn scalar_subquery(value: f64, name: &str) -> Result<Expr> {
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![lit(value).alias(name)])?
            .build()?;
        Ok(Expr::ScalarSubquery(Subquery {
            subquery: Arc::new(plan),
            outer_ref_columns: vec![],
            spans: Default::default(),
        }))
    }

    fn case(subquery: Expr, name: &str) -> Result<Expr> {
        Ok(when(col("id").eq(lit(0_i64)), lit(1.0))
            .otherwise(subquery)?
            .alias(name))
    }

    #[test]
    fn test_column_key_ignores_aliases_and_subquery_names() -> Result<()> {
        let a = ColumnKey::try_new(&case(scalar_subquery(0.0, "#1")?, "a1")?)?;
        let b = ColumnKey::try_new(&case(scalar_subquery(0.0, "#2")?, "a2")?)?;
        let c = ColumnKey::try_new(&case(scalar_subquery(1.0, "#3")?, "a3")?)?;
        assert!(a.is_some());
        assert!(a == b);
        assert!(a != c);
        Ok(())
    }

    #[test]
    fn test_column_key_preserves_literal_types() -> Result<()> {
        use datafusion_common::ScalarValue;

        // Arrow literal equality ignores these type properties, but column reuse must not.
        for (a, b) in [
            (
                ScalarValue::TimestampMicrosecond(Some(0), None),
                ScalarValue::TimestampMicrosecond(Some(0), Some("UTC".into())),
            ),
            (
                ScalarValue::FixedSizeBinary(1, None),
                ScalarValue::FixedSizeBinary(2, None),
            ),
        ] {
            assert_eq!(a, b);
            let a = ColumnKey::try_new(&when(col("id").eq(lit(0_i64)), lit(a)).end()?.alias("a"))?;
            let b = ColumnKey::try_new(&when(col("id").eq(lit(0_i64)), lit(b)).end()?.alias("b"))?;
            assert!(a.is_some() && b.is_some());
            assert!(a != b);
        }
        Ok(())
    }

    #[test]
    fn test_column_key_rejects_unshareable_subqueries() -> Result<()> {
        use datafusion_expr::ScalarUDF;
        use sail_function::scalar::math::random::Random;

        let random = Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(ScalarUDF::new_from_impl(Random::new())),
            vec![],
        ));
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![random.alias("#1")])?
            .build()?;
        let subquery = Expr::ScalarSubquery(Subquery {
            subquery: Arc::new(plan),
            outer_ref_columns: vec![],
            spans: Default::default(),
        });
        // Even the same unresolved volatile plan must not bypass the subquery sharing rules.
        assert!(ColumnKey::try_new(&case(subquery.clone(), "a")?)?.is_none());
        assert!(ColumnKey::try_new(&case(subquery, "b")?)?.is_none());
        Ok(())
    }
}
