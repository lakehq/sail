use std::sync::Arc;

use datafusion_common::DFSchemaRef;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion_expr::logical_plan::{Filter, Projection};
use datafusion_expr::{Expr, ExprSchemable, LogicalPlan, expr_fn};
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
use crate::resolver::expression::predicate::coerce_in_subquery_pair;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) async fn resolve_expression_in_subquery(
        &self,
        expr: spec::Expr,
        subquery: spec::QueryPlan,
        negated: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let mut expr = self.resolve_expression(expr, schema, state).await?;
        let mut subquery = {
            let mut scope = state.enter_query_scope(Arc::clone(schema));
            let plan = self.resolve_query_plan(subquery, scope.state()).await?;
            // Wrap in a Projection so that DataFusion's head_output_expr() works
            // for plans without a top-level Projection (e.g. bare Extension nodes
            // like RangeNode from Spark Connect). SQL subqueries already have a
            // Projection on top, so wrapping them again can cause duplicate column
            // names during decorrelation.
            if matches!(plan, LogicalPlan::Projection(_)) {
                plan
            } else {
                let exprs: Vec<Expr> = plan
                    .schema()
                    .columns()
                    .into_iter()
                    .map(Expr::Column)
                    .collect();
                LogicalPlan::Projection(Projection::try_new(exprs, Arc::new(plan))?)
            }
        };
        // The RHS is not part of the parent expression tree, so timestamp coercion must update
        // both plans before DataFusion analyzes the IN-subquery comparison.
        if subquery.schema().fields().len() != 1 {
            return Err(PlanError::analysis(
                "IN subquery must return exactly one column",
            ));
        }
        {
            let left_type = expr.get_type(schema.as_ref())?;
            let right_type = subquery.schema().field(0).data_type().clone();
            let right_column = subquery
                .schema()
                .columns()
                .into_iter()
                .next()
                .ok_or_else(|| {
                    PlanError::internal("IN subquery should expose one output column")
                })?;
            let (coerced_left, coerced_right, _, right_changed) = coerce_in_subquery_pair(
                expr,
                &left_type,
                schema,
                Expr::Column(right_column.clone()),
                &right_type,
                subquery.schema(),
                &self.config,
            )?;
            expr = coerced_left;
            if right_changed {
                subquery = LogicalPlan::Projection(Projection::try_new(
                    vec![coerced_right.alias(right_column.name)],
                    Arc::new(subquery),
                )?);
            }
        }
        let in_subquery = if !negated {
            expr_fn::in_subquery(expr, Arc::new(subquery))
        } else {
            expr_fn::not_in_subquery(expr, Arc::new(subquery))
        };
        Ok(NamedExpr::new(vec!["in_subquery".to_string()], in_subquery))
    }

    pub(super) async fn resolve_expression_scalar_subquery(
        &self,
        subquery: spec::QueryPlan,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let subquery = {
            let mut scope = state.enter_query_scope(Arc::clone(schema));
            self.resolve_query_plan(subquery, scope.state()).await?
        };
        let mut root = &subquery;
        while let LogicalPlan::Projection(projection) = root {
            root = &projection.input;
        }
        if !subquery.all_out_ref_exprs().is_empty()
            && let LogicalPlan::Aggregate(aggregate) = root
            && aggregate.group_expr.iter().any(|expr| {
                if let Expr::GroupingSet(datafusion_expr::expr::GroupingSet::GroupingSets(sets)) =
                    expr
                {
                    let mut seen = std::collections::HashSet::new();
                    sets.iter().any(|set| !seen.insert(set))
                } else {
                    false
                }
            })
        {
            return Err(PlanError::analysis(
                "Correlated scalar subquery has duplicate grouping sets and can return more than one row",
            ));
        }
        let subquery = self.protect_scalar_cardinality(subquery, state)?;
        Ok(NamedExpr::new(
            vec!["subquery".to_string()],
            expr_fn::scalar_subquery(Arc::new(subquery)),
        ))
    }

    /// Aggregate non-aggregate correlated scalar queries with a runtime cardinality
    /// check. A top-level LIMIT 1 instead selects its first ordered value per key.
    fn protect_scalar_cardinality(
        &self,
        mut query: LogicalPlan,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        use datafusion::functions_aggregate::expr_fn::{count, first_value};
        use datafusion_common::Column;
        use datafusion_expr::{LogicalPlanBuilder, ScalarUDF, lit, when};
        use sail_common_datafusion::literal::{LiteralEvaluator, LiteralValue};
        use sail_function::scalar::misc::raise_error::RaiseError;
        if query.all_out_ref_exprs().is_empty() || query.schema().fields().len() != 1 {
            return Ok(query);
        }
        if query.exists(|plan| Ok(matches!(plan, LogicalPlan::Aggregate(_))))? {
            return Ok(query);
        }
        let mut limited = false;
        if let LogicalPlan::Limit(limit) = &query {
            let evaluator = LiteralEvaluator::new();
            let skip = limit
                .skip
                .as_ref()
                .map(|expr| {
                    evaluator
                        .evaluate(expr)
                        .and_then(|value| LiteralValue(&value).try_to_i64())
                })
                .transpose()?
                .unwrap_or(0);
            let fetch = limit
                .fetch
                .as_ref()
                .map(|expr| {
                    evaluator
                        .evaluate(expr)
                        .and_then(|value| LiteralValue(&value).try_to_i64())
                })
                .transpose()?;
            if skip == 0 && fetch == Some(0) {
                return Ok(LogicalPlan::EmptyRelation(datafusion_expr::EmptyRelation {
                    produce_one_row: false,
                    schema: Arc::clone(query.schema()),
                }));
            }
            if skip == 0 && fetch == Some(1) {
                limited = true;
                query = Arc::unwrap_or_clone(limit.input.clone());
            } else {
                return Ok(query);
            }
        }
        let output = Column::from(query.schema().qualified_field(0));
        // ORDER BY can have hidden inputs removed by a projection above the sort.
        // Carry them to FIRST_VALUE before removing the original Sort nodes.
        query = query
            .transform_down_up(
                |plan| {
                    let recursion = if matches!(
                        plan,
                        LogicalPlan::Projection(_)
                            | LogicalPlan::SubqueryAlias(_)
                            | LogicalPlan::Filter(_)
                    ) {
                        TreeNodeRecursion::Continue
                    } else {
                        // Do not change the schema below DISTINCT or other operators
                        // whose semantics depend on the complete set of input columns.
                        TreeNodeRecursion::Jump
                    };
                    Ok(Transformed::new(plan, false, recursion))
                },
                |plan| {
                    let LogicalPlan::Projection(mut projection) = plan else {
                        return Ok(Transformed::no(plan));
                    };
                    let order =
                        Self::find_input_sort_ordering(&projection.input).unwrap_or_default();
                    let columns = order
                        .iter()
                        .flat_map(|sort| sort.expr.column_refs())
                        .filter(|column| !projection.schema.has_column(column))
                        .cloned()
                        .collect::<std::collections::BTreeSet<_>>();
                    if columns.is_empty() {
                        return Ok(Transformed::no(LogicalPlan::Projection(projection)));
                    }
                    projection
                        .expr
                        .extend(columns.into_iter().map(Expr::Column));
                    Ok(Transformed::yes(LogicalPlan::Projection(
                        Projection::try_new(projection.expr, projection.input)?,
                    )))
                },
            )?
            .data;
        let order = Self::find_input_sort_ordering(&query).unwrap_or_default();
        // Remove sorts only through operators that select rows independently of ordering.
        // a sort below a nested LIMIT/OFFSET determines which rows survive and
        // cannot be replaced by ordering the enclosing FIRST_VALUE.
        query = query
            .transform_down_up(
                |plan| {
                    let recursion = match plan {
                        LogicalPlan::Projection(_)
                        | LogicalPlan::SubqueryAlias(_)
                        | LogicalPlan::Filter(_)
                        | LogicalPlan::Distinct(datafusion_expr::Distinct::All(_))
                        | LogicalPlan::Sort(_) => TreeNodeRecursion::Continue,
                        _ => TreeNodeRecursion::Jump,
                    };
                    Ok(Transformed::new(plan, false, recursion))
                },
                |plan| match plan {
                    LogicalPlan::Sort(sort) => {
                        Ok(Transformed::yes(Arc::unwrap_or_clone(sort.input)))
                    }
                    _ => Ok(Transformed::no(plan)),
                },
            )?
            .data;
        let value_name = state.register_field_name("");
        let count_name = state.register_field_name("");
        let aggregate = LogicalPlanBuilder::from(query)
            .aggregate(
                Vec::<Expr>::new(),
                vec![
                    first_value(Expr::Column(output.clone()), order).alias(&value_name),
                    count(lit(1_i64)).alias(&count_name),
                ],
            )?
            .build()?;
        let value = Expr::Column(Column::new_unqualified(value_name));
        let value = if limited {
            value
        } else {
            when(
                Expr::Column(Column::new_unqualified(count_name)).gt(lit(1_i64)),
                ScalarUDF::from(RaiseError::new()).call(vec![lit(
                    "SCALAR_SUBQUERY_TOO_MANY_ROWS: scalar subquery returned more than one row",
                )]),
            )
            .otherwise(value)?
        };
        Ok(LogicalPlanBuilder::from(aggregate)
            .project(vec![value.alias_qualified(output.relation, output.name)])?
            .build()?)
    }

    pub(super) async fn resolve_expression_exists(
        &self,
        subquery: spec::QueryPlan,
        negated: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let subquery = {
            let mut scope = state.enter_query_scope(Arc::clone(schema));
            self.resolve_query_plan(subquery, scope.state()).await?
        };
        let exists = if !negated {
            expr_fn::exists(Arc::new(subquery))
        } else {
            expr_fn::not_exists(Arc::new(subquery))
        };
        Ok(NamedExpr::new(vec!["exists".to_string()], exists))
    }

    /// Resolves a multi-column IN subquery by converting it to a correlated EXISTS.
    /// `(e1, e2, ...) IN (SELECT c1, c2, ... FROM sub)` becomes
    /// `[NOT] EXISTS (SELECT ... FROM sub WHERE c1 = e1 AND c2 = e2 AND ...)`
    pub(super) async fn resolve_multi_column_in_subquery(
        &self,
        in_subquery_values: Vec<spec::Expr>,
        subquery: spec::QueryPlan,
        negated: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let mut outer_refs = Vec::with_capacity(in_subquery_values.len());
        for value in in_subquery_values {
            let expr = self.resolve_expression(value, schema, state).await?;
            // Recursively convert column references to OuterReferenceColumn so
            // DataFusion's decorrelation pass recognizes them as correlated
            // references. This handles both bare columns (e.g. `a`) and
            // expressions containing columns (e.g. `a + 1`).
            let outer_ref = expr
                .transform(|e| match e {
                    Expr::Column(ref col) => {
                        let (_, field) = schema.qualified_field_from_column(col)?;
                        Ok(Transformed::yes(Expr::OuterReferenceColumn(
                            field.clone(),
                            col.clone(),
                        )))
                    }
                    _ => Ok(Transformed::no(e)),
                })
                .data()?;
            outer_refs.push(outer_ref);
        }
        let subquery_plan = {
            let mut scope = state.enter_query_scope(Arc::clone(schema));
            self.resolve_query_plan(subquery, scope.state()).await?
        };
        let subquery_cols = subquery_plan.schema().columns();
        if subquery_cols.len() != outer_refs.len() {
            return Err(PlanError::invalid(format!(
                "IN subquery column count mismatch: {} values vs {} subquery columns",
                outer_refs.len(),
                subquery_cols.len()
            )));
        }
        let filter_expr = outer_refs
            .into_iter()
            .zip(subquery_cols)
            .map(|(outer, inner_col)| -> PlanResult<Expr> {
                let outer_type = outer.get_type(schema.as_ref())?;
                let (_, inner_field) = subquery_plan
                    .schema()
                    .qualified_field_from_column(&inner_col)?;
                let inner_type = inner_field.data_type().clone();
                let (outer, inner, _, _) = coerce_in_subquery_pair(
                    outer,
                    &outer_type,
                    schema,
                    Expr::Column(inner_col),
                    &inner_type,
                    subquery_plan.schema(),
                    &self.config,
                )?;
                let equal = Expr::eq(inner, outer);
                Ok(if negated {
                    equal.clone().or(equal.is_null())
                } else {
                    equal
                })
            })
            .collect::<PlanResult<Vec<_>>>()?
            .into_iter()
            .reduce(Expr::and)
            .ok_or_else(|| PlanError::invalid("empty IN subquery values"))?;
        let filtered = LogicalPlan::Filter(Filter::try_new(filter_expr, Arc::new(subquery_plan))?);
        let exists_expr = if !negated {
            expr_fn::exists(Arc::new(filtered))
        } else {
            expr_fn::not_exists(Arc::new(filtered))
        };
        Ok(NamedExpr::new(vec!["in_subquery".to_string()], exists_expr))
    }

    /// Resolves a Subquery by looking up the referenced plan from state and delegating to existing subquery resolvers.
    pub(super) async fn resolve_expression_subquery(
        &self,
        plan_id: i64,
        subquery_type: spec::SubqueryType,
        in_subquery_values: Vec<spec::Expr>,
        negated: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let subquery_plan = state.get_subquery_reference(plan_id).ok_or_else(|| {
            PlanError::invalid(format!(
                "subquery plan_id {plan_id} not found in WithRelations references"
            ))
        })?;

        match subquery_type {
            spec::SubqueryType::In => {
                if in_subquery_values.is_empty() {
                    return Err(PlanError::invalid("IN subquery missing value expression"));
                }
                if in_subquery_values.len() == 1 {
                    let expr = in_subquery_values.into_iter().next().ok_or_else(|| {
                        PlanError::invalid("IN subquery missing value expression")
                    })?;
                    self.resolve_expression_in_subquery(expr, subquery_plan, negated, schema, state)
                        .await
                } else {
                    self.resolve_multi_column_in_subquery(
                        in_subquery_values,
                        subquery_plan,
                        negated,
                        schema,
                        state,
                    )
                    .await
                }
            }
            spec::SubqueryType::Scalar => {
                self.resolve_expression_scalar_subquery(subquery_plan, schema, state)
                    .await
            }
            spec::SubqueryType::Exists => {
                self.resolve_expression_exists(subquery_plan, negated, schema, state)
                    .await
            }
        }
    }
}
