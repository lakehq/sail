use std::sync::Arc;

use datafusion::functions_aggregate::{average, bit_and_or_xor, bool_and_or, count, min_max, sum};
use datafusion_common::arrow::datatypes::DataType;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion_common::{
    Column, DFSchemaRef, DataFusionError, Result as DataFusionResult, ScalarValue,
};
use datafusion_expr::expr_rewriter::normalize_col;
use datafusion_expr::logical_plan::{FetchType, SkipType};
use datafusion_expr::utils::find_aggregate_exprs;
use datafusion_expr::{
    Aggregate, AggregateUDF, Expr, ExprSchemable, Extension, LogicalPlan, LogicalPlanBuilder,
    Projection, SortExpr, Volatility, bitwise_and, bitwise_shift_right, cast, ident,
};
use datafusion_spark::function::aggregate::try_sum::SparkTrySum;
use sail_common::spec;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::aggregate::try_avg::TryAvgFunction;
use sail_function::scalar::datetime::spark_session_window::SparkSessionWindow;
use sail_function::scalar::explode::Explode;
use sail_logical_plan::monotonic_id::MonotonicIdNode;
use sail_logical_plan::session_aggregate::SessionAggregateNode;
use sail_logical_plan::session_window::SessionWindowNode;
use sail_logical_plan::sort::{RequiredSortNode, SortWithinPartitionsNode};
use sail_logical_plan::spark_partition_id::SparkPartitionIdNode;
use sail_python_udf::get_udf_display_name;
use sail_python_udf::udf::pyspark_udaf::PySparkGroupAggregateUDF;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::{AggregateState, PlanResolverState};
use crate::resolver::tree::explode::ExplodeRewriter;
use crate::resolver::tree::monotonic_id::MonotonicIdRewriter;
use crate::resolver::tree::spark_partition_id::SparkPartitionIdRewriter;
use crate::resolver::tree::window::WindowRewriter;

/// Projections resolved by index (a `None` marks a projection deferred until the
/// grouping is materialized), paired with the indices of the deferred projections.
type ResolvedProjections = (Vec<Option<NamedExpr>>, Vec<usize>);

/// A map from a grouping generator expression to the column that materializes it.
type GeneratorReplacements = Vec<(Expr, Expr)>;

/// A grouping desugaring pass. Each one detects its own special expression in the
/// grouping, rewrites the plan and grouping to materialize it as a column, and
/// extends the replacement map; it is a no-op when its expression is absent. All
/// passes share this signature so [`PlanResolver::expand_grouping`] can apply them
/// uniformly as a list.
type GroupingExpander<'a> = fn(
    &PlanResolver<'a>,
    LogicalPlan,
    Vec<NamedExpr>,
    GeneratorReplacements,
    &mut PlanResolverState,
) -> PlanResult<(LogicalPlan, Vec<NamedExpr>, GeneratorReplacements)>;

/// Returns the name of a volatile (non-deterministic) scalar expression found
/// in an aggregate context. Catches two Spark CheckAnalysis violations:
/// 1. Volatile scalar UDF used directly in aggregate projections (outside any aggregate fn)
/// 2. Volatile scalar UDF nested inside aggregate function arguments
fn find_volatile_in_aggregate_context(expr: &Expr) -> Option<String> {
    let mut found_name: Option<String> = None;
    let _ = expr.apply(|e| {
        if let Expr::ScalarFunction(f) = e
            && f.func.signature().volatility == Volatility::Volatile
        {
            found_name = Some(f.func.name().to_string());
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found_name
}

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_aggregate(
        &self,
        aggregate: spec::Aggregate,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::Aggregate {
            input,
            grouping,
            aggregate: projections,
            having,
            with_grouping_expressions,
        } = aggregate;

        let input = self
            .resolve_query_plan_with_hidden_fields(*input, state)
            .await?;
        let schema = input.schema();

        // Resolve the projections, deferring any that reference a grouping output
        // until the grouping is materialized below.
        let (resolved_projections, deferred_projections) = self
            .resolve_projections_deferring(&projections, schema, state)
            .await?;

        let grouping = {
            let projections = resolved_projections.iter().flatten().cloned().collect();
            let mut scope = state.enter_aggregate_scope(AggregateState::Grouping { projections });
            let state = scope.state();
            self.resolve_named_expressions(grouping, schema, state)
                .await?
        };

        // GROUP BY ordinals must resolve before grouping expansion: an
        // unresolved ordinal literal would otherwise be materialized as a
        // constant grouping key by the session_window expander, and a marker
        // referenced only by ordinal would never desugar.
        let grouping = self.resolve_grouping_positions_early(grouping, &resolved_projections)?;

        // Expand any special grouping expressions into materialized grouping
        // columns; a no-op for ordinary aggregates.
        let (input, grouping, generator_replacements) =
            self.expand_grouping(input, grouping, state)?;
        let schema = input.schema();

        // Resolve the deferred projections (grouping columns are now in scope) and
        // inline any re-used generator expressions.
        let projections = self
            .finish_projections(
                resolved_projections,
                deferred_projections,
                &projections,
                &generator_replacements,
                schema,
                state,
            )
            .await?;

        // Spark evaluates `session_window` inside an aggregate function
        // argument with per-row (pre-merge) semantics; Sail does not implement
        // that path, so reject it instead of silently aggregating the merged
        // struct.
        let session_columns: Vec<&Expr> = generator_replacements
            .iter()
            .filter(|(from, _)| Self::contains_session_window(from))
            .map(|(_, to)| to)
            .collect();
        if !session_columns.is_empty() {
            let all_exprs = projections
                .iter()
                .map(|p| p.expr.clone())
                .collect::<Vec<_>>();
            for agg in find_aggregate_exprs(&all_exprs) {
                let references_session = agg
                    .exists(|e| Ok(session_columns.contains(&e)))
                    .unwrap_or(false);
                if references_session {
                    return Err(PlanError::AnalysisError(
                        "session_window inside an aggregate function has per-row semantics \
                         and is not supported"
                            .to_string(),
                    ));
                }
            }
        }

        // Spark CheckAnalysis: reject non-deterministic expressions in aggregate context
        for proj in &projections {
            if let Some(name) = find_volatile_in_aggregate_context(&proj.expr) {
                return Err(PlanError::AnalysisError(format!(
                    "Non-deterministic expression {name} should not appear in an aggregate query",
                )));
            }
        }

        // Spark CheckAnalysis: GroupedAgg Pandas/Arrow UDFs cannot be mixed with regular
        // (non-UDF) aggregate functions in the same .agg() call.
        Self::check_no_mixed_grouped_agg_udf(&projections)?;

        let having = {
            let mut scope = state.enter_aggregate_scope(AggregateState::Having {
                projections: projections.clone(),
                grouping: grouping.clone(),
            });
            let state = scope.state();
            match having {
                Some(having) => Some(Self::replace_generator_expressions(
                    self.resolve_expression(having, schema, state).await?,
                    &generator_replacements,
                )?),
                None => None,
            }
        };

        self.rewrite_aggregate(
            input,
            projections,
            grouping,
            having,
            with_grouping_expressions,
            state,
        )
    }

    /// Resolves `GROUP BY <ordinal>` against the select list before grouping
    /// expansion. A deferred (not yet resolved) select item leaves the ordinal
    /// in place for [`Self::resolve_grouping_positions`] to finish later.
    fn resolve_grouping_positions_early(
        &self,
        exprs: Vec<NamedExpr>,
        projections: &[Option<NamedExpr>],
    ) -> PlanResult<Vec<NamedExpr>> {
        let num_projections = projections.len() as i64;
        exprs
            .into_iter()
            .map(|named_expr| {
                let NamedExpr { expr, .. } = &named_expr;
                let Expr::Literal(scalar_value, _) = expr else {
                    return Ok(named_expr);
                };
                let position = match scalar_value {
                    ScalarValue::Int32(Some(position)) => *position as i64,
                    ScalarValue::Int64(Some(position)) => *position,
                    _ => return Ok(named_expr),
                };
                if position > 0_i64 && position <= num_projections {
                    match &projections[(position - 1) as usize] {
                        Some(resolved) => Ok(resolved.clone()),
                        None => Ok(named_expr),
                    }
                } else {
                    Err(PlanError::invalid(format!(
                        "Cannot resolve column position {position}. Valid positions are 1 to {num_projections}."
                    )))
                }
            })
            .collect()
    }

    fn resolve_grouping_positions(
        &self,
        exprs: Vec<NamedExpr>,
        projections: &[NamedExpr],
    ) -> PlanResult<Vec<NamedExpr>> {
        let num_projections = projections.len() as i64;
        exprs
            .into_iter()
            .map(|named_expr| {
                let NamedExpr { expr, .. } = &named_expr;
                match expr {
                    Expr::Literal(scalar_value, _metadata) => {
                        let position = match scalar_value {
                            ScalarValue::Int32(Some(position)) => *position as i64,
                            ScalarValue::Int64(Some(position)) => *position,
                            _ => return Ok(named_expr),
                        };
                        if position > 0_i64 && position <= num_projections {
                            Ok(projections[(position - 1) as usize].clone())
                        } else {
                            Err(PlanError::invalid(format!(
                                "Cannot resolve column position {position}. Valid positions are 1 to {num_projections}."
                            )))
                        }
                    }
                    _ => Ok(named_expr),
                }
            })
            .collect()
    }

    pub(super) fn rewrite_aggregate(
        &self,
        input: LogicalPlan,
        projections: Vec<NamedExpr>,
        grouping: Vec<NamedExpr>,
        having: Option<Expr>,
        with_grouping_expressions: bool,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let grouping = self.resolve_grouping_positions(grouping, &projections)?;
        let group_exprs = grouping.iter().map(|x| x.expr.clone()).collect::<Vec<_>>();
        let has_grouping_set = Self::has_grouping_set(&group_exprs);
        let grouping_exprs = Self::distinct_grouping_expressions_from_exprs(&group_exprs);
        let projections = projections
            .into_iter()
            .map(|x| Self::rewrite_grouping_functions(x, &grouping_exprs, has_grouping_set))
            .collect::<PlanResult<Vec<_>>>()?;
        let having = having
            .map(|having| Self::rewrite_grouping_expr(having, &grouping_exprs, has_grouping_set))
            .transpose()?;
        let mut aggregate_candidates = projections
            .iter()
            .map(|x| x.expr.clone())
            .collect::<Vec<_>>();
        if let Some(having) = having.as_ref() {
            aggregate_candidates.push(having.clone());
        }
        let aggregate_exprs = find_aggregate_exprs(&aggregate_candidates);
        let plan = LogicalPlanBuilder::from(input)
            .aggregate(group_exprs, aggregate_exprs.clone())?
            .build()?;
        // Phase 2: if this is a `session_window` aggregate with fusable aggregates,
        // fuse the `Aggregate` + `SessionWindowNode` into one `SessionAggregateNode`
        // (Spark's `MergingSessionsExec`). A no-op otherwise — including DISTINCT,
        // which stays on the baseline `SessionWindowNode` path (Spark's fallback).
        let plan = Self::maybe_fuse_session_aggregate(plan);
        let (grouping_exprs, aggregate_or_grouping_exprs) = {
            let mut grouping_exprs = vec![];
            let mut aggregate_or_grouping_exprs = aggregate_exprs;
            for expr in grouping {
                let NamedExpr {
                    name,
                    expr,
                    metadata,
                } = expr;
                let exprs = match expr {
                    Expr::GroupingSet(g) => g.distinct_expr().into_iter().cloned().collect(),
                    expr => vec![expr],
                };
                if name.len() != exprs.len() {
                    return Err(PlanError::internal(format!(
                        "group-by name count does not match expression count: {name:?} {exprs:?}",
                    )));
                }
                grouping_exprs.extend(exprs.iter().zip(name).map(|(expr, name)| NamedExpr {
                    name: vec![name],
                    expr: expr.clone(),
                    metadata: metadata.clone(),
                }));
                aggregate_or_grouping_exprs.extend(exprs);
            }
            (grouping_exprs, aggregate_or_grouping_exprs)
        };
        let projections = if with_grouping_expressions {
            grouping_exprs.into_iter().chain(projections).collect()
        } else {
            projections
        };
        let projections = projections
            .into_iter()
            .map(|x| {
                let NamedExpr {
                    name,
                    expr,
                    metadata,
                } = x;
                let expr = Self::rebase_expression(expr, &aggregate_or_grouping_exprs, &plan)?;
                Ok(NamedExpr {
                    name,
                    expr,
                    metadata,
                })
            })
            .collect::<PlanResult<Vec<_>>>()?;
        let plan = match having {
            Some(having) => {
                let having =
                    Self::rebase_expression(having.clone(), &aggregate_or_grouping_exprs, &plan)?;
                LogicalPlanBuilder::from(plan).having(having)?.build()?
            }
            None => plan,
        };
        let (plan, projections) =
            self.rewrite_projection::<MonotonicIdRewriter>(plan, projections, state)?;
        let (plan, projections) =
            self.rewrite_projection::<SparkPartitionIdRewriter>(plan, projections, state)?;
        let (plan, projections) =
            self.rewrite_projection::<ExplodeRewriter>(plan, projections, state)?;
        let (plan, projections) =
            self.rewrite_projection::<WindowRewriter>(plan, projections, state)?;
        let projections = projections
            .into_iter()
            .map(|x| {
                let NamedExpr {
                    name,
                    expr,
                    metadata: _,
                } = x;
                Ok(expr.alias(state.register_field_name(name.one()?)))
            })
            .collect::<PlanResult<Vec<_>>>()?;
        Ok(LogicalPlanBuilder::from(plan)
            .project(projections)?
            .build()?)
    }

    pub(crate) fn input_sort_ordering(input: &LogicalPlan) -> Option<Vec<SortExpr>> {
        Self::find_input_sort_ordering(input)?
            .into_iter()
            .map(|sort| {
                Some(SortExpr {
                    expr: normalize_col(sort.expr, input).ok()?,
                    asc: sort.asc,
                    nulls_first: sort.nulls_first,
                })
            })
            .collect()
    }

    fn find_input_sort_ordering(input: &LogicalPlan) -> Option<Vec<SortExpr>> {
        match input {
            LogicalPlan::Sort(sort) => Some(sort.expr.clone()),
            LogicalPlan::SubqueryAlias(alias) => {
                let ordering = Self::find_input_sort_ordering(alias.input.as_ref())?;
                ordering
                    .into_iter()
                    .map(|sort| {
                        let expr = sort
                            .expr
                            .transform(|expr| match expr {
                                Expr::Column(mut column) => {
                                    column.relation = Some(alias.alias.clone());
                                    Ok(Transformed::yes(Expr::Column(column)))
                                }
                                expr => Ok(Transformed::no(expr)),
                            })
                            .data()
                            .ok()?;
                        Some(SortExpr { expr, ..sort })
                    })
                    .collect()
            }
            LogicalPlan::Filter(filter) => Self::find_input_sort_ordering(filter.input.as_ref()),
            LogicalPlan::Limit(limit) => Self::find_input_sort_ordering(limit.input.as_ref()),
            LogicalPlan::Projection(projection) => {
                let ordering = Self::find_input_sort_ordering(projection.input.as_ref())?;
                ordering
                    .into_iter()
                    .map(|sort| {
                        let expr = Self::remap_through_projection(sort.expr, projection)?;
                        Some(SortExpr { expr, ..sort })
                    })
                    .collect()
            }
            LogicalPlan::Window(window) => {
                let [Expr::WindowFunction(function)] = window.window_expr.as_slice() else {
                    return None;
                };
                let mut required = function
                    .params
                    .partition_by
                    .iter()
                    .cloned()
                    .map(|expr| SortExpr {
                        expr,
                        asc: true,
                        nulls_first: true,
                    })
                    .collect::<Vec<_>>();
                required.extend(function.params.order_by.clone());

                let child = Self::find_input_sort_ordering(window.input.as_ref());
                if required.is_empty() {
                    child
                } else {
                    match child {
                        Some(child) if child.starts_with(&required) => Some(child),
                        _ => Some(required),
                    }
                }
            }
            LogicalPlan::Unnest(unnest) => {
                let ordering = Self::find_input_sort_ordering(unnest.input.as_ref())?;
                let references_unnested_column = ordering.iter().any(|sort| {
                    sort.expr.column_refs().iter().any(|column| {
                        unnest
                            .input
                            .schema()
                            .maybe_index_of_column(column)
                            .is_none_or(|index| {
                                unnest
                                    .list_type_columns
                                    .iter()
                                    .any(|(unnested, _)| *unnested == index)
                                    || unnest.struct_type_columns.contains(&index)
                            })
                    })
                });
                (!references_unnested_column).then_some(ordering)
            }
            LogicalPlan::Extension(extension) => {
                let node = extension.node.as_any();
                if let Some(sort) = node.downcast_ref::<RequiredSortNode>() {
                    Some(sort.sort_expr().to_vec())
                } else if let Some(sort) = node.downcast_ref::<SortWithinPartitionsNode>() {
                    Some(sort.sort_expr().to_vec())
                } else if let Some(node) = node.downcast_ref::<MonotonicIdNode>() {
                    Self::find_input_sort_ordering(node.input().as_ref())
                } else if let Some(node) = node.downcast_ref::<SparkPartitionIdNode>() {
                    Self::find_input_sort_ordering(node.input().as_ref())
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn remap_through_projection(expr: Expr, projection: &Projection) -> Option<Expr> {
        let rewritten = expr
            .transform_down(|expr| {
                match projection
                    .expr
                    .iter()
                    .position(|projection| projection.clone().unalias() == expr)
                {
                    Some(index) => {
                        let (qualifier, field) = projection.schema.qualified_field(index);
                        Ok(Transformed::new(
                            Expr::from(Column::from((qualifier, field))),
                            true,
                            TreeNodeRecursion::Jump,
                        ))
                    }
                    None => Ok(Transformed::no(expr)),
                }
            })
            .data()
            .ok()?;
        let mut derivable = true;
        let _ = rewritten.apply(|expr| {
            if let Expr::Column(column) = expr
                && projection
                    .schema
                    .qualified_field_from_column(column)
                    .is_err()
            {
                derivable = false;
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        });
        derivable.then_some(rewritten)
    }

    pub(crate) fn is_order_irrelevant_udaf(
        udf: &AggregateUDF,
        args: &[Expr],
        schema: &DFSchemaRef,
    ) -> PlanResult<bool> {
        if udf == min_max::min_udaf().as_ref()
            || udf == min_max::max_udaf().as_ref()
            || udf == count::count_udaf().as_ref()
            || udf == bit_and_or_xor::bit_and_udaf().as_ref()
            || udf == bit_and_or_xor::bit_or_udaf().as_ref()
            || udf == bit_and_or_xor::bit_xor_udaf().as_ref()
            || udf == bool_and_or::bool_and_udaf().as_ref()
            || udf == bool_and_or::bool_or_udaf().as_ref()
        {
            return Ok(true);
        }

        if udf == sum::sum_udaf().as_ref()
            || udf == average::avg_udaf().as_ref()
            || udf.inner().is::<SparkTrySum>()
            || udf.inner().is::<TryAvgFunction>()
        {
            let Some(arg) = args.first() else {
                return Ok(false);
            };
            return Ok(!matches!(
                arg.get_type(schema.as_ref())?,
                DataType::Float16 | DataType::Float32 | DataType::Float64
            ));
        }

        Ok(false)
    }

    fn requires_input_order(
        aggregates: &[Expr],
        schema: &DFSchemaRef,
        has_grouping: bool,
    ) -> PlanResult<bool> {
        for aggregate in aggregates {
            let Expr::AggregateFunction(function) = aggregate else {
                return Ok(true);
            };
            let udf = function.func.as_ref();
            if !has_grouping
                && (udf == sum::sum_udaf().as_ref() || udf == average::avg_udaf().as_ref())
            {
                continue;
            }
            if !Self::is_order_irrelevant_udaf(udf, &function.params.args, schema)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(crate) fn preserve_order_sensitive_aggregate_sorts(
        plan: LogicalPlan,
    ) -> PlanResult<LogicalPlan> {
        Ok(plan
            .transform_up_with_subqueries(|plan| {
                let LogicalPlan::Aggregate(mut aggregate) = plan else {
                    return Ok(Transformed::no(plan));
                };
                let aggregate_exprs = find_aggregate_exprs(&aggregate.aggr_expr);
                if !Self::requires_input_order(
                    &aggregate_exprs,
                    aggregate.input.schema(),
                    !aggregate.group_expr.is_empty(),
                )
                .map_err(|error| DataFusionError::External(Box::new(error)))?
                {
                    return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
                }

                let (input, found, _) = Self::require_input_sort_inner(Arc::unwrap_or_clone(
                    Arc::clone(&aggregate.input),
                ))
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
                if !found {
                    return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
                }
                aggregate.input = Arc::new(input);
                Ok(Transformed::yes(LogicalPlan::Aggregate(aggregate)))
            })
            .data()?)
    }

    pub(crate) fn require_input_sort_inner(
        plan: LogicalPlan,
    ) -> PlanResult<(LogicalPlan, bool, bool)> {
        match plan {
            LogicalPlan::Sort(sort) => Ok((
                LogicalPlan::Extension(Extension {
                    node: Arc::new(RequiredSortNode::new(
                        sort.input, sort.expr, sort.fetch, false,
                    )),
                }),
                true,
                true,
            )),
            LogicalPlan::Limit(mut limit) => {
                let fetch = match (limit.get_skip_type()?, limit.get_fetch_type()?) {
                    (SkipType::Literal(skip), FetchType::Literal(Some(fetch))) => Some(
                        skip.checked_add(fetch)
                            .ok_or_else(|| PlanError::invalid("LIMIT + OFFSET overflow"))?,
                    ),
                    _ => None,
                };
                let input = Arc::unwrap_or_clone(std::mem::replace(
                    &mut limit.input,
                    Arc::new(LogicalPlan::default()),
                ));
                let (input, found, global) = match fetch {
                    Some(fetch) => Self::require_input_sort_with_fetch(input, fetch)?,
                    None => Self::require_input_sort_inner(input)?,
                };
                limit.input = Arc::new(input);
                Ok((LogicalPlan::Limit(limit), found, global))
            }
            LogicalPlan::Extension(extension) => {
                if let Some(sort) = extension.node.as_any().downcast_ref::<RequiredSortNode>() {
                    let global = !sort.preserve_partitioning();
                    return Ok((LogicalPlan::Extension(extension), true, global));
                }
                if let Some(sort) = extension
                    .node
                    .as_any()
                    .downcast_ref::<SortWithinPartitionsNode>()
                {
                    return Ok((
                        LogicalPlan::Extension(Extension {
                            node: Arc::new(RequiredSortNode::new(
                                Arc::clone(sort.input()),
                                sort.sort_expr().to_vec(),
                                sort.fetch(),
                                true,
                            )),
                        }),
                        true,
                        false,
                    ));
                }
                if !extension.node.as_any().is::<MonotonicIdNode>()
                    && !extension.node.as_any().is::<SparkPartitionIdNode>()
                {
                    return Ok((LogicalPlan::Extension(extension), false, false));
                }

                let plan = LogicalPlan::Extension(extension);
                let expressions = plan.expressions();
                let child = plan.inputs().one()?.clone();
                let (child, found, global) = Self::require_input_sort_inner(child)?;
                if found {
                    Ok((plan.with_new_exprs(expressions, vec![child])?, true, global))
                } else {
                    Ok((plan, false, false))
                }
            }
            plan => {
                let transparent = matches!(
                    plan,
                    LogicalPlan::SubqueryAlias(_)
                        | LogicalPlan::Projection(_)
                        | LogicalPlan::Filter(_)
                        | LogicalPlan::Window(_)
                        | LogicalPlan::Unnest(_)
                );
                if !transparent {
                    return Ok((plan, false, false));
                }

                // Unnest's with_new_exprs requires no expressions and rebuilds from its own exec_columns.
                let expressions = if matches!(&plan, LogicalPlan::Unnest(_)) {
                    vec![]
                } else {
                    plan.expressions()
                };
                let child = plan.inputs().one()?.clone();
                let (child, found, global) = Self::require_input_sort_inner(child)?;
                if found {
                    Ok((plan.with_new_exprs(expressions, vec![child])?, true, global))
                } else {
                    Ok((plan, false, false))
                }
            }
        }
    }

    fn require_input_sort_with_fetch(
        plan: LogicalPlan,
        fetch: usize,
    ) -> PlanResult<(LogicalPlan, bool, bool)> {
        match plan {
            LogicalPlan::Sort(sort) => Ok((
                LogicalPlan::Extension(Extension {
                    node: Arc::new(RequiredSortNode::new(
                        sort.input,
                        sort.expr,
                        Some(sort.fetch.map_or(fetch, |old| old.min(fetch))),
                        false,
                    )),
                }),
                true,
                true,
            )),
            plan => {
                if !matches!(
                    plan,
                    LogicalPlan::Projection(_) | LogicalPlan::SubqueryAlias(_)
                ) {
                    return Self::require_input_sort_inner(plan);
                }

                let expressions = plan.expressions();
                let child = plan.inputs().one()?.clone();
                let (child, found, global) = Self::require_input_sort_with_fetch(child, fetch)?;
                if found {
                    Ok((plan.with_new_exprs(expressions, vec![child])?, true, global))
                } else {
                    Ok((plan, false, false))
                }
            }
        }
    }

    pub(super) fn has_grouping_set(grouping: &[Expr]) -> bool {
        grouping.iter().any(|x| matches!(x, Expr::GroupingSet(_)))
    }

    pub(super) fn distinct_grouping_expressions_from_exprs(grouping: &[Expr]) -> Vec<Expr> {
        grouping
            .iter()
            .flat_map(|x| match x {
                Expr::GroupingSet(g) => g.distinct_expr().into_iter().cloned().collect(),
                expr => vec![expr.clone()],
            })
            .collect()
    }

    /// Resolves the projections against the input. If that fails, resolves each one
    /// individually and defers (by index) those that cannot resolve yet because they
    /// reference a grouping output not in scope until the grouping is materialized.
    async fn resolve_projections_deferring(
        &self,
        projections: &[spec::Expr],
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<ResolvedProjections> {
        if let Ok(resolved) = self
            .resolve_named_expressions(projections.to_vec(), schema, state)
            .await
        {
            return Ok((resolved.into_iter().map(Some).collect(), vec![]));
        }
        let mut resolved = Vec::with_capacity(projections.len());
        let mut deferred = vec![];
        for (index, projection) in projections.iter().enumerate() {
            match self
                .resolve_named_expression(projection.clone(), schema, state)
                .await
            {
                Ok(named) => resolved.push(Some(named)),
                Err(_) => {
                    resolved.push(None);
                    deferred.push(index);
                }
            }
        }
        Ok((resolved, deferred))
    }

    /// Resolves the deferred projections (the grouping columns are now in scope) and
    /// inlines re-used generator expressions, producing the final projection list.
    async fn finish_projections(
        &self,
        mut resolved: Vec<Option<NamedExpr>>,
        deferred: Vec<usize>,
        projections: &[spec::Expr],
        generator_replacements: &[(Expr, Expr)],
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<NamedExpr>> {
        for index in deferred {
            resolved[index] = Some(
                self.resolve_named_expression(projections[index].clone(), schema, state)
                    .await?,
            );
        }
        resolved
            .into_iter()
            .enumerate()
            .map(|(index, named)| {
                let NamedExpr {
                    name,
                    expr,
                    metadata,
                } = named.ok_or_else(|| {
                    PlanError::internal(format!("projection {index} was not resolved"))
                })?;
                Ok(NamedExpr {
                    name,
                    expr: Self::replace_generator_expressions(expr, generator_replacements)?,
                    metadata,
                })
            })
            .collect()
    }

    fn rewrite_grouping_functions(
        named_expr: NamedExpr,
        grouping_exprs: &[Expr],
        has_grouping_set: bool,
    ) -> PlanResult<NamedExpr> {
        let NamedExpr {
            name,
            expr,
            metadata,
        } = named_expr;
        Ok(NamedExpr {
            name,
            expr: Self::rewrite_grouping_expr(expr, grouping_exprs, has_grouping_set)?,
            metadata,
        })
    }

    pub(super) fn rewrite_grouping_expr(
        expr: Expr,
        grouping_exprs: &[Expr],
        has_grouping_set: bool,
    ) -> PlanResult<Expr> {
        Ok(expr
            .transform_down(|expr| {
                if let Expr::AggregateFunction(function) = expr {
                    match function.func.name() {
                        "grouping" => Ok(Transformed::yes(Self::grouping_on_grouping_id(
                            function,
                            grouping_exprs,
                            has_grouping_set,
                        )?)),
                        "grouping_id" => Ok(Transformed::yes(Self::grouping_id_on_grouping_id(
                            function,
                            grouping_exprs,
                            has_grouping_set,
                        )?)),
                        _ => Ok(Transformed::no(Expr::AggregateFunction(function))),
                    }
                } else {
                    Ok(Transformed::no(expr))
                }
            })
            .data()?)
    }

    fn grouping_id_column() -> Expr {
        Expr::Column(Column::from(Aggregate::INTERNAL_GROUPING_ID))
    }

    fn grouping_bitmask_literal(value: u64, grouping_expr_count: usize) -> Expr {
        let value = if grouping_expr_count <= 8 {
            ScalarValue::UInt8(Some(value as u8))
        } else if grouping_expr_count <= 16 {
            ScalarValue::UInt16(Some(value as u16))
        } else if grouping_expr_count <= 32 {
            ScalarValue::UInt32(Some(value as u32))
        } else {
            ScalarValue::UInt64(Some(value))
        };
        Expr::Literal(value, None)
    }

    fn format_grouping_exprs(exprs: &[Expr]) -> String {
        exprs
            .iter()
            .map(|expr| expr.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn reject_grouping_clause(
        function: &datafusion_expr::expr::AggregateFunction,
    ) -> DataFusionResult<()> {
        if function.params.distinct
            || function.params.filter.is_some()
            || !function.params.order_by.is_empty()
            || function.params.null_treatment.is_some()
        {
            Err(DataFusionError::Plan(format!(
                "invalid {} function clause",
                function.func.name()
            )))
        } else {
            Ok(())
        }
    }

    fn grouping_on_grouping_id(
        function: datafusion_expr::expr::AggregateFunction,
        grouping_exprs: &[Expr],
        has_grouping_set: bool,
    ) -> DataFusionResult<Expr> {
        Self::reject_grouping_clause(&function)?;
        if !has_grouping_set {
            return Err(DataFusionError::Plan(
                "[UNSUPPORTED_GROUPING_EXPRESSION] grouping functions can only be used with grouping sets, cube, or rollup".to_string(),
            ));
        }
        let args = &function.params.args;
        let [arg] = args.as_slice() else {
            return Err(DataFusionError::Plan(
                "grouping requires exactly one argument".to_string(),
            ));
        };
        let position = grouping_exprs
            .iter()
            .position(|expr| expr == arg)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "[GROUPING_COLUMN_MISMATCH] grouping column {} is not in grouping columns ({})",
                    arg,
                    Self::format_grouping_exprs(grouping_exprs)
                ))
            })?;
        let bitmap_index = grouping_exprs.len() - 1 - position;
        let group_bit = bitwise_and(
            Self::grouping_id_column(),
            Self::grouping_bitmask_literal(1_u64 << bitmap_index, grouping_exprs.len()),
        );
        let shifted = if bitmap_index == 0 {
            group_bit
        } else {
            bitwise_shift_right(
                group_bit,
                Self::grouping_bitmask_literal(bitmap_index as u64, grouping_exprs.len()),
            )
        };
        Ok(cast(
            shifted,
            datafusion_common::arrow::datatypes::DataType::Int8,
        ))
    }

    fn grouping_id_on_grouping_id(
        function: datafusion_expr::expr::AggregateFunction,
        grouping_exprs: &[Expr],
        has_grouping_set: bool,
    ) -> DataFusionResult<Expr> {
        Self::reject_grouping_clause(&function)?;
        if !has_grouping_set {
            return Err(DataFusionError::Plan(
                "[UNSUPPORTED_GROUPING_EXPRESSION] grouping functions can only be used with grouping sets, cube, or rollup".to_string(),
            ));
        }
        if grouping_exprs.len() > 64 {
            return Err(DataFusionError::Plan(
                "[GROUPING_SIZE_LIMIT_EXCEEDED] grouping set size cannot be greater than 64"
                    .to_string(),
            ));
        }
        let args = &function.params.args;
        if !args.is_empty() && args.as_slice() != grouping_exprs {
            return Err(DataFusionError::Plan(format!(
                "[GROUPING_ID_COLUMN_MISMATCH] grouping_id columns ({}) do not match grouping columns ({})",
                Self::format_grouping_exprs(args),
                Self::format_grouping_exprs(grouping_exprs)
            )));
        }
        Ok(cast(
            Self::grouping_id_column(),
            datafusion_common::arrow::datatypes::DataType::Int64,
        ))
    }

    /// Single entry point for desugaring special grouping expressions into
    /// materialized grouping columns. Each expander detects its own marker and
    /// is a no-op otherwise. Returns the rewritten plan and grouping, plus the
    /// map from each original expression to its column (for SELECT/HAVING).
    fn expand_grouping(
        &self,
        input: LogicalPlan,
        grouping: Vec<NamedExpr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<(LogicalPlan, Vec<NamedExpr>, GeneratorReplacements)> {
        // Generators run first so that combining `window` with `session_window`
        // is caught downstream. New grouping functions append an expander here.
        let expanders: &[GroupingExpander<'_>] = &[
            Self::expand_grouping_generators,
            Self::expand_session_window,
        ];
        let mut acc = (input, grouping, GeneratorReplacements::new());
        for expand in expanders {
            acc = expand(self, acc.0, acc.1, acc.2, state)?;
        }
        Ok(acc)
    }

    /// Expands a generator in the grouping into rows, naming the unnested
    /// column after the grouping output. A no-op without a generator.
    fn expand_grouping_generators(
        &self,
        input: LogicalPlan,
        grouping: Vec<NamedExpr>,
        mut replacements: GeneratorReplacements,
        state: &mut PlanResolverState,
    ) -> PlanResult<(LogicalPlan, Vec<NamedExpr>, GeneratorReplacements)> {
        if !grouping.iter().any(Self::grouping_has_generator) {
            return Ok((input, grouping, replacements));
        }
        let generators = grouping.iter().map(|x| x.expr.clone()).collect::<Vec<_>>();
        let (input, mut grouping) =
            self.rewrite_projection::<ExplodeRewriter>(input, grouping, state)?;
        for (group, generator) in grouping.iter_mut().zip(generators) {
            // The rewriter returns the unnested column wrapped in an alias. Rename
            // that column to the grouping's output name and use it directly.
            let column = match (&group.expr, group.name.as_slice()) {
                (Expr::Alias(alias), [name]) => match alias.expr.as_ref() {
                    Expr::Column(column) => {
                        state.set_field_name(column.name(), name);
                        Some(Expr::Column(column.clone()))
                    }
                    _ => None,
                },
                _ => None,
            };
            if let Some(column) = column {
                replacements.push((generator, column.clone()));
                group.expr = column;
            }
        }
        Ok((input, grouping, replacements))
    }

    /// Whether a grouping expression contains a generator (e.g. `explode`) and so
    /// must expand the input rows before grouping.
    fn grouping_has_generator(group: &NamedExpr) -> bool {
        group
            .expr
            .exists(|e| Ok(matches!(e, Expr::ScalarFunction(f) if f.func.inner().is::<Explode>())))
            .unwrap_or(false)
    }

    /// Replaces each generator expression with a reference to its materialized
    /// grouping column, so a re-used generator resolves to the same column.
    fn replace_generator_expressions(
        expr: Expr,
        replacements: &[(Expr, Expr)],
    ) -> PlanResult<Expr> {
        if replacements.is_empty() {
            return Ok(expr);
        }
        Ok(expr
            .transform_down(|e| match replacements.iter().find(|(from, _)| *from == e) {
                Some((_, to)) => Ok(Transformed::yes(to.clone())),
                None => Ok(Transformed::no(e)),
            })
            .data()?)
    }

    /// Fuses an `Aggregate` directly on a `SessionWindowNode` into a single
    /// `SessionAggregateNode` (Spark's `MergingSessionsExec`) when the
    /// aggregates allow it; otherwise returns the plan untouched.
    fn maybe_fuse_session_aggregate(plan: LogicalPlan) -> LogicalPlan {
        let LogicalPlan::Aggregate(agg) = &plan else {
            return plan;
        };
        let LogicalPlan::Extension(ext) = agg.input.as_ref() else {
            return plan;
        };
        let Some(node) = ext.node.as_any().downcast_ref::<SessionWindowNode>() else {
            return plan;
        };
        if !Self::session_fusable(&agg.aggr_expr) {
            return plan;
        }
        // The fused node's input is the SessionWindowNode's child, which lacks
        // the session struct; an aggregate (or its FILTER) referencing it —
        // e.g. `max(session_window(...).start)` — must stay on the baseline
        // path where the struct exists.
        let input_has = |name: &str| {
            node.input()
                .schema()
                .field_with_unqualified_name(name)
                .is_ok()
        };
        let references_missing_column = agg.aggr_expr.iter().any(|e| {
            e.column_refs()
                .iter()
                .any(|c| c.name == node.output_column() || !input_has(&c.name))
        });
        if references_missing_column {
            return plan;
        }
        // The leading `group_expr.len()` output fields are the group columns (the
        // session struct among them), in order.
        let group_columns = agg
            .schema
            .fields()
            .iter()
            .take(agg.group_expr.len())
            .map(|f| f.name().clone())
            .collect::<Vec<_>>();
        let fused = SessionAggregateNode::new(
            Arc::clone(node.input()),
            node.partition_columns().to_vec(),
            node.time_column().to_string(),
            node.end_column().to_string(),
            node.output_column().to_string(),
            group_columns,
            agg.aggr_expr.clone(),
            Arc::clone(&agg.schema),
        );
        LogicalPlan::Extension(Extension {
            node: Arc::new(fused),
        })
    }

    /// Whether every aggregate can run through the fused session operator.
    /// DISTINCT, ordered-set, and group UDAFs fall back to the baseline
    /// `SessionWindowNode` path (as Spark does); `FILTER (WHERE)` is fused.
    fn session_fusable(aggr_exprs: &[Expr]) -> bool {
        aggr_exprs.iter().all(|e| {
            let inner = match e {
                Expr::Alias(alias) => alias.expr.as_ref(),
                other => other,
            };
            match inner {
                Expr::AggregateFunction(af) => {
                    !af.params.distinct
                        && af.params.order_by.is_empty()
                        && af
                            .func
                            .inner()
                            .downcast_ref::<PySparkGroupAggregateUDF>()
                            .is_none()
                }
                _ => false,
            }
        })
    }

    /// Whether an expression is a top-level `session_window` marker call.
    fn is_session_window_marker(expr: &Expr) -> bool {
        // Grouping by a projection alias (`GROUP BY sw`, `.alias("sw")`) keeps
        // the `Expr::Alias` wrapper around the marker, so peel one layer.
        let inner = match expr {
            Expr::Alias(alias) => alias.expr.as_ref(),
            other => other,
        };
        matches!(inner, Expr::ScalarFunction(f)
            if f.func.inner().downcast_ref::<SparkSessionWindow>().is_some())
    }

    /// Whether an expression contains a `session_window` marker anywhere.
    fn contains_session_window(expr: &Expr) -> bool {
        expr.exists(|e| Ok(Self::is_session_window_marker(e)))
            .unwrap_or(false)
    }

    /// Rewrites a `session_window` grouping marker into a `SessionWindowNode`
    /// appending the `{start, end}` struct column. A no-op without a marker.
    ///
    /// ```text
    /// SessionWindowNode partition_by=[K...] time=#t end=#e0 output=#w
    ///   Filter: #t IS NOT NULL AND #e0 > #t        (drop null time / non-positive gap)
    ///     Projection: *, ts AS #t, ts + gap AS #e0
    /// ```
    /// The marker grouping expression becomes a reference to `#w`.
    fn expand_session_window(
        &self,
        input: LogicalPlan,
        mut grouping: Vec<NamedExpr>,
        mut replacements: GeneratorReplacements,
        state: &mut PlanResolverState,
    ) -> PlanResult<(LogicalPlan, Vec<NamedExpr>, GeneratorReplacements)> {
        let Some(marker_idx) = grouping
            .iter()
            .position(|g| Self::is_session_window_marker(&g.expr))
        else {
            // A marker inside a larger grouping expression gets per-row
            // semantics in Spark (no merge); Sail does not implement that path,
            // so reject it at analysis time.
            if grouping
                .iter()
                .any(|g| Self::contains_session_window(&g.expr))
            {
                return Err(PlanError::AnalysisError(
                    "session_window is only supported as a top-level grouping expression"
                        .to_string(),
                ));
            }
            return Ok((input, grouping, replacements));
        };

        // Only one time-window expression per grouping; `session_window` cannot
        // be combined with `window` (its generator fills `replacements`).
        let extra_window = grouping
            .iter()
            .enumerate()
            .any(|(i, g)| i != marker_idx && Self::contains_session_window(&g.expr));
        if extra_window || !replacements.is_empty() {
            return Err(PlanError::AnalysisError(
                "only one session_window or window expression is allowed in a grouping".to_string(),
            ));
        }

        // Non-marker grouping keys form the session partition. An expression key
        // is projected to a fresh column so the operator can partition by it;
        // the grouping and SELECT/HAVING re-uses point at that same column, so
        // the downstream aggregate reuses the hash distribution (no reshuffle).
        let mut partition_columns = Vec::new();
        let mut key_projections: Vec<Expr> = Vec::new();
        for (i, g) in grouping.iter_mut().enumerate() {
            if i == marker_idx {
                continue;
            }
            match &g.expr {
                Expr::Column(col) => partition_columns.push(col.name().to_string()),
                // A grouping-set sibling cannot be materialized as a key
                // column; reject it cleanly instead of a downstream internal
                // error (and silently destroyed CUBE/ROLLUP semantics).
                Expr::GroupingSet(_) => {
                    return Err(PlanError::AnalysisError(
                        "session_window cannot be combined with GROUPING SETS, CUBE, or ROLLUP"
                            .to_string(),
                    ));
                }
                _ => {
                    let key_name = state.register_field_name("");
                    key_projections.push(g.expr.clone().alias(&key_name));
                    partition_columns.push(key_name.clone());
                    let key_col = ident(&key_name);
                    replacements.push((g.expr.clone(), key_col.clone()));
                    g.expr = key_col;
                }
            }
        }

        // Pull the (already cast) time column and normalized gap interval out of
        // the marker call (peeling a projection alias if present).
        let marker = match &grouping[marker_idx].expr {
            Expr::Alias(alias) => alias.expr.as_ref(),
            other => other,
        };
        let (time_ts, gap_interval) = match marker {
            Expr::ScalarFunction(f) => {
                let mut args = f.args.iter().cloned();
                match (args.next(), args.next()) {
                    (Some(t), Some(g)) => (t, g),
                    _ => {
                        return Err(PlanError::internal(
                            "session_window marker is missing arguments",
                        ));
                    }
                }
            }
            _ => {
                return Err(PlanError::internal(
                    "session_window marker is not a scalar function",
                ));
            }
        };

        // Project every input column through, then add `#t = ts` and the per-row
        // session-end candidate `#e0 = ts + gap`.
        let t_name = state.register_field_name("");
        let e0_name = state.register_field_name("");
        let mut proj: Vec<Expr> = input
            .schema()
            .columns()
            .into_iter()
            .map(Expr::Column)
            .collect();
        // Materialize any expression grouping keys as fresh columns alongside.
        proj.extend(key_projections);
        proj.push(time_ts.clone().alias(&t_name));
        proj.push((time_ts + gap_interval).alias(&e0_name));
        let projected = LogicalPlanBuilder::from(input).project(proj)?.build()?;

        // Drop the rows Spark's `SessionWindowing` rule drops: null time, or a
        // non-positive gap (`end <= time`).
        let t_col = ident(&t_name);
        let e0_col = ident(&e0_name);
        let filtered = LogicalPlanBuilder::from(projected)
            .filter(t_col.clone().is_not_null().and(e0_col.gt(t_col)))?
            .build()?;

        // Append the session `{start, end}` struct column.
        let w_name = state.register_field_name("");
        let node = SessionWindowNode::try_new(
            Arc::new(filtered),
            partition_columns,
            t_name,
            e0_name,
            w_name.clone(),
        )?;
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        });

        // Name the struct column after the grouping output (`session_window`) so
        // `session_window.start` / `.end` resolve, then point the grouping (and any
        // re-use of the marker in SELECT/HAVING) at that column.
        if let [display_name] = grouping[marker_idx].name.as_slice() {
            state.set_field_name(&w_name, display_name);
        }
        let w_col = ident(&w_name);
        let original_marker = grouping[marker_idx].expr.clone();
        // Register both the grouping form and (when aliased) the bare marker,
        // so SELECT/HAVING re-uses of either shape resolve to the struct.
        if let Expr::Alias(alias) = &original_marker {
            replacements.push((alias.expr.as_ref().clone(), w_col.clone()));
        }
        replacements.push((original_marker, w_col.clone()));
        grouping[marker_idx].expr = w_col;

        Ok((plan, grouping, replacements))
    }

    /// Reference: [datafusion_sql::utils::rebase_expr]
    pub(super) fn rebase_expression(
        expr: Expr,
        base: &[Expr],
        plan: &LogicalPlan,
    ) -> PlanResult<Expr> {
        Ok(expr
            .transform_down(|e| {
                if base.contains(&e) {
                    Ok(Transformed::yes(
                        Self::expr_as_column_expr(&e, plan)
                            .map_err(|e| DataFusionError::External(Box::new(e)))?,
                    ))
                } else {
                    Ok(Transformed::no(e))
                }
            })
            .data()?)
    }

    // Modification of DataFusion's `expr_as_column_expr`
    fn expr_as_column_expr(expr: &Expr, plan: &LogicalPlan) -> PlanResult<Expr> {
        match expr {
            Expr::Column(column) => {
                let result = plan
                    .schema()
                    .qualified_field_from_column(column)
                    .or_else(|_| {
                        let column = Column::new_unqualified(column.name.clone());
                        plan.schema().qualified_field_from_column(&column)
                    })?;
                let (qualifier, field) = result;
                Ok(Expr::from(Column::from((qualifier, field))))
            }
            _ => Ok(Expr::Column(Column::from_name(
                expr.schema_name().to_string(),
            ))),
        }
    }

    /// Spark CheckAnalysis: GroupedAgg Pandas/Arrow UDFs cannot be mixed with regular
    /// (non-UDF) aggregate functions in the same `.agg()` call.
    fn check_no_mixed_grouped_agg_udf(projections: &[NamedExpr]) -> PlanResult<()> {
        let mut pyspark_agg_name: Option<String> = None;
        let mut has_regular_agg = false;
        for proj in projections {
            let _ = proj.expr.apply(|e| {
                if let Expr::AggregateFunction(agg) = e {
                    if agg.func.inner().is::<PySparkGroupAggregateUDF>() {
                        if pyspark_agg_name.is_none() {
                            let full = agg.func.name();
                            pyspark_agg_name = Some(get_udf_display_name(full).to_string());
                        }
                    } else {
                        has_regular_agg = true;
                    }
                    // Don't recurse into the aggregate's args — no nested aggs here
                    return Ok(TreeNodeRecursion::Jump);
                }
                Ok(TreeNodeRecursion::Continue)
            });
        }
        if let Some(udf_name) = pyspark_agg_name
            && has_regular_agg
        {
            return Err(PlanError::AnalysisError(format!(
                // Spark tests expect this error message. Typo is intended.
                "The group aggregate pandas UDF `{udf_name}` cannot be invoked \
                     together with as other, non-pandas aggregate functions."
            )));
        }
        Ok(())
    }
}
