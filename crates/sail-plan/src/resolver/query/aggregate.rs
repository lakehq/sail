use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion_common::{
    Column, DFSchemaRef, DataFusionError, Result as DataFusionResult, ScalarValue,
};
use datafusion_expr::utils::{expr_as_column_expr, find_aggregate_exprs};
use datafusion_expr::{
    bitwise_and, bitwise_shift_right, cast, Aggregate, Expr, LogicalPlan, LogicalPlanBuilder,
    Volatility,
};
use sail_common::spec;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::explode::Explode;
use sail_python_udf::get_udf_display_name;
use sail_python_udf::udf::pyspark_udaf::PySparkGroupAggregateUDF;

use crate::error::{PlanError, PlanResult};
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::{AggregateState, PlanResolverState};
use crate::resolver::tree::explode::ExplodeRewriter;
use crate::resolver::tree::monotonic_id::MonotonicIdRewriter;
use crate::resolver::tree::spark_partition_id::SparkPartitionIdRewriter;
use crate::resolver::tree::window::WindowRewriter;
use crate::resolver::PlanResolver;

/// Projections resolved by index (a `None` marks a projection deferred until the
/// grouping is materialized), paired with the indices of the deferred projections.
type ResolvedProjections = (Vec<Option<NamedExpr>>, Vec<usize>);

/// A map from a grouping generator expression to the column that materializes it.
type GeneratorReplacements = Vec<(Expr, Expr)>;

/// Returns the name of a volatile (non-deterministic) scalar expression found
/// in an aggregate context. Catches two Spark CheckAnalysis violations:
/// 1. Volatile scalar UDF used directly in aggregate projections (outside any aggregate fn)
/// 2. Volatile scalar UDF nested inside aggregate function arguments
fn find_volatile_in_aggregate_context(expr: &Expr) -> Option<String> {
    let mut found_name: Option<String> = None;
    let _ = expr.apply(|e| {
        if let Expr::ScalarFunction(f) = e {
            if f.func.signature().volatility == Volatility::Volatile {
                found_name = Some(f.func.name().to_string());
                return Ok(TreeNodeRecursion::Stop);
            }
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
            let grouping_scope = resolved_projections.iter().flatten().cloned().collect();
            let mut scope = state.enter_aggregate_scope(AggregateState::Grouping {
                projections: grouping_scope,
            });
            let state = scope.state();
            self.resolve_named_expressions(grouping, schema, state)
                .await?
        };

        // Expand a generator (e.g. window's `explode`) in the grouping into a named
        // column; a no-op for ordinary aggregates.
        let (input, grouping, generator_replacements) =
            self.expand_grouping_generators(input, grouping, state)?;
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

    /// Expands a generator in the grouping into rows, naming the unnested column
    /// after the grouping output. Returns a map from each generator to its column.
    /// A no-op when the grouping has no generator.
    fn expand_grouping_generators(
        &self,
        input: LogicalPlan,
        grouping: Vec<NamedExpr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<(LogicalPlan, Vec<NamedExpr>, GeneratorReplacements)> {
        if !grouping.iter().any(Self::grouping_has_generator) {
            return Ok((input, grouping, vec![]));
        }
        let generators = grouping.iter().map(|x| x.expr.clone()).collect::<Vec<_>>();
        let (input, mut grouping) =
            self.rewrite_projection::<ExplodeRewriter>(input, grouping, state)?;
        let mut replacements = vec![];
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
            .exists(|e| {
                Ok(matches!(e, Expr::ScalarFunction(f) if f.func.inner().as_any().is::<Explode>()))
            })
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

    /// Reference: [datafusion_sql::utils::rebase_expr]
    pub(super) fn rebase_expression(
        expr: Expr,
        base: &[Expr],
        plan: &LogicalPlan,
    ) -> PlanResult<Expr> {
        Ok(expr
            .transform_down(|e| {
                if base.contains(&e) {
                    Ok(Transformed::yes(expr_as_column_expr(&e, plan)?))
                } else {
                    Ok(Transformed::no(e))
                }
            })
            .data()?)
    }

    /// Spark CheckAnalysis: GroupedAgg Pandas/Arrow UDFs cannot be mixed with regular
    /// (non-UDF) aggregate functions in the same `.agg()` call.
    fn check_no_mixed_grouped_agg_udf(projections: &[NamedExpr]) -> PlanResult<()> {
        let mut pyspark_agg_name: Option<String> = None;
        let mut has_regular_agg = false;
        for proj in projections {
            let _ = proj.expr.apply(|e| {
                if let Expr::AggregateFunction(agg) = e {
                    if agg
                        .func
                        .inner()
                        .downcast_ref::<PySparkGroupAggregateUDF>()
                        .is_some()
                    {
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
        if let Some(udf_name) = pyspark_agg_name {
            if has_regular_agg {
                return Err(PlanError::AnalysisError(format!(
                    // Spark tests expect this error message. Typo is intended.
                    "The group aggregate pandas UDF `{udf_name}` cannot be invoked \
                     together with as other, non-pandas aggregate functions."
                )));
            }
        }
        Ok(())
    }
}
