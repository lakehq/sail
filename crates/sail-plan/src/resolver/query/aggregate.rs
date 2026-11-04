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
    Projection, SortExpr, Volatility, bitwise_and, bitwise_shift_right, cast,
};
use datafusion_spark::function::aggregate::try_sum::SparkTrySum;
use sail_common::spec;
use sail_common_datafusion::input_file::is_input_file_metadata_function;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::aggregate::try_avg::TryAvgFunction;
use sail_function::scalar::explode::Explode;
use sail_function::scalar::misc::monotonically_increasing_id::SparkMonotonicallyIncreasingId;
use sail_function::scalar::misc::scalar_subquery_result::ScalarSubqueryResult;
use sail_function::scalar::misc::spark_partition_id::SparkPartitionId;
use sail_logical_plan::monotonic_id::MonotonicIdNode;
use sail_logical_plan::sort::{RequiredSortNode, SortWithinPartitionsNode};
use sail_logical_plan::spark_partition_id::SparkPartitionIdNode;
use sail_python_udf::get_udf_display_name;
use sail_python_udf::udf::pyspark_udaf::PySparkGroupAggregateUDF;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::{AggregateState, PlanResolverState};
use crate::resolver::tree::exists::ExistsRewriter;
use crate::resolver::tree::explode::ExplodeRewriter;
use crate::resolver::tree::monotonic_id::MonotonicIdRewriter;
use crate::resolver::tree::spark_partition_id::SparkPartitionIdRewriter;

/// Projections resolved by index, with typed columns for lateral dependencies.
/// A `None` marks a projection deferred until grouping is materialized.
struct ResolvedProjections {
    expressions: Vec<Option<NamedExpr>>,
    deferred: Vec<(usize, spec::Expr)>,
    lateral_columns: Vec<Column>,
    schema: DFSchemaRef,
}

struct AggregateProjections {
    expressions: Vec<NamedExpr>,
    lateral_columns: Vec<Column>,
}

/// A map from a grouping expression to the column that materializes it.
type GroupingReplacements = Vec<(Expr, Expr)>;

/// Find volatile scalar calls in an aggregate function's inputs, including
/// filters and ordering. Output expressions are evaluated after aggregation.
fn find_volatile_aggregate_input(expr: &Expr) -> Option<String> {
    let mut found_name: Option<String> = None;
    let _ = expr.apply(|e| {
        if let Expr::ScalarFunction(f) = e
            && f.func.signature().volatility == Volatility::Volatile
            && f.func.as_async().is_none()
            // This internal assertion is evaluation-sensitive, but does not
            // make the scalar subquery's value non-deterministic.
            && !f.func.inner().is::<ScalarSubqueryResult>()
        {
            found_name = Some(f.func.name().to_string());
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found_name
}

#[cfg(test)]
mod volatility_tests {
    use datafusion::functions::math::expr_fn::random;
    use datafusion::functions_aggregate::expr_fn::{count, sum};
    use datafusion::prelude::SessionContext;
    use datafusion_common::Result;
    use datafusion_expr::utils::find_aggregate_exprs;
    use datafusion_expr::{ExprFunctionExt, ScalarUDF, col, lit};

    use super::*;
    use crate::config::PlanConfig;

    #[test]
    fn volatile_output_stays_above_aggregate() -> PlanResult<()> {
        let context = SessionContext::new();
        let resolver = PlanResolver::new(&context, Arc::new(PlanConfig::new()?));
        let mut state = PlanResolverState::new();
        let id = state.register_field_name("id");
        let input = LogicalPlanBuilder::values(vec![vec![lit(1_i64)], vec![lit(2_i64)]])?
            .project(vec![col("column1").alias(&id)])?
            .build()?;
        let plan = resolver.rewrite_aggregate(
            input.clone(),
            vec![NamedExpr::new(vec!["c".into()], count(lit(1)) + random())],
            vec![NamedExpr::new(vec!["id".into()], col(id))],
            None,
            false,
            &mut state,
        )?;
        let LogicalPlan::Projection(projection) = plan else {
            return Err(PlanError::internal("expected output projection"));
        };
        assert!(projection.expr.iter().any(Expr::is_volatile));
        assert!(find_aggregate_exprs(&projection.expr).is_empty());
        let LogicalPlan::Aggregate(aggregate) = projection.input.as_ref() else {
            return Err(PlanError::internal(
                "expected aggregate below volatile output",
            ));
        };
        assert_eq!(aggregate.aggr_expr, vec![count(lit(1))]);
        assert_eq!(aggregate.input.as_ref(), &input);
        Ok(())
    }

    #[test]
    fn scalar_cardinality_assertion_is_not_nondeterminism() {
        let assertion = ScalarUDF::from(ScalarSubqueryResult::new()).call(vec![lit(1)]);
        assert!(find_volatile_aggregate_input(&sum(assertion.clone())).is_none());
        assert!(find_volatile_aggregate_input(&sum(assertion + random())).is_some());
    }

    #[test]
    fn output_volatility_is_not_an_aggregate_input() {
        let aggregates = find_aggregate_exprs(&[count(lit(1)) + random()]);
        assert_eq!(aggregates.len(), 1);
        assert!(find_volatile_aggregate_input(&aggregates[0]).is_none());
    }

    #[test]
    fn aggregate_arguments_filters_and_ordering_reject_volatility() -> Result<()> {
        let expressions: Vec<Expr> = vec![
            sum(random()),
            count(lit(1)).filter(random().gt(lit(0.5))).build()?,
            sum(lit(1))
                .order_by(vec![random().sort(true, true)])
                .build()?,
        ];
        for expr in expressions {
            assert_eq!(
                find_volatile_aggregate_input(&expr).as_deref(),
                Some("random")
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod grouping_tests {
    use datafusion::functions_aggregate::grouping::grouping;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::{GroupingSet, col, lit, out_ref_col};
    use sail_function::aggregate::grouping_id::GroupingIdFunction;

    use super::*;
    use crate::config::PlanConfig;

    #[test]
    #[expect(clippy::unwrap_used)]
    fn correlated_mask_is_materialized_at_having_stage() -> PlanResult<()> {
        let context = SessionContext::new();
        let resolver = PlanResolver::new(&context, Arc::new(PlanConfig::new()?));
        for data_type in [DataType::Int64, DataType::Float32] {
            let mut state = PlanResolverState::new();
            let name = state.register_field_name("k");
            let key = col(&name);
            let input = LogicalPlanBuilder::values(vec![vec![lit(1i64)], vec![lit(2i64)]])?
                .project(vec![col("column1").alias(&name)])?
                .build()?;
            let gid = AggregateUDF::from(GroupingIdFunction::new()).call(vec![key.clone()]);
            let plan = resolver.rewrite_aggregate(
                input,
                vec![NamedExpr::new(vec!["z".into()], key.clone())],
                vec![NamedExpr::new(
                    vec!["k".into()],
                    Expr::GroupingSet(GroupingSet::GroupingSets(vec![
                        vec![key.clone()],
                        vec![key],
                    ])),
                )],
                Some(gid.eq(out_ref_col(data_type.clone(), "o.g"))),
                false,
                &mut state,
            )?;
            let LogicalPlan::Projection(output) = &plan else {
                unreachable!()
            };
            let LogicalPlan::Filter(having) = output.input.as_ref() else {
                unreachable!()
            };
            let LogicalPlan::Projection(keys) = having.input.as_ref() else {
                unreachable!()
            };
            assert!(matches!(keys.input.as_ref(), LogicalPlan::Aggregate(_)));
            let Expr::BinaryExpr(comparison) = &having.predicate else {
                unreachable!()
            };
            let Expr::Column(key) = comparison.left.as_ref() else {
                unreachable!()
            };
            assert_eq!(comparison.right.as_ref(), &out_ref_col(data_type, "o.g"));
            assert_eq!(
                keys.schema
                    .field(keys.schema.index_of_column(key)?)
                    .data_type(),
                &DataType::Int64
            );
            assert_eq!(
                keys.expr.last().unwrap().clone().unalias(),
                cast(
                    bitwise_and(
                        cast(PlanResolver::grouping_id_column(), DataType::UInt64),
                        lit(1u64)
                    ),
                    DataType::Int64
                )
            );
            assert_eq!(plan.schema().fields().len(), 1);
            assert!(!plan.schema().has_column(key));
            let input = Arc::unwrap_or_clone(Arc::clone(&keys.input));
            let predicate = (keys.expr.last().unwrap().clone().unalias() + lit(1i64))
                .eq(out_ref_col(DataType::Int64, "o.g"));
            let (unchanged, retained) = PlanResolver::materialize_grouping_comparison(
                input.clone(),
                predicate.clone(),
                &mut state,
            )?;
            assert_eq!(unchanged, input);
            assert_eq!(retained, predicate);
            for compared_type in [DataType::Float32, DataType::Float64] {
                let value = cast(
                    keys.expr.last().unwrap().clone().unalias(),
                    compared_type.clone(),
                );
                let predicate = value.clone().eq(out_ref_col(compared_type.clone(), "o.g"));
                let (typed, predicate) = PlanResolver::materialize_grouping_comparison(
                    input.clone(),
                    predicate,
                    &mut state,
                )?;
                let LogicalPlan::Projection(typed) = typed else {
                    unreachable!()
                };
                assert_eq!(typed.input.as_ref(), &input);
                assert_eq!(typed.expr.last().unwrap().clone().unalias(), value);
                let Expr::BinaryExpr(comparison) = predicate else {
                    unreachable!()
                };
                let Expr::Column(key) = comparison.left.as_ref() else {
                    unreachable!()
                };
                assert_eq!(
                    typed
                        .schema
                        .field(typed.schema.index_of_column(key)?)
                        .data_type(),
                    &compared_type
                );
                let predicate = cast(
                    keys.expr.last().unwrap().clone().unalias() + lit(1i64),
                    compared_type.clone(),
                )
                .eq(out_ref_col(compared_type, "o.g"));
                let (unchanged, retained) = PlanResolver::materialize_grouping_comparison(
                    input.clone(),
                    predicate.clone(),
                    &mut state,
                )?;
                assert_eq!(unchanged, input);
                assert_eq!(retained, predicate);
            }
        }
        Ok(())
    }

    #[test]
    fn duplicate_grouping_reads_use_original_bits_with_or_without_correlation() -> PlanResult<()> {
        let context = SessionContext::new();
        let resolver = PlanResolver::new(&context, Arc::new(PlanConfig::new()?));
        for key_count in [0, 1, 8, 16, 32, 63] {
            for duplicate in [false, true] {
                for correlated in [false, true] {
                    let mut state = PlanResolverState::new();
                    let names = (0..key_count.max(1))
                        .map(|_| state.register_field_name("k"))
                        .collect::<Vec<_>>();
                    let input = LogicalPlanBuilder::values(vec![vec![lit(1i64)]])?
                        .project(names.iter().map(|name| col("column1").alias(name)))?
                        .filter(col(&names[0]).eq(if correlated {
                            out_ref_col(DataType::Int64, "outer.k")
                        } else {
                            lit(1i64)
                        }))?
                        .build()?;
                    let keys = names[..key_count].iter().map(col).collect::<Vec<_>>();
                    let mut sets = vec![keys.clone(), vec![]];
                    if duplicate {
                        sets.extend([keys.clone(), vec![]]);
                    }
                    let groups = vec![Expr::GroupingSet(GroupingSet::GroupingSets(sets))];
                    let repeated = PlanResolver::has_duplicate_grouping_sets(&groups)?;
                    let gid = AggregateUDF::from(GroupingIdFunction::new()).call(keys.clone());
                    let mut projections = vec![NamedExpr::new(vec!["gid".into()], gid)];
                    if !keys.is_empty() {
                        projections.push(NamedExpr::new(
                            vec!["bit".into()],
                            grouping(keys[0].clone()),
                        ));
                    }
                    let plan = resolver.rewrite_aggregate(
                        input,
                        projections,
                        vec![NamedExpr::new(
                            names[..key_count].to_vec(),
                            groups[0].clone(),
                        )],
                        None,
                        false,
                        &mut state,
                    )?;
                    let mask = if key_count == 0 {
                        0
                    } else {
                        u64::MAX >> (64 - key_count)
                    };
                    let expected = if repeated {
                        cast(
                            bitwise_and(
                                cast(PlanResolver::grouping_id_column(), DataType::UInt64),
                                lit(mask),
                            ),
                            DataType::Int64,
                        )
                    } else {
                        cast(PlanResolver::grouping_id_column(), DataType::Int64)
                    };
                    let mut found = false;
                    plan.apply(|node| {
                        if let LogicalPlan::Projection(projection) = node {
                            found |= projection
                                .expr
                                .iter()
                                .any(|expr| expr.clone().unalias() == expected);
                        }
                        if let LogicalPlan::Aggregate(aggregate) = node {
                            assert_eq!(aggregate.group_expr, groups);
                            let expected_type = Aggregate::grouping_id_type(
                                key_count,
                                if key_count == 0 {
                                    if duplicate { 3 } else { 1 }
                                } else {
                                    usize::from(duplicate)
                                },
                            );
                            assert_eq!(
                                aggregate.schema.field(key_count).data_type(),
                                &expected_type
                            );
                            assert!(!aggregate.schema.field(key_count).is_nullable());
                        }
                        Ok(TreeNodeRecursion::Continue)
                    })?;
                    assert!(
                        found,
                        "{key_count} {duplicate} {correlated}: {}",
                        plan.display_indent()
                    );
                }
            }
        }
        Ok(())
    }
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
        let ResolvedProjections {
            expressions: resolved_projections,
            deferred: deferred_projections,
            lateral_columns,
            schema: lateral_schema,
        } = self
            .resolve_projections_deferring(&projections, &input, state)
            .await?;

        let infer_grouping = match grouping.as_slice() {
            [spec::Expr::UnresolvedAttribute { name, .. }]
                if name.parts().len() == 1
                    && name.parts()[0].as_ref().eq_ignore_ascii_case("all") =>
            {
                let name = name.parts()[0].as_ref();
                let has_alias = self.config.group_by_aliases
                    && resolved_projections.iter().flatten().any(|projection| {
                        matches!(projection.name.as_slice(), [alias]
                            if self.merge_name_key(alias) == self.merge_name_key(name))
                    });
                !has_alias
                    && self
                        .resolve_column_candidates(schema, name, None, state)
                        .is_empty()
            }
            _ => false,
        };
        let grouping = if infer_grouping {
            let expanded = resolved_projections.iter().flatten().cloned().collect();
            let expanded = if lateral_columns.is_empty() {
                self.rewrite_wildcard(input.clone(), expanded, state)?.1
            } else {
                expanded
            };
            let mut aggregate_aliases = std::collections::HashSet::new();
            expanded
                .into_iter()
                .enumerate()
                .filter_map(|(index, named)| {
                    let aggregate = !find_aggregate_exprs(std::slice::from_ref(&named.expr))
                        .is_empty()
                        || named
                            .expr
                            .column_refs()
                            .iter()
                            .any(|column| aggregate_aliases.contains(*column));
                    if aggregate {
                        if let Some(column) = lateral_columns.get(index) {
                            aggregate_aliases.insert(column.clone());
                        }
                        None
                    } else {
                        Some(named)
                    }
                })
                .collect()
        } else {
            let projections = resolved_projections.iter().flatten().cloned().collect();
            let mut scope = state.enter_aggregate_scope(AggregateState::Grouping { projections });
            let state = scope.state();
            self.resolve_named_expressions(grouping, &lateral_schema, state)
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
                &generator_replacements,
                schema,
                state,
            )
            .await?;

        let (input, projections) = if lateral_columns.is_empty() {
            self.rewrite_wildcard(input, projections, state)?
        } else {
            (input, projections)
        };
        let grouping = if infer_grouping {
            grouping
        } else {
            self.resolve_grouping_positions(grouping, &projections)?
        };
        let (input, projections, grouping, lateral_replacements) =
            self.prepare_lateral_grouping(input, projections, grouping, &lateral_columns, state)?;
        let (input, projections, grouping) =
            self.prepare_aggregate_input(input, projections, grouping, state)?;
        let schema = input.schema();

        // Spark CheckAnalysis: GroupedAgg Pandas/Arrow UDFs cannot be mixed with regular
        // (non-UDF) aggregate functions in the same .agg() call.
        Self::check_no_mixed_grouped_agg_udf(&projections)?;

        let having = {
            let mut having_schema = schema.as_ref().clone();
            having_schema.merge(&lateral_schema);
            let having_schema = Arc::new(having_schema);
            let mut scope = state.enter_aggregate_scope(AggregateState::Having {
                projections: projections.clone(),
                grouping: grouping.clone(),
            });
            let state = scope.state();
            match having {
                Some(having) => Some(Self::replace_grouping_expressions(
                    Self::replace_grouping_expressions(
                        self.resolve_expression(having, &having_schema, state)
                            .await?,
                        &lateral_replacements,
                    )?,
                    &generator_replacements,
                )?),
                None => None,
            }
        };

        let (input, replacements) =
            Self::materialize_grouping_input_file_metadata(input, &grouping, state)?;
        let rewrite = |named: NamedExpr| -> PlanResult<NamedExpr> {
            Ok(NamedExpr {
                expr: Self::replace_grouping_expressions(named.expr, &replacements)?,
                ..named
            })
        };
        let grouping = grouping
            .into_iter()
            .map(rewrite)
            .collect::<PlanResult<_>>()?;
        let projections = projections
            .into_iter()
            .map(rewrite)
            .collect::<PlanResult<Vec<_>>>()?;
        let having = having
            .map(|expr| Self::replace_grouping_expressions(expr, &replacements))
            .transpose()?;

        self.rewrite_prepared_aggregate(
            input,
            AggregateProjections {
                expressions: projections,
                lateral_columns,
            },
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
        if !self.config.group_by_ordinal {
            return Ok(exprs);
        }
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
        let (input, projections, grouping) =
            self.prepare_aggregate_input(input, projections, grouping, state)?;
        self.rewrite_prepared_aggregate(
            input,
            AggregateProjections {
                expressions: projections,
                lateral_columns: vec![],
            },
            grouping,
            having,
            with_grouping_expressions,
            state,
        )
    }

    /// Materialize aliases needed by grouping keys on the input, keeping their
    /// dependency graph as columns instead of recursively expanding expressions.
    fn prepare_lateral_grouping(
        &self,
        input: LogicalPlan,
        projections: Vec<NamedExpr>,
        grouping: Vec<NamedExpr>,
        columns: &[Column],
        state: &mut PlanResolverState,
    ) -> PlanResult<(
        LogicalPlan,
        Vec<NamedExpr>,
        Vec<NamedExpr>,
        GroupingReplacements,
    )> {
        let mut required = grouping
            .iter()
            .flat_map(|named| named.expr.column_refs().into_iter().cloned())
            .collect::<std::collections::HashSet<_>>();
        for (column, named) in columns.iter().zip(&projections).rev() {
            if required.contains(column) {
                required.extend(named.expr.column_refs().into_iter().cloned());
            }
        }
        let mut expressions = vec![];
        let mut fields = vec![];
        let mut replacements = vec![];
        for (column, named) in columns.iter().zip(&projections) {
            if !required.contains(column) {
                continue;
            }
            if !find_aggregate_exprs(std::slice::from_ref(&named.expr)).is_empty() {
                return Err(PlanError::analysis(
                    "aggregate functions are not allowed in GROUP BY",
                ));
            }
            let value = Expr::Column(column.clone());
            replacements.push((named.expr.clone(), value.clone()));
            replacements.push((named.expr.clone().unalias(), value));
            expressions.push(named.clone());
            fields.push(column.clone());
        }
        if expressions.is_empty() {
            return Ok((input, projections, grouping, vec![]));
        }
        let (input, _) = self.materialize_lateral_expressions(input, expressions, fields, state)?;
        let replace = |named: NamedExpr| -> PlanResult<NamedExpr> {
            Ok(NamedExpr {
                expr: Self::replace_grouping_expressions(named.expr, &replacements)?,
                ..named
            })
        };
        let original_grouping = grouping;
        let grouping = original_grouping
            .iter()
            .cloned()
            .map(replace)
            .collect::<PlanResult<Vec<_>>>()?;
        // Only replace actual grouping keys in the output. Intermediate aliases
        // still need to be computed from grouped values above the aggregate.
        let replacements = original_grouping
            .iter()
            .zip(&grouping)
            .flat_map(|(original, grouped)| {
                [
                    (original.expr.clone(), grouped.expr.clone()),
                    (
                        original.expr.clone().unalias(),
                        grouped.expr.clone().unalias(),
                    ),
                ]
            })
            .collect::<Vec<_>>();
        let projections = projections
            .into_iter()
            .map(|named| {
                Ok(NamedExpr {
                    expr: Self::replace_grouping_expressions(named.expr, &replacements)?,
                    ..named
                })
            })
            .collect::<PlanResult<_>>()?;
        Ok((input, projections, grouping, replacements))
    }

    fn prepare_aggregate_input(
        &self,
        input: LogicalPlan,
        projections: Vec<NamedExpr>,
        grouping: Vec<NamedExpr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<(LogicalPlan, Vec<NamedExpr>, Vec<NamedExpr>)> {
        // Spark materializes non-deterministic grouping keys before CheckAnalysis,
        // and reuses those values wherever the grouping expressions occur.
        let mut partition_expressions = vec![];
        for group in &grouping {
            group.expr.apply(|expr| {
                if let Expr::ScalarFunction(function) = expr
                    && (function.func.inner().is::<SparkMonotonicallyIncreasingId>()
                        || function.func.inner().is::<SparkPartitionId>())
                    && !partition_expressions.contains(expr)
                {
                    partition_expressions.push(expr.clone());
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
        }
        let (input, columns) =
            self.rewrite_partition_expressions(input, partition_expressions.clone(), state)?;
        let replacements = partition_expressions
            .into_iter()
            .zip(columns.into_iter().map(Expr::unalias))
            .collect::<Vec<_>>();
        let replace = |named: NamedExpr| -> PlanResult<NamedExpr> {
            Ok(NamedExpr {
                expr: Self::replace_grouping_expressions(named.expr, &replacements)?,
                ..named
            })
        };
        let grouping = grouping
            .into_iter()
            .map(replace)
            .collect::<PlanResult<Vec<_>>>()?;
        let projections = projections
            .into_iter()
            .map(replace)
            .collect::<PlanResult<Vec<_>>>()?;
        Ok((input, projections, grouping))
    }

    fn rewrite_prepared_aggregate(
        &self,
        input: LogicalPlan,
        projections: AggregateProjections,
        grouping: Vec<NamedExpr>,
        having: Option<Expr>,
        with_grouping_expressions: bool,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let AggregateProjections {
            expressions: projections,
            mut lateral_columns,
        } = projections;
        let group_exprs = grouping.iter().map(|x| x.expr.clone()).collect::<Vec<_>>();
        let has_grouping_set = Self::has_grouping_set(&group_exprs);
        let grouping_exprs = Self::distinct_grouping_expressions_from_exprs(&group_exprs);
        // Duplicate ordinals are internal, regardless of correlation. Resolve
        // user reads against the original grouping keys; any later rewrite must
        // preserve that layout rather than expose the ordinal as a grouping bit.
        let duplicate_sets = has_grouping_set && Self::has_duplicate_grouping_sets(&group_exprs)?;
        let projections = projections
            .into_iter()
            .map(|x| {
                Self::rewrite_grouping_functions(
                    x,
                    &grouping_exprs,
                    has_grouping_set,
                    duplicate_sets,
                )
            })
            .collect::<PlanResult<Vec<_>>>()?;
        let having = having
            .map(|having| {
                Self::rewrite_grouping_expr(
                    having,
                    &grouping_exprs,
                    has_grouping_set,
                    duplicate_sets,
                )
            })
            .transpose()?;
        let mut aggregate_candidates = projections
            .iter()
            .map(|x| x.expr.clone())
            .collect::<Vec<_>>();
        if let Some(having) = having.as_ref() {
            aggregate_candidates.push(having.clone());
        }
        let aggregate_exprs = find_aggregate_exprs(&aggregate_candidates);
        for aggregate in &aggregate_exprs {
            if let Some(name) = find_volatile_aggregate_input(aggregate) {
                return Err(PlanError::analysis(format!(
                    "Non-deterministic expression {name} should not appear in the arguments of an aggregate function",
                )));
            }
        }
        let aggregate_exprs = Self::disambiguate_aggregate_expressions(aggregate_exprs, state);
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
            if !lateral_columns.is_empty() {
                // The lateral aliases are staged in a projection above the aggregate, and the
                // grouping columns are carried along it. A fresh unqualified field would drop
                // the qualifier and the attribute identity of the aggregate's grouping column,
                // so stage it under a field that keeps both.
                let mut columns = grouping_exprs
                    .iter()
                    .map(|x| {
                        let source = match &x.expr {
                            Expr::Column(column) => state
                                .get_field_info(&column.name)
                                .is_ok()
                                .then(|| (column.name.clone(), column.relation.clone())),
                            _ => None,
                        };
                        match source {
                            Some((field_id, relation)) => {
                                let field_id =
                                    state.register_identity_field(String::new(), &field_id)?;
                                Ok(Column::new(relation, field_id))
                            }
                            None => Ok(Column::new_unqualified(state.register_field_name(""))),
                        }
                    })
                    .collect::<PlanResult<Vec<_>>>()?;
                columns.extend(lateral_columns);
                lateral_columns = columns;
            }
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
        let (plan, projections) = if lateral_columns.is_empty() {
            (plan, projections)
        } else {
            self.materialize_lateral_expressions(plan, projections, lateral_columns, state)?
        };
        let plan = match having {
            Some(having) => {
                let (plan, having) =
                    self.rewrite_partition_expressions(plan, vec![having], state)?;
                let having =
                    Self::rebase_expression(having.one()?, &aggregate_or_grouping_exprs, &plan)?;
                let (plan, having) = if duplicate_sets && having.contains_outer() {
                    Self::materialize_grouping_comparison(plan, having, state)?
                } else {
                    (plan, having)
                };
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
        let (plan, projections) = self.rewrite_window_projection(plan, projections, state)?;
        let (plan, projections) =
            self.rewrite_projection::<ExistsRewriter>(plan, projections, state)?;
        let projections = projections
            .into_iter()
            .map(|x| {
                let NamedExpr {
                    name,
                    expr,
                    metadata: _,
                } = x;
                let name = name.one()?;
                // A grouping column stays the aggregate's own attribute, so it keeps the
                // qualifier and the field identity. An aggregate value is named after its
                // DataFusion schema name, which is not a registered field.
                let source = match &expr {
                    Expr::Column(column) => state
                        .get_field_info(&column.name)
                        .is_ok()
                        .then(|| (column.name.clone(), column.relation.clone())),
                    _ => None,
                };
                match source {
                    Some((field_id, relation)) => {
                        // Spark expands a grouping set into `Expand`, whose grouping column is a
                        // new nullable attribute: the qualifier survives, but a reference to the
                        // input column no longer resolves against it.
                        let field_id = if has_grouping_set {
                            state.register_field_name(name)
                        } else {
                            state.register_identity_field(name, &field_id)?
                        };
                        let qualifier = relation
                            .filter(|relation| relation.table() != datafusion_expr::UNNAMED_TABLE);
                        Ok(expr.alias_qualified(qualifier, field_id))
                    }
                    None => Ok(expr.alias(state.register_field_name(name))),
                }
            })
            .collect::<PlanResult<Vec<_>>>()?;
        Ok(LogicalPlanBuilder::from(plan)
            .project(projections)?
            .build()?)
    }

    /// Keep masked grouping-ID equality a column comparison at its original
    /// HAVING stage. Native decorrelation can carry that typed value through an
    /// enclosing aggregate without grouping by the internal duplicate ordinal.
    fn materialize_grouping_comparison(
        plan: LogicalPlan,
        predicate: Expr,
        state: &mut PlanResolverState,
    ) -> PlanResult<(LogicalPlan, Expr)> {
        let mut materialized = Vec::<(Expr, Column)>::new();
        let predicate = predicate
            .transform_up(|expr| {
                let Expr::BinaryExpr(mut binary) = expr else {
                    return Ok(Transformed::no(expr));
                };
                if binary.op != datafusion_expr::Operator::Eq {
                    return Ok(Transformed::no(Expr::BinaryExpr(binary)));
                }
                let is_key = |inner: &Expr, outer: &Expr| {
                    if !outer.contains_outer() || outer.any_column_refs() {
                        return false;
                    }
                    // The comparison resolver may already have coerced the
                    // Int64 read to a floating type. This conversion is total;
                    // arbitrary expressions around the masked read are not.
                    let inner = match inner {
                        Expr::Cast(cast) if matches!(cast.field.data_type(), DataType::Float32 | DataType::Float64) => cast.expr.as_ref(),
                        _ => inner,
                    };
                    // Only the resolver-generated masked read. Do not eagerly
                    // evaluate user expressions from guarded predicates merely
                    // because they read this ID.
                    let Expr::Cast(value) = inner else { return false };
                    let Expr::BinaryExpr(mask) = value.expr.as_ref() else { return false };
                    let Expr::Cast(id) = mask.left.as_ref() else { return false };
                    value.field.data_type() == &DataType::Int64
                        && mask.op == datafusion_expr::Operator::BitwiseAnd
                        && id.field.data_type() == &DataType::UInt64
                        && matches!(id.expr.as_ref(), Expr::Column(column) if column.name == Aggregate::INTERNAL_GROUPING_ID)
                        && matches!(mask.right.as_ref(), Expr::Literal(ScalarValue::UInt64(Some(_)), _))
                };
                let inner = if is_key(&binary.left, &binary.right) {
                    &mut binary.left
                } else if is_key(&binary.right, &binary.left) {
                    &mut binary.right
                } else {
                    return Ok(Transformed::no(Expr::BinaryExpr(binary)));
                };
                let column = if let Some((_, column)) =
                    materialized.iter().find(|(expr, _)| expr == inner.as_ref())
                {
                    column.clone()
                } else {
                    let column =
                        Column::new_unqualified(state.register_hidden_field_name("grouping_key"));
                    materialized.push((inner.as_ref().clone(), column.clone()));
                    column
                };
                **inner = Expr::Column(column);
                Ok(Transformed::yes(Expr::BinaryExpr(binary)))
            })?
            .data;
        if materialized.is_empty() {
            return Ok((plan, predicate));
        }
        let mut projection = plan
            .schema()
            .columns()
            .into_iter()
            .map(Expr::Column)
            .collect::<Vec<_>>();
        projection.extend(
            materialized
                .into_iter()
                .map(|(expr, column)| expr.alias(column.name)),
        );
        Ok((
            LogicalPlanBuilder::from(plan)
                .project(projection)?
                .build()?,
            predicate,
        ))
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

    pub(in crate::resolver) fn find_input_sort_ordering(
        input: &LogicalPlan,
    ) -> Option<Vec<SortExpr>> {
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

    /// Resolve each projection once, deferring only expressions that need the
    /// grouping output to be materialized before their names can resolve.
    async fn resolve_projections_deferring(
        &self,
        projections: &[spec::Expr],
        input: &LogicalPlan,
        state: &mut PlanResolverState,
    ) -> PlanResult<ResolvedProjections> {
        let resolved = self
            .resolve_lateral_expressions(projections.to_vec(), input, true, state)
            .await?;
        Ok(ResolvedProjections {
            expressions: resolved.expressions,
            deferred: resolved.deferred,
            lateral_columns: resolved.columns,
            schema: resolved.schema,
        })
    }

    /// Resolves the deferred projections (the grouping columns are now in scope) and
    /// inlines re-used generator expressions, producing the final projection list.
    async fn finish_projections(
        &self,
        mut resolved: Vec<Option<NamedExpr>>,
        deferred: Vec<(usize, spec::Expr)>,
        generator_replacements: &[(Expr, Expr)],
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<NamedExpr>> {
        for (index, projection) in deferred {
            resolved[index] = Some(
                self.resolve_named_expression(projection, schema, state)
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
                    expr: Self::replace_grouping_expressions(expr, generator_replacements)?,
                    metadata,
                })
            })
            .collect()
    }

    fn rewrite_grouping_functions(
        named_expr: NamedExpr,
        grouping_exprs: &[Expr],
        has_grouping_set: bool,
        duplicate_sets: bool,
    ) -> PlanResult<NamedExpr> {
        let NamedExpr {
            name,
            expr,
            metadata,
        } = named_expr;
        Ok(NamedExpr {
            name,
            expr: Self::rewrite_grouping_expr(
                expr,
                grouping_exprs,
                has_grouping_set,
                duplicate_sets,
            )?,
            metadata,
        })
    }

    pub(super) fn rewrite_grouping_expr(
        expr: Expr,
        grouping_exprs: &[Expr],
        has_grouping_set: bool,
        duplicate_sets: bool,
    ) -> PlanResult<Expr> {
        Ok(expr
            .transform_down(|expr| {
                if let Expr::AggregateFunction(function) = expr {
                    match function.func.name() {
                        "grouping" => Ok(Transformed::yes(Self::grouping_on_grouping_id(
                            function,
                            grouping_exprs,
                            has_grouping_set,
                            duplicate_sets,
                        )?)),
                        "grouping_id" => Ok(Transformed::yes(Self::grouping_id_on_grouping_id(
                            function,
                            grouping_exprs,
                            has_grouping_set,
                            duplicate_sets,
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
        duplicate_sets: bool,
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
        // The duplicate-set ordinal can widen the grouping ID past the literal type.
        let (grouping_id, width) = if duplicate_sets {
            (
                cast(
                    Self::grouping_id_column(),
                    datafusion_common::arrow::datatypes::DataType::UInt64,
                ),
                64,
            )
        } else {
            (Self::grouping_id_column(), grouping_exprs.len())
        };
        let group_bit = bitwise_and(
            grouping_id,
            Self::grouping_bitmask_literal(1_u64 << bitmap_index, width),
        );
        let shifted = if bitmap_index == 0 {
            group_bit
        } else {
            bitwise_shift_right(
                group_bit,
                Self::grouping_bitmask_literal(bitmap_index as u64, width),
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
        duplicate_sets: bool,
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
        let grouping_id = if duplicate_sets {
            // Like Spark, keep only the grouping bits: DataFusion packs a duplicate-set
            // ordinal above them.
            let mask = match grouping_exprs.len() {
                0 => 0,
                count => u64::MAX >> (64 - count),
            };
            // The ordinal can widen the grouping ID past the type of a grouping-bit literal.
            bitwise_and(
                cast(
                    Self::grouping_id_column(),
                    datafusion_common::arrow::datatypes::DataType::UInt64,
                ),
                datafusion_expr::lit(mask),
            )
        } else {
            Self::grouping_id_column()
        };
        Ok(cast(
            grouping_id,
            datafusion_common::arrow::datatypes::DataType::Int64,
        ))
    }

    /// Whether native decorrelation keeps the grouping bits in place: it groups every set
    /// by the inner columns that correlations compare, which adds nothing when each is
    /// already grouped in every set. `NormalizeScalarSubqueryKeys` masks the other
    /// correlated grouping IDs it rewrites.
    pub(super) fn correlation_keeps_grouping(
        plan: &LogicalPlan,
        group_exprs: &[Expr],
    ) -> PlanResult<bool> {
        let mut columns = Vec::new();
        let mut filters_only = true;
        plan.apply(|node| {
            match node {
                LogicalPlan::Filter(filter) => columns.extend(
                    datafusion_expr::utils::split_conjunction(&filter.predicate)
                        .into_iter()
                        .filter(|expr| expr.contains_outer())
                        .flat_map(Expr::column_refs)
                        .cloned(),
                ),
                node => filters_only &= !node.contains_outer_reference(),
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        if columns.is_empty() || !filters_only {
            return Ok(filters_only);
        }
        let expanded = datafusion_expr::utils::enumerate_grouping_sets(group_exprs.to_vec())?;
        let [Expr::GroupingSet(datafusion_expr::GroupingSet::GroupingSets(sets))] =
            expanded.as_slice()
        else {
            // Some rollup or cube set omits every expression.
            return Ok(false);
        };
        Ok(columns.into_iter().all(|column| {
            let column = Expr::Column(column);
            sets.iter().all(|set| set.contains(&column))
        }))
    }

    /// Whether two grouping sets group by the same expressions, in any order.
    pub(super) fn has_duplicate_grouping_sets(group_exprs: &[Expr]) -> PlanResult<bool> {
        let expanded = datafusion_expr::utils::enumerate_grouping_sets(group_exprs.to_vec())?;
        let sets = match expanded.as_slice() {
            [Expr::GroupingSet(datafusion_expr::GroupingSet::GroupingSets(sets))] => sets,
            // Only a repeated expression makes two rollup or cube sets equal.
            [
                Expr::GroupingSet(
                    datafusion_expr::GroupingSet::Rollup(exprs)
                    | datafusion_expr::GroupingSet::Cube(exprs),
                ),
            ] => {
                let mut seen = std::collections::HashSet::new();
                return Ok(!exprs.iter().all(|expr| seen.insert(expr)));
            }
            _ => return Ok(false),
        };
        let groups = datafusion_expr::utils::grouping_set_to_exprlist(&expanded)?;
        let mut seen = std::collections::HashSet::new();
        Ok(!sets.iter().all(|set| {
            seen.insert(
                groups
                    .iter()
                    .map(|group| set.contains(group))
                    .collect::<Vec<_>>(),
            )
        }))
    }

    /// Expands a generator in the grouping into rows, naming the unnested column
    /// after the grouping output. Returns a map from each generator to its column.
    /// A no-op when the grouping has no generator.
    pub(super) fn expand_grouping_generators(
        &self,
        input: LogicalPlan,
        grouping: Vec<NamedExpr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<(LogicalPlan, Vec<NamedExpr>, GroupingReplacements)> {
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
            .exists(|e| Ok(matches!(e, Expr::ScalarFunction(f) if f.func.inner().is::<Explode>())))
            .unwrap_or(false)
    }

    fn materialize_grouping_input_file_metadata(
        input: LogicalPlan,
        grouping: &[NamedExpr],
        state: &mut PlanResolverState,
    ) -> PlanResult<(LogicalPlan, GroupingReplacements)> {
        let mut metadata = Vec::new();
        for group in grouping {
            group.expr.apply(|expr| {
                if let Expr::ScalarFunction(function) = expr
                    && is_input_file_metadata_function(&function.func)
                    && !metadata.contains(expr)
                {
                    metadata.push(expr.clone());
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
        }
        if metadata.is_empty() {
            return Ok((input, vec![]));
        }

        let mut projection = input
            .schema()
            .columns()
            .into_iter()
            .map(Expr::Column)
            .collect::<Vec<_>>();
        let replacements = metadata
            .into_iter()
            .map(|expr| {
                let name = state.register_field_name("");
                projection.push(expr.clone().alias(&name));
                (expr, Expr::Column(Column::from_name(name)))
            })
            .collect();
        let input = LogicalPlan::Projection(Projection::try_new(projection, Arc::new(input))?);
        Ok((input, replacements))
    }

    /// Reuses the input column for each materialized grouping expression.
    pub(super) fn replace_grouping_expressions(
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

    /// Casts are invisible in DataFusion schema names, so distinct aggregate expressions
    /// can claim the same output name: `min(x)` and `min(to_date(x))` are both `min(x)`.
    /// Such expressions get an internal alias, which keeps the aggregate schema unique.
    fn disambiguate_aggregate_expressions(
        expressions: Vec<Expr>,
        state: &mut PlanResolverState,
    ) -> Vec<Expr> {
        let mut names = std::collections::HashSet::new();
        expressions
            .into_iter()
            .map(|expr| {
                if names.insert(expr.schema_name().to_string()) {
                    expr
                } else {
                    expr.alias(state.register_field_name(""))
                }
            })
            .collect()
    }

    /// Reference: [datafusion_sql::utils::rebase_expr]
    pub(super) fn rebase_expression(
        expr: Expr,
        base: &[Expr],
        plan: &LogicalPlan,
    ) -> PlanResult<Expr> {
        Ok(expr
            .transform_down(|e| {
                if let Some(base) = base.iter().find(|base| Self::is_rebase_target(base, &e)) {
                    Ok(Transformed::yes(
                        Self::expr_as_column_expr(base, plan)
                            .map_err(|e| DataFusionError::External(Box::new(e)))?,
                    ))
                } else {
                    Ok(Transformed::no(e))
                }
            })
            .data()?)
    }

    fn is_rebase_target(base: &Expr, expr: &Expr) -> bool {
        base == expr || matches!(base, Expr::Alias(alias) if alias.expr.as_ref() == expr)
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
