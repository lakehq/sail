use std::sync::Arc;

use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::aggregate_statistics::AggregateStatistics;
use datafusion::physical_optimizer::combine_partial_final_agg::CombinePartialFinalAggregate;
use datafusion::physical_optimizer::eliminate_constant_join::EliminateConstantJoin;
use datafusion::physical_optimizer::ensure_coop::EnsureCooperative;
use datafusion::physical_optimizer::ensure_requirements::EnsureRequirements;
use datafusion::physical_optimizer::filter_pushdown::FilterPushdown;
use datafusion::physical_optimizer::hash_join_buffering::HashJoinBuffering;
use datafusion::physical_optimizer::join_selection::JoinSelection;
use datafusion::physical_optimizer::limit_pushdown::LimitPushdown;
use datafusion::physical_optimizer::limit_pushdown_past_window::LimitPushPastWindows;
use datafusion::physical_optimizer::limited_distinct_aggregation::LimitedDistinctAggregation;
use datafusion::physical_optimizer::output_requirements::OutputRequirements;
use datafusion::physical_optimizer::pushdown_sort::PushdownSort;
use datafusion::physical_optimizer::topk_aggregation::TopKAggregation;
use datafusion::physical_optimizer::topk_repartition::TopKRepartition;
use datafusion::physical_optimizer::update_aggr_exprs::OptimizeAggregateOrder;
use datafusion::physical_optimizer::window_topn::WindowTopN;

use crate::barrier::EnforceBarrierPartitioning;
use crate::collect_left::RewriteCollectLeftHashJoin;
use crate::explicit_repartition::RewriteExplicitRepartition;
use crate::filter_pushdown::PostFilterPushdown;
use crate::input_file_name::{
    PushDownInputFileMetadata, PushDownStreamedJoinInputFileMetadata,
    RewriteInputFileMetadataFallback, WrapInputFileMetadataSource,
};
use crate::join_reorder::JoinReorder;
pub use crate::join_reorder::JoinReorderOptions;
use crate::projection_pushdown::ContextSafeProjectionPushdown;
use crate::select_semi_join_build_side::SelectSemiJoinBuildSide;

mod barrier;
mod collect_left;
mod correlation_cast;
mod explicit_repartition;
mod filter_pushdown;
mod input_file_name;
mod join_reorder;
mod mixed_numeric;
mod projection_pushdown;
mod scan_partitions;
mod select_semi_join_build_side;

#[derive(Debug, Clone, Default)]
pub struct PhysicalOptimizerOptions {
    pub enable_join_reorder: bool,
    pub join_reorder: JoinReorderOptions,
}

pub fn get_physical_optimizers(
    options: PhysicalOptimizerOptions,
) -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    let mut rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> = vec![];

    rules.push(Arc::new(OutputRequirements::new_add_mode()));
    rules.push(Arc::new(AggregateStatistics::new()));
    rules.push(Arc::new(EliminateConstantJoin::new()));
    if options.enable_join_reorder {
        rules.push(Arc::new(JoinReorder::new(options.join_reorder)));
    }
    rules.push(Arc::new(JoinSelection::new()));
    rules.push(Arc::new(SelectSemiJoinBuildSide));
    rules.push(Arc::new(LimitedDistinctAggregation::new()));
    rules.push(Arc::new(correlation_cast::OptimizeCorrelationCasts));
    rules.push(Arc::new(FilterPushdown::new()));
    // WindowTopN checks DataFusion's `enable_window_topn`, which defaults to false because
    // PartitionedTopKExec can regress memory and runtime for high-cardinality partition keys.
    // Revisit the opt-in default when that trade-off is addressed.
    rules.push(Arc::new(WindowTopN::new()));
    rules.push(Arc::new(WrapInputFileMetadataSource::new()));
    rules.push(Arc::new(PushDownInputFileMetadata::new()));
    rules.push(Arc::new(EnsureRequirements::new()));
    rules.push(Arc::new(scan_partitions::OptimizeScanPartitions));
    rules.push(Arc::new(CombinePartialFinalAggregate::new()));
    rules.push(Arc::new(OptimizeAggregateOrder::new()));
    rules.push(Arc::new(ContextSafeProjectionPushdown::new()));
    rules.push(Arc::new(OutputRequirements::new_remove_mode()));
    rules.push(Arc::new(TopKAggregation::new()));
    rules.push(Arc::new(LimitPushPastWindows::new()));
    rules.push(Arc::new(HashJoinBuffering::new()));
    rules.push(Arc::new(LimitPushdown::new()));
    rules.push(Arc::new(TopKRepartition::new()));
    rules.push(Arc::new(ContextSafeProjectionPushdown::new()));
    rules.push(Arc::new(PushdownSort::new()));
    rules.push(Arc::new(EnsureCooperative::new()));
    rules.push(Arc::new(PostFilterPushdown));
    rules.push(Arc::new(PushDownStreamedJoinInputFileMetadata));
    rules.push(Arc::new(RewriteInputFileMetadataFallback::new()));
    rules.push(Arc::new(RewriteExplicitRepartition::new()));
    rules.push(Arc::new(RewriteCollectLeftHashJoin::new()));
    rules.push(Arc::new(EnforceBarrierPartitioning::new()));

    rules
}

#[cfg(test)]
mod tests {
    mod constant_join {
        use std::collections::HashMap;
        use std::sync::Arc;

        use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, StringArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::common::{JoinSide, JoinType, Result, ScalarValue};
        use datafusion::config::ConfigOptions;
        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::logical_expr::Operator;
        use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
        use datafusion::physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr};
        use datafusion::physical_plan::filter::FilterExec;
        use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
        use datafusion::physical_plan::joins::{CrossJoinExec, NestedLoopJoinExec};
        use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
        use datafusion::physical_plan::projection::ProjectionExec;
        use datafusion::physical_plan::sorts::sort::SortExec;
        use datafusion::physical_plan::{ExecutionPlan, Partitioning};

        use super::{
            EliminateConstantJoin, PhysicalOptimizerOptions, PhysicalOptimizerRule,
            get_physical_optimizers,
        };

        // DataFusion's eliminate_constant_join.slt and keeps_join_with_multi_row_constant
        // establish single-row elimination and the one-row-per-partition boundary.
        #[test]
        #[expect(clippy::unwrap_used)]
        fn constant_joins_preserve_typed_mappings_metadata_and_multiplicity() -> Result<()> {
            for enable_join_reorder in [false, true] {
                let rules = get_physical_optimizers(PhysicalOptimizerOptions {
                    enable_join_reorder,
                    ..Default::default()
                });
                let rule = rules
                    .iter()
                    .find(|rule| rule.name() == EliminateConstantJoin::new().name())
                    .unwrap();
                for constant_left in [false, true] {
                    for filtered in [false, true] {
                        for partitions in [1, 2] {
                            let schema = Arc::new(Schema::new_with_metadata(
                                vec![
                                    Field::new("id", DataType::Int64, false),
                                    Field::new("value", DataType::Int64, true).with_metadata(
                                        HashMap::from([("field".into(), "event".into())]),
                                    ),
                                ],
                                HashMap::from([("events".into(), "retained".into())]),
                            ));
                            let batch = RecordBatch::try_new(
                                Arc::clone(&schema),
                                vec![
                                    Arc::new(Int64Array::from(vec![1, 1, 2, 3, 4])),
                                    Arc::new(Int64Array::from(vec![
                                        Some(10),
                                        Some(10),
                                        None,
                                        Some(30),
                                        Some(5),
                                    ])),
                                ],
                            )?;
                            let order = LexOrdering::new([PhysicalSortExpr::new_default(
                                Arc::new(Column::new("id", 0)),
                            )])
                            .unwrap();
                            let events: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(
                                order,
                                MemorySourceConfig::try_new_exec(
                                    &[vec![batch.clone()]],
                                    schema,
                                    None,
                                )?,
                            ));
                            let constant: Arc<dyn ExecutionPlan> =
                                Arc::new(ProjectionExec::try_new(
                                    vec![
                                        (
                                            Arc::new(Literal::new(ScalarValue::Int64(Some(25))))
                                                as Arc<dyn PhysicalExpr>,
                                            "cutoff".into(),
                                        ),
                                        (
                                            Arc::new(Literal::new(ScalarValue::Utf8(None)))
                                                as Arc<dyn PhysicalExpr>,
                                            "note".into(),
                                        ),
                                    ],
                                    Arc::new(
                                        PlaceholderRowExec::new(Arc::new(
                                            Schema::new_with_metadata(
                                                Vec::<Field>::new(),
                                                HashMap::from([(
                                                    "constant".into(),
                                                    "retained".into(),
                                                )]),
                                            ),
                                        ))
                                        .with_partitions(partitions),
                                    ),
                                )?);
                            let (
                                left,
                                right,
                                constant_side,
                                event_side,
                                constant_start,
                                event_start,
                            ) = if constant_left {
                                (
                                    constant,
                                    Arc::clone(&events),
                                    JoinSide::Left,
                                    JoinSide::Right,
                                    0,
                                    2,
                                )
                            } else {
                                (
                                    Arc::clone(&events),
                                    constant,
                                    JoinSide::Right,
                                    JoinSide::Left,
                                    2,
                                    0,
                                )
                            };
                            // The filter schema deliberately reverses the expression's column order.
                            let filter = JoinFilter::new(
                                Arc::new(BinaryExpr::new(
                                    Arc::new(Column::new("event_filter", 1)),
                                    Operator::Lt,
                                    Arc::new(Column::new("constant_filter", 0)),
                                )),
                                vec![
                                    ColumnIndex {
                                        index: 0,
                                        side: constant_side,
                                    },
                                    ColumnIndex {
                                        index: 1,
                                        side: event_side,
                                    },
                                ],
                                Arc::new(Schema::new(vec![
                                    Field::new("constant_filter", DataType::Int64, false),
                                    Field::new("event_filter", DataType::Int64, true),
                                ])),
                            );
                            let indices = if filtered {
                                vec![
                                    constant_start + 1,
                                    event_start + 1,
                                    constant_start,
                                    event_start + 1,
                                ]
                            } else {
                                (0..4).collect()
                            };
                            let original: Arc<dyn ExecutionPlan> = if filtered {
                                Arc::new(NestedLoopJoinExec::try_new(
                                    left,
                                    right,
                                    Some(filter),
                                    &JoinType::Inner,
                                    Some(indices.clone()),
                                )?)
                            } else {
                                Arc::new(CrossJoinExec::new(left, right))
                            };
                            let optimized =
                                rule.optimize(Arc::clone(&original), &ConfigOptions::default())?;
                            assert_eq!(optimized.schema(), original.schema());
                            assert_eq!(
                                optimized
                                    .schema()
                                    .metadata()
                                    .get("constant")
                                    .map(String::as_str),
                                Some("retained")
                            );
                            if partitions == 2 {
                                assert!(Arc::ptr_eq(&optimized, &original));
                                continue;
                            }
                            let projection = optimized.downcast_ref::<ProjectionExec>().unwrap();
                            let input = if filtered {
                                let filter =
                                    projection.input().downcast_ref::<FilterExec>().unwrap();
                                let predicate =
                                    filter.predicate().downcast_ref::<BinaryExpr>().unwrap();
                                assert_eq!(predicate.op(), &Operator::Lt);
                                assert_eq!(
                                    predicate.left().downcast_ref::<Column>().unwrap(),
                                    &Column::new("value", 1)
                                );
                                assert_eq!(
                                    predicate.right().downcast_ref::<Literal>().unwrap().value(),
                                    &ScalarValue::Int64(Some(25))
                                );
                                assert_eq!(
                                    filter
                                        .predicate()
                                        .evaluate(&batch)?
                                        .into_array(5)?
                                        .to_data(),
                                    BooleanArray::from(vec![
                                        Some(true),
                                        Some(true),
                                        None,
                                        Some(false),
                                        Some(true)
                                    ])
                                    .to_data()
                                );
                                assert_eq!(filter.schema(), events.schema());
                                assert_eq!(
                                    filter.properties().output_ordering(),
                                    events.properties().output_ordering()
                                );
                                filter.input()
                            } else {
                                projection.input()
                            };
                            assert!(Arc::ptr_eq(input, &events));
                            assert!(matches!(
                                (&optimized.properties().partitioning, &events.properties().partitioning),
                                (Partitioning::UnknownPartitioning(a), Partitioning::UnknownPartitioning(b)) if a == b
                            ));
                            assert_eq!(projection.expr().len(), indices.len());
                            for (expression, index) in projection.expr().iter().zip(indices) {
                                let expected: ArrayRef = if (constant_start..constant_start + 2)
                                    .contains(&index)
                                {
                                    let value = if index == constant_start {
                                        ScalarValue::Int64(Some(25))
                                    } else {
                                        ScalarValue::Utf8(None)
                                    };
                                    assert_eq!(
                                        expression.expr.downcast_ref::<Literal>().unwrap().value(),
                                        &value
                                    );
                                    if index == constant_start {
                                        Arc::new(Int64Array::from(vec![25; 5]))
                                    } else {
                                        Arc::new(StringArray::from(vec![None::<&str>; 5]))
                                    }
                                } else {
                                    let index = index - event_start;
                                    assert_eq!(
                                        expression.expr.downcast_ref::<Column>().unwrap(),
                                        &Column::new(batch.schema().field(index).name(), index)
                                    );
                                    Arc::clone(batch.column(index))
                                };
                                assert_eq!(
                                    expression.expr.evaluate(&batch)?.into_array(5)?.to_data(),
                                    expected.to_data()
                                );
                            }
                            if filtered {
                                assert_eq!(
                                    optimized.schema().field(1),
                                    optimized.schema().field(3)
                                );
                                assert_eq!(optimized.schema().field(1).name(), "value");
                                assert_eq!(
                                    optimized
                                        .schema()
                                        .field(1)
                                        .metadata()
                                        .get("field")
                                        .map(String::as_str),
                                    Some("event")
                                );
                            }
                        }
                    }
                }
            }
            Ok(())
        }
    }

    mod upstream {
        // Licensed under the Apache License, Version 2.0 (the "License");
        // you may not use this file except in compliance with the License.
        // You may obtain a copy of the License at
        //
        //     http://www.apache.org/licenses/LICENSE-2.0
        //
        // Unless required by applicable law or agreed to in writing, software
        // distributed under the License is distributed on an "AS IS" BASIS,
        // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
        // See the License for the specific language governing permissions and
        // limitations under the License.

        //! Behavioral ports of DataFusion 1635ed94742c1c060852bc34dc6ebb2302241c7b:
        //! aggregate_stream::aggregate_dynamic_filter_recovers_from_null_bounds,
        //! push_down_filter_regression.slt, nested_loop_join_lambda.slt, and limit.slt.
        //! The same inputs run with upstream and Sail physical optimizer pipelines.

        #![expect(clippy::unwrap_used)]

        use std::fs::File;
        use std::sync::Arc;

        use datafusion::arrow::array::{Array, Int32Array, ListArray, StringArray};
        use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
        use datafusion::common::{Result, ScalarValue};
        use datafusion::config::ConfigOptions;
        use datafusion::dataframe::DataFrame;
        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::execution::TaskContext;
        use datafusion::execution::session_state::SessionStateBuilder;
        use datafusion::functions::expr_fn::abs;
        use datafusion::functions_aggregate::expr_fn::{max, min};
        use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
        use datafusion::functions_nested::expr_fn::{array_any_match, array_transform};
        use datafusion::logical_expr::{Expr, JoinType, cast, col as column, lambda, lit};
        use datafusion::parquet::arrow::ArrowWriter;
        use datafusion::parquet::file::properties::WriterProperties;
        use datafusion::physical_expr::aggregate::AggregateExprBuilder;
        use datafusion::physical_expr::expressions::{
            Column, DynamicFilterPhysicalExpr, LambdaVariable, Literal, col,
        };
        use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
        use datafusion::physical_optimizer::PhysicalOptimizerRule;
        use datafusion::physical_optimizer::ensure_requirements::EnsureRequirements;
        use datafusion::physical_optimizer::output_requirements::{
            OutputRequirementExec, OutputRequirements,
        };
        use datafusion::physical_optimizer::sanity_checker::check_plan_tree_sanity;
        use datafusion::physical_plan::aggregates::{
            AggregateExec, AggregateMode, PhysicalGroupBy,
        };
        use datafusion::physical_plan::joins::NestedLoopJoinExec;
        use datafusion::physical_plan::limit::GlobalLimitExec;
        use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
        use datafusion::physical_plan::projection::ProjectionExec;
        use datafusion::physical_plan::sorts::sort::SortExec;
        use datafusion::physical_plan::{Distribution, ExecutionPlan, collect, displayable};
        use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};

        use crate::{PhysicalOptimizerOptions, get_physical_optimizers};

        fn context(sail: bool, dynamic_filters: bool) -> SessionContext {
            let mut config = SessionConfig::new()
                .with_target_partitions(2)
                .with_batch_size(2);
            config.options_mut().execution.collect_statistics = false;
            config.options_mut().execution.parquet.pushdown_filters = true;
            config.options_mut().optimizer.join_reordering = false;
            config
                .options_mut()
                .optimizer
                .enable_aggregate_dynamic_filter_pushdown = dynamic_filters;
            let mut builder = SessionStateBuilder::new()
                .with_config(config)
                .with_default_features();
            if sail {
                builder = builder.with_physical_optimizer_rules(get_physical_optimizers(
                    PhysicalOptimizerOptions::default(),
                ));
            }
            SessionContext::new_with_state(builder.build())
        }

        async fn plan(
            context: &SessionContext,
            frame: DataFrame,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            let logical = frame.into_optimized_plan()?;
            println!("Logical:\n{}", logical.display_indent());
            let physical = context.state().create_physical_plan(&logical).await?;
            println!("Physical:\n{}", displayable(physical.as_ref()).indent(true));
            Ok(physical)
        }

        fn rows(batches: &[RecordBatch]) -> Result<Vec<String>> {
            let mut rows = Vec::new();
            for batch in batches {
                for row in 0..batch.num_rows() {
                    rows.push(
                        batch
                            .columns()
                            .iter()
                            .map(|column| {
                                ScalarValue::try_from_array(column, row)
                                    .map(|value| value.to_string())
                            })
                            .collect::<Result<Vec<_>>>()?
                            .join("|"),
                    );
                }
            }
            Ok(rows)
        }

        // Behavioral port of the aggregate dynamic-filter regression test.
        // [CREDIT]: https://github.com/apache/datafusion/blob/1635ed94742c1c060852bc34dc6ebb2302241c7b/datafusion/physical-plan/src/aggregates/aggregate_stream.rs
        #[tokio::test]
        async fn aggregate_dynamic_filter_recovers_from_null_bounds() -> Result<()> {
            let schema = Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int32, true),
            ]));
            let batch = |a: Vec<i32>, b: Vec<Option<i32>>| {
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int32Array::from(a)), Arc::new(Int32Array::from(b))],
                )
            };
            let input = MemorySourceConfig::try_new_exec(
                &[
                    vec![batch(vec![1, 8], vec![None, None])?],
                    vec![batch(vec![2, 3], vec![Some(5), Some(7)])?],
                    // A later NULL-only partition must not reset an established bound.
                    vec![batch(vec![4, 6], vec![None, None])?],
                ],
                Arc::clone(&schema),
                None,
            )?;
            let mut aggregates = Vec::new();
            for column in ["a", "b"] {
                for function in [min_udaf(), max_udaf()] {
                    let alias = format!("{}({column})", function.name());
                    aggregates.push(Arc::new(
                        AggregateExprBuilder::new(function, vec![col(column, &schema)?])
                            .schema(Arc::clone(&schema))
                            .alias(alias)
                            .build()?,
                    ));
                }
            }
            let aggregate = AggregateExec::try_new(
                AggregateMode::Partial,
                PhysicalGroupBy::default(),
                aggregates,
                vec![None; 4],
                input,
                Arc::clone(&schema),
            )?;
            let produced = aggregate.dynamic_expressions_produced();
            assert_eq!(produced.len(), 1);
            let filter = produced[0]
                .downcast_ref::<DynamicFilterPhysicalExpr>()
                .unwrap();
            let context = Arc::new(TaskContext::default());
            datafusion::physical_plan::common::collect(aggregate.execute(0, Arc::clone(&context))?)
                .await?;
            assert_eq!(
                filter.current()?.downcast_ref::<Literal>().unwrap().value(),
                &ScalarValue::Boolean(Some(true))
            );
            for partition in [1, 2] {
                datafusion::physical_plan::common::collect(
                    aggregate.execute(partition, Arc::clone(&context))?,
                )
                .await?;
                // Interior values cannot improve any bound; each extremum can independently improve it.
                let candidates = batch(
                    vec![2, 0, 9, 3, 3],
                    vec![Some(6), Some(6), Some(6), Some(4), Some(8)],
                )?;
                let actual = filter.current()?.evaluate(&candidates)?.into_array(5)?;
                assert_eq!(
                    actual.to_data(),
                    datafusion::arrow::array::BooleanArray::from(vec![
                        false, true, true, true, true
                    ])
                    .to_data()
                );
            }
            Ok(())
        }

        // Behavioral port of the multi-aggregate filter-pushdown regression cases.
        // [CREDIT]: https://github.com/apache/datafusion/blob/1635ed94742c1c060852bc34dc6ebb2302241c7b/datafusion/sqllogictest/test_files/push_down_filter_regression.slt
        #[tokio::test]
        async fn multi_aggregate_parquet_late_bounds() -> Result<()> {
            let directory = tempfile::tempdir()?;
            let schema = Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int32, true),
            ]));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int32Array::from(vec![1, 8, 2, 3])),
                    Arc::new(Int32Array::from(vec![None, None, Some(5), Some(7)])),
                ],
            )?;
            for index in 0..2 {
                let file = File::create(directory.path().join(format!("part-{index}.parquet")))?;
                let properties = WriterProperties::builder()
                    .set_max_row_group_row_count(Some(2))
                    .build();
                let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(properties))?;
                writer.write(&batch)?;
                writer.close()?;
            }
            for sail in [false, true] {
                for enabled in [false, true] {
                    let context = context(sail, enabled);
                    let frame = context
                        .read_parquet(
                            directory.path().to_str().unwrap(),
                            ParquetReadOptions::default(),
                        )
                        .await?
                        .aggregate(
                            vec![],
                            vec![
                                min(column("a")),
                                max(column("a")),
                                min(column("b")),
                                max(column("b")),
                            ],
                        )?;
                    let physical = plan(&context, frame).await?;
                    let mut dynamic_aggregates = 0;
                    physical.apply(|node| {
                        if node.is::<AggregateExec>()
                            && !node.dynamic_expressions_produced().is_empty()
                        {
                            dynamic_aggregates += 1;
                        }
                        assert!(!node.is::<PlaceholderRowExec>());
                        Ok(TreeNodeRecursion::Continue)
                    })?;
                    assert_eq!(dynamic_aggregates > 0, enabled);
                    assert_eq!(
                        rows(&collect(physical, context.task_ctx()).await?)?,
                        ["1|8|5|7"]
                    );
                }
            }
            Ok(())
        }

        fn variable(name: &str) -> Expr {
            Expr::LambdaVariable(datafusion::logical_expr::expr::LambdaVariable::new(
                name.into(),
                Some(Arc::new(Field::new(name, DataType::Int32, true))),
            ))
        }

        fn lists(context: &SessionContext) -> Result<DataFrame> {
            let items = ListArray::from_iter_primitive::<Int32Type, _, _>([
                Some(vec![Some(1), Some(30)]),
                Some(vec![Some(40)]),
                Some(vec![]),
                None,
            ]);
            let schema = Arc::new(Schema::new(vec![
                Field::new("items", items.data_type().clone(), true),
                Field::new("k", DataType::Utf8, false),
                Field::new("n", DataType::Int32, false),
                Field::new("threshold", DataType::Int32, false),
            ]));
            context.read_batch(RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(items),
                    Arc::new(StringArray::from(vec!["a", "b", "empty", "null"])),
                    Arc::new(Int32Array::from(vec![7, 8, 9, 10])),
                    Arc::new(Int32Array::from(vec![2, 4, 2, 2])),
                ],
            )?)
        }

        // Behavioral port of captured-lambda nested-loop join cases.
        // [CREDIT]: https://github.com/apache/datafusion/blob/1635ed94742c1c060852bc34dc6ebb2302241c7b/datafusion/sqllogictest/test_files/nested_loop_join_lambda.slt
        #[tokio::test]
        async fn nested_loop_join_captured_lambdas() -> Result<()> {
            let captured = |body| array_any_match(column("items"), lambda(["x"], body));
            let opposite = captured(variable("x").lt(column("cutoff")));
            let ordinary = abs(column("score")).gt(abs(column("threshold")));
            let nested = |capture| {
                captured(array_any_match(
                    column("items"),
                    lambda(
                        ["y"],
                        variable("y")
                            .lt_eq(variable("x"))
                            .and(variable("x").lt(column(capture))),
                    ),
                ))
            };
            let complete_expected = vec![
                "25|a|7|2",
                "25|b|8|4",
                "25|empty|9|2",
                "5|b|8|4",
                "5|empty|9|2",
            ];
            let cases = [
                (opposite.clone(), vec!["25|a|7|2", "5|a|7|2"], false),
                (opposite.and(ordinary.clone()), vec!["25|a|7|2"], true),
                (
                    array_any_match(
                        array_transform(column("items"), lambda(["y"], variable("y") + lit(1i32))),
                        lambda(["x"], variable("x").lt(column("cutoff"))),
                    )
                    .and(ordinary.clone()),
                    vec!["25|a|7|2"],
                    true,
                ),
                (
                    nested("cutoff").and(ordinary.clone()),
                    vec!["25|a|7|2"],
                    true,
                ),
                (
                    captured(captured(variable("x").lt(column("cutoff")))).and(ordinary),
                    vec!["25|a|7|2"],
                    true,
                ),
                (
                    cast(captured(variable("x").lt(column("n"))), DataType::Int32)
                        .lt(abs(column("score"))),
                    complete_expected.clone(),
                    true,
                ),
                (
                    cast(nested("n"), DataType::Int32).lt(abs(column("score"))),
                    complete_expected,
                    true,
                ),
            ];
            for sail in [false, true] {
                let context = context(sail, true);
                for lists_left in [false, true] {
                    for (predicate, expected, pushed) in &cases {
                        let cutoffs = context.read_batch(RecordBatch::try_new(
                            Arc::new(Schema::new(vec![
                                Field::new("cutoff", DataType::Int32, false),
                                Field::new("score", DataType::Int32, false),
                            ])),
                            vec![
                                Arc::new(Int32Array::from(vec![25, 5])),
                                Arc::new(Int32Array::from(vec![-3, -1])),
                            ],
                        )?)?;
                        let lists = lists(&context)?;
                        let (left, right) = if lists_left {
                            (lists, cutoffs)
                        } else {
                            (cutoffs, lists)
                        };
                        let frame = left
                            .join_on(right, JoinType::Inner, [predicate.clone()])?
                            .select(vec![
                                column("cutoff"),
                                column("k"),
                                column("n"),
                                column("threshold"),
                            ])?;
                        let physical = plan(&context, frame).await?;
                        let mut joins = 0;
                        physical.apply(|node| {
                            if let Some(join) = node.downcast_ref::<NestedLoopJoinExec>() {
                                joins += 1;
                                let filter = join.filter().unwrap();
                                filter.expression().apply(|expr| {
                                    if let Some(column) = expr.downcast_ref::<Column>() {
                                        assert!(column.index() < filter.schema().fields().len());
                                    } else if let Some(variable) =
                                        expr.downcast_ref::<LambdaVariable>()
                                    {
                                        assert!(variable.index() >= filter.schema().fields().len());
                                    }
                                    Ok(TreeNodeRecursion::Continue)
                                })?;
                                if *pushed {
                                    assert!(join.left().is::<ProjectionExec>());
                                    assert!(join.right().is::<ProjectionExec>());
                                }
                            }
                            Ok(TreeNodeRecursion::Continue)
                        })?;
                        if *pushed {
                            assert_eq!(joins, 1);
                        }
                        let mut actual = rows(&collect(physical, context.task_ctx()).await?)?;
                        actual.sort();
                        assert_eq!(
                            &actual, expected,
                            "sail={sail}, lists_left={lists_left}: {predicate}"
                        );
                    }
                }
            }
            Ok(())
        }

        // Behavioral port of OFFSET followed by ordering, with Sail plan assertions.
        // [CREDIT]: https://github.com/apache/datafusion/blob/1635ed94742c1c060852bc34dc6ebb2302241c7b/datafusion/sqllogictest/test_files/limit.slt
        #[tokio::test]
        async fn offset_ordering_keeps_selected_rows() -> Result<()> {
            for sail in [false, true] {
                let context = context(sail, true);
                let batch = RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
                    vec![Arc::new(Int32Array::from(vec![
                        5, 3, 9, 1, 7, 2, 10, 4, 8, 6,
                    ]))],
                )?;
                for (fetch, expected) in [
                    (None, vec!["1", "2", "4", "6", "7", "8", "10"]),
                    (Some(4), vec!["1", "2", "7", "10"]),
                ] {
                    let frame = context
                        .read_batch(batch.clone())?
                        .limit(3, fetch)?
                        .sort(vec![column("a").sort(true, false)])?;
                    let physical = plan(&context, frame).await?;
                    assert!(physical.exists(|node| {
                        if let Some(sort) = node.downcast_ref::<SortExec>() {
                            return sort.input().exists(|node| Ok(node.is::<GlobalLimitExec>()));
                        }
                        Ok(false)
                    })?);
                    physical.apply(|node| {
                        if let Some(limit) = node.downcast_ref::<GlobalLimitExec>() {
                            assert!(!limit.input().exists(|node| Ok(node.is::<SortExec>()))?);
                        }
                        Ok(TreeNodeRecursion::Continue)
                    })?;
                    assert_eq!(
                        rows(&collect(physical, context.task_ctx()).await?)?,
                        expected
                    );
                }
            }
            Ok(())
        }

        #[tokio::test]
        async fn offset_preserves_required_sort_and_hidden_keys() -> Result<()> {
            for required in [false, true] {
                for hidden in [false, true] {
                    let schema = Arc::new(Schema::new(vec![
                        Field::new("payload", DataType::Int32, false),
                        Field::new("key", DataType::Int32, false),
                    ]));
                    let batch = RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![
                            Arc::new(Int32Array::from(vec![60, 10, 40, 20, 50, 30])),
                            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
                        ],
                    )?;
                    let input = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None)?;
                    let ordering = LexOrdering::new([PhysicalSortExpr::new_default(Arc::new(
                        Column::new("key", 1),
                    ))])
                    .unwrap();
                    let mut input: Arc<dyn ExecutionPlan> =
                        Arc::new(SortExec::new(ordering.clone(), input));
                    // Matches sail-session's RequiredSortNode physical lowering.
                    if required {
                        input = Arc::new(OutputRequirementExec::new(
                            input,
                            Some(ordering.into()),
                            Distribution::SinglePartition,
                            None,
                        ));
                    }
                    if hidden {
                        input = Arc::new(ProjectionExec::try_new(
                            vec![(col("payload", &input.schema())?, "payload".into())],
                            input,
                        )?);
                    }
                    let input = Arc::new(GlobalLimitExec::new(input, 2, None));
                    let ordering = LexOrdering::new([PhysicalSortExpr::new_default(Arc::new(
                        Column::new("payload", 0),
                    ))])
                    .unwrap();
                    let original: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(ordering, input));
                    for sail in [false, true] {
                        let config = ConfigOptions::default();
                        let mut optimized = Arc::clone(&original);
                        if sail {
                            for rule in get_physical_optimizers(PhysicalOptimizerOptions::default())
                            {
                                optimized = rule.optimize(optimized, &config)?;
                            }
                            check_plan_tree_sanity(&optimized, &config.optimizer)?;
                        } else {
                            optimized = EnsureRequirements::new().optimize(optimized, &config)?;
                            optimized = OutputRequirements::new_remove_mode()
                                .optimize(optimized, &config)?;
                        }
                        println!(
                            "required={required}, hidden={hidden}, sail={sail}\n{}",
                            displayable(optimized.as_ref()).indent(true)
                        );
                        let result = collect(optimized, Arc::new(TaskContext::default())).await?;
                        let actual = result
                            .iter()
                            .flat_map(|batch| {
                                batch
                                    .column(0)
                                    .as_any()
                                    .downcast_ref::<Int32Array>()
                                    .unwrap()
                                    .values()
                                    .iter()
                                    .copied()
                            })
                            .collect::<Vec<_>>();
                        assert_eq!(actual, [20, 30, 40, 50]);
                    }
                }
            }
            Ok(())
        }
    }

    use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;

    use super::*;

    #[test]
    fn test_optimizer_rules() -> datafusion::common::Result<()> {
        let datafusion_optimizers = PhysicalOptimizer::default().rules;

        let datafusion_optimizer_names: Vec<&str> =
            datafusion_optimizers.iter().map(|opt| opt.name()).collect();
        // `JoinReorder` and `JoinSelection` estimate cardinality from the `FilterExec` above
        // a scan, which `FilterPushdown` removes when the source applies the predicate exactly
        // (e.g. Parquet with `pushdown_filters`). Enabling join reorder must not move it earlier.
        for enable_join_reorder in [false, true] {
            let optimizers = get_physical_optimizers(PhysicalOptimizerOptions {
                enable_join_reorder,
                ..Default::default()
            });
            let names: Vec<_> = optimizers.iter().map(|rule| rule.name()).collect();
            let mut prefix = vec![
                OutputRequirements::new_add_mode().name().to_owned(),
                AggregateStatistics::new().name().to_owned(),
                EliminateConstantJoin::new().name().to_owned(),
            ];
            if enable_join_reorder {
                prefix.push(
                    JoinReorder::new(JoinReorderOptions::default())
                        .name()
                        .to_owned(),
                );
            }
            prefix.push(JoinSelection::new().name().to_owned());
            assert_eq!(&names[..prefix.len()], prefix.as_slice());
            assert!(names.windows(3).any(|rules| rules
                == [
                    PostFilterPushdown.name(),
                    PushDownStreamedJoinInputFileMetadata.name(),
                    RewriteInputFileMetadataFallback::new().name(),
                ]));
            let actual_datafusion_optimizer_names: Vec<&str> = optimizers
                .iter()
                .map(|opt| opt.name())
                .filter(|name| datafusion_optimizer_names.contains(name))
                .collect();
            assert_eq!(
                datafusion_optimizer_names, actual_datafusion_optimizer_names,
                "the custom physical optimizer rules should include all the default DataFusion optimizer rules in the same order (enable_join_reorder = {enable_join_reorder})"
            );
        }

        Ok(())
    }
}
