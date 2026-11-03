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
    EliminateConstantJoin, PhysicalOptimizerOptions, PhysicalOptimizerRule, get_physical_optimizers,
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
                            Field::new("value", DataType::Int64, true)
                                .with_metadata(HashMap::from([("field".into(), "event".into())])),
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
                    let order = LexOrdering::new([PhysicalSortExpr::new_default(Arc::new(
                        Column::new("id", 0),
                    ))])
                    .unwrap();
                    let events: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(
                        order,
                        MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], schema, None)?,
                    ));
                    let constant: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
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
                            PlaceholderRowExec::new(Arc::new(Schema::new_with_metadata(
                                Vec::<Field>::new(),
                                HashMap::from([("constant".into(), "retained".into())]),
                            )))
                            .with_partitions(partitions),
                        ),
                    )?);
                    let (left, right, constant_side, event_side, constant_start, event_start) =
                        if constant_left {
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
                        let filter = projection.input().downcast_ref::<FilterExec>().unwrap();
                        let predicate = filter.predicate().downcast_ref::<BinaryExpr>().unwrap();
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
                        let expected: ArrayRef =
                            if (constant_start..constant_start + 2).contains(&index) {
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
                        assert_eq!(optimized.schema().field(1), optimized.schema().field(3));
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
