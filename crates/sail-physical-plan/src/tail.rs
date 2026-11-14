use std::collections::VecDeque;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{Distribution, Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, ReplaceChildrenOptions,
};
use datafusion_common::{DataFusionError, Result, exec_err, internal_datafusion_err};
use futures::{StreamExt, TryStreamExt, stream};
use sail_common_datafusion::utils::items::ItemTaker;

/// Returns the last rows of the input, which is evaluated once (see Spark's `CollectTailExec`).
/// The input has one partition, and only the batches that hold the last rows are kept.
#[derive(Debug, Clone)]
pub struct TailExec {
    input: Arc<dyn ExecutionPlan>,
    fetch: usize,
    properties: Arc<PlanProperties>,
}

impl TailExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, fetch: usize) -> Self {
        let properties = Arc::new(PlanProperties::new(
            input.equivalence_properties().clone(),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            input.boundedness(),
        ));
        Self {
            input,
            fetch,
            properties,
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn fetch(&self) -> usize {
        self.fetch
    }
}

impl DisplayAs for TailExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "TailExec: fetch={}", self.fetch)
    }
}

impl ExecutionPlan for TailExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input = children
            .one()
            .map_err(|_| internal_datafusion_err!("TailExec should have one child"))?;
        match options.children_properties {
            ChildrenPropertiesMode::Keep => Ok(Arc::new(Self {
                input,
                ..self.as_ref().clone()
            })),
            ChildrenPropertiesMode::Recompute => Ok(Arc::new(Self::new(input, self.fetch))),
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return exec_err!("TailExec: partition index out of range: {partition}");
        }
        let fetch = self.fetch;
        let input = if fetch == 0 {
            None
        } else {
            Some(self.input.execute(0, context)?)
        };
        let output = stream::once(async move {
            let batches = match input {
                Some(input) => take_last(last_rows(input, fetch).await?, fetch),
                None => VecDeque::new(),
            };
            Ok::<_, DataFusionError>(stream::iter(batches.into_iter().map(Ok)))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            output,
        )))
    }
}

/// Returns the batches that hold the last rows of the stream, keeping no more batches than
/// needed for them.
async fn last_rows(
    mut input: SendableRecordBatchStream,
    fetch: usize,
) -> Result<VecDeque<RecordBatch>> {
    let mut batches = VecDeque::new();
    let mut rows = 0;
    while let Some(batch) = input.next().await {
        let batch = batch?;
        if batch.num_rows() == 0 {
            continue;
        }
        rows += batch.num_rows();
        batches.push_back(batch);
        while let Some(front) = batches.front()
            && rows - front.num_rows() >= fetch
        {
            rows -= front.num_rows();
            batches.pop_front();
        }
    }
    Ok(batches)
}

/// Returns the last rows of the batches.
fn take_last(mut batches: VecDeque<RecordBatch>, fetch: usize) -> VecDeque<RecordBatch> {
    let mut rows = batches.iter().map(|x| x.num_rows()).sum::<usize>();
    while let Some(front) = batches.front_mut()
        && rows > fetch
    {
        let excess = rows - fetch;
        if front.num_rows() <= excess {
            rows -= front.num_rows();
            batches.pop_front();
        } else {
            *front = front.slice(excess, front.num_rows() - excess);
            rows = fetch;
        }
    }
    batches
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{AsArray, Int64Array, RecordBatchOptions, new_null_array};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::{
        DataType, Field, Int64Type, IntervalUnit, Schema, TimeUnit, UnionFields, UnionMode,
    };
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::collect;
    use sail_common_datafusion::array::placeholder::placeholder_array;

    use super::*;

    /// Returns the last rows of a partition of six batches of three rows each,
    /// where one batch has no rows.
    async fn tail(fetch: usize) -> Result<Vec<i64>> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batches = (0..6)
            .map(|batch| {
                let start = batch * 3;
                let end = if batch == 4 { start } else { start + 3 };
                Ok(RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int64Array::from_iter_values(start..end))],
                )?)
            })
            .collect::<Result<Vec<_>>>()?;
        let input = MemorySourceConfig::try_new_exec(&[batches], schema, None)?;
        let plan = Arc::new(TailExec::new(input, fetch));
        let batches = collect(plan, Arc::new(TaskContext::default())).await?;
        Ok(batches
            .iter()
            .flat_map(|x| x.column(0).as_primitive::<Int64Type>().values().to_vec())
            .collect())
    }

    #[tokio::test]
    async fn test_tail() -> Result<()> {
        let rows = (0..12).chain(15..18).collect::<Vec<_>>();
        assert_eq!(tail(0).await?, Vec::<i64>::new());
        assert_eq!(tail(1).await?, vec![17]);
        assert_eq!(tail(4).await?, vec![11, 15, 16, 17]);
        assert_eq!(tail(10).await?, rows[5..].to_vec());
        assert_eq!(tail(100).await?, rows);
        Ok(())
    }

    #[tokio::test]
    async fn test_tail_propagates_late_error_under_an_early_limit() -> Result<()> {
        use datafusion::physical_plan::limit::GlobalLimitExec;
        use datafusion::physical_plan::test::exec::MockExec;

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )?;
        let input = Arc::new(
            MockExec::new(
                vec![Ok(batch), exec_err!("terminal input error after rows")],
                schema,
            )
            .with_unknown_statistics()
            .with_use_task(false),
        );
        // Tail must observe the end of its input before emitting any retained rows.
        // Otherwise LIMIT 1 could stop polling before the source reports its error.
        let tail = Arc::new(TailExec::new(input, 2));
        let limited = Arc::new(GlobalLimitExec::new(tail, 0, Some(1)));
        let error = collect(limited, Arc::new(TaskContext::default()))
            .await
            .expect_err("tail must propagate a terminal error before returning rows");
        assert!(
            error
                .to_string()
                .contains("terminal input error after rows")
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_zero_tail_does_not_execute_its_input() -> Result<()> {
        use datafusion::physical_plan::test::exec::ErrorExec;

        // ErrorExec fails in execute(), before producing a stream. A zero tail must
        // avoid executing the input, not merely avoid polling its stream.
        let plan = Arc::new(TailExec::new(Arc::new(ErrorExec::new()), 0));
        assert!(
            collect(plan, Arc::new(TaskContext::default()))
                .await?
                .is_empty()
        );
        Ok(())
    }

    fn arrow_types() -> Vec<DataType> {
        let child = Arc::new(Field::new("item", DataType::Int32, true));
        let fields = vec![
            Field::new("number", DataType::Int32, true),
            Field::new("text", DataType::Utf8, true),
        ];
        let union_fields: UnionFields = fields
            .iter()
            .enumerate()
            .map(|(i, f)| (i as i8, Arc::new(f.clone())))
            .collect();
        let mut types = vec![
            DataType::Null,
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float16,
            DataType::Float32,
            DataType::Float64,
            DataType::Date32,
            DataType::Date64,
            DataType::Time32(TimeUnit::Second),
            DataType::Time32(TimeUnit::Millisecond),
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::Interval(IntervalUnit::YearMonth),
            DataType::Interval(IntervalUnit::DayTime),
            DataType::Interval(IntervalUnit::MonthDayNano),
            DataType::Binary,
            DataType::LargeBinary,
            DataType::BinaryView,
            DataType::FixedSizeBinary(4),
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Utf8View,
            DataType::Decimal32(8, 2),
            DataType::Decimal64(16, 4),
            DataType::Decimal128(30, 6),
            DataType::Decimal256(60, 8),
            DataType::List(Arc::clone(&child)),
            DataType::LargeList(Arc::clone(&child)),
            DataType::ListView(Arc::clone(&child)),
            DataType::LargeListView(Arc::clone(&child)),
            DataType::FixedSizeList(Arc::clone(&child), 3),
            DataType::Struct(fields.into()),
            DataType::Union(union_fields.clone(), UnionMode::Sparse),
            DataType::Union(union_fields, UnionMode::Dense),
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::List(child), true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
        ];
        for unit in [
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ] {
            types.push(DataType::Duration(unit.clone()));
            types.push(DataType::Timestamp(unit.clone(), None));
            types.push(DataType::Timestamp(unit, Some("UTC".into())));
        }
        for key in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
        ] {
            types.push(DataType::Dictionary(
                Box::new(key),
                Box::new(DataType::Utf8),
            ));
        }
        for run_end in [DataType::Int16, DataType::Int32, DataType::Int64] {
            types.push(DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends", run_end, false)),
                Arc::new(Field::new("values", DataType::Utf8, true)),
            ));
        }
        types
    }

    #[tokio::test]
    async fn test_tail_preserves_arrow_values_and_schema() -> Result<()> {
        // Alternate valid and null batches, including empty batches and nonzero array
        // offsets. The ordinal column makes batch loss, duplication and reordering visible.
        for data_type in arrow_types() {
            let schema = Arc::new(Schema::new_with_metadata(
                vec![
                    Field::new("ordinal", DataType::Int64, false),
                    Field::new("value", data_type.clone(), true)
                        .with_metadata([("field-key".to_owned(), "field-value".to_owned())].into()),
                ],
                [("schema-key".to_owned(), "schema-value".to_owned())].into(),
            ));
            let mut ordinal = 0;
            let batches = [0, 1, 4, 0, 6, 2, 4, 0]
                .into_iter()
                .enumerate()
                .map(|(index, length)| {
                    let values = if index % 2 == 0 {
                        let array = placeholder_array(&data_type, length + 2)?;
                        // Placeholder decimal arrays use their default precision and scale.
                        cast(array.as_ref(), &data_type)?
                    } else {
                        new_null_array(&data_type, length + 2)
                    };
                    let ids = Int64Array::from_iter_values(ordinal..ordinal + length as i64);
                    ordinal += length as i64;
                    Ok(RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![Arc::new(ids), values.slice(1, length)],
                    )?)
                })
                .collect::<Result<Vec<_>>>()?;
            let expected = batches
                .iter()
                .flat_map(|batch| (0..batch.num_rows()).map(|row| batch.slice(row, 1)))
                .collect::<Vec<_>>();
            for fetch in [0, 1, 4, 5, 9, 17, 100] {
                let input = MemorySourceConfig::try_new_exec(
                    std::slice::from_ref(&batches),
                    Arc::clone(&schema),
                    None,
                )?;
                let plan = Arc::new(TailExec::new(input, fetch));
                assert_eq!(plan.schema(), schema);
                let actual = collect(plan, Arc::new(TaskContext::default())).await?;
                let actual = actual
                    .iter()
                    .flat_map(|batch| (0..batch.num_rows()).map(|row| batch.slice(row, 1)))
                    .collect::<Vec<_>>();
                assert_eq!(
                    actual,
                    expected[expected.len().saturating_sub(fetch)..],
                    "type={data_type:?}, fetch={fetch}"
                );
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_tail_of_zero_column_batches() -> Result<()> {
        let schema = Arc::new(Schema::empty());
        let batches = [0, 3, 0, 7]
            .into_iter()
            .map(|rows| {
                RecordBatch::try_new_with_options(
                    Arc::clone(&schema),
                    vec![],
                    &RecordBatchOptions::new().with_row_count(Some(rows)),
                )
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        for fetch in [0, 1, 5, 10, 20] {
            let input = MemorySourceConfig::try_new_exec(
                std::slice::from_ref(&batches),
                Arc::clone(&schema),
                None,
            )?;
            let output = collect(
                Arc::new(TailExec::new(input, fetch)),
                Arc::new(TaskContext::default()),
            )
            .await?;
            assert_eq!(
                output.iter().map(RecordBatch::num_rows).sum::<usize>(),
                fetch.min(10)
            );
            assert!(output.iter().all(|batch| batch.schema() == schema));
        }
        Ok(())
    }
}
