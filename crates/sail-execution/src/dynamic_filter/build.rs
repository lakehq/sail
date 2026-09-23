use std::fmt::{self, Formatter};
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Float32Array, Float64Array};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, ScalarValue, plan_datafusion_err};
use datafusion::config::OptimizerOptions;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion::logical_expr::{Accumulator, Operator};
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
use datafusion::physical_expr::expressions::{
    BinaryExpr, DynamicFilterPhysicalExpr, InListExpr, IsNullExpr, lit,
};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::joins::{HashJoinExec, HashJoinExecBuilder};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, replace_children_if_necessary,
};
use futures::TryStreamExt;
use indexmap::IndexSet;
use sail_physical_plan::coalesce::CoalesceExec;
use sail_physical_plan::repartition::ExplicitRepartitionExec;

/// Collect bounded key summaries before the build-side exchange. Publishing only
/// after EOF makes every partition summary safe for the driver's union.
#[derive(Debug, Clone)]
pub(crate) struct DynamicFilterBuildExec {
    pub input: Arc<dyn ExecutionPlan>,
    pub keys: Vec<Arc<dyn PhysicalExpr>>,
    pub filter: Arc<dyn PhysicalExpr>,
    pub probe_schema: SchemaRef,
    pub null_equals_null: bool,
    pub null_aware: bool,
}

impl DisplayAs for DynamicFilterBuildExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "DynamicFilterBuildExec: keys=[{}]",
            self.keys
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl ExecutionPlan for DynamicFilterBuildExec {
    fn name(&self) -> &str {
        "DynamicFilterBuildExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn dynamic_expressions_produced(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.filter.clone()]
    }
    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        datafusion::physical_plan::apply_expression_roots(
            self.keys.iter().chain(std::iter::once(&self.filter)),
            f,
        )
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input]: [Arc<dyn ExecutionPlan>; 1] = children
            .try_into()
            .map_err(|_| plan_datafusion_err!("expected one dynamic filter build input"))?;
        Ok(Arc::new(Self {
            input,
            ..self.as_ref().clone()
        }))
    }
    #[expect(deprecated)]
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _: datafusion::physical_plan::ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_new_children(children)
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut input = self.input.execute(partition, context.clone())?;
        let keys = self.keys.clone();
        let filter = self.filter.clone();
        let probe_schema = self.probe_schema.clone();
        let null_equals_null = self.null_equals_null;
        let null_aware = self.null_aware;
        let mut summaries = keys
            .iter()
            .map(|key| {
                KeySummary::new(
                    key.clone(),
                    self.input.schema(),
                    &context.session_config().options().optimizer,
                )
            })
            .collect::<Vec<_>>();
        let output = async_stream::try_stream! {
            let mut rows = 0;
            while let Some(batch) = input.try_next().await? {
                rows += batch.num_rows();
                for (key, summary) in keys.iter().zip(&mut summaries) {
                    summary.update(&key.evaluate(&batch)?.into_array(batch.num_rows())?);
                }
                yield batch;
            }
            let dynamic = filter.downcast_ref::<DynamicFilterPhysicalExpr>().ok_or_else(|| plan_datafusion_err!("expected build dynamic filter"))?;
            let mut predicate = lit(rows != 0);
            if rows != 0 {
                for (summary, probe_key) in summaries.into_iter().zip(filter.children()) {
                    let keep_null = null_aware || (null_equals_null && summary.has_null);
                    let mut condition = summary.predicate(probe_key.clone(), &probe_schema)
                        .unwrap_or_else(|_| lit(true));
                    if keep_null {
                        condition = Arc::new(BinaryExpr::new(Arc::new(IsNullExpr::new(probe_key.clone())), Operator::Or, condition));
                    }
                    predicate = Arc::new(BinaryExpr::new(predicate, Operator::And, condition));
                }
            }
            dynamic.update(predicate)?;
            dynamic.mark_complete();
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            output,
        )))
    }
}

struct KeySummary {
    values: Option<IndexSet<ScalarValue>>,
    bytes: usize,
    max_values: usize,
    max_bytes: usize,
    bounds: Option<(Box<dyn Accumulator>, Box<dyn Accumulator>)>,
    has_null: bool,
    has_nan: bool,
    has_float_zero: bool,
}

impl KeySummary {
    fn new(key: Arc<dyn PhysicalExpr>, schema: SchemaRef, options: &OptimizerOptions) -> Self {
        let bounds = || -> Result<_> {
            let min = AggregateExprBuilder::new(min_udaf(), vec![key.clone()])
                .schema(schema.clone())
                .alias("min")
                .build()?
                .create_accumulator()?;
            let max = AggregateExprBuilder::new(max_udaf(), vec![key])
                .schema(schema)
                .alias("max")
                .build()?
                .create_accumulator()?;
            Ok((min, max))
        };
        Self {
            values: Some(IndexSet::new()),
            bytes: 0,
            max_values: options.hash_join_inlist_pushdown_max_distinct_values,
            max_bytes: options.hash_join_inlist_pushdown_max_size,
            bounds: bounds().ok(),
            has_null: false,
            has_nan: false,
            has_float_zero: false,
        }
    }

    fn update(&mut self, array: &ArrayRef) {
        self.has_null |= array.null_count() > 0;
        if let Some(array) = array.as_any().downcast_ref::<Float32Array>() {
            for value in array.iter().flatten() {
                self.has_nan |= value.is_nan();
                self.has_float_zero |= value == 0.0;
            }
        } else if let Some(array) = array.as_any().downcast_ref::<Float64Array>() {
            for value in array.iter().flatten() {
                self.has_nan |= value.is_nan();
                self.has_float_zero |= value == 0.0;
            }
        }
        if let Some((min, max)) = &mut self.bounds
            && (min.update_batch(std::slice::from_ref(array)).is_err()
                || max.update_batch(std::slice::from_ref(array)).is_err())
        {
            self.bounds = None;
        }
        if let Some(values) = &mut self.values {
            for index in 0..array.len() {
                if array.is_null(index) {
                    continue;
                }
                let Ok(value) = ScalarValue::try_from_array(array.as_ref(), index) else {
                    self.values = None;
                    break;
                };
                if !values.contains(&value) {
                    self.bytes += value.size();
                    if values.len() >= self.max_values || self.bytes > self.max_bytes {
                        self.values = None;
                        break;
                    }
                    values.insert(value);
                }
            }
        }
    }

    fn predicate(
        self,
        key: Arc<dyn PhysicalExpr>,
        schema: &datafusion::arrow::datatypes::Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let has_nan = self.has_nan;
        let has_float_zero = self.has_float_zero;
        let mut predicate = self.value_predicate(key.clone(), schema)?;
        // Membership and ordering kernels can distinguish floating-point bit
        // patterns that join equality treats alike. Keep both zeros and all NaNs.
        if has_float_zero {
            let zeros = match key.data_type(schema)? {
                DataType::Float32 => vec![lit(-0.0_f32), lit(0.0_f32)],
                DataType::Float64 => vec![lit(-0.0_f64), lit(0.0_f64)],
                _ => vec![],
            };
            predicate = Arc::new(BinaryExpr::new(
                predicate,
                Operator::Or,
                Arc::new(InListExpr::try_new(key.clone(), zeros, false, schema)?),
            ));
        }
        if has_nan {
            let nan = ScalarFunctionExpr::try_new(
                datafusion::functions::math::isnan(),
                vec![key],
                schema,
                Arc::default(),
            )?;
            predicate = Arc::new(BinaryExpr::new(predicate, Operator::Or, Arc::new(nan)));
        }
        Ok(predicate)
    }

    fn value_predicate(
        self,
        key: Arc<dyn PhysicalExpr>,
        schema: &datafusion::arrow::datatypes::Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let mut predicate = lit(true);
        if let Some((mut min, mut max)) = self.bounds {
            let min = min.evaluate()?;
            let max = max.evaluate()?;
            if !min.is_null() && !max.is_null() {
                predicate = Arc::new(BinaryExpr::new(
                    Arc::new(BinaryExpr::new(key.clone(), Operator::GtEq, lit(min))),
                    Operator::And,
                    Arc::new(BinaryExpr::new(key.clone(), Operator::LtEq, lit(max))),
                ));
            }
        }
        if let Some(values) = self.values {
            if values.is_empty() {
                return Ok(lit(false));
            }
            // Statistics pruning may ignore large IN lists. Keep the bounds
            // alongside exact membership so row groups can still be skipped.
            predicate = Arc::new(BinaryExpr::new(
                predicate,
                Operator::And,
                Arc::new(InListExpr::try_new(
                    key,
                    values.into_iter().map(lit).collect(),
                    false,
                    schema,
                )?),
            ));
        }
        Ok(predicate)
    }
}

pub(crate) fn prepare_join_filters(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    plan.transform_up(|plan| {
        let Some(join) = plan.downcast_ref::<HashJoinExec>() else {
            return Ok(Transformed::no(plan));
        };
        let Some(filter) = join.dynamic_expressions_produced().into_iter().next() else {
            return Ok(Transformed::no(plan));
        };
        let has_consumer = join.right().exists(|node| {
            Ok(super::consumer_filter_ids(node.as_ref())?
                .iter()
                .any(|id| Some(*id) == filter.expression_id()))
        })?;
        let left = if has_consumer {
            move_before_exchange(DynamicFilterBuildExec {
                input: join.left().clone(),
                keys: join.on.iter().map(|(left, _)| left.clone()).collect(),
                filter,
                probe_schema: join.right().schema(),
                null_equals_null: join.null_equality
                    == datafusion::common::NullEquality::NullEqualsNull,
                null_aware: join.null_aware,
            })?
        } else {
            join.left().clone()
        };
        // The distributed producer owns filter completion. The ordinary join must
        // not wait on DataFusion's process-local sibling accumulator.
        let join =
            HashJoinExecBuilder::new(left, join.right().clone(), join.on.clone(), join.join_type)
                .with_partition_mode(join.mode)
                .with_filter(join.filter.clone())
                .with_projection_ref(join.projection.clone())
                .with_null_equality(join.null_equality)
                .with_null_aware(join.null_aware)
                .with_fetch(join.fetch())
                .build_exec()?;
        Ok(Transformed::yes(join))
    })
    .data()
}

fn move_before_exchange(build: DynamicFilterBuildExec) -> Result<Arc<dyn ExecutionPlan>> {
    let input = &build.input;
    if input.is::<RepartitionExec>()
        || input.is::<CoalescePartitionsExec>()
        || input.is::<SortPreservingMergeExec>()
        || input.is::<CooperativeExec>()
        || input.is::<CoalesceExec>()
        || input.is::<ExplicitRepartitionExec>()
    {
        let children = input.children();
        if let [child] = children.as_slice() {
            let child = move_before_exchange(DynamicFilterBuildExec {
                input: (*child).clone(),
                ..build.clone()
            })?;
            return replace_children_if_necessary(input.clone(), vec![child]);
        }
    }
    Ok(Arc::new(build))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{BooleanArray, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::expressions::Column;

    use super::*;

    #[test]
    fn bounded_key_sets_fall_back_to_ranges() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("k", 0));
        let mut summary =
            KeySummary::new(key.clone(), schema.clone(), &OptimizerOptions::default());
        summary.update(&(Arc::new(Int64Array::from_iter_values(64..8256)) as ArrayRef));
        assert!(summary.values.is_none());
        let predicate = summary.predicate(key, &schema)?;
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![63, 64, 8255, 8256]))],
        )?;
        let values = predicate.evaluate(&batch)?.into_array(4)?;
        let values = values
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| plan_datafusion_err!("expected Boolean array"))?;
        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            vec![Some(false), Some(true), Some(true), Some(false)]
        );
        Ok(())
    }

    #[test]
    fn key_summaries_honor_inlist_limits() {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("k", 0));
        let array: ArrayRef = Arc::new(Int64Array::from(vec![1, 3, 5]));
        for (max_values, max_bytes, keep_values) in
            [(3, 1024, true), (2, 1024, false), (3, 0, false)]
        {
            let options = OptimizerOptions {
                hash_join_inlist_pushdown_max_distinct_values: max_values,
                hash_join_inlist_pushdown_max_size: max_bytes,
                ..Default::default()
            };
            let mut summary = KeySummary::new(key.clone(), schema.clone(), &options);
            summary.update(&array);
            assert_eq!(summary.values.is_some(), keep_values);
            assert!(summary.bounds.is_some());
        }
    }

    #[test]
    fn large_exact_sets_retain_row_group_pruning() -> Result<()> {
        use datafusion::common::Statistics;
        use datafusion::common::pruning::PrunableStatistics;
        use datafusion::common::stats::Precision;
        use datafusion::physical_optimizer::pruning::PruningPredicateBuilder;

        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("k", 0));
        let mut summary =
            KeySummary::new(key.clone(), schema.clone(), &OptimizerOptions::default());
        summary.update(&(Arc::new(Int64Array::from_iter_values(100..131)) as ArrayRef));
        assert_eq!(summary.values.as_ref().map(IndexSet::len), Some(31));
        let predicate = summary.predicate(key, &schema)?;
        let statistics = [0, 100, 200]
            .into_iter()
            .map(|min| {
                let mut stats = Statistics::new_unknown(&schema);
                stats.num_rows = Precision::Exact(100);
                stats.column_statistics[0].min_value =
                    Precision::Exact(ScalarValue::Int64(Some(min)));
                stats.column_statistics[0].max_value =
                    Precision::Exact(ScalarValue::Int64(Some(min + 99)));
                stats.column_statistics[0].null_count = Precision::Exact(0);
                Arc::new(stats)
            })
            .collect();
        let pruning = PruningPredicateBuilder::new()
            .with_file_schema(schema.clone())
            .with_max_in_list_size(20)
            .try_build(predicate.clone())?;
        assert_eq!(
            pruning.prune(&PrunableStatistics::new(statistics, schema.clone()))?,
            vec![false, true, false]
        );
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![100, 150]))])?;
        let values = predicate.evaluate(&batch)?.into_array(2)?;
        assert_eq!(
            values.as_ref(),
            &BooleanArray::from(vec![true, false]) as &dyn datafusion::arrow::array::Array
        );
        Ok(())
    }

    #[test]
    fn large_strings_exhaust_the_byte_budget_without_dropping_matches() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Utf8, true)]));
        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("k", 0));
        let value = "x".repeat(128 * 1024);
        let array: ArrayRef = Arc::new(StringArray::from(vec![Some(value.as_str()), None]));
        let mut summary =
            KeySummary::new(key.clone(), schema.clone(), &OptimizerOptions::default());
        summary.update(&array);
        assert!(summary.values.is_none());
        assert!(summary.has_null);
        let predicate = summary.predicate(key, &schema)?;
        let batch = RecordBatch::try_new(schema, vec![array])?;
        let values = predicate.evaluate(&batch)?.into_array(2)?;
        let values = values
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| plan_datafusion_err!("expected Boolean array"))?;
        assert_eq!(values.iter().collect::<Vec<_>>(), vec![Some(true), None]);
        Ok(())
    }

    #[test]
    fn unused_filters_do_not_collect_build_keys() -> Result<()> {
        use datafusion::common::JoinType;
        use datafusion::physical_plan::empty::EmptyExec;
        use datafusion::physical_plan::joins::PartitionMode;

        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let input = Arc::new(EmptyExec::new(schema));
        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("k", 0));
        let filter = Arc::new(DynamicFilterPhysicalExpr::new(vec![key.clone()], lit(true)));
        let join = HashJoinExecBuilder::new(
            input.clone(),
            input,
            vec![(key.clone(), key)],
            JoinType::Inner,
        )
        .with_partition_mode(PartitionMode::CollectLeft)
        .build()?
        .with_dynamic_filter_expr(filter)?;
        let plan = prepare_join_filters(Arc::new(join))?;
        let join = plan
            .downcast_ref::<HashJoinExec>()
            .ok_or_else(|| plan_datafusion_err!("expected hash join"))?;
        assert!(join.left().is::<EmptyExec>());
        assert!(join.dynamic_expressions_produced().is_empty());
        Ok(())
    }

    #[test]
    fn floating_point_summaries_keep_nan_and_signed_zero() -> Result<()> {
        use datafusion::arrow::compute::cast;
        use datafusion::prelude::SessionContext;

        use crate::dynamic_filter::wire::{DynamicFilterBinding, snapshot};

        for data_type in [DataType::Float32, DataType::Float64] {
            for large in [false, true] {
                let schema = Arc::new(Schema::new(vec![Field::new("k", data_type.clone(), true)]));
                let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("k", 0));
                let mut summary =
                    KeySummary::new(key.clone(), schema.clone(), &OptimizerOptions::default());
                let mut build = vec![f64::NAN, 0.0, f64::INFINITY];
                if large {
                    build.extend((1..8192).map(f64::from));
                }
                summary.update(&cast(&Float64Array::from(build), &data_type)?);
                let predicate = summary.predicate(key.clone(), &schema)?;
                let producer: Arc<dyn PhysicalExpr> =
                    Arc::new(DynamicFilterPhysicalExpr::new(vec![key.clone()], predicate));
                let consumer = DynamicFilterBinding {
                    filter: Arc::new(DynamicFilterPhysicalExpr::new(vec![key], lit(true))),
                };
                consumer.apply(
                    &snapshot(&producer, &schema, true)?,
                    &SessionContext::new().task_ctx(),
                )?;
                let values = Float64Array::from(vec![
                    Some(f64::NAN),
                    Some(-f64::NAN),
                    Some(-0.0),
                    Some(0.0),
                    Some(f64::INFINITY),
                ]);
                let batch = RecordBatch::try_new(schema, vec![cast(&values, &data_type)?])?;
                let values = consumer.filter.evaluate(&batch)?.into_array(5)?;
                let values = values
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| plan_datafusion_err!("expected Boolean array"))?;
                assert_eq!(values.iter().collect::<Vec<_>>(), vec![Some(true); 5]);
            }
        }
        Ok(())
    }
}
