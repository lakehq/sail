//! This module bridges DataFusion query execution metrics with OpenTelemetry.

mod default;
mod filter;
mod join;
mod projection;
#[cfg(test)]
pub(super) mod testing;

use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, PiecewiseMergeJoinExec, SortMergeJoinExec,
    SymmetricHashJoinExec,
};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, Metric};
use paste::paste;

use crate::common::KeyValue;
use crate::metrics::MetricRegistry;

/// A flag indicating whether the metric was handled by a metric emitter.
pub enum MetricHandled {
    /// The metric was emitted or intentionally ignored.
    Yes,
    /// The metric was not handled.
    No,
}

/// A trait for emitting DataFusion execution metrics.
pub trait MetricEmitter: Send {
    /// Try to emit the given metric.
    fn try_emit(
        &self,
        metric: &Metric,
        attributes: &[KeyValue],
        registry: &MetricRegistry,
    ) -> MetricHandled;
}

macro_rules! impl_chained_metric_emitter {
    // We have to use an explicit separator before the last type parameter
    // to avoid macro parsing ambiguity.
    ($($Ts:ident)* : $Tn:ident) => {
        impl<$($Ts: MetricEmitter,)* $Tn: MetricEmitter> MetricEmitter for ($($Ts,)* $Tn,) {
            fn try_emit(
                &self,
                metric: &Metric,
                attributes: &[KeyValue],
                registry: &MetricRegistry,
            ) -> MetricHandled {
                let paste!(($([<$Ts:lower>],)* [<$Tn:lower>],)) = self;
                $(if let x @ MetricHandled::Yes = paste!([<$Ts:lower>]).try_emit(metric, attributes, registry) {
                    return x;
                })*
                paste!([<$Tn:lower>]).try_emit(metric, attributes, registry)
            }
        }
    }
}

impl_chained_metric_emitter!(: T1);
impl_chained_metric_emitter!(T1 : T2);
impl_chained_metric_emitter!(T1 T2 : T3);
impl_chained_metric_emitter!(T1 T2 T3 : T4);
impl_chained_metric_emitter!(T1 T2 T3 T4 : T5);
impl_chained_metric_emitter!(T1 T2 T3 T4 T5 : T6);
impl_chained_metric_emitter!(T1 T2 T3 T4 T5 T6 : T7);
impl_chained_metric_emitter!(T1 T2 T3 T4 T5 T6 T7 : T8);
impl_chained_metric_emitter!(T1 T2 T3 T4 T5 T6 T7 T8 : T9);

/// Build a metric emitter based on the type of the execution plan.
pub fn build_metric_emitter(plan: &dyn ExecutionPlan) -> Box<dyn MetricEmitter> {
    let plan = plan.as_any();
    if plan.is::<ProjectionExec>() {
        Box::new((
            projection::ProjectionMetricEmitter,
            default::DefaultMetricEmitter,
        ))
    } else if plan.is::<FilterExec>() {
        Box::new((filter::FilterMetricEmitter, default::DefaultMetricEmitter))
    } else if plan.is::<HashJoinExec>()
        || plan.is::<PiecewiseMergeJoinExec>()
        || plan.is::<CrossJoinExec>()
    {
        Box::new((
            join::BuildProbeJoinMetricEmitter,
            default::DefaultMetricEmitter,
        ))
    } else if plan.is::<NestedLoopJoinExec>() {
        Box::new((
            join::BuildProbeJoinMetricEmitter,
            join::NestedLoopJoinMetricEmitter,
            default::DefaultMetricEmitter,
        ))
    } else if plan.is::<SymmetricHashJoinExec>() {
        Box::new((join::StreamJoinMetricEmitter, default::DefaultMetricEmitter))
    } else if plan.is::<SortMergeJoinExec>() {
        Box::new((
            join::SortMergeJoinMetricEmitter,
            default::DefaultMetricEmitter,
        ))
    } else {
        Box::new(default::DefaultMetricEmitter)
    }
}
