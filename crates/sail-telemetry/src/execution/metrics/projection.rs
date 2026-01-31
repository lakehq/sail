use datafusion::physical_expr_common::metrics::MetricValue;
use datafusion::physical_plan::Metric;
use lazy_static::lazy_static;
use regex::Regex;

use crate::common::KeyValue;
use crate::execution::metrics::default::LabelExtractor;
use crate::execution::metrics::{MetricEmitter, MetricHandled};
use crate::metrics::{MetricAttribute, MetricRegistry};

lazy_static! {
    static ref EXPR_EVAL_TIME_METRIC_NAME_REGEX: Regex = {
        #[allow(clippy::unwrap_used)]
        Regex::new(r"^expr_([0-9]+)_eval_time$").unwrap()
    };
}

/// A metric emitter for projection operator metrics.
pub struct ProjectionMetricEmitter;

impl MetricEmitter for ProjectionMetricEmitter {
    fn try_emit(
        &self,
        metric: &Metric,
        attributes: &[KeyValue],
        registry: &MetricRegistry,
    ) -> MetricHandled {
        match metric.value() {
            MetricValue::Time { name, time } => {
                if let Some(captures) = EXPR_EVAL_TIME_METRIC_NAME_REGEX.captures(name) {
                    registry
                        .execution_expression_evaluation_time
                        .recorder(time)
                        .with_attributes(attributes)
                        .with_attributes(
                            &LabelExtractor::new()
                                .with_extractor("expr", MetricAttribute::EXECUTION_EXPRESSION_NAME)
                                .extract(metric.labels(), registry),
                        )
                        .with_optional_attribute(
                            MetricAttribute::EXECUTION_EXPRESSION_ID,
                            captures.get(1).map(|m| m.as_str()),
                        )
                        .with_optional_attribute(
                            MetricAttribute::EXECUTION_PARTITION,
                            metric.partition(),
                        )
                        .emit();
                    MetricHandled::Yes
                } else {
                    MetricHandled::No
                }
            }
            _ => MetricHandled::No,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::Result;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion::physical_plan::PhysicalExpr;

    use crate::execution::metrics::testing::MetricEmitterTester;

    #[tokio::test]
    async fn test_projection_metrics() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));
        let plan = Arc::new(EmptyExec::new(schema));
        let plan = Arc::new(ProjectionExec::try_new(
            vec![(
                Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
                "b".to_string(),
            )],
            plan,
        )?);

        MetricEmitterTester::new()
            .with_plan(plan)
            .with_expected_metrics(|registry| {
                vec![registry.execution_expression_evaluation_time.name()]
            })
            .run()
            .await
    }
}
