use std::sync::Arc;

use datafusion::common::Statistics;
use datafusion::error::Result;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizerContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::operator_statistics::StatisticsRegistry;
use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};

/// Use the same statistics source as DataFusion's join selection, including overrides.
pub(super) fn statistics(
    plan: &dyn ExecutionPlan,
    context: Option<&dyn PhysicalOptimizerContext>,
) -> Result<Arc<Statistics>> {
    if let Some(context) = context.filter(|c| c.config_options().optimizer.use_statistics_registry)
    {
        if let Some(registry) = context.statistics_registry() {
            Ok(Arc::clone(registry.compute(plan)?.base_arc()))
        } else {
            Ok(Arc::clone(
                StatisticsRegistry::default_with_builtin_providers()
                    .compute(plan)?
                    .base_arc(),
            ))
        }
    } else {
        StatisticsContext::new().compute(plan, &StatisticsArgs::new())
    }
}
