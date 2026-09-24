use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion::common::stats::{ColumnStatistics, Precision, Statistics};
use datafusion::common::tree_node::{Transformed, TreeNodeRecursion};
use datafusion::logical_expr::expr_rewriter::unnormalize_cols;
use datafusion::logical_expr::{Expr, LogicalPlan};
use sail_common_datafusion::logical_rewriter::LogicalRewriter;
use sail_common_datafusion::metadata_aggregate::{
    AggregateInput, ExactAggregateStatistics, rewrite_aggregate,
};

use super::IcebergTableSource;
use crate::datasource::scan::{IcebergScan, IcebergScanPlan};

#[derive(Debug)]
pub struct IcebergMetadataAggregateRewriter;

struct AggregateRequest {
    scan: Arc<IcebergScan>,
    filters: Vec<Expr>,
    planned: Option<Arc<IcebergScanPlan>>,
}

#[async_trait]
impl LogicalRewriter for IcebergMetadataAggregateRewriter {
    fn name(&self) -> &str {
        "iceberg_metadata_aggregate"
    }

    async fn rewrite(
        &self,
        plan: LogicalPlan,
        session: &dyn Session,
    ) -> Result<Transformed<LogicalPlan>> {
        let mut requests: Vec<AggregateRequest> = vec![];
        plan.apply_with_subqueries(|plan| {
            let LogicalPlan::Aggregate(aggregate) = plan else {
                return Ok(TreeNodeRecursion::Continue);
            };
            if !aggregate.group_expr.is_empty()
                || !aggregate.aggr_expr.iter().any(supported_aggregate)
            {
                return Ok(TreeNodeRecursion::Continue);
            }
            let Some(input) = AggregateInput::try_new(aggregate.input.as_ref()) else {
                return Ok(TreeNodeRecursion::Continue);
            };
            let scan = input.scan();
            let Some(source) = scan.source.downcast_ref::<IcebergTableSource>() else {
                return Ok(TreeNodeRecursion::Continue);
            };
            if scan.fetch.is_some() || !source.scan().metadata_aggregate_enabled() {
                return Ok(TreeNodeRecursion::Continue);
            }
            let filters = unnormalize_cols(scan.filters.clone());
            if !requests.iter().any(|request| {
                Arc::ptr_eq(&request.scan, source.scan()) && request.filters == filters
            }) {
                requests.push(AggregateRequest {
                    scan: Arc::clone(source.scan()),
                    filters,
                    planned: None,
                });
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        for request in &mut requests {
            request.planned = Some(Arc::new(
                request
                    .scan
                    .plan_files(session, &request.filters, None)
                    .await?,
            ));
        }
        plan.transform_up_with_subqueries(|plan| {
            let LogicalPlan::Aggregate(mut aggregate) = plan else {
                return Ok(Transformed::no(plan));
            };
            let Some(input) = AggregateInput::try_new(aggregate.input.as_ref()) else {
                return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
            };
            let scan = input.scan();
            let Some(source) = scan.source.downcast_ref::<IcebergTableSource>() else {
                return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
            };
            let filters = unnormalize_cols(scan.filters.clone());
            let Some(request) = requests.iter().find(|request| {
                Arc::ptr_eq(&request.scan, source.scan())
                    && request.filters == filters
                    && scan.fetch.is_none()
            }) else {
                return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
            };
            let Some(planned) = &request.planned else {
                return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
            };
            let statistics =
                source
                    .scan()
                    .exact_statistics(planned)
                    .map(|statistics| AggregateStatistics {
                        schema: source.scan().schema(),
                        statistics,
                    });
            aggregate.input =
                Arc::new(input.replace_source(Arc::new(source.prepare(Arc::clone(planned))))?);
            if let Some(statistics) = statistics
                && let Some(rewritten) = rewrite_aggregate(&aggregate, &statistics)?
            {
                return Ok(Transformed::yes(rewritten));
            }
            // Even when metrics are incomplete, the residual scan reuses this request's file plan.
            Ok(Transformed::yes(LogicalPlan::Aggregate(aggregate)))
        })
    }
}

fn supported_aggregate(expression: &Expr) -> bool {
    match expression {
        Expr::Alias(alias) => supported_aggregate(&alias.expr),
        Expr::AggregateFunction(function) => {
            use datafusion::functions_aggregate::count::Count;
            use datafusion::functions_aggregate::min_max::{Max, Min};
            if function.params.filter.is_some()
                || !function.params.order_by.is_empty()
                || function.params.null_treatment.is_some()
            {
                return false;
            }
            let function = function.func.inner();
            function.is::<Count>() || function.is::<Min>() || function.is::<Max>()
        }
        _ => false,
    }
}

struct AggregateStatistics {
    schema: SchemaRef,
    statistics: Statistics,
}

impl ExactAggregateStatistics for AggregateStatistics {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn row_count(&self) -> Option<usize> {
        match self.statistics.num_rows {
            Precision::Exact(rows) => Some(rows),
            _ => None,
        }
    }
    fn column_statistics(&self, logical_path: &[String]) -> Option<ColumnStatistics> {
        let [name] = logical_path else {
            return None;
        };
        self.statistics
            .column_statistics
            .get(self.schema.index_of(name).ok()?)
            .cloned()
    }
}
