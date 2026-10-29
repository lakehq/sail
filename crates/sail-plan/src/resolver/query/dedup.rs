use std::sync::Arc;

use datafusion_expr::{Distinct, DistinctOn, Expr, LogicalPlan};
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;
use crate::streaming::rewriter::is_streaming_plan;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_deduplicate(
        &self,
        deduplicate: spec::Deduplicate,
        dataframe: bool,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::Deduplicate {
            input,
            column_names,
            all_columns_as_keys,
            within_watermark,
        } = deduplicate;
        let input = self
            .resolve_query_plan_with_hidden_fields(*input, state)
            .await?;
        let schema = input.schema();
        if within_watermark {
            // Spark only defines watermark-based deduplication for streaming input.
            if !is_streaming_plan(&input)? {
                return Err(PlanError::analysis(
                    "dropDuplicatesWithinWatermark is not supported with batch DataFrames/DataSets",
                ));
            }
            return Err(PlanError::todo("deduplicate within watermark"));
        }
        if !column_names.is_empty() && !all_columns_as_keys {
            let on_expr: Vec<Expr> = self
                .resolve_columns(schema, &column_names, state)?
                .into_iter()
                .map(Expr::Column)
                .collect();
            let select_expr: Vec<Expr> = schema.columns().into_iter().map(Expr::Column).collect();
            Ok(LogicalPlan::Distinct(Distinct::On(DistinctOn::try_new(
                on_expr,
                select_expr,
                None,
                Arc::new(input),
            )?)))
        } else if column_names.is_empty() && all_columns_as_keys {
            if dataframe && !schema.fields().is_empty() {
                let columns = schema
                    .columns()
                    .into_iter()
                    .map(Expr::Column)
                    .collect::<Vec<_>>();
                Ok(LogicalPlan::Distinct(Distinct::On(DistinctOn::try_new(
                    columns.clone(),
                    columns,
                    None,
                    Arc::new(input),
                )?)))
            } else {
                Ok(LogicalPlan::Distinct(Distinct::All(Arc::new(input))))
            }
        } else {
            Err(PlanError::invalid(
                "must either specify deduplicate column names or use all columns as keys",
            ))
        }
    }
}
