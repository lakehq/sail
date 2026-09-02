use std::sync::Arc;

use datafusion_expr::{Distinct, DistinctOn, Expr, LogicalPlan};
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_deduplicate(
        &self,
        deduplicate: spec::Deduplicate,
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
            return Err(PlanError::todo("deduplicate within watermark"));
        }
        if !column_names.is_empty() && !all_columns_as_keys {
            // The name selects output columns, so it is matched with the resolver alone, and
            // every column that matches becomes a key.
            // A column that is named more than once is a key only once, since the same
            // expression cannot be repeated in the plan.
            let mut on_expr: Vec<Expr> = Vec::new();
            for name in &column_names {
                for column in self.resolve_columns_by_resolver(schema, name.as_ref(), state)? {
                    let expr = Expr::Column(column);
                    if !on_expr.contains(&expr) {
                        on_expr.push(expr);
                    }
                }
            }
            let select_expr: Vec<Expr> = schema.columns().into_iter().map(Expr::Column).collect();
            Ok(LogicalPlan::Distinct(Distinct::On(DistinctOn::try_new(
                on_expr,
                select_expr,
                None,
                Arc::new(input),
            )?)))
        } else if column_names.is_empty() && all_columns_as_keys {
            Ok(LogicalPlan::Distinct(Distinct::All(Arc::new(input))))
        } else {
            Err(PlanError::invalid(
                "must either specify deduplicate column names or use all columns as keys",
            ))
        }
    }
}
