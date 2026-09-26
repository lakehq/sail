use std::sync::Arc;

use datafusion_common::TableReference;
use datafusion_expr::{Expr, LogicalPlan, Projection, SubqueryAlias};
use sail_common::spec;
use sail_sql_analyzer::query::AUTO_GENERATED_SUBQUERY_NAME;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_subquery_alias(
        &self,
        input: spec::QueryPlan,
        alias: spec::Identifier,
        qualifier: Vec<spec::Identifier>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self
            .resolve_query_plan_with_hidden_fields(input, state)
            .await?;
        Ok(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
            Arc::new(input),
            self.resolve_table_reference(&spec::ObjectName::from(qualifier).child(alias))?,
        )?))
    }

    pub(super) async fn resolve_query_table_alias(
        &self,
        input: spec::QueryPlan,
        name: spec::Identifier,
        columns: Vec<spec::Identifier>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self.resolve_query_plan(input, state).await?;
        let schema = input.schema();
        let input = if columns.is_empty() {
            input
        } else {
            if columns.len() != schema.fields().len() {
                return Err(PlanError::invalid(format!(
                    "number of column names ({}) does not match number of columns ({})",
                    columns.len(),
                    schema.fields().len()
                )));
            }
            let expr: Vec<Expr> = schema
                .columns()
                .into_iter()
                .zip(columns)
                .map(|(col, name)| Expr::Column(col.clone()).alias(state.register_field_name(name)))
                .collect();
            LogicalPlan::Projection(Projection::try_new(expr, Arc::new(input))?)
        };
        let alias = TableReference::Bare {
            table: Arc::from(String::from(name)),
        };
        if alias.table() == AUTO_GENERATED_SUBQUERY_NAME {
            // Spark removes subquery aliases before decorrelation, and correlated predicates
            // cannot be pulled up through `SubqueryAlias` here. So the generated alias only
            // requalifies the output and, like Spark, stops missing-reference resolution.
            let expr: Vec<Expr> = input
                .schema()
                .columns()
                .into_iter()
                .map(|col| {
                    let name = col.name.clone();
                    Expr::Column(col).alias_qualified(Some(alias.clone()), name)
                })
                .collect();
            let plan = LogicalPlan::Projection(Projection::try_new(expr, Arc::new(input))?);
            state.register_missing_input_boundary(&plan);
            return Ok(plan);
        }
        Ok(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
            Arc::new(input),
            alias,
        )?))
    }
}
