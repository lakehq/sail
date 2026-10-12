use std::sync::Arc;

use datafusion_common::arrow::datatypes::Schema;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{DFSchema, TableReference};
use datafusion_expr::{Expr, LogicalPlan, Projection, SubqueryAlias};
use sail_common::spec;
use sail_sql_analyzer::query::AUTO_GENERATED_SUBQUERY_NAME;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
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
        let input = if columns.is_empty() {
            input
        } else {
            Self::rename_query_output(input, columns, state)?
        };
        let alias = TableReference::Bare {
            table: Arc::from(String::from(name)),
        };
        // An uncorrelated derived table uses a plain `SubqueryAlias`, like Spark. It
        // requalifies the output and stops missing-reference resolution without adding
        // an operator to the physical plan.
        // DataFusion cannot merge projections across a `SubqueryAlias`, so nested derived
        // tables would keep every layer through optimization. A derived table over a
        // projection is therefore requalified in place, which adds no operator either.
        let mut requalify = false;
        if alias.table() == AUTO_GENERATED_SUBQUERY_NAME {
            requalify = matches!(input, LogicalPlan::Projection(_));
            // Include nested subqueries: their outer references can reach past this table.
            if !requalify {
                input.apply_with_subqueries(|plan| {
                    requalify = plan.contains_outer_reference();
                    Ok(if requalify {
                        TreeNodeRecursion::Stop
                    } else {
                        TreeNodeRecursion::Continue
                    })
                })?;
            }
        }
        if requalify {
            // Spark removes subquery aliases before decorrelation, and correlated predicates
            // cannot be pulled up through `SubqueryAlias` here. So the generated alias only
            // requalifies the output and, like Spark, stops missing-reference resolution.
            // Give the projection fresh field IDs so optimizer rewrites cannot combine
            // its qualified output with unqualified input columns of the same name.
            let names = Self::get_field_names(input.schema(), state)?;
            let expr = input
                .schema()
                .columns()
                .into_iter()
                .zip(names)
                .map(|(col, name)| NamedExpr::new(vec![name], Expr::Column(col)))
                .collect();
            let mut expr = self.rewrite_named_expressions(expr, input.schema(), state)?;
            let mut fields = Vec::with_capacity(expr.len());
            for (expr, field) in expr.iter_mut().zip(input.schema().fields()) {
                if let Expr::Alias(expr) = expr {
                    expr.relation = Some(alias.clone());
                    fields.push(Arc::new(
                        field.as_ref().clone().with_name(expr.name.clone()),
                    ));
                }
            }
            // This projection only renames fields, preserving their types, metadata,
            // and positions. Reuse the schema instead of re-inferring every column;
            // functional dependency indices are unchanged as well. Registered field
            // IDs are unique, so they need no duplicate-name validation.
            let schema = Arc::new(
                DFSchema::try_from(Schema::new_with_metadata(
                    fields,
                    input.schema().metadata().clone(),
                ))?
                .replace_qualifier(alias.clone())
                .with_functional_dependencies(input.schema().functional_dependencies().clone())?,
            );
            // Each input field is used exactly once, so an existing projection can
            // supply the expressions directly without another layer in the plan.
            let input = if let LogicalPlan::Projection(projection) = input {
                for (alias, expr) in expr.iter_mut().zip(projection.expr) {
                    if let Expr::Alias(alias) = alias {
                        match expr {
                            Expr::Alias(inner) => {
                                alias.expr = inner.expr;
                                alias.metadata = inner.metadata;
                            }
                            expr => *alias.expr = expr,
                        }
                    }
                }
                projection.input
            } else {
                Arc::new(input)
            };
            let plan =
                LogicalPlan::Projection(Projection::try_new_with_schema(expr, input, schema)?);
            state.register_missing_input_boundary(&plan);
            return Ok(plan);
        }
        Ok(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
            Arc::new(input),
            alias,
        )?))
    }

    /// Renames every output column as a new attribute, like Spark's
    /// `UnresolvedSubqueryColumnAliases` for `toDF` and table alias column lists.
    pub(super) fn rename_query_output(
        input: LogicalPlan,
        columns: Vec<spec::Identifier>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let schema = input.schema();
        if columns.len() != schema.fields().len() {
            return Err(PlanError::invalid(format!(
                "number of column names ({}) does not match number of columns ({})",
                columns.len(),
                schema.fields().len()
            )));
        }
        let expr = schema
            .columns()
            .into_iter()
            .zip(columns)
            .map(|(col, name)| Expr::Column(col).alias(state.register_field_name(name)))
            .collect();
        Self::projection_reusing_input_fields(expr, input)
    }
}
