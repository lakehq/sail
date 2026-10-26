//! Resolved MERGE actions bind target columns by identity, including defaults
//! and schema-aware assignment casts.
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use datafusion_expr::LogicalPlan;
use sail_common_datafusion::datasource;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::lakesource::RowLevelOperation;
use sail_common_datafusion::logical_expr::ExprWithSource;

use crate::analyzer::merge::*;
use crate::analyzer::model;
use crate::analyzer::row_level::Condition;
use crate::error::{PlanError, PlanResult};

impl MergeOperation {
    pub(crate) async fn lower(self, session: &SessionContext) -> PlanResult<LogicalPlan> {
        let target_schema = self.target.schema.resolved("MERGE target")?.clone();
        let source_schema = self.source.schema.resolved("MERGE source")?.clone();
        let schema = model::QuerySchema {
            attributes: target_schema
                .attributes
                .iter()
                .chain(&source_schema.attributes)
                .cloned()
                .collect(),
        };
        let target = crate::analyzer::lowering::lower_query(self.target)?.plan;
        let source = crate::analyzer::lowering::lower_query(self.source)?.plan;
        let target = Arc::new(if let Some(alias) = &self.target_alias {
            datafusion_expr::LogicalPlanBuilder::new(target)
                .alias(alias.as_str())?
                .build()?
        } else {
            target
        });
        let source = Arc::new(if let Some(alias) = &self.source_alias {
            datafusion_expr::LogicalPlanBuilder::new(source)
                .alias(alias.as_str())?
                .build()?
        } else {
            source
        });
        let bindings = Bindings {
            schema: &schema,
            target: &target_schema,
        };
        let matched_clauses = self
            .matched
            .into_iter()
            .map(|clause| {
                let action = match clause.action {
                    MergeAction::Delete => datasource::MergeMatchedAction::Delete,
                    MergeAction::Update(values) => {
                        datasource::MergeMatchedAction::UpdateSet(bindings.assignments(values)?)
                    }
                    _ => return Err(PlanError::internal("invalid matched MERGE action")),
                };
                Ok(datasource::MergeMatchedClause {
                    condition: bindings.condition(clause.condition)?,
                    action,
                })
            })
            .collect::<PlanResult<_>>()?;
        let not_matched_by_source_clauses = self
            .source_missing
            .into_iter()
            .map(|clause| {
                let action = match clause.action {
                    MergeAction::Delete => datasource::MergeNotMatchedBySourceAction::Delete,
                    MergeAction::Update(values) => {
                        datasource::MergeNotMatchedBySourceAction::UpdateSet(
                            bindings.assignments(values)?,
                        )
                    }
                    _ => return Err(PlanError::internal("invalid source-missing MERGE action")),
                };
                Ok(datasource::MergeNotMatchedBySourceClause {
                    condition: bindings.condition(clause.condition)?,
                    action,
                })
            })
            .collect::<PlanResult<_>>()?;
        let not_matched_by_target_clauses = self
            .target_missing
            .into_iter()
            .map(|clause| {
                let action = match clause.action {
                    MergeAction::Insert(values) => {
                        let values = bindings.assignments(values)?;
                        datasource::MergeNotMatchedByTargetAction::InsertColumns {
                            columns: values.iter().map(|a| a.column.clone()).collect(),
                            values: values.into_iter().map(|a| a.value).collect(),
                        }
                    }
                    _ => return Err(PlanError::internal("invalid target-missing MERGE action")),
                };
                Ok(datasource::MergeNotMatchedByTargetClause {
                    condition: bindings.condition(clause.condition)?,
                    action,
                })
            })
            .collect::<PlanResult<_>>()?;
        let generated_column_exprs = bindings.named(self.generated)?;
        let default_column_exprs = bindings.named(self.defaults)?;
        let check_constraint_exprs = self
            .constraints
            .into_iter()
            .map(|c| {
                Ok(datasource::DeltaCheckConstraintExpr {
                    name: c.name,
                    expression: c.source,
                    expr: bindings.expression(&c.expression)?,
                    violation: c.violation,
                })
            })
            .collect::<PlanResult<_>>()?;
        let join_key_pairs = self
            .join_keys
            .into_iter()
            .map(|(left, right)| Ok((bindings.expression(&left)?, bindings.expression(&right)?)))
            .collect::<PlanResult<_>>()?;
        let residual_predicates = self
            .residual
            .iter()
            .map(|e| bindings.expression(e))
            .collect::<PlanResult<_>>()?;
        let target_only_predicates = self
            .target_predicates
            .iter()
            .map(|e| bindings.expression(e))
            .collect::<PlanResult<_>>()?;
        let on_condition = bindings
            .condition(Some(self.condition))?
            .ok_or_else(|| PlanError::internal("MERGE condition missing"))?;
        let format = self.metadata.format.clone();
        let options = datasource::MergeIntoOptions {
            target_alias: self.target_alias,
            source_alias: self.source_alias,
            target: self.metadata,
            with_schema_evolution: self.with_schema_evolution,
            case_sensitive: self.case_sensitive,
            resolved_target_schema: Arc::clone(target.schema()),
            resolved_source_schema: Arc::clone(source.schema()),
            resolved_target_field_names: target_schema
                .attributes
                .iter()
                .map(|a| a.name.clone())
                .collect(),
            resolved_source_field_names: source_schema
                .attributes
                .iter()
                .map(|a| a.name.clone())
                .collect(),
            source_column_aliases: vec![],
            on_condition,
            matched_clauses,
            not_matched_by_source_clauses,
            not_matched_by_target_clauses,
            join_key_pairs,
            residual_predicates,
            target_only_predicates,
            generated_column_exprs,
            default_column_exprs,
            check_constraint_exprs,
        };
        let input_schema = Arc::new(datafusion_expr::build_join_schema(
            target.schema(),
            source.schema(),
            &datafusion_expr::JoinType::Inner,
        )?);
        Ok(session
            .extension::<datasource::DataSourceRegistry>()?
            .get_lake_source(&format)?
            .plan_row_level_operation(
                &session.state(),
                RowLevelOperation::Merge(Box::new(datasource::MergeInfo {
                    target,
                    source,
                    options,
                    input_schema,
                })),
            )
            .await?)
    }
}

struct Bindings<'a> {
    schema: &'a model::QuerySchema,
    target: &'a model::QuerySchema,
}
impl Bindings<'_> {
    fn expression(&self, expression: &model::Expr) -> PlanResult<datafusion_expr::Expr> {
        super::checked_expression(expression, self.schema)
    }
    fn condition(&self, value: Option<Condition>) -> PlanResult<Option<ExprWithSource>> {
        value
            .map(|value| {
                Ok(ExprWithSource::new(
                    self.expression(&value.expression)?,
                    value.source,
                ))
            })
            .transpose()
    }
    fn assignments(
        &self,
        values: Vec<(model::AttributeId, model::Expr)>,
    ) -> PlanResult<Vec<datasource::MergeAssignment>> {
        values
            .into_iter()
            .map(|(attribute, value)| {
                self.target.attribute(attribute)?;
                Ok(datasource::MergeAssignment {
                    column: super::backend_name(attribute),
                    value: self.expression(&value)?,
                })
            })
            .collect()
    }
    fn named(
        &self,
        values: Vec<(String, model::Expr)>,
    ) -> PlanResult<Vec<(String, datafusion_expr::Expr)>> {
        values
            .into_iter()
            .map(|(name, expression)| Ok((name, self.expression(&expression)?)))
            .collect()
    }
}
