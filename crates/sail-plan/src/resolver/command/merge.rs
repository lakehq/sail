use std::collections::HashMap;
use std::sync::Arc;

use datafusion_common::{DFField, DFSchema, TableReference};
use datafusion_expr::{Expr, Extension, LogicalPlan, SubqueryAlias};
use sail_common::spec;
use sail_logical_plan::merge::{
    MergeAssignment, MergeIntoNode, MergeIntoOptions, MergeMatchedAction, MergeMatchedClause,
    MergeNotMatchedBySourceAction, MergeNotMatchedBySourceClause, MergeNotMatchedByTargetAction,
    MergeNotMatchedByTargetClause,
};

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_command_merge_into(
        &self,
        merge: spec::MergeInto,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::MergeInto {
            target,
            target_alias,
            source,
            on_condition,
            clauses,
            with_schema_evolution,
        } = merge;

        let target_alias_string = target_alias.as_ref().map(|alias| alias.clone().into());
        let mut target_plan = self.resolve_merge_table_plan(target.clone(), state).await?;

        if let Some(alias) = target_alias_string.as_ref() {
            target_plan = self.apply_table_alias(target_plan, alias)?;
        }

        let (source_plan, source_alias_string) = self.resolve_merge_source(source, state).await?;

        let target_schema = target_plan.schema();
        let source_schema = source_plan.schema();
        let merge_schema =
            self.combine_merge_schema(target_schema.clone(), source_schema.clone())?;

        let on_condition = self
            .resolve_expression(on_condition, &merge_schema, state)
            .await?;

        let (matched_clauses, not_matched_by_source, not_matched_by_target) = self
            .resolve_merge_clauses(clauses, &merge_schema, target_schema.clone(), state)
            .await?;

        let options = MergeIntoOptions {
            target_alias: target_alias_string,
            source_alias: source_alias_string,
            with_schema_evolution,
            on_condition,
            matched_clauses,
            not_matched_by_source_clauses: not_matched_by_source,
            not_matched_by_target_clauses: not_matched_by_target,
        };

        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(MergeIntoNode::new(
                Arc::new(target_plan),
                Arc::new(source_plan),
                options,
            )),
        }))
    }

    async fn resolve_merge_source(
        &self,
        source: spec::MergeSource,
        state: &mut PlanResolverState,
    ) -> PlanResult<(LogicalPlan, Option<String>)> {
        match source {
            spec::MergeSource::Table { name, alias } => {
                let alias_string = alias.as_ref().map(|a| a.clone().into());
                let mut plan = self.resolve_merge_table_plan(name, state).await?;
                if let Some(alias) = alias_string.as_ref() {
                    plan = self.apply_table_alias(plan, alias)?;
                }
                Ok((plan, alias_string))
            }
            spec::MergeSource::Query { input, alias } => {
                let mut plan = self.resolve_query_plan(*input, state).await?;
                let alias_string = alias.as_ref().map(|a| a.clone().into());
                if let Some(alias) = alias_string.as_ref() {
                    plan = self.apply_table_alias(plan, alias)?;
                }
                Ok((plan, alias_string))
            }
        }
    }

    async fn resolve_merge_table_plan(
        &self,
        name: spec::ObjectName,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let read = spec::ReadNamedTable {
            name,
            temporal: None,
            sample: None,
            options: vec![],
        };
        let plan = spec::QueryPlan::new(spec::QueryNode::Read {
            read_type: spec::ReadType::NamedTable(read),
            is_streaming: false,
        });
        self.resolve_query_plan(plan, state).await
    }

    fn apply_table_alias(&self, plan: LogicalPlan, alias: &str) -> PlanResult<LogicalPlan> {
        Ok(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
            Arc::new(plan),
            TableReference::Bare {
                table: Arc::from(alias.to_string()),
            },
        )?))
    }

    fn combine_merge_schema(
        &self,
        target_schema: datafusion_common::DFSchemaRef,
        source_schema: datafusion_common::DFSchemaRef,
    ) -> PlanResult<datafusion_common::DFSchemaRef> {
        let mut fields: Vec<DFField> = target_schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect();
        fields.extend(
            source_schema
                .fields()
                .iter()
                .map(|field| field.as_ref().clone()),
        );
        let schema = DFSchema::new_with_metadata(fields, HashMap::new())?;
        Ok(Arc::new(schema))
    }

    async fn resolve_merge_clauses(
        &self,
        clauses: Vec<spec::MergeClause>,
        merge_schema: &datafusion_common::DFSchemaRef,
        target_schema: datafusion_common::DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<(
        Vec<MergeMatchedClause>,
        Vec<MergeNotMatchedBySourceClause>,
        Vec<MergeNotMatchedByTargetClause>,
    )> {
        let mut matched = vec![];
        let mut not_matched_by_source = vec![];
        let mut not_matched_by_target = vec![];

        for clause in clauses {
            match clause {
                spec::MergeClause::Matched(inner) => {
                    matched.push(
                        self.resolve_merge_matched_clause(
                            inner,
                            merge_schema,
                            target_schema.clone(),
                            state,
                        )
                        .await?,
                    );
                }
                spec::MergeClause::NotMatchedBySource(inner) => {
                    not_matched_by_source.push(
                        self.resolve_merge_not_matched_by_source_clause(
                            inner,
                            merge_schema,
                            target_schema.clone(),
                            state,
                        )
                        .await?,
                    );
                }
                spec::MergeClause::NotMatchedByTarget(inner) => {
                    not_matched_by_target.push(
                        self.resolve_merge_not_matched_by_target_clause(
                            inner,
                            merge_schema,
                            target_schema.clone(),
                            state,
                        )
                        .await?,
                    );
                }
            }
        }

        Ok((matched, not_matched_by_source, not_matched_by_target))
    }

    async fn resolve_merge_matched_clause(
        &self,
        clause: spec::MergeMatchedClause,
        merge_schema: &datafusion_common::DFSchemaRef,
        target_schema: datafusion_common::DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<MergeMatchedClause> {
        let condition = self
            .resolve_optional_expression(clause.condition, merge_schema, state)
            .await?;
        let action = match clause.action {
            spec::MergeMatchedAction::Delete => MergeMatchedAction::Delete,
            spec::MergeMatchedAction::UpdateAll => MergeMatchedAction::UpdateAll,
            spec::MergeMatchedAction::UpdateSet(assignments) => {
                let assignments = self
                    .resolve_merge_assignments(assignments, merge_schema, target_schema, state)
                    .await?;
                MergeMatchedAction::UpdateSet(assignments)
            }
        };
        Ok(MergeMatchedClause { condition, action })
    }

    async fn resolve_merge_not_matched_by_source_clause(
        &self,
        clause: spec::MergeNotMatchedBySourceClause,
        merge_schema: &datafusion_common::DFSchemaRef,
        target_schema: datafusion_common::DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<MergeNotMatchedBySourceClause> {
        let condition = self
            .resolve_optional_expression(clause.condition, merge_schema, state)
            .await?;
        let action = match clause.action {
            spec::MergeNotMatchedBySourceAction::Delete => MergeNotMatchedBySourceAction::Delete,
            spec::MergeNotMatchedBySourceAction::UpdateSet(assignments) => {
                let assignments = self
                    .resolve_merge_assignments(assignments, merge_schema, target_schema, state)
                    .await?;
                MergeNotMatchedBySourceAction::UpdateSet(assignments)
            }
        };
        Ok(MergeNotMatchedBySourceClause { condition, action })
    }

    async fn resolve_merge_not_matched_by_target_clause(
        &self,
        clause: spec::MergeNotMatchedByTargetClause,
        merge_schema: &datafusion_common::DFSchemaRef,
        target_schema: datafusion_common::DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<MergeNotMatchedByTargetClause> {
        let condition = self
            .resolve_optional_expression(clause.condition, merge_schema, state)
            .await?;
        let action = match clause.action {
            spec::MergeNotMatchedByTargetAction::InsertAll => {
                MergeNotMatchedByTargetAction::InsertAll
            }
            spec::MergeNotMatchedByTargetAction::InsertColumns { columns, values } => {
                let columns = self
                    .resolve_merge_columns(columns, target_schema, state)
                    .await?;
                let values = self
                    .resolve_merge_values(values, merge_schema, state)
                    .await?;
                MergeNotMatchedByTargetAction::InsertColumns { columns, values }
            }
        };
        Ok(MergeNotMatchedByTargetClause { condition, action })
    }

    async fn resolve_merge_assignments(
        &self,
        assignments: Vec<(spec::ObjectName, spec::Expr)>,
        merge_schema: &datafusion_common::DFSchemaRef,
        target_schema: datafusion_common::DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<MergeAssignment>> {
        let mut out = Vec::with_capacity(assignments.len());
        for (column, value) in assignments {
            let resolved_column = self
                .resolve_merge_column(column, target_schema.clone(), state)
                .await?;
            let resolved_value = self.resolve_expression(value, merge_schema, state).await?;
            out.push(MergeAssignment {
                column: resolved_column,
                value: resolved_value,
            });
        }
        Ok(out)
    }

    async fn resolve_merge_columns(
        &self,
        columns: Vec<spec::ObjectName>,
        target_schema: datafusion_common::DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<String>> {
        let mut out = Vec::with_capacity(columns.len());
        for column in columns {
            out.push(
                self.resolve_merge_column(column, target_schema.clone(), state)
                    .await?,
            );
        }
        Ok(out)
    }

    async fn resolve_merge_values(
        &self,
        values: Vec<spec::Expr>,
        merge_schema: &datafusion_common::DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<Expr>> {
        let mut out = Vec::with_capacity(values.len());
        for value in values {
            out.push(self.resolve_expression(value, merge_schema, state).await?);
        }
        Ok(out)
    }

    async fn resolve_merge_column(
        &self,
        column: spec::ObjectName,
        target_schema: datafusion_common::DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<String> {
        let expr = spec::Expr::UnresolvedAttribute {
            name: column,
            plan_id: None,
            is_metadata_column: false,
        };
        let resolved = self.resolve_expression(expr, &target_schema, state).await?;
        match resolved {
            Expr::Column(column) => Ok(column.name),
            _ => Err(PlanError::invalid(
                "MERGE assignments must reference columns only",
            )),
        }
    }

    async fn resolve_optional_expression(
        &self,
        expression: Option<spec::Expr>,
        schema: &datafusion_common::DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<Option<Expr>> {
        match expression {
            Some(expr) => Ok(Some(self.resolve_expression(expr, schema, state).await?)),
            None => Ok(None),
        }
    }
}
