use std::sync::Arc;

use datafusion_common::{JoinType, TableReference};
use datafusion_expr::utils::{expr_to_columns, split_conjunction};
use datafusion_expr::{build_join_schema, Expr, Extension, LogicalPlan, SubqueryAlias};
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::catalog::TableKind;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_logical_plan::merge::{
    MergeAssignment, MergeIntoNode, MergeIntoOptions, MergeMatchedAction, MergeMatchedClause,
    MergeNotMatchedBySourceAction, MergeNotMatchedBySourceClause, MergeNotMatchedByTargetAction,
    MergeNotMatchedByTargetClause, MergeTargetInfo,
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

        let target_metadata = self.get_merge_target_info(&target).await?;

        let target_alias_string = target_alias
            .as_ref()
            .map(|alias| alias.as_ref().to_string());
        let mut target_plan = self.resolve_merge_table_plan(target.clone(), state).await?;

        if let Some(alias) = target_alias_string.as_ref() {
            target_plan = self.apply_table_alias(target_plan, alias)?;
        }

        let (source_plan, source_alias_string) = self.resolve_merge_source(source, state).await?;

        let target_schema = target_plan.schema();
        let source_schema = source_plan.schema();
        let merge_schema = Arc::new(build_join_schema(
            target_schema,
            source_schema,
            &JoinType::Inner,
        )?);

        let on_condition = self
            .resolve_expression(on_condition, &merge_schema, state)
            .await?;
        let (join_key_pairs, residual_predicates, target_only_predicates) =
            analyze_merge_join(&on_condition, &merge_schema, target_schema.clone());

        let (matched_clauses, not_matched_by_source, not_matched_by_target) = self
            .resolve_merge_clauses(clauses, &merge_schema, target_schema.clone(), state)
            .await?;

        let options = MergeIntoOptions {
            target_alias: target_alias_string,
            source_alias: source_alias_string,
            target: target_metadata,
            with_schema_evolution,
            resolved_target_schema: target_schema.clone(),
            resolved_source_schema: source_schema.clone(),
            on_condition,
            matched_clauses,
            not_matched_by_source_clauses: not_matched_by_source,
            not_matched_by_target_clauses: not_matched_by_target,
            join_key_pairs,
            residual_predicates,
            target_only_predicates,
        };

        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(MergeIntoNode::new(
                Arc::new(target_plan),
                Arc::new(source_plan),
                options,
                merge_schema,
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
                let alias_string = alias.as_ref().map(|a| a.as_ref().to_string());
                let mut plan = self.resolve_merge_table_plan(name, state).await?;
                if let Some(alias) = alias_string.as_ref() {
                    plan = self.apply_table_alias(plan, alias)?;
                }
                Ok((plan, alias_string))
            }
            spec::MergeSource::Query { input, alias } => {
                let mut plan = self.resolve_query_plan(*input, state).await?;
                let alias_string = alias.as_ref().map(|a| a.as_ref().to_string());
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

    async fn get_merge_target_info(&self, table: &spec::ObjectName) -> PlanResult<MergeTargetInfo> {
        let catalog_manager = self.ctx.extension::<CatalogManager>()?;
        let status = catalog_manager
            .get_table_or_view(table.parts())
            .await
            .map_err(PlanError::from)?;
        match status.kind {
            TableKind::Table {
                location,
                format,
                partition_by,
                options,
                ..
            } => {
                let location = location.ok_or_else(|| {
                    PlanError::invalid(format!("table does not have a location: {table:?}"))
                })?;
                Ok(MergeTargetInfo {
                    table_name: table.clone().into(),
                    format,
                    location,
                    partition_by,
                    options: vec![options],
                })
            }
            _ => Err(PlanError::unsupported(
                "MERGE is only supported against tables",
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExprColumnDomain {
    None,
    TargetOnly,
    SourceOnly,
    Mixed,
}

fn classify_expr_domain(
    expr: &Expr,
    merge_schema: &datafusion_common::DFSchemaRef,
    target_len: usize,
) -> ExprColumnDomain {
    use std::collections::HashSet;

    let mut seen_target = false;
    let mut seen_source = false;
    let mut cols: HashSet<datafusion_common::Column> = HashSet::new();
    if expr_to_columns(expr, &mut cols).is_err() {
        return ExprColumnDomain::Mixed;
    }
    for col in cols.into_iter() {
        if let Ok(idx) = merge_schema.index_of_column(&col) {
            if idx < target_len {
                seen_target = true;
            } else {
                seen_source = true;
            }
        } else {
            return ExprColumnDomain::Mixed;
        }
    }
    match (seen_target, seen_source) {
        (false, false) => ExprColumnDomain::None,
        (true, false) => ExprColumnDomain::TargetOnly,
        (false, true) => ExprColumnDomain::SourceOnly,
        (true, true) => ExprColumnDomain::Mixed,
    }
}

/// Extract join key pairs and residual predicates from the ON condition.
/// target_len is the number of target columns in the merged schema.
fn analyze_merge_join(
    on_condition: &Expr,
    merge_schema: &datafusion_common::DFSchemaRef,
    target_schema: datafusion_common::DFSchemaRef,
) -> (Vec<(Expr, Expr)>, Vec<Expr>, Vec<Expr>) {
    let mut join_key_pairs = Vec::new();
    let mut residual_predicates = Vec::new();
    let mut target_only_predicates = Vec::new();

    let target_len = target_schema.fields().len();

    for predicate in split_conjunction(on_condition) {
        if let Expr::BinaryExpr(be) = predicate {
            if be.op == datafusion_expr::Operator::Eq {
                let left_domain = classify_expr_domain(&be.left, merge_schema, target_len);
                let right_domain = classify_expr_domain(&be.right, merge_schema, target_len);
                match (left_domain, right_domain) {
                    (ExprColumnDomain::TargetOnly, ExprColumnDomain::SourceOnly) => {
                        join_key_pairs.push(((*be.left).clone(), (*be.right).clone()));
                        continue;
                    }
                    (ExprColumnDomain::SourceOnly, ExprColumnDomain::TargetOnly) => {
                        join_key_pairs.push(((*be.right).clone(), (*be.left).clone()));
                        continue;
                    }
                    _ => {}
                }
            }
        }

        if classify_expr_domain(predicate, merge_schema, target_len) == ExprColumnDomain::TargetOnly
        {
            target_only_predicates.push(predicate.clone());
        }
        residual_predicates.push(predicate.clone());
    }

    (join_key_pairs, residual_predicates, target_only_predicates)
}
