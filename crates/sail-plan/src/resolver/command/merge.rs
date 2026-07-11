use std::collections::HashSet;
use std::sync::Arc;

use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchema, JoinType, TableReference};
use datafusion_expr::utils::{expr_to_columns, find_aggregate_exprs, split_conjunction};
use datafusion_expr::{Expr, LogicalPlan, SubqueryAlias, build_join_schema};
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::catalog::{LakehouseOperation, TableKind};
use sail_common_datafusion::column_features::ColumnFeatures;
use sail_common_datafusion::datasource::{MergeInfo, OptionLayer, SourceInfo, TableFormatRegistry};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::logical_expr::ExprWithSource;
use sail_logical_plan::merge::{
    MergeAssignment, MergeIntoOptions, MergeMatchedAction, MergeMatchedClause,
    MergeNotMatchedBySourceAction, MergeNotMatchedBySourceClause, MergeNotMatchedByTargetAction,
    MergeNotMatchedByTargetClause, MergeTargetInfo,
};

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

// When we receive an unqualified attribute with `plan_id=None`, we need:
// - if it exists only in target -> treat as target
// - if it exists only in source -> treat as source
// - if it exists in both -> default to target
//
// FIXME: We currently disambiguate `plan_id=None` attributes only for MERGE.
// Similar ambiguity can arise in other multi-input operations (e.g. joins) when using
// unqualified column names from ExpressionString / col("..."). Consider a shared resolver utility.
// These are hardcoded values and need to pay attention if other implementations got conflicting values.

const MERGE_TARGET_DEFAULT_PLAN_ID: i64 = i64::MIN + 4242;
const MERGE_SOURCE_DEFAULT_PLAN_ID: i64 = i64::MIN + 4243;

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
        if target_schema.fields().iter().any(|field| {
            ColumnFeatures::from_map(field.metadata())
                .identity()
                .is_some()
        }) {
            return Err(PlanError::unsupported(
                "MERGE INTO tables with Delta identity columns is not yet supported",
            ));
        }

        // Capture the user-facing field names before further resolution pollutes the state.
        let resolved_target_field_names = Self::get_field_names(target_schema, state)?;
        let resolved_source_field_names = Self::get_field_names(source_schema, state)?;

        // Register synthetic plan ids for both sides. These are only used to disambiguate
        // unqualified attributes when the Connect proto omits `plan_id`.
        for field in target_schema.fields() {
            state.register_plan_id_for_field(field.name(), MERGE_TARGET_DEFAULT_PLAN_ID)?;
        }
        for field in source_schema.fields() {
            state.register_plan_id_for_field(field.name(), MERGE_SOURCE_DEFAULT_PLAN_ID)?;
        }

        let merge_schema = Arc::new(build_join_schema(
            target_schema,
            source_schema,
            &JoinType::Inner,
        )?);

        let on_condition_source = on_condition.source;
        let on_condition_expr = merge_disambiguate_unqualified_plan_ids(
            on_condition.expr,
            state,
            target_schema,
            source_schema,
        );
        let on_condition = self
            .resolve_expression(on_condition_expr, &merge_schema, state)
            .await?;
        validate_merge_condition(&on_condition)?;
        let (join_key_pairs, residual_predicates, target_only_predicates) =
            analyze_merge_join(&on_condition, &merge_schema, target_schema.clone());

        let (matched_clauses, not_matched_by_source, not_matched_by_target) = self
            .resolve_merge_clauses(
                clauses,
                &merge_schema,
                target_schema.clone(),
                source_schema.clone(),
                state,
            )
            .await?;
        self.validate_merge_star_columns(
            &matched_clauses,
            &not_matched_by_target,
            target_schema,
            &resolved_target_field_names,
            &resolved_source_field_names,
            with_schema_evolution,
        )?;

        let generated_column_exprs = self
            .resolve_delta_merge_generated_column_exprs(
                target_schema,
                source_schema,
                &merge_schema,
                state,
            )
            .await?;
        let default_column_exprs = self
            .resolve_merge_default_column_exprs(target_schema, &resolved_target_field_names, state)
            .await?;
        let check_constraint_exprs = self
            .resolve_delta_merge_check_constraints(
                &target_metadata.format,
                &target_metadata.options,
                target_schema,
                state,
            )
            .await?;

        let target_format = target_metadata.format.clone();
        let options = MergeIntoOptions {
            target_alias: target_alias_string,
            source_alias: source_alias_string,
            target: target_metadata,
            with_schema_evolution,
            case_sensitive: self.config.case_sensitive,
            resolved_target_schema: target_schema.clone(),
            resolved_source_schema: source_schema.clone(),
            resolved_target_field_names,
            resolved_source_field_names,
            source_column_aliases: vec![],
            on_condition: ExprWithSource::new(on_condition, on_condition_source),
            matched_clauses,
            not_matched_by_source_clauses: not_matched_by_source,
            not_matched_by_target_clauses: not_matched_by_target,
            join_key_pairs,
            residual_predicates,
            target_only_predicates,
            generated_column_exprs,
            default_column_exprs,
            check_constraint_exprs,
        };

        let registry = self.ctx.extension::<TableFormatRegistry>()?;
        let format = registry.get(&target_format)?;
        let session_state = self.ctx.state();
        Ok(format
            .create_merger(
                &session_state,
                MergeInfo {
                    target: Arc::new(target_plan),
                    source: Arc::new(source_plan),
                    options,
                    input_schema: merge_schema,
                },
            )
            .await?)
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
            read_type: spec::ReadType::NamedTable(Box::new(read)),
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
        source_schema: datafusion_common::DFSchemaRef,
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
                            source_schema.clone(),
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
                            source_schema.clone(),
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
                            source_schema.clone(),
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
        source_schema: datafusion_common::DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<MergeMatchedClause> {
        let condition = self
            .resolve_merge_optional_condition(
                clause.condition,
                merge_schema,
                &target_schema,
                &source_schema,
                state,
            )
            .await?;
        let action = match clause.action {
            spec::MergeMatchedAction::Delete => MergeMatchedAction::Delete,
            spec::MergeMatchedAction::UpdateAll => MergeMatchedAction::UpdateAll,
            spec::MergeMatchedAction::UpdateSet(assignments) => {
                let assignments = self
                    .resolve_merge_assignments(
                        assignments,
                        merge_schema,
                        target_schema,
                        &source_schema,
                        state,
                    )
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
        source_schema: datafusion_common::DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<MergeNotMatchedBySourceClause> {
        let condition = self
            .resolve_merge_optional_condition(
                clause.condition,
                merge_schema,
                &target_schema,
                &source_schema,
                state,
            )
            .await?;
        let action = match clause.action {
            spec::MergeNotMatchedBySourceAction::Delete => MergeNotMatchedBySourceAction::Delete,
            spec::MergeNotMatchedBySourceAction::UpdateSet(assignments) => {
                let assignments = self
                    .resolve_merge_assignments(
                        assignments,
                        merge_schema,
                        target_schema,
                        &source_schema,
                        state,
                    )
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
        source_schema: datafusion_common::DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<MergeNotMatchedByTargetClause> {
        let condition = self
            .resolve_merge_optional_condition(
                clause.condition,
                merge_schema,
                &target_schema,
                &source_schema,
                state,
            )
            .await?;
        let action = match clause.action {
            spec::MergeNotMatchedByTargetAction::InsertAll => {
                MergeNotMatchedByTargetAction::InsertAll
            }
            spec::MergeNotMatchedByTargetAction::InsertColumns { columns, values } => {
                let columns = self
                    .resolve_merge_columns(columns, target_schema.clone(), state)
                    .await?;
                let values = self
                    .resolve_merge_values(
                        values,
                        merge_schema,
                        &target_schema,
                        &source_schema,
                        state,
                    )
                    .await?;
                if columns.len() != values.len() {
                    return Err(PlanError::invalid(
                        "MERGE INSERT column and value counts do not match",
                    ));
                }
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
        source_schema: &datafusion_common::DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<MergeAssignment>> {
        let mut out = Vec::with_capacity(assignments.len());
        let mut assigned_columns = HashSet::with_capacity(assignments.len());
        for (column, value) in assignments {
            let resolved_column = self
                .resolve_merge_column(column, target_schema.clone(), state)
                .await?;
            let assignment_key = self.merge_name_key(&resolved_column);
            if !assigned_columns.insert(assignment_key) {
                return Err(PlanError::invalid(format!(
                    "Multiple assignments for MERGE target column `{resolved_column}`"
                )));
            }
            let value = merge_disambiguate_unqualified_plan_ids(
                value,
                state,
                &target_schema,
                source_schema,
            );
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
        let mut assigned_columns = HashSet::with_capacity(columns.len());
        for column in columns {
            let resolved_column = self
                .resolve_merge_column(column, target_schema.clone(), state)
                .await?;
            let assignment_key = self.merge_name_key(&resolved_column);
            if !assigned_columns.insert(assignment_key) {
                return Err(PlanError::invalid(format!(
                    "Multiple assignments for MERGE target column `{resolved_column}`"
                )));
            }
            out.push(resolved_column);
        }
        Ok(out)
    }

    async fn resolve_merge_values(
        &self,
        values: Vec<spec::Expr>,
        merge_schema: &datafusion_common::DFSchemaRef,
        target_schema: &datafusion_common::DFSchemaRef,
        source_schema: &datafusion_common::DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<Expr>> {
        let mut out = Vec::with_capacity(values.len());
        for value in values {
            let value =
                merge_disambiguate_unqualified_plan_ids(value, state, target_schema, source_schema);
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

    async fn resolve_merge_optional_condition(
        &self,
        expression: Option<spec::ExprWithSource>,
        schema: &datafusion_common::DFSchemaRef,
        target_schema: &datafusion_common::DFSchemaRef,
        source_schema: &datafusion_common::DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<Option<ExprWithSource>> {
        match expression {
            Some(expr) => {
                let condition = self
                    .resolve_expression(
                        merge_disambiguate_unqualified_plan_ids(
                            expr.expr,
                            state,
                            target_schema,
                            source_schema,
                        ),
                        schema,
                        state,
                    )
                    .await?;
                validate_merge_condition(&condition)?;
                Ok(Some(ExprWithSource::new(condition, expr.source)))
            }
            None => Ok(None),
        }
    }

    fn merge_name_key(&self, name: &str) -> String {
        if self.config.case_sensitive {
            name.to_string()
        } else {
            name.to_ascii_lowercase()
        }
    }

    fn merge_names_equal(&self, left: &str, right: &str) -> bool {
        if self.config.case_sensitive {
            left == right
        } else {
            left.eq_ignore_ascii_case(right)
        }
    }

    fn validate_merge_star_columns(
        &self,
        matched_clauses: &[MergeMatchedClause],
        not_matched_by_target_clauses: &[MergeNotMatchedByTargetClause],
        target_schema: &datafusion_common::DFSchemaRef,
        target_names: &[String],
        source_names: &[String],
        with_schema_evolution: bool,
    ) -> PlanResult<()> {
        if with_schema_evolution
            || (!matched_clauses
                .iter()
                .any(|clause| matches!(clause.action, MergeMatchedAction::UpdateAll))
                && !not_matched_by_target_clauses.iter().any(|clause| {
                    matches!(clause.action, MergeNotMatchedByTargetAction::InsertAll)
                }))
        {
            return Ok(());
        }

        for (field, target_name) in target_schema.fields().iter().zip(target_names) {
            if ColumnFeatures::from_field(field)
                .generation_expression()
                .is_some()
            {
                continue;
            }
            if !source_names
                .iter()
                .any(|source_name| self.merge_names_equal(source_name, target_name))
            {
                return Err(PlanError::invalid(format!(
                    "Cannot resolve source column `{target_name}` for MERGE * action without schema evolution"
                )));
            }
        }
        Ok(())
    }

    async fn resolve_merge_default_column_exprs(
        &self,
        target_schema: &datafusion_common::DFSchemaRef,
        target_names: &[String],
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<(String, Expr)>> {
        let empty_schema = Arc::new(DFSchema::empty());
        let mut defaults = Vec::new();
        for (field, target_name) in target_schema.fields().iter().zip(target_names) {
            let Some(default) = ColumnFeatures::from_field(field).current_default() else {
                continue;
            };
            let ast_expr =
                sail_sql_analyzer::parser::parse_expression(&default).map_err(|error| {
                    PlanError::invalid(format!(
                        "failed to parse default expression `{default}`: {error}"
                    ))
                })?;
            let spec_expr =
                sail_sql_analyzer::expression::from_ast_expression(ast_expr).map_err(|error| {
                    PlanError::invalid(format!(
                        "failed to analyze default expression `{default}`: {error}"
                    ))
                })?;
            let spec_expr = if matches!(spec_expr, spec::Expr::UnresolvedAttribute { .. }) {
                spec::Expr::Literal(spec::Literal::Utf8 {
                    value: Some(default),
                })
            } else {
                spec_expr
            };
            let resolved = self
                .resolve_expression(spec_expr, &empty_schema, state)
                .await?;
            defaults.push((target_name.clone(), resolved));
        }
        Ok(defaults)
    }

    async fn get_merge_target_info(&self, table: &spec::ObjectName) -> PlanResult<MergeTargetInfo> {
        // Handle path-based table access like `delta.`/path/to/table``
        // where the first part is a registered table format name.
        if let [format, path] = table.parts() {
            let format = format.as_ref().to_ascii_lowercase();
            let registry = self.ctx.extension::<TableFormatRegistry>()?;
            if let Ok(table_format) = registry.get(&format) {
                let location = path.as_ref().to_string();
                let metadata = table_format
                    .infer_metadata(
                        &self.ctx.state(),
                        SourceInfo {
                            paths: vec![location.clone()],
                            lakehouse_table: None,
                            schema: None,
                            constraints: Default::default(),
                            partition_by: vec![],
                            bucket_by: None,
                            sort_order: vec![],
                            options: vec![],
                            read_case_sensitive: self.config.case_sensitive,
                        },
                    )
                    .await?;
                return Ok(MergeTargetInfo {
                    table_name: table.clone().into(),
                    format,
                    location,
                    partition_by: vec![],
                    options: vec![OptionLayer::TablePropertyList {
                        items: metadata.properties,
                    }],
                    lakehouse_table: None,
                });
            }
        }
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
                properties,
                ..
            } => {
                let location = location.ok_or_else(|| {
                    PlanError::invalid(format!("table does not have a location: {table:?}"))
                })?;
                let table_name: Vec<String> = table.clone().into();
                let lakehouse_table = self
                    .resolve_lakehouse_table_context(
                        &table_name,
                        LakehouseOperation::Write,
                        Some(&format),
                        vec![],
                    )
                    .await?;
                Ok(MergeTargetInfo {
                    table_name,
                    format,
                    location,
                    partition_by: partition_by.into_iter().map(|field| field.column).collect(),
                    options: vec![OptionLayer::TablePropertyList { items: properties }],
                    lakehouse_table: Some(lakehouse_table),
                })
            }
            _ => Err(PlanError::unsupported(
                "MERGE is only supported against tables",
            )),
        }
    }
}

fn validate_merge_condition(condition: &Expr) -> PlanResult<()> {
    if condition.is_volatile() {
        return Err(PlanError::AnalysisError(
            "Non-deterministic expressions are not allowed in MERGE conditions".to_string(),
        ));
    }

    let mut contains_subquery = false;
    condition.apply(|expr| {
        if matches!(
            expr,
            Expr::Exists(_)
                | Expr::InSubquery(_)
                | Expr::SetComparison(_)
                | Expr::ScalarSubquery(_)
        ) {
            contains_subquery = true;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    })?;
    if contains_subquery {
        return Err(PlanError::AnalysisError(
            "Subqueries are not allowed in MERGE conditions".to_string(),
        ));
    }

    if !find_aggregate_exprs([condition]).is_empty() {
        return Err(PlanError::AnalysisError(
            "Aggregate expressions are not allowed in MERGE conditions".to_string(),
        ));
    }

    Ok(())
}

fn merge_schema_has_column_name(
    schema: &datafusion_common::DFSchemaRef,
    state: &PlanResolverState,
    name: &str,
) -> bool {
    schema.iter().any(|(_qualifier, field)| {
        state
            .get_field_info(field.name())
            .is_ok_and(|info| !info.is_hidden() && info.name().eq_ignore_ascii_case(name))
    })
}

/// Disambiguate column references in a generation expression for MERGE INSERT/UPDATE context.
pub(super) fn merge_disambiguate_unqualified_plan_ids(
    expr: spec::Expr,
    state: &PlanResolverState,
    target_schema: &datafusion_common::DFSchemaRef,
    source_schema: &datafusion_common::DFSchemaRef,
) -> spec::Expr {
    use spec::Expr;

    match expr {
        Expr::UnresolvedAttribute {
            name,
            plan_id: None,
            is_metadata_column,
        } => {
            // Only disambiguate a bare identifier like `id`. Qualified identifiers
            // like `t.id` should be resolved using the qualifier.
            if let [part] = name.parts() {
                let col = part.as_ref();
                let in_target = merge_schema_has_column_name(target_schema, state, col);
                let in_source = merge_schema_has_column_name(source_schema, state, col);
                let plan_id = match (in_target, in_source) {
                    (true, false) => Some(MERGE_TARGET_DEFAULT_PLAN_ID),
                    (false, true) => Some(MERGE_SOURCE_DEFAULT_PLAN_ID),
                    (true, true) => Some(MERGE_TARGET_DEFAULT_PLAN_ID),
                    (false, false) => None,
                };
                Expr::UnresolvedAttribute {
                    name,
                    plan_id,
                    is_metadata_column,
                }
            } else {
                Expr::UnresolvedAttribute {
                    name,
                    plan_id: None,
                    is_metadata_column,
                }
            }
        }
        Expr::UnresolvedAttribute { .. } => expr,
        Expr::Literal(_) => expr,
        Expr::UnresolvedTime { .. } => expr,
        Expr::UnresolvedFunction(mut f) => {
            f.arguments = f
                .arguments
                .into_iter()
                .map(|e| {
                    merge_disambiguate_unqualified_plan_ids(e, state, target_schema, source_schema)
                })
                .collect();
            Expr::UnresolvedFunction(f)
        }
        Expr::UnresolvedStar {
            target,
            plan_id: None,
            wildcard_options,
        } => Expr::UnresolvedStar {
            target,
            plan_id: Some(MERGE_TARGET_DEFAULT_PLAN_ID),
            wildcard_options,
        },
        Expr::UnresolvedStar { .. } => expr,
        Expr::Alias {
            expr,
            name,
            metadata,
        } => Expr::Alias {
            expr: Box::new(merge_disambiguate_unqualified_plan_ids(
                *expr,
                state,
                target_schema,
                source_schema,
            )),
            name,
            metadata,
        },
        Expr::Cast {
            expr,
            cast_to_type,
            rename,
            is_try,
        } => Expr::Cast {
            expr: Box::new(merge_disambiguate_unqualified_plan_ids(
                *expr,
                state,
                target_schema,
                source_schema,
            )),
            cast_to_type,
            rename,
            is_try,
        },
        Expr::UnresolvedRegex {
            col_name,
            plan_id: None,
        } => Expr::UnresolvedRegex {
            col_name,
            plan_id: Some(MERGE_TARGET_DEFAULT_PLAN_ID),
        },
        Expr::UnresolvedRegex { .. } => expr,
        Expr::SortOrder(sort) => Expr::SortOrder(spec::SortOrder {
            child: Box::new(merge_disambiguate_unqualified_plan_ids(
                *sort.child,
                state,
                target_schema,
                source_schema,
            )),
            ..sort
        }),
        Expr::LambdaFunction {
            function,
            arguments,
        } => Expr::LambdaFunction {
            function: Box::new(merge_disambiguate_unqualified_plan_ids(
                *function,
                state,
                target_schema,
                source_schema,
            )),
            arguments,
        },
        Expr::Window {
            window_function,
            window,
        } => Expr::Window {
            window_function: Box::new(merge_disambiguate_unqualified_plan_ids(
                *window_function,
                state,
                target_schema,
                source_schema,
            )),
            window,
        },
        Expr::UnresolvedExtractValue { child, extraction } => Expr::UnresolvedExtractValue {
            child: Box::new(merge_disambiguate_unqualified_plan_ids(
                *child,
                state,
                target_schema,
                source_schema,
            )),
            extraction: Box::new(merge_disambiguate_unqualified_plan_ids(
                *extraction,
                state,
                target_schema,
                source_schema,
            )),
        },
        Expr::UpdateFields {
            struct_expression,
            field_name,
            value_expression,
        } => Expr::UpdateFields {
            struct_expression: Box::new(merge_disambiguate_unqualified_plan_ids(
                *struct_expression,
                state,
                target_schema,
                source_schema,
            )),
            field_name,
            value_expression: value_expression.map(|v| {
                Box::new(merge_disambiguate_unqualified_plan_ids(
                    *v,
                    state,
                    target_schema,
                    source_schema,
                ))
            }),
        },
        Expr::UnresolvedNamedLambdaVariable(_) => expr,
        Expr::CommonInlineUserDefinedFunction(_) => expr,
        Expr::CallFunction {
            function_name,
            arguments,
        } => Expr::CallFunction {
            function_name,
            arguments: arguments
                .into_iter()
                .map(|e| {
                    merge_disambiguate_unqualified_plan_ids(e, state, target_schema, source_schema)
                })
                .collect(),
        },
        Expr::DefaultColumnValue | Expr::Placeholder(_) => expr,
        Expr::Rollup(exprs) => Expr::Rollup(
            exprs
                .into_iter()
                .map(|e| {
                    merge_disambiguate_unqualified_plan_ids(e, state, target_schema, source_schema)
                })
                .collect(),
        ),
        Expr::Cube(exprs) => Expr::Cube(
            exprs
                .into_iter()
                .map(|e| {
                    merge_disambiguate_unqualified_plan_ids(e, state, target_schema, source_schema)
                })
                .collect(),
        ),
        Expr::GroupingSets(sets) => Expr::GroupingSets(
            sets.into_iter()
                .map(|set| {
                    set.into_iter()
                        .map(|e| {
                            merge_disambiguate_unqualified_plan_ids(
                                e,
                                state,
                                target_schema,
                                source_schema,
                            )
                        })
                        .collect()
                })
                .collect(),
        ),
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => Expr::InSubquery {
            expr: Box::new(merge_disambiguate_unqualified_plan_ids(
                *expr,
                state,
                target_schema,
                source_schema,
            )),
            subquery,
            negated,
        },
        Expr::ScalarSubquery { .. } => expr,
        Expr::Exists { .. } => expr,
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(merge_disambiguate_unqualified_plan_ids(
                *expr,
                state,
                target_schema,
                source_schema,
            )),
            list: list
                .into_iter()
                .map(|e| {
                    merge_disambiguate_unqualified_plan_ids(e, state, target_schema, source_schema)
                })
                .collect(),
            negated,
        },
        Expr::IsFalse(e) => Expr::IsFalse(Box::new(merge_disambiguate_unqualified_plan_ids(
            *e,
            state,
            target_schema,
            source_schema,
        ))),
        Expr::IsNotFalse(e) => Expr::IsNotFalse(Box::new(merge_disambiguate_unqualified_plan_ids(
            *e,
            state,
            target_schema,
            source_schema,
        ))),
        Expr::IsTrue(e) => Expr::IsTrue(Box::new(merge_disambiguate_unqualified_plan_ids(
            *e,
            state,
            target_schema,
            source_schema,
        ))),
        Expr::IsNotTrue(e) => Expr::IsNotTrue(Box::new(merge_disambiguate_unqualified_plan_ids(
            *e,
            state,
            target_schema,
            source_schema,
        ))),
        Expr::IsNull(e) => Expr::IsNull(Box::new(merge_disambiguate_unqualified_plan_ids(
            *e,
            state,
            target_schema,
            source_schema,
        ))),
        Expr::IsNotNull(e) => Expr::IsNotNull(Box::new(merge_disambiguate_unqualified_plan_ids(
            *e,
            state,
            target_schema,
            source_schema,
        ))),
        Expr::IsUnknown(e) => Expr::IsUnknown(Box::new(merge_disambiguate_unqualified_plan_ids(
            *e,
            state,
            target_schema,
            source_schema,
        ))),
        Expr::IsNotUnknown(e) => Expr::IsNotUnknown(Box::new(
            merge_disambiguate_unqualified_plan_ids(*e, state, target_schema, source_schema),
        )),
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => Expr::Between {
            expr: Box::new(merge_disambiguate_unqualified_plan_ids(
                *expr,
                state,
                target_schema,
                source_schema,
            )),
            negated,
            low: Box::new(merge_disambiguate_unqualified_plan_ids(
                *low,
                state,
                target_schema,
                source_schema,
            )),
            high: Box::new(merge_disambiguate_unqualified_plan_ids(
                *high,
                state,
                target_schema,
                source_schema,
            )),
        },
        Expr::IsDistinctFrom { left, right } => Expr::IsDistinctFrom {
            left: Box::new(merge_disambiguate_unqualified_plan_ids(
                *left,
                state,
                target_schema,
                source_schema,
            )),
            right: Box::new(merge_disambiguate_unqualified_plan_ids(
                *right,
                state,
                target_schema,
                source_schema,
            )),
        },
        Expr::IsNotDistinctFrom { left, right } => Expr::IsNotDistinctFrom {
            left: Box::new(merge_disambiguate_unqualified_plan_ids(
                *left,
                state,
                target_schema,
                source_schema,
            )),
            right: Box::new(merge_disambiguate_unqualified_plan_ids(
                *right,
                state,
                target_schema,
                source_schema,
            )),
        },
        Expr::SimilarTo {
            expr,
            pattern,
            negated,
            escape_char,
            case_insensitive,
        } => Expr::SimilarTo {
            expr: Box::new(merge_disambiguate_unqualified_plan_ids(
                *expr,
                state,
                target_schema,
                source_schema,
            )),
            pattern: Box::new(merge_disambiguate_unqualified_plan_ids(
                *pattern,
                state,
                target_schema,
                source_schema,
            )),
            negated,
            escape_char,
            case_insensitive,
        },
        Expr::Table { expr } => Expr::Table {
            expr: Box::new(merge_disambiguate_unqualified_plan_ids(
                *expr,
                state,
                target_schema,
                source_schema,
            )),
        },
        Expr::UnresolvedDate { .. } => expr,
        Expr::UnresolvedTimestamp { .. } => expr,
        Expr::IdentifierClause { expr: inner } => Expr::IdentifierClause {
            expr: Box::new(merge_disambiguate_unqualified_plan_ids(
                *inner,
                state,
                target_schema,
                source_schema,
            )),
        },
        Expr::Subquery {
            plan_id,
            subquery_type,
            in_subquery_values,
            negated,
        } => Expr::Subquery {
            plan_id,
            subquery_type,
            in_subquery_values: in_subquery_values
                .into_iter()
                .map(|value_expr| {
                    merge_disambiguate_unqualified_plan_ids(
                        value_expr,
                        state,
                        target_schema,
                        source_schema,
                    )
                })
                .collect(),
            negated,
        },
        // NamedArgument is not expected in MERGE statements; pass through unchanged
        Expr::NamedArgument { key, value } => Expr::NamedArgument {
            key,
            value: Box::new(merge_disambiguate_unqualified_plan_ids(
                *value,
                state,
                target_schema,
                source_schema,
            )),
        },
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
        match merge_schema.index_of_column(&col) {
            Ok(idx) => {
                if idx < target_len {
                    seen_target = true;
                } else {
                    seen_source = true;
                }
            }
            _ => {
                return ExprColumnDomain::Mixed;
            }
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
        if let Expr::BinaryExpr(be) = predicate
            && be.op == datafusion_expr::Operator::Eq
        {
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

        if classify_expr_domain(predicate, merge_schema, target_len) == ExprColumnDomain::TargetOnly
        {
            target_only_predicates.push(predicate.clone());
        }
        residual_predicates.push(predicate.clone());
    }

    (join_key_pairs, residual_predicates, target_only_predicates)
}
