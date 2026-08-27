use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, DFSchema, DFSchemaRef, DataFusionError, Result, plan_err};
use datafusion_expr::{
    Expr, LogicalPlan, LogicalPlanBuilder, UserDefinedLogicalNodeCore, cast, col, lit, when,
};
use educe::Educe;
use sail_common_datafusion::catalog::LakehouseExecutionContext;
use sail_common_datafusion::datasource::{
    DeltaCheckConstraintExpr, MergeIntoOptions, OPERATION_COLUMN, OptionLayer, RowLevelCommand,
    RowLevelOperationType, RowLevelTarget, UpdateAssignment, UpdateInfo,
};
use sail_common_datafusion::logical_expr::ExprWithSource;
use sail_common_datafusion::utils::items::ItemTaker;

use crate::check_constraints::apply_delta_check_constraint_filter;

/// A logical effect produced by a row-level operation.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd)]
pub enum RowLevelEffect {
    WriteRows(Arc<LogicalPlan>),
    TouchFiles(Arc<LogicalPlan>),
    DeleteRows(Arc<LogicalPlan>),
}

impl RowLevelEffect {
    fn plan(&self) -> &Arc<LogicalPlan> {
        match self {
            Self::WriteRows(plan) | Self::TouchFiles(plan) | Self::DeleteRows(plan) => plan,
        }
    }

    fn replace_plan(&self, plan: LogicalPlan) -> Self {
        match self {
            Self::WriteRows(_) => Self::WriteRows(Arc::new(plan)),
            Self::TouchFiles(_) => Self::TouchFiles(Arc::new(plan)),
            Self::DeleteRows(_) => Self::DeleteRows(Arc::new(plan)),
        }
    }
}

fn row_level_effects(
    write_plan: Arc<LogicalPlan>,
    touched_files_plan: Option<Arc<LogicalPlan>>,
    row_index_delete_plan: Option<Arc<LogicalPlan>>,
) -> Vec<RowLevelEffect> {
    let mut effects = vec![RowLevelEffect::WriteRows(write_plan)];
    if let Some(plan) = touched_files_plan {
        effects.push(RowLevelEffect::TouchFiles(plan));
    }
    if let Some(plan) = row_index_delete_plan {
        effects.push(RowLevelEffect::DeleteRows(plan));
    }
    effects
}

/// Information retained until the format-specific commit is planned.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum RowLevelCommitInfo {
    Delete { predicate: Option<ExprWithSource> },
    Update { predicate: Option<ExprWithSource> },
    Merge { options: Box<MergeIntoOptions> },
}

impl RowLevelCommitInfo {
    pub fn command(&self) -> RowLevelCommand {
        match self {
            Self::Delete { .. } => RowLevelCommand::Delete,
            Self::Update { .. } => RowLevelCommand::Update,
            Self::Merge { .. } => RowLevelCommand::Merge,
        }
    }

    pub fn predicate(&self) -> Option<&ExprWithSource> {
        match self {
            Self::Delete { predicate } | Self::Update { predicate } => predicate.as_ref(),
            Self::Merge { .. } => None,
        }
    }

    pub fn merge_options(&self) -> Option<&MergeIntoOptions> {
        match self {
            Self::Merge { options } => Some(options),
            Self::Delete { .. } | Self::Update { .. } => None,
        }
    }
}

/// Unified post-expansion node for DELETE, UPDATE, and MERGE.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct RowLevelWriteNode {
    target: RowLevelTarget,
    raw_target: Arc<LogicalPlan>,
    effects: Vec<RowLevelEffect>,
    #[educe(PartialOrd(method(partial_cmp_by_equality), rank = 0))]
    commit: RowLevelCommitInfo,
    /// `Some` means the target scan must still match at commit time. The inner
    /// value is `None` when the table had no current snapshot when it was read.
    expected_snapshot_id: Option<Option<i64>>,
    #[educe(PartialOrd(method(partial_cmp_by_equality), rank = 1))]
    schema: DFSchemaRef,
}

fn partial_cmp_by_equality<T: PartialEq>(left: &T, right: &T) -> Option<Ordering> {
    left.eq(right).then_some(Ordering::Equal)
}

impl RowLevelWriteNode {
    pub fn new_merge(
        raw_target: Arc<LogicalPlan>,
        write_plan: Arc<LogicalPlan>,
        touched_files_plan: Option<Arc<LogicalPlan>>,
        row_index_delete_plan: Option<Arc<LogicalPlan>>,
        options: MergeIntoOptions,
        schema: DFSchemaRef,
    ) -> Self {
        let effects = row_level_effects(write_plan, touched_files_plan, row_index_delete_plan);
        Self {
            target: options.target.clone(),
            raw_target,
            effects,
            commit: RowLevelCommitInfo::Merge {
                options: Box::new(options),
            },
            expected_snapshot_id: None,
            schema,
        }
    }

    pub fn new_delete(
        raw_target: Arc<LogicalPlan>,
        condition: Option<ExprWithSource>,
        target: RowLevelTarget,
    ) -> Self {
        Self {
            target,
            raw_target,
            effects: vec![],
            commit: RowLevelCommitInfo::Delete {
                predicate: condition,
            },
            expected_snapshot_id: None,
            schema: Arc::new(DFSchema::empty()),
        }
    }

    pub fn new_update(
        raw_target: Arc<LogicalPlan>,
        write_plan: Arc<LogicalPlan>,
        touched_files_plan: Option<Arc<LogicalPlan>>,
        row_index_delete_plan: Option<Arc<LogicalPlan>>,
        condition: Option<ExprWithSource>,
        target: RowLevelTarget,
        schema: DFSchemaRef,
    ) -> Self {
        let effects = row_level_effects(write_plan, touched_files_plan, row_index_delete_plan);
        Self {
            target,
            raw_target,
            effects,
            commit: RowLevelCommitInfo::Update {
                predicate: condition,
            },
            expected_snapshot_id: None,
            schema,
        }
    }

    pub fn with_expected_snapshot_id(mut self, expected_snapshot_id: Option<Option<i64>>) -> Self {
        self.expected_snapshot_id = expected_snapshot_id;
        self
    }

    pub fn command(&self) -> RowLevelCommand {
        self.commit.command()
    }

    pub fn target(&self) -> &RowLevelTarget {
        &self.target
    }

    pub fn effects(&self) -> &[RowLevelEffect] {
        &self.effects
    }

    pub fn merge_options(&self) -> Option<&MergeIntoOptions> {
        self.commit.merge_options()
    }

    pub fn raw_target(&self) -> &Arc<LogicalPlan> {
        &self.raw_target
    }

    pub fn condition(&self) -> Option<&ExprWithSource> {
        self.commit.predicate()
    }

    pub fn target_format(&self) -> &str {
        &self.target.format
    }

    pub fn target_location(&self) -> &str {
        &self.target.location
    }

    pub fn target_options(&self) -> &[OptionLayer] {
        &self.target.options
    }

    pub fn target_lakehouse_table(&self) -> Option<&LakehouseExecutionContext> {
        self.target.lakehouse_table.as_ref()
    }

    pub fn expected_snapshot_id(&self) -> Option<Option<i64>> {
        self.expected_snapshot_id
    }

    pub fn with_schema_evolution(&self) -> bool {
        self.merge_options()
            .is_some_and(|options| options.with_schema_evolution)
    }
}

impl UserDefinedLogicalNodeCore for RowLevelWriteNode {
    fn name(&self) -> &str {
        "RowLevelWrite"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.effects
            .iter()
            .map(|effect| effect.plan().as_ref())
            .collect()
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        let table = self
            .target
            .table_name
            .last()
            .map(String::as_str)
            .unwrap_or(&self.target.location);
        write!(
            f,
            "RowLevelWrite: command={:?}, target={}, format={}",
            self.command(),
            table,
            self.target.format
        )?;
        match &self.commit {
            RowLevelCommitInfo::Delete { predicate } | RowLevelCommitInfo::Update { predicate } => {
                if let Some(source) = predicate.as_ref().and_then(|value| value.source.as_deref()) {
                    write!(f, ", condition={}", source.trim())?;
                }
            }
            RowLevelCommitInfo::Merge { options } => {
                write!(
                    f,
                    ", matched={}, not_matched={}, not_matched_by_source={}",
                    options.matched_clauses.len(),
                    options.not_matched_by_target_clauses.len(),
                    options.not_matched_by_source_clauses.len()
                )?;
            }
        }
        Ok(())
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        exprs.zero()?;
        if inputs.len() != self.effects.len() {
            return Err(DataFusionError::Internal(format!(
                "RowLevelWriteNode expected {} inputs, got {}",
                self.effects.len(),
                inputs.len()
            )));
        }
        let effects = self
            .effects
            .iter()
            .zip(inputs)
            .map(|(effect, plan)| effect.replace_plan(plan))
            .collect();
        Ok(Self {
            target: self.target.clone(),
            raw_target: self.raw_target.clone(),
            effects,
            commit: self.commit.clone(),
            expected_snapshot_id: self.expected_snapshot_id,
            schema: self.schema.clone(),
        })
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        None
    }
}

struct NormalizedRowLevelTarget {
    plan: LogicalPlan,
    field_names: Vec<String>,
    rename_map: HashMap<String, String>,
}

fn normalize_row_level_target(
    plan: LogicalPlan,
    input_schema: &DFSchemaRef,
    resolved_field_names: &[String],
    path_column: &str,
    row_index_column: Option<&str>,
) -> Result<NormalizedRowLevelTarget> {
    let rename_map =
        row_level_target_rename_map(input_schema, plan.schema(), resolved_field_names)?;
    let mut projections = Vec::with_capacity(
        resolved_field_names.len() + 1 + usize::from(row_index_column.is_some()),
    );
    for (plan_field, resolved_name) in plan.schema().fields().iter().zip(resolved_field_names) {
        projections.push(
            Expr::Column(Column::from_name(plan_field.name().clone())).alias(resolved_name.clone()),
        );
    }

    append_metadata_projection(&plan, &mut projections, path_column)?;
    if let Some(row_index_column) = row_index_column {
        append_metadata_projection(&plan, &mut projections, row_index_column)?;
    }

    let plan = LogicalPlanBuilder::from(plan)
        .project(projections)?
        .build()?;
    Ok(NormalizedRowLevelTarget {
        plan,
        field_names: resolved_field_names.to_vec(),
        rename_map,
    })
}

fn row_level_target_rename_map(
    input_schema: &DFSchemaRef,
    plan_schema: &DFSchemaRef,
    resolved_field_names: &[String],
) -> Result<HashMap<String, String>> {
    if input_schema.fields().len() != resolved_field_names.len() {
        return plan_err!(
            "row-level target schema has {} fields but {} resolved names",
            input_schema.fields().len(),
            resolved_field_names.len()
        );
    }
    if plan_schema.fields().len() < resolved_field_names.len() {
        return plan_err!(
            "row-level target plan has {} fields but {} data columns are required",
            plan_schema.fields().len(),
            resolved_field_names.len()
        );
    }

    let mut rename_map = HashMap::new();
    for ((input_field, plan_field), resolved_name) in input_schema
        .fields()
        .iter()
        .zip(plan_schema.fields())
        .zip(resolved_field_names)
    {
        rename_map.insert(input_field.name().clone(), resolved_name.clone());
        rename_map.insert(plan_field.name().clone(), resolved_name.clone());
        rename_map.insert(resolved_name.clone(), resolved_name.clone());
    }
    Ok(rename_map)
}

fn append_metadata_projection(
    plan: &LogicalPlan,
    projections: &mut Vec<Expr>,
    column: &str,
) -> Result<()> {
    if plan
        .schema()
        .index_of_column_by_name(None, column)
        .is_none()
    {
        return plan_err!("row-level target plan is missing required metadata column {column}");
    }
    projections.push(col(column).alias(column));
    Ok(())
}

fn rewrite_target_expr(expr: Expr, rename_map: &HashMap<String, String>) -> Result<Expr> {
    expr.transform(|expr| {
        if let Expr::Column(column) = &expr
            && let Some(name) = rename_map.get(&column.name)
        {
            return Ok(Transformed::yes(Expr::Column(Column {
                relation: None,
                name: name.clone(),
                spans: column.spans.clone(),
            })));
        }
        Ok(Transformed::no(expr))
    })
    .map(|transformed| transformed.data)
}

pub fn rewrite_row_level_target_condition(
    condition: Option<ExprWithSource>,
    input_schema: &DFSchemaRef,
    plan_schema: &DFSchemaRef,
    resolved_field_names: &[String],
) -> Result<Option<ExprWithSource>> {
    let rename_map = row_level_target_rename_map(input_schema, plan_schema, resolved_field_names)?;
    condition
        .map(|condition| {
            Ok(ExprWithSource::new(
                rewrite_target_expr(condition.expr, &rename_map)?,
                condition.source,
            ))
        })
        .transpose()
}

pub fn expand_update(
    info: UpdateInfo,
    path_column: &str,
    row_index_column: Option<&str>,
) -> Result<RowLevelWriteNode> {
    let UpdateInfo {
        target_plan,
        target,
        condition,
        assignments,
        input_schema,
        resolved_target_field_names,
        case_sensitive,
        generated_column_exprs,
        check_constraint_exprs,
    } = info;
    validate_row_level_internal_columns(
        &input_schema,
        &resolved_target_field_names,
        path_column,
        row_index_column,
        case_sensitive,
    )?;
    let normalized = normalize_row_level_target(
        target_plan.as_ref().clone(),
        &input_schema,
        &resolved_target_field_names,
        path_column,
        row_index_column,
    )?;
    let condition = condition
        .map(|condition| -> Result<_> {
            Ok(ExprWithSource::new(
                rewrite_target_expr(condition.expr, &normalized.rename_map)?,
                condition.source,
            ))
        })
        .transpose()?;
    let predicate = condition
        .as_ref()
        .map(|condition| condition.expr.clone())
        .unwrap_or_else(|| lit(true));

    let assignments = rewrite_assignments(
        assignments,
        &normalized.rename_map,
        &normalized.field_names,
        case_sensitive,
    )?;
    let assigned_columns = assignments
        .iter()
        .map(|assignment| row_level_name_key(&assignment.column, case_sensitive))
        .collect::<std::collections::HashSet<_>>();
    let assignment_map = assignments
        .into_iter()
        .map(|assignment| {
            (
                row_level_name_key(&assignment.column, case_sensitive),
                assignment.value,
            )
        })
        .collect::<HashMap<_, _>>();
    let mut write_projection = Vec::with_capacity(normalized.field_names.len() + 2);
    for (index, name) in normalized.field_names.iter().enumerate() {
        let current = col(name);
        let value = assignment_map
            .get(&row_level_name_key(name, case_sensitive))
            .map(|value| {
                let target_type = input_schema
                    .fields()
                    .get(index)
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "UPDATE target field is missing during projection".to_string(),
                        )
                    })?
                    .data_type()
                    .clone();
                when(predicate.clone(), cast(value.clone(), target_type))
                    .otherwise(current.clone())
                    .map(|expr| expr.alias(name))
            })
            .transpose()?
            .unwrap_or_else(|| current.alias(name));
        write_projection.push(value);
    }
    write_projection.push(col(path_column).alias(path_column));
    write_projection.push(
        when(
            predicate.clone(),
            lit(RowLevelOperationType::Update.as_i32()),
        )
        .otherwise(lit(RowLevelOperationType::Copy.as_i32()))?
        .alias(OPERATION_COLUMN),
    );
    let write_rows = LogicalPlanBuilder::from(normalized.plan.clone())
        .project(write_projection)?
        .build()?;

    let generated_column_exprs = generated_column_exprs
        .into_iter()
        .map(|(name, expression)| {
            Ok((
                name,
                rewrite_target_expr(expression, &normalized.rename_map)?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let write_rows = apply_update_generation(
        write_rows,
        &generated_column_exprs,
        &assigned_columns,
        case_sensitive,
    )?;
    let constraints = check_constraint_exprs
        .into_iter()
        .map(|constraint| {
            Ok(DeltaCheckConstraintExpr {
                name: constraint.name,
                expression: constraint.expression,
                expr: rewrite_target_expr(constraint.expr, &normalized.rename_map)?,
                violation: constraint.violation,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let write_rows = apply_delta_check_constraint_filter(
        write_rows,
        &constraints,
        Some(col(OPERATION_COLUMN).eq(lit(RowLevelOperationType::Update.as_i32()))),
    )?;

    let touched_files = LogicalPlanBuilder::from(normalized.plan.clone())
        .filter(predicate.clone())?
        .aggregate(vec![col(path_column)], Vec::<Expr>::new())?
        .project(vec![col(path_column).alias(path_column)])?
        .build()?;
    let delete_rows = row_index_column
        .map(|row_index_column| {
            LogicalPlanBuilder::from(normalized.plan)
                .filter(predicate)?
                .project(vec![
                    col(path_column).alias(path_column),
                    col(row_index_column).alias(row_index_column),
                ])?
                .build()
        })
        .transpose()?
        .map(Arc::new);

    Ok(RowLevelWriteNode::new_update(
        target_plan,
        Arc::new(write_rows),
        Some(Arc::new(touched_files)),
        delete_rows,
        condition,
        target,
        Arc::new(DFSchema::empty()),
    ))
}

fn rewrite_assignments(
    assignments: Vec<UpdateAssignment>,
    rename_map: &HashMap<String, String>,
    field_names: &[String],
    case_sensitive: bool,
) -> Result<Vec<UpdateAssignment>> {
    assignments
        .into_iter()
        .map(|assignment| {
            let UpdateAssignment { column, value } = assignment;
            let column = rename_map.get(&column).cloned().unwrap_or(column);
            let column =
                resolve_assignment_column(&column, field_names, case_sensitive)?.to_string();
            Ok(UpdateAssignment {
                column,
                value: rewrite_target_expr(value, rename_map)?,
            })
        })
        .collect()
}

pub fn validate_row_level_internal_columns(
    input_schema: &DFSchemaRef,
    resolved_field_names: &[String],
    path_column: &str,
    row_index_column: Option<&str>,
    case_sensitive: bool,
) -> Result<()> {
    let reserved = [OPERATION_COLUMN, path_column]
        .into_iter()
        .chain(row_index_column);
    for reserved_name in reserved {
        if resolved_field_names.iter().any(|name| {
            if case_sensitive {
                name == reserved_name
            } else {
                name.eq_ignore_ascii_case(reserved_name)
            }
        }) || input_schema.fields().iter().any(|field| {
            if case_sensitive {
                field.name() == reserved_name
            } else {
                field.name().eq_ignore_ascii_case(reserved_name)
            }
        }) {
            return plan_err!(
                "row-level target column '{reserved_name}' uses a reserved internal column name"
            );
        }
    }
    Ok(())
}

fn apply_update_generation(
    plan: LogicalPlan,
    generated_column_exprs: &[(String, Expr)],
    assigned_columns: &std::collections::HashSet<String>,
    case_sensitive: bool,
) -> Result<LogicalPlan> {
    if generated_column_exprs.is_empty() {
        return Ok(plan);
    }
    let generated = generated_column_exprs
        .iter()
        .map(|(name, expression)| (row_level_name_key(name, case_sensitive), expression))
        .collect::<HashMap<_, _>>();
    let update_row = col(OPERATION_COLUMN).eq(lit(RowLevelOperationType::Update.as_i32()));
    let projection = plan
        .schema()
        .fields()
        .iter()
        .map(|field| {
            let name = field.name();
            if let Some(generation_expr) = generated.get(&row_level_name_key(name, case_sensitive))
            {
                let generated_value = if assigned_columns
                    .contains(&row_level_name_key(name, case_sensitive))
                {
                    let current_value = col(name);
                    let matches_generation = Expr::BinaryExpr(
                        datafusion_expr::expr::BinaryExpr::new(
                            Box::new(current_value.clone()),
                            datafusion_expr::Operator::IsNotDistinctFrom,
                            Box::new((*generation_expr).clone()),
                        ),
                    );
                    let message = format!(
                        "[DELTA_GENERATED_COLUMNS_VALUE_MISMATCH] CHECK constraint for generated column `{name}` violated: user-provided value does not match the generation expression."
                    );
                    let raise = datafusion_expr::ScalarUDF::from(
                        sail_function::scalar::misc::raise_error::RaiseError::new(),
                    )
                    .call(vec![lit(message)]);
                    when(matches_generation, current_value).otherwise(raise)?
                } else {
                    (*generation_expr).clone()
                };
                when(update_row.clone(), generated_value)
                    .otherwise(col(name))
                    .map(|expr| expr.alias(name))
            } else {
                Ok(col(name))
            }
        })
        .collect::<Result<Vec<_>>>()?;
    LogicalPlanBuilder::from(plan).project(projection)?.build()
}

fn resolve_assignment_column<'a>(
    column: &str,
    field_names: &'a [String],
    case_sensitive: bool,
) -> Result<&'a str> {
    let matches = field_names
        .iter()
        .filter(|field| {
            if case_sensitive {
                field.as_str() == column
            } else {
                field.eq_ignore_ascii_case(column)
            }
        })
        .collect::<Vec<_>>();
    if matches.len() != 1 {
        return plan_err!("unable to resolve column {column} in UPDATE target projection");
    }
    Ok(matches[0])
}

fn row_level_name_key(name: &str, case_sensitive: bool) -> String {
    if case_sensitive {
        name.to_string()
    } else {
        name.to_ascii_lowercase()
    }
}

#[cfg(test)]
mod tests {
    use datafusion_expr::{LogicalPlanBuilder, UserDefinedLogicalNode, lit};

    use super::*;

    #[test]
    fn delete_node_preserves_empty_snapshot_requirement() -> Result<()> {
        let plan = Arc::new(LogicalPlanBuilder::empty(false).build()?);
        let target = RowLevelTarget {
            table_name: vec!["catalog".into(), "schema".into(), "table".into()],
            format: "iceberg".into(),
            location: "file:///tmp/table".into(),
            partition_by: vec![],
            options: vec![],
            lakehouse_table: None,
        };
        let node =
            RowLevelWriteNode::new_delete(plan, None, target).with_expected_snapshot_id(Some(None));

        assert_eq!(node.command(), RowLevelCommand::Delete);
        assert!(node.effects().is_empty());
        assert_eq!(node.expected_snapshot_id(), Some(None));

        let rebuilt = UserDefinedLogicalNodeCore::with_exprs_and_inputs(&node, vec![], vec![])?;
        assert_eq!(rebuilt.expected_snapshot_id(), Some(None));
        assert_eq!(rebuilt.target_format(), "iceberg");
        Ok(())
    }

    #[test]
    fn row_level_write_partial_order_matches_equality() -> Result<()> {
        let plan = Arc::new(LogicalPlanBuilder::empty(false).build()?);
        let target = RowLevelTarget {
            table_name: vec!["catalog".into(), "schema".into(), "table".into()],
            format: "iceberg".into(),
            location: "file:///tmp/table".into(),
            partition_by: vec![],
            options: vec![],
            lakehouse_table: None,
        };
        let node = RowLevelWriteNode::new_delete(plan, None, target);
        let mut distinct_commit = node.clone();
        distinct_commit.commit = RowLevelCommitInfo::Delete {
            predicate: Some(ExprWithSource::new(lit(true), Some("true".into()))),
        };

        assert_ne!(node, distinct_commit);
        assert_eq!(node.partial_cmp(&node), Some(Ordering::Equal));
        assert_ne!(node.partial_cmp(&distinct_commit), Some(Ordering::Equal));
        assert_ne!(distinct_commit.partial_cmp(&node), Some(Ordering::Equal));

        let node_trait: &dyn UserDefinedLogicalNode = &node;
        let distinct_trait: &dyn UserDefinedLogicalNode = &distinct_commit;
        assert!(node_trait.dyn_eq(node_trait));
        assert_eq!(node_trait.dyn_ord(node_trait), Some(Ordering::Equal));
        assert!(!node_trait.dyn_eq(distinct_trait));
        assert_ne!(node_trait.dyn_ord(distinct_trait), Some(Ordering::Equal));
        assert_ne!(distinct_trait.dyn_ord(node_trait), Some(Ordering::Equal));
        Ok(())
    }
}
