use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::functions::core::expr_ext::FieldAccessor;
use datafusion_common::arrow::datatypes::{DataType, Field};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{
    Column, DFSchema, DFSchemaRef, DataFusionError, Result, ScalarValue, plan_err,
};
use datafusion_expr::{
    Expr, LogicalPlan, LogicalPlanBuilder, ScalarUDF, UserDefinedLogicalNodeCore, cast, col, lit,
    when,
};
use educe::Educe;
use sail_common_datafusion::catalog::LakehouseExecutionContext;
use sail_common_datafusion::datasource::{
    DeltaCheckConstraintExpr, MergeIntoOptions, OPERATION_COLUMN, OptionLayer, RowLevelCommand,
    RowLevelOperationType, RowLevelTarget, RowLevelWriteMode, UpdateAssignment, UpdateInfo,
};
use sail_common_datafusion::logical_expr::ExprWithSource;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::struct_function::StructFunction;

use crate::check_constraints::apply_delta_check_constraint_filter;

/// Format-selected auxiliary effects for a row-level expansion.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RowLevelEffectRequirements {
    pub touched_files: bool,
    pub row_index_deletes: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StructFieldAlignment {
    Exact,
    FillMissingWithNull,
}

pub fn align_row_level_value(
    value: Expr,
    source_type: &DataType,
    target_type: &DataType,
    case_sensitive: bool,
    alignment: StructFieldAlignment,
) -> Result<Expr> {
    if source_type == target_type {
        return Ok(value);
    }

    let (DataType::Struct(source_fields), DataType::Struct(target_fields)) =
        (source_type, target_type)
    else {
        return Ok(cast(value, target_type.clone()));
    };

    let names_equal = |left: &str, right: &str| {
        if case_sensitive {
            left == right
        } else {
            left.eq_ignore_ascii_case(right)
        }
    };
    let mut child_values = Vec::with_capacity(target_fields.len());
    for target_field in target_fields {
        let mut matching_fields = source_fields
            .iter()
            .filter(|source_field| names_equal(source_field.name(), target_field.name()));
        let source_field = matching_fields.next();
        if matching_fields.next().is_some() {
            return plan_err!(
                "ambiguous source struct field '{}' during row-level assignment",
                target_field.name()
            );
        }
        let child_value = match source_field {
            Some(source_field) => align_row_level_value(
                value.clone().field(source_field.name().clone()),
                source_field.data_type(),
                target_field.data_type(),
                case_sensitive,
                alignment,
            )?,
            None if alignment == StructFieldAlignment::FillMissingWithNull => {
                lit(ScalarValue::try_new_null(target_field.data_type())?)
            }
            None => {
                return plan_err!(
                    "source struct is missing target field '{}' during row-level assignment",
                    target_field.name()
                );
            }
        };
        child_values.push(child_value);
    }

    let field_names = target_fields
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let rebuilt = ScalarUDF::from(StructFunction::new(field_names)).call(child_values);
    let rebuilt = cast(rebuilt, target_type.clone());
    when(
        value.is_null(),
        lit(ScalarValue::try_new_null(target_type)?),
    )
    .otherwise(rebuilt)
}

pub fn evolve_row_level_field(
    target_field: &Field,
    source_field: &Field,
    case_sensitive: bool,
) -> Field {
    fn evolve_data_type(
        target_type: &DataType,
        source_type: &DataType,
        case_sensitive: bool,
    ) -> DataType {
        let (DataType::Struct(target_fields), DataType::Struct(source_fields)) =
            (target_type, source_type)
        else {
            return target_type.clone();
        };
        let names_equal = |left: &str, right: &str| {
            if case_sensitive {
                left == right
            } else {
                left.eq_ignore_ascii_case(right)
            }
        };
        let mut evolved_fields = target_fields
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        for source_child in source_fields {
            if let Some(index) = evolved_fields
                .iter()
                .position(|target_child| names_equal(target_child.name(), source_child.name()))
            {
                evolved_fields[index] = evolve_row_level_field(
                    &evolved_fields[index],
                    source_child.as_ref(),
                    case_sensitive,
                );
            } else {
                evolved_fields.push(source_child.as_ref().clone().with_nullable(true));
            }
        }
        DataType::Struct(evolved_fields.into())
    }

    target_field.clone().with_data_type(evolve_data_type(
        target_field.data_type(),
        source_field.data_type(),
        case_sensitive,
    ))
}

/// Sparse logical plans required to materialize a row-level write.
///
/// The slots are ordered for DataFusion extension planning as write rows,
/// touched files, then row-index deletes. A missing slot means the selected
/// write mode does not require that effect.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, PartialOrd)]
pub struct RowLevelEffectPlans {
    write_rows: Option<Arc<LogicalPlan>>,
    touched_files: Option<Arc<LogicalPlan>>,
    row_index_deletes: Option<Arc<LogicalPlan>>,
}

impl RowLevelEffectPlans {
    pub fn new(
        write_rows: Option<Arc<LogicalPlan>>,
        touched_files: Option<Arc<LogicalPlan>>,
        row_index_deletes: Option<Arc<LogicalPlan>>,
    ) -> Self {
        Self {
            write_rows,
            touched_files,
            row_index_deletes,
        }
    }

    pub fn write_rows(&self) -> Option<&Arc<LogicalPlan>> {
        self.write_rows.as_ref()
    }

    pub fn touched_files(&self) -> Option<&Arc<LogicalPlan>> {
        self.touched_files.as_ref()
    }

    pub fn row_index_deletes(&self) -> Option<&Arc<LogicalPlan>> {
        self.row_index_deletes.as_ref()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        usize::from(self.write_rows.is_some())
            + usize::from(self.touched_files.is_some())
            + usize::from(self.row_index_deletes.is_some())
    }

    fn plans(&self) -> impl Iterator<Item = &LogicalPlan> {
        [
            self.write_rows.as_deref(),
            self.touched_files.as_deref(),
            self.row_index_deletes.as_deref(),
        ]
        .into_iter()
        .flatten()
    }

    fn replace_plans(&self, plans: Vec<LogicalPlan>) -> Result<Self> {
        if plans.len() != self.len() {
            return Err(DataFusionError::Internal(format!(
                "RowLevelEffectPlans expected {} plans, got {}",
                self.len(),
                plans.len()
            )));
        }
        let mut plans = plans.into_iter();
        let mut replace = |present: bool| -> Result<Option<Arc<LogicalPlan>>> {
            if !present {
                return Ok(None);
            }
            plans.next().map(Arc::new).map(Some).ok_or_else(|| {
                DataFusionError::Internal(
                    "RowLevelEffectPlans replacement plan is missing".to_string(),
                )
            })
        };
        Ok(Self {
            write_rows: replace(self.write_rows.is_some())?,
            touched_files: replace(self.touched_files.is_some())?,
            row_index_deletes: replace(self.row_index_deletes.is_some())?,
        })
    }
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
    mode: RowLevelWriteMode,
    effects: RowLevelEffectPlans,
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
        mode: RowLevelWriteMode,
        effects: RowLevelEffectPlans,
        options: MergeIntoOptions,
        schema: DFSchemaRef,
    ) -> Self {
        Self {
            target: options.target.clone(),
            raw_target,
            mode,
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
        mode: RowLevelWriteMode,
        condition: Option<ExprWithSource>,
        target: RowLevelTarget,
    ) -> Self {
        Self {
            target,
            raw_target,
            mode,
            effects: RowLevelEffectPlans::default(),
            commit: RowLevelCommitInfo::Delete {
                predicate: condition,
            },
            expected_snapshot_id: None,
            schema: Arc::new(DFSchema::empty()),
        }
    }

    pub fn new_update(
        raw_target: Arc<LogicalPlan>,
        mode: RowLevelWriteMode,
        effects: RowLevelEffectPlans,
        condition: Option<ExprWithSource>,
        target: RowLevelTarget,
        schema: DFSchemaRef,
    ) -> Self {
        Self {
            target,
            raw_target,
            mode,
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

    pub fn mode(&self) -> RowLevelWriteMode {
        self.mode
    }

    pub fn effects(&self) -> &RowLevelEffectPlans {
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
        self.effects.plans().collect()
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
            "RowLevelWrite: command={:?}, mode={:?}, target={}, format={}",
            self.command(),
            self.mode,
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
        let effects = self.effects.replace_plans(inputs)?;
        Ok(Self {
            target: self.target.clone(),
            raw_target: self.raw_target.clone(),
            mode: self.mode,
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
    mode: RowLevelWriteMode,
    requirements: RowLevelEffectRequirements,
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

    let touched_files = requirements
        .touched_files
        .then(|| {
            LogicalPlanBuilder::from(normalized.plan.clone())
                .filter(predicate.clone())?
                .aggregate(vec![col(path_column)], Vec::<Expr>::new())?
                .project(vec![col(path_column).alias(path_column)])?
                .build()
        })
        .transpose()?
        .map(Arc::new);
    let row_index_deletes = if let (true, Some(row_index_column)) =
        (requirements.row_index_deletes, row_index_column)
    {
        Some(Arc::new(
            LogicalPlanBuilder::from(normalized.plan)
                .filter(predicate)?
                .project(vec![
                    col(path_column).alias(path_column),
                    col(row_index_column).alias(row_index_column),
                ])?
                .build()?,
        ))
    } else {
        None
    };
    let effects =
        RowLevelEffectPlans::new(Some(Arc::new(write_rows)), touched_files, row_index_deletes);

    Ok(RowLevelWriteNode::new_update(
        target_plan,
        mode,
        effects,
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

    fn named_plan(name: &str) -> Result<Arc<LogicalPlan>> {
        Ok(Arc::new(
            LogicalPlanBuilder::empty(false)
                .project(vec![lit(1_i32).alias(name)])?
                .build()?,
        ))
    }

    #[test]
    fn sparse_effect_plans_preserve_slots_when_replaced() -> Result<()> {
        let write_rows = named_plan("write_rows")?;
        let row_index_deletes = named_plan("row_index_deletes")?;
        let effects = RowLevelEffectPlans::new(Some(write_rows), None, Some(row_index_deletes));

        assert_eq!(effects.len(), 2);
        assert!(effects.write_rows().is_some());
        assert!(effects.touched_files().is_none());
        assert!(effects.row_index_deletes().is_some());

        let replacement_write_rows = named_plan("replacement_write_rows")?;
        let replacement_row_index_deletes = named_plan("replacement_row_index_deletes")?;
        let replaced = effects.replace_plans(vec![
            replacement_write_rows.as_ref().clone(),
            replacement_row_index_deletes.as_ref().clone(),
        ])?;

        assert_eq!(replaced.write_rows(), Some(&replacement_write_rows));
        assert!(replaced.touched_files().is_none());
        assert_eq!(
            replaced.row_index_deletes(),
            Some(&replacement_row_index_deletes)
        );
        Ok(())
    }

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
            RowLevelWriteNode::new_delete(plan, RowLevelWriteMode::MergeOnRead, None, target)
                .with_expected_snapshot_id(Some(None));

        assert_eq!(node.command(), RowLevelCommand::Delete);
        assert_eq!(node.mode(), RowLevelWriteMode::MergeOnRead);
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
        let node =
            RowLevelWriteNode::new_delete(plan, RowLevelWriteMode::CopyOnWrite, None, target);
        let mut distinct_commit = node.clone();
        distinct_commit.commit = RowLevelCommitInfo::Delete {
            predicate: Some(ExprWithSource::new(lit(true), Some("true".into()))),
        };
        let mut distinct_mode = node.clone();
        distinct_mode.mode = RowLevelWriteMode::MergeOnRead;

        assert_ne!(node, distinct_commit);
        assert_ne!(node, distinct_mode);
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
