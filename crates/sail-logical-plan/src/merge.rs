use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Formatter;
use std::hash::Hash;
use std::sync::Arc;

use datafusion::functions::core::expr_ext::FieldAccessor;
use datafusion::functions_aggregate::count::count_udaf;
use sail_common_datafusion::replay::ReplayId;

use crate::merge_evaluate::evaluate_once;
use crate::replay::ReplayNode;

mod intent;
#[cfg(test)]
mod tests;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::utils::expr::COUNT_STAR_EXPANSION;
use datafusion_common::{
    Column, DFSchema, DFSchemaRef, DataFusionError, Dependency, NullEquality, Result, ScalarValue,
    TableReference, plan_err,
};
use datafusion_expr::expr::{AggregateFunction, AggregateFunctionParams, Case, FieldMetadata};
use datafusion_expr::logical_plan::{
    Aggregate, Extension, Filter, LogicalPlanBuilder, Projection, SubqueryAlias,
};
use datafusion_expr::utils::expr_to_columns;
use datafusion_expr::{
    BinaryExpr, Expr, ExprSchemable, Join, JoinConstraint, JoinType, LogicalPlan, Operator,
    ScalarUDF, UserDefinedLogicalNodeCore, cast, col, lit, when,
};
use educe::Educe;
use intent::{MergeClauseSelection, capture_merge_values};
use log::trace;
use sail_common_datafusion::column_features::ColumnFeaturesBuilder;
use sail_common_datafusion::logical_expr::ExprWithSource;
use sail_common_datafusion::utils::datetime::arrow_timezone_label;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::misc::raise_error::RaiseError;

use crate::check_constraints::apply_delta_check_constraint_filter;
use crate::monotonic_id::MonotonicIdNode;
use crate::row_level::{
    RowLevelEffectRequirements, StructFieldAlignment, align_row_level_value, evolve_row_level_field,
};

pub const SOURCE_PRESENT_COLUMN: &str = "__sail_merge_source_row_present";
pub const TARGET_PRESENT_COLUMN: &str = "__sail_merge_target_row_present";
pub const TARGET_ROW_ID_COLUMN: &str = "__sail_merge_target_row_id";

type GeneratedAssignmentMarker = (String, String);
type MergeProjection = (Vec<Expr>, Vec<GeneratedAssignmentMarker>);

use sail_common_datafusion::datasource::RowLevelOperationType;
pub use sail_common_datafusion::datasource::{
    DeltaCheckConstraintExpr, MERGE_SOURCE_METRIC_COLUMN, MergeAssignment, MergeInfo,
    MergeIntoOptions, MergeMatchedAction, MergeMatchedClause, MergeNotMatchedBySourceAction,
    MergeNotMatchedBySourceClause, MergeNotMatchedByTargetAction, MergeNotMatchedByTargetClause,
    OPERATION_COLUMN, RowLevelTarget,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct MergeCardinalityCheckNode {
    input: Arc<LogicalPlan>,
    target_row_id_col: String,
    target_present_col: String,
    source_present_col: String,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
}

impl MergeCardinalityCheckNode {
    pub fn new(
        input: Arc<LogicalPlan>,
        target_row_id_col: impl Into<String>,
        target_present_col: impl Into<String>,
        source_present_col: impl Into<String>,
    ) -> Self {
        let schema = input.schema().clone();
        Self {
            input,
            target_row_id_col: target_row_id_col.into(),
            target_present_col: target_present_col.into(),
            source_present_col: source_present_col.into(),
            schema,
        }
    }

    pub fn target_row_id_col(&self) -> &str {
        &self.target_row_id_col
    }

    pub fn target_present_col(&self) -> &str {
        &self.target_present_col
    }

    pub fn source_present_col(&self) -> &str {
        &self.source_present_col
    }
}

impl UserDefinedLogicalNodeCore for MergeCardinalityCheckNode {
    fn name(&self) -> &str {
        "MergeCardinalityCheck"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "MergeCardinalityCheck: target_row_id_col={}, target_present_col={}, source_present_col={}",
            self.target_row_id_col, self.target_present_col, self.source_present_col
        )
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Self> {
        exprs.zero()?;
        let [input] = inputs.as_slice() else {
            return Err(DataFusionError::Internal(
                "MergeCardinalityCheckNode expects exactly 1 input".to_string(),
            ));
        };
        Ok(Self::new(
            Arc::new(input.clone()),
            self.target_row_id_col.clone(),
            self.target_present_col.clone(),
            self.source_present_col.clone(),
        ))
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        None
    }
}

#[derive(Clone, Debug)]
pub struct MergeExpansion {
    pub write_plan: LogicalPlan,
    pub touched_files_plan: Option<LogicalPlan>,
    pub row_index_delete_plan: Option<LogicalPlan>,
    pub change_data_plan: Option<LogicalPlan>,
    pub change_data_preimage: Option<String>,
    pub output_schema: DFSchemaRef,
    pub options: MergeIntoOptions,
}

#[derive(Clone, Copy, Debug)]
pub struct MergePlanRequirements {
    /// Whether the write branch must retain unchanged target rows for file rewrites.
    pub preserve_unmodified_target_rows: bool,
    pub source_metrics: bool,
    pub effects: RowLevelEffectRequirements,
}

fn merge_name_key(name: &str, case_sensitive: bool) -> String {
    if case_sensitive {
        name.to_string()
    } else {
        name.to_ascii_lowercase()
    }
}

fn merge_names_equal(left: &str, right: &str, case_sensitive: bool) -> bool {
    if case_sensitive {
        left == right
    } else {
        left.eq_ignore_ascii_case(right)
    }
}

fn collision_free_source_alias(
    source_name: &str,
    occupied_names: &mut HashSet<String>,
    case_sensitive: bool,
) -> String {
    let base = format!("__sail_src_{source_name}");
    let mut alias = base.clone();
    let mut suffix = 0usize;
    while occupied_names.contains(&merge_name_key(&alias, case_sensitive)) {
        suffix += 1;
        alias = format!("{base}_{suffix}");
    }
    occupied_names.insert(merge_name_key(&alias, case_sensitive));
    alias
}

pub fn validate_merge_internal_columns(info: &MergeInfo, format_columns: &[&str]) -> Result<()> {
    let target_schema = info.target.schema();
    let reserved_columns = [
        TARGET_PRESENT_COLUMN,
        SOURCE_PRESENT_COLUMN,
        TARGET_ROW_ID_COLUMN,
        OPERATION_COLUMN,
        MERGE_SOURCE_METRIC_COLUMN,
    ]
    .into_iter()
    .chain(format_columns.iter().copied());

    for reserved_column in reserved_columns {
        if info
            .options
            .resolved_target_field_names
            .iter()
            .any(|name| merge_names_equal(name, reserved_column, info.options.case_sensitive))
            || target_schema.fields().iter().any(|field| {
                merge_names_equal(field.name(), reserved_column, info.options.case_sensitive)
            })
        {
            return plan_err!(
                "MERGE target column '{reserved_column}' uses a reserved internal MERGE column name"
            );
        }
    }
    Ok(())
}

pub fn expand_merge(
    info: MergeInfo,
    path_column: &str,
    row_index_column: Option<&str>,
    row_delete_metadata_columns: &[&str],
    requirements: MergePlanRequirements,
) -> Result<MergeExpansion> {
    let target_plan = info.target.as_ref().clone();
    let source_plan = info.source.as_ref().clone();
    let mut options = info.options;
    let merge_schema = info.input_schema;
    let mut should_check_cardinality = should_check_cardinality(&options.matched_clauses);
    if should_check_cardinality
        && source_is_unique_on_merge_join_keys(&source_plan, &options.join_key_pairs)
    {
        should_check_cardinality = false;
    }
    trace!(
        "merge input schema fields: {:?}",
        merge_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>()
    );
    trace!(
        "resolved target/source schema fields - target: {:?}, source: {:?}",
        options
            .resolved_target_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>(),
        options
            .resolved_source_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>()
    );

    // Use the real field names captured at resolution time to map opaque IDs
    // back to user-facing column names. Fall back to the `recover_field_names`
    // heuristic only when the resolver did not provide names.
    let desired_target_names = if !options.resolved_target_field_names.is_empty() {
        options.resolved_target_field_names.clone()
    } else {
        recover_field_names(&target_plan, path_column).unwrap_or_else(|| {
            options
                .resolved_target_schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect()
        })
    };
    let desired_source_names = if !options.resolved_source_field_names.is_empty() {
        options.resolved_source_field_names.clone()
    } else {
        recover_field_names(&source_plan, path_column).unwrap_or_else(|| {
            options
                .resolved_source_schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect()
        })
    };
    trace!("resolved target names: {:?}", desired_target_names);
    trace!("resolved source names: {:?}", desired_source_names);

    let _target_relation = options.target_alias.as_ref().map(|a| TableReference::Bare {
        table: a.clone().into(),
    });
    let source_relation = options.source_alias.as_ref().map(|a| TableReference::Bare {
        table: a.clone().into(),
    });

    let target_scan_fields: Vec<String> = target_plan
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    trace!(
        "target scan fields pre-projection: {:?}",
        target_scan_fields
    );

    let evolves_star_columns = options.with_schema_evolution && merge_has_star_action(&options);
    let mut target_proj_exprs = target_plan
        .schema()
        .fields()
        .iter()
        .zip(desired_target_names.iter())
        .enumerate()
        .map(|(index, (field, desired))| -> Result<Expr> {
            let target_field = options
                .resolved_target_schema
                .fields()
                .get(index)
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "MERGE target field is missing during schema evolution".to_string(),
                    )
                })?;
            let evolved_target = if evolves_star_columns {
                desired_source_names
                    .iter()
                    .position(|source_name| {
                        merge_names_equal(source_name, desired, options.case_sensitive)
                    })
                    .and_then(|source_index| {
                        options.resolved_source_schema.fields().get(source_index)
                    })
                    .map(|source_field| {
                        evolve_row_level_field(target_field, source_field, options.case_sensitive)
                    })
            } else {
                None
            };
            let target_type = evolved_target
                .as_ref()
                .map(|field| field.data_type())
                .unwrap_or_else(|| field.data_type());
            // Use unqualified column to avoid alias-mismatch when upstream qualifiers differ.
            Ok(align_row_level_value(
                Expr::Column(Column::from_name(field.name().clone())),
                field.data_type(),
                target_type,
                options.case_sensitive,
                StructFieldAlignment::FillMissingWithNull,
                &arrow_timezone_label(&options.session_timezone),
            )?
            .alias(desired.clone()))
        })
        .collect::<Result<Vec<_>>>()?;

    if evolves_star_columns {
        for (source_field, source_name) in options
            .resolved_source_schema
            .fields()
            .iter()
            .zip(&desired_source_names)
        {
            if desired_target_names.iter().any(|target_name| {
                merge_names_equal(target_name, source_name, options.case_sensitive)
            }) {
                continue;
            }
            if is_reserved_merge_column(
                source_name,
                path_column,
                row_index_column,
                row_delete_metadata_columns,
                options.case_sensitive,
            ) {
                return plan_err!(
                    "MERGE schema evolution cannot add reserved internal column '{source_name}'"
                );
            }
            let null_value = ScalarValue::try_new_null(source_field.data_type())?;
            target_proj_exprs.push(lit(null_value).alias(source_name.clone()));
        }
    }

    // Ensure MERGE metadata columns are preserved even when desired_target_names was shorter.
    let path_already_present = target_proj_exprs
        .iter()
        .any(|expr| matches!(expr, Expr::Alias(alias) if alias.name == path_column));
    if !path_already_present {
        target_proj_exprs.push(
            Expr::Column(Column::from_name(path_column.to_string())).alias(path_column.to_string()),
        );
    }
    if let Some(row_index_column) = row_index_column {
        let row_index_already_present = target_proj_exprs
            .iter()
            .any(|expr| matches!(expr, Expr::Alias(alias) if alias.name == row_index_column));
        if !row_index_already_present {
            target_proj_exprs.push(
                Expr::Column(Column::from_name(row_index_column.to_string()))
                    .alias(row_index_column.to_string()),
            );
        }
    }
    for metadata_column in row_delete_metadata_columns {
        let already_present = target_proj_exprs
            .iter()
            .any(|expr| matches!(expr, Expr::Alias(alias) if alias.name == *metadata_column));
        if !already_present
            && target_plan
                .schema()
                .has_column_with_unqualified_name(metadata_column)
        {
            target_proj_exprs.push(
                Expr::Column(Column::from_name((*metadata_column).to_string()))
                    .alias((*metadata_column).to_string()),
            );
        }
    }

    trace!(
        "target projection expr names: {:?}",
        target_proj_exprs
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
    );

    let mut target_plan = LogicalPlanBuilder::from(target_plan.clone())
        .project(target_proj_exprs)?
        .build()?;

    // To avoid duplicate unqualified names after JOIN, assign collision-free source aliases.
    let target_input_len = options.resolved_target_schema.fields().len();
    let mut target_rename_map: HashMap<String, String> = HashMap::new();
    for (idx, desired) in desired_target_names
        .iter()
        .take(target_input_len)
        .enumerate()
    {
        target_rename_map.insert(
            merge_schema
                .fields()
                .get(idx)
                .map(|f| f.name().clone())
                .unwrap_or_else(|| desired.clone()),
            desired.clone(),
        );
    }
    // keep path column mapping stable if present
    target_rename_map.insert(path_column.to_string(), path_column.to_string());
    if let Some(row_index_column) = row_index_column {
        target_rename_map.insert(row_index_column.to_string(), row_index_column.to_string());
    }
    for metadata_column in row_delete_metadata_columns {
        target_rename_map.insert(
            (*metadata_column).to_string(),
            (*metadata_column).to_string(),
        );
    }
    // keep row id stable if present
    target_rename_map.insert(
        TARGET_ROW_ID_COLUMN.to_string(),
        TARGET_ROW_ID_COLUMN.to_string(),
    );

    let mut occupied_names = target_plan
        .schema()
        .fields()
        .iter()
        .map(|field| merge_name_key(field.name(), options.case_sensitive))
        .collect::<HashSet<_>>();
    let mut source_rename_map: HashMap<String, String> = HashMap::new();
    options.source_column_aliases.clear();
    let target_input_len = options.resolved_target_schema.fields().len();
    for (idx, desired) in desired_source_names.iter().enumerate() {
        let source_alias =
            collision_free_source_alias(desired, &mut occupied_names, options.case_sensitive);
        options
            .source_column_aliases
            .push((desired.clone(), source_alias.clone()));
        source_rename_map.insert(desired.clone(), source_alias.clone());
        if let Some(field) = merge_schema.fields().get(target_input_len + idx) {
            source_rename_map.insert(field.name().clone(), source_alias);
        }
    }

    normalize_target_column_names(&mut options, &target_rename_map);

    let source_plan = LogicalPlanBuilder::from(source_plan.clone())
        .project(
            source_plan
                .schema()
                .fields()
                .iter()
                .zip(desired_source_names.iter())
                .map(|(field, desired)| {
                    let renamed = source_rename_map
                        .get(desired)
                        .cloned()
                        .unwrap_or_else(|| desired.clone());
                    Expr::Column(Column::new(source_relation.clone(), field.name().clone()))
                        .alias(renamed)
                })
                .collect::<Vec<_>>(),
        )?
        .build()?;

    // Rewrite all expressions that reference source columns to the new prefixed names.
    let rewrite = |expr: Expr| rewrite_merge_columns(expr, &target_rename_map, &source_rename_map);
    options.on_condition = ExprWithSource::new(
        rewrite(options.on_condition.expr.clone())?,
        options.on_condition.source.clone(),
    );
    options.join_key_pairs = options
        .join_key_pairs
        .iter()
        .map(|(l, r)| Ok((rewrite(l.clone())?, rewrite(r.clone())?)))
        .collect::<Result<Vec<_>>>()?;
    options.residual_predicates = options
        .residual_predicates
        .iter()
        .map(|e| rewrite(e.clone()))
        .collect::<Result<Vec<_>>>()?;
    options.target_only_predicates = options
        .target_only_predicates
        .iter()
        .map(|e| rewrite(e.clone()))
        .collect::<Result<Vec<_>>>()?;
    rewrite_clauses(&mut options.matched_clauses, &rewrite)?;
    rewrite_not_matched_by_source(&mut options.not_matched_by_source_clauses, &rewrite)?;
    rewrite_not_matched_by_target(&mut options.not_matched_by_target_clauses, &rewrite)?;
    options.generated_column_exprs = options
        .generated_column_exprs
        .iter()
        .map(|(name, expr)| Ok((name.clone(), rewrite(expr.clone())?)))
        .collect::<Result<Vec<_>>>()?;
    options.default_column_exprs = options
        .default_column_exprs
        .iter()
        .map(|(name, expr)| Ok((name.clone(), rewrite(expr.clone())?)))
        .collect::<Result<Vec<_>>>()?;
    options.check_constraint_exprs = options
        .check_constraint_exprs
        .iter()
        .map(|constraint| {
            Ok(DeltaCheckConstraintExpr {
                name: constraint.name.clone(),
                expression: constraint.expression.clone(),
                expr: rewrite(constraint.expr.clone())?,
                violation: constraint.violation.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    if options.target.format.eq_ignore_ascii_case("delta") {
        target_plan = filter_merge_target_partitions(
            target_plan,
            &options.target_only_predicates,
            &options.target.partition_by,
            !options.not_matched_by_source_clauses.is_empty(),
            options.case_sensitive,
        )?;
    }

    let scope = uuid::Uuid::new_v4();
    let source_id =
        collision_free_source_alias("row_id", &mut occupied_names, options.case_sensitive);
    let source_plan = LogicalPlan::Extension(Extension {
        node: Arc::new(MonotonicIdNode::try_new(
            Arc::new(source_plan),
            source_id.clone(),
        )?),
    });
    let source_plan = replay_plan(source_plan, ReplayId { scope, ordinal: 0 });

    {
        // Add stable per-target-row id before join; JOIN will duplicate this value for matches.
        // Use a dedicated logical node so we don't rely on later expression rewriters (MERGE builds plans directly).
        target_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(MonotonicIdNode::try_new(
                Arc::new(target_plan),
                TARGET_ROW_ID_COLUMN.to_string(),
            )?),
        });
    }

    let target_schema = target_plan.schema();
    let source_schema = source_plan.schema();
    trace!(
        "expand_merge target/source fields - target: {:?}, source: {:?}",
        target_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>(),
        source_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>()
    );
    trace!(
        "expand_merge options after rewrite - join_key_pairs: {:?}, matched_clauses: {:?}, not_matched_by_source_clauses: {:?}, not_matched_by_target_clauses: {:?}, on_condition: {:?}",
        options.join_key_pairs,
        options.matched_clauses,
        options.not_matched_by_source_clauses,
        options.not_matched_by_target_clauses,
        options.on_condition
    );

    // Detect insert-only MERGE that can use fast append (no touched files).
    let single_stream_cdf =
        requirements.effects.change_data && !options.identity_columns.is_empty();
    let can_fast_append =
        !single_stream_cdf && can_fast_append_insert_only(&options, target_schema, path_column)?;

    if can_fast_append {
        let join_on = options
            .join_key_pairs
            .iter()
            .map(|(t, s)| Ok((s.clone(), t.clone())))
            .collect::<Result<Vec<_>>>()?;
        let residual_filter = combine_conjunction(&options.residual_predicates);

        let insert_rows = Join::try_new(
            Arc::new(source_plan.clone()),
            Arc::new(target_plan.clone()),
            join_on.clone(),
            residual_filter.clone(),
            JoinType::LeftAnti,
            JoinConstraint::On,
            NullEquality::NullEqualsNothing,
            false,
        )?;
        let output_options = options.clone();
        let (insert_rows, selection) =
            MergeClauseSelection::apply(LogicalPlan::Join(insert_rows), &mut options, true)?;
        let insert_rows = LogicalPlanBuilder::from(insert_rows)
            .filter(selection.insert_predicate())?
            .build()?;
        let insert_rows = capture_merge_values(
            insert_rows,
            &mut options,
            target_schema,
            path_column,
            row_index_column,
            row_delete_metadata_columns,
        )?;
        let insert_value_schema = insert_rows.schema().clone();
        let insert_operation = lit(RowLevelOperationType::Insert.as_i32());

        let mut insert_projection_exprs = build_insert_only_projection(
            &options,
            target_schema,
            &insert_value_schema,
            path_column,
            row_index_column,
            row_delete_metadata_columns,
            insert_operation,
        )?;
        let writer_columns = insert_projection_exprs
            .iter()
            .map(|expr| expr.schema_name().to_string())
            .collect::<Vec<_>>();
        insert_projection_exprs.extend([
            col(&source_id),
            col(&selection.ordinal),
            lit(ScalarValue::Int64(None)).alias(TARGET_ROW_ID_COLUMN),
        ]);
        for name in std::iter::once(path_column)
            .chain(row_index_column)
            .chain(row_delete_metadata_columns.iter().copied())
        {
            let field = target_schema.field_with_unqualified_name(name)?;
            insert_projection_exprs
                .push(lit(ScalarValue::try_new_null(field.data_type())?).alias(name));
        }
        let insert_projected = project_merge_with_identity(
            insert_rows,
            insert_projection_exprs,
            &options,
            &selection,
        )?;
        let insert_projected =
            apply_generation_projection(insert_projected, &options.generated_column_exprs, &[])?;

        let projected = apply_delta_check_constraint_filter(
            insert_projected,
            &options.check_constraint_exprs,
            Some(row_level_data_operation_expr()),
        )?;

        let projected = replay_plan(projected, ReplayId { scope, ordinal: 1 });
        let change_data_plan = requirements
            .effects
            .change_data
            .then(|| {
                change_data_postimages(
                    projected.clone(),
                    target_schema,
                    path_column,
                    row_index_column,
                    row_delete_metadata_columns,
                )
            })
            .transpose()?;
        let projected = LogicalPlanBuilder::from(projected)
            .project(
                writer_columns
                    .iter()
                    .map(|name| Expr::Column(Column::from_name(name)))
                    .collect::<Vec<_>>(),
            )?
            .build()?;
        let projected = if requirements.source_metrics {
            let mut expressions = projected
                .schema()
                .columns()
                .into_iter()
                .map(Expr::Column)
                .collect::<Vec<_>>();
            expressions.push(lit(ScalarValue::Int64(None)).alias(MERGE_SOURCE_METRIC_COLUMN));
            let writer = LogicalPlanBuilder::from(projected)
                .project(expressions)?
                .build()?;
            let metric = build_source_metric_plan(source_plan.clone(), writer.schema())?;
            LogicalPlanBuilder::from(writer).union(metric)?.build()?
        } else {
            projected
        };
        let command_schema = Arc::new(DFSchema::empty());
        return Ok(MergeExpansion {
            write_plan: projected,
            touched_files_plan: None,
            row_index_delete_plan: None,
            change_data_plan,
            change_data_preimage: None,
            output_schema: command_schema,
            options: output_options,
        });
    }

    build_default_merge_expansion(
        options,
        target_plan,
        source_plan,
        should_check_cardinality,
        path_column,
        row_index_column,
        row_delete_metadata_columns,
        requirements,
        ReplayId { scope, ordinal: 1 },
        &source_id,
    )
}

fn replay_plan(input: LogicalPlan, id: ReplayId) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(ReplayNode::new(id, Arc::new(input))),
    })
}

/// Default MERGE expansion: clause-aware join + presence columns + touched files.
#[expect(clippy::too_many_arguments, reason = "explicit MERGE planning inputs")]
fn build_default_merge_expansion(
    mut options: MergeIntoOptions,
    target_plan: LogicalPlan,
    source_plan: LogicalPlan,
    should_check_cardinality: bool,
    path_column: &str,
    row_index_column: Option<&str>,
    row_delete_metadata_columns: &[&str],
    requirements: MergePlanRequirements,
    intent_id: ReplayId,
    source_id: &str,
) -> Result<MergeExpansion> {
    let target_schema = target_plan.schema();
    // Unconditional matched DELETE needs only source presence, not source values.
    // Count and delete each physical target once even when several source rows match.
    let unconditional_matched_delete = matches!(
        options.matched_clauses.as_slice(),
        [clause] if clause.condition.is_none() && matches!(clause.action, MergeMatchedAction::Delete)
    );
    let single_stream_cdf =
        requirements.effects.change_data && !options.identity_columns.is_empty();
    let deduplicate_targets = unconditional_matched_delete || options.matched_clauses.is_empty();
    let output_options = options.clone();
    let source_schema = source_plan.schema().clone();

    let augmented_target = LogicalPlanBuilder::from(target_plan.clone())
        .project(append_presence_projection(
            target_schema,
            TARGET_PRESENT_COLUMN,
            Some(path_column),
        )?)?
        .build()?;

    let augmented_source = LogicalPlanBuilder::from(source_plan.clone())
        .project(append_presence_projection(
            &source_schema,
            SOURCE_PRESENT_COLUMN,
            None,
        )?)?
        .build()?;

    let join_on = options.join_key_pairs.clone();
    let residual_filter = combine_conjunction(&options.residual_predicates);
    let join_type = select_merge_join_type(
        &options.matched_clauses,
        &options.not_matched_by_source_clauses,
        &options.not_matched_by_target_clauses,
        requirements.preserve_unmodified_target_rows,
    );

    let join = Join::try_new(
        Arc::new(augmented_target.clone()),
        Arc::new(augmented_source.clone()),
        join_on.clone(),
        residual_filter.clone(),
        join_type,
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
        false,
    )?;

    let join = Arc::new(LogicalPlan::Join(join));
    let join: Arc<LogicalPlan> = if should_check_cardinality {
        Arc::new(LogicalPlan::Extension(Extension {
            node: Arc::new(MergeCardinalityCheckNode::new(
                Arc::clone(&join),
                TARGET_ROW_ID_COLUMN,
                TARGET_PRESENT_COLUMN,
                SOURCE_PRESENT_COLUMN,
            )),
        }))
    } else {
        join
    };

    let join = if deduplicate_targets {
        let keys = vec![
            col(TARGET_ROW_ID_COLUMN),
            when(
                col(TARGET_PRESENT_COLUMN).is_not_null(),
                lit(ScalarValue::Int64(None)),
            )
            .otherwise(col(source_id))?,
        ];
        LogicalPlanBuilder::from(join.as_ref().clone())
            .distinct_on(
                keys,
                join.schema()
                    .columns()
                    .into_iter()
                    .map(Expr::Column)
                    .collect(),
                None,
            )?
            .build()?
    } else {
        join.as_ref().clone()
    };
    let (join, selection) = MergeClauseSelection::apply(join, &mut options, false)?;
    let write_join = Arc::new(join);

    let target_present = col(TARGET_PRESENT_COLUMN).is_not_null();
    let active_expr = target_present.or(selection.insert_predicate());

    let filtered = LogicalPlanBuilder::from(write_join.as_ref().clone())
        .filter(active_expr)?
        .build()?;
    let filtered = capture_merge_values(
        filtered,
        &mut options,
        target_schema,
        path_column,
        row_index_column,
        row_delete_metadata_columns,
    )?;
    let value_schema = filtered.schema().clone();

    let (mut projection_exprs, generated_assignment_markers) = build_merge_projection(
        &options,
        &selection,
        target_schema,
        &value_schema,
        path_column,
        row_index_column,
        row_delete_metadata_columns,
    )?;
    let writer_columns = projection_exprs
        .iter()
        .map(|expr| expr.schema_name().to_string())
        .filter(|name| {
            !generated_assignment_markers
                .iter()
                .any(|(_, marker)| marker == name)
        })
        .collect::<Vec<_>>();
    projection_exprs.extend([
        col(TARGET_ROW_ID_COLUMN),
        col(source_id),
        col(&selection.ordinal),
    ]);
    if row_delete_metadata_columns.is_empty()
        && let Some(index) = row_index_column
    {
        projection_exprs.push(col(index));
    }
    let change_data_preimage =
        if requirements.effects.change_data {
            let mut occupied = filtered
                .schema()
                .fields()
                .iter()
                .map(|field| merge_name_key(field.name(), options.case_sensitive))
                .collect::<HashSet<_>>();
            occupied.extend(projection_exprs.iter().map(|expr| {
                merge_name_key(&expr.schema_name().to_string(), options.case_sensitive)
            }));
            let name =
                collision_free_source_alias("cdc_preimage", &mut occupied, options.case_sensitive);
            let values = change_data_columns(
                target_schema,
                path_column,
                row_index_column,
                row_delete_metadata_columns,
            );
            let names = values
                .iter()
                .map(|expr| expr.schema_name().to_string())
                .collect();
            projection_exprs.push(
                ScalarUDF::from(
                    sail_function::scalar::r#struct::struct_function::StructFunction::new(names),
                )
                .call(values)
                .alias(&name),
            );
            Some(name)
        } else {
            None
        };

    trace!("projection exprs: {:?}", projection_exprs);

    let projected =
        project_merge_with_identity(filtered, projection_exprs.clone(), &options, &selection)?;
    let projected = apply_generation_projection(
        projected,
        &options.generated_column_exprs,
        &generated_assignment_markers,
    )?;
    let projected = apply_delta_check_constraint_filter(
        projected,
        &options.check_constraint_exprs,
        Some(row_level_data_operation_expr()),
    )?;
    let intent = replay_plan(projected, intent_id);
    let projected = intent.clone();
    let change_data_plan = if !single_stream_cdf && let Some(preimage) = &change_data_preimage {
        let before = intent.clone();
        let update = change_data_update_operation();
        let deleted = col(OPERATION_COLUMN)
            .eq(lit(RowLevelOperationType::MatchedDelete.as_i32()))
            .or(col(OPERATION_COLUMN)
                .eq(lit(RowLevelOperationType::NotMatchedBySourceDelete.as_i32())));
        let mut fields = change_data_columns(
            target_schema,
            path_column,
            row_index_column,
            row_delete_metadata_columns,
        )
        .into_iter()
        .map(|expr| {
            let name = expr.schema_name().to_string();
            col(preimage).field(name.clone()).alias(name)
        })
        .collect::<Vec<_>>();
        fields.push(
            when(update.clone(), lit("update_preimage"))
                .otherwise(lit("delete"))?
                .alias("_change_type"),
        );
        let before = LogicalPlanBuilder::from(before)
            .filter(update.or(deleted))?
            .project(fields)?
            .build()?;
        let after = change_data_postimages(
            projected.clone(),
            target_schema,
            path_column,
            row_index_column,
            row_delete_metadata_columns,
        )?;
        Some(LogicalPlanBuilder::from(before).union(after)?.build()?)
    } else {
        None
    };

    // Targeted rewrites may omit matched or source-only rows from the write branch,
    // so formats that report source-row metrics use a separate aggregate branch.
    let change_data_preimage = change_data_preimage.filter(|_| single_stream_cdf);
    let writer_expressions = writer_columns
        .iter()
        .chain(change_data_preimage.iter())
        .map(|name| Expr::Column(Column::from_name(name)))
        .collect::<Vec<_>>();
    let projected = LogicalPlanBuilder::from(projected)
        .project(writer_expressions)?
        .build()?;
    let projected = if requirements.source_metrics {
        let metric = build_source_metric_plan(source_plan.clone(), projected.schema())?;
        LogicalPlanBuilder::from(projected).union(metric)?.build()?
    } else {
        projected
    };

    let rewrite_filter = (!options.matched_clauses.is_empty()
        || !options.not_matched_by_source_clauses.is_empty())
    .then(|| {
        change_data_update_operation()
            .or(col(OPERATION_COLUMN).eq(lit(RowLevelOperationType::MatchedDelete.as_i32())))
            .or(col(OPERATION_COLUMN)
                .eq(lit(RowLevelOperationType::NotMatchedBySourceDelete.as_i32())))
    });

    let touched_files_plan = if requirements.effects.touched_files {
        rewrite_filter
            .clone()
            .map(|rewrite_filter| {
                LogicalPlanBuilder::from(intent.clone())
                    .filter(rewrite_filter)?
                    .aggregate(vec![col(path_column)], Vec::<Expr>::new())?
                    .project(vec![col(path_column).alias(path_column.to_string())])?
                    .build()
            })
            .transpose()?
    } else {
        None
    };

    let row_index_delete_plan = if requirements.effects.row_index_deletes
        && let Some(row_index_column) = row_index_column
        && let Some(row_delete_pred) = rewrite_filter
    {
        let mut delete_projection = vec![
            col(path_column).alias(path_column.to_string()),
            col(row_index_column).alias(row_index_column.to_string()),
        ];
        delete_projection.extend(
            row_delete_metadata_columns
                .iter()
                .map(|column| col(*column).alias((*column).to_string())),
        );
        delete_projection.push(col(OPERATION_COLUMN));
        Some(
            LogicalPlanBuilder::from(intent)
                .filter(row_delete_pred)?
                .project(delete_projection)?
                .build()?,
        )
    } else {
        None
    };

    let command_schema = Arc::new(DFSchema::empty());

    Ok(MergeExpansion {
        write_plan: projected,
        touched_files_plan,
        row_index_delete_plan,
        change_data_plan,
        change_data_preimage,
        output_schema: command_schema,
        options: output_options,
    })
}

fn select_merge_join_type(
    matched_clauses: &[MergeMatchedClause],
    not_matched_by_source_clauses: &[MergeNotMatchedBySourceClause],
    not_matched_by_target_clauses: &[MergeNotMatchedByTargetClause],
    preserve_unmodified_target_rows: bool,
) -> JoinType {
    if preserve_unmodified_target_rows {
        // A left join preserves every target row; source-only inserts require a full join.
        return if not_matched_by_target_clauses.is_empty() {
            JoinType::Left
        } else {
            JoinType::Full
        };
    }

    let matched_only = !matched_clauses.is_empty()
        && not_matched_by_source_clauses.is_empty()
        && not_matched_by_target_clauses.is_empty();

    if matched_only {
        JoinType::Inner
    } else if not_matched_by_source_clauses.is_empty() {
        JoinType::Right
    } else if not_matched_by_target_clauses.is_empty() {
        JoinType::Left
    } else {
        JoinType::Full
    }
}

fn append_presence_projection(
    schema: &DFSchemaRef,
    present_col: &str,
    path_column: Option<&str>,
) -> Result<Vec<Expr>> {
    let mut exprs: Vec<Expr> = schema
        .fields()
        .iter()
        .map(|f| Expr::Column(Column::from_name(f.name().clone())))
        .collect();

    if let Some(path_name) = path_column
        && schema.index_of_column_by_name(None, path_name).is_none()
    {
        let path_expr = lit(ScalarValue::Utf8(None));
        exprs.push(path_expr.alias(path_name.to_string()));
    }

    exprs.push(lit(true).alias(present_col));
    Ok(exprs)
}

fn can_fast_append_insert_only(
    options: &MergeIntoOptions,
    target_schema: &DFSchemaRef,
    path_column: &str,
) -> Result<bool> {
    // Insert-only: only NOT MATCHED BY TARGET clauses, no updates/deletes.
    if !options.matched_clauses.is_empty() {
        return Ok(false);
    }
    if !options.not_matched_by_source_clauses.is_empty() {
        return Ok(false);
    }
    if options.not_matched_by_target_clauses.is_empty() {
        return Ok(false);
    }

    // Robustness gate: ensure NOT MATCHED BY TARGET conditions/values do not reference target.
    // (We still allow referencing target columns in the ON condition for the anti join.)
    let target_names: std::collections::HashSet<String> = target_schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .chain(std::iter::once(path_column.to_string()))
        .collect();

    let references_target = |expr: &Expr| -> Result<bool> {
        let mut cols: std::collections::HashSet<Column> = std::collections::HashSet::new();
        expr_to_columns(expr, &mut cols)?;
        Ok(cols.iter().any(|c| target_names.contains(&c.name)))
    };

    for clause in &options.not_matched_by_target_clauses {
        if let Some(cond) = &clause.condition
            && references_target(&cond.expr)?
        {
            return Ok(false);
        }
        match &clause.action {
            MergeNotMatchedByTargetAction::InsertAll => {}
            MergeNotMatchedByTargetAction::InsertColumns { values, .. } => {
                for v in values {
                    if references_target(v)? {
                        return Ok(false);
                    }
                }
            }
        }
    }

    Ok(true)
}

fn merge_has_star_action(options: &MergeIntoOptions) -> bool {
    options
        .matched_clauses
        .iter()
        .any(|clause| matches!(clause.action, MergeMatchedAction::UpdateAll))
        || options
            .not_matched_by_target_clauses
            .iter()
            .any(|clause| matches!(clause.action, MergeNotMatchedByTargetAction::InsertAll))
}

fn merge_insert_omits_identity(
    options: &MergeIntoOptions,
    action: &MergeNotMatchedByTargetAction,
    name: &str,
) -> bool {
    match action {
        MergeNotMatchedByTargetAction::InsertColumns { columns, .. } => !columns
            .iter()
            .any(|column| merge_names_equal(column, name, options.case_sensitive)),
        MergeNotMatchedByTargetAction::InsertAll => {
            options.with_schema_evolution && merge_source_expr(options, name).is_none()
        }
    }
}

/// Whether an identity can be generated by any INSERT clause after source alias
/// resolution. Presence, not nullness, controls allocation and writer tracking.
pub fn merge_generates_identity(options: &MergeIntoOptions, name: &str) -> bool {
    options
        .not_matched_by_target_clauses
        .iter()
        .any(|clause| merge_insert_omits_identity(options, &clause.action, name))
}

fn project_merge_with_identity(
    input: LogicalPlan,
    mut projections: Vec<Expr>,
    options: &MergeIntoOptions,
    selection: &MergeClauseSelection,
) -> Result<LogicalPlan> {
    let predicates = selection.predicates(&selection.inserts);
    let mut occupied =
        input
            .schema()
            .fields()
            .iter()
            .map(|field| merge_name_key(field.name(), options.case_sensitive))
            .chain(projections.iter().map(|expr| {
                merge_name_key(&expr.schema_name().to_string(), options.case_sensitive)
            }))
            .collect::<HashSet<_>>();
    let mut domains: Vec<(Expr, String, String)> = Vec::new();
    let mut allocations = Vec::new();
    for (name, identity) in &options.identity_columns {
        if !merge_generates_identity(options, name) {
            continue;
        }
        let mask = combine_disjunction(
            &options
                .not_matched_by_target_clauses
                .iter()
                .zip(&predicates)
                .filter(|(clause, _)| merge_insert_omits_identity(options, &clause.action, name))
                .map(|(_, predicate)| predicate.clone())
                .collect::<Vec<_>>(),
        )
        .unwrap_or_else(|| lit(false));
        let domain = {
            let marker = collision_free_source_alias(
                "identity_generate",
                &mut occupied,
                options.case_sensitive,
            );
            let number = collision_free_source_alias(
                "identity_value",
                &mut occupied,
                options.case_sensitive,
            );
            projections.push(mask.clone().alias(marker.clone()));
            domains.push((mask, marker, number));
            domains.len() - 1
        };
        allocations.push((name, identity, domain));
    }
    let mut plan = evaluate_once(input, projections)?;
    if allocations.is_empty() {
        return Ok(plan);
    }
    plan = LogicalPlan::Extension(Extension {
        node: Arc::new(crate::identity_generate::IdentityGenerateNode::try_new(
            Arc::new(plan),
            allocations
                .iter()
                .map(|(name, identity, domain)| {
                    let (_, marker, output) = &domains[*domain];
                    crate::identity_generate::IdentityGenerateColumn {
                        output: output.clone(),
                        column_name: (*name).clone(),
                        mask: Some(marker.clone()),
                        start: identity.start,
                        step: identity.step,
                        high_water_mark: identity.high_water_mark,
                    }
                })
                .collect(),
        )?),
    });
    let schema = plan.schema();
    let column = |name: &str| Expr::Column(Column::from_name(name));
    let output = schema
        .fields()
        .iter()
        .filter(|field| {
            !domains
                .iter()
                .any(|(_, marker, number)| field.name() == marker || field.name() == number)
        })
        .map(|field| {
            let Some((name, identity, domain)) = allocations
                .iter()
                .find(|(name, _, _)| *name == field.name())
            else {
                return Ok(column(field.name()));
            };
            let (_, marker, number) = &domains[*domain];
            let value = column(number);
            let value = when(column(marker), value).otherwise(column(field.name()))?;
            let mut metadata = options
                .resolved_target_field_names
                .iter()
                .position(|target| target == *name)
                .map(|index| {
                    options
                        .resolved_target_schema
                        .field(index)
                        .metadata()
                        .clone()
                })
                .unwrap_or_default();
            metadata.extend(ColumnFeaturesBuilder::new().with_identity(identity).build());
            Ok(value.alias_with_metadata(field.name(), Some(FieldMetadata::from(metadata))))
        })
        .collect::<Result<Vec<_>>>()?;
    evaluate_once(plan, output)
}

fn apply_generation_projection(
    plan: LogicalPlan,
    generated_column_exprs: &[(String, Expr)],
    generated_assignment_markers: &[GeneratedAssignmentMarker],
) -> Result<LogicalPlan> {
    if generated_column_exprs.is_empty() {
        return Ok(plan);
    }
    let mut occupied = plan
        .schema()
        .fields()
        .iter()
        .map(|field| merge_name_key(field.name(), false))
        .collect();
    let mut expected_projection = plan
        .schema()
        .columns()
        .into_iter()
        .map(Expr::Column)
        .collect::<Vec<_>>();
    let mut expected_columns = Vec::new();
    let mut expected_names = Vec::new();
    for (name, expr) in generated_column_exprs {
        let slot = collision_free_source_alias("generated_expected", &mut occupied, false);
        let data_type = expr.get_type(plan.schema())?;
        let selected = col(OPERATION_COLUMN)
            .eq(lit(RowLevelOperationType::Insert.as_i32()))
            .or(change_data_update_operation());
        expected_projection.push(
            when(selected, expr.clone())
                .otherwise(lit(ScalarValue::try_new_null(&data_type)?))?
                .alias(&slot),
        );
        expected_columns.push((name.clone(), col(&slot)));
        expected_names.push(slot);
    }
    let plan = evaluate_once(plan, expected_projection)?;
    let generated_column_exprs = &expected_columns;
    let gen_map: HashMap<&str, &Expr> = generated_column_exprs
        .iter()
        .map(|(name, expr)| (name.as_str(), expr))
        .collect();
    let schema = plan.schema().clone();
    let has_op_col = schema.has_column_with_unqualified_name(OPERATION_COLUMN);
    let insert_op_val = lit(RowLevelOperationType::Insert.as_i32());
    let matched_update_op_val = lit(RowLevelOperationType::MatchedUpdate.as_i32());
    let not_matched_by_source_update_op_val =
        lit(RowLevelOperationType::NotMatchedBySourceUpdate.as_i32());
    let marker_map = generated_assignment_markers
        .iter()
        .map(|(column, marker)| (column.as_str(), marker.as_str()))
        .collect::<HashMap<_, _>>();
    let post_exprs: Vec<Expr> = schema
        .fields()
        .iter()
        .filter(|field| {
            !expected_names.contains(field.name())
                && !generated_assignment_markers
                    .iter()
                    .any(|(_, marker)| marker == field.name())
        })
        .map(|f| {
            let name = f.name();
            if let Some(gen_expr) = gen_map.get(name.as_str()) {
                let gen_expr = (*gen_expr).clone();
                let current_value = Expr::Column(Column::from_name(name));
                // For INSERT rows, enforce Delta protocol: if the user explicitly provided a
                // non-NULL value for the generated column that doesn't match the expression,
                // raise an error instead of silently overwriting.
                //
                // UPDATE recomputes omitted generated columns, while explicit assignments must
                // satisfy the generation expression and retain their supplied value.
                let explicit_value_matches = Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(current_value.clone()),
                    Operator::IsNotDistinctFrom,
                    Box::new(gen_expr.clone()),
                ));
                let err_msg = format!(
                    "[DELTA_GENERATED_COLUMNS_VALUE_MISMATCH] \
                     CHECK constraint for generated column `{name}` violated: \
                     user-provided value does not match the generation expression."
                );
                let raise = ScalarUDF::from(RaiseError::new()).call(vec![lit(err_msg)]);
                let explicit_update_value =
                    when(explicit_value_matches.clone(), current_value.clone())
                        .otherwise(raise.clone())
                        .map(|e| e.alias(name.clone()))?;
                let insert_value = when(
                    current_value.clone().is_null().or(explicit_value_matches),
                    gen_expr.clone(),
                )
                .otherwise(raise)
                .map(|e| e.alias(name.clone()))?;
                if has_op_col {
                    let update_op = col(OPERATION_COLUMN)
                        .eq(matched_update_op_val.clone())
                        .or(col(OPERATION_COLUMN).eq(not_matched_by_source_update_op_val.clone()));
                    let explicit_update = marker_map
                        .get(name.as_str())
                        .map(|marker| col(*marker))
                        .unwrap_or_else(|| lit(false));
                    when(
                        col(OPERATION_COLUMN).eq(insert_op_val.clone()),
                        insert_value,
                    )
                    .when(
                        update_op.clone().and(explicit_update),
                        explicit_update_value,
                    )
                    .when(update_op, gen_expr.clone())
                    .otherwise(current_value)
                    .map(|e| e.alias(name.clone()))
                } else {
                    // Insert-only path (fast-append): always enforce.
                    Ok(insert_value)
                }
            } else {
                Ok(Expr::Column(Column::from_name(name)))
            }
        })
        .collect::<Result<Vec<Expr>>>()?;
    evaluate_once(plan, post_exprs)
}

fn row_level_data_operation_expr() -> Expr {
    let op = col(OPERATION_COLUMN);
    op.clone()
        .eq(lit(RowLevelOperationType::Copy.as_i32()))
        .or(op.clone().eq(lit(RowLevelOperationType::Insert.as_i32())))
        .or(op.clone().eq(lit(RowLevelOperationType::Update.as_i32())))
        .or(op
            .clone()
            .eq(lit(RowLevelOperationType::MatchedUpdate.as_i32())))
        .or(op.eq(lit(RowLevelOperationType::NotMatchedBySourceUpdate.as_i32())))
}

fn is_merge_metadata_column(
    name: &str,
    path_column: &str,
    row_index_column: Option<&str>,
    row_delete_metadata_columns: &[&str],
) -> bool {
    name == path_column
        || row_index_column.is_some_and(|column| name == column)
        || row_delete_metadata_columns.contains(&name)
        || name == TARGET_ROW_ID_COLUMN
}

fn is_reserved_merge_column(
    name: &str,
    path_column: &str,
    row_index_column: Option<&str>,
    row_delete_metadata_columns: &[&str],
    case_sensitive: bool,
) -> bool {
    [
        path_column,
        TARGET_PRESENT_COLUMN,
        SOURCE_PRESENT_COLUMN,
        TARGET_ROW_ID_COLUMN,
        OPERATION_COLUMN,
        MERGE_SOURCE_METRIC_COLUMN,
    ]
    .into_iter()
    .chain(row_index_column)
    .chain(row_delete_metadata_columns.iter().copied())
    .any(|reserved| merge_names_equal(name, reserved, case_sensitive))
}

fn merge_source_expr(options: &MergeIntoOptions, target_name: &str) -> Option<Expr> {
    options
        .source_column_aliases
        .iter()
        .find(|(source_name, _)| {
            merge_names_equal(source_name, target_name, options.case_sensitive)
        })
        .map(|(_, source_alias)| Expr::Column(Column::from_name(source_alias.clone())))
}

fn merge_default_expr(options: &MergeIntoOptions, target_name: &str) -> Option<Expr> {
    options
        .default_column_exprs
        .iter()
        .find(|(column_name, _)| {
            merge_names_equal(column_name, target_name, options.case_sensitive)
        })
        .map(|(_, expr)| expr.clone())
}

fn merge_insert_value(options: &MergeIntoOptions, target_name: &str) -> Expr {
    merge_default_expr(options, target_name).unwrap_or_else(|| lit(ScalarValue::Null))
}

fn align_merge_value(
    value: Expr,
    target_field: &datafusion_common::arrow::datatypes::Field,
    value_schema: &DFSchemaRef,
    options: &MergeIntoOptions,
) -> Result<Expr> {
    let source_type = value.get_type(value_schema)?;
    if &source_type == target_field.data_type() {
        return Ok(cast(value, target_field.data_type().clone()));
    }
    align_row_level_value(
        value,
        &source_type,
        target_field.data_type(),
        options.case_sensitive,
        StructFieldAlignment::FillMissingWithNull,
        &arrow_timezone_label(&options.session_timezone),
    )
}

fn build_insert_only_projection(
    options: &MergeIntoOptions,
    target_schema: &DFSchemaRef,
    value_schema: &DFSchemaRef,
    path_column: &str,
    row_index_column: Option<&str>,
    row_delete_metadata_columns: &[&str],
    operation_expr: Expr,
) -> Result<Vec<Expr>> {
    // Match existing MERGE behavior: clause order determines first-match semantics.
    // Source rows that should not be written flow through as `Noop` rows for metrics.
    let mut projections = Vec::new();

    for field in target_schema.fields().iter() {
        if is_merge_metadata_column(
            field.name(),
            path_column,
            row_index_column,
            row_delete_metadata_columns,
        ) {
            continue;
        }
        let name = field.name().clone();
        let mut branches: Vec<(Expr, Expr)> = Vec::new();

        for clause in &options.not_matched_by_target_clauses {
            let pred = clause
                .condition
                .as_ref()
                .map(|x| x.expr.clone())
                .unwrap_or_else(|| lit(true));
            let value = match &clause.action {
                MergeNotMatchedByTargetAction::InsertAll => merge_source_expr(options, &name)
                    .unwrap_or_else(|| merge_insert_value(options, &name)),
                MergeNotMatchedByTargetAction::InsertColumns { columns, values } => {
                    let mut out = merge_insert_value(options, &name);
                    for (col_name, expr) in columns.iter().zip(values.iter()) {
                        if merge_names_equal(col_name, &name, options.case_sensitive) {
                            out = expr.clone();
                            break;
                        }
                    }
                    out
                }
            };
            branches.push((
                pred,
                align_merge_value(value, field, value_schema, options)?,
            ));
        }

        let when_then_expr = branches
            .into_iter()
            .map(|(p, v)| (Box::new(p), Box::new(v)))
            .collect::<Vec<_>>();

        // Rows not selected by any insert clause become `Noop`, so keep NULL as the data value.
        let expr = Expr::Case(Case {
            expr: None,
            when_then_expr,
            else_expr: Some(Box::new(lit(ScalarValue::Null))),
        });
        projections.push(
            expr.alias_with_metadata(name, Some(FieldMetadata::from(field.metadata().clone()))),
        );
    }

    projections.push(operation_expr.alias(OPERATION_COLUMN));

    Ok(projections)
}

fn build_source_metric_plan(
    source_plan: LogicalPlan,
    writer_schema: &DFSchemaRef,
) -> Result<LogicalPlan> {
    let source_count = Expr::AggregateFunction(AggregateFunction {
        func: count_udaf(),
        params: AggregateFunctionParams {
            args: vec![Expr::Literal(COUNT_STAR_EXPANSION, None)],
            distinct: false,
            filter: None,
            order_by: vec![],
            null_treatment: None,
        },
    })
    .alias(MERGE_SOURCE_METRIC_COLUMN);
    let count_plan = LogicalPlanBuilder::from(source_plan)
        .aggregate(Vec::<Expr>::new(), vec![source_count])?
        .build()?;

    let projections = writer_schema
        .fields()
        .iter()
        .map(|field| {
            let value = match field.name().as_str() {
                OPERATION_COLUMN => lit(RowLevelOperationType::SourceMetric.as_i32()),
                MERGE_SOURCE_METRIC_COLUMN => col(MERGE_SOURCE_METRIC_COLUMN),
                _ => lit(ScalarValue::try_new_null(field.data_type())?),
            };
            Ok(value.alias_with_metadata(
                field.name(),
                Some(FieldMetadata::from(field.metadata().clone())),
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    LogicalPlanBuilder::from(count_plan)
        .project(projections)?
        .build()
}

fn should_check_cardinality(matched_clauses: &[MergeMatchedClause]) -> bool {
    // MERGE semantics: if there are no matched clauses, nothing to check.
    // If there is exactly one matched clause and it is an unconditional DELETE, skip.
    if matched_clauses.is_empty() {
        return false;
    }
    if matched_clauses.len() == 1 {
        let clause = &matched_clauses[0];
        if matches!(clause.action, MergeMatchedAction::Delete) && clause.condition.is_none() {
            return false;
        }
    }
    true
}

fn source_is_unique_on_merge_join_keys(
    source_plan: &LogicalPlan,
    join_key_pairs: &[(Expr, Expr)],
) -> bool {
    let Some(mut key_cols) = source_join_key_column_names(join_key_pairs) else {
        return false;
    };

    // Prefer functional-dependency tracking on the plan schema.
    // If it proves uniqueness, we can safely skip MERGE cardinality checks.
    if schema_implies_unique_for_columns(source_plan.schema().as_ref(), &key_cols) {
        return true;
    }

    let mut plan = source_plan;
    loop {
        match plan {
            LogicalPlan::SubqueryAlias(SubqueryAlias { input, .. }) => {
                plan = input.as_ref();
            }
            LogicalPlan::Filter(Filter { input, .. }) => {
                plan = input.as_ref();
            }
            LogicalPlan::Projection(projection) => {
                let Some(mapped) = map_key_cols_through_projection(projection, &key_cols) else {
                    return false;
                };
                key_cols = mapped;
                plan = projection.input.as_ref();
            }
            LogicalPlan::Aggregate(aggregate) => {
                return aggregate_groups_exactly_on_columns(aggregate, &key_cols);
            }
            _ => return false,
        }
    }
}

fn source_join_key_column_names(join_key_pairs: &[(Expr, Expr)]) -> Option<Vec<String>> {
    // Only handle equi-join keys that are plain source columns.
    let mut out = Vec::with_capacity(join_key_pairs.len());
    for (_, source_key) in join_key_pairs {
        match source_key {
            Expr::Column(c) => out.push(c.name.clone()),
            _ => return None,
        }
    }
    (!out.is_empty()).then_some(out)
}

fn schema_implies_unique_for_columns(schema: &DFSchema, key_cols: &[String]) -> bool {
    use std::collections::{HashMap, HashSet};

    fn norm_ident(s: &str) -> String {
        s.to_ascii_lowercase()
    }

    let mut index_by_name: HashMap<String, usize> = HashMap::new();
    for (idx, field) in schema.fields().iter().enumerate() {
        let name = norm_ident(field.name());
        if index_by_name.insert(name, idx).is_some() {
            return false;
        }
    }

    // Resolve join key column indices. If any key is missing or duplicates
    // another key (after normalization), we cannot reason safely.
    let mut key_indices: HashSet<usize> = HashSet::with_capacity(key_cols.len());
    for key in key_cols {
        let name = norm_ident(key);
        let Some(&idx) = index_by_name.get(&name) else {
            return false;
        };
        if !key_indices.insert(idx) {
            return false;
        }
    }
    if key_indices.is_empty() {
        return false;
    }

    schema.functional_dependencies().iter().any(|fd| {
        fd.mode == Dependency::Single && fd.source_indices.iter().all(|i| key_indices.contains(i))
    })
}

fn map_key_cols_through_projection(
    projection: &Projection,
    key_cols: &[String],
) -> Option<Vec<String>> {
    let mut out = Vec::with_capacity(key_cols.len());
    for key in key_cols {
        let idx = projection
            .schema
            .fields()
            .iter()
            .position(|f| f.name() == key)?;
        let expr = projection.expr.get(idx)?;
        let mapped = match expr {
            Expr::Column(c) => c.name.clone(),
            Expr::Alias(alias) => match alias.expr.as_ref() {
                Expr::Column(c) => c.name.clone(),
                _ => return None,
            },
            _ => return None,
        };
        out.push(mapped);
    }
    Some(out)
}

fn aggregate_groups_exactly_on_columns(aggregate: &Aggregate, key_cols: &[String]) -> bool {
    let mut group_cols: Vec<String> = Vec::with_capacity(aggregate.group_expr.len());
    for expr in aggregate.group_expr.iter() {
        match expr {
            Expr::Column(c) => group_cols.push(c.name.clone()),
            _ => return false,
        }
    }
    same_column_set_case_insensitive(&group_cols, key_cols)
}

fn same_column_set_case_insensitive(a: &[String], b: &[String]) -> bool {
    use std::collections::HashSet;
    if a.len() != b.len() {
        return false;
    }
    let norm = |s: &String| s.to_ascii_lowercase();
    let sa: HashSet<String> = a.iter().map(norm).collect();
    let sb: HashSet<String> = b.iter().map(norm).collect();
    sa.len() == a.len() && sb.len() == b.len() && sa == sb
}

fn combine_conjunction(exprs: &[Expr]) -> Option<Expr> {
    let mut iter = exprs.iter().cloned();
    let first = iter.next()?;
    Some(iter.fold(first, |acc, expr| acc.and(expr)))
}

/// Filter Delta target partitions that cannot satisfy partition-only MERGE conditions.
/// Non-partition predicates must remain join residuals so touched-file rewrites keep
/// target rows that do not satisfy the MERGE condition.
fn filter_merge_target_partitions(
    target_plan: LogicalPlan,
    target_only_predicates: &[Expr],
    partition_columns: &[String],
    has_not_matched_by_source_clauses: bool,
    case_sensitive: bool,
) -> Result<LogicalPlan> {
    // These clauses can act on target rows outside the ON predicate.
    if has_not_matched_by_source_clauses {
        return Ok(target_plan);
    }

    let mut partition_predicates = Vec::new();
    for predicate in target_only_predicates {
        let mut referenced_columns = HashSet::new();
        expr_to_columns(predicate, &mut referenced_columns)?;
        if !referenced_columns.is_empty()
            && referenced_columns.iter().all(|column| {
                partition_columns.iter().any(|partition_column| {
                    merge_names_equal(&column.name, partition_column, case_sensitive)
                })
            })
        {
            partition_predicates.push(predicate.clone());
        }
    }

    let Some(target_filter) = combine_conjunction(&partition_predicates) else {
        return Ok(target_plan);
    };
    LogicalPlanBuilder::from(target_plan)
        .filter(target_filter)?
        .build()
}

fn combine_disjunction(exprs: &[Expr]) -> Option<Expr> {
    let mut iter = exprs.iter().cloned();
    let first = iter.next()?;
    Some(iter.fold(first, |acc, expr| acc.or(expr)))
}

fn build_merge_projection(
    options: &MergeIntoOptions,
    selection: &MergeClauseSelection,
    target_schema: &DFSchemaRef,
    value_schema: &DFSchemaRef,
    path_column: &str,
    row_index_column: Option<&str>,
    row_delete_metadata_columns: &[&str],
) -> Result<MergeProjection> {
    let matched_clause_predicates = selection.predicates(&selection.matched);
    let not_matched_by_source_clause_predicates = selection.predicates(&selection.by_source);
    let not_matched_by_target_clause_predicates = selection.predicates(&selection.inserts);

    let mut cases: HashMap<String, Vec<(Expr, Expr)>> = target_schema
        .fields()
        .iter()
        .filter(|field| {
            !is_merge_metadata_column(
                field.name(),
                path_column,
                row_index_column,
                row_delete_metadata_columns,
            )
        })
        .map(|f| (f.name().clone(), Vec::new()))
        .collect();

    let target_exprs_by_name: HashMap<String, Expr> = target_schema
        .fields()
        .iter()
        .map(|f| (f.name().clone(), Expr::Column(Column::from_name(f.name()))))
        .collect();

    for (clause, pred) in options
        .matched_clauses
        .iter()
        .zip(&matched_clause_predicates)
    {
        match &clause.action {
            MergeMatchedAction::Delete => {}
            MergeMatchedAction::UpdateAll => {
                for field in target_schema.fields().iter() {
                    let value = merge_source_expr(options, field.name())
                        .or_else(|| target_exprs_by_name.get(field.name()).cloned())
                        .unwrap_or_else(|| lit(ScalarValue::Null));
                    if let Some(v) = cases.get_mut(field.name()) {
                        v.push((
                            pred.clone(),
                            align_merge_value(value, field, value_schema, options)?,
                        ));
                    }
                }
            }
            MergeMatchedAction::UpdateSet(assignments) => {
                for assignment in assignments {
                    let resolved = resolve_target_column(
                        assignment.column.as_str(),
                        target_schema,
                        options.case_sensitive,
                    )?;
                    if let Some(v) = cases.get_mut(&resolved) {
                        let field = merge_target_field(target_schema, &resolved)?;
                        v.push((
                            pred.clone(),
                            align_merge_value(
                                assignment.value.clone(),
                                field,
                                value_schema,
                                options,
                            )?,
                        ));
                    }
                }
            }
        }
    }

    for (clause, pred) in options
        .not_matched_by_source_clauses
        .iter()
        .zip(&not_matched_by_source_clause_predicates)
    {
        match &clause.action {
            MergeNotMatchedBySourceAction::Delete => {}
            MergeNotMatchedBySourceAction::UpdateSet(assignments) => {
                for assignment in assignments {
                    let resolved = resolve_target_column(
                        assignment.column.as_str(),
                        target_schema,
                        options.case_sensitive,
                    )?;
                    if let Some(v) = cases.get_mut(&resolved) {
                        let field = merge_target_field(target_schema, &resolved)?;
                        v.push((
                            pred.clone(),
                            align_merge_value(
                                assignment.value.clone(),
                                field,
                                value_schema,
                                options,
                            )?,
                        ));
                    }
                }
            }
        }
    }

    for (clause, pred) in options
        .not_matched_by_target_clauses
        .iter()
        .zip(&not_matched_by_target_clause_predicates)
    {
        match &clause.action {
            MergeNotMatchedByTargetAction::InsertAll => {
                for field in target_schema.fields().iter() {
                    let value = merge_source_expr(options, field.name())
                        .unwrap_or_else(|| merge_insert_value(options, field.name()));
                    if let Some(v) = cases.get_mut(field.name()) {
                        v.push((
                            pred.clone(),
                            align_merge_value(value, field, value_schema, options)?,
                        ));
                    }
                }
            }
            MergeNotMatchedByTargetAction::InsertColumns { columns, values } => {
                for field in target_schema.fields().iter() {
                    if let Some(v) = cases.get_mut(field.name()) {
                        let value = columns
                            .iter()
                            .zip(values.iter())
                            .find(|(column_name, _)| {
                                merge_names_equal(column_name, field.name(), options.case_sensitive)
                            })
                            .map(|(_, value)| value.clone())
                            .unwrap_or_else(|| merge_insert_value(options, field.name()));
                        v.push((
                            pred.clone(),
                            align_merge_value(value, field, value_schema, options)?,
                        ));
                    }
                }
            }
        }
    }

    let mut projections = Vec::new();
    for field in target_schema.fields() {
        if is_merge_metadata_column(
            field.name(),
            path_column,
            row_index_column,
            row_delete_metadata_columns,
        ) {
            continue;
        }
        let name = field.name();
        let default_expr = target_exprs_by_name
            .get(name)
            .cloned()
            .unwrap_or_else(|| lit(ScalarValue::Null));
        let case_branches = cases.remove(name).unwrap_or_default();

        let expr = if case_branches.is_empty() {
            default_expr
        } else {
            let when_then_expr = case_branches
                .into_iter()
                .map(|(pred, value)| (Box::new(pred), Box::new(value)))
                .collect::<Vec<_>>();
            Expr::Case(Case {
                expr: None,
                when_then_expr,
                else_expr: Some(Box::new(default_expr)),
            })
        };

        projections.push(expr.alias_with_metadata(
            name.clone(),
            Some(FieldMetadata::from(field.metadata().clone())),
        ));
    }

    // Preserve the file path column so downstream physical planning can implement
    // targeted rewrite (filter writer input to touched files, while keeping inserts).
    projections.push(col(path_column).alias(path_column.to_string()));
    // A format that supplies delete metadata consumes row deletes from this projection,
    // so keep the row identity next to that metadata for the downstream writer.
    if !row_delete_metadata_columns.is_empty() {
        if let Some(row_index_column) = row_index_column {
            projections.push(col(row_index_column).alias(row_index_column.to_string()));
        }
        projections.extend(
            row_delete_metadata_columns
                .iter()
                .map(|column| col(*column).alias((*column).to_string())),
        );
    }

    // Append the operation type column so downstream writers know per-row intent.
    // Delete rows are preserved as metric-only rows; sinks that rewrite data
    // files must filter them after consuming the tag.
    // TODO: When more row-level sinks consume this projection, keep row intent
    // handling centralized around `OPERATION_COLUMN` instead of deriving it from
    // each writer's local plan shape.
    let insert_op = lit(RowLevelOperationType::Insert.as_i32());
    let copy_op = lit(RowLevelOperationType::Copy.as_i32());
    let noop_op = lit(RowLevelOperationType::Noop.as_i32());
    let mut operation_branches = Vec::new();
    operation_branches.extend(
        options
            .matched_clauses
            .iter()
            .zip(&matched_clause_predicates)
            .map(|(clause, predicate)| {
                let operation = match &clause.action {
                    MergeMatchedAction::UpdateAll | MergeMatchedAction::UpdateSet(_) => {
                        RowLevelOperationType::MatchedUpdate
                    }
                    MergeMatchedAction::Delete => RowLevelOperationType::MatchedDelete,
                };
                (
                    Box::new(predicate.clone()),
                    Box::new(lit(operation.as_i32())),
                )
            }),
    );
    operation_branches.extend(
        options
            .not_matched_by_source_clauses
            .iter()
            .zip(&not_matched_by_source_clause_predicates)
            .map(|(clause, predicate)| {
                let operation = match &clause.action {
                    MergeNotMatchedBySourceAction::UpdateSet(_) => {
                        RowLevelOperationType::NotMatchedBySourceUpdate
                    }
                    MergeNotMatchedBySourceAction::Delete => {
                        RowLevelOperationType::NotMatchedBySourceDelete
                    }
                };
                (
                    Box::new(predicate.clone()),
                    Box::new(lit(operation.as_i32())),
                )
            }),
    );
    operation_branches.extend(
        not_matched_by_target_clause_predicates
            .iter()
            .map(|predicate| (Box::new(predicate.clone()), Box::new(insert_op.clone()))),
    );
    // Source rows that do not select an insert clause carry no write intent.
    operation_branches.push((
        Box::new(col(TARGET_PRESENT_COLUMN).is_null()),
        Box::new(noop_op),
    ));
    let op_expr = Expr::Case(Case {
        expr: None,
        when_then_expr: operation_branches,
        else_expr: Some(Box::new(copy_op)),
    });
    projections.push(op_expr.alias(OPERATION_COLUMN));
    projections.push(lit(ScalarValue::Int64(None)).alias(MERGE_SOURCE_METRIC_COLUMN));

    let mut generated_assignment_markers = Vec::new();
    for (index, (generated_column, _)) in options.generated_column_exprs.iter().enumerate() {
        let mut marker_name = format!("__sail_generated_explicit_{index}");
        while target_schema.has_column_with_unqualified_name(&marker_name) {
            marker_name.push('_');
        }
        let mut branches = Vec::new();
        for (clause, predicate) in options
            .matched_clauses
            .iter()
            .zip(&matched_clause_predicates)
        {
            let explicitly_assigned = match &clause.action {
                MergeMatchedAction::UpdateAll => {
                    merge_source_expr(options, generated_column).is_some()
                }
                MergeMatchedAction::UpdateSet(assignments) => {
                    assignments.iter().any(|assignment| {
                        merge_names_equal(
                            &assignment.column,
                            generated_column,
                            options.case_sensitive,
                        )
                    })
                }
                MergeMatchedAction::Delete => false,
            };
            if explicitly_assigned {
                branches.push((Box::new(predicate.clone()), Box::new(lit(true))));
            }
        }
        for (clause, predicate) in options
            .not_matched_by_source_clauses
            .iter()
            .zip(&not_matched_by_source_clause_predicates)
        {
            if let MergeNotMatchedBySourceAction::UpdateSet(assignments) = &clause.action
                && assignments.iter().any(|assignment| {
                    merge_names_equal(&assignment.column, generated_column, options.case_sensitive)
                })
            {
                branches.push((Box::new(predicate.clone()), Box::new(lit(true))));
            }
        }
        if branches.is_empty() {
            continue;
        }
        let marker = Expr::Case(Case {
            expr: None,
            when_then_expr: branches,
            else_expr: Some(Box::new(lit(false))),
        });
        projections.push(marker.alias(marker_name.clone()));
        generated_assignment_markers.push((generated_column.clone(), marker_name));
    }

    Ok((projections, generated_assignment_markers))
}

fn merge_target_field<'a>(
    target_schema: &'a DFSchemaRef,
    column: &str,
) -> Result<&'a datafusion_common::arrow::datatypes::Field> {
    target_schema
        .fields()
        .iter()
        .find(|field| field.name() == column)
        .map(AsRef::as_ref)
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "unable to resolve column {column} in MERGE target projection"
            ))
        })
}

fn resolve_target_column(
    column: &str,
    target_schema: &DFSchemaRef,
    case_sensitive: bool,
) -> Result<String> {
    let matches = target_schema
        .fields()
        .iter()
        .filter(|field| merge_names_equal(field.name(), column, case_sensitive))
        .collect::<Vec<_>>();
    if matches.len() != 1 {
        return plan_err!("unable to resolve column {column} in MERGE target projection");
    }
    Ok(matches[0].name().to_string())
}

fn rewrite_merge_columns(
    expr: Expr,
    target_map: &HashMap<String, String>,
    source_map: &HashMap<String, String>,
) -> Result<Expr> {
    expr.transform(|expr| {
        if let Expr::Column(col) = &expr
            && let Some(new_name) = target_map
                .get(&col.name)
                .or_else(|| source_map.get(&col.name))
        {
            return Ok(Transformed::yes(Expr::Column(Column {
                relation: None,
                name: new_name.clone(),
                spans: col.spans.clone(),
            })));
        }
        Ok(Transformed::no(expr))
    })
    .map(|t| t.data)
}

fn normalize_target_column_names(
    options: &mut MergeIntoOptions,
    target_map: &HashMap<String, String>,
) {
    let normalize = |name: &str, map: &HashMap<String, String>| {
        map.get(name).cloned().unwrap_or_else(|| name.to_string())
    };

    for (name, _) in &mut options.identity_columns {
        *name = normalize(name, target_map);
    }

    for clause in options.matched_clauses.iter_mut() {
        if let MergeMatchedAction::UpdateSet(assignments) = &mut clause.action {
            for assignment in assignments.iter_mut() {
                assignment.column = normalize(&assignment.column, target_map);
            }
        }
    }

    for clause in options.not_matched_by_source_clauses.iter_mut() {
        if let MergeNotMatchedBySourceAction::UpdateSet(assignments) = &mut clause.action {
            for assignment in assignments.iter_mut() {
                assignment.column = normalize(&assignment.column, target_map);
            }
        }
    }

    for clause in options.not_matched_by_target_clauses.iter_mut() {
        if let MergeNotMatchedByTargetAction::InsertColumns { columns, .. } = &mut clause.action {
            for col in columns.iter_mut() {
                *col = normalize(col, target_map);
            }
        }
    }
}

fn rewrite_clauses<F>(clauses: &mut [MergeMatchedClause], rewrite: &F) -> Result<()>
where
    F: Fn(Expr) -> Result<Expr>,
{
    for clause in clauses.iter_mut() {
        if let Some(cond) = clause.condition.take() {
            clause.condition = Some(ExprWithSource::new(rewrite(cond.expr)?, cond.source));
        }
        if let MergeMatchedAction::UpdateSet(assignments) = &mut clause.action {
            for assignment in assignments.iter_mut() {
                assignment.value = rewrite(assignment.value.clone())?;
            }
        }
    }
    Ok(())
}

fn rewrite_not_matched_by_source<F>(
    clauses: &mut [MergeNotMatchedBySourceClause],
    rewrite: &F,
) -> Result<()>
where
    F: Fn(Expr) -> Result<Expr>,
{
    for clause in clauses.iter_mut() {
        if let Some(cond) = clause.condition.take() {
            clause.condition = Some(ExprWithSource::new(rewrite(cond.expr)?, cond.source));
        }
        if let MergeNotMatchedBySourceAction::UpdateSet(assignments) = &mut clause.action {
            for assignment in assignments.iter_mut() {
                assignment.value = rewrite(assignment.value.clone())?;
            }
        }
    }
    Ok(())
}

fn rewrite_not_matched_by_target<F>(
    clauses: &mut [MergeNotMatchedByTargetClause],
    rewrite: &F,
) -> Result<()>
where
    F: Fn(Expr) -> Result<Expr>,
{
    for clause in clauses.iter_mut() {
        if let Some(cond) = clause.condition.take() {
            clause.condition = Some(ExprWithSource::new(rewrite(cond.expr)?, cond.source));
        }
        if let MergeNotMatchedByTargetAction::InsertColumns { values, .. } = &mut clause.action {
            for value in values.iter_mut() {
                *value = rewrite(value.clone())?;
            }
        }
    }
    Ok(())
}

/// Try to recover meaningful field names from a logical plan by walking its inputs
/// until we find a schema whose fields are not all placeholder names like "#0".
/// The recovered schema must have the same number of fields as the top-level plan
/// to ensure a 1:1 mapping between opaque IDs and real names.
fn recover_field_names(plan: &LogicalPlan, path_column: &str) -> Option<Vec<String>> {
    let expected_len = plan.schema().fields().len();
    let mut queue = VecDeque::new();
    queue.push_back(plan);
    while let Some(p) = queue.pop_front() {
        let schema = p.schema();
        if !all_placeholder_schema(schema, path_column) && schema.fields().len() == expected_len {
            return Some(schema.fields().iter().map(|f| f.name().clone()).collect());
        }
        queue.extend(p.inputs());
    }
    None
}

// TODO: Plan resolver might need to provide utilities for working with "resolved" opaque field names.
// The need to work with the original schema in this file indicates limitations in the current plan resolver design.
// The merge operation would become a good example for future improvements on the plan resolver.
fn all_placeholder_schema(schema: &DFSchemaRef, path_column: &str) -> bool {
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    let non_path: Vec<&str> = names
        .iter()
        .copied()
        .filter(|name| *name != path_column)
        .collect();
    !non_path.is_empty() && non_path.iter().all(|name| name.starts_with('#'))
}

fn change_data_columns(
    schema: &DFSchemaRef,
    path_column: &str,
    row_index_column: Option<&str>,
    metadata: &[&str],
) -> Vec<Expr> {
    schema
        .fields()
        .iter()
        .filter(|field| {
            !is_merge_metadata_column(field.name(), path_column, row_index_column, metadata)
        })
        .map(|field| Expr::Column(Column::from_name(field.name())))
        .collect()
}

fn change_data_update_operation() -> Expr {
    col(OPERATION_COLUMN)
        .eq(lit(RowLevelOperationType::MatchedUpdate.as_i32()))
        .or(col(OPERATION_COLUMN).eq(lit(RowLevelOperationType::NotMatchedBySourceUpdate.as_i32())))
}

fn change_data_postimages(
    plan: LogicalPlan,
    schema: &DFSchemaRef,
    path_column: &str,
    row_index_column: Option<&str>,
    metadata: &[&str],
) -> Result<LogicalPlan> {
    let insert = col(OPERATION_COLUMN).eq(lit(RowLevelOperationType::Insert.as_i32()));
    let mut fields = change_data_columns(schema, path_column, row_index_column, metadata);
    fields.push(
        when(insert.clone(), lit("insert"))
            .otherwise(lit("update_postimage"))?
            .alias("_change_type"),
    );
    LogicalPlanBuilder::from(plan)
        .filter(insert.or(change_data_update_operation()))?
        .project(fields)?
        .build()
}
