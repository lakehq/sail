use std::collections::{HashMap, VecDeque};
use std::fmt::Formatter;
use std::hash::Hash;
use std::sync::Arc;

use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{
    plan_err, Column, DFSchema, DFSchemaRef, DataFusionError, Dependency, NullEquality, Result,
    ScalarValue, TableReference,
};
use datafusion_expr::expr::Case;
use datafusion_expr::expr_fn::not;
use datafusion_expr::logical_plan::{
    Aggregate, Extension, Filter, LogicalPlanBuilder, Projection, SubqueryAlias,
};
use datafusion_expr::utils::expr_to_columns;
use datafusion_expr::{
    col, lit, when, BinaryExpr, Expr, Join, JoinConstraint, JoinType, LogicalPlan, Operator,
    ScalarUDF, UserDefinedLogicalNodeCore,
};
use educe::Educe;
use log::trace;
use sail_common_datafusion::logical_expr::ExprWithSource;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::misc::raise_error::RaiseError;

use crate::monotonic_id::MonotonicIdNode;

pub const SOURCE_PRESENT_COLUMN: &str = "__sail_merge_source_row_present";
pub const TARGET_PRESENT_COLUMN: &str = "__sail_merge_target_row_present";
pub const TARGET_ROW_ID_COLUMN: &str = "__sail_merge_target_row_id";

pub use sail_common_datafusion::datasource::OPERATION_COLUMN;
use sail_common_datafusion::datasource::{OptionLayer, RowLevelOperationType};

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

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct MergeIntoOptions {
    pub target_alias: Option<String>,
    pub source_alias: Option<String>,
    pub target: MergeTargetInfo,
    pub with_schema_evolution: bool,
    /// Resolved logical schemas from analysis time (before any rewrites)
    pub resolved_target_schema: DFSchemaRef,
    pub resolved_source_schema: DFSchemaRef,
    /// User-facing field names for target and source, resolved from opaque IDs
    /// at plan resolution time. Used by `expand_merge` to map opaque IDs back
    /// to real column names without the fragile `recover_field_names` heuristic.
    pub resolved_target_field_names: Vec<String>,
    pub resolved_source_field_names: Vec<String>,
    pub on_condition: ExprWithSource,
    pub matched_clauses: Vec<MergeMatchedClause>,
    pub not_matched_by_source_clauses: Vec<MergeNotMatchedBySourceClause>,
    pub not_matched_by_target_clauses: Vec<MergeNotMatchedByTargetClause>,
    /// Pre-analyzed join equality keys extracted from the ON condition (target, source)
    pub join_key_pairs: Vec<(Expr, Expr)>,
    /// Residual predicates from the ON condition that are not equality join keys
    pub residual_predicates: Vec<Expr>,
    /// Predicates from ON that only touch target columns (useful for early pruning)
    pub target_only_predicates: Vec<Expr>,
    /// Generation expressions for generated columns in the target table.
    /// Each entry is `(column_name, resolved_expr)` where `resolved_expr` initially
    /// references target schema field IDs and is rewritten to actual column names
    /// by `expand_merge` before being applied as a post-processing projection.
    pub generated_column_exprs: Vec<(String, Expr)>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd)]
pub struct MergeTargetInfo {
    pub table_name: Vec<String>,
    pub format: String,
    pub location: String,
    pub partition_by: Vec<String>,
    pub options: Vec<OptionLayer>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct MergeMatchedClause {
    pub condition: Option<ExprWithSource>,
    pub action: MergeMatchedAction,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum MergeMatchedAction {
    Delete,
    UpdateAll,
    UpdateSet(Vec<MergeAssignment>),
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct MergeNotMatchedBySourceClause {
    pub condition: Option<ExprWithSource>,
    pub action: MergeNotMatchedBySourceAction,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum MergeNotMatchedBySourceAction {
    Delete,
    UpdateSet(Vec<MergeAssignment>),
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct MergeNotMatchedByTargetClause {
    pub condition: Option<ExprWithSource>,
    pub action: MergeNotMatchedByTargetAction,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum MergeNotMatchedByTargetAction {
    InsertAll,
    InsertColumns {
        columns: Vec<String>,
        values: Vec<Expr>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct MergeAssignment {
    pub column: String,
    pub value: Expr,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct MergeIntoNode {
    target: Arc<LogicalPlan>,
    source: Arc<LogicalPlan>,
    #[educe(PartialOrd(ignore))]
    options: MergeIntoOptions,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
    #[educe(PartialOrd(ignore))]
    input_schema: DFSchemaRef,
}

impl MergeIntoNode {
    pub fn new(
        target: Arc<LogicalPlan>,
        source: Arc<LogicalPlan>,
        options: MergeIntoOptions,
        input_schema: DFSchemaRef,
    ) -> Self {
        Self {
            target,
            source,
            options,
            schema: Arc::new(DFSchema::empty()),
            input_schema,
        }
    }

    pub fn options(&self) -> &MergeIntoOptions {
        &self.options
    }

    pub fn target(&self) -> &Arc<LogicalPlan> {
        &self.target
    }

    pub fn source(&self) -> &Arc<LogicalPlan> {
        &self.source
    }

    pub fn input_schema(&self) -> &DFSchemaRef {
        &self.input_schema
    }
}

impl UserDefinedLogicalNodeCore for MergeIntoNode {
    fn name(&self) -> &str {
        "MergeInto"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.target.as_ref(), self.source.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "MergeInto: options={:?}", self.options)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Self> {
        exprs.zero()?;
        let (target, source) = inputs.two()?;
        Ok(Self {
            target: Arc::new(target),
            source: Arc::new(source),
            options: self.options.clone(),
            schema: self.schema.clone(),
            input_schema: self.input_schema.clone(),
        })
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        None
    }
}

use sail_common_datafusion::datasource::RowLevelCommand;

/// Unified post-expansion node for row-level operations (DELETE, UPDATE, MERGE).
///
/// For MERGE: `write_plan` and `touched_files_plan` are populated by the optimizer.
/// For DELETE: `condition` is carried through; the physical planner builds the full plan.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct RowLevelWriteNode {
    command: RowLevelCommand,
    raw_target: Arc<LogicalPlan>,
    raw_source: Option<Arc<LogicalPlan>>,
    #[educe(PartialOrd(ignore))]
    raw_input_schema: DFSchemaRef,
    /// Expanded write plan (MERGE).
    write_plan: Option<Arc<LogicalPlan>>,
    /// Plan yielding touched file paths (MERGE targeted rewrite).
    touched_files_plan: Option<Arc<LogicalPlan>>,
    /// Condition for DELETE/UPDATE (passed through to physical planner).
    #[educe(PartialOrd(ignore))]
    condition: Option<ExprWithSource>,
    #[educe(PartialOrd(ignore))]
    merge_options: Option<MergeIntoOptions>,
    target_format: String,
    target_location: String,
    target_table_name: Vec<String>,
    target_partition_by: Vec<String>,
    target_options: Vec<OptionLayer>,
    with_schema_evolution: bool,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
}

impl RowLevelWriteNode {
    /// Create a MERGE write node from expansion results.
    pub fn new_merge(
        raw_target: Arc<LogicalPlan>,
        raw_source: Arc<LogicalPlan>,
        raw_input_schema: DFSchemaRef,
        write_plan: Arc<LogicalPlan>,
        touched_files_plan: Arc<LogicalPlan>,
        options: MergeIntoOptions,
        schema: DFSchemaRef,
    ) -> Self {
        Self {
            command: RowLevelCommand::Merge,
            target_format: options.target.format.clone(),
            target_location: options.target.location.clone(),
            target_table_name: options.target.table_name.clone(),
            target_partition_by: options.target.partition_by.clone(),
            target_options: options.target.options.clone(),
            with_schema_evolution: options.with_schema_evolution,
            raw_target,
            raw_source: Some(raw_source),
            raw_input_schema,
            write_plan: Some(write_plan),
            touched_files_plan: Some(touched_files_plan),
            condition: None,
            merge_options: Some(options),
            schema,
        }
    }

    /// Create a DELETE write node carrying the condition for the physical planner.
    pub fn new_delete(
        raw_target: Arc<LogicalPlan>,
        raw_input_schema: DFSchemaRef,
        condition: Option<ExprWithSource>,
        format: String,
        location: String,
        table_name: Vec<String>,
        options: Vec<OptionLayer>,
    ) -> Self {
        Self {
            command: RowLevelCommand::Delete,
            raw_target,
            raw_source: None,
            raw_input_schema,
            write_plan: None,
            touched_files_plan: None,
            condition,
            merge_options: None,
            target_format: format,
            target_location: location,
            target_table_name: table_name,
            target_partition_by: Vec::new(),
            target_options: options,
            with_schema_evolution: false,
            schema: Arc::new(DFSchema::empty()),
        }
    }

    pub fn command(&self) -> RowLevelCommand {
        self.command
    }

    pub fn merge_options(&self) -> Option<&MergeIntoOptions> {
        self.merge_options.as_ref()
    }

    pub fn write_plan(&self) -> Option<&Arc<LogicalPlan>> {
        self.write_plan.as_ref()
    }

    pub fn raw_target(&self) -> &Arc<LogicalPlan> {
        &self.raw_target
    }

    pub fn raw_source(&self) -> Option<&Arc<LogicalPlan>> {
        self.raw_source.as_ref()
    }

    pub fn raw_input_schema(&self) -> &DFSchemaRef {
        &self.raw_input_schema
    }

    pub fn touched_files_plan(&self) -> Option<&Arc<LogicalPlan>> {
        self.touched_files_plan.as_ref()
    }

    pub fn condition(&self) -> Option<&ExprWithSource> {
        self.condition.as_ref()
    }

    pub fn target_format(&self) -> &str {
        &self.target_format
    }

    pub fn target_location(&self) -> &str {
        &self.target_location
    }

    pub fn target_table_name(&self) -> &[String] {
        &self.target_table_name
    }

    pub fn target_partition_by(&self) -> &[String] {
        &self.target_partition_by
    }

    pub fn target_options(&self) -> &[OptionLayer] {
        &self.target_options
    }

    pub fn with_schema_evolution(&self) -> bool {
        self.with_schema_evolution
    }
}

impl UserDefinedLogicalNodeCore for RowLevelWriteNode {
    fn name(&self) -> &str {
        "RowLevelWrite"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        let mut inputs = Vec::new();
        if let Some(wp) = &self.write_plan {
            inputs.push(wp.as_ref());
        }
        if let Some(tp) = &self.touched_files_plan {
            inputs.push(tp.as_ref());
        }
        inputs
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        let table = self
            .target_table_name
            .last()
            .map(|s| s.as_str())
            .unwrap_or(&self.target_location);
        write!(
            f,
            "RowLevelWrite: command={:?}, target={}, format={}",
            self.command, table, self.target_format
        )?;
        match self.command {
            RowLevelCommand::Delete => {
                if let Some(cond) = self.condition.as_ref().and_then(|c| c.source.as_deref()) {
                    write!(f, ", condition={}", cond.trim())?;
                }
            }
            RowLevelCommand::Merge => {
                if let Some(opts) = &self.merge_options {
                    write!(
                        f,
                        ", matched={}, not_matched={}, not_matched_by_source={}",
                        opts.matched_clauses.len(),
                        opts.not_matched_by_target_clauses.len(),
                        opts.not_matched_by_source_clauses.len()
                    )?;
                }
            }
            RowLevelCommand::Update => {}
        }
        Ok(())
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Self> {
        exprs.zero()?;
        let mut iter = inputs.into_iter();
        let write_plan = if self.write_plan.is_some() {
            Some(Arc::new(iter.next().ok_or_else(|| {
                DataFusionError::Internal("RowLevelWriteNode: missing write_plan input".into())
            })?))
        } else {
            None
        };
        let touched_files_plan = if self.touched_files_plan.is_some() {
            Some(Arc::new(iter.next().ok_or_else(|| {
                DataFusionError::Internal(
                    "RowLevelWriteNode: missing touched_files_plan input".into(),
                )
            })?))
        } else {
            None
        };
        Ok(Self {
            command: self.command,
            raw_target: self.raw_target.clone(),
            raw_source: self.raw_source.clone(),
            raw_input_schema: self.raw_input_schema.clone(),
            write_plan,
            touched_files_plan,
            condition: self.condition.clone(),
            merge_options: self.merge_options.clone(),
            target_format: self.target_format.clone(),
            target_location: self.target_location.clone(),
            target_table_name: self.target_table_name.clone(),
            target_partition_by: self.target_partition_by.clone(),
            target_options: self.target_options.clone(),
            with_schema_evolution: self.with_schema_evolution,
            schema: self.schema.clone(),
        })
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        None
    }
}

#[derive(Clone, Debug)]
pub struct MergeExpansion {
    pub write_plan: LogicalPlan,
    pub touched_files_plan: LogicalPlan,
    pub output_schema: DFSchemaRef,
    pub options: MergeIntoOptions,
}

pub fn expand_merge(node: &MergeIntoNode, path_column: &str) -> Result<MergeExpansion> {
    let target_plan = node.target.as_ref().clone();
    let source_plan = node.source.as_ref().clone();
    let mut options = node.options().clone();
    let merge_schema = node.input_schema.clone();
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
    trace!("resolved target names: {:?}", &desired_target_names);
    trace!("resolved source names: {:?}", &desired_source_names);

    let _target_relation = node
        .options()
        .target_alias
        .as_ref()
        .map(|a| TableReference::Bare {
            table: a.clone().into(),
        });
    let source_relation = node
        .options()
        .source_alias
        .as_ref()
        .map(|a| TableReference::Bare {
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
        &target_scan_fields
    );

    let mut target_proj_exprs: Vec<Expr> = target_plan
        .schema()
        .fields()
        .iter()
        .zip(desired_target_names.iter())
        .map(|(field, desired)| {
            // Use unqualified column to avoid alias-mismatch when upstream qualifiers differ.
            Expr::Column(Column::from_name(field.name().clone())).alias(desired.clone())
        })
        .collect();

    // Ensure file path column (if present) is preserved even when desired_target_names was shorter.
    // Always project the file path column to keep it available downstream.
    let already_present = target_proj_exprs
        .iter()
        .any(|expr| matches!(expr, Expr::Alias(alias) if alias.name == path_column));
    if !already_present {
        target_proj_exprs.push(
            Expr::Column(Column::from_name(path_column.to_string())).alias(path_column.to_string()),
        );
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

    if should_check_cardinality {
        // Add stable per-target-row id before join; JOIN will duplicate this value for matches.
        // Use a dedicated logical node so we don't rely on later expression rewriters (MERGE builds plans directly).
        target_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(MonotonicIdNode::try_new(
                Arc::new(target_plan),
                TARGET_ROW_ID_COLUMN.to_string(),
            )?),
        });
    }

    // To avoid duplicate unqualified names after JOIN, rename source columns with a stable prefix.
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
    // keep row id stable if present
    target_rename_map.insert(
        TARGET_ROW_ID_COLUMN.to_string(),
        TARGET_ROW_ID_COLUMN.to_string(),
    );

    let mut source_rename_map: HashMap<String, String> = HashMap::new();
    let target_input_len = options.resolved_target_schema.fields().len();
    for (idx, desired) in desired_source_names.iter().enumerate() {
        let prefixed = format!("__sail_src_{desired}");
        source_rename_map.insert(desired.clone(), prefixed.clone());
        if let Some(field) = merge_schema.fields().get(target_input_len + idx) {
            source_rename_map.insert(field.name().clone(), prefixed.clone());
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
    trace!(
        "expand_merge options after rewrite - join_key_pairs: {:?}, matched_clauses: {:?}, not_matched_by_source_clauses: {:?}, not_matched_by_target_clauses: {:?}, on_condition: {:?}",
        &options.join_key_pairs,
        &options.matched_clauses,
        &options.not_matched_by_source_clauses,
        &options.not_matched_by_target_clauses,
        &options.on_condition
    );

    // Detect insert-only MERGE that can use fast append (anti-join + no touched files).
    let can_fast_append = can_fast_append_insert_only(&options, target_schema, path_column)?;

    if can_fast_append {
        // source ANTI target
        let join_on = options
            .join_key_pairs
            .iter()
            .map(|(t, s)| Ok((s.clone(), t.clone())))
            .collect::<Result<Vec<_>>>()?;
        let residual_filter = combine_conjunction(&options.residual_predicates);

        let join = Join::try_new(
            Arc::new(source_plan.clone()),
            Arc::new(target_plan.clone()),
            join_on,
            residual_filter,
            JoinType::LeftAnti,
            JoinConstraint::On,
            NullEquality::NullEqualsNothing,
            false,
        )?;
        let join = Arc::new(LogicalPlan::Join(join));

        // Filter rows that do not match any NOT MATCHED BY TARGET clause conditions.
        let insert_filter = insert_only_insert_filter(&options);
        let filtered = LogicalPlanBuilder::from(join.as_ref().clone())
            .filter(insert_filter)?
            .build()?;

        let projection_exprs =
            build_insert_only_projection(&options, target_schema, source_schema, path_column)?;
        let projected = LogicalPlanBuilder::from(filtered)
            .project(projection_exprs)?
            .build()?;
        let projected = apply_generation_projection(projected, &options.generated_column_exprs)?;

        let touched_plan = LogicalPlanBuilder::empty(false).build()?;
        let command_schema = Arc::new(DFSchema::empty());
        return Ok(MergeExpansion {
            write_plan: projected,
            touched_files_plan: touched_plan,
            output_schema: command_schema,
            options,
        });
    }

    build_default_merge_expansion(
        options,
        target_plan,
        source_plan,
        should_check_cardinality,
        path_column,
    )
}

/// Default MERGE expansion: full outer join + presence columns + touched files.
fn build_default_merge_expansion(
    options: MergeIntoOptions,
    target_plan: LogicalPlan,
    source_plan: LogicalPlan,
    should_check_cardinality: bool,
    path_column: &str,
) -> Result<MergeExpansion> {
    let target_schema = target_plan.schema();
    let source_schema = source_plan.schema();

    let augmented_target = LogicalPlanBuilder::from(target_plan.clone())
        .project(append_presence_projection(
            target_schema,
            TARGET_PRESENT_COLUMN,
            Some(path_column),
        )?)?
        .build()?;

    let augmented_source = LogicalPlanBuilder::from(source_plan.clone())
        .project(append_presence_projection(
            source_schema,
            SOURCE_PRESENT_COLUMN,
            None,
        )?)?
        .build()?;

    let join_on = options.join_key_pairs.clone();
    let residual_filter = combine_conjunction(&options.residual_predicates);

    let join = Join::try_new(
        Arc::new(augmented_target),
        Arc::new(augmented_source),
        join_on,
        residual_filter,
        JoinType::Full,
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

    let target_present = col(TARGET_PRESENT_COLUMN).is_not_null();
    let source_present = col(SOURCE_PRESENT_COLUMN).is_not_null();

    let matched_pred = target_present.clone().and(source_present.clone());
    let not_matched_by_source_pred = target_present.clone().and(not(source_present.clone()));
    let not_matched_by_target_pred = not(target_present.clone()).and(source_present.clone());

    let mut delete_pred: Option<Expr> = None;
    let mut insert_pred: Option<Expr> = None;

    for clause in &options.matched_clauses {
        let mut pred = matched_pred.clone();
        if let Some(cond) = &clause.condition {
            pred = pred.and(cond.expr.clone());
        }
        match clause.action {
            MergeMatchedAction::Delete => {
                delete_pred = or_pred(delete_pred, pred);
            }
            MergeMatchedAction::UpdateAll | MergeMatchedAction::UpdateSet(_) => {}
        }
    }

    for clause in &options.not_matched_by_source_clauses {
        let mut pred = not_matched_by_source_pred.clone();
        if let Some(cond) = &clause.condition {
            pred = pred.and(cond.expr.clone());
        }
        match clause.action {
            MergeNotMatchedBySourceAction::Delete => {
                delete_pred = or_pred(delete_pred, pred);
            }
            MergeNotMatchedBySourceAction::UpdateSet(_) => {}
        }
    }

    for clause in &options.not_matched_by_target_clauses {
        let mut pred = not_matched_by_target_pred.clone();
        if let Some(cond) = &clause.condition {
            pred = pred.and(cond.expr.clone());
        }
        match clause.action {
            MergeNotMatchedByTargetAction::InsertAll
            | MergeNotMatchedByTargetAction::InsertColumns { .. } => {
                insert_pred = or_pred(insert_pred, pred);
            }
        }
    }

    let delete_expr = delete_pred.unwrap_or_else(|| lit(false));
    let insert_expr = insert_pred.unwrap_or_else(|| lit(false));
    let active_expr = target_present.and(not(delete_expr)).or(insert_expr);

    let filtered = LogicalPlanBuilder::from(join.as_ref().clone())
        .filter(active_expr)?
        .build()?;

    let projection_exprs =
        build_merge_projection(&options, target_schema, source_schema, path_column)?;
    trace!("projection exprs: {:?}", &projection_exprs);
    let projected = LogicalPlanBuilder::from(filtered)
        .project(projection_exprs.clone())?
        .build()?;
    let projected = apply_generation_projection(projected, &options.generated_column_exprs)?;

    let (rewrite_matched, rewrite_not_matched_by_source) =
        build_rewrite_predicates(&options, &matched_pred, &not_matched_by_source_pred);
    let rewrite_filter = combine_rewrite_preds(rewrite_matched, rewrite_not_matched_by_source);

    let touched_plan = LogicalPlanBuilder::from(join.as_ref().clone())
        .filter(rewrite_filter.unwrap_or_else(|| lit(false)))?
        .aggregate(vec![col(path_column)], Vec::<Expr>::new())?
        .project(vec![col(path_column).alias(path_column.to_string())])?
        .build()?;

    let command_schema = Arc::new(DFSchema::empty());

    Ok(MergeExpansion {
        write_plan: projected.clone(),
        touched_files_plan: touched_plan,
        output_schema: command_schema,
        options,
    })
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

    if let Some(path_name) = path_column {
        if schema.index_of_column_by_name(None, path_name).is_none() {
            let path_expr = lit(ScalarValue::Utf8(None));
            exprs.push(path_expr.alias(path_name.to_string()));
        }
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
        if let Some(cond) = &clause.condition {
            if references_target(&cond.expr)? {
                return Ok(false);
            }
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

fn insert_only_insert_filter(options: &MergeIntoOptions) -> Expr {
    let preds = options
        .not_matched_by_target_clauses
        .iter()
        .map(|c| {
            c.condition
                .as_ref()
                .map(|x| x.expr.clone())
                .unwrap_or_else(|| lit(true))
        })
        .collect::<Vec<_>>();
    combine_disjunction(&preds).unwrap_or_else(|| lit(false))
}

fn apply_generation_projection(
    plan: LogicalPlan,
    generated_column_exprs: &[(String, Expr)],
) -> Result<LogicalPlan> {
    if generated_column_exprs.is_empty() {
        return Ok(plan);
    }
    let gen_map: HashMap<&str, &Expr> = generated_column_exprs
        .iter()
        .map(|(name, expr)| (name.as_str(), expr))
        .collect();
    let schema = plan.schema().clone();
    let has_op_col = schema.has_column_with_unqualified_name(OPERATION_COLUMN);
    let insert_op_val = lit(RowLevelOperationType::Insert.as_i32());
    let post_exprs: Vec<Expr> = schema
        .fields()
        .iter()
        .map(|f| {
            let name = f.name();
            if let Some(gen_expr) = gen_map.get(name.as_str()) {
                let gen_expr = (*gen_expr).clone();
                let current_value = col(name.clone());
                // For INSERT rows, enforce Delta protocol: if the user explicitly provided a
                // non-NULL value for the generated column that doesn't match the expression,
                // raise an error instead of silently overwriting.
                //
                // For UPDATE rows, the generated column's current value is stale (from the
                // existing target row) — always silently recompute from the expression.
                //
                // We distinguish INSERT from UPDATE via the operation column when available.
                let mismatch_check =
                    current_value
                        .clone()
                        .is_null()
                        .or(Expr::BinaryExpr(BinaryExpr::new(
                            Box::new(current_value),
                            Operator::IsNotDistinctFrom,
                            Box::new(gen_expr.clone()),
                        )));
                let err_msg = format!(
                    "[DELTA_GENERATED_COLUMNS_VALUE_MISMATCH] \
                     CHECK constraint for generated column `{name}` violated: \
                     user-provided value does not match the generation expression."
                );
                let raise = ScalarUDF::from(RaiseError::new()).call(vec![lit(err_msg)]);
                let enforced = when(mismatch_check, gen_expr.clone())
                    .otherwise(raise)
                    .map(|e| e.alias(name.clone()))?;
                if has_op_col {
                    // Only enforce for INSERT operations; UPDATE always recomputes silently.
                    when(col(OPERATION_COLUMN).eq(insert_op_val.clone()), enforced)
                        .otherwise(gen_expr.clone())
                        .map(|e| e.alias(name.clone()))
                } else {
                    // Insert-only path (fast-append): always enforce.
                    Ok(enforced)
                }
            } else {
                Ok(col(name.clone()))
            }
        })
        .collect::<Result<Vec<Expr>>>()?;
    LogicalPlanBuilder::from(plan).project(post_exprs)?.build()
}

fn build_insert_only_projection(
    options: &MergeIntoOptions,
    target_schema: &DFSchemaRef,
    source_schema: &DFSchemaRef,
    path_column: &str,
) -> Result<Vec<Expr>> {
    // Match existing MERGE behavior: produce one output row per inserted source row,
    // with clause order determining first-match semantics.
    let mut projections = Vec::new();

    // Source columns are prefixed with `__sail_src_`, so target field "id" maps
    // to source column "__sail_src_id". Keys are lowercased for case-insensitive
    // resolution (Spark's default).
    let source_exprs_by_name: HashMap<String, Expr> = source_schema
        .fields()
        .iter()
        .map(|f| {
            (
                f.name().to_ascii_lowercase(),
                Expr::Column(Column::from_name(f.name().clone())),
            )
        })
        .collect();

    for field in target_schema.fields().iter() {
        if field.name() == path_column || field.name() == TARGET_ROW_ID_COLUMN {
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
                MergeNotMatchedByTargetAction::InsertAll => source_exprs_by_name
                    .get(&format!("__sail_src_{}", name.to_ascii_lowercase()))
                    .cloned()
                    .unwrap_or_else(|| lit(ScalarValue::Null)),
                MergeNotMatchedByTargetAction::InsertColumns { columns, values } => {
                    // If column not specified in this clause, it becomes NULL for this clause
                    // (and must NOT fall through to later clauses).
                    let mut out = lit(ScalarValue::Null);
                    for (col_name, expr) in columns.iter().zip(values.iter()) {
                        if col_name.eq_ignore_ascii_case(&name) {
                            out = expr.clone();
                            break;
                        }
                    }
                    out
                }
            };
            branches.push((pred, value));
        }

        let when_then_expr = branches
            .into_iter()
            .map(|(p, v)| (Box::new(p), Box::new(v)))
            .collect::<Vec<_>>();

        // Rows are pre-filtered by insert_only_insert_filter, but keep an else NULL to be safe.
        let expr = Expr::Case(Case {
            expr: None,
            when_then_expr,
            else_expr: Some(Box::new(lit(ScalarValue::Null))),
        });
        projections.push(expr.alias(name));
    }

    // Insert-only path: all rows are INSERTs.
    projections.push(lit(RowLevelOperationType::Insert.as_i32()).alias(OPERATION_COLUMN));

    Ok(projections)
}

fn should_check_cardinality(matched_clauses: &[MergeMatchedClause]) -> bool {
    // Spark semantics: If there are no matched clauses, nothing to check.
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

fn combine_disjunction(exprs: &[Expr]) -> Option<Expr> {
    let mut iter = exprs.iter().cloned();
    let first = iter.next()?;
    Some(iter.fold(first, |acc, expr| acc.or(expr)))
}

fn or_pred(existing: Option<Expr>, expr: Expr) -> Option<Expr> {
    Some(match existing {
        Some(prev) => prev.or(expr),
        None => expr,
    })
}

fn build_merge_projection(
    options: &MergeIntoOptions,
    target_schema: &DFSchemaRef,
    source_schema: &DFSchemaRef,
    path_column: &str,
) -> Result<Vec<Expr>> {
    let mut cases: Vec<(String, Vec<(Expr, Expr)>)> = target_schema
        .fields()
        .iter()
        .filter(|f| f.name() != path_column && f.name() != TARGET_ROW_ID_COLUMN)
        .map(|f| (f.name().clone(), Vec::new()))
        .collect();

    let mut target_exprs = Vec::new();
    for field in target_schema.fields() {
        target_exprs.push(Expr::Column(Column::from_name(field.name().clone())));
    }

    let source_exprs_by_name: HashMap<String, Expr> = source_schema
        .fields()
        .iter()
        .map(|f| {
            (
                f.name().to_ascii_lowercase(),
                Expr::Column(Column::from_name(f.name().clone())),
            )
        })
        .collect();

    // Find the source expression that corresponds to a target field by name.
    // Source columns are prefixed with `__sail_src_`, so target field "id"
    // maps to source column "__sail_src_id". Keys are lowercased for
    // case-insensitive resolution (Spark's default).
    let source_expr_for_target = |target_name: &str| -> Expr {
        let prefixed = format!("__sail_src_{}", target_name.to_ascii_lowercase());
        source_exprs_by_name
            .get(&prefixed)
            .cloned()
            .unwrap_or_else(|| lit(ScalarValue::Null))
    };

    for clause in &options.matched_clauses {
        let mut pred = col(TARGET_PRESENT_COLUMN)
            .is_not_null()
            .and(col(SOURCE_PRESENT_COLUMN).is_not_null());
        if let Some(cond) = &clause.condition {
            pred = pred.and(cond.expr.clone());
        }
        match &clause.action {
            MergeMatchedAction::Delete => {}
            MergeMatchedAction::UpdateAll => {
                for field in target_schema.fields().iter() {
                    let value = source_expr_for_target(field.name());
                    if let Some(entry) = cases.iter_mut().find(|(name, _)| name == field.name()) {
                        entry.1.push((pred.clone(), value));
                    }
                }
            }
            MergeMatchedAction::UpdateSet(assignments) => {
                for assignment in assignments {
                    let resolved =
                        resolve_target_column(assignment.column.as_str(), target_schema)?;
                    if let Some(entry) = cases.iter_mut().find(|(name, _)| name == &resolved) {
                        entry.1.push((pred.clone(), assignment.value.clone()));
                    }
                }
            }
        }
    }

    for clause in &options.not_matched_by_source_clauses {
        let mut pred = col(TARGET_PRESENT_COLUMN)
            .is_not_null()
            .and(col(SOURCE_PRESENT_COLUMN).is_null());
        if let Some(cond) = &clause.condition {
            pred = pred.and(cond.expr.clone());
        }
        match &clause.action {
            MergeNotMatchedBySourceAction::Delete => {}
            MergeNotMatchedBySourceAction::UpdateSet(assignments) => {
                for assignment in assignments {
                    let resolved =
                        resolve_target_column(assignment.column.as_str(), target_schema)?;
                    if let Some(entry) = cases.iter_mut().find(|(name, _)| name == &resolved) {
                        entry.1.push((pred.clone(), assignment.value.clone()));
                    }
                }
            }
        }
    }

    for clause in &options.not_matched_by_target_clauses {
        let mut pred = col(TARGET_PRESENT_COLUMN)
            .is_null()
            .and(col(SOURCE_PRESENT_COLUMN).is_not_null());
        if let Some(cond) = &clause.condition {
            pred = pred.and(cond.expr.clone());
        }

        match &clause.action {
            MergeNotMatchedByTargetAction::InsertAll => {
                for field in target_schema.fields().iter() {
                    let value = source_expr_for_target(field.name());
                    if let Some(entry) = cases.iter_mut().find(|(name, _)| name == field.name()) {
                        entry.1.push((pred.clone(), value));
                    }
                }
            }
            MergeNotMatchedByTargetAction::InsertColumns { columns, values } => {
                for (col_name, value) in columns.iter().zip(values.iter()) {
                    let resolved = resolve_target_column(col_name, target_schema)?;
                    if let Some(entry) = cases.iter_mut().find(|(name, _)| name == &resolved) {
                        entry.1.push((pred.clone(), value.clone()));
                    }
                }
            }
        }
    }

    let mut projections = Vec::new();
    for field in target_schema.fields() {
        if field.name() == path_column || field.name() == TARGET_ROW_ID_COLUMN {
            continue;
        }
        let name = field.name();
        let default_expr = target_exprs
            .iter()
            .find(|expr| matches!(expr, Expr::Column(col) if col.name == *name))
            .cloned()
            .unwrap_or_else(|| lit(ScalarValue::Null));

        let case_branches = cases
            .iter_mut()
            .find(|(col, _)| col == name)
            .map(|(_, branches)| branches.split_off(0))
            .unwrap_or_default();

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

        projections.push(expr.alias(name.clone()));
    }

    // Preserve the file path column so downstream physical planning can implement
    // targeted rewrite (filter writer input to touched files, while keeping inserts).
    projections.push(col(path_column).alias(path_column.to_string()));

    // Append the operation type column so downstream writers know per-row semantics.
    //   INSERT = 3 (not_matched_by_target → insert)
    //   UPDATE = 2 (matched → update, not_matched_by_source → update, or unchanged copy)
    //   DELETE is already filtered out, but matched → delete clauses are excluded above.
    //
    // Rows that survive the active_expr filter are either:
    //   - Target rows (present) that were not deleted → UPDATE (2)
    //   - Source-only rows (not_matched_by_target) that matched insert clauses → INSERT (3)
    let insert_op = lit(RowLevelOperationType::Insert.as_i32());
    let update_op = lit(RowLevelOperationType::Update.as_i32());
    let op_expr = Expr::Case(Case {
        expr: None,
        when_then_expr: vec![(
            Box::new(col(TARGET_PRESENT_COLUMN).is_null()),
            Box::new(insert_op),
        )],
        else_expr: Some(Box::new(update_op)),
    });
    projections.push(op_expr.alias(OPERATION_COLUMN));

    Ok(projections)
}

fn build_rewrite_predicates(
    options: &MergeIntoOptions,
    matched_pred: &Expr,
    not_matched_by_source_pred: &Expr,
) -> (Vec<Expr>, Vec<Expr>) {
    let mut matched = Vec::new();
    let mut not_matched_by_source = Vec::new();

    for clause in &options.matched_clauses {
        let mut pred = matched_pred.clone().and(options.on_condition.expr.clone());
        if let Some(cond) = &clause.condition {
            pred = pred.and(cond.expr.clone());
        }
        matched.push(pred);
    }

    for clause in &options.not_matched_by_source_clauses {
        let mut pred = not_matched_by_source_pred.clone();
        if let Some(cond) = &clause.condition {
            pred = pred.and(cond.expr.clone());
        }
        not_matched_by_source.push(pred);
    }

    (matched, not_matched_by_source)
}

fn combine_rewrite_preds(matched: Vec<Expr>, not_matched_by_source: Vec<Expr>) -> Option<Expr> {
    let mut preds = Vec::new();
    preds.extend(matched);
    preds.extend(not_matched_by_source);
    combine_disjunction(&preds)
}

fn resolve_target_column(column: &str, target_schema: &DFSchemaRef) -> Result<String> {
    let matches = target_schema
        .fields()
        .iter()
        .filter(|f| f.name().eq_ignore_ascii_case(column))
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
        if let Expr::Column(col) = &expr {
            if let Some(new_name) = target_map
                .get(&col.name)
                .or_else(|| source_map.get(&col.name))
            {
                return Ok(Transformed::yes(Expr::Column(Column {
                    relation: None,
                    name: new_name.clone(),
                    spans: col.spans.clone(),
                })));
            }
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
