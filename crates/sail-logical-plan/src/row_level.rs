use std::cmp::Ordering;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion_common::{DFSchema, DFSchemaRef, DataFusionError, Result};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use sail_common_datafusion::catalog::LakehouseExecutionContext;
use sail_common_datafusion::datasource::{
    MergeIntoOptions, OptionLayer, RowLevelCommand, RowLevelTarget,
};
use sail_common_datafusion::logical_expr::ExprWithSource;
use sail_common_datafusion::utils::items::ItemTaker;

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
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RowLevelWriteNode {
    target: RowLevelTarget,
    raw_target: Arc<LogicalPlan>,
    raw_source: Option<Arc<LogicalPlan>>,
    raw_input_schema: DFSchemaRef,
    effects: Vec<RowLevelEffect>,
    commit: RowLevelCommitInfo,
    /// `Some` means the target scan must still match at commit time. The inner
    /// value is `None` when the table had no current snapshot when it was read.
    expected_snapshot_id: Option<Option<i64>>,
    schema: DFSchemaRef,
}

impl PartialOrd for RowLevelWriteNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self == other {
            return Some(Ordering::Equal);
        }

        (
            &self.target,
            &self.raw_target,
            &self.raw_source,
            &self.effects,
            &self.expected_snapshot_id,
        )
            .partial_cmp(&(
                &other.target,
                &other.raw_target,
                &other.raw_source,
                &other.effects,
                &other.expected_snapshot_id,
            ))
            .filter(|ordering| *ordering != Ordering::Equal)
    }
}

impl RowLevelWriteNode {
    pub fn new_merge(
        raw_target: Arc<LogicalPlan>,
        raw_source: Arc<LogicalPlan>,
        raw_input_schema: DFSchemaRef,
        write_plan: Arc<LogicalPlan>,
        touched_files_plan: Arc<LogicalPlan>,
        row_index_delete_plan: Option<Arc<LogicalPlan>>,
        options: MergeIntoOptions,
        schema: DFSchemaRef,
    ) -> Self {
        let mut effects = vec![
            RowLevelEffect::WriteRows(write_plan),
            RowLevelEffect::TouchFiles(touched_files_plan),
        ];
        if let Some(plan) = row_index_delete_plan {
            effects.push(RowLevelEffect::DeleteRows(plan));
        }
        Self {
            target: options.target.clone(),
            raw_target,
            raw_source: Some(raw_source),
            raw_input_schema,
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
        raw_input_schema: DFSchemaRef,
        condition: Option<ExprWithSource>,
        target: RowLevelTarget,
    ) -> Self {
        Self {
            target,
            raw_target,
            raw_source: None,
            raw_input_schema,
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
        raw_input_schema: DFSchemaRef,
        write_plan: Arc<LogicalPlan>,
        touched_files_plan: Arc<LogicalPlan>,
        row_index_delete_plan: Option<Arc<LogicalPlan>>,
        condition: Option<ExprWithSource>,
        target: RowLevelTarget,
        schema: DFSchemaRef,
    ) -> Self {
        let mut effects = vec![
            RowLevelEffect::WriteRows(write_plan),
            RowLevelEffect::TouchFiles(touched_files_plan),
        ];
        if let Some(plan) = row_index_delete_plan {
            effects.push(RowLevelEffect::DeleteRows(plan));
        }
        Self {
            target,
            raw_target,
            raw_source: None,
            raw_input_schema,
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

    pub fn commit(&self) -> &RowLevelCommitInfo {
        &self.commit
    }

    pub fn merge_options(&self) -> Option<&MergeIntoOptions> {
        self.commit.merge_options()
    }

    pub fn write_plan(&self) -> Option<&Arc<LogicalPlan>> {
        self.effects.iter().find_map(|effect| match effect {
            RowLevelEffect::WriteRows(plan) => Some(plan),
            RowLevelEffect::TouchFiles(_) | RowLevelEffect::DeleteRows(_) => None,
        })
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
        self.effects.iter().find_map(|effect| match effect {
            RowLevelEffect::TouchFiles(plan) => Some(plan),
            RowLevelEffect::WriteRows(_) | RowLevelEffect::DeleteRows(_) => None,
        })
    }

    pub fn row_index_delete_plan(&self) -> Option<&Arc<LogicalPlan>> {
        self.effects.iter().find_map(|effect| match effect {
            RowLevelEffect::DeleteRows(plan) => Some(plan),
            RowLevelEffect::WriteRows(_) | RowLevelEffect::TouchFiles(_) => None,
        })
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

    pub fn target_table_name(&self) -> &[String] {
        &self.target.table_name
    }

    pub fn target_partition_by(&self) -> &[String] {
        &self.target.partition_by
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
            raw_source: self.raw_source.clone(),
            raw_input_schema: self.raw_input_schema.clone(),
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
        let node = RowLevelWriteNode::new_delete(plan, Arc::new(DFSchema::empty()), None, target)
            .with_expected_snapshot_id(Some(None));

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
        let node = RowLevelWriteNode::new_delete(plan, Arc::new(DFSchema::empty()), None, target);
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
