use std::fmt::Formatter;
use std::sync::Arc;

use datafusion_common::{DFSchemaRef, DataFusionError, Result};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common_datafusion::datasource::{MergeIntoOptions, RowLevelCommand, RowLevelTarget};
use sail_common_datafusion::logical_expr::ExprWithSource;
use sail_common_datafusion::utils::items::ItemTaker;

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

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ExpandedRowLevelOperation {
    target: RowLevelTarget,
    effects: Vec<RowLevelEffect>,
    commit: RowLevelCommitInfo,
    output_schema: DFSchemaRef,
}

impl ExpandedRowLevelOperation {
    pub fn try_new(
        target: RowLevelTarget,
        effects: Vec<RowLevelEffect>,
        commit: RowLevelCommitInfo,
        output_schema: DFSchemaRef,
    ) -> Result<Self> {
        let mut write_effects = 0;
        let mut touch_effects = 0;
        let mut delete_effects = 0;
        for effect in &effects {
            match effect {
                RowLevelEffect::WriteRows(_) => write_effects += 1,
                RowLevelEffect::TouchFiles(_) => touch_effects += 1,
                RowLevelEffect::DeleteRows(_) => delete_effects += 1,
            }
        }
        if write_effects != 1 {
            return Err(DataFusionError::Plan(format!(
                "expanded row-level operation requires exactly one WriteRows effect, got {write_effects}"
            )));
        }
        if touch_effects > 1 || delete_effects > 1 {
            return Err(DataFusionError::Plan(format!(
                "expanded row-level operation accepts at most one TouchFiles and one DeleteRows effect, got {touch_effects} and {delete_effects}"
            )));
        }
        Ok(Self {
            target,
            effects,
            commit,
            output_schema,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct RowLevelWriteNode {
    target: RowLevelTarget,
    effects: Vec<RowLevelEffect>,
    #[educe(PartialOrd(ignore))]
    commit: RowLevelCommitInfo,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
}

impl RowLevelWriteNode {
    pub fn new(operation: ExpandedRowLevelOperation) -> Self {
        Self {
            target: operation.target,
            effects: operation.effects,
            commit: operation.commit,
            schema: operation.output_schema,
        }
    }

    pub fn command(&self) -> RowLevelCommand {
        self.commit.command()
    }

    pub fn target(&self) -> &RowLevelTarget {
        &self.target
    }

    pub fn commit(&self) -> &RowLevelCommitInfo {
        &self.commit
    }

    pub fn merge_options(&self) -> Option<&MergeIntoOptions> {
        self.commit.merge_options()
    }

    pub fn write_rows_plan(&self) -> Result<&Arc<LogicalPlan>> {
        self.effects
            .iter()
            .find_map(|effect| match effect {
                RowLevelEffect::WriteRows(plan) => Some(plan),
                RowLevelEffect::TouchFiles(_) | RowLevelEffect::DeleteRows(_) => None,
            })
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "validated row-level operation is missing its WriteRows effect".to_string(),
                )
            })
    }

    pub fn touched_files_plan(&self) -> Option<&Arc<LogicalPlan>> {
        self.effects.iter().find_map(|effect| match effect {
            RowLevelEffect::TouchFiles(plan) => Some(plan),
            RowLevelEffect::WriteRows(_) | RowLevelEffect::DeleteRows(_) => None,
        })
    }

    pub fn delete_rows_plan(&self) -> Option<&Arc<LogicalPlan>> {
        self.effects.iter().find_map(|effect| match effect {
            RowLevelEffect::DeleteRows(plan) => Some(plan),
            RowLevelEffect::WriteRows(_) | RowLevelEffect::TouchFiles(_) => None,
        })
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
            .map(|name| name.as_str())
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
            effects,
            commit: self.commit.clone(),
            schema: self.schema.clone(),
        })
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        None
    }
}
