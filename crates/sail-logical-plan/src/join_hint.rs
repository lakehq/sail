use std::fmt::Formatter;
use std::sync::Arc;

use datafusion_common::{DFSchemaRef, Result, plan_err};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

/// Ordered by Spark's precedence when opposite join inputs request different strategies.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub enum JoinStrategy {
    Broadcast,
    Merge,
    ShuffleHash,
    ShuffleReplicateNl,
}

impl JoinStrategy {
    pub fn from_hint(name: &str) -> Option<Self> {
        match name.to_ascii_uppercase().as_str() {
            "BROADCAST" | "BROADCASTJOIN" | "MAPJOIN" => Some(Self::Broadcast),
            "MERGE" | "MERGEJOIN" | "SHUFFLE_MERGE" => Some(Self::Merge),
            "SHUFFLE_HASH" => Some(Self::ShuffleHash),
            "SHUFFLE_REPLICATE_NL" => Some(Self::ShuffleReplicateNl),
            _ => None,
        }
    }
}

/// A strategy attached to a relation, consumed by its first enclosing join.
#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd)]
pub struct JoinHintNode {
    pub input: Arc<LogicalPlan>,
    pub strategy: JoinStrategy,
}

impl UserDefinedLogicalNodeCore for JoinHintNode {
    fn name(&self) -> &str {
        "JoinHint"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "JoinHint: {:?}", self.strategy)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        let (Some(input), true, true) = (inputs.pop(), inputs.is_empty(), exprs.is_empty()) else {
            return plan_err!("JoinHint expects one input and no expressions");
        };
        Ok(Self {
            input: Arc::new(input),
            strategy: self.strategy,
        })
    }

    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![output_columns.to_vec()])
    }
}
