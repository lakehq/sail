use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::logical_expr::LogicalPlan;
use datafusion_common::{plan_err, DFSchemaRef, Result};
use datafusion_expr::{expr_vec_fmt, Expr, UserDefinedLogicalNodeCore};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub enum ExplicitRepartitionKind {
    Coalesce,
    RoundRobin,
    Hash,
}

impl ExplicitRepartitionKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Coalesce => "coalesce",
            Self::RoundRobin => "round_robin",
            Self::Hash => "hash",
        }
    }
}

/// A logical plan node for explicit repartitioning in the query.
#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd)]
pub struct ExplicitRepartitionNode {
    input: Arc<LogicalPlan>,
    num_partitions: Option<usize>,
    kind: ExplicitRepartitionKind,
    partitioning_expressions: Vec<Expr>,
}

impl ExplicitRepartitionNode {
    pub fn new(
        input: Arc<LogicalPlan>,
        num_partitions: Option<usize>,
        kind: ExplicitRepartitionKind,
        partitioning_expressions: Vec<Expr>,
    ) -> Self {
        Self {
            input,
            num_partitions,
            kind,
            partitioning_expressions,
        }
    }

    pub fn input(&self) -> &Arc<LogicalPlan> {
        &self.input
    }

    pub fn num_partitions(&self) -> Option<usize> {
        self.num_partitions
    }

    pub fn kind(&self) -> ExplicitRepartitionKind {
        self.kind
    }

    pub fn partitioning_expressions(&self) -> &Vec<Expr> {
        &self.partitioning_expressions
    }
}

impl UserDefinedLogicalNodeCore for ExplicitRepartitionNode {
    fn name(&self) -> &str {
        "ExplicitRepartition"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        self.partitioning_expressions.clone()
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "ExplicitRepartition: kind={}, n={:?}, expr=[{}]",
            self.kind.as_str(),
            self.num_partitions,
            expr_vec_fmt!(self.partitioning_expressions)
        )
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        let (Some(input), true) = (inputs.pop(), inputs.is_empty()) else {
            return plan_err!("{} expects exactly one input", self.name());
        };
        Ok(Self::new(
            Arc::new(input),
            self.num_partitions,
            self.kind,
            exprs,
        ))
    }

    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![output_columns.to_vec()])
    }
}
