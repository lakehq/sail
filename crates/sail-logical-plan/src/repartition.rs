use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::logical_expr::LogicalPlan;
use datafusion_common::{plan_err, DFSchemaRef, Result};
use datafusion_expr::{expr_vec_fmt, Expr, UserDefinedLogicalNodeCore};

/// The explicit repartitioning strategy requested by the Spark client.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, PartialOrd)]
pub enum ExplicitRepartitionKind {
    /// `DataFrame.coalesce(n)`: reduce partitions without a shuffle.
    Coalesce,
    /// `DataFrame.repartition(n)`: shuffle records using round-robin partitioning.
    RoundRobin,
    /// `DataFrame.repartition(n, exprs...)`: shuffle records using hash partitioning.
    Hash,
}

/// A logical plan node for explicit repartitioning in the query.
#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd)]
pub struct ExplicitRepartitionNode {
    input: Arc<LogicalPlan>,
    num_partitions: Option<usize>,
    partitioning_expressions: Vec<Expr>,
    kind: ExplicitRepartitionKind,
}

impl ExplicitRepartitionNode {
    pub fn new(
        input: Arc<LogicalPlan>,
        num_partitions: Option<usize>,
        partitioning_expressions: Vec<Expr>,
        kind: ExplicitRepartitionKind,
    ) -> Self {
        Self {
            input,
            num_partitions,
            partitioning_expressions,
            kind,
        }
    }

    pub fn input(&self) -> &Arc<LogicalPlan> {
        &self.input
    }

    pub fn num_partitions(&self) -> Option<usize> {
        self.num_partitions
    }

    pub fn partitioning_expressions(&self) -> &Vec<Expr> {
        &self.partitioning_expressions
    }

    pub fn kind(&self) -> ExplicitRepartitionKind {
        self.kind
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
            "ExplicitRepartition: kind={:?}, n={:?}, expr=[{}]",
            self.kind,
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
            exprs,
            self.kind,
        ))
    }

    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![output_columns.to_vec()])
    }
}
