use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::logical_expr::LogicalPlan;
use datafusion_common::{DFSchemaRef, Result, plan_err};
use datafusion_expr::{Expr, SortExpr, UserDefinedLogicalNodeCore, expr_vec_fmt};

#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd)]
pub enum ExplicitRepartitionKind {
    Coalesce,
    RoundRobin,
    Hash(Vec<Expr>),
    /// Each key keeps its sort direction and null order, which decide the partition ranges.
    Range(Vec<SortExpr>),
}

impl ExplicitRepartitionKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Coalesce => "coalesce",
            Self::RoundRobin => "round_robin",
            Self::Hash(_) => "hash",
            Self::Range(_) => "range",
        }
    }
}

/// A logical plan node for explicit repartitioning in the query.
#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd)]
pub struct ExplicitRepartitionNode {
    input: Arc<LogicalPlan>,
    num_partitions: Option<usize>,
    kind: ExplicitRepartitionKind,
}

impl ExplicitRepartitionNode {
    pub fn new(
        input: Arc<LogicalPlan>,
        num_partitions: Option<usize>,
        kind: ExplicitRepartitionKind,
    ) -> Self {
        Self {
            input,
            num_partitions,
            kind,
        }
    }

    pub fn input(&self) -> &Arc<LogicalPlan> {
        &self.input
    }

    pub fn num_partitions(&self) -> Option<usize> {
        self.num_partitions
    }

    pub fn kind(&self) -> &ExplicitRepartitionKind {
        &self.kind
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
        match &self.kind {
            ExplicitRepartitionKind::Coalesce | ExplicitRepartitionKind::RoundRobin => vec![],
            ExplicitRepartitionKind::Hash(exprs) => exprs.clone(),
            ExplicitRepartitionKind::Range(sorts) => {
                sorts.iter().map(|sort| sort.expr.clone()).collect()
            }
        }
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "ExplicitRepartition: kind={}, n={:?}, expr=[",
            self.kind.as_str(),
            self.num_partitions,
        )?;
        match &self.kind {
            ExplicitRepartitionKind::Coalesce | ExplicitRepartitionKind::RoundRobin => {}
            ExplicitRepartitionKind::Hash(exprs) => write!(f, "{}", expr_vec_fmt!(exprs))?,
            ExplicitRepartitionKind::Range(sorts) => write!(f, "{}", expr_vec_fmt!(sorts))?,
        }
        write!(f, "]")
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        let (Some(input), true) = (inputs.pop(), inputs.is_empty()) else {
            return plan_err!("{} expects exactly one input", self.name());
        };
        let expected = match &self.kind {
            ExplicitRepartitionKind::Coalesce | ExplicitRepartitionKind::RoundRobin => 0,
            ExplicitRepartitionKind::Hash(exprs) => exprs.len(),
            ExplicitRepartitionKind::Range(sorts) => sorts.len(),
        };
        if exprs.len() != expected {
            return plan_err!(
                "{} expects {expected} expressions, got {}",
                self.name(),
                exprs.len()
            );
        }
        let kind = match &self.kind {
            ExplicitRepartitionKind::Coalesce => ExplicitRepartitionKind::Coalesce,
            ExplicitRepartitionKind::RoundRobin => ExplicitRepartitionKind::RoundRobin,
            ExplicitRepartitionKind::Hash(_) => ExplicitRepartitionKind::Hash(exprs),
            ExplicitRepartitionKind::Range(sorts) => ExplicitRepartitionKind::Range(
                sorts
                    .iter()
                    .zip(exprs)
                    .map(|(sort, expr)| sort.with_expr(expr))
                    .collect(),
            ),
        };
        Ok(Self::new(Arc::new(input), self.num_partitions, kind))
    }

    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![output_columns.to_vec()])
    }
}
