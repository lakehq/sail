//! Logical Operations for DataFusion

use std::collections::HashSet;

use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore};

// Metric Observer is used to update DataFusion metrics from a record batch.
// See MetricObserverExec for the physical implementation

#[derive(Debug, Hash, Eq, PartialEq, PartialOrd)]
pub(crate) struct MetricObserver {
    // id is preserved during conversion to physical node
    pub id: String,
    pub input: LogicalPlan,
    pub enable_pushdown: bool,
}

impl UserDefinedLogicalNodeCore for MetricObserver {
    // Predicate push down is not supported for this node. Try to limit usage
    // near the end of plan.
    fn name(&self) -> &str {
        "MetricObserver"
    }

    fn inputs(&self) -> Vec<&datafusion::logical_expr::LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }

    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        if self.enable_pushdown {
            HashSet::new()
        } else {
            self.schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect()
        }
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "MetricObserver id={}", self.id)
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        inputs: Vec<datafusion::logical_expr::LogicalPlan>,
    ) -> datafusion::common::Result<Self> {
        Ok(MetricObserver {
            id: self.id.clone(),
            input: inputs[0].clone(),
            enable_pushdown: self.enable_pushdown,
        })
    }
}
