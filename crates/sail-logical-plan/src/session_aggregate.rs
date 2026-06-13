use std::fmt::Formatter;
use std::sync::Arc;

use datafusion_common::{DFSchemaRef, Result};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common_datafusion::utils::items::ItemTaker;

/// Fused logical node for Spark `session_window` aggregation (the
/// `MergingSessionsExec`-equivalent): replaces an `Aggregate` over a
/// [`SessionWindowNode`](crate::session_window::SessionWindowNode) when the
/// aggregates allow it (no `DISTINCT`), merging sessions and driving the
/// accumulators in one pass with O(1) state per open session. The output
/// schema is identical to the `Aggregate` it replaces.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct SessionAggregateNode {
    /// The `SessionWindowNode`'s former child, already filtered of null time /
    /// non-positive gap rows.
    input: Arc<LogicalPlan>,
    /// Session group keys (column names in `input`); the partition of the merge.
    partition_columns: Vec<String>,
    /// Per-row session start candidate (`Timestamp(us)`).
    time_column: String,
    /// Per-row session end candidate (`time + gap`).
    end_column: String,
    /// Output column name of the `{start, end}` session struct.
    session_output: String,
    /// Output group columns in order. Exactly one equals `session_output` (the
    /// struct); the rest are the key columns. Drives output column order.
    group_columns: Vec<String>,
    /// User aggregate expressions, in output order, after the group columns.
    aggregate_exprs: Vec<Expr>,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
}

impl SessionAggregateNode {
    pub fn new(
        input: Arc<LogicalPlan>,
        partition_columns: Vec<String>,
        time_column: String,
        end_column: String,
        session_output: String,
        group_columns: Vec<String>,
        aggregate_exprs: Vec<Expr>,
        schema: DFSchemaRef,
    ) -> Self {
        Self {
            input,
            partition_columns,
            time_column,
            end_column,
            session_output,
            group_columns,
            aggregate_exprs,
            schema,
        }
    }

    pub fn input(&self) -> &Arc<LogicalPlan> {
        &self.input
    }

    pub fn partition_columns(&self) -> &[String] {
        &self.partition_columns
    }

    pub fn time_column(&self) -> &str {
        &self.time_column
    }

    pub fn end_column(&self) -> &str {
        &self.end_column
    }

    pub fn session_output(&self) -> &str {
        &self.session_output
    }

    pub fn group_columns(&self) -> &[String] {
        &self.group_columns
    }

    pub fn aggregate_exprs(&self) -> &[Expr] {
        &self.aggregate_exprs
    }
}

impl UserDefinedLogicalNodeCore for SessionAggregateNode {
    fn name(&self) -> &str {
        "SessionAggregate"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        // The aggregate expressions reference input columns, so expose them for the
        // optimizer to rewrite. Group/time/end columns are referenced by name (like
        // `SessionWindowNode`) and are not listed here.
        self.aggregate_exprs.clone()
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "SessionAggregate: partition_by=[{}], time={}, end={}, session={}, aggs={}",
            self.partition_columns.join(", "),
            self.time_column,
            self.end_column,
            self.session_output,
            self.aggregate_exprs.len()
        )
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        let input = Arc::new(inputs.one()?);
        Ok(Self {
            input,
            partition_columns: self.partition_columns.clone(),
            time_column: self.time_column.clone(),
            end_column: self.end_column.clone(),
            session_output: self.session_output.clone(),
            group_columns: self.group_columns.clone(),
            aggregate_exprs: exprs,
            schema: Arc::clone(&self.schema),
        })
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        // Keep every input column: keys, time, end, and any aggregate inputs are all
        // needed by the merge, so none can be pruned.
        Some(vec![(0..self.input.schema().fields().len()).collect()])
    }
}
