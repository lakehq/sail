use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion_common::{DFSchema, DFSchemaRef, Result, plan_err};
use datafusion_expr::{Expr, ExprSchemable, LogicalPlan, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common_datafusion::utils::items::ItemTaker;

/// Logical node for Spark `session_window`: appends a `{start, end}` struct
/// column with the bounds of each row's session. Sessions merge per group of
/// `partition_columns`, ordered by `time_column`, while the next row starts at
/// or before the session's end (`end_column` = per-row `time + gap`). The
/// physical operator requests the required hash partitioning and sort.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct SessionWindowNode {
    input: Arc<LogicalPlan>,
    /// Group keys (column names in `input`). May be empty: then the whole input
    /// is one group (a single session stream).
    partition_columns: Vec<String>,
    /// Per-row session start candidate: the time column cast to `Timestamp(us)`.
    time_column: String,
    /// Per-row session end candidate: `time_column + gap`, same timestamp type.
    end_column: String,
    /// Name of the appended `{start, end}` struct column.
    output_column: String,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
}

impl SessionWindowNode {
    pub fn try_new(
        input: Arc<LogicalPlan>,
        partition_columns: Vec<String>,
        time_column: String,
        end_column: String,
        output_column: String,
    ) -> Result<Self> {
        // The struct's `start`/`end` fields carry exactly the time column's type
        // (a `Timestamp(us, tz)`), so the session bounds keep `ts`'s timezone.
        let time_type = Expr::Column(time_column.as_str().into()).get_type(input.schema())?;
        if !matches!(time_type, DataType::Timestamp(_, _)) {
            return plan_err!(
                "session_window time column {time_column} must be a timestamp, got {time_type:?}"
            );
        }
        let struct_type = DataType::Struct(Fields::from(vec![
            Field::new("start", time_type.clone(), true),
            Field::new("end", time_type, true),
        ]));

        // Output schema = every input column, then the new struct column.
        let mut qualified_fields = input
            .schema()
            .iter()
            .map(|(qualifier, field)| (qualifier.cloned(), Arc::clone(field)))
            .collect::<Vec<_>>();
        qualified_fields.push((
            None,
            // The struct column itself is non-nullable (matching Spark); its
            // start/end fields stay nullable.
            Arc::new(Field::new(output_column.clone(), struct_type, false)),
        ));
        let schema =
            DFSchema::new_with_metadata(qualified_fields, input.schema().metadata().clone())?
                .with_functional_dependencies(input.schema().functional_dependencies().clone())?;

        Ok(Self {
            input,
            partition_columns,
            time_column,
            end_column,
            output_column,
            schema: Arc::new(schema),
        })
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

    pub fn output_column(&self) -> &str {
        &self.output_column
    }
}

impl UserDefinedLogicalNodeCore for SessionWindowNode {
    fn name(&self) -> &str {
        "SessionWindow"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        // Group/time/end columns are referenced by name (like `MonotonicIdNode`),
        // so there are no expressions for the optimizer to rewrite.
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "SessionWindow: partition_by=[{}], time={}, end={}, output={}",
            self.partition_columns.join(", "),
            self.time_column,
            self.end_column,
            self.output_column
        )
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        exprs.zero()?;
        let input = Arc::new(inputs.one()?);
        Self::try_new(
            input,
            self.partition_columns.clone(),
            self.time_column.clone(),
            self.end_column.clone(),
            self.output_column.clone(),
        )
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        // Keep every input column: the merge needs the group/time/end columns, and
        // the appended struct is built from them, so none can be pruned away.
        Some(vec![(0..self.input.schema().fields().len()).collect()])
    }
}
