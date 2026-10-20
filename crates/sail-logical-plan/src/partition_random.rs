use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion_common::{DFSchema, DFSchemaRef, Result};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common_datafusion::utils::items::ItemTaker;

/// Appends a column of random numbers in `[0, 1)` generated like Spark's `rand(seed)`.
/// Each partition draws from its own `XORShiftRandom` seeded with `seed + partition index`,
/// and the generator advances across all the batches of the partition.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct PartitionRandomNode {
    input: Arc<LogicalPlan>,
    column_name: String,
    seed: i64,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
}

impl PartitionRandomNode {
    pub fn try_new(input: Arc<LogicalPlan>, column_name: String, seed: i64) -> Result<Self> {
        let mut qualified_fields = input
            .schema()
            .iter()
            .map(|(qualifier, field)| (qualifier.cloned(), Arc::clone(field)))
            .collect::<Vec<_>>();
        qualified_fields.push((
            None,
            Arc::new(Field::new(column_name.clone(), DataType::Float64, false)),
        ));
        let schema =
            DFSchema::new_with_metadata(qualified_fields, input.schema().metadata().clone())?
                .with_functional_dependencies(input.schema().functional_dependencies().clone())?;
        Ok(Self {
            input,
            column_name,
            seed,
            schema: Arc::new(schema),
        })
    }

    pub fn input(&self) -> &Arc<LogicalPlan> {
        &self.input
    }

    pub fn column_name(&self) -> &str {
        &self.column_name
    }

    pub fn seed(&self) -> i64 {
        self.seed
    }
}

impl UserDefinedLogicalNodeCore for PartitionRandomNode {
    fn name(&self) -> &str {
        "PartitionRandom"
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
        write!(f, "PartitionRandom: seed={}", self.seed)
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        exprs.zero()?;
        let input = Arc::new(inputs.one()?);
        Self::try_new(input, self.column_name.clone(), self.seed)
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![(0..self.input.schema().fields().len()).collect()])
    }
}
