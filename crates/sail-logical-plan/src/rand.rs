use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion_common::{DFSchema, DFSchemaRef, Result};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common_datafusion::utils::items::ItemTaker;

/// Whether the random column produces uniform or Gaussian values.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd)]
pub enum RandMode {
    /// Uniform in [0.0, 1.0) — equivalent to Spark's `rand(seed)`.
    Uniform,
    /// Standard normal (Gaussian) — equivalent to Spark's `randn(seed)`.
    Gaussian,
}

/// A logical plan node that adds a partition-aware random column.
///
/// During physical planning this becomes [`RandExec`] which seeds its RNG
/// with `seed + partitionIndex`, matching Spark's `Rand` / `Randn` expressions.
#[derive(Clone, Debug, PartialEq, Educe)]
#[educe(Eq, Hash, PartialOrd)]
pub struct RandNode {
    input: Arc<LogicalPlan>,
    column_name: String,
    seed: i64,
    mode: RandMode,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
}

impl RandNode {
    pub fn try_new(
        input: Arc<LogicalPlan>,
        column_name: String,
        seed: i64,
        mode: RandMode,
    ) -> Result<Self> {
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
            mode,
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

    pub fn mode(&self) -> &RandMode {
        &self.mode
    }
}

impl UserDefinedLogicalNodeCore for RandNode {
    fn name(&self) -> &str {
        "Rand"
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
        write!(
            f,
            "Rand: col={}, seed={}, mode={:?}",
            self.column_name, self.seed, self.mode
        )
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        exprs.zero()?;
        let input = Arc::new(inputs.one()?);
        Self::try_new(
            input,
            self.column_name.clone(),
            self.seed,
            self.mode.clone(),
        )
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![(0..self.input.schema().fields().len()).collect()])
    }
}
