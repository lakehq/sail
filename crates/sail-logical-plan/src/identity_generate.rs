use std::collections::HashSet;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion_common::{Column, DFSchema, DFSchemaRef, Result, plan_err};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common_datafusion::utils::items::ItemTaker;

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd)]
pub struct IdentityGenerateColumn {
    pub output: String,
    pub mask: Option<String>,
    pub start: i64,
    pub step: i64,
    pub high_water_mark: Option<i64>,
}

/// A stateful, partition-preserving allocation boundary. Masks are evaluated upstream once.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct IdentityGenerateNode {
    input: Arc<LogicalPlan>,
    columns: Vec<IdentityGenerateColumn>,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
}

impl IdentityGenerateNode {
    pub fn try_new(input: Arc<LogicalPlan>, columns: Vec<IdentityGenerateColumn>) -> Result<Self> {
        if columns.is_empty() {
            return plan_err!("IdentityGenerate requires columns");
        }
        let mut names = input
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<HashSet<_>>();
        let mut fields = input
            .schema()
            .iter()
            .map(|(q, f)| (q.cloned(), f.clone()))
            .collect::<Vec<_>>();
        for column in &columns {
            if column.step == 0 || column.output.is_empty() || !names.insert(column.output.clone())
            {
                return plan_err!("IdentityGenerate requires nonzero steps and fresh output names");
            }
            if let Some(mask) = &column.mask {
                let index = input.schema().index_of_column(&Column::from_name(mask))?;
                if input.schema().field(index).data_type() != &DataType::Boolean {
                    return plan_err!("IdentityGenerate requires Boolean mask: {mask}");
                }
            }
            fields.push((
                None,
                Arc::new(Field::new(
                    &column.output,
                    DataType::Int64,
                    column.mask.is_some(),
                )),
            ));
        }
        let schema = DFSchema::new_with_metadata(fields, input.schema().metadata().clone())?
            .with_functional_dependencies(input.schema().functional_dependencies().clone())?;
        Ok(Self {
            input,
            columns,
            schema: Arc::new(schema),
        })
    }
    pub fn columns(&self) -> &[IdentityGenerateColumn] {
        &self.columns
    }
}

impl UserDefinedLogicalNodeCore for IdentityGenerateNode {
    fn name(&self) -> &str {
        "IdentityGenerate"
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
        write!(f, "IdentityGenerate: {:?}", self.columns)
    }
    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        exprs.zero()?;
        Self::try_new(Arc::new(inputs.one()?), self.columns.clone())
    }
    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![(0..self.input.schema().fields().len()).collect()])
    }
}
