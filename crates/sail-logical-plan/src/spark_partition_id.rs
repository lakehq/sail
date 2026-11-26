// SPDX-License-Identifier: Apache-2.0

use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion_common::{DFSchema, DFSchemaRef, Result};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common_datafusion::utils::items::ItemTaker;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct SparkPartitionIdNode {
    input: Arc<LogicalPlan>,
    column_name: String,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
}

impl SparkPartitionIdNode {
    pub fn try_new(input: Arc<LogicalPlan>, column_name: String) -> Result<Self> {
        let mut qualified_fields = input
            .schema()
            .iter()
            .map(|(qualifier, field)| (qualifier.cloned(), Arc::clone(field)))
            .collect::<Vec<_>>();
        qualified_fields.push((
            None,
            Arc::new(Field::new(column_name.clone(), DataType::Int32, false)),
        ));
        let schema =
            DFSchema::new_with_metadata(qualified_fields, input.schema().metadata().clone())?
                .with_functional_dependencies(input.schema().functional_dependencies().clone())?;
        Ok(Self {
            input,
            column_name,
            schema: Arc::new(schema),
        })
    }

    pub fn input(&self) -> &Arc<LogicalPlan> {
        &self.input
    }

    pub fn column_name(&self) -> &str {
        &self.column_name
    }
}

impl UserDefinedLogicalNodeCore for SparkPartitionIdNode {
    fn name(&self) -> &str {
        "SparkPartitionId"
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
        write!(f, "SparkPartitionId")
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        exprs.zero()?;
        let input = Arc::new(inputs.one()?);
        Self::try_new(input, self.column_name.clone())
    }

    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        // The added column depends only on the partition of each row, which the columns of the
        // input do not change.
        let input_len = self.input.schema().fields().len();
        Some(vec![
            output_columns
                .iter()
                .copied()
                .filter(|&i| i < input_len)
                .collect(),
        ])
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::Schema;
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::datasource::provider_as_source;
    use datafusion::optimizer::{Optimizer, OptimizerContext};
    use datafusion_expr::{Extension, LogicalPlanBuilder, col};

    use super::*;

    #[test]
    fn test_unused_input_columns_are_pruned_below_the_node() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]);
        let scan = LogicalPlanBuilder::scan(
            "t",
            provider_as_source(Arc::new(EmptyTable::new(Arc::new(schema)))),
            None,
        )?
        .build()?;
        let node = SparkPartitionIdNode::try_new(Arc::new(scan), "pid".to_string())?;
        let plan = LogicalPlanBuilder::from(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        }))
        .project(vec![col("a"), col("pid")])?
        .build()?;
        let plan = Optimizer::new().optimize(plan, &OptimizerContext::new(), |_, _| {})?;
        let display = plan.display_indent().to_string();
        assert!(display.contains("SparkPartitionId"), "{display}");
        assert!(display.contains("TableScan: t projection=[a]"), "{display}");
        Ok(())
    }
}
