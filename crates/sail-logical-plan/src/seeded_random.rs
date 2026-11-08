use std::fmt::Formatter;
use std::sync::Arc;

use datafusion_common::{DFSchema, DFSchemaRef, Result, plan_err};
use datafusion_expr::{Expr, ExprSchemable, LogicalPlan, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::math::partition_random::PartitionSeed;

/// A column computed by an expression with calls of random functions with a seed.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd)]
pub struct SeededRandomColumn {
    pub name: String,
    /// The expression, where each call of a random function with a seed and constant
    /// arguments is evaluated by its own generator, which advances only for the rows that
    /// evaluate the call.
    pub expr: Expr,
    pub seed: PartitionSeed,
}

/// Appends columns computed by expressions with calls of random functions with a seed.
/// Each call is evaluated by a generator that is seeded once per partition and continues
/// across the rows of the partition, as in Spark.
///
/// Filters and limits are not pushed below the node, since that would change which rows
/// consume the random values. Spark does not push filters below a projection with
/// nondeterministic expressions either.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct SeededRandomNode {
    input: Arc<LogicalPlan>,
    columns: Vec<SeededRandomColumn>,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
}

impl SeededRandomNode {
    pub fn try_new(input: Arc<LogicalPlan>, columns: Vec<SeededRandomColumn>) -> Result<Self> {
        if columns.is_empty() {
            return plan_err!("seeded random node requires at least one column");
        }
        let mut qualified_fields = input
            .schema()
            .iter()
            .map(|(qualifier, field)| (qualifier.cloned(), Arc::clone(field)))
            .collect::<Vec<_>>();
        for column in &columns {
            let (_, field) = column.expr.to_field(input.schema().as_ref())?;
            qualified_fields.push((
                None,
                Arc::new(field.as_ref().clone().with_name(column.name.clone())),
            ));
        }
        let schema =
            DFSchema::new_with_metadata(qualified_fields, input.schema().metadata().clone())?
                .with_functional_dependencies(input.schema().functional_dependencies().clone())?;
        Ok(Self {
            input,
            columns,
            schema: Arc::new(schema),
        })
    }

    pub fn input(&self) -> &Arc<LogicalPlan> {
        &self.input
    }

    pub fn columns(&self) -> &[SeededRandomColumn] {
        &self.columns
    }
}

impl UserDefinedLogicalNodeCore for SeededRandomNode {
    fn name(&self) -> &str {
        "SeededRandom"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.columns.iter().map(|x| x.expr.clone()).collect()
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SeededRandom: ")?;
        for (i, column) in self.columns.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{} AS {}", column.expr, column.name)?;
        }
        Ok(())
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if exprs.len() != self.columns.len() {
            return plan_err!(
                "seeded random node expects {} expressions, got {}",
                self.columns.len(),
                exprs.len()
            );
        }
        let columns = self
            .columns
            .iter()
            .zip(exprs)
            .map(|(column, expr)| SeededRandomColumn {
                name: column.name.clone(),
                expr,
                seed: column.seed,
            })
            .collect();
        Self::try_new(Arc::new(inputs.one()?), columns)
    }

    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        // The columns that the expressions refer to are added by the optimizer.
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
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::datasource::provider_as_source;
    use datafusion::optimizer::{Optimizer, OptimizerContext};
    use datafusion_expr::{Extension, LogicalPlanBuilder, ScalarUDF, col, lit};
    use sail_function::scalar::math::random::Random;

    use super::*;

    #[test]
    fn test_filters_and_limits_stay_above_seeded_random_node() -> Result<()> {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let scan = LogicalPlanBuilder::scan(
            "t",
            provider_as_source(Arc::new(EmptyTable::new(Arc::new(schema)))),
            None,
        )?
        .build()?;
        let node = SeededRandomNode::try_new(
            Arc::new(scan),
            vec![SeededRandomColumn {
                name: "r".to_string(),
                expr: ScalarUDF::from(Random::new()).call(vec![lit(42_i64)]),
                seed: PartitionSeed::Offset,
            }],
        )?;
        // Moving a filter or a limit below the node would change which rows consume
        // the random values.
        let plan = LogicalPlanBuilder::from(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        }))
        .filter(col("id").gt(lit(1_i64)).and(col("r").lt(lit(0.5))))?
        .limit(0, Some(3))?
        .build()?;
        let plan = Optimizer::new().optimize(plan, &OptimizerContext::new(), |_, _| {})?;
        let display = plan.display_indent().to_string();
        let position = |name: &str| display.find(name);
        assert!(position("Filter").is_some(), "{display}");
        assert!(position("Limit").is_some(), "{display}");
        assert!(position("Filter") < position("SeededRandom"), "{display}");
        assert!(position("Limit") < position("SeededRandom"), "{display}");
        assert!(
            position("SeededRandom") < position("TableScan"),
            "{display}"
        );
        Ok(())
    }

    #[test]
    fn test_unused_input_columns_are_pruned_below_seeded_random_node() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("c", DataType::Int64, false),
        ]);
        let scan = LogicalPlanBuilder::scan(
            "t",
            provider_as_source(Arc::new(EmptyTable::new(Arc::new(schema)))),
            None,
        )?
        .build()?;
        let node = SeededRandomNode::try_new(
            Arc::new(scan),
            vec![SeededRandomColumn {
                name: "r".to_string(),
                expr: ScalarUDF::from(Random::new()).call(vec![lit(42_i64)]),
                seed: PartitionSeed::Offset,
            }],
        )?;
        let plan = LogicalPlanBuilder::from(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        }))
        .project(vec![col("a"), col("r")])?
        .build()?;
        let plan = Optimizer::new().optimize(plan, &OptimizerContext::new(), |_, _| {})?;
        let display = plan.display_indent().to_string();
        assert!(display.contains("TableScan: t projection=[a]"), "{display}");
        Ok(())
    }
}
