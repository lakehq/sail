use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::datatypes::FieldRef;
use datafusion_common::tree_node::TreeNode;
use datafusion_common::{DFSchema, DFSchemaRef, Result, plan_err};
use datafusion_expr::{Expr, ExprSchemable, LogicalPlan, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::array::spark_shuffle::SparkShuffle;
use sail_function::scalar::math::partition_random::{
    PartitionSeed, is_row_argument, seed_argument_position,
};
use sail_function::scalar::misc::spark_uuid::SparkUuid;

/// Whether a random call has reproducible arguments, rather than a fresh entropy seed.
/// Callers must also inspect its children: a shuffle's row argument can itself be volatile.
pub fn is_reproducible_seeded_random_call(expr: &Expr) -> bool {
    let Expr::ScalarFunction(call) = expr else {
        return false;
    };
    let Some(seed_index) = seed_argument_position(&call.func, call.args.len()) else {
        return false;
    };
    let null_uses_entropy =
        call.func.inner().is::<SparkUuid>() || call.func.inner().is::<SparkShuffle>();
    if !call.args.iter().enumerate().all(|(i, arg)| {
        is_row_argument(&call.func, i)
            || (arg.column_refs().is_empty()
                && !arg.is_volatile()
                && !arg
                    .exists(|e| {
                        if let Expr::ScalarSubquery(query) = e {
                            // Its cached value is constant across replays, including NULL.
                            return Ok(!query.outer_ref_columns.is_empty());
                        }
                        Ok(matches!(
                            e,
                            Expr::LambdaVariable(_)
                                | Expr::OuterReferenceColumn(_, _)
                                | Expr::Exists(_)
                                | Expr::InSubquery(_)
                                | Expr::SetComparison(_)
                                | Expr::ScalarVariable(_, _)
                                | Expr::Placeholder(_)
                        ))
                    })
                    .unwrap_or(true))
    }) {
        return false;
    }
    // These functions interpret NULL as an absent seed, unlike rand/randn's seed zero.
    // Nullability also handles casts and other constant expressions before simplification.
    !null_uses_entropy
        || matches!(
            call.args[seed_index].nullable(&DFSchema::empty()),
            Ok(false)
        )
}

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
        let fields = columns
            .iter()
            .map(|column| {
                let (_, field) = column.expr.to_field(input.schema().as_ref())?;
                Ok(Arc::new(
                    field.as_ref().clone().with_name(column.name.clone()),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        Self::try_new_with_fields(input, columns, fields)
    }

    fn try_new_with_fields(
        input: Arc<LogicalPlan>,
        columns: Vec<SeededRandomColumn>,
        fields: Vec<FieldRef>,
    ) -> Result<Self> {
        let qualified_fields = input
            .schema()
            .iter()
            .map(|(qualifier, field)| (qualifier.cloned(), Arc::clone(field)))
            .chain(fields.into_iter().map(|field| (None, field)))
            .collect::<Vec<_>>();
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
        let input = inputs.one()?;
        // A tree traversal replaces the input before the expressions, which may refer to the
        // qualifiers of the previous input, so the fields are kept if nothing they depend on
        // has changed.
        let input_fields = self.input.schema().fields();
        if columns == self.columns && input.schema().fields() == input_fields {
            if input.schema() == self.input.schema() {
                return Ok(Self {
                    input: Arc::new(input),
                    columns,
                    schema: Arc::clone(&self.schema),
                });
            }
            let fields = self.schema.fields()[input_fields.len()..].to_vec();
            return Self::try_new_with_fields(Arc::new(input), columns, fields);
        }
        Self::try_new(Arc::new(input), columns)
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
    fn test_reproducible_seeds_distinguish_null_from_zero() -> Result<()> {
        use datafusion_common::ScalarValue;
        use datafusion_expr::ExprSchemable;
        let random = ScalarUDF::from(Random::new());
        let uuid = ScalarUDF::from(SparkUuid::new());
        let null = lit(ScalarValue::Int64(None));
        assert!(is_reproducible_seeded_random_call(
            &random.call(vec![null.clone()])
        ));
        assert!(!is_reproducible_seeded_random_call(
            &uuid.call(vec![null.clone()])
        ));
        assert!(!is_reproducible_seeded_random_call(&uuid.call(vec![
            null.cast_to(&DataType::Int64, &DFSchema::empty())?
        ])));
        assert!(is_reproducible_seeded_random_call(
            &uuid.call(vec![lit(40_i64) + lit(2_i64)])
        ));
        assert!(!is_reproducible_seeded_random_call(&random.call(vec![])));
        assert!(!is_reproducible_seeded_random_call(
            &uuid.call(vec![random.call(vec![])])
        ));
        assert!(!is_reproducible_seeded_random_call(
            &uuid.call(vec![col("seed")])
        ));
        for value in [lit(42_i64), lit(ScalarValue::Int64(None))] {
            let nullable = value.nullable(&DFSchema::empty())?;
            let plan = LogicalPlanBuilder::empty(true)
                .project(vec![value])?
                .build()?;
            let seed = datafusion_expr::expr_fn::scalar_subquery(Arc::new(plan));
            assert!(is_reproducible_seeded_random_call(
                &random.call(vec![seed.clone()])
            ));
            assert_eq!(
                is_reproducible_seeded_random_call(&uuid.call(vec![seed])),
                !nullable
            );
        }
        Ok(())
    }

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

    #[test]
    fn test_seeded_random_node_reuses_fields_for_unchanged_expressions() -> Result<()> {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let scan = LogicalPlanBuilder::scan(
            "t",
            provider_as_source(Arc::new(EmptyTable::new(Arc::new(schema)))),
            None,
        )?
        .build()?;
        let expr = col("t.id") + ScalarUDF::from(Random::new()).call(vec![lit(42_i64)]);
        let node = SeededRandomNode::try_new(
            Arc::new(scan.clone()),
            vec![SeededRandomColumn {
                name: "r".to_string(),
                expr: expr.clone(),
                seed: PartitionSeed::Offset,
            }],
        )?;
        let same = node.with_exprs_and_inputs(vec![expr.clone()], vec![scan.clone()])?;
        assert!(Arc::ptr_eq(same.schema(), node.schema()));
        // Optimizer rules may replace the input qualifiers before rewriting the expressions
        // that refer to them.
        let input = LogicalPlanBuilder::from(scan).alias("u")?.build()?;
        let renamed = node.with_exprs_and_inputs(vec![expr], vec![input.clone()])?;
        assert_eq!(
            renamed.schema().columns(),
            vec![
                datafusion_common::Column::new(Some("u"), "id"),
                datafusion_common::Column::new_unqualified("r"),
            ]
        );
        assert_eq!(renamed.schema().field(1), node.schema().field(1));
        // Changed expressions are validated against the new input.
        let changed = col("t.id") + lit(1.0);
        assert!(
            node.with_exprs_and_inputs(vec![changed], vec![input])
                .is_err()
        );
        Ok(())
    }
}
