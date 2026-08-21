use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use datafusion::optimizer::simplify_expressions::{
    ExprSimplifier, SimplifyExpressions as DataFusionSimplifyExpressions,
};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchema, DFSchemaRef, Result, ScalarValue};
use datafusion_expr::expr::{Placeholder, ScalarFunction};
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::simplify::SimplifyContext;
use datafusion_expr::utils::merge_schema;
use datafusion_expr::{Expr, ExprSchemable};
use sail_common_datafusion::logical_expr::lazy_scalar::{
    LazyScalarUDF, is_constant_null, is_proven_infallible,
};

static BARRIER_ID: AtomicU64 = AtomicU64::new(0);

/// DataFusion expression simplification with lazy-scalar argument boundaries.
///
/// DataFusion 54 recursively folds scalar-function arguments even when the function reports that
/// it short-circuits. Lazy scalar arguments are simplified explicitly from left to right, then
/// hidden behind typed placeholders while the ordinary simplifier processes the surrounding plan.
#[derive(Debug, Default)]
pub struct SimplifyExpressions {
    datafusion: DataFusionSimplifyExpressions,
}

impl SimplifyExpressions {
    pub fn new() -> Self {
        Self::default()
    }
}

impl OptimizerRule for SimplifyExpressions {
    fn name(&self) -> &str {
        "simplify_expressions"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if !plan.expressions().iter().any(contains_lazy_scalar) {
            return self.datafusion.rewrite(plan, config);
        }

        let original = plan.clone();
        let schema = input_schema(&plan)?;
        let context = SimplifyContext::builder()
            .with_schema(Arc::clone(&schema))
            .with_config_options(config.options())
            .with_query_execution_start_time(config.query_execution_start_time())
            .build();
        let simplifier =
            ExprSimplifier::new(context).with_canonicalize(!matches!(plan, LogicalPlan::Join(_)));

        let names = NamePreserver::new(&plan);
        let prepared = plan.map_expressions(|expression| {
            let name = names.save(&expression);
            simplify_lazy_arguments(expression, &simplifier, schema.as_ref())
                .map(|result| result.update_data(|expression| name.restore(expression)))
        })?;

        let prepared_plan = prepared.data;
        let shielded_names = NamePreserver::new(&prepared_plan);
        let mut barriers = HashMap::new();
        let shielded = prepared_plan.map_expressions(|expression| {
            let name = shielded_names.save(&expression);
            shield_lazy_arguments(expression, schema.as_ref(), &mut barriers)
                .map(|result| result.update_data(|expression| name.restore(expression)))
        })?;

        let simplified = self.datafusion.rewrite(shielded.data, config)?;
        let restored = restore_plan(simplified.data, &barriers)?.data;
        Ok(Transformed::new_transformed(
            restored.clone(),
            restored != original,
        ))
    }
}

fn input_schema(plan: &LogicalPlan) -> Result<DFSchemaRef> {
    if !plan.inputs().is_empty() {
        Ok(DFSchemaRef::new(merge_schema(&plan.inputs())))
    } else if let LogicalPlan::TableScan(scan) = plan {
        Ok(Arc::new(DFSchema::try_from_qualified_schema(
            scan.table_name.clone(),
            &scan.source.schema(),
        )?))
    } else {
        Ok(Arc::new(DFSchema::empty()))
    }
}

fn contains_lazy_scalar(expression: &Expr) -> bool {
    expression
        .exists(|node| Ok(is_lazy_scalar(node)))
        .unwrap_or(true)
}

fn is_lazy_scalar(expression: &Expr) -> bool {
    matches!(
        expression,
        Expr::ScalarFunction(function)
            if function.func.inner().downcast_ref::<LazyScalarUDF>().is_some()
    )
}

fn simplify_lazy_arguments(
    expression: Expr,
    simplifier: &ExprSimplifier,
    schema: &DFSchema,
) -> Result<Transformed<Expr>> {
    expression.transform_down(|node| {
        let Expr::ScalarFunction(mut function) = node else {
            return Ok(Transformed::no(node));
        };
        if function
            .func
            .inner()
            .downcast_ref::<LazyScalarUDF>()
            .is_none()
        {
            return Ok(Transformed::no(Expr::ScalarFunction(function)));
        }

        let mut changed = false;
        let mut foldable_fallible_prefix = false;
        for index in 0..function.args.len() {
            let prepared =
                simplify_lazy_arguments(function.args[index].clone(), simplifier, schema)?;
            let simplified = simplify_with_lazy_barriers(prepared.data, simplifier, schema)?;
            changed |= prepared.transformed || simplified.transformed;
            function.args[index] = simplified.data;

            if is_constant_null(&function.args[index]) && !foldable_fallible_prefix {
                let call = Expr::ScalarFunction(function);
                let result_type = call.get_type(schema)?;
                return Ok(Transformed::new(
                    Expr::Literal(ScalarValue::try_new_null(&result_type)?, None),
                    true,
                    TreeNodeRecursion::Jump,
                ));
            }

            foldable_fallible_prefix |=
                is_foldable(&function.args[index]) && !is_proven_infallible(&function.args[index]);
        }

        Ok(Transformed::new(
            Expr::ScalarFunction(function),
            changed,
            TreeNodeRecursion::Jump,
        ))
    })
}

fn simplify_with_lazy_barriers(
    expression: Expr,
    simplifier: &ExprSimplifier,
    schema: &DFSchema,
) -> Result<Transformed<Expr>> {
    let original = expression.clone();
    let mut barriers = HashMap::new();
    let shielded = shield_lazy_arguments(expression, schema, &mut barriers)?;
    let simplified = simplifier
        .simplify_with_cycle_count_transformed(shielded.data)?
        .0;
    let restored = restore_expression(simplified.data, &barriers)?.data;
    Ok(Transformed::new_transformed(
        restored.clone(),
        restored != original,
    ))
}

fn shield_lazy_arguments(
    expression: Expr,
    schema: &DFSchema,
    barriers: &mut HashMap<String, Expr>,
) -> Result<Transformed<Expr>> {
    expression.transform_down(|node| {
        let Expr::ScalarFunction(mut function) = node else {
            return Ok(Transformed::no(node));
        };
        if function
            .func
            .inner()
            .downcast_ref::<LazyScalarUDF>()
            .is_none()
        {
            return Ok(Transformed::no(Expr::ScalarFunction(function)));
        }

        function.args = function
            .args
            .into_iter()
            .map(|argument| {
                let field = argument.to_field(schema)?.1;
                let id = format!(
                    "$__sail_lazy_scalar_{}_{}",
                    BARRIER_ID.fetch_add(1, Ordering::Relaxed),
                    barriers.len()
                );
                barriers.insert(id.clone(), argument);
                Ok(Expr::Placeholder(Placeholder::new_with_field(
                    id,
                    Some(field),
                )))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Transformed::new(
            Expr::ScalarFunction(ScalarFunction::new_udf(function.func, function.args)),
            true,
            TreeNodeRecursion::Jump,
        ))
    })
}

fn restore_expression(
    expression: Expr,
    barriers: &HashMap<String, Expr>,
) -> Result<Transformed<Expr>> {
    expression.transform_up(|node| {
        if let Expr::Placeholder(placeholder) = &node
            && let Some(argument) = barriers.get(&placeholder.id)
        {
            return Ok(Transformed::yes(argument.clone()));
        }
        Ok(Transformed::no(node))
    })
}

fn restore_plan(
    plan: LogicalPlan,
    barriers: &HashMap<String, Expr>,
) -> Result<Transformed<LogicalPlan>> {
    plan.transform_down(|node| {
        node.map_expressions(|expression| restore_expression(expression, barriers))
    })
}

#[expect(deprecated)]
fn is_foldable(expression: &Expr) -> bool {
    expression
        .exists(|node| {
            Ok(node.is_volatile_node()
                || matches!(
                    node,
                    Expr::Column(_)
                        | Expr::ScalarVariable(_, _)
                        | Expr::AggregateFunction(_)
                        | Expr::WindowFunction(_)
                        | Expr::Exists(_)
                        | Expr::InSubquery(_)
                        | Expr::SetComparison(_)
                        | Expr::ScalarSubquery(_)
                        | Expr::Placeholder(_)
                        | Expr::OuterReferenceColumn(_, _)
                        | Expr::Wildcard { .. }
                        | Expr::GroupingSet(_)
                        | Expr::LambdaVariable(_)
                ))
        })
        .is_ok_and(|contains_non_foldable_node| !contains_non_foldable_node)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion_common::{DFSchema, Result, ScalarValue};
    use datafusion_expr::simplify::SimplifyContext;
    use datafusion_expr::{
        ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
        cast, col, lit,
    };

    use super::{ExprSimplifier, simplify_lazy_arguments};
    use sail_common_datafusion::logical_expr::lazy_scalar::LazyScalarUDF;

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct FirstArgument {
        signature: Signature,
    }

    impl FirstArgument {
        fn new() -> Self {
            Self {
                signature: Signature::exact(
                    vec![DataType::Int64, DataType::Int64],
                    Volatility::Immutable,
                ),
            }
        }
    }

    impl ScalarUDFImpl for FirstArgument {
        fn name(&self) -> &str {
            "first_argument"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _: &[DataType]) -> Result<DataType> {
            Ok(DataType::Int64)
        }

        fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            Ok(args.args[0].clone())
        }
    }

    fn lazy(arguments: Vec<Expr>, schema: &DFSchema) -> Result<Expr> {
        LazyScalarUDF::call_fallible(
            Arc::new(ScalarUDF::new_from_impl(FirstArgument::new())),
            arguments,
            schema,
        )
    }

    fn simplifier(schema: &DFSchema) -> ExprSimplifier {
        ExprSimplifier::new(
            SimplifyContext::builder()
                .with_schema(Arc::new(schema.clone()))
                .build(),
        )
    }

    #[test]
    fn reports_foldable_errors_from_left_to_right() -> Result<()> {
        let schema = DFSchema::empty();
        let expression = lazy(
            vec![
                cast(lit("bad-first"), DataType::Int64),
                cast(lit(1_i64) / lit(0_i64), DataType::Int64),
            ],
            &schema,
        )?;

        let error = match simplify_lazy_arguments(expression, &simplifier(&schema), &schema) {
            Ok(_) => return datafusion_common::internal_err!("the first cast must fail"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("bad-first"));
        Ok(())
    }

    #[test]
    fn later_constant_null_prunes_row_dependent_prefix() -> Result<()> {
        let schema = DFSchema::new_with_metadata(
            vec![(None, Arc::new(Field::new("id", DataType::Int64, false)))],
            Default::default(),
        )?;
        let expression = lazy(
            vec![lit(1_i64) / col("id"), lit(ScalarValue::Int64(None))],
            &schema,
        )?;

        let simplified = simplify_lazy_arguments(expression, &simplifier(&schema), &schema)?.data;
        assert!(matches!(simplified, Expr::Literal(value, _) if value.is_null()));
        Ok(())
    }

    #[test]
    fn later_constant_null_keeps_foldable_fallible_prefix() -> Result<()> {
        let schema = DFSchema::empty();
        let expression = lazy(
            vec![lit(1_i64) / lit(0_i64), lit(ScalarValue::Int64(None))],
            &schema,
        )?;

        let simplified = simplify_lazy_arguments(expression, &simplifier(&schema), &schema)?.data;
        assert!(matches!(simplified, Expr::ScalarFunction(_)));
        Ok(())
    }
}
