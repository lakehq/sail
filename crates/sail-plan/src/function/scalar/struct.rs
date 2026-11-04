use std::sync::Arc;

use datafusion_common::ScalarValue;
use datafusion_expr::{Expr, ScalarUDF, expr};
use sail_common_datafusion::literal::{LiteralEvaluator, LiteralValue};
use sail_function::scalar::struct_function::StructFunction;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

fn r#struct(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let field_names: Vec<String> = input
        .arguments
        .iter()
        .zip(input.function_context.argument_display_names)
        .enumerate()
        .map(|(i, (expr, name))| -> PlanResult<_> {
            match expr {
                Expr::Column(_) => Ok(name.clone()),
                Expr::Alias(alias) => Ok(alias.name.clone()),
                #[expect(deprecated)]
                Expr::Wildcard { .. } => Err(PlanError::internal(
                    "wildcard should have been expanded before struct",
                )),
                _ => Ok(format!("col{}", i + 1)),
            }
        })
        .collect::<PlanResult<_>>()?;
    Ok(Expr::ScalarFunction(expr::ScalarFunction {
        func: Arc::new(ScalarUDF::from(StructFunction::new(field_names))),
        args: input.arguments,
    }))
}

fn named_struct(input: ScalarFunctionInput) -> PlanResult<Expr> {
    if !input.arguments.len().is_multiple_of(2) {
        return Err(PlanError::analysis(format!(
            "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] named_struct requires an even number of arguments, got {}",
            input.arguments.len()
        )));
    }
    let evaluator = LiteralEvaluator::new();
    let mut field_names = Vec::with_capacity(input.arguments.len() / 2);
    let mut values = Vec::with_capacity(input.arguments.len() / 2);
    for [name, value] in input.arguments.as_chunks::<2>().0 {
        let invalid_name = || {
            PlanError::analysis(
                "[DATATYPE_MISMATCH.CREATE_NAMED_STRUCT_WITHOUT_FOLDABLE_STRING] named_struct field names must be foldable non-null strings",
            )
        };
        if name.is_volatile() {
            return Err(invalid_name());
        }
        let name = evaluator.evaluate(name).map_err(|_| invalid_name())?;
        if matches!(
            name,
            ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) | ScalarValue::Utf8View(None)
        ) {
            return Err(PlanError::analysis(
                "[DATATYPE_MISMATCH.UNEXPECTED_NULL] named_struct field names must not be null",
            ));
        }
        field_names.push(
            LiteralValue(&name)
                .try_to_string()
                .map_err(|_| invalid_name())?
                .to_owned(),
        );
        values.push(value.clone());
    }
    Ok(ScalarUDF::from(StructFunction::new(field_names)).call(values))
}

pub(super) fn list_built_in_struct_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("named_struct", F::custom(named_struct)),
        ("struct", F::custom(r#struct)),
    ]
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::array::{Array, AsArray, Int32Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;
    use datafusion_common::{DFSchema, DFSchemaRef};
    use datafusion_expr::{col, lit};

    use super::*;
    use crate::config::PlanConfig;
    use crate::function::common::FunctionContextInput;

    fn build(
        arguments: Vec<Expr>,
        context: &SessionContext,
        schema: &DFSchemaRef,
    ) -> PlanResult<Expr> {
        let config = Arc::new(PlanConfig::default());
        let functions = list_built_in_struct_functions();
        let (_, function) = functions
            .iter()
            .find(|(name, _)| *name == "named_struct")
            .ok_or_else(|| PlanError::internal("named_struct must be registered"))?;
        function(ScalarFunctionInput {
            arguments,
            function_context: FunctionContextInput {
                argument_display_names: &[],
                plan_config: &config,
                session_context: context,
                schema,
            },
        })
    }

    #[test]
    fn named_struct_evaluates_zero_row_and_zero_field_batches() -> PlanResult<()> {
        let context = SessionContext::new();
        let metadata = HashMap::from([("tag".to_owned(), "kept".to_owned())]);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int32, false).with_metadata(metadata.clone()),
                Field::new("b", DataType::Int32, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![None, Some(3)])),
            ],
        )?;
        let schema = Arc::new(DFSchema::try_from(batch.schema())?);
        let expr = build(
            vec![lit("x"), col("a"), lit("y"), col("b")],
            &context,
            &schema,
        )?;
        let expected = DataType::Struct(
            vec![
                Field::new("x", DataType::Int32, false).with_metadata(metadata),
                Field::new("y", DataType::Int32, true),
            ]
            .into(),
        );
        let physical = context.create_physical_expr(expr, schema.as_ref())?;
        // An empty SQL result need not invoke the expression on a zero-row batch.
        let input = batch.slice(0, 0);
        let result = physical.evaluate(&input)?.into_array(0)?;
        assert_eq!(result.data_type(), &expected);
        assert_eq!(result.len(), 0);
        assert_eq!(result.null_count(), 0);
        assert_eq!(result.as_struct().columns(), input.columns());

        let empty = build(vec![], &context, &schema)?;
        let physical = context.create_physical_expr(empty, schema.as_ref())?;
        for rows in [0, 2] {
            let result = physical.evaluate(&batch.slice(0, rows))?.into_array(rows)?;
            assert_eq!(result.len(), rows);
            assert_eq!(result.null_count(), 0);
            assert_eq!(result.as_struct().num_columns(), 0);
        }
        Ok(())
    }
}
