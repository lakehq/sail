use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

/// Retain IN's analyzed nullability when its value is lowered to an existence test.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct InSubqueryNullable {
    signature: Signature,
}

impl Default for InSubqueryNullable {
    fn default() -> Self {
        Self::new()
    }
}

impl InSubqueryNullable {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Boolean], Volatility::Immutable),
        }
    }

    pub fn wrap(expr: Expr, nullable: bool) -> Expr {
        if nullable {
            ScalarUDF::from(Self::new()).call(vec![expr])
        } else {
            expr
        }
    }
}

impl ScalarUDFImpl for InSubqueryNullable {
    fn name(&self) -> &str {
        "in_subquery_nullable"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types != [DataType::Boolean] {
            return internal_err!("in_subquery_nullable requires one Boolean argument");
        }
        Ok(DataType::Boolean)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [field] = args.arg_fields else {
            return internal_err!("in_subquery_nullable requires one Boolean argument");
        };
        self.return_type(&[field.data_type().clone()])?;
        Ok(Arc::new(Field::new(self.name(), DataType::Boolean, true)))
    }

    fn should_evaluate_const(&self, _args: &[&ScalarValue]) -> bool {
        // Even an empty RHS must retain the source IN expression's nullable field.
        false
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return internal_err!("in_subquery_nullable requires one Boolean argument");
        }
        let Some(value) = args.args.pop() else {
            return internal_err!("in_subquery_nullable requires one Boolean argument");
        };
        self.return_type(&[value.data_type()])?;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Array, ArrayRef, BooleanArray};
    use datafusion_common::DFSchema;
    use datafusion_expr::simplify::SimplifyContext;
    use datafusion_expr::{ExprSchemable, lit};
    use sail_common_datafusion::simplify_expressions::ExprSimplifier;

    use super::*;

    #[test]
    fn nullable_identity_preserves_scalar_array_slice_and_empty_values() -> Result<()> {
        let udf = InSubqueryNullable::new();
        assert_eq!(
            udf.signature(),
            &Signature::exact(vec![DataType::Boolean], Volatility::Immutable)
        );
        assert!(udf.return_type(&[DataType::Int32]).is_err());
        assert!(udf.return_type(&[]).is_err());
        let array: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)]));
        for value in [
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))),
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(false))),
            ColumnarValue::Scalar(ScalarValue::Boolean(None)),
            ColumnarValue::Array(Arc::clone(&array)),
            ColumnarValue::Array(array.slice(1, 2)),
            ColumnarValue::Array(array.slice(0, 0)),
        ] {
            for nullable in [false, true] {
                let fields = vec![Arc::new(Field::new("input", DataType::Boolean, nullable))];
                let output = udf.return_field_from_args(ReturnFieldArgs {
                    arg_fields: &fields,
                    scalar_arguments: &[None],
                })?;
                assert!(output.is_nullable());
                assert!(output.metadata().is_empty());
                let actual = udf.invoke_with_args(ScalarFunctionArgs {
                    args: vec![value.clone()],
                    arg_fields: fields,
                    number_rows: 3,
                    return_field: output,
                    config_options: Arc::new(Default::default()),
                })?;
                match (&value, actual) {
                    (ColumnarValue::Scalar(expected), ColumnarValue::Scalar(actual)) => {
                        assert_eq!(expected, &actual);
                    }
                    (ColumnarValue::Array(expected), ColumnarValue::Array(actual)) => {
                        assert!(Arc::ptr_eq(expected, &actual));
                    }
                    _ => panic!("identity changed scalar/array representation"),
                }
            }
        }
        Ok(())
    }

    #[test]
    fn nullable_identity_survives_constant_simplification_and_aliases() -> Result<()> {
        let schema = Arc::new(DFSchema::empty());
        let context = SimplifyContext::builder()
            .with_schema(schema.clone())
            .build();
        let simplifier = ExprSimplifier::new(context);
        for value in [Some(true), Some(false), None] {
            let expr = InSubqueryNullable::wrap(lit(ScalarValue::Boolean(value)), true);
            for expr in [expr.clone(), expr.clone().alias("membership"), !expr] {
                let simplified = simplifier.simplify(expr.clone())?;
                assert_eq!(simplified, expr);
                assert!(simplified.nullable(schema.as_ref())?);
                assert!(
                    simplified
                        .to_field(schema.as_ref())?
                        .1
                        .metadata()
                        .is_empty()
                );
            }
        }
        assert_eq!(InSubqueryNullable::wrap(lit(false), false), lit(false));
        Ok(())
    }
}
