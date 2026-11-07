use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    lit,
};

/// The Spark `typeof` function.
///
/// The type string is computed from the argument type when the function is resolved.
/// The argument is kept so that aggregate functions inside it are still planned,
/// but its value is never needed: `simplify` replaces the call with the type string.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTypeOf {
    type_name: String,
    signature: Signature,
}

impl SparkTypeOf {
    pub fn new(type_name: String) -> Self {
        Self {
            type_name,
            signature: Signature::any(1, Volatility::Immutable),
        }
    }

    pub fn type_name(&self) -> &str {
        &self.type_name
    }
}

impl ScalarUDFImpl for SparkTypeOf {
    fn name(&self) -> &str {
        "typeof"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, false)))
    }

    // This should never be reached, because `simplify` always folds `typeof` to a constant.
    // It exists just in case, and returns the type string without using the argument values.
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            self.type_name.clone(),
        ))))
    }

    fn simplify(&self, _args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        Ok(ExprSimplifyResult::Simplified(lit(self.type_name.clone())))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{
        ArrayRef, BinaryViewArray, Int32Array, LargeStringArray, NullArray, StringViewArray,
        UInt64Array, new_null_array,
    };
    use datafusion::arrow::datatypes::{Fields, IntervalUnit, TimeUnit};
    use datafusion_common::internal_err;
    use datafusion_expr::ScalarUDF;

    use super::*;

    #[test]
    fn test_type_of_return_field_is_non_nullable_string() -> Result<()> {
        let function = SparkTypeOf::new("int".to_string());
        for nullable in [true, false] {
            let fields = vec![Arc::new(Field::new("x", DataType::Int32, nullable))];
            let output = function.return_field_from_args(ReturnFieldArgs {
                arg_fields: &fields,
                scalar_arguments: &[None],
            })?;
            assert_eq!(output.data_type(), &DataType::Utf8);
            assert!(!output.is_nullable());
        }
        Ok(())
    }

    #[test]
    fn test_type_of_simplifies_to_type_name_literal() -> Result<()> {
        let function = SparkTypeOf::new("bigint".to_string());
        let args = vec![lit(1_i64) / lit(0_i64)];
        let result = function.simplify(args, &SimplifyContext::default())?;
        let ExprSimplifyResult::Simplified(expr) = result else {
            return internal_err!("expected typeof to simplify");
        };
        assert_eq!(expr, lit("bigint"));
        Ok(())
    }

    #[test]
    fn test_type_of_ignores_argument_values() -> Result<()> {
        let struct_type =
            DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int32, true)]));
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(NullArray::new(3)),
            Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
            Arc::new(UInt64Array::from(vec![1, 2, 3])),
            new_null_array(&DataType::Float16, 3),
            Arc::new(LargeStringArray::from(vec!["a", "b", "c"])),
            Arc::new(StringViewArray::from(vec!["a", "b", "c"])),
            Arc::new(BinaryViewArray::from(vec![b"a".as_ref(), b"b", b"c"])),
            new_null_array(&DataType::Decimal256(76, 10), 3),
            new_null_array(&DataType::Timestamp(TimeUnit::Nanosecond, None), 3),
            new_null_array(&DataType::Interval(IntervalUnit::YearMonth), 3),
            new_null_array(&DataType::Duration(TimeUnit::Microsecond), 3),
            new_null_array(
                &DataType::LargeList(Arc::new(Field::new_list_field(DataType::Int8, true))),
                3,
            ),
            new_null_array(
                &DataType::FixedSizeList(
                    Arc::new(Field::new_list_field(DataType::Float32, true)),
                    2,
                ),
                3,
            ),
            new_null_array(&struct_type, 3),
            new_null_array(
                &DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                3,
            ),
        ];
        let function = ScalarUDF::from(SparkTypeOf::new("anything".to_string()));
        for array in arrays {
            let field = Arc::new(Field::new("x", array.data_type().clone(), true));
            let output = function.invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(array)],
                arg_fields: vec![field],
                number_rows: 3,
                return_field: Arc::new(Field::new("typeof", DataType::Utf8, false)),
                config_options: Arc::default(),
            })?;
            let ColumnarValue::Scalar(ScalarValue::Utf8(Some(value))) = output else {
                return internal_err!("expected a non-null string scalar");
            };
            assert_eq!(value, "anything");
        }
        Ok(())
    }
}
