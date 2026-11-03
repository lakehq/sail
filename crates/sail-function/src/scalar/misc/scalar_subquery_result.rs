use std::sync::Arc;

use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion_common::cast::{as_int64_array, as_struct_array};
use datafusion_common::{DataFusionError, Result, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use sail_common_datafusion::error::NonRetryableExecutionError;

use crate::functions_nested_utils::make_scalar_function;

/// Check a correlated scalar result only after it has matched an outer row.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ScalarSubqueryResult {
    signature: Signature,
}

impl Default for ScalarSubqueryResult {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarSubqueryResult {
    pub fn new() -> Self {
        Self {
            // Moving this assertion below the correlation join can raise errors
            // for groups that are never referenced by an outer row.
            signature: Signature::any(1, Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for ScalarSubqueryResult {
    fn name(&self) -> &str {
        "scalar_subquery_result"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [input] = args.arg_fields else {
            return internal_err!("scalar_subquery_result requires one argument");
        };
        let DataType::Struct(fields) = input.data_type() else {
            return internal_err!("scalar_subquery_result requires a value/count struct");
        };
        if fields.len() != 2 || fields[1].data_type() != &DataType::Int64 {
            return internal_err!("scalar_subquery_result requires a value/count struct");
        }
        Ok(Arc::new(fields[0].as_ref().clone().with_nullable(true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(|arrays| {
            let [array] = arrays else {
                return internal_err!("scalar_subquery_result requires one argument");
            };
            let result = as_struct_array(array)?;
            let counts = as_int64_array(result.column(1))?;
            for index in 0..result.len() {
                if result.is_valid(index) && counts.is_valid(index) && counts.value(index) > 1 {
                    return Err(DataFusionError::External(Box::new(NonRetryableExecutionError(
                        "SCALAR_SUBQUERY_TOO_MANY_ROWS: scalar subquery returned more than one row"
                            .to_string(),
                    ))));
                }
            }
            // A missing correlated row nulls the parent struct. Propagate that
            // validity to its value, including nested values and sliced arrays.
            let (_, columns) = result.flatten();
            Ok(Arc::clone(&columns[0]))
        })(&args.args)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{ArrayRef, Int32Array, Int64Array, StructArray};
    use datafusion::arrow::datatypes::Field;

    use super::*;

    #[test]
    fn multiple_matching_rows_return_typed_nonretryable_error() -> Result<()> {
        for count in [1, 2] {
            let value = Arc::new(Int32Array::from(vec![10])) as ArrayRef;
            let input = StructArray::from(vec![
                (
                    Arc::new(Field::new("value", DataType::Int32, true)),
                    value.clone(),
                ),
                (
                    Arc::new(Field::new("count", DataType::Int64, false)),
                    Arc::new(Int64Array::from(vec![count])) as ArrayRef,
                ),
            ]);
            let arg_fields = vec![Arc::new(Field::new(
                "result",
                input.data_type().clone(),
                true,
            ))];
            let udf = ScalarSubqueryResult::new();
            let return_field = udf.return_field_from_args(ReturnFieldArgs {
                arg_fields: &arg_fields,
                scalar_arguments: &[None],
            })?;
            let result = udf.invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(Arc::new(input))],
                arg_fields,
                number_rows: 1,
                return_field,
                config_options: Arc::new(Default::default()),
            });
            if count == 1 {
                assert_eq!(result?.into_array(1)?.to_data(), value.to_data());
            } else {
                let Err(DataFusionError::External(error)) = result else {
                    panic!("expected typed scalar cardinality failure");
                };
                assert!(error.is::<NonRetryableExecutionError>());
                assert_eq!(
                    error.to_string(),
                    "SCALAR_SUBQUERY_TOO_MANY_ROWS: scalar subquery returned more than one row"
                );
            }
        }
        Ok(())
    }
}
