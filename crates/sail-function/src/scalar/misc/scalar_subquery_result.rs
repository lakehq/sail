use std::sync::Arc;

use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion_common::cast::{as_int64_array, as_struct_array};
use datafusion_common::{Result, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

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
                    return exec_err!(
                        "SCALAR_SUBQUERY_TOO_MANY_ROWS: scalar subquery returned more than one row"
                    );
                }
            }
            // A missing correlated row nulls the parent struct. Propagate that
            // validity to its value, including nested values and sliced arrays.
            let (_, columns) = result.flatten();
            Ok(Arc::clone(&columns[0]))
        })(&args.args)
    }
}
