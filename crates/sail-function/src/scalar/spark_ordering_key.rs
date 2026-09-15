use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use sail_common_datafusion::ordering::normalize_floats_for_ordering;
use sail_common_datafusion::utils::items::ItemTaker;

/// Rewrites an ordering key so that DataFusion's sort kernels compare it the way Spark does.
///
/// It is not a user-facing function: aggregate rewrites wrap a sort key with it, so that `-0.0`
/// and `0.0`, and all NaNs, tie as they do in Spark's `SQLOrderingUtil.compareDoubles`. The key
/// keeps its field, so the plan schema is unchanged.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkOrderingKey {
    signature: Signature,
}

impl Default for SparkOrderingKey {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkOrderingKey {
    pub const NAME: &'static str = "spark_ordering_key";

    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkOrderingKey {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        arg_types.to_vec().one()
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        args.arg_fields.to_vec().one()
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        Ok(match args.one()? {
            ColumnarValue::Array(array) => {
                ColumnarValue::Array(normalize_floats_for_ordering(&array)?)
            }
            ColumnarValue::Scalar(scalar) => {
                let array = normalize_floats_for_ordering(&scalar.to_array_of_size(1)?)?;
                ColumnarValue::Scalar(ScalarValue::try_from_array(&array, 0)?)
            }
        })
    }
}
