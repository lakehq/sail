use datafusion::arrow::array::make_array;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

/// Converts Spark's already evaluated regexp_instr index to a DataFusion search start.
/// A UDF preserves evaluation of fallible index expressions that an IS NULL expression
/// could discard when the optimizer knows the input is non-nullable.
// TODO: Match Spark's null short-circuiting of fallible index expressions and
// legacy explicit INT overflow before evaluation reaches this adapter.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkRegexpInstrIndex {
    signature: Signature,
}

impl Default for SparkRegexpInstrIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkRegexpInstrIndex {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkRegexpInstrIndex {
    fn name(&self) -> &str {
        "spark_regexp_instr_index"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types {
            [data_type] => Ok(data_type.clone()),
            _ => exec_err!("spark_regexp_instr_index requires 1 argument"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = args.args.as_slice() else {
            return exec_err!("spark_regexp_instr_index requires 1 argument");
        };
        let data_type = value.data_type();
        // Check the evaluated type: CASE and similar expressions can be widened
        // after initial expression resolution. Preserve their baseline coercion.
        // TODO: Apply Spark's implicit INT coercion to wider/noninteger indices,
        // including ANSI overflow handling, before normalizing their search start.
        if !matches!(
            data_type,
            DataType::Int8 | DataType::Int16 | DataType::Int32
        ) {
            return Ok(value.clone());
        }
        let one = ScalarValue::new_one(&data_type)?;
        match value {
            ColumnarValue::Scalar(value) if value.is_null() => {
                Ok(ColumnarValue::Scalar(value.clone()))
            }
            ColumnarValue::Scalar(_) => Ok(ColumnarValue::Scalar(one)),
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(make_array(
                one.to_array_of_size(array.len())?
                    .to_data()
                    .into_builder()
                    .nulls(array.nulls().cloned())
                    .build()?,
            ))),
        }
    }
}
