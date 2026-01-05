use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, UInt64Array, UInt64Builder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use jiter::Peek;

use crate::common::{get_err, invoke, jiter_json_find, return_type_check, GetError, InvokeResult, JsonPath};
use crate::common_macros::make_udf_function;

make_udf_function!(
    JsonLength,
    json_length,
    json_data path,
    r"Get the length of the array or object at the given path."
);

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct JsonLength {
    signature: Signature,
    aliases: [String; 2],
}

impl Default for JsonLength {
    fn default() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: ["json_length".to_string(), "json_len".to_string()],
        }
    }
}

impl ScalarUDFImpl for JsonLength {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.aliases[0].as_str()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        return_type_check(arg_types, self.name(), DataType::UInt64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        invoke::<UInt64Array>(&args.args, jiter_json_length)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

impl InvokeResult for UInt64Array {
    type Item = u64;

    type Builder = UInt64Builder;

    // cheaper to return integers without dict-encoding them
    const ACCEPT_DICT_RETURN: bool = false;

    fn builder(capacity: usize) -> Self::Builder {
        UInt64Builder::with_capacity(capacity)
    }

    fn append_value(builder: &mut Self::Builder, value: Option<Self::Item>) {
        builder.append_option(value);
    }

    fn finish(mut builder: Self::Builder) -> DataFusionResult<ArrayRef> {
        Ok(Arc::new(builder.finish()))
    }

    fn scalar(value: Option<Self::Item>) -> ScalarValue {
        ScalarValue::UInt64(value)
    }
}

fn jiter_json_length(opt_json: Option<&str>, path: &[JsonPath]) -> Result<u64, GetError> {
    if let Some((mut jiter, peek)) = jiter_json_find(opt_json, path) {
        match peek {
            Peek::Array => {
                let mut peek_opt = jiter.known_array()?;
                let mut length: u64 = 0;
                while let Some(peek) = peek_opt {
                    jiter.known_skip(peek)?;
                    length += 1;
                    peek_opt = jiter.array_step()?;
                }
                Ok(length)
            }
            Peek::Object => {
                let mut opt_key = jiter.known_object()?;

                let mut length: u64 = 0;
                while opt_key.is_some() {
                    jiter.next_skip()?;
                    length += 1;
                    opt_key = jiter.next_key()?;
                }
                Ok(length)
            }
            _ => get_err!(),
        }
    } else {
        get_err!()
    }
}
