// https://github.com/datafusion-contrib/datafusion-functions-json/blob/cb1ba7a80a84e10a4d658f3100eae8f6bca2ced9/LICENSE
//
// [Credit]: https://github.com/datafusion-contrib/datafusion-functions-json/blob/78c5abbf7222510ff221517f5d2e3c344969da98/src/json_length.rs

use std::any::Any;
use std::sync::{Arc, OnceLock};

use datafusion::arrow::array::{ArrayRef, UInt64Array, UInt64Builder};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use jiter::Peek;

use crate::scalar::json::common::{
    get_err, invoke, jiter_json_find, return_type_check, GetError, InvokeResult, JsonPath,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct JsonLength {
    signature: Signature,
    aliases: [String; 2],
}

impl Default for JsonLength {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonLength {
    pub fn new() -> Self {
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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        return_type_check(arg_types, self.name(), DataType::UInt64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke::<UInt64Array>(&args.args, jiter_json_length)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

pub fn json_length_udf() -> Arc<ScalarUDF> {
    static STATIC_JSON_LENGTH: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    STATIC_JSON_LENGTH
        .get_or_init(|| Arc::new(ScalarUDF::new_from_impl(JsonLength::new())))
        .clone()
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

    fn finish(mut builder: Self::Builder) -> Result<ArrayRef> {
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
