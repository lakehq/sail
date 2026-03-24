// https://github.com/datafusion-contrib/datafusion-functions-json/blob/cb1ba7a80a84e10a4d658f3100eae8f6bca2ced9/LICENSE
//
// [Credit]: https://github.com/datafusion-contrib/datafusion-functions-json/blob/78c5abbf7222510ff221517f5d2e3c344969da98/src/json_as_text.rs

use std::any::Any;
use std::sync::{Arc, OnceLock};

use datafusion::arrow::array::{ArrayRef, StringArray, StringBuilder};
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
pub struct JsonAsText {
    signature: Signature,
    aliases: [String; 1],
}

impl Default for JsonAsText {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonAsText {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: ["json_as_text".to_string()],
        }
    }
}

impl ScalarUDFImpl for JsonAsText {
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
        return_type_check(arg_types, self.name(), DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke::<StringArray>(&args.args, jiter_json_as_text)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

pub fn json_as_text_udf() -> Arc<ScalarUDF> {
    static STATIC_JSON_AS_TEXT: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    STATIC_JSON_AS_TEXT
        .get_or_init(|| Arc::new(ScalarUDF::new_from_impl(JsonAsText::new())))
        .clone()
}

impl InvokeResult for StringArray {
    type Item = String;

    type Builder = StringBuilder;

    const ACCEPT_DICT_RETURN: bool = true;

    fn builder(capacity: usize) -> Self::Builder {
        StringBuilder::with_capacity(capacity, 0)
    }

    fn append_value(builder: &mut Self::Builder, value: Option<Self::Item>) {
        builder.append_option(value);
    }

    fn finish(mut builder: Self::Builder) -> Result<ArrayRef> {
        Ok(Arc::new(builder.finish()))
    }

    fn scalar(value: Option<Self::Item>) -> ScalarValue {
        ScalarValue::Utf8(value)
    }
}

fn jiter_json_as_text(opt_json: Option<&str>, path: &[JsonPath]) -> Result<String, GetError> {
    if let Some((mut jiter, peek)) = jiter_json_find(opt_json, path) {
        match peek {
            Peek::Null => {
                jiter.known_null()?;
                get_err!()
            }
            Peek::String => Ok(jiter.known_str()?.to_owned()),
            _ => {
                let start = jiter.current_index();
                jiter.known_skip(peek)?;
                let object_slice = jiter.slice_to_current(start);
                let object_string = std::str::from_utf8(object_slice)?;
                Ok(object_string.to_owned())
            }
        }
    } else {
        get_err!()
    }
}
