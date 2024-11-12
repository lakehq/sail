/// TODO: This file is no longer needed after datafusion-functions-json upgrades to DataFusion 43.0.0
/// [Credit]: <https://github.com/datafusion-contrib/datafusion-functions-json/blob/2a7c5b28ac7e32a0c8704427d66d732b583fd84e/src/json_as_text.rs>
use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use jiter::Peek;

use crate::extension::function::functions_json_utils::{
    get_err, invoke, jiter_json_find, return_type_check, GetError, JsonPath,
};

#[derive(Debug)]
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

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        return_type_check(arg_types, self.name(), DataType::Utf8)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
        invoke::<StringArray, String>(
            args,
            jiter_json_as_text,
            |c| Ok(Arc::new(c) as ArrayRef),
            ScalarValue::Utf8,
            true,
        )
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
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
