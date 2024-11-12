/// TODO: This file is no longer needed after datafusion-functions-json upgrades to DataFusion 43.0.0
/// [Credit]: <https://github.com/datafusion-contrib/datafusion-functions-json/blob/2a7c5b28ac7e32a0c8704427d66d732b583fd84e/src/json_length.rs>
use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, UInt64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use jiter::Peek;

use crate::extension::function::functions_json_utils::{
    get_err, invoke, jiter_json_find, return_type_check, GetError, JsonPath,
};

#[derive(Debug)]
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

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        return_type_check(arg_types, self.name(), DataType::UInt64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
        invoke::<UInt64Array, u64>(
            args,
            jiter_json_length,
            |c| Ok(Arc::new(c) as ArrayRef),
            ScalarValue::UInt64,
            true,
        )
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
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
