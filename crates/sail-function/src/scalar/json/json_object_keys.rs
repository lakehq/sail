// https://github.com/datafusion-contrib/datafusion-functions-json/blob/cb1ba7a80a84e10a4d658f3100eae8f6bca2ced9/LICENSE
//
// [Credit]: https://github.com/datafusion-contrib/datafusion-functions-json/blob/78c5abbf7222510ff221517f5d2e3c344969da98/src/json_object_keys.rs

use std::any::Any;
use std::sync::{Arc, OnceLock};

use datafusion::arrow::array::{ArrayRef, ListBuilder, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use jiter::Peek;

use crate::scalar::json::common::{
    get_err, invoke, jiter_json_find, return_type_check, GetError, InvokeResult, JsonPath,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct JsonObjectKeys {
    signature: Signature,
    aliases: [String; 2],
}

impl Default for JsonObjectKeys {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonObjectKeys {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: ["json_object_keys".to_string(), "json_keys".to_string()],
        }
    }
}

impl ScalarUDFImpl for JsonObjectKeys {
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
        return_type_check(
            arg_types,
            self.name(),
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        )
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke::<BuildListArray>(&args.args, jiter_json_object_keys)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

pub fn json_object_keys_udf() -> Arc<ScalarUDF> {
    static STATIC_JSON_OBJECT_KEYS: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    STATIC_JSON_OBJECT_KEYS
        .get_or_init(|| Arc::new(ScalarUDF::new_from_impl(JsonObjectKeys::new())))
        .clone()
}

/// Struct used to build a `ListArray` from the result of `jiter_json_object_keys`.
#[derive(Debug)]
struct BuildListArray;

impl InvokeResult for BuildListArray {
    type Item = Vec<String>;

    type Builder = ListBuilder<StringBuilder>;

    const ACCEPT_DICT_RETURN: bool = true;

    fn builder(capacity: usize) -> Self::Builder {
        let values_builder = StringBuilder::new();
        ListBuilder::with_capacity(values_builder, capacity)
    }

    fn append_value(builder: &mut Self::Builder, value: Option<Self::Item>) {
        builder.append_option(value.map(|v| v.into_iter().map(Some)));
    }

    fn finish(mut builder: Self::Builder) -> Result<ArrayRef> {
        Ok(Arc::new(builder.finish()))
    }

    fn scalar(value: Option<Self::Item>) -> ScalarValue {
        keys_to_scalar(value)
    }
}

fn keys_to_scalar(opt_keys: Option<Vec<String>>) -> ScalarValue {
    let values_builder = StringBuilder::new();
    let mut builder = ListBuilder::new(values_builder);
    if let Some(keys) = opt_keys {
        for value in keys {
            builder.values().append_value(value);
        }
        builder.append(true);
    } else {
        builder.append(false);
    }
    let array = builder.finish();
    ScalarValue::List(Arc::new(array))
}

fn jiter_json_object_keys(
    opt_json: Option<&str>,
    path: &[JsonPath],
) -> Result<Vec<String>, GetError> {
    if let Some((mut jiter, peek)) = jiter_json_find(opt_json, path) {
        match peek {
            Peek::Object => {
                let mut opt_key = jiter.known_object()?;

                let mut keys = Vec::new();
                while let Some(key) = opt_key {
                    keys.push(key.to_string());
                    jiter.next_skip()?;
                    opt_key = jiter.next_key()?;
                }
                Ok(keys)
            }
            _ => get_err!(),
        }
    } else {
        get_err!()
    }
}
