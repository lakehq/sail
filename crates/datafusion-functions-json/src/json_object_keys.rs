use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, ListBuilder, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use jiter::Peek;

use crate::common::{get_err, invoke, jiter_json_find, return_type_check, GetError, InvokeResult, JsonPath};
use crate::common_macros::make_udf_function;

make_udf_function!(
    JsonObjectKeys,
    json_object_keys,
    json_data path,
    r"Get the keys of a JSON object as an array."
);

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct JsonObjectKeys {
    signature: Signature,
    aliases: [String; 2],
}

impl Default for JsonObjectKeys {
    fn default() -> Self {
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

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        return_type_check(
            arg_types,
            self.name(),
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        )
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        invoke::<BuildListArray>(&args.args, jiter_json_object_keys)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
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

    fn finish(mut builder: Self::Builder) -> DataFusionResult<ArrayRef> {
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

fn jiter_json_object_keys(opt_json: Option<&str>, path: &[JsonPath]) -> Result<Vec<String>, GetError> {
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
