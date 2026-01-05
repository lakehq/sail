use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, ListBuilder, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use jiter::Peek;

use crate::common::{get_err, invoke, jiter_json_find, return_type_check, GetError, InvokeResult, JsonPath};
use crate::common_macros::make_udf_function;

make_udf_function!(
    JsonGetArray,
    json_get_array,
    json_data path,
    r#"Get an arrow array from a JSON string by its "path""#
);

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct JsonGetArray {
    signature: Signature,
    aliases: [String; 1],
}

impl Default for JsonGetArray {
    fn default() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: ["json_get_array".to_string()],
        }
    }
}

impl ScalarUDFImpl for JsonGetArray {
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
            DataType::List(Arc::new(datafusion::arrow::datatypes::Field::new(
                "item",
                DataType::Utf8,
                true,
            ))),
        )
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        invoke::<BuildArrayList>(&args.args, jiter_json_get_array)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

#[derive(Debug)]
struct BuildArrayList;

impl InvokeResult for BuildArrayList {
    type Item = Vec<String>;

    type Builder = ListBuilder<StringBuilder>;

    const ACCEPT_DICT_RETURN: bool = false;

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
        let mut builder = ListBuilder::new(StringBuilder::new());

        if let Some(array_items) = value {
            for item in array_items {
                builder.values().append_value(item);
            }

            builder.append(true);
        } else {
            builder.append(false);
        }
        let array = builder.finish();
        ScalarValue::List(Arc::new(array))
    }
}

fn jiter_json_get_array(opt_json: Option<&str>, path: &[JsonPath]) -> Result<Vec<String>, GetError> {
    if let Some((mut jiter, peek)) = jiter_json_find(opt_json, path) {
        match peek {
            Peek::Array => {
                let mut peek_opt = jiter.known_array()?;
                let mut array_items: Vec<String> = Vec::new();

                while let Some(element_peek) = peek_opt {
                    // Get the raw JSON slice for each array element
                    let start = jiter.current_index();
                    jiter.known_skip(element_peek)?;
                    let slice = jiter.slice_to_current(start);
                    let element_str = std::str::from_utf8(slice)?.to_string();

                    array_items.push(element_str);
                    peek_opt = jiter.array_step()?;
                }

                Ok(array_items)
            }
            _ => get_err!(),
        }
    } else {
        get_err!()
    }
}
