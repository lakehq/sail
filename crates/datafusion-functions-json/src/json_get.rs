use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::array::UnionArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DataFusionResult;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion::scalar::ScalarValue;
use jiter::{Jiter, NumberAny, NumberInt, Peek};

use crate::common::InvokeResult;
use crate::common::{get_err, invoke, jiter_json_find, return_type_check, GetError, JsonPath};
use crate::common_macros::make_udf_function;
use crate::common_union::{JsonUnion, JsonUnionField};

make_udf_function!(
    JsonGet,
    json_get,
    json_data path,
    r#"Get a value from a JSON string by its "path""#
);

// build_typed_get!(JsonGet, "json_get", Union, Float64Array, jiter_json_get_float);

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct JsonGet {
    signature: Signature,
    aliases: [String; 1],
}

impl Default for JsonGet {
    fn default() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: ["json_get".to_string()],
        }
    }
}

impl ScalarUDFImpl for JsonGet {
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
        return_type_check(arg_types, self.name(), JsonUnion::data_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        invoke::<JsonUnion>(&args.args, jiter_json_get_union)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

impl InvokeResult for JsonUnion {
    type Item = JsonUnionField;

    type Builder = JsonUnion;

    const ACCEPT_DICT_RETURN: bool = true;

    fn builder(capacity: usize) -> Self::Builder {
        JsonUnion::new(capacity)
    }

    fn append_value(builder: &mut Self::Builder, value: Option<Self::Item>) {
        if let Some(value) = value {
            builder.push(value);
        } else {
            builder.push_none();
        }
    }

    fn finish(builder: Self::Builder) -> DataFusionResult<ArrayRef> {
        let array: UnionArray = builder.try_into()?;
        Ok(Arc::new(array) as ArrayRef)
    }

    fn scalar(value: Option<Self::Item>) -> ScalarValue {
        JsonUnionField::scalar_value(value)
    }
}

fn jiter_json_get_union(opt_json: Option<&str>, path: &[JsonPath]) -> Result<JsonUnionField, GetError> {
    if let Some((mut jiter, peek)) = jiter_json_find(opt_json, path) {
        build_union(&mut jiter, peek)
    } else {
        get_err!()
    }
}

fn build_union(jiter: &mut Jiter, peek: Peek) -> Result<JsonUnionField, GetError> {
    match peek {
        Peek::Null => {
            jiter.known_null()?;
            Ok(JsonUnionField::JsonNull)
        }
        Peek::True | Peek::False => {
            let value = jiter.known_bool(peek)?;
            Ok(JsonUnionField::Bool(value))
        }
        Peek::String => {
            let value = jiter.known_str()?;
            Ok(JsonUnionField::Str(value.to_owned()))
        }
        Peek::Array => {
            let start = jiter.current_index();
            jiter.known_skip(peek)?;
            let array_slice = jiter.slice_to_current(start);
            let array_string = std::str::from_utf8(array_slice)?;
            Ok(JsonUnionField::Array(array_string.to_owned()))
        }
        Peek::Object => {
            let start = jiter.current_index();
            jiter.known_skip(peek)?;
            let object_slice = jiter.slice_to_current(start);
            let object_string = std::str::from_utf8(object_slice)?;
            Ok(JsonUnionField::Object(object_string.to_owned()))
        }
        _ => match jiter.known_number(peek)? {
            NumberAny::Int(NumberInt::Int(value)) => Ok(JsonUnionField::Int(value)),
            NumberAny::Int(NumberInt::BigInt(_)) => todo!("BigInt not supported yet"),
            NumberAny::Float(value) => Ok(JsonUnionField::Float(value)),
        },
    }
}
