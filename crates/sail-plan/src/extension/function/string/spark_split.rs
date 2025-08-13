use std::any::Any;
use std::sync::Arc;

use crate::extension::function::error_utils::{generic_exec_err, unsupported_data_types_exec_err};
use crate::extension::function::functions_nested_utils::downcast_arg;
use crate::extension::function::functions_utils::make_scalar_function;
use arrow::array::{Array, ArrayRef, Int32Array, ListBuilder, StringArray, StringBuilder};
use arrow::datatypes::{DataType, Field};
use datafusion::common::DataFusionError;
use datafusion_common::Result;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::signature::{Signature, Volatility};
use regex::Regex;
use std::any::type_name;

#[derive(Debug)]
pub struct SparkSplit {
    signature: Signature,
}

impl Default for SparkSplit {
    fn default() -> Self {
        Self::new()
    }
}
impl SparkSplit {
    pub const NAME: &'static str = "split";
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkSplit {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        Self::NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new_list_field(
            DataType::Utf8,
            true,
        ))))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8, DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8] => {
                Ok(vec![
                    arg_types[0].clone(),
                    arg_types[1].clone(),
                    DataType::Int32,
                ])
            }
            [DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8, DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8, DataType::Int32 | DataType::Int64 | DataType::UInt32 | DataType::UInt64] => {
                Ok(vec![
                    arg_types[0].clone(),
                    arg_types[1].clone(),
                    arg_types[2].clone(),
                ])
            }
            _ => Err(unsupported_data_types_exec_err(
                Self::NAME,
                "Must be of type (String, String) or (String, String, Int)",
                arg_types,
            )),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(spark_split_inner, vec![])(&args)
    }
}

pub fn spark_split_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let values: &StringArray = downcast_arg!(&args[0], StringArray);
    let format: &StringArray = downcast_arg!(&args[1], StringArray);
    let limit: &Int32Array = match args.get(2) {
        Some(limit) => downcast_arg!(limit, Int32Array),
        None => &Int32Array::from(vec![0; values.len()]),
    };

    let mut builder = ListBuilder::new(StringBuilder::new());

    for i in 0..values.len() {
        if values.is_null(i) {
            builder.append_null();
        } else {
            let values: &str = values.value(i);
            let format: &str = format.value(i);
            let limit = if limit.is_null(i) { 0 } else { limit.value(i) };
            let values_format = split_to_array(values, format, limit)?;
            builder.append_value(values_format);
        }
    }

    let array = builder.finish();
    Ok(Arc::new(array))

    // let mut children_scalars: Vec<Vec<ScalarValue>> =
    //     vec![Vec::with_capacity(values.len()); format.len()];
    // let mut validity: Vec<bool> = Vec::with_capacity(values.len());
    //
    // for  i in 0..values.len() {
    //     if values.is_null(i) {
    //         for j in 0..children_scalars.len() {
    //             children_scalars[j].push(ScalarValue::try_new_null(format.value_type()));
    //         }
    //         validity.push(false);
    //     } else {
    //         let values: &str = values.value(i);
    //         let format: &str = format.value(i);
    //         let values_format = split_to_array(values, format);
    //         for (j, value) in values_format.into_iter().enumerate() {
    //             children_scalars[j].push(value);
    //         }
    //         validity.push(true);
    //     }
    // }
    //
    // let children_arrays: Vec<ArrayRef> = children_scalars
    //     .into_iter()
    //     .map(|arr| ScalarValue::iter_to_array(arr))
    //     .collect::<Result<Vec<_>>>()?;
    //
    // Ok(Arc::new(GenericListBuilder::<i32>::new(children_arrays, validity).finish()))
}

pub fn split_to_array(value: &str, format: &str, limit: i32) -> Result<Vec<Option<String>>> {
    let format: Regex =
        Regex::new(format).map_err(|_| generic_exec_err(SparkSplit::NAME, "Invalid regex"))?;
    let values = format.split(value).collect::<Vec<&str>>();
    let values = if limit <= 0 {
        values
    } else {
        values[..limit as usize].to_vec()
    };
    Ok(values
        .iter()
        .map(|value| Some(value.to_string()))
        .collect::<Vec<Option<String>>>())
}
