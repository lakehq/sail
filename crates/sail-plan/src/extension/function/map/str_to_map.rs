use std::any::Any;
use std::sync::Arc;

use arrow::array::{AsArray, ListArray};
use datafusion::arrow::array::{Array, ArrayRef, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion_common::utils::take_function_args;
use datafusion_common::{internal_err, Result};
use datafusion_expr::function::Hint;
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion_functions::utils::make_scalar_function;

use crate::extension::function::map::map_function::map_from_arrays_inner;
use crate::extension::function::string::spark_split::{parse_regex, split_to_array, SparkSplit};

#[derive(Debug)]
pub struct StrToMap {
    signature: Signature,
}

impl Default for StrToMap {
    fn default() -> Self {
        Self::new()
    }
}

impl StrToMap {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8View, DataType::Utf8, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Utf8, DataType::Utf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for StrToMap {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "str_to_map"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        let (key_type, value_type) = (DataType::Utf8, DataType::Utf8);

        Ok(DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    // the key must not be nullable
                    Field::new("key", key_type.clone(), false),
                    Field::new("value", value_type.clone(), true),
                ])),
                false, // the entry is not nullable
            )),
            false, // the keys are not sorted
        ))
    }

    fn invoke_with_args(&self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [strs, pair_delims, key_value_delims] = take_function_args("str_to_map", args.args)?;
        let split_func = SparkSplit::new();

        let args_for_split = vec![strs, pair_delims];
        let fields_for_split = vec![args.arg_fields[0].clone(), args.arg_fields[1].clone()];
        let scalars_for_split = args_for_split
            .iter()
            .map(|arg| match arg {
                ColumnarValue::Scalar(scalar) => Some(scalar),
                _ => None,
            })
            .collect::<Vec<_>>();
        let split_return_field = split_func.return_field_from_args(ReturnFieldArgs {
            arg_fields: fields_for_split.as_slice(),
            scalar_arguments: scalars_for_split.as_slice(),
        })?;

        let split_result = split_func.invoke_with_args(ScalarFunctionArgs {
            args: args_for_split,
            arg_fields: fields_for_split,
            number_rows: args.number_rows,
            return_field: split_return_field,
        })?;

        make_scalar_function(str_to_map_inner, vec![Hint::Pad, Hint::AcceptsSingular])(&[
            split_result,
            key_value_delims,
        ])
    }
}

fn str_to_map_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [pair_strs, key_value_delims] = take_function_args("str_to_map", args)?;
    let pair_lists = pair_strs.as_list::<i32>();
    match (
        pair_lists.values().as_any().downcast_ref::<StringArray>(),
        key_value_delims.as_any().downcast_ref::<StringArray>(),
    ) {
        (Some(pair_strs), Some(key_value_delim_strs)) => {
            let pair_offsets = pair_lists.offsets().as_ref();
            let result_row_cnt = pair_offsets.last().cloned().unwrap_or(0) as usize;
            let mut keys = Vec::with_capacity(result_row_cnt);
            let mut values = Vec::with_capacity(result_row_cnt);

            let key_value_delim_scalar = (key_value_delim_strs.len() == 1
                && key_value_delim_strs.is_valid(0))
            .then(|| parse_regex(key_value_delim_strs.value(0)))
            .transpose()?;

            for rn in 0..pair_offsets.len() - 1 {
                if pair_lists.is_null(rn) {
                    continue;
                }
                let key_value_delim = key_value_delim_scalar.as_ref().map_or_else(
                    || parse_regex(key_value_delim_strs.value(rn)),
                    |delim| Ok(delim.clone()),
                )?;

                for pair_rn in pair_offsets[rn]..pair_offsets[rn + 1] {
                    let pair = pair_strs.value(pair_rn as usize);
                    let key_value = split_to_array(pair, &key_value_delim, 2)?;
                    keys.push(key_value.first().cloned().flatten());
                    values.push(key_value.get(1).cloned().flatten());
                }
            }

            let create_list_array = |values: Vec<Option<String>>, name: &str, nullable: bool| {
                Arc::new(ListArray::new(
                    Arc::new(Field::new(name, DataType::Utf8, nullable)),
                    pair_lists.offsets().clone(),
                    Arc::new(StringArray::from_iter(values)),
                    pair_lists.nulls().cloned(),
                ))
            };

            let keys = create_list_array(keys, "key", false);
            let values = create_list_array(values, "value", true);
            map_from_arrays_inner(&[keys, values])
        }
        _ => internal_err!("str_to_map: failed to downcast arguments to StringArrays"),
    }
}
