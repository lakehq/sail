use std::sync::Arc;

use arrow::array::{Array, ArrayRef, ListArray};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::cast::as_map_array;
use datafusion_common::utils::take_function_args;
use datafusion_common::{exec_err, internal_err, Result};
use datafusion_expr::{
    ArrayFunctionSignature, ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, TypeSignature, Volatility,
};
use datafusion_functions::utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMapEntries {
    signature: Signature,
}

impl Default for SparkMapEntries {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkMapEntries {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::ArraySignature(ArrayFunctionSignature::MapArray),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkMapEntries {
    fn name(&self) -> &str {
        "map_entries"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [map_field] = args.arg_fields else {
            return exec_err!("map_entries: expected one argument");
        };

        let DataType::Map(entries_field, _) = map_field.data_type() else {
            return exec_err!(
                "map_entries: expected map argument, got {:?}",
                map_field.data_type()
            );
        };

        Ok(Arc::new(Field::new(
            self.name(),
            DataType::List(Arc::new(Field::new_list_field(
                entries_field.data_type().clone(),
                entries_field.is_nullable(),
            ))),
            map_field.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(map_entries_inner, vec![])(&args.args)
    }
}

fn map_entries_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [map_arg] = take_function_args("map_entries", args)?;

    let map_array = match map_arg.data_type() {
        DataType::Map(_, _) => as_map_array(&map_arg)?,
        _ => return exec_err!("map_entries: expected map argument"),
    };

    let DataType::Map(entries_field, _) = map_arg.data_type() else {
        unreachable!("map_arg data type was checked above")
    };

    Ok(Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(
            entries_field.data_type().clone(),
            entries_field.is_nullable(),
        )),
        map_array.offsets().clone(),
        Arc::new(map_array.entries().clone()),
        map_array.nulls().cloned(),
    )))
}
