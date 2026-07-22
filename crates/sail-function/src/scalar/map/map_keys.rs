/// [Credit]: <https://github.com/apache/datafusion/blob/main/datafusion/functions-nested/src/map_keys.rs>
/// Adapted from DataFusion's `map_keys`: DataFusion makes the result array and its
/// elements nullable (to sync with DuckDB), whereas Spark keeps the array
/// non-nullable when the map is non-nullable and never produces null keys.
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

/// Spark-compatible `map_keys`: unlike DataFusion's `map_keys`, the result array
/// is non-nullable when the map is non-nullable, and the key elements are
/// non-nullable (map keys are never null in Spark).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMapKeys {
    signature: Signature,
}

impl Default for SparkMapKeys {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkMapKeys {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::ArraySignature(ArrayFunctionSignature::MapArray),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkMapKeys {
    fn name(&self) -> &str {
        "map_keys"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [map_field] = args.arg_fields else {
            return exec_err!("map_keys: expected one argument");
        };

        let DataType::Map(entries_field, _) = map_field.data_type() else {
            return exec_err!(
                "map_keys: expected map argument, got {:?}",
                map_field.data_type()
            );
        };

        let DataType::Struct(fields) = entries_field.data_type() else {
            return exec_err!("map_keys: expected map entries to be a struct");
        };

        let key_type = fields
            .first()
            .map(|field| field.data_type().clone())
            .unwrap_or(DataType::Null);

        Ok(Arc::new(Field::new(
            self.name(),
            DataType::List(Arc::new(Field::new_list_field(key_type, false))),
            map_field.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(map_keys_inner, vec![])(&args.args)
    }
}

fn map_keys_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [map_arg] = take_function_args("map_keys", args)?;

    let map_array = match map_arg.data_type() {
        DataType::Map(_, _) => as_map_array(&map_arg)?,
        _ => return exec_err!("map_keys: expected map argument"),
    };

    Ok(Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(map_array.key_type().clone(), false)),
        map_array.offsets().clone(),
        Arc::clone(map_array.keys()),
        map_array.nulls().cloned(),
    )))
}
