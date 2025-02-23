/// [Credit]: <https://github.com/apache/datafusion/blob/b10b820acb6ad92b5d69810e3d4de0ef6f2d6a87/datafusion/functions-nested/src/cardinality.rs>
use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, GenericListArray, OffsetSizeTrait, UInt64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::cast::{as_large_list_array, as_list_array, as_map_array};
use datafusion_common::{exec_err, plan_err, Result};
use datafusion_expr::{
    ArrayFunctionSignature, ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

use crate::extension::function::functions_nested_utils::{
    compute_array_dims, make_scalar_function,
};

// expr_fn::cardinality doesn't fully match expected behavior.
// Spark's cardinality function seems to be the same as the size function.
// `cardinality`: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.cardinality.html
// `size`: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.size.html
#[derive(Debug)]
pub struct SparkSize {
    signature: Signature,
}

impl Default for SparkSize {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSize {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::ArraySignature(ArrayFunctionSignature::Array),
                    TypeSignature::ArraySignature(ArrayFunctionSignature::MapArray),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkSize {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_size"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(match arg_types[0] {
            DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Map(_, _) => DataType::UInt64,
            _ => {
                return plan_err!(
                    "The size function can only accept List/LargeList/FixedSizeList/Map."
                );
            }
        })
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(size_inner)(args)
    }
}

pub fn size_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("size expects one argument");
    }
    match &args[0].data_type() {
        DataType::List(_) => {
            let list_array = as_list_array(&args[0])?;
            generic_list_size::<i32>(list_array)
        }
        DataType::LargeList(_) => {
            let list_array = as_large_list_array(&args[0])?;
            generic_list_size::<i64>(list_array)
        }
        DataType::Map(_, _) => {
            let map_array = as_map_array(&args[0])?;
            let result: UInt64Array = map_array
                .iter()
                .map(|opt_arr| opt_arr.map(|arr| arr.len() as u64))
                .collect();
            Ok(Arc::new(result))
        }
        other => {
            exec_err!("size does not support type '{:?}'", other)
        }
    }
}

fn generic_list_size<O: OffsetSizeTrait>(array: &GenericListArray<O>) -> Result<ArrayRef> {
    let result = array
        .iter()
        .map(|arr| match compute_array_dims(arr)? {
            Some(vector) => Ok(Some(vector.iter().map(|x| x.unwrap()).product::<u64>())),
            None => Ok(Some(0)),
        })
        .collect::<Result<UInt64Array>>()?;
    Ok(Arc::new(result) as ArrayRef)
}
