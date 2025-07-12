/// [Credit]: <https://github.com/apache/datafusion/blob/b10b820acb6ad92b5d69810e3d4de0ef6f2d6a87/datafusion/functions-nested/src/cardinality.rs>
use std::any::Any;
use std::sync::Arc;

use arrow::array::{Int32Array, Int64Array};
use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::cast::{as_large_list_array, as_list_array, as_map_array};
use datafusion_common::{exec_err, plan_err, Result, ScalarValue};
use datafusion_expr::{
    ArrayFunctionSignature, ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion_expr_common::signature::ArrayFunctionArgument;

// expr_fn::cardinality doesn't fully match expected behavior.
// Spark's cardinality function seems to be the same as the size function.
// `cardinality`: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.cardinality.html
// `size`: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.size.html
#[derive(Debug)]
pub struct SparkSize {
    signature: Signature,
    is_array_size: bool,
    is_legacy_cardinality: bool,
}

impl Default for SparkSize {
    fn default() -> Self {
        Self::new(false, false)
    }
}

impl SparkSize {
    pub fn new(is_array_size: bool, is_legacy_cardinality: bool) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::ArraySignature(ArrayFunctionSignature::Array {
                        arguments: vec![ArrayFunctionArgument::Array],
                        array_coercion: None,
                    }),
                    TypeSignature::ArraySignature(ArrayFunctionSignature::MapArray),
                ],
                Volatility::Immutable,
            ),
            is_array_size,
            is_legacy_cardinality,
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
        Ok(match (self.is_array_size, &arg_types[0]) {
            (_, DataType::List(_))
            | (_, DataType::FixedSizeList(_, _))
            | (_, DataType::ListView(_))
            | (false, DataType::Map(_, _)) => DataType::Int32,
            (false, DataType::LargeList(_)) | (false, DataType::LargeListView(_)) => {
                DataType::Int64
            }
            (false, _) => {
                return plan_err!(
                    "The size function can only accept List/LargeList/FixedSizeList/Map"
                );
            }
            (true, _) => {
                return plan_err!(
                    "The array_size function can only accept List/ListView/FixedSizeList"
                );
            }
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        if args.len() != 1 {
            return exec_err!("size expects one argument");
        }
        let is_scalar = matches!(args.first(), Some(ColumnarValue::Scalar(_)));
        let args = ColumnarValue::values_to_arrays(&args)?;

        let legacy_cardinality = self.is_legacy_cardinality.then_some(-1);

        let result = match &args[0].data_type() {
            DataType::List(_) | DataType::FixedSizeList(_, _) | DataType::ListView(_) => {
                Ok(Arc::new(
                    as_list_array(&args[0])?
                        .iter()
                        .map(|opt_arr| opt_arr.map(|arr| arr.len() as i32).or(legacy_cardinality))
                        .collect::<Int32Array>(),
                ) as ArrayRef)
            }
            DataType::Map(_, _) => Ok(Arc::new(
                as_map_array(&args[0])?
                    .iter()
                    .map(|opt_arr| opt_arr.map(|arr| arr.len() as i32).or(legacy_cardinality))
                    .collect::<Int32Array>(),
            ) as ArrayRef),
            DataType::LargeList(_) | DataType::LargeListView(_) => Ok(Arc::new(
                as_large_list_array(&args[0])?
                    .iter()
                    .map(|opt_arr| opt_arr.map(|arr| arr.len() as i64))
                    .collect::<Int64Array>(),
            ) as ArrayRef),
            other => {
                exec_err!("size does not support type '{:?}'", other)
            }
        };

        if is_scalar {
            // If all inputs are scalar, keeps output as scalar
            let result = result.and_then(|arr| ScalarValue::try_from_array(&arr, 0));
            result.map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    }
}
