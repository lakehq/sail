use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, PrimitiveArray};
use datafusion::arrow::compute::kernels::length::{bit_length, length};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Int32Type, Int64Type};
use datafusion_common::Result;
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions::utils::make_scalar_function;

use crate::error::generic_internal_err;

/// Spark measures the byte length of both string and binary data, unlike DataFusion's
/// `octet_length` and `bit_length`, whose signatures accept string data only. Feeding them
/// binary data coerces it to UTF-8 first, which rejects data that is not valid UTF-8.
///
/// The Arrow kernels already measure both, from the offset buffer alone, and already wrap
/// on overflow the way Spark does. They return `Int64` for the large variants, whereas Spark
/// always returns `Int32`.
macro_rules! define_length_udf {
    ($udf:ident, $name:expr_2021, $kernel:expr_2021) => {
        #[derive(Debug, PartialEq, Eq, Hash)]
        pub struct $udf {
            signature: Signature,
        }

        impl Default for $udf {
            fn default() -> Self {
                Self::new()
            }
        }

        impl $udf {
            pub fn new() -> Self {
                Self {
                    signature: Signature::uniform(
                        1,
                        vec![
                            DataType::Utf8,
                            DataType::LargeUtf8,
                            DataType::Utf8View,
                            DataType::Binary,
                            DataType::LargeBinary,
                            DataType::BinaryView,
                        ],
                        Volatility::Immutable,
                    ),
                }
            }
        }

        impl ScalarUDFImpl for $udf {
            fn name(&self) -> &str {
                $name
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                Err(generic_internal_err(
                    $name,
                    "`return_type` should not be called, call `return_field_from_args` instead",
                ))
            }

            fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
                let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
                Ok(Arc::new(Field::new(self.name(), DataType::Int32, nullable)))
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                make_scalar_function(|args: &[ArrayRef]| into_int32($kernel(&args[0])?), vec![])(
                    &args.args,
                )
            }
        }
    };
}

define_length_udf!(SparkOctetLength, "spark_octet_length", length);
define_length_udf!(SparkBitLength, "spark_bit_length", bit_length);

/// The large variants measure into `Int64`; Spark returns `Int32` for every input type.
/// The conversion truncates rather than nullifying, so a value whose length overflows
/// `Int32` wraps, which is what Spark does.
fn into_int32(lengths: ArrayRef) -> Result<ArrayRef> {
    match lengths.data_type() {
        DataType::Int64 => {
            let lengths: PrimitiveArray<Int32Type> = lengths
                .as_primitive::<Int64Type>()
                .unary(|length| length as i32);
            Ok(Arc::new(lengths))
        }
        _ => Ok(lengths),
    }
}
