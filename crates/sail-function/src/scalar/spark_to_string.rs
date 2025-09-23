use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, GenericStringBuilder, OffsetSizeTrait, StringViewBuilder,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_expr::ScalarFunctionArgs;
use sail_common_datafusion::display::{ArrayFormatter, FormatOptions};
use sail_common_datafusion::utils::items::ItemTaker;

macro_rules! define_to_string_udf {
    ($udf:ident, $name:expr, $return_type:expr, $func:expr $(,)?) => {
        #[derive(Debug, PartialEq, Eq, Hash)]
        pub struct $udf {
            signature: Signature,
            options: FormatOptions<'static>,
        }

        impl Default for $udf {
            fn default() -> Self {
                Self::new()
            }
        }

        impl $udf {
            pub fn new() -> Self {
                Self {
                    signature: Signature::any(1, Volatility::Immutable),
                    options: FormatOptions::default(),
                }
            }
        }

        impl ScalarUDFImpl for $udf {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn name(&self) -> &str {
                $name
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                Ok($return_type)
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                let ScalarFunctionArgs { args, .. } = args;
                let args = ColumnarValue::values_to_arrays(&args)?;
                let arg = args.one()?;
                let array = $func(&arg, &self.options)?;
                Ok(ColumnarValue::Array(array))
            }
        }
    };
}

define_to_string_udf!(
    SparkToUtf8,
    "spark_to_utf8",
    DataType::Utf8,
    value_to_string::<i32>,
);

define_to_string_udf!(
    SparkToLargeUtf8,
    "spark_to_large_utf8",
    DataType::LargeUtf8,
    value_to_string::<i64>,
);

define_to_string_udf!(
    SparkToUtf8View,
    "spark_to_utf8_view",
    DataType::Utf8View,
    value_to_string_view,
);

// [Credit]: <https://github.com/apache/arrow-rs/blob/main/arrow-cast/src/cast/string.rs>

fn value_to_string<O: OffsetSizeTrait>(
    array: &dyn Array,
    options: &FormatOptions<'static>,
) -> Result<ArrayRef> {
    let mut builder = GenericStringBuilder::<O>::new();
    let formatter = ArrayFormatter::try_new(array, options)?;
    let nulls = array.nulls();
    for i in 0..array.len() {
        match nulls.map(|x| x.is_null(i)).unwrap_or_default() {
            true => builder.append_null(),
            false => {
                formatter.value(i).write(&mut builder)?;
                // tell the builder the row is finished
                builder.append_value("");
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn value_to_string_view(array: &dyn Array, options: &FormatOptions<'static>) -> Result<ArrayRef> {
    let mut builder = StringViewBuilder::with_capacity(array.len());
    let formatter = ArrayFormatter::try_new(array, options)?;
    let nulls = array.nulls();
    // buffer to avoid reallocating on each value
    // TODO: replace with write to builder after https://github.com/apache/arrow-rs/issues/6373
    let mut buffer = String::new();
    for i in 0..array.len() {
        match nulls.map(|x| x.is_null(i)).unwrap_or_default() {
            true => builder.append_null(),
            false => {
                // write to buffer first and then copy into target array
                buffer.clear();
                formatter.value(i).write(&mut buffer)?;
                builder.append_value(&buffer)
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}
