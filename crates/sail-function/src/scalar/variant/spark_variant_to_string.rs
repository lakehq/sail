//! Spark-compatible `CAST(variant AS STRING)`.
//!
//! This differs from `variant_to_json` (`to_json`): Spark's cast unquotes scalar
//! strings and maps a VOID (JSON `null`) value to SQL NULL, whereas `to_json`
//! always produces a JSON document.

use std::sync::Arc;

use arrow::array::StringViewBuilder;
use arrow_schema::DataType;
use datafusion::common::{exec_datafusion_err, exec_err};
use datafusion::error::Result;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use parquet_variant::Variant;
use parquet_variant_compute::VariantArray;
use parquet_variant_json::VariantToJson;

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};
use crate::scalar::variant::utils::helper::try_field_as_variant_array;

/// Appends one variant to `builder` using Spark's `CAST(variant AS STRING)`
/// semantics, which differ from `to_json`:
/// - a VOID (JSON `null`) value appends SQL NULL (not the literal `"null"`)
/// - a scalar string appends the raw string (not the quoted JSON literal)
/// - everything else (numbers, booleans, objects, arrays) renders as JSON
///
/// String and short-string variants are written straight into the builder (no
/// intermediate `String`); only the JSON fallback allocates.
///
/// NOTE: large doubles still render via `to_json_string` (full decimal expansion)
/// rather than Spark's scientific notation — that number-formatting gap is tracked
/// separately.
fn append_variant_as_string(v: &Variant, builder: &mut StringViewBuilder) -> Result<()> {
    match v {
        Variant::Null => builder.append_null(),
        Variant::String(s) => builder.append_value(s),
        Variant::ShortString(s) => builder.append_value(s.as_str()),
        _ => builder.append_value(v.to_json_string()?),
    }
    Ok(())
}

/// Converts a variant `ColumnarValue` to a Utf8View string using Spark's
/// `CAST(variant AS STRING)` semantics (see [`append_variant_as_string`]).
pub fn variant_to_string_cast_columnar(arg: &ColumnarValue) -> Result<ColumnarValue> {
    match arg {
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Null => Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(None))),
            ScalarValue::Struct(variant_array) => {
                let variant_array = VariantArray::try_new(variant_array.as_ref())?;
                if variant_array.is_empty() {
                    return exec_err!(
                        "Cannot cast empty VariantArray to STRING: the array must contain at least one element"
                    );
                }
                let mut builder = StringViewBuilder::with_capacity(1);
                if variant_array.is_null(0) {
                    builder.append_null();
                } else {
                    append_variant_as_string(&variant_array.value(0), &mut builder)?;
                }
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &builder.finish(),
                    0,
                )?))
            }
            _ => Err(unsupported_data_type_exec_err(
                "variant_to_string",
                "VARIANT",
                &scalar.data_type(),
            )),
        },
        ColumnarValue::Array(arr) => match arr.data_type() {
            DataType::Struct(_) => {
                let variant_array = VariantArray::try_new(arr.as_ref())?;
                let mut builder = StringViewBuilder::with_capacity(variant_array.len());
                for variant in variant_array.iter() {
                    match variant {
                        Some(v) => append_variant_as_string(&v, &mut builder)?,
                        None => builder.append_null(),
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
            unsupported => Err(unsupported_data_type_exec_err(
                "variant_to_string",
                "VARIANT",
                unsupported,
            )),
        },
    }
}

/// `CAST(variant AS STRING)` UDF — wraps [`variant_to_string_cast_columnar`].
/// Distinct from `variant_to_json` because Spark's cast unquotes scalar strings
/// and maps VOID to SQL NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkVariantToStringUdf {
    signature: Signature,
}

impl SparkVariantToStringUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for SparkVariantToStringUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for SparkVariantToStringUdf {
    fn name(&self) -> &str {
        "variant_to_string"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8View)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let field = args
            .arg_fields
            .first()
            .ok_or_else(|| exec_datafusion_err!("missing argument field metadata"))?;
        try_field_as_variant_array(field.as_ref())?;
        variant_to_string_cast_columnar(&args.args[0])
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return Err(invalid_arg_count_exec_err(
                "variant_to_string",
                (1, 1),
                arg_types.len(),
            ));
        }
        Ok(arg_types.to_vec())
    }
}
