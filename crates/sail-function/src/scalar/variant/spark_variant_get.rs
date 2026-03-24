use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use parquet_variant::VariantPath;
use parquet_variant_compute::{variant_get, GetOptions};

/// Spark-compatible `variant_get(variant, path, type)` function.
///
/// Extracts a value from a Variant column at the given JSON path and casts it
/// to the specified type. Raises an error if the value cannot be cast.
///
/// Reference: <https://spark.apache.org/docs/latest/api/sql/index.html#variant_get>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkVariantGet {
    signature: Signature,
    /// If true, return NULL instead of error on cast failure (try_variant_get)
    safe: bool,
}

impl SparkVariantGet {
    pub fn new(safe: bool) -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
            safe,
        }
    }
}

impl ScalarUDFImpl for SparkVariantGet {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        if self.safe {
            "try_variant_get"
        } else {
            "variant_get"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // return_field_from_args is used instead
        datafusion_common::internal_err!("return_field_from_args should be used")
    }

    fn return_field_from_args(&self, args: datafusion_expr::ReturnFieldArgs) -> Result<Arc<Field>> {
        // The third argument (type string) determines the return type
        let type_str = args
            .scalar_arguments
            .get(2)
            .and_then(|opt| opt.as_ref())
            .and_then(|sv| sv.try_as_str().flatten())
            .unwrap_or("string");

        let dt = spark_type_to_arrow(type_str)?;
        Ok(Arc::new(Field::new(self.name(), dt, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        // Extract path string (must be scalar/constant)
        let path_str = match &args[1] {
            ColumnarValue::Scalar(scalar) => match scalar.try_as_str().flatten() {
                Some(s) => s.to_string(),
                None => return exec_err!("{}: path must be a non-null string", self.name()),
            },
            _ => return exec_err!("{}: path must be a constant string", self.name()),
        };

        // Extract type string (must be scalar/constant)
        let type_str = match &args[2] {
            ColumnarValue::Scalar(scalar) => match scalar.try_as_str().flatten() {
                Some(s) => s.to_string(),
                None => return exec_err!("{}: type must be a non-null string", self.name()),
            },
            _ => return exec_err!("{}: type must be a constant string", self.name()),
        };

        // Parse the Spark path: strip leading "$." or "$"
        let clean_path = path_str
            .strip_prefix("$.")
            .or_else(|| path_str.strip_prefix("$"))
            .unwrap_or(&path_str);

        let variant_path = if clean_path.is_empty() {
            VariantPath::default()
        } else {
            VariantPath::from(clean_path)
        };

        // Parse the type string to Arrow DataType
        let target_type = spark_type_to_arrow(&type_str)?;
        let target_field = Arc::new(Field::new("result", target_type.clone(), true));

        // Get the variant array
        let variant_arr = match &args[0] {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(s) => s.to_array()?,
        };

        // Build options
        let mut options = GetOptions::new_with_path(variant_path).with_as_type(Some(target_field));

        if self.safe {
            options = options.with_cast_options(datafusion::arrow::compute::CastOptions {
                safe: true,
                ..Default::default()
            });
        }

        // Execute
        let result = variant_get(&variant_arr, options).map_err(|e| {
            datafusion_common::DataFusionError::Execution(format!("{}: {e}", self.name()))
        })?;

        // If input was scalar, return scalar
        if matches!(&args[0], ColumnarValue::Scalar(_)) {
            let scalar = ScalarValue::try_from_array(&result, 0)?;
            Ok(ColumnarValue::Scalar(scalar))
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }
}

/// Converts Spark type strings to Arrow DataType.
fn spark_type_to_arrow(type_str: &str) -> Result<DataType> {
    match type_str.to_lowercase().as_str() {
        "boolean" => Ok(DataType::Boolean),
        "byte" | "tinyint" => Ok(DataType::Int8),
        "short" | "smallint" => Ok(DataType::Int16),
        "int" | "integer" => Ok(DataType::Int32),
        "long" | "bigint" => Ok(DataType::Int64),
        "float" => Ok(DataType::Float32),
        "double" => Ok(DataType::Float64),
        "string" => Ok(DataType::Utf8),
        "binary" => Ok(DataType::Binary),
        "date" => Ok(DataType::Date32),
        other => exec_err!(
            "variant_get: unsupported target type '{other}'. Supported: boolean, byte, short, int, long, float, double, string, binary, date"
        ),
    }
}
