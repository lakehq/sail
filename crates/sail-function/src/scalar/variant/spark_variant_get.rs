use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use parquet_variant::VariantPath;
use parquet_variant_compute::{variant_get, GetOptions};

use crate::error::generic_exec_err;

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
            signature: Signature::user_defined(Volatility::Immutable),
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
        // Validate path (arg 1) is a constant non-null string
        if let Some(path_opt) = args.scalar_arguments.get(1) {
            if let Some(sv) = path_opt.as_ref() {
                if sv.try_as_str().flatten().is_none() {
                    return Err(generic_exec_err(
                        self.name(),
                        "path must be a non-null string",
                    ));
                }
            }
        }

        // 2-arg form: variant_get(variant, path) → returns Variant
        // 3-arg form: variant_get(variant, path, type) → returns the specified type
        if args.scalar_arguments.len() >= 3 {
            if let Some(type_sv) = args.scalar_arguments.get(2).and_then(|opt| opt.as_ref()) {
                if let Some(t) = type_sv.try_as_str().flatten() {
                    let dt = spark_type_to_arrow(t)?;
                    return Ok(Arc::new(Field::new(self.name(), dt, true)));
                }
            }
            return Err(generic_exec_err(
                self.name(),
                "type must be a non-null constant string",
            ));
        }

        // 2-arg: return Variant type with extension metadata
        // so downstream functions (is_variant_null, etc.) recognize it
        use parquet_variant_compute::VariantType;
        let variant_struct = DataType::Struct(
            vec![
                // Use Binary (not BinaryView) for PySpark compatibility,
                // matching parse_json's output format
                Field::new("metadata", DataType::Binary, false),
                Field::new("value", DataType::Binary, true),
            ]
            .into(),
        );
        let field = Field::new(self.name(), variant_struct, true).with_extension_type(VariantType);
        Ok(Arc::new(field))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 && arg_types.len() != 3 {
            return Err(crate::error::invalid_arg_count_exec_err(
                self.name(),
                (2, 3),
                arg_types.len(),
            ));
        }
        // arg[0]: variant (struct), arg[1]: path (string), arg[2]: type (string, optional)
        let mut result = vec![arg_types[0].clone()];
        // Coerce path to Utf8
        result.push(match &arg_types[1] {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => arg_types[1].clone(),
            DataType::Null => DataType::Utf8,
            _ => {
                return Err(crate::error::unsupported_data_type_exec_err(
                    self.name(),
                    "string",
                    &arg_types[1],
                ))
            }
        });
        if arg_types.len() == 3 {
            result.push(match &arg_types[2] {
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => arg_types[2].clone(),
                DataType::Null => DataType::Utf8,
                _ => {
                    return Err(crate::error::unsupported_data_type_exec_err(
                        self.name(),
                        "string",
                        &arg_types[2],
                    ))
                }
            });
        }
        Ok(result)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        // Extract path string (must be scalar/constant)
        let name = self.name();
        let path_str = match &args[1] {
            ColumnarValue::Scalar(scalar) => match scalar.try_as_str().flatten() {
                Some(s) => s.to_string(),
                None => return Err(generic_exec_err(name, "path must be a non-null string")),
            },
            _ => return Err(generic_exec_err(name, "path must be a constant string")),
        };

        // Extract type string (optional — 2-arg form returns variant)
        let type_str = if args.len() >= 3 {
            match &args[2] {
                ColumnarValue::Scalar(scalar) => {
                    scalar.try_as_str().flatten().map(|s| s.to_string())
                }
                _ => return Err(generic_exec_err(name, "type must be a constant string")),
            }
        } else {
            None
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

        // 2-arg: return variant, 3-arg: cast to specified type
        // For Decimal/Timestamp, parquet-variant doesn't support direct extraction,
        // so we extract as an intermediate type (double/string) and then cast.
        let final_type = type_str.as_deref().map(spark_type_to_arrow).transpose()?;
        let (extract_field, needs_post_cast) = match &final_type {
            Some(DataType::Decimal128(_, _)) => {
                // Extract as double, then cast to decimal
                (
                    Some(Arc::new(Field::new("result", DataType::Float64, true))),
                    true,
                )
            }
            Some(DataType::Timestamp(_, _)) => {
                // Extract as string, then cast to timestamp
                (
                    Some(Arc::new(Field::new("result", DataType::Utf8, true))),
                    true,
                )
            }
            Some(dt) => (
                Some(Arc::new(Field::new("result", dt.clone(), true))),
                false,
            ),
            None => (None, false),
        };

        // Get the variant array
        let variant_arr = match &args[0] {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(s) => s.to_array()?,
        };

        // Build options
        let mut options = GetOptions::new_with_path(variant_path).with_as_type(extract_field);

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

        // Post-cast for types that parquet-variant can't extract directly
        let result = if needs_post_cast {
            if let Some(ref dt) = final_type {
                datafusion::arrow::compute::cast(&result, dt)?
            } else {
                result
            }
        } else if type_str.is_none() {
            // 2-arg form: convert BinaryView→Binary for PySpark compatibility
            datafusion::arrow::compute::cast(
                &result,
                &DataType::Struct(
                    vec![
                        Field::new("metadata", DataType::Binary, false),
                        Field::new("value", DataType::Binary, true),
                    ]
                    .into(),
                ),
            )?
        } else {
            result
        };

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
/// TODO: Add support for array<T>, map<K,V>, struct<...>
fn spark_type_to_arrow(type_str: &str) -> Result<DataType> {
    let lower = type_str.to_lowercase();
    let trimmed = lower.trim();

    // Handle parameterized types: decimal(p,s)
    if let Some(params) = trimmed
        .strip_prefix("decimal(")
        .and_then(|s| s.strip_suffix(')'))
    {
        let parts: Vec<&str> = params.split(',').map(|s| s.trim()).collect();
        return match parts.as_slice() {
            [p, s] => {
                let precision = p.parse::<u8>().map_err(|_| {
                    generic_exec_err("variant_get", &format!("invalid decimal precision: '{p}'"))
                })?;
                let scale = s.parse::<i8>().map_err(|_| {
                    generic_exec_err("variant_get", &format!("invalid decimal scale: '{s}'"))
                })?;
                Ok(DataType::Decimal128(precision, scale))
            }
            _ => Err(generic_exec_err(
                "variant_get",
                &format!("invalid decimal type: '{type_str}'. Expected: decimal(precision, scale)"),
            )),
        };
    }

    match trimmed {
        "boolean" => Ok(DataType::Boolean),
        "byte" | "tinyint" => Ok(DataType::Int8),
        "short" | "smallint" => Ok(DataType::Int16),
        "int" | "integer" => Ok(DataType::Int32),
        "long" | "bigint" => Ok(DataType::Int64),
        "float" => Ok(DataType::Float32),
        "double" => Ok(DataType::Float64),
        "decimal" => Ok(DataType::Decimal128(10, 0)),
        "string" => Ok(DataType::Utf8),
        "binary" => Ok(DataType::Binary),
        "date" => Ok(DataType::Date32),
        "timestamp" => Ok(DataType::Timestamp(
            datafusion::arrow::datatypes::TimeUnit::Microsecond,
            None,
        )),
        "timestamp_ntz" => Ok(DataType::Timestamp(
            datafusion::arrow::datatypes::TimeUnit::Microsecond,
            None,
        )),
        other => Err(generic_exec_err(
            "variant_get",
            &format!("unsupported target type '{other}'"),
        )),
    }
}
