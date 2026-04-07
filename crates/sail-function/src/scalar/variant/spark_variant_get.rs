use std::any::Any;
use std::sync::Arc;

/// [Credit]: <https://github.com/datafusion-contrib/datafusion-variant/blob/main/src/variant_get.rs>
use arrow_schema::{ArrowError, DataType, Field, FieldRef};
use datafusion::arrow::datatypes::TimeUnit;
use datafusion_common::{arrow_datafusion_err, exec_datafusion_err, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use parquet_variant::VariantPath;
use parquet_variant_compute::{variant_get, GetOptions, VariantType};

use crate::error::{generic_exec_err, invalid_arg_count_exec_err, unsupported_data_type_exec_err};
use crate::scalar::variant::utils::helper::{try_field_as_variant_array, try_parse_string_scalar};

/// Converts a scalar type-hint value to an Arrow [`FieldRef`].
///
/// First tries Spark-compatible type names (e.g. `"int"`, `"string"`, `"decimal(10,2)"`),
/// then falls back to Arrow's `DataType::parse()` for full type coverage.
fn type_hint_from_scalar(field_name: &str, scalar: &ScalarValue) -> Result<FieldRef> {
    let type_name = match scalar {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => value.as_str(),
        other => {
            return Err(generic_exec_err(
                field_name,
                &format!(
                    "type must be a non-null string literal, got {}",
                    other.data_type()
                ),
            ));
        }
    };

    let dt = spark_type_to_arrow(type_name)?;
    Ok(Arc::new(Field::new(field_name, dt, true)))
}

/// Strips Spark's JSON-path prefix from a path string.
/// - `$.field` → `field`
/// - `$[0]` → `[0]`
/// - `$` → `` (empty, root access)
fn strip_spark_path_prefix(path: &str) -> &str {
    path.strip_prefix("$.")
        .or_else(|| path.strip_prefix("$"))
        .unwrap_or(path)
}

fn build_get_options<'a>(path: VariantPath<'a>, as_type: &Option<FieldRef>) -> GetOptions<'a> {
    match as_type {
        Some(field) => GetOptions::new_with_path(path).with_as_type(Some(field.clone())),
        None => GetOptions::new_with_path(path),
    }
}

/// Shared return-field logic for variant_get / try_variant_get.
///
/// - 3-arg form: resolves the type hint to a concrete Arrow DataType.
/// - 2-arg form: returns a Variant struct (Binary for PySpark compat).
fn return_field_for_variant_get(name: &str, args: &ReturnFieldArgs) -> Result<FieldRef> {
    // Validate path (arg 1) is a constant non-null string
    if let Some(path_opt) = args.scalar_arguments.get(1) {
        if let Some(sv) = path_opt.as_ref() {
            if sv.try_as_str().flatten().is_none() {
                return Err(generic_exec_err(name, "path must be a non-null string"));
            }
        }
    }

    // 3-arg form: variant_get(variant, path, type) → typed result
    if args.scalar_arguments.len() >= 3 {
        if let Some(type_sv) = args.scalar_arguments.get(2).and_then(|opt| opt.as_ref()) {
            return type_hint_from_scalar(name, type_sv);
        }
        return Err(generic_exec_err(
            name,
            "type must be a non-null constant string",
        ));
    }

    // 2-arg form: return Variant struct with extension metadata.
    // Use Binary (not BinaryView) for PySpark compatibility.
    let variant_struct = DataType::Struct(
        vec![
            Field::new("metadata", DataType::Binary, false),
            Field::new("value", DataType::Binary, true),
        ]
        .into(),
    );
    let field = Field::new(name, variant_struct, true).with_extension_type(VariantType);
    Ok(Arc::new(field))
}

/// Shared invoke logic for `variant_get` / `try_variant_get`.
///
/// Handles both scalar and array variant inputs (path is always scalar in Spark).
/// Based on datafusion-variant's `invoke_variant_get` pattern.
fn invoke_variant_get(args: ScalarFunctionArgs, name: &str, safe: bool) -> Result<ColumnarValue> {
    let variant_field = args
        .arg_fields
        .first()
        .ok_or_else(|| exec_datafusion_err!("expected argument field type"))?;

    try_field_as_variant_array(variant_field.as_ref())?;

    // Extract path string (must be scalar/constant)
    let path_str = match args.args.get(1) {
        Some(ColumnarValue::Scalar(scalar)) => match try_parse_string_scalar(scalar)? {
            Some(s) => s.clone(),
            None => return Err(generic_exec_err(name, "path must be a non-null string")),
        },
        _ => return Err(generic_exec_err(name, "path must be a constant string")),
    };

    // Extract type hint (optional — 2-arg form returns variant)
    let type_str = if args.args.len() >= 3 {
        match &args.args[2] {
            ColumnarValue::Scalar(scalar) => {
                try_parse_string_scalar(scalar)?.map(|s| s.to_string())
            }
            _ => return Err(generic_exec_err(name, "type must be a constant string")),
        }
    } else {
        None
    };

    // Parse the Spark path: strip leading "$." or "$"
    // Validate: "$." alone is invalid (trailing dot with no field name)
    if path_str == "$." {
        return Err(generic_exec_err(
            name,
            "path '$.' is not a valid variant extraction path",
        ));
    }
    let clean_path = strip_spark_path_prefix(&path_str);
    let variant_path = if clean_path.is_empty() {
        VariantPath::default()
    } else {
        VariantPath::try_from(clean_path)?
    };

    // Resolve the target type.
    // For Decimal/Timestamp, parquet-variant doesn't support direct extraction,
    // so we extract as an intermediate type and then cast.
    // Decimal: extract as Float64 then cast (loses precision beyond ~15 digits;
    //   a more precise approach would be String→Decimal parsing).
    // Timestamp: extract as Utf8 then cast (preserves full precision).
    let final_type = type_str.as_deref().map(spark_type_to_arrow).transpose()?;
    let (extract_field, needs_post_cast) = match &final_type {
        Some(DataType::Decimal128(_, _)) => (
            Some(Arc::new(Field::new(name, DataType::Float64, true))),
            true,
        ),
        Some(DataType::Timestamp(_, _)) => {
            (Some(Arc::new(Field::new(name, DataType::Utf8, true))), true)
        }
        Some(dt) => (Some(Arc::new(Field::new(name, dt.clone(), true))), false),
        None => (None, false),
    };

    // Build options
    let mut options = build_get_options(variant_path, &extract_field);
    if safe {
        options = options.with_cast_options(datafusion::arrow::compute::CastOptions {
            safe: true,
            ..Default::default()
        });
    }

    // Get the variant array (expand scalar to single-element array)
    let variant_arr = match &args.args[0] {
        ColumnarValue::Array(arr) => arr.clone(),
        ColumnarValue::Scalar(s) => s.to_array()?,
    };

    // Execute
    let result = variant_get(&variant_arr, options)
        .map_err(|e| datafusion_common::DataFusionError::Execution(format!("{name}: {e}")))?;

    // Fallback: if extracting as numeric type returned all NULLs,
    // try extracting as Boolean and cast (Spark casts true→1, false→0)
    let result = if !needs_post_cast && result.null_count() == result.len() && !result.is_empty() {
        if let Some(ref dt) = final_type {
            if dt.is_integer() {
                let bool_field = Some(Arc::new(Field::new(name, DataType::Boolean, true)));
                let bool_options = build_get_options(
                    if clean_path.is_empty() {
                        VariantPath::default()
                    } else {
                        VariantPath::try_from(clean_path).map_err(|e| arrow_datafusion_err!(e))?
                    },
                    &bool_field,
                );
                let bool_result = variant_get(&variant_arr, bool_options).map_err(|e| {
                    datafusion_common::DataFusionError::Execution(format!("{name}: {e}"))
                })?;
                if bool_result.null_count() < bool_result.len() {
                    if safe {
                        datafusion::arrow::compute::cast_with_options(
                            &bool_result,
                            dt,
                            &datafusion::arrow::compute::CastOptions {
                                safe: true,
                                ..Default::default()
                            },
                        )?
                    } else {
                        datafusion::arrow::compute::cast(&bool_result, dt)?
                    }
                } else {
                    result
                }
            } else {
                result
            }
        } else {
            result
        }
    } else {
        result
    };

    // Post-cast for types parquet-variant can't extract directly
    // Use safe cast for try_variant_get to return NULL instead of error
    let result = if needs_post_cast {
        if let Some(ref dt) = final_type {
            if safe {
                datafusion::arrow::compute::cast_with_options(
                    &result,
                    dt,
                    &datafusion::arrow::compute::CastOptions {
                        safe: true,
                        ..Default::default()
                    },
                )?
            } else {
                datafusion::arrow::compute::cast(&result, dt)?
            }
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
    if matches!(&args.args[0], ColumnarValue::Scalar(_)) {
        let scalar = ScalarValue::try_from_array(&result, 0)?;
        Ok(ColumnarValue::Scalar(scalar))
    } else {
        Ok(ColumnarValue::Array(result))
    }
}

/// Spark-compatible `variant_get(variant, path, type)` / `try_variant_get` function.
///
/// Extracts a value from a Variant column at the given JSON path and casts it
/// to the specified type. `try_variant_get` returns NULL instead of error on cast failure.
///
/// Reference: <https://spark.apache.org/docs/latest/api/sql/index.html#variant_get>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkVariantGet {
    signature: Signature,
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
        datafusion_common::internal_err!("return_field_from_args should be used")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        return_field_for_variant_get(self.name(), &args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 && arg_types.len() != 3 {
            return Err(invalid_arg_count_exec_err(
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
                return Err(unsupported_data_type_exec_err(
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
                    return Err(unsupported_data_type_exec_err(
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
        invoke_variant_get(args, self.name(), self.safe)
    }
}

/// Converts Spark type strings to Arrow DataType.
///
/// Supports Spark-compatible names first, then falls back to Arrow's `DataType::parse()`
/// for full type coverage (e.g. `"Int64"`, `"Decimal128(10,2)"`).
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
                    generic_exec_err(
                        "spark_type_to_arrow",
                        &format!("invalid decimal precision: '{p}'"),
                    )
                })?;
                let scale = s.parse::<i8>().map_err(|_| {
                    generic_exec_err(
                        "spark_type_to_arrow",
                        &format!("invalid decimal scale: '{s}'"),
                    )
                })?;
                Ok(DataType::Decimal128(precision, scale))
            }
            _ => Err(generic_exec_err(
                "spark_type_to_arrow",
                &format!("invalid decimal type: '{type_str}'. Expected: decimal(precision, scale)"),
            )),
        };
    }

    // Spark-compatible type names
    match trimmed {
        "boolean" => return Ok(DataType::Boolean),
        "byte" | "tinyint" => return Ok(DataType::Int8),
        "short" | "smallint" => return Ok(DataType::Int16),
        "int" | "integer" => return Ok(DataType::Int32),
        "long" | "bigint" => return Ok(DataType::Int64),
        "float" => return Ok(DataType::Float32),
        "double" => return Ok(DataType::Float64),
        "decimal" => return Ok(DataType::Decimal128(10, 0)),
        "string" => return Ok(DataType::Utf8),
        "binary" => return Ok(DataType::Binary),
        "date" => return Ok(DataType::Date32),
        "timestamp" | "timestamp_ntz" => {
            return Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
        }
        _ => {}
    }

    // Fallback to Arrow's DataType::parse() for full type coverage.
    // Use original (non-lowercased) string since Arrow type names are case-sensitive.
    match type_str.trim().parse::<DataType>() {
        Ok(data_type) => Ok(data_type),
        Err(ArrowError::ParseError(e)) => Err(exec_datafusion_err!("{e}")),
        Err(e) => Err(arrow_datafusion_err!(e)),
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{ArrayRef, BinaryArray, Int64Array, StructArray};
    use arrow_schema::{Field, Fields};
    use datafusion_common::{exec_err, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
    use parquet_variant::Variant;
    use parquet_variant_compute::{VariantArrayBuilder, VariantType};
    use parquet_variant_json::JsonToVariant;

    use super::*;

    fn variant_scalar_from_json(json: serde_json::Value) -> Result<ScalarValue> {
        let mut builder = VariantArrayBuilder::new(1);
        builder.append_json(json.to_string().as_str())?;
        let arr: StructArray = builder.build().into();
        Ok(ScalarValue::Struct(Arc::new(arr)))
    }

    fn variant_array_from_json_rows(rows: &[serde_json::Value]) -> Result<ArrayRef> {
        let mut builder = VariantArrayBuilder::new(rows.len());
        for row in rows {
            builder.append_json(row.to_string().as_str())?;
        }
        let arr: StructArray = builder.build().into();
        Ok(Arc::new(arr) as ArrayRef)
    }

    fn variant_arg_field() -> FieldRef {
        Arc::new(
            Field::new("input", DataType::Struct(Fields::empty()), true)
                .with_extension_type(VariantType),
        )
    }

    fn build_args(
        variant: ColumnarValue,
        path: &str,
        type_hint: Option<&str>,
    ) -> ScalarFunctionArgs {
        let mut args = vec![
            variant,
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(path.to_string()))),
        ];
        let mut arg_fields: Vec<FieldRef> = vec![
            variant_arg_field(),
            Arc::new(Field::new("path", DataType::Utf8, false)),
        ];

        let type_sv = type_hint.map(|t| ScalarValue::Utf8(Some(t.to_string())));
        if let Some(ref sv) = type_sv {
            args.push(ColumnarValue::Scalar(sv.clone()));
            arg_fields.push(Arc::new(Field::new("type", DataType::Utf8, false)));
        }

        let return_field = Arc::new(Field::new("result", DataType::Int64, true));
        ScalarFunctionArgs {
            args,
            return_field,
            arg_fields,
            number_rows: Default::default(),
            config_options: Default::default(),
        }
    }

    #[test]
    fn test_scalar_extract_with_type_hint() -> Result<()> {
        let variant = variant_scalar_from_json(serde_json::json!({"age": 50}))?;
        let args = build_args(ColumnarValue::Scalar(variant), "$.age", Some("int"));

        let udf = SparkVariantGet::new(false);
        let result = udf.invoke_with_args(args)?;

        let ColumnarValue::Scalar(ScalarValue::Int32(Some(v))) = result else {
            return exec_err!("expected Int32 scalar, got {result:?}");
        };
        assert_eq!(v, 50);
        Ok(())
    }

    #[test]
    fn test_scalar_extract_nested() -> Result<()> {
        let variant = variant_scalar_from_json(serde_json::json!({"a": {"b": {"c": 99}}}))?;
        let args = build_args(ColumnarValue::Scalar(variant), "$.a.b.c", Some("int"));

        let udf = SparkVariantGet::new(false);
        let result = udf.invoke_with_args(args)?;

        let ColumnarValue::Scalar(ScalarValue::Int32(Some(v))) = result else {
            return exec_err!("expected Int32 scalar");
        };
        assert_eq!(v, 99);
        Ok(())
    }

    #[test]
    fn test_array_extract_with_type_hint() -> Result<()> {
        let variant_arr = variant_array_from_json_rows(&[
            serde_json::json!({"age": 50}),
            serde_json::json!({"age": 60}),
        ])?;
        let args = build_args(ColumnarValue::Array(variant_arr), "$.age", Some("long"));

        let udf = SparkVariantGet::new(false);
        let result = udf.invoke_with_args(args)?;

        let ColumnarValue::Array(arr) = result else {
            return exec_err!("expected array");
        };
        let values = arr.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
            datafusion_common::DataFusionError::Execution("expected Int64Array".into())
        })?;
        assert_eq!(values.value(0), 50);
        assert_eq!(values.value(1), 60);
        Ok(())
    }

    #[test]
    fn test_two_arg_returns_variant() -> Result<()> {
        let variant = variant_scalar_from_json(serde_json::json!({"name": "sail"}))?;
        let args = build_args(ColumnarValue::Scalar(variant), "$.name", None);

        let udf = SparkVariantGet::new(false);
        let result = udf.invoke_with_args(args)?;

        // 2-arg form returns a Variant struct (Binary, not BinaryView)
        let ColumnarValue::Scalar(ScalarValue::Struct(struct_arr)) = result else {
            return exec_err!("expected Struct scalar");
        };
        let metadata_arr = struct_arr
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Execution("expected BinaryArray".into())
            })?;
        let value_arr = struct_arr
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Execution("expected BinaryArray".into())
            })?;
        let v = Variant::try_new(metadata_arr.value(0), value_arr.value(0))?;
        assert_eq!(v, Variant::from("sail"));
        Ok(())
    }

    #[test]
    fn test_try_variant_get_wrong_type_returns_null() -> Result<()> {
        let variant = variant_scalar_from_json(serde_json::json!("hello"))?;
        let args = build_args(ColumnarValue::Scalar(variant), "$", Some("int"));

        let udf = SparkVariantGet::new(true); // safe mode
        let result = udf.invoke_with_args(args)?;

        let ColumnarValue::Scalar(ScalarValue::Int32(None)) = result else {
            return exec_err!("expected NULL Int32");
        };
        Ok(())
    }

    #[test]
    fn test_missing_field_returns_null() -> Result<()> {
        let variant = variant_scalar_from_json(serde_json::json!({"a": 1}))?;
        let args = build_args(ColumnarValue::Scalar(variant), "$.b", Some("int"));

        let udf = SparkVariantGet::new(false);
        let result = udf.invoke_with_args(args)?;

        let ColumnarValue::Scalar(ScalarValue::Int32(None)) = result else {
            return exec_err!("expected NULL Int32");
        };
        Ok(())
    }

    #[test]
    fn test_arrow_type_parse_fallback() -> Result<()> {
        // Arrow type name "Int64" should work via DataType::parse() fallback
        let dt = spark_type_to_arrow("Int64")?;
        assert_eq!(dt, DataType::Int64);
        Ok(())
    }

    #[test]
    fn test_spark_type_names() -> Result<()> {
        assert_eq!(spark_type_to_arrow("boolean")?, DataType::Boolean);
        assert_eq!(spark_type_to_arrow("byte")?, DataType::Int8);
        assert_eq!(spark_type_to_arrow("short")?, DataType::Int16);
        assert_eq!(spark_type_to_arrow("int")?, DataType::Int32);
        assert_eq!(spark_type_to_arrow("long")?, DataType::Int64);
        assert_eq!(spark_type_to_arrow("float")?, DataType::Float32);
        assert_eq!(spark_type_to_arrow("double")?, DataType::Float64);
        assert_eq!(spark_type_to_arrow("string")?, DataType::Utf8);
        assert_eq!(spark_type_to_arrow("binary")?, DataType::Binary);
        assert_eq!(spark_type_to_arrow("date")?, DataType::Date32);
        assert_eq!(
            spark_type_to_arrow("decimal(10,2)")?,
            DataType::Decimal128(10, 2)
        );
        Ok(())
    }
}
