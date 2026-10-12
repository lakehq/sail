use std::sync::Arc;

/// [Credit]: <https://github.com/datafusion-contrib/datafusion-variant/blob/main/src/variant_get.rs>
use arrow_schema::{ArrowError, DataType, Field, FieldRef};
use chumsky::prelude::*;
use datafusion::arrow::datatypes::TimeUnit;
use datafusion_common::{Result, ScalarValue, arrow_datafusion_err, exec_datafusion_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use parquet_variant::{Variant, VariantPath, VariantPathElement};
use parquet_variant_compute::{GetOptions, VariantArray, VariantType, variant_get};
use parquet_variant_json::VariantToJson;
use sail_common_datafusion::variant::{VARIANT_VALUE_FIELD_NAME, variant_metadata_field};

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
    if let Some(path_opt) = args.scalar_arguments.get(1)
        && let Some(sv) = path_opt.as_ref()
        && sv.try_as_str().flatten().is_none()
    {
        return Err(generic_exec_err(name, "path must be a non-null string"));
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
            Field::new(VARIANT_VALUE_FIELD_NAME, DataType::Binary, true),
            variant_metadata_field(DataType::Binary, false),
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

    // Parse and validate the Spark path expression into a parquet-variant path.
    let variant_path = spark_path_to_variant_path(&path_str, name)?;

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
    let mut options = build_get_options(variant_path.clone(), &extract_field);
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

    if let Some(DataType::Timestamp(TimeUnit::Microsecond, timezone)) = &final_type {
        let result = cast_variant_timestamp(&variant_arr, variant_path, timezone.clone(), safe)?;
        return if matches!(&args.args[0], ColumnarValue::Scalar(_)) {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &result, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(result))
        };
    }

    // Execute
    let result = variant_get(&variant_arr, options)
        .map_err(|e| datafusion_common::DataFusionError::Execution(format!("{name}: {e}")))?;

    // Fallback: if extracting as numeric type returned all NULLs,
    // try extracting as Boolean and cast (Spark casts true→1, false→0)
    let result = if !needs_post_cast && result.null_count() == result.len() && !result.is_empty() {
        if let Some(ref dt) = final_type {
            if dt.is_integer() {
                let bool_field = Some(Arc::new(Field::new(name, DataType::Boolean, true)));
                let bool_options = build_get_options(variant_path.clone(), &bool_field);
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
                    Field::new(VARIANT_VALUE_FIELD_NAME, DataType::Binary, true),
                    variant_metadata_field(DataType::Binary, false),
                ]
                .into(),
            ),
        )?
    } else {
        result
    };

    // `variant_get` silently returns NULL when the value's runtime type cannot be
    // extracted as the requested type (e.g. an integer variant cast to TIME/DATE/
    // BINARY/ARRAY/MAP/STRUCT), rather than raising. Only a genuine JSON null
    // (`Variant::Null`) or an absent/Arrow-null source should produce a legitimate
    // NULL; anything else is a cast failure Spark reports as `INVALID_VARIANT_CAST`.
    // Scoped to the root path (`$`) of a `CAST ... AS <type>`, which is the shape
    // this divergence was found in; a sub-path lookup already has its own,
    // separately-verified null semantics that this must not disturb.
    if !needs_post_cast
        && type_str.is_some()
        && variant_path.is_empty()
        && let Some(dt) = &final_type
    {
        let variant_array = VariantArray::try_new(variant_arr.as_ref())?;
        for i in 0..result.len() {
            if result.is_null(i) && !variant_array.is_null(i) {
                let value = variant_array.value(i);
                if !matches!(value, Variant::Null) {
                    if safe {
                        continue;
                    }
                    let json = value.to_json_string()?;
                    let target = dt.to_string().to_ascii_uppercase();
                    return Err(exec_datafusion_err!(
                        "[INVALID_VARIANT_CAST] The variant value `{json}` cannot be cast into `\"{target}\"`. Please use `try_variant_get` instead."
                    ));
                }
            }
        }
    }

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

    pub fn safe(&self) -> bool {
        self.safe
    }
}

impl ScalarUDFImpl for SparkVariantGet {
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
        datafusion_common::internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
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
                ));
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
                    ));
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
            return Ok(DataType::Timestamp(TimeUnit::Microsecond, None));
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

fn spark_path_to_variant_path(path: &str, name: &str) -> Result<VariantPath<'static>> {
    spark_variant_path_parser()
        .parse(path)
        .into_result()
        .map_err(|errors| {
            let details = errors
                .into_iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join(", ");

            generic_exec_err(
                name,
                &format!("'{path}' is not a valid variant extraction path: {details}"),
            )
        })
}

fn quoted_field_name<'src>(
    quote: char,
) -> impl Parser<'src, &'src str, String, extra::Err<Rich<'src, char>>> + Copy {
    let escaped_char = just('\\').ignore_then(any());
    let regular_char = none_of([quote, '\\']);

    just(quote)
        .ignore_then(escaped_char.or(regular_char).repeated().collect::<String>())
        .then_ignore(just(quote))
}

fn spark_variant_path_parser<'src>()
-> impl Parser<'src, &'src str, VariantPath<'static>, extra::Err<Rich<'src, char>>> {
    let ident_field_name = text::ident().map(|s: &str| VariantPathElement::field(s.to_string()));

    let single_quoted_field_name = quoted_field_name('\'').map(VariantPathElement::field);
    let double_quoted_field_name = quoted_field_name('"').map(VariantPathElement::field);
    let backtick_quoted_field_name = quoted_field_name('`').map(VariantPathElement::field);

    let field = just('.').ignore_then(choice((
        ident_field_name,
        single_quoted_field_name,
        double_quoted_field_name,
        backtick_quoted_field_name,
    )));

    let bracket_field = just('[')
        .ignore_then(choice((
            single_quoted_field_name,
            double_quoted_field_name,
            backtick_quoted_field_name,
        )))
        .then_ignore(just(']'));

    let index = just('[')
        .ignore_then(text::int(10).try_map(|s: &str, span| {
            s.parse::<usize>()
                .map(VariantPathElement::index)
                .map_err(|e| Rich::custom(span, format!("invalid array index: {e}")))
        }))
        .then_ignore(just(']'));

    just('$')
        .ignore_then(
            choice((field, index, bracket_field))
                .repeated()
                .collect::<Vec<_>>(),
        )
        .then_ignore(end())
        .map(|segments| segments.into_iter().collect())
}

/// Spark VariantGet uses seconds for numeric values, with stricter overflow
/// checks than an ordinary numeric-to-timestamp cast (variantExpressions.scala:545).
fn decimal_timestamp_micros(integer: i128, scale: u8) -> Option<i64> {
    let micros = if scale <= 6 {
        integer.checked_mul(10_i128.checked_pow(u32::from(6 - scale))?)?
    } else {
        // Integer division truncates toward zero, as BigDecimal.toBigInteger does.
        integer / 10_i128.checked_pow(u32::from(scale - 6))?
    };
    i64::try_from(micros).ok()
}

fn float_timestamp_micros(seconds: f64) -> Option<i64> {
    let micros = seconds * 1_000_000.0;
    // Spark DoubleExactNumeric compares floating floor/ceil with long bounds.
    // The subsequent JVM conversion saturates at the rounded upper boundary.
    (micros.floor() <= i64::MAX as f64 && micros.ceil() >= i64::MIN as f64).then_some(micros as i64)
}

fn cast_variant_timestamp(
    input: &arrow::array::ArrayRef,
    path: VariantPath<'_>,
    timezone: Option<Arc<str>>,
    safe: bool,
) -> Result<arrow::array::ArrayRef> {
    use arrow::array::{Array, AsArray, TimestampMicrosecondArray};
    use arrow::datatypes::TimestampMicrosecondType;
    use parquet_variant::Variant;
    use parquet_variant_compute::VariantArray;
    use parquet_variant_json::VariantToJson;

    let extracted = variant_get(input, GetOptions::new_with_path(path))?;
    let variants = VariantArray::try_new(extracted.as_ref())?;
    let target = DataType::Timestamp(TimeUnit::Microsecond, timezone.clone());
    // Keep the existing text parser for string/date/timestamp variants. Numeric
    // values are converted below without going through floating point or text.
    let strings = variant_get(
        &extracted,
        GetOptions::new().with_as_type(Some(Arc::new(Field::new("value", DataType::Utf8, true)))),
    )?;
    let fallback = arrow::compute::cast_with_options(
        &strings,
        &target,
        &arrow::compute::CastOptions {
            safe: true,
            ..Default::default()
        },
    )?;
    let fallback = fallback.as_primitive::<TimestampMicrosecondType>();
    let mut builder = TimestampMicrosecondArray::builder(variants.len());
    for (index, value) in variants.iter().enumerate() {
        let Some(value) = value else {
            builder.append_null();
            continue;
        };
        if value == Variant::Null {
            builder.append_null();
            continue;
        }
        let numeric = match &value {
            Variant::Int8(value) => Some(i64::from(*value).checked_mul(1_000_000)),
            Variant::Int16(value) => Some(i64::from(*value).checked_mul(1_000_000)),
            Variant::Int32(value) => Some(i64::from(*value).checked_mul(1_000_000)),
            Variant::Int64(value) => Some(value.checked_mul(1_000_000)),
            Variant::Decimal4(value) => Some(decimal_timestamp_micros(
                i128::from(value.integer()),
                value.scale(),
            )),
            Variant::Decimal8(value) => Some(decimal_timestamp_micros(
                i128::from(value.integer()),
                value.scale(),
            )),
            Variant::Decimal16(value) => {
                Some(decimal_timestamp_micros(value.integer(), value.scale()))
            }
            Variant::Float(value) => Some(float_timestamp_micros(f64::from(*value))),
            Variant::Double(value) => Some(float_timestamp_micros(*value)),
            _ => None,
        };
        let micros = match numeric {
            // Cast.canAnsiCast forbids numeric -> TIMESTAMP_NTZ.
            Some(value) if timezone.is_some() => value,
            Some(_) => None,
            None => {
                if fallback.is_null(index) {
                    None
                } else {
                    Some(fallback.value(index))
                }
            }
        };
        if micros.is_none() && !safe {
            let target = if timezone.is_some() {
                "TIMESTAMP"
            } else {
                "TIMESTAMP_NTZ"
            };
            return Err(exec_datafusion_err!(
                "[INVALID_VARIANT_CAST] The variant value `{}` cannot be cast into `\"{}\"`. Please use `try_variant_get` instead.",
                value.to_json_string()?,
                target
            ));
        }
        builder.append_option(micros);
    }
    Ok(Arc::new(builder.finish().with_timezone_opt(timezone)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn variant_path(elements: Vec<VariantPathElement<'static>>) -> VariantPath<'static> {
        elements.into_iter().collect()
    }

    #[test]
    fn test_spark_variant_path_parser_accepts_valid_paths() -> Result<()> {
        assert_eq!(
            spark_path_to_variant_path("$", "variant_get")?,
            variant_path(vec![])
        );
        assert_eq!(
            spark_path_to_variant_path("$.a", "variant_get")?,
            variant_path(vec![VariantPathElement::field("a".to_string())])
        );
        assert_eq!(
            spark_path_to_variant_path("$.a[0]", "variant_get")?,
            variant_path(vec![
                VariantPathElement::field("a".to_string()),
                VariantPathElement::index(0),
            ])
        );
        assert_eq!(
            spark_path_to_variant_path("$[0]", "variant_get")?,
            variant_path(vec![VariantPathElement::index(0)])
        );
        assert_eq!(
            spark_path_to_variant_path("$.a.b.c", "variant_get")?,
            variant_path(vec![
                VariantPathElement::field("a".to_string()),
                VariantPathElement::field("b".to_string()),
                VariantPathElement::field("c".to_string()),
            ])
        );
        assert_eq!(
            spark_path_to_variant_path("$.a[0].b", "variant_get")?,
            variant_path(vec![
                VariantPathElement::field("a".to_string()),
                VariantPathElement::index(0),
                VariantPathElement::field("b".to_string()),
            ])
        );
        Ok(())
    }

    #[test]
    fn test_spark_variant_path_parser_accepts_quoted_field_names() -> Result<()> {
        assert_eq!(
            spark_path_to_variant_path("$.`a-b`", "variant_get")?,
            variant_path(vec![VariantPathElement::field("a-b".to_string())])
        );
        assert_eq!(
            spark_path_to_variant_path("$['a-b']", "variant_get")?,
            variant_path(vec![VariantPathElement::field("a-b".to_string())])
        );
        assert_eq!(
            spark_path_to_variant_path("$[\"a-b\"]", "variant_get")?,
            variant_path(vec![VariantPathElement::field("a-b".to_string())])
        );
        Ok(())
    }

    #[test]
    fn test_spark_variant_path_parser_keeps_quoted_dot_as_single_field() -> Result<()> {
        assert_eq!(
            spark_path_to_variant_path("$['a.b']", "variant_get")?,
            variant_path(vec![VariantPathElement::field("a.b".to_string())])
        );
        Ok(())
    }

    #[test]
    fn test_spark_variant_path_parser_keeps_quoted_brackets_as_single_field() -> Result<()> {
        assert_eq!(
            spark_path_to_variant_path("$['a[0]']", "variant_get")?,
            variant_path(vec![VariantPathElement::field("a[0]".to_string())])
        );
        Ok(())
    }

    #[test]
    fn test_spark_variant_path_parser_rejects_invalid_paths() -> Result<()> {
        assert!(spark_path_to_variant_path("$a[0]", "variant_get").is_err());
        assert!(spark_path_to_variant_path("$.", "variant_get").is_err());
        assert!(spark_path_to_variant_path("a[0]", "variant_get").is_err());
        assert!(spark_path_to_variant_path("$.a[]", "variant_get").is_err());
        Ok(())
    }

    #[test]
    fn test_spark_variant_path_parser_accepts_escaped_double_quote() -> Result<()> {
        assert_eq!(
            spark_path_to_variant_path("$[\"a\\\"b\"]", "variant_get")?,
            variant_path(vec![VariantPathElement::field("a\"b".to_string())])
        );
        Ok(())
    }

    #[test]
    fn test_spark_variant_path_parser_accepts_escaped_single_quote() -> Result<()> {
        assert_eq!(
            spark_path_to_variant_path("$['a\\'b']", "variant_get")?,
            variant_path(vec![VariantPathElement::field("a'b".to_string())])
        );
        Ok(())
    }

    #[test]
    fn test_spark_variant_path_parser_accepts_escaped_backtick() -> Result<()> {
        assert_eq!(
            spark_path_to_variant_path("$.`a\\`b`", "variant_get")?,
            variant_path(vec![VariantPathElement::field("a`b".to_string())])
        );
        Ok(())
    }

    #[test]
    fn test_spark_variant_path_parser_accepts_escaped_backslash() -> Result<()> {
        assert_eq!(
            spark_path_to_variant_path("$[\"a\\\\b\"]", "variant_get")?,
            variant_path(vec![VariantPathElement::field("a\\b".to_string())])
        );
        Ok(())
    }
}
