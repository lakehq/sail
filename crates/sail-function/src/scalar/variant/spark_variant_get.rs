use std::sync::Arc;

/// [Credit]: <https://github.com/datafusion-contrib/datafusion-variant/blob/main/src/variant_get.rs>
use arrow::array::{Array, ArrayRef, BooleanArray, Float32Array, Float64Array};
use arrow::compute::kernels::zip::zip;
use arrow_schema::{ArrowError, DataType, Field, FieldRef};
use chumsky::prelude::*;
use datafusion::arrow::datatypes::TimeUnit;
use datafusion_common::{Result, ScalarValue, arrow_datafusion_err, exec_datafusion_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use parquet_variant::{Variant, VariantPath, VariantPathElement};
use parquet_variant_compute::{GetOptions, VariantArray, VariantType, variant_get};
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

fn variant_decimal_as_float<T: std::str::FromStr>(value: Variant<'_, '_>) -> Option<T> {
    match value {
        // Parsing the exact decimal text avoids double rounding through an
        // intermediate integer-to-float conversion and matches Spark's
        // BigDecimal float/double casts.
        Variant::Decimal4(value) => value.to_string().parse().ok(),
        Variant::Decimal8(value) => value.to_string().parse().ok(),
        Variant::Decimal16(value) => value.to_string().parse().ok(),
        _ => None,
    }
}

/// parquet-variant currently leaves decimal-to-float extractions NULL. Fill
/// only those slots from an untyped extraction, preserving its handling of all
/// other values and casts.
fn fill_decimal_float_values(
    result: ArrayRef,
    input: &ArrayRef,
    path: &VariantPath<'_>,
    name: &str,
) -> Result<ArrayRef> {
    if result.null_count() == 0 || result.is_empty() {
        return Ok(result);
    }

    let variants = VariantArray::try_new(input.as_ref())?;

    match result.data_type() {
        DataType::Float32 => {
            let floats = result
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| exec_datafusion_err!("{name}: expected Float32 extraction"))?;
            let values = (0..result.len())
                .map(|index| {
                    if floats.is_valid(index) {
                        Ok(Some(floats.value(index)))
                    } else if variants.is_null(index) {
                        Ok(None)
                    } else {
                        let variant = variants.try_value(index)?;
                        let value = variant
                            .get_path(path)
                            .and_then(variant_decimal_as_float::<f32>);
                        Ok(value)
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(Float32Array::from(values)))
        }
        DataType::Float64 => {
            let floats = result
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| exec_datafusion_err!("{name}: expected Float64 extraction"))?;
            let values = (0..result.len())
                .map(|index| {
                    if floats.is_valid(index) {
                        Ok(Some(floats.value(index)))
                    } else if variants.is_null(index) {
                        Ok(None)
                    } else {
                        let variant = variants.try_value(index)?;
                        let value = variant
                            .get_path(path)
                            .and_then(variant_decimal_as_float::<f64>);
                        Ok(value)
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(Float64Array::from(values)))
        }
        _ => Ok(result),
    }
}

fn fill_boolean_integer_values(
    result: ArrayRef,
    input: &ArrayRef,
    path: &VariantPath<'_>,
    target_type: &DataType,
) -> Result<ArrayRef> {
    if result.null_count() == 0 || result.is_empty() || !target_type.is_integer() {
        return Ok(result);
    }

    let variants = VariantArray::try_new(input.as_ref())?;
    let mut has_boolean = false;
    let booleans = (0..result.len())
        .map(|index| {
            if result.is_valid(index) || variants.is_null(index) {
                return Ok(None);
            }
            let variant = variants.try_value(index)?;
            let value = variant.get_path(path).and_then(|value| value.as_boolean());
            has_boolean |= value.is_some();
            Ok(value)
        })
        .collect::<Result<BooleanArray>>()?;
    if !has_boolean {
        return Ok(result);
    }

    let fallback = datafusion::arrow::compute::cast(&booleans, target_type)?;
    let use_fallback =
        BooleanArray::from_iter((0..result.len()).map(|index| result.is_null(index)));
    Ok(zip(&use_fallback, &fallback, &result)?)
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

    // Timestamp is extracted as Utf8 and then cast. parquet-variant handles Decimal128
    // directly, including exact rescaling and overflow behavior.
    let final_type = type_str.as_deref().map(spark_type_to_arrow).transpose()?;
    let (extract_field, needs_post_cast) = match &final_type {
        Some(DataType::Timestamp(_, _)) => {
            (Some(Arc::new(Field::new(name, DataType::Utf8, true))), true)
        }
        Some(dt) => (Some(Arc::new(Field::new(name, dt.clone(), true))), false),
        None => (None, false),
    };
    let extracts_float = extract_field
        .as_ref()
        .is_some_and(|field| matches!(field.data_type(), DataType::Float32 | DataType::Float64));

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

    // Execute
    let result = variant_get(&variant_arr, options)
        .map_err(|e| datafusion_common::DataFusionError::Execution(format!("{name}: {e}")))?;

    let result = if extracts_float {
        fill_decimal_float_values(result, &variant_arr, &variant_path, name)?
    } else {
        result
    };

    // parquet-variant does not currently apply Spark's Boolean-to-integral cast. Fill only the
    // missing rows, preserving successful typed extraction and path-missing NULLs.
    let result = if !needs_post_cast && let Some(ref data_type) = final_type {
        fill_boolean_integer_values(result, &variant_arr, &variant_path, data_type)?
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
