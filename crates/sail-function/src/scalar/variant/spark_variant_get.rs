use std::sync::Arc;

/// [Credit]: <https://github.com/datafusion-contrib/datafusion-variant/blob/main/src/variant_get.rs>
use arrow_schema::{DataType, Field, FieldRef};
use chumsky::prelude::*;
use datafusion_common::{Result, ScalarValue, exec_datafusion_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use parquet_variant::{VariantPath, VariantPathElement};
use parquet_variant_compute::{GetOptions, variant_get};
use sail_common_datafusion::variant::{VARIANT_VALUE_FIELD_NAME, variant_metadata_field};

use crate::error::generic_exec_err;
use crate::scalar::variant::utils::helper::{try_field_as_variant_array, try_parse_string_scalar};

fn build_get_options<'a>(path: VariantPath<'a>, as_type: &Option<FieldRef>) -> GetOptions<'a> {
    match as_type {
        Some(field) => GetOptions::new_with_path(path).with_as_type(Some(field.clone())),
        None => GetOptions::new_with_path(path),
    }
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
        sail_type_system::policies::scalar_variant_spark_variant_get_typing::spark_variant_get_return_type(_arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        sail_type_system::policies::scalar_variant_spark_variant_get_typing::spark_variant_get_return_field_from_args(self.name(), args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        sail_type_system::policies::scalar_variant_spark_variant_get_typing::spark_variant_get_coerce_types(self.name(), arg_types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke_variant_get(args, self.name(), self.safe)
    }
}

/// Converts Spark type strings to Arrow DataType.
///
/// Supports Spark-compatible names first, then falls back to Arrow's `DataType::parse()`
/// for full type coverage (e.g. `"Int64"`, `"Decimal128(10,2)"`).
use sail_type_system::policies::scalar_variant_spark_variant_get_typing::spark_type_to_arrow;

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
