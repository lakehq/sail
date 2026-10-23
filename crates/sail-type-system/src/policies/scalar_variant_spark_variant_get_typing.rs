//! Shared function type policies.

use std::sync::Arc;

use arrow_schema::{ArrowError, DataType, Field, FieldRef, TimeUnit};
use datafusion_common::{Result, ScalarValue, arrow_datafusion_err, exec_datafusion_err};
use datafusion_expr::ReturnFieldArgs;
use parquet_variant_compute::VariantType;
use sail_common_datafusion::variant::{VARIANT_VALUE_FIELD_NAME, variant_metadata_field};

use crate::error::{generic_exec_err, invalid_arg_count_exec_err, unsupported_data_type_exec_err};

pub fn spark_variant_get_return_type(_arg_types: &[DataType]) -> Result<DataType> {
    datafusion_common::internal_err!(
        "`return_type` should not be called; `return_field_from_args` is used instead"
    )
}

pub fn spark_variant_get_return_field_from_args(
    name: &str,
    args: ReturnFieldArgs,
) -> Result<FieldRef> {
    return_field_for_variant_get(name, &args)
}

pub fn spark_variant_get_coerce_types(name: &str, arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if arg_types.len() != 2 && arg_types.len() != 3 {
        return Err(invalid_arg_count_exec_err(name, (2, 3), arg_types.len()));
    }
    // arg[0]: variant (struct), arg[1]: path (string), arg[2]: type (string, optional)
    let mut result = vec![arg_types[0].clone()];
    // Coerce path to Utf8
    result.push(match &arg_types[1] {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => arg_types[1].clone(),
        DataType::Null => DataType::Utf8,
        _ => {
            return Err(unsupported_data_type_exec_err(
                name,
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
                    name,
                    "string",
                    &arg_types[2],
                ));
            }
        });
    }
    Ok(result)
}

pub fn return_field_for_variant_get(name: &str, args: &ReturnFieldArgs) -> Result<FieldRef> {
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

pub fn type_hint_from_scalar(field_name: &str, scalar: &ScalarValue) -> Result<FieldRef> {
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

pub fn spark_type_to_arrow(type_str: &str) -> Result<DataType> {
    let lower = type_str.to_lowercase();
    let trimmed = lower.trim();

    // Handle parameterized types: decimal(p,s)
    if let Some(params) = trimmed.strip_circumfix("decimal(", ')') {
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
