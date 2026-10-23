use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{Result, plan_datafusion_err, plan_err};
use datafusion_expr::ReturnFieldArgs;
use datafusion_expr::type_coercion::binary::comparison_coercion;

/// Result type after the arguments have been coerced.
pub fn return_type(arg_types: &[DataType]) -> Result<DataType> {
    match arg_types.len() {
        0 => Ok(empty_array_type()),
        _ => {
            let expr_type = arg_types
                .iter()
                .find(|f| !f.is_null())
                .cloned()
                .unwrap_or(DataType::Null);

            Ok(DataType::List(Arc::new(Field::new_list_field(
                expr_type, true,
            ))))
        }
    }
}

/// Infer both container and element nullability from the coerced arguments.
pub fn return_field(args: ReturnFieldArgs) -> Result<FieldRef> {
    let data_types = args
        .arg_fields
        .iter()
        .map(|f| f.data_type())
        .cloned()
        .collect::<Vec<_>>();
    let contains_null = args.arg_fields.iter().any(|f| f.is_nullable());
    let return_type = match return_type(&data_types)? {
        DataType::List(field) => DataType::List(Arc::new(
            field.as_ref().clone().with_nullable(contains_null),
        )),
        data_type => data_type,
    };
    Ok(Arc::new(Field::new("spark_array", return_type, false)))
}

pub fn coerce_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    let first_type = arg_types.first().ok_or_else(|| {
        plan_datafusion_err!("Spark array function requires at least one argument")
    })?;
    // Spark non-ANSI semantics: when mixing strings with other (non-null) types,
    // coerce everything to string. DataFusion's `comparison_coercion` prefers
    // numeric types, which would break Spark's string-wins behavior and cause
    // runtime cast failures for values like `array('a', 1)`.
    let is_string_like = |dt: &DataType| {
        matches!(
            dt,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        )
    };
    let has_string = arg_types.iter().any(is_string_like);
    let has_non_string_non_null = arg_types
        .iter()
        .any(|dt| !is_string_like(dt) && !dt.is_null());
    if has_string && has_non_string_non_null {
        let string_type = if arg_types.iter().any(|dt| matches!(dt, DataType::LargeUtf8)) {
            DataType::LargeUtf8
        } else if arg_types.iter().any(|dt| matches!(dt, DataType::Utf8View)) {
            DataType::Utf8View
        } else {
            DataType::Utf8
        };
        return Ok(vec![string_type; arg_types.len()]);
    }
    let new_type = arg_types
        .iter()
        .skip(1)
        .try_fold(first_type.clone(), |acc, x| {
            // The coerced types found by `comparison_coercion` are not guaranteed to be
            // coercible for the arguments. `comparison_coercion` returns more loose
            // types that can be coerced to both `acc` and `x` for comparison purpose.
            // See `maybe_data_types` for the actual coercion.
            let coerced_type = comparison_coercion(&acc, x);
            if let Some(coerced_type) = coerced_type {
                Ok(coerced_type)
            } else {
                plan_err!("Coercion from {acc:?} to {x:?} failed.")
            }
        })?;
    // When any input is a floating-point type (Double/Float), keep it as Double
    // instead of promoting to Decimal128. Floats support NaN/Infinity which
    // Decimal128 cannot represent, causing runtime overflow errors.
    let new_type = if matches!(new_type, DataType::Decimal128(_, _))
        && arg_types.iter().any(|dt| dt.is_floating())
    {
        DataType::Float64
    } else {
        new_type
    };
    Ok(vec![new_type; arg_types.len()])
}

pub fn empty_array_type() -> DataType {
    DataType::List(Arc::new(Field::new_list_field(DataType::Null, false)))
}
