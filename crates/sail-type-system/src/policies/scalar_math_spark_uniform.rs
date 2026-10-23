//! Shared function typing, including literal-dependent result fields.

use std::sync::Arc;

use arrow_schema::{DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE, DataType, Field, FieldRef};
use datafusion_common::{Result, internal_err};
use datafusion_expr::ReturnFieldArgs;

use crate::error::{
    invalid_arg_count_exec_err, unsupported_data_type_exec_err, unsupported_data_types_exec_err,
};

pub fn extract_decimal_info(dt: &DataType) -> Option<(u8, i8)> {
    match dt {
        DataType::Decimal32(p, s) | DataType::Decimal64(p, s) | DataType::Decimal128(p, s) => {
            Some((*p, *s))
        }
        DataType::Decimal256(p, s) => {
            if *p <= DECIMAL128_MAX_PRECISION && *s <= DECIMAL128_MAX_SCALE {
                Some((*p, *s))
            } else {
                None
            }
        }
        _ => None,
    }
}

pub fn calculate_decimal_output(p1: u8, s1: i8, p2: u8, s2: i8) -> (u8, i8) {
    if p1 > p2 {
        (p1, s1)
    } else if p2 > p1 {
        (p2, s2)
    } else {
        // Same precision → use max scale
        (p1, s1.max(s2))
    }
}

pub fn calculate_output_type(t_min: &DataType, t_max: &DataType) -> DataType {
    use DataType::*;

    // Spark's literal NULL produces a DOUBLE fallback regardless of the other side.
    if matches!(t_min, Null) || matches!(t_max, Null) {
        return Float64;
    }

    // Float precedence: DOUBLE wins over FLOAT, which wins over DECIMAL/INT.
    if matches!(t_min, Float64) || matches!(t_max, Float64) {
        return Float64;
    }
    if matches!(t_min, Float32) || matches!(t_max, Float32) {
        return Float32;
    }

    if t_min.is_integer() && t_max.is_integer() {
        // Integer promotion hierarchy: Int8 < Int16 < Int32 < Int64.
        // UInt32/UInt64 require Int64 to safely represent all values.
        return match (t_min, t_max) {
            (Int64, _) | (_, Int64) | (UInt32, _) | (_, UInt32) | (UInt64, _) | (_, UInt64) => {
                Int64
            }
            (Int32, _) | (_, Int32) | (UInt16, _) | (_, UInt16) => Int32,
            (Int16, _) | (_, Int16) | (UInt8, _) | (_, UInt8) => Int16,
            (Int8, Int8) => Int8,
            _ => Int32,
        };
    }

    let decimal_min = extract_decimal_info(t_min);
    let decimal_max = extract_decimal_info(t_max);

    match (decimal_min, decimal_max) {
        (Some((p1, s1)), Some((p2, s2))) => {
            // When one decimal has significantly larger precision, use that decimal's
            // type completely (both precision AND scale): e.g. Decimal(2,1) + Decimal(20,0)
            // → Decimal(20,0), not Decimal(20,1).
            let (precision, scale) = calculate_decimal_output(p1, s1, p2, s2);
            Decimal128(precision, scale)
        }
        (Some((p, s)), None) | (None, Some((p, s))) => Decimal128(p, s),
        (None, None) => Float64,
    }
}

pub fn return_type(_arg_types: &[DataType]) -> Result<DataType> {
    internal_err!("`return_type` should not be called; `return_field_from_args` is used instead")
}

pub fn return_field_from_args(args: ReturnFieldArgs) -> Result<FieldRef> {
    if !matches!(args.arg_fields.len(), 2 | 3) {
        return Err(invalid_arg_count_exec_err(
            "uniform",
            (2, 3),
            args.arg_fields.len(),
        ));
    }

    let t_min = args.arg_fields[0].data_type();
    let t_max = args.arg_fields[1].data_type();
    let return_type = calculate_output_type(t_min, t_max);

    // The result is NULL whenever a bound is NULL, so the field must be
    // nullable if either bound is a NULL literal or a nullable expression.
    let nullable = matches!(t_min, DataType::Null)
        || matches!(t_max, DataType::Null)
        || args.arg_fields[0].is_nullable()
        || args.arg_fields[1].is_nullable();

    Ok(Arc::new(Field::new("uniform", return_type, nullable)))
}

pub fn coerce_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if arg_types.len() < 2 || arg_types.len() > 3 {
        return Err(invalid_arg_count_exec_err(
            "uniform",
            (2, 3),
            arg_types.len(),
        ));
    }

    let t_min = &arg_types[0];
    let t_max = &arg_types[1];

    if !is_valid_bound_type(t_min) {
        return Err(unsupported_data_type_exec_err(
            "uniform",
            "NUMERIC type for min",
            t_min,
        ));
    }
    if !is_valid_bound_type(t_max) {
        return Err(unsupported_data_type_exec_err(
            "uniform",
            "NUMERIC type for max",
            t_max,
        ));
    }

    let output_type: DataType = calculate_output_type(t_min, t_max);

    let mut coerced_types = vec![output_type.clone(), output_type];

    if arg_types.len() == 3 {
        // Spark accepts only INT or BIGINT for the seed. Both are coerced to
        // Int64 internally so the RNG always receives a 64-bit seed.
        let seed_type = match &arg_types[2] {
            DataType::Int32 | DataType::Int64 => DataType::Int64,
            DataType::Null => DataType::Null,
            _ => {
                return Err(unsupported_data_types_exec_err(
                    "uniform",
                    "INT or BIGINT type for seed",
                    &arg_types[2..],
                ));
            }
        };
        coerced_types.push(seed_type);
    }

    Ok(coerced_types)
}

/// Returns true if `dt` is an acceptable min/max bound type for `uniform`:
/// any numeric (integer, float, decimal) plus the literal NULL type.
///
/// `Decimal256` is only accepted when its precision and scale fit in
/// `Decimal128`, since `calculate_output_type` collapses all decimal paths to
/// `Decimal128`. Rejecting oversized `Decimal256` here — instead of silently
/// falling back to `Float64` downstream — keeps `uniform` consistent with
/// other math UDFs (e.g. ceil/floor) and surfaces precision loss early.
fn is_valid_bound_type(dt: &DataType) -> bool {
    dt.is_integer()
        || matches!(dt, DataType::Float32 | DataType::Float64)
        || matches!(
            dt,
            DataType::Decimal32(_, _) | DataType::Decimal64(_, _) | DataType::Decimal128(_, _)
        )
        || matches!(
            dt,
            DataType::Decimal256(precision, scale)
                if *precision <= DECIMAL128_MAX_PRECISION && *scale <= DECIMAL128_MAX_SCALE
        )
        || matches!(dt, DataType::Null)
}
