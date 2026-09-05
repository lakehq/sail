use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef, Fields, TimeUnit};
use datafusion_expr::{Expr, ExprSchemable, ScalarUDF, cast, lit};
use sail_function::scalar::datetime::convert_tz::ConvertTz;
use sail_function::scalar::spark_struct_rename::SparkStructRename;
use sail_function::scalar::spark_to_string::SparkToUtf8;

use super::nullability::{cast_force_nullable, expr_nullable};
use super::{coerce_to_temporal, is_string_type, is_temporal_type};
use crate::error::{PlanError, PlanResult};
use crate::function::common::FunctionContextInput;
use crate::function::is_spark_compatible_arrow_fixed_offset;
use crate::resolver::build_rename_target_type;

/// Spark CaseWhenTypeCoercion: resolve the common type before Sail consumes a
/// CASE's schema, then cast every result, including unreachable branches.
pub(super) fn coerce_case_values(
    values: Vec<Expr>,
    context: &FunctionContextInput<'_>,
) -> PlanResult<(Vec<Expr>, Vec<bool>)> {
    let types = values
        .iter()
        .map(|value| value.get_type(context.schema))
        .collect::<datafusion_common::Result<Vec<_>>>()?;
    let mut ordered = types.iter().collect::<Vec<_>>();
    if !context.plan_config.ansi_mode {
        // Legacy string promotion is not associative. Spark groups string and
        // recursively string-array types first, preserving order within groups.
        ordered.sort_by_key(|data_type| !has_string_type(data_type));
        let mut strings = Vec::new();
        ordered.retain(|data_type| {
            if !has_string_type(data_type) {
                return true;
            }
            if strings.contains(data_type) {
                return false;
            }
            strings.push(*data_type);
            true
        });
    }
    let target = ordered.into_iter().try_fold(DataType::Null, |left, right| {
        wider_type(&left, right, context)
    }).ok_or_else(|| PlanError::analysis(format!(
        "[DATATYPE_MISMATCH.DATA_DIFF_TYPES] CASE result branches must have compatible types: {types:?}"
    )))?;
    let nullable = values
        .iter()
        .zip(&types)
        .map(|(value, from)| {
            Ok(expr_nullable(value, context)? || cast_force_nullable(from, &target))
        })
        .collect::<datafusion_common::Result<Vec<_>>>()?;
    let values = values
        .into_iter()
        .zip(types)
        .map(|(value, from)| {
            if from == target {
                return Ok(value);
            }
            if is_string_type(&target) && !is_string_type(&from) && !from.is_null() {
                return Ok(ScalarUDF::from(SparkToUtf8::new())
                    .call(vec![value])
                    .cast_to(&target, context.schema)?);
            }
            if is_string_type(&from) && is_temporal_type(&target) {
                return coerce_to_temporal(value, &from, &target);
            }
            if let (DataType::Timestamp(_, None), DataType::Timestamp(_, Some(timezone))) =
                (&from, &target)
                && !is_spark_compatible_arrow_fixed_offset(timezone)
            {
                // Reuse Sail's explicit CAST handling for Spark's DST gap/overlap rule.
                let instant = ScalarUDF::from(ConvertTz::new(false)).call(vec![
                    lit(timezone.to_string()),
                    lit("UTC"),
                    cast(value, DataType::Timestamp(TimeUnit::Microsecond, None)),
                ]);
                return Ok(cast(cast(instant, DataType::Int64), target.clone()));
            }
            // CASE matches struct names using Spark's resolver, then casts by
            // position. Reuse the existing recursive struct/list/map renaming path.
            let renamed_type = build_rename_target_type(&from, &target);
            let value = if renamed_type != from {
                ScalarUDF::from(SparkStructRename::new(renamed_type)).call(vec![value])
            } else {
                value
            };
            Ok(value.cast_to(&target, context.schema)?)
        })
        .collect::<PlanResult<Vec<_>>>()?;
    Ok((values, nullable))
}

fn has_string_type(data_type: &DataType) -> bool {
    is_string_type(data_type)
        || match data_type {
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _) => has_string_type(field.data_type()),
            _ => false,
        }
}

fn wider_type(
    left: &DataType,
    right: &DataType,
    context: &FunctionContextInput<'_>,
) -> Option<DataType> {
    if left == right || right.is_null() {
        return Some(left.clone());
    }
    if left.is_null() {
        return Some(right.clone());
    }
    if is_string_type(left) && is_string_type(right) {
        return Some(left.clone());
    }
    if left.is_numeric() && right.is_numeric() {
        return wider_numeric(left, right, context.plan_config.ansi_mode);
    }
    if is_temporal_type(left) && is_temporal_type(right) {
        return Some(match (left, right) {
            (DataType::Timestamp(_, Some(_)), _) | (_, DataType::Timestamp(_, Some(_))) => {
                DataType::Timestamp(
                    TimeUnit::Microsecond,
                    Some(Arc::clone(&context.plan_config.session_timezone)),
                )
            }
            (DataType::Timestamp(_, None), _) | (_, DataType::Timestamp(_, None)) => {
                DataType::Timestamp(TimeUnit::Microsecond, None)
            }
            _ => DataType::Date32,
        });
    }
    if is_string_type(left) || is_string_type(right) {
        let (string, other) = if is_string_type(left) {
            (left, right)
        } else {
            (right, left)
        };
        if context.plan_config.ansi_mode {
            return match other {
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                    Some(DataType::Int64)
                }
                DataType::Float32 | DataType::Float64 | DataType::Decimal128(_, _) => {
                    Some(DataType::Float64)
                }
                DataType::Boolean
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::BinaryView => Some(other.clone()),
                other if is_temporal_type(other) => Some(other.clone()),
                _ => None,
            };
        }
        return (other.is_numeric()
            || is_temporal_type(other)
            || matches!(other, DataType::Duration(_) | DataType::Interval(_)))
        .then(|| string.clone());
    }
    match (left, right) {
        (DataType::List(left), DataType::List(right)) => {
            Some(DataType::List(wider_field(left, right, context)?))
        }
        (DataType::LargeList(left), DataType::LargeList(right)) => {
            Some(DataType::LargeList(wider_field(left, right, context)?))
        }
        (DataType::FixedSizeList(left, _), DataType::FixedSizeList(right, _))
        | (DataType::List(left), DataType::FixedSizeList(right, _))
        | (DataType::FixedSizeList(left, _), DataType::List(right)) => {
            Some(DataType::List(wider_field(left, right, context)?))
        }
        (DataType::Struct(left), DataType::Struct(right)) if left.len() == right.len() => {
            let fields = left
                .iter()
                .zip(right)
                .map(|(left, right)| {
                    let names_match = if context.plan_config.case_sensitive {
                        left.name() == right.name()
                    } else {
                        left.name().eq_ignore_ascii_case(right.name())
                    };
                    names_match
                        .then(|| wider_field(left, right, context))
                        .flatten()
                })
                .collect::<Option<Fields>>()?;
            Some(DataType::Struct(fields))
        }
        (DataType::Map(left, _), DataType::Map(right, _)) => {
            let (DataType::Struct(left_fields), DataType::Struct(right_fields)) =
                (left.data_type(), right.data_type())
            else {
                return None;
            };
            let ([left_key, left_value], [right_key, right_value]) =
                (left_fields.as_ref(), right_fields.as_ref())
            else {
                return None;
            };
            let key = wider_type(left_key.data_type(), right_key.data_type(), context)?;
            if cast_force_nullable(left_key.data_type(), &key)
                || cast_force_nullable(right_key.data_type(), &key)
            {
                return None;
            }
            let value = wider_field(left_value, right_value, context)?;
            let fields = vec![Arc::new(Field::new(left_key.name(), key, false)), value];
            Some(DataType::Map(
                Arc::new(Field::new(
                    left.name(),
                    DataType::Struct(fields.into()),
                    false,
                )),
                false,
            ))
        }
        _ => None,
    }
}

fn wider_field(
    left: &FieldRef,
    right: &FieldRef,
    context: &FunctionContextInput<'_>,
) -> Option<FieldRef> {
    let data_type = wider_type(left.data_type(), right.data_type(), context)?;
    let nullable = left.is_nullable()
        || right.is_nullable()
        || cast_force_nullable(left.data_type(), &data_type)
        || cast_force_nullable(right.data_type(), &data_type);
    Some(Arc::new(Field::new(left.name(), data_type, nullable)))
}

fn wider_numeric(left: &DataType, right: &DataType, ansi: bool) -> Option<DataType> {
    if left.is_floating() || right.is_floating() {
        return Some(
            if left == &DataType::Float64
                || right == &DataType::Float64
                || left.is_decimal()
                || right.is_decimal()
                || ansi
            {
                DataType::Float64
            } else {
                DataType::Float32
            },
        );
    }
    if left.is_decimal() || right.is_decimal() {
        let decimal = |data_type: &DataType| match data_type {
            DataType::Int8 => Some((3, 0)),
            DataType::Int16 => Some((5, 0)),
            DataType::Int32 => Some((10, 0)),
            DataType::Int64 => Some((20, 0)),
            DataType::Decimal128(p, s) => Some((*p, *s)),
            _ => None,
        };
        let (p1, s1) = decimal(left)?;
        let (p2, s2) = decimal(right)?;
        let integral = (i16::from(p1) - i16::from(s1)).max(i16::from(p2) - i16::from(s2));
        let scale = i16::from(s1.max(s2)).min(38 - integral);
        let precision = (integral + scale).min(38);
        return Some(DataType::Decimal128(
            u8::try_from(precision).ok()?,
            i8::try_from(scale).ok()?,
        ));
    }
    [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ]
    .into_iter()
    .rfind(|data_type| data_type == left || data_type == right)
}
