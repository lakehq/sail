//! Spark store-assignment compatibility, independent of query representation.
use arrow_schema::{DataType, Field, FieldRef, IntervalUnit, TimeUnit};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd)]
pub enum StoreAssignmentPolicy {
    Ansi,
    Strict,
    Legacy,
}

pub fn compatible(
    write_type: &DataType,
    target_type: &DataType,
    policy: StoreAssignmentPolicy,
    case_sensitive: bool,
) -> bool {
    if let DataType::Dictionary(_, value_type) = write_type {
        return compatible(value_type, target_type, policy, case_sensitive);
    }
    if let DataType::Dictionary(_, value_type) = target_type {
        return compatible(write_type, value_type, policy, case_sensitive);
    }

    if let (Some(write_field), Some(target_field)) = (
        list_element_field(write_type),
        list_element_field(target_type),
    ) {
        return resolvable_nullability(write_field.is_nullable(), target_field.is_nullable())
            && compatible(
                write_field.data_type(),
                target_field.data_type(),
                policy,
                case_sensitive,
            );
    }

    if let (Some((write_key, write_value)), Some((target_key, target_value))) =
        (map_fields(write_type), map_fields(target_type))
    {
        return resolvable_nullability(write_value.is_nullable(), target_value.is_nullable())
            && compatible(
                write_key.data_type(),
                target_key.data_type(),
                policy,
                case_sensitive,
            )
            && compatible(
                write_value.data_type(),
                target_value.data_type(),
                policy,
                case_sensitive,
            );
    }

    if let (DataType::Struct(write_fields), DataType::Struct(target_fields)) =
        (write_type, target_type)
    {
        return write_fields.len() == target_fields.len()
            && target_fields.iter().all(|target_field| {
                let mut matching_fields = write_fields.iter().filter(|write_field| {
                    if case_sensitive {
                        write_field.name() == target_field.name()
                    } else {
                        write_field.name().eq_ignore_ascii_case(target_field.name())
                    }
                });
                let Some(write_field) = matching_fields.next() else {
                    return false;
                };
                matching_fields.next().is_none()
                    && resolvable_nullability(write_field.is_nullable(), target_field.is_nullable())
                    && compatible(
                        write_field.data_type(),
                        target_field.data_type(),
                        policy,
                        case_sensitive,
                    )
            });
    }

    if is_nested_type(write_type) || is_nested_type(target_type) {
        return false;
    }

    match policy {
        StoreAssignmentPolicy::Ansi => ansi_compatible(write_type, target_type),
        StoreAssignmentPolicy::Strict => strict_compatible(write_type, target_type),
        StoreAssignmentPolicy::Legacy => false,
    }
}

fn list_element_field(data_type: &DataType) -> Option<&FieldRef> {
    match data_type {
        DataType::List(field)
        | DataType::ListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::LargeList(field)
        | DataType::LargeListView(field) => Some(field),
        _ => None,
    }
}

fn map_fields(data_type: &DataType) -> Option<(&Field, &Field)> {
    let DataType::Map(entries, _) = data_type else {
        return None;
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return None;
    };
    let [key, value] = fields.as_ref() else {
        return None;
    };
    Some((key.as_ref(), value.as_ref()))
}

fn is_nested_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::List(_)
            | DataType::ListView(_)
            | DataType::FixedSizeList(_, _)
            | DataType::LargeList(_)
            | DataType::LargeListView(_)
            | DataType::Struct(_)
            | DataType::Map(_, _)
            | DataType::Union(_, _)
            | DataType::RunEndEncoded(_, _)
    )
}

fn resolvable_nullability(write_nullable: bool, target_nullable: bool) -> bool {
    !write_nullable || target_nullable
}

fn ansi_compatible(write_type: &DataType, target_type: &DataType) -> bool {
    write_type == target_type
        || matches!(write_type, DataType::Null)
        || (is_numeric_type(write_type) && is_numeric_type(target_type))
        || (is_atomic_type(write_type) && is_string_type(target_type))
        || (is_interval_type(write_type) && is_string_type(target_type))
        || (is_datetime_type(write_type) && is_datetime_type(target_type))
}

fn strict_compatible(write_type: &DataType, target_type: &DataType) -> bool {
    write_type == target_type
        || matches!(write_type, DataType::Null)
        || decimal_wider_than(target_type, write_type)
        || decimal_tighter_than(write_type, target_type)
        || legal_numeric_precedence(write_type, target_type)
        || (is_date_type(write_type) && is_timestamp_type(target_type))
        || (is_timestamp_type(write_type) && is_timestamp_type(target_type))
        || (is_atomic_type(write_type) && is_string_type(target_type))
        || (is_interval_type(write_type) && is_string_type(target_type))
        || (is_timestamp_ltz_type(write_type) && matches!(target_type, DataType::Int64))
        || (matches!(write_type, DataType::Int64) && is_timestamp_ltz_type(target_type))
        || interval_family_matches(write_type, target_type)
}

fn is_numeric_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

fn is_string_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn is_datetime_type(data_type: &DataType) -> bool {
    is_date_type(data_type) || is_timestamp_type(data_type)
}

fn is_date_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Date32 | DataType::Date64)
}

fn is_timestamp_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Timestamp(_, _))
}

fn is_timestamp_ltz_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Timestamp(TimeUnit::Microsecond, Some(_))
    )
}

fn is_interval_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Interval(_))
}

fn is_atomic_type(data_type: &DataType) -> bool {
    !matches!(data_type, DataType::Null) && !is_nested_type(data_type)
}

fn decimal_parts(data_type: &DataType) -> Option<(u8, i8)> {
    match data_type {
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => Some((*precision, *scale)),
        _ => None,
    }
}

fn integral_decimal_precision(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::Int8 | DataType::UInt8 => Some(3),
        DataType::Int16 | DataType::UInt16 => Some(5),
        DataType::Int32 | DataType::UInt32 => Some(10),
        DataType::Int64 | DataType::UInt64 => Some(20),
        _ => None,
    }
}

fn decimal_wider_than(target_type: &DataType, write_type: &DataType) -> bool {
    let Some((target_precision, target_scale)) = decimal_parts(target_type) else {
        return false;
    };
    let write_decimal = decimal_parts(write_type)
        .or_else(|| integral_decimal_precision(write_type).map(|precision| (precision, 0)));
    let Some((write_precision, write_scale)) = write_decimal else {
        return false;
    };
    i16::from(target_precision) - i16::from(target_scale)
        >= i16::from(write_precision) - i16::from(write_scale)
        && target_scale >= write_scale
}

fn decimal_tighter_than(write_type: &DataType, target_type: &DataType) -> bool {
    let Some((write_precision, write_scale)) = decimal_parts(write_type) else {
        return false;
    };
    if let Some((target_precision, target_scale)) = decimal_parts(target_type) {
        return i16::from(write_precision) - i16::from(write_scale)
            <= i16::from(target_precision) - i16::from(target_scale)
            && write_scale <= target_scale;
    }
    integral_decimal_precision(target_type)
        .is_some_and(|target_precision| write_precision < target_precision && write_scale == 0)
}

fn legal_numeric_precedence(write_type: &DataType, target_type: &DataType) -> bool {
    let rank = |data_type: &DataType| match data_type {
        DataType::Int8 => Some(0),
        DataType::Int16 => Some(1),
        DataType::Int32 => Some(2),
        DataType::Int64 => Some(3),
        DataType::Float16 | DataType::Float32 => Some(4),
        DataType::Float64 => Some(5),
        _ => None,
    };
    if let (Some(write_rank), Some(target_rank)) = (rank(write_type), rank(target_type)) {
        return write_rank < target_rank;
    }

    let unsigned_bits = |data_type: &DataType| match data_type {
        DataType::UInt8 => Some(8),
        DataType::UInt16 => Some(16),
        DataType::UInt32 => Some(32),
        DataType::UInt64 => Some(64),
        _ => None,
    };
    let signed_bits = |data_type: &DataType| match data_type {
        DataType::Int8 => Some(8),
        DataType::Int16 => Some(16),
        DataType::Int32 => Some(32),
        DataType::Int64 => Some(64),
        _ => None,
    };
    match (unsigned_bits(write_type), unsigned_bits(target_type)) {
        (Some(write_bits), Some(target_bits)) => return write_bits < target_bits,
        (Some(write_bits), None) => {
            if let Some(target_bits) = signed_bits(target_type) {
                return write_bits < target_bits;
            }
            return matches!(
                target_type,
                DataType::Float16 | DataType::Float32 | DataType::Float64
            );
        }
        _ => {}
    }
    false
}

fn interval_family_matches(write_type: &DataType, target_type: &DataType) -> bool {
    matches!(
        (write_type, target_type),
        (
            DataType::Interval(IntervalUnit::YearMonth),
            DataType::Interval(IntervalUnit::YearMonth)
        ) | (
            DataType::Interval(IntervalUnit::DayTime | IntervalUnit::MonthDayNano),
            DataType::Interval(IntervalUnit::DayTime | IntervalUnit::MonthDayNano)
        )
    )
}

pub fn evolve_row_level_field(
    target_field: &Field,
    source_field: &Field,
    case_sensitive: bool,
) -> Field {
    fn evolve_data_type(
        target_type: &DataType,
        source_type: &DataType,
        case_sensitive: bool,
    ) -> DataType {
        let (DataType::Struct(target_fields), DataType::Struct(source_fields)) =
            (target_type, source_type)
        else {
            return target_type.clone();
        };
        let names_equal = |left: &str, right: &str| {
            if case_sensitive {
                left == right
            } else {
                left.eq_ignore_ascii_case(right)
            }
        };
        let mut evolved_fields = target_fields
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        for source_child in source_fields {
            if let Some(index) = evolved_fields
                .iter()
                .position(|target_child| names_equal(target_child.name(), source_child.name()))
            {
                evolved_fields[index] = evolve_row_level_field(
                    &evolved_fields[index],
                    source_child.as_ref(),
                    case_sensitive,
                );
            } else {
                evolved_fields.push(source_child.as_ref().clone().with_nullable(true));
            }
        }
        DataType::Struct(evolved_fields.into())
    }

    target_field.clone().with_data_type(evolve_data_type(
        target_field.data_type(),
        source_field.data_type(),
        case_sensitive,
    ))
}
