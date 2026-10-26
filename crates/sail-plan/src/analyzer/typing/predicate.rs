use std::sync::Arc;

use arrow_schema::{DataType, IntervalUnit, TimeUnit};
use datafusion_expr::Operator;

pub enum TimestampStringInCoercion {
    ToString,
    ToTimestamp(Option<Arc<str>>),
    List {
        null_input_type: DataType,
        element: Box<TimestampStringInCoercion>,
    },
    Struct {
        null_input_type: DataType,
        field_names: Vec<String>,
        fields: Vec<Option<TimestampStringInCoercion>>,
    },
}

pub fn find_timestamp_string_in_coercion(
    data_types: &[DataType],
    session_timezone: &Arc<str>,
    ansi_mode: bool,
    case_sensitive: bool,
) -> Option<TimestampStringInCoercion> {
    let non_null_types = data_types
        .iter()
        .filter(|data_type| !data_type.is_null())
        .cloned()
        .collect::<Vec<_>>();
    let first = non_null_types.first()?;

    match first {
        DataType::List(_) | DataType::LargeList(_) => {
            let fields = non_null_types
                .iter()
                .map(|data_type| match data_type {
                    DataType::List(field) | DataType::LargeList(field) => Some(field.as_ref()),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;
            let element_types = fields
                .iter()
                .map(|field| field.data_type().clone())
                .collect::<Vec<_>>();
            let element = find_timestamp_string_in_coercion(
                &element_types,
                session_timezone,
                ansi_mode,
                case_sensitive,
            )?;
            return Some(TimestampStringInCoercion::List {
                null_input_type: first.clone(),
                element: Box::new(element),
            });
        }
        DataType::Struct(first_fields) => {
            let structs = non_null_types
                .iter()
                .map(|data_type| match data_type {
                    DataType::Struct(fields) => Some(fields),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;
            if structs.iter().any(|fields| {
                fields.len() != first_fields.len()
                    || fields
                        .iter()
                        .zip(first_fields.iter())
                        .any(|(field, first)| {
                            !struct_field_names_equal(field.name(), first.name(), case_sensitive)
                        })
            }) {
                return None;
            }

            let fields = first_fields
                .iter()
                .enumerate()
                .map(|(index, _)| {
                    let field_types = structs
                        .iter()
                        .map(|fields| fields[index].data_type().clone())
                        .collect::<Vec<_>>();
                    find_timestamp_string_in_coercion(
                        &field_types,
                        session_timezone,
                        ansi_mode,
                        case_sensitive,
                    )
                })
                .collect::<Vec<_>>();
            if fields.iter().all(Option::is_none) {
                return None;
            }
            return Some(TimestampStringInCoercion::Struct {
                null_input_type: first.clone(),
                field_names: first_fields
                    .iter()
                    .map(|field| field.name().clone())
                    .collect(),
                fields,
            });
        }
        _ => {}
    }

    let has_timestamp = non_null_types
        .iter()
        .any(|data_type| matches!(data_type, DataType::Timestamp(_, _)));
    let has_string = non_null_types.iter().any(is_string_type);
    if !has_timestamp || !has_string {
        return None;
    }

    if !ansi_mode {
        return data_types
            .iter()
            .all(non_ansi_in_string_promotable)
            .then_some(TimestampStringInCoercion::ToString);
    }

    if !data_types.iter().all(|data_type| {
        data_type.is_null()
            || data_type.is_string()
            || matches!(
                data_type,
                DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)
            )
    }) {
        return None;
    }

    let timezone = non_null_types
        .iter()
        .any(|data_type| matches!(data_type, DataType::Timestamp(_, Some(_))))
        .then(|| Arc::clone(session_timezone));
    Some(TimestampStringInCoercion::ToTimestamp(timezone))
}

pub fn timestamp_string_coercion_requires_rewrite(
    data_type: &DataType,
    coercion: &TimestampStringInCoercion,
) -> bool {
    match coercion {
        TimestampStringInCoercion::ToString => !is_string_type(data_type),
        TimestampStringInCoercion::ToTimestamp(timezone) => {
            data_type != &DataType::Timestamp(TimeUnit::Microsecond, timezone.clone())
        }
        TimestampStringInCoercion::List { element, .. } => {
            data_type.is_null()
                || match data_type {
                    DataType::List(field) | DataType::LargeList(field) => {
                        timestamp_string_coercion_requires_rewrite(field.data_type(), element)
                    }
                    _ => false,
                }
        }
        TimestampStringInCoercion::Struct {
            field_names,
            fields: field_coercions,
            ..
        } => {
            data_type.is_null()
                || match data_type {
                    DataType::Struct(fields) => fields.iter().enumerate().any(|(index, field)| {
                        field.name() != &field_names[index]
                            || field_coercions[index].as_ref().is_some_and(|coercion| {
                                timestamp_string_coercion_requires_rewrite(
                                    field.data_type(),
                                    coercion,
                                )
                            })
                    }),
                    _ => false,
                }
        }
    }
}

/// Applies Spark timestamp/string coercion to one predicate before DataFusion's generic
/// coercion, preserving session-zone parsing and Spark TimestampType's microsecond precision.
fn non_ansi_in_string_promotable(data_type: &DataType) -> bool {
    data_type.is_null()
        || data_type.is_numeric()
        || data_type.is_string()
        || (data_type.is_temporal()
            && !matches!(data_type, DataType::Interval(IntervalUnit::MonthDayNano)))
}

fn is_string_type(data_type: &DataType) -> bool {
    data_type.is_string()
}

pub fn timestamp_parse_timezone_for_type(
    data_type: &DataType,
    session_timezone: &Arc<str>,
) -> Option<Option<Arc<str>>> {
    match data_type {
        DataType::Timestamp(_, Some(_)) => Some(Some(Arc::clone(session_timezone))),
        DataType::Timestamp(_, None) => Some(None),
        _ => None,
    }
}

fn struct_field_names_equal(left: &str, right: &str, case_sensitive: bool) -> bool {
    if case_sensitive {
        left == right
    } else {
        left.eq_ignore_ascii_case(right)
    }
}

pub fn is_date_or_timestamp_ltz(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, Some(_))
    )
}

pub fn is_ordering_comparison(operator: Operator) -> bool {
    matches!(
        operator,
        Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
    )
}

pub fn is_comparison(operator: Operator) -> bool {
    matches!(
        operator,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom
    )
}
