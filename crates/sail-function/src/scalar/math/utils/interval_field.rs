use datafusion::arrow::datatypes::{DataType, Field, FieldRef, IntervalUnit, TimeUnit};
use sail_common::interval::{IntervalQualifierMetadata, SparkIntervalKind};
use sail_common::spec::{
    EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY, SAIL_INTERVAL_EXTENSION_NAME,
};

/// Attach the broadest Spark interval qualifier extension metadata to `field`
/// when its data type is an interval/duration that Sail surfaces with a
/// qualifier — `YEAR TO MONTH` for `Interval(YearMonth)`, `DAY TO SECOND` for
/// `Duration(Microsecond)`. Other types are returned unchanged.
///
/// This is the right metadata for operations that always widen the qualifier
/// regardless of the inputs (`Interval × Numeric`, `Interval / Numeric`).
/// Surfacing it on the UDF's own return field is durable across DataFusion's
/// physical planning, which drops `Alias` metadata for non-`Literal` inner
/// expressions.
pub fn widen_interval_qualifier_field(field: Field) -> Field {
    let (start, end) = match field.data_type() {
        DataType::Interval(IntervalUnit::YearMonth) => (0, 1),
        DataType::Duration(TimeUnit::Microsecond) => (0, 3),
        _ => return field,
    };
    attach_interval_qualifier(field, start, end)
}

/// Compute the Spark interval qualifier of an interval/duration `field` from
/// the qualifiers carried by its `arg_fields` and attach the result. Used by
/// operations that preserve the input qualifier when all inputs agree (e.g.
/// `INTERVAL '10' YEAR + INTERVAL '5' YEAR` stays as `YEAR`) and widen to the
/// covering range when inputs disagree (e.g. `YEAR + MONTH` → `YEAR TO MONTH`).
///
/// Returns the field unchanged when:
///   - the output isn't an interval/duration Sail surfaces with a qualifier,
///   - any input field is missing the Sail interval extension metadata,
///   - the widened `(start, end)` pair isn't a valid Spark interval kind in
///     the chosen family (defensive — should not happen for well-typed input).
pub fn widen_interval_qualifier_field_from_args(field: Field, arg_fields: &[FieldRef]) -> Field {
    let from_fields: fn(i32, i32) -> Option<SparkIntervalKind> = match field.data_type() {
        DataType::Interval(IntervalUnit::YearMonth) => SparkIntervalKind::from_year_month_fields,
        DataType::Duration(TimeUnit::Microsecond) => SparkIntervalKind::from_day_time_fields,
        _ => return field,
    };
    let mut min_start: Option<i32> = None;
    let mut max_end: Option<i32> = None;
    for arg in arg_fields {
        let Some((start, end)) = parse_interval_qualifier(arg) else {
            return field;
        };
        if from_fields(start, end).is_none() {
            return field;
        }
        min_start = Some(min_start.map_or(start, |s| s.min(start)));
        max_end = Some(max_end.map_or(end, |e| e.max(end)));
    }
    let (Some(start), Some(end)) = (min_start, max_end) else {
        return field;
    };
    if from_fields(start, end).is_none() {
        return field;
    }
    attach_interval_qualifier(field, start, end)
}

/// Read the Sail interval qualifier `(start_field, end_field)` from a Field's
/// extension metadata, if present and well-formed.
fn parse_interval_qualifier(field: &Field) -> Option<(i32, i32)> {
    if field
        .metadata()
        .get(EXTENSION_TYPE_NAME_KEY)
        .map(String::as_str)
        != Some(SAIL_INTERVAL_EXTENSION_NAME)
    {
        return None;
    }
    let json = field.metadata().get(EXTENSION_TYPE_METADATA_KEY)?;
    let parsed: IntervalQualifierMetadata = serde_json::from_str(json).ok()?;
    Some((parsed.start_field?, parsed.end_field?))
}

/// Attach a `(start_field, end_field)` qualifier to `field` under the Sail
/// interval extension metadata keys.
fn attach_interval_qualifier(field: Field, start: i32, end: i32) -> Field {
    let qualifier = IntervalQualifierMetadata {
        start_field: Some(start),
        end_field: Some(end),
    };
    let Ok(json) = serde_json::to_string(&qualifier) else {
        return field;
    };
    field.with_metadata(
        [
            (
                EXTENSION_TYPE_NAME_KEY.to_string(),
                SAIL_INTERVAL_EXTENSION_NAME.to_string(),
            ),
            (EXTENSION_TYPE_METADATA_KEY.to_string(), json),
        ]
        .into_iter()
        .collect(),
    )
}
