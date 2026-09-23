use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields, IntervalUnit};
use datafusion_common::DFSchema;
use datafusion_expr::expr::Cast;
use datafusion_expr::{Expr, ExprSchemable};
use sail_common_datafusion::interval::IntervalRange;

use crate::error::PlanResult;

/// Declares the Spark interval range of the value of an expression.
///
/// The range is carried in the field metadata of a cast to the same type. Unlike the metadata of
/// an alias, the target field of a cast is part of both the logical and the physical expression,
/// and constant folding keeps it, so the range survives wherever the expression is used. The
/// nullability stays the expression's own: a cast takes it from its input, not from its target.
pub(crate) fn with_interval_range(
    expr: Expr,
    range: IntervalRange,
    schema: &DFSchema,
) -> PlanResult<Expr> {
    let (_, field) = expr.to_field(schema)?;
    let target = Field::new("", field.data_type().clone(), field.is_nullable())
        .with_metadata(range.to_metadata()?);
    Ok(Expr::Cast(Cast::new_from_field(
        Box::new(expr),
        Arc::new(target),
    )))
}

/// The interval range an expression declares, if any.
pub(crate) fn interval_range_of(expr: &Expr, schema: &DFSchema) -> Option<IntervalRange> {
    let (_, field) = expr.to_field(schema).ok()?;
    IntervalRange::from_field(&field)
}

/// The range that covers every interval in `exprs`, when they are all of one family; `None` when one
/// of them is not an interval or the families differ. A NULL argument takes the others' range.
pub(crate) fn common_interval_range(exprs: &[Expr], schema: &DFSchema) -> Option<IntervalRange> {
    let mut common: Option<(bool, IntervalRange)> = None;
    for expr in exprs {
        let (_, field) = expr.to_field(schema).ok()?;
        if field.data_type().is_null() {
            continue;
        }
        // The FAMILY is the Arrow type's: a year-month interval and a day-time one never widen into
        // each other, and Spark refuses the pair.
        let year_month = matches!(
            field.data_type(),
            DataType::Interval(IntervalUnit::YearMonth)
        );
        let range = IntervalRange::from_field(&field)?;
        common = Some(match common {
            Some((family, current)) if family == year_month => (family, current.widen(range)),
            Some(_) => return None,
            None => (year_month, range),
        });
    }
    common.map(|(_, range)| range)
}

/// Declares the range of the intervals a CONTAINER holds. The metadata lives on the ELEMENT field of
/// a list and on the VALUE field of a map's entries, so `array(INTERVAL '1' DAY)` reports
/// `array<interval day>` the way Spark does. Built like [`with_interval_range`]: a cast to the same
/// type whose target field carries it.
pub(crate) fn with_element_interval_range(
    expr: Expr,
    range: IntervalRange,
    schema: &DFSchema,
) -> PlanResult<Expr> {
    let (_, field) = expr.to_field(schema)?;
    let metadata = range.to_metadata()?;
    let with_metadata =
        |field: &Arc<Field>| Arc::new(field.as_ref().clone().with_metadata(metadata.clone()));
    let data_type = match field.data_type() {
        DataType::List(element) => DataType::List(with_metadata(element)),
        DataType::LargeList(element) => DataType::LargeList(with_metadata(element)),
        DataType::FixedSizeList(element, size) => {
            DataType::FixedSizeList(with_metadata(element), *size)
        }
        DataType::Map(entries, sorted) => match entries.data_type() {
            DataType::Struct(fields) if fields.len() == 2 => {
                let key = Arc::clone(&fields[0]);
                let value = with_metadata(&fields[1]);
                let entries = Field::new(
                    entries.name(),
                    DataType::Struct(Fields::from(vec![key, value])),
                    entries.is_nullable(),
                );
                DataType::Map(Arc::new(entries), *sorted)
            }
            _ => return Ok(expr),
        },
        _ => return Ok(expr),
    };
    let target = Field::new("", data_type, field.is_nullable());
    Ok(Expr::Cast(Cast::new_from_field(
        Box::new(expr),
        Arc::new(target),
    )))
}

/// The same for a struct built in place: each field takes the range of the value it was built from.
pub(crate) fn with_struct_field_interval_ranges(
    expr: Expr,
    ranges: &[Option<IntervalRange>],
    schema: &DFSchema,
) -> PlanResult<Expr> {
    if ranges.iter().all(Option::is_none) {
        return Ok(expr);
    }
    let (_, field) = expr.to_field(schema)?;
    let DataType::Struct(fields) = field.data_type() else {
        return Ok(expr);
    };
    if fields.len() != ranges.len() {
        return Ok(expr);
    }
    let mapped = fields
        .iter()
        .zip(ranges)
        .map(|(field, range)| match range {
            Some(range) => Ok(field.as_ref().clone().with_metadata(range.to_metadata()?)),
            None => Ok(field.as_ref().clone()),
        })
        .collect::<PlanResult<Vec<_>>>()?;
    let target = Field::new(
        "",
        DataType::Struct(Fields::from(mapped)),
        field.is_nullable(),
    );
    Ok(Expr::Cast(Cast::new_from_field(
        Box::new(expr),
        Arc::new(target),
    )))
}

/// The range the ELEMENTS of an array, or the VALUES of a map, declare, so a function that returns
/// one of them keeps it.
pub(crate) fn element_interval_range(expr: &Expr, schema: &DFSchema) -> Option<IntervalRange> {
    let (_, field) = expr.to_field(schema).ok()?;
    let element = match field.data_type() {
        DataType::List(element)
        | DataType::LargeList(element)
        | DataType::FixedSizeList(element, _)
        | DataType::ListView(element)
        | DataType::LargeListView(element) => Arc::clone(element),
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(fields) if fields.len() == 2 => Arc::clone(&fields[1]),
            _ => return None,
        },
        _ => return None,
    };
    IntervalRange::from_field(&element)
}

/// Re-declares on a value READ OUT of a container the range that container's element carries.
/// `array_element` reports only a `DataType` (`functions-nested/src/extract.rs:164`), so the field
/// metadata does not survive the read and the range has to be put back.
pub(crate) fn preserve_element_interval_range(
    result: Expr,
    collection: &Expr,
    schema: &DFSchema,
) -> PlanResult<Expr> {
    match element_interval_range(collection, schema) {
        Some(range) => with_interval_range(result, range, schema),
        None => Ok(result),
    }
}
