use datafusion::arrow::datatypes::{DataType, IntervalUnit};

use crate::variant::is_variant_storage_type;

/// Spark's `RowOrdering.isOrderable`, which delegates to `OrderUtils.isOrderable`.
///
/// Spark rejects `MAP` and `VARIANT`, plus any `STRUCT` or `ARRAY` that contains one of them,
/// as the ordering key of a sort or of an order-sensitive expression. Everything atomic, and
/// `NULL`, is orderable.
///
/// Two Spark types map onto Arrow in a way that is easy to get wrong here:
/// `DayTimeIntervalType` becomes [`DataType::Duration`] (orderable, because Spark's ANSI interval
/// types are `AtomicType`s), while `CalendarIntervalType` becomes
/// [`IntervalUnit::MonthDayNano`] and is *not* orderable, because `CalendarIntervalType` extends
/// `DataType` directly rather than `AtomicType`.
///
/// `GEOMETRY` and `GEOGRAPHY` are also unorderable in Spark, but in Sail their identity lives in
/// the field metadata rather than in the [`DataType`], so they cannot be recognized here.
pub fn is_orderable(data_type: &DataType) -> bool {
    match data_type {
        DataType::Map(_, _) => false,
        DataType::Interval(IntervalUnit::MonthDayNano) => false,
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            is_orderable(field.data_type())
        }
        // A Spark VARIANT is carried as its Arrow storage struct, so it must be ruled out before
        // the general struct case below.
        DataType::Struct(_) if is_variant_storage_type(data_type) => false,
        DataType::Struct(fields) => fields.iter().all(|field| is_orderable(field.data_type())),
        _ => true,
    }
}
