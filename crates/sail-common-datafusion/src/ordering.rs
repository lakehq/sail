use datafusion::arrow::datatypes::{DataType, IntervalUnit};

use crate::variant::is_marked_variant_storage_type;

/// Spark's `RowOrdering.isOrderable`, which delegates to `OrderUtils.isOrderable`.
///
/// This mirrors the Scala definition arm by arm, including its shape: everything atomic is
/// orderable, `STRUCT` and `ARRAY` recurse, and anything else is *not* orderable. Keeping the
/// final arm `false` is what makes the port fail closed the way Spark does — a type nobody
/// thought about is rejected rather than silently sorted by its physical bytes.
///
/// Two Spark types map onto Arrow in a way that is easy to get wrong here: `DayTimeIntervalType`
/// becomes [`DataType::Duration`] and `YearMonthIntervalType` becomes
/// [`IntervalUnit::YearMonth`], both orderable because Spark's ANSI interval types are
/// `AtomicType`s, while `CalendarIntervalType` becomes [`IntervalUnit::MonthDayNano`] and is not,
/// because it extends `DataType` directly.
///
/// `GEOMETRY` and `GEOGRAPHY` are unorderable in Spark too, but in Sail their identity lives in
/// the field metadata rather than in the [`DataType`], so they cannot be recognized here.
pub fn is_orderable(data_type: &DataType) -> bool {
    match data_type {
        DataType::Null => true,
        // A Spark VARIANT is carried as its Arrow storage struct, so it must be ruled out before
        // the struct case below.
        DataType::Struct(_) if is_marked_variant_storage_type(data_type) => false,
        // Atomic types.
        DataType::Boolean
        | DataType::Int8
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
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        // Spark's day-time and year-month intervals are `AnsiIntervalType extends AtomicType`.
        | DataType::Duration(_)
        | DataType::Interval(IntervalUnit::DayTime)
        | DataType::Interval(IntervalUnit::YearMonth) => true,
        DataType::Struct(fields) => fields.iter().all(|field| is_orderable(field.data_type())),
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::RunEndEncoded(_, field) => is_orderable(field.data_type()),
        DataType::Dictionary(_, value_type) => is_orderable(value_type),
        // `MapType` and `CalendarIntervalType` are not `AtomicType`s in Spark, so they reach its
        // final `case _ => false`, as does everything else with no Spark counterpart.
        DataType::Map(_, _) | DataType::Interval(IntervalUnit::MonthDayNano) | DataType::Union(_, _) => false,
    }
}
