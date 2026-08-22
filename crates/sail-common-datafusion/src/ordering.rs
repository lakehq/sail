use datafusion::arrow::datatypes::{DataType, Field, IntervalUnit};
use sail_common::geoarrow::extension::GeoArrowWkbType;

use crate::variant::{is_marked_variant_storage_type, is_variant_storage_field};

/// Spark's `RowOrdering.isOrderable`, which delegates to `OrderUtils.isOrderable`.
///
/// Everything atomic is orderable, `STRUCT` and the list family recurse, and anything else is
/// not. Spark fails closed on the types it does not name (`case _ => false`); this port has no
/// wildcard arm at all, so a new *Arrow* variant fails the build and has to be classified
/// deliberately. That guarantee does not extend to a new *Spark* type carried inside an existing
/// Arrow variant: such a type looks ordinary here and is caught only by [`is_orderable_field`],
/// which sees the field metadata this function never receives.
///
/// Beyond Spark's own arms this also recurses through `Dictionary` and `RunEndEncoded`, which
/// have no Spark counterpart. Spark's `UserDefinedType` arm has none here because a Sail UDT
/// already presents as its underlying storage type.
///
/// Two Spark types map onto Arrow in a way that is easy to get wrong here: `DayTimeIntervalType`
/// becomes [`DataType::Duration`] and `YearMonthIntervalType` becomes
/// [`IntervalUnit::YearMonth`], both orderable because Spark's ANSI interval types are
/// `AtomicType`s, while `CalendarIntervalType` becomes [`IntervalUnit::MonthDayNano`] and is not,
/// because it extends `DataType` directly. [`IntervalUnit::DayTime`] is accepted for the same
/// reason even though Sail never produces it for a Spark type, so no scenario can reach it: the
/// arm exists so that a day-time interval arriving from outside the resolver is not rejected.
///
/// `GEOMETRY` and `GEOGRAPHY` are unorderable in Spark too, but in Sail they are lowered to plain
/// `Binary` with their identity in the field metadata, so they pass this check; use
/// [`is_orderable_field`] wherever a `Field` is available.
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
        DataType::Struct(fields) => fields.iter().all(|field| is_orderable_field(field)),
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::RunEndEncoded(_, field) => is_orderable_field(field),
        DataType::Dictionary(_, value_type) => is_orderable(value_type),
        // `MapType` and `CalendarIntervalType` are not `AtomicType`s in Spark, so they reach its
        // final `case _ => false`, as does everything else with no Spark counterpart.
        DataType::Map(_, _)
        | DataType::Interval(IntervalUnit::MonthDayNano)
        | DataType::Union(_, _) => false,
    }
}

/// Spark orderability for an Arrow *field*, which is the only view that can see the logical
/// types Sail carries in metadata rather than in the [`DataType`].
///
/// `GEOMETRY`/`GEOGRAPHY` are lowered to `Binary` tagged `geoarrow.wkb`, and a VARIANT read
/// through a path that does not mark its child fields is recognizable only by its
/// `arrow.parquet.variant` extension. Both are unorderable in Spark and both look ordinary to
/// [`is_orderable`], so prefer this function wherever a `Field` is available — in a UDF that
/// means `return_field`, not `coerce_types`.
pub fn is_orderable_field(field: &Field) -> bool {
    if field.extension_type_name() == Some(GeoArrowWkbType::NAME) || is_variant_storage_field(field)
    {
        return false;
    }
    is_orderable(field.data_type())
}
