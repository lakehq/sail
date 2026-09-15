use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, FixedSizeListArray, LargeListArray, ListArray, StructArray,
};
use datafusion::arrow::datatypes::{
    DataType, Field, Float16Type, Float32Type, Float64Type, IntervalUnit,
};
use datafusion_common::Result;
use half::f16;
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

/// Rewrites every floating-point value, at any nesting depth, to the one Spark compares it as:
/// `-0.0` becomes `0.0` and every NaN becomes the same positive NaN.
///
/// Spark orders floating-point values with `SQLOrderingUtil.compareDoubles`, where `-0.0` equals
/// `0.0` and all NaNs are equal and greater than any other value. Arrow's row format and
/// comparison kernels use the IEEE total order instead, which tells both apart, so an ordering
/// key must go through here before it is compared with them.
pub fn normalize_floats_for_ordering(array: &ArrayRef) -> Result<ArrayRef> {
    if !contains_float(array.data_type()) {
        return Ok(Arc::clone(array));
    }
    Ok(match array.data_type() {
        DataType::Float16 => Arc::new(array.as_primitive::<Float16Type>().unary::<_, Float16Type>(
            |v| {
                if v.is_nan() {
                    f16::NAN
                } else if v == f16::ZERO {
                    f16::ZERO
                } else {
                    v
                }
            },
        )),
        DataType::Float32 => Arc::new(array.as_primitive::<Float32Type>().unary::<_, Float32Type>(
            |v| {
                if v.is_nan() {
                    f32::NAN
                } else if v == 0.0 {
                    0.0
                } else {
                    v
                }
            },
        )),
        DataType::Float64 => Arc::new(array.as_primitive::<Float64Type>().unary::<_, Float64Type>(
            |v| {
                if v.is_nan() {
                    f64::NAN
                } else if v == 0.0 {
                    0.0
                } else {
                    v
                }
            },
        )),
        DataType::Struct(_) => {
            let array = array.as_struct();
            let columns = array
                .columns()
                .iter()
                .map(normalize_floats_for_ordering)
                .collect::<Result<Vec<_>>>()?;
            Arc::new(StructArray::try_new(
                array.fields().clone(),
                columns,
                array.nulls().cloned(),
            )?)
        }
        DataType::List(field) => {
            let array = array.as_list::<i32>();
            Arc::new(ListArray::try_new(
                Arc::clone(field),
                array.offsets().clone(),
                normalize_floats_for_ordering(array.values())?,
                array.nulls().cloned(),
            )?)
        }
        DataType::LargeList(field) => {
            let array = array.as_list::<i64>();
            Arc::new(LargeListArray::try_new(
                Arc::clone(field),
                array.offsets().clone(),
                normalize_floats_for_ordering(array.values())?,
                array.nulls().cloned(),
            )?)
        }
        DataType::FixedSizeList(field, size) => {
            let array = array.as_fixed_size_list();
            Arc::new(FixedSizeListArray::try_new(
                Arc::clone(field),
                *size,
                normalize_floats_for_ordering(array.values())?,
                array.nulls().cloned(),
            )?)
        }
        DataType::Dictionary(_, _) => {
            let array = array.as_any_dictionary();
            array.with_values(normalize_floats_for_ordering(array.values())?)
        }
        // Sail never builds these around an ordering key, and `contains_float` does not look
        // inside them, so they cannot reach this arm with a float to rewrite.
        _ => Arc::clone(array),
    })
}

/// Whether a value of this type holds a float that [`normalize_floats_for_ordering`] rewrites.
pub fn contains_float(data_type: &DataType) -> bool {
    match data_type {
        DataType::Float16 | DataType::Float32 | DataType::Float64 => true,
        DataType::Struct(fields) => fields.iter().any(|field| contains_float(field.data_type())),
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            contains_float(field.data_type())
        }
        DataType::Dictionary(_, value_type) => contains_float(value_type),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::Fields;
    use datafusion::arrow::error::ArrowError;
    use parquet_variant_compute::VariantType;
    use sail_common::geoarrow::extension::GeoArrowMetadata;

    use super::*;

    /// A VARIANT as read from a lakehouse table: the `arrow.parquet.variant` extension on the
    /// field, but no Sail marker on its `metadata` child, so the `DataType` alone looks like an
    /// ordinary struct. SQL cannot build one, which is why this is not a BDD scenario.
    fn extension_only_variant(name: &str) -> Result<Field, ArrowError> {
        let storage = DataType::Struct(Fields::from(vec![
            Field::new("metadata", DataType::Binary, false),
            Field::new("value", DataType::Binary, true),
        ]));
        let mut field = Field::new(name, storage, true);
        field.try_with_extension_type(VariantType)?;
        Ok(field)
    }

    fn geometry(name: &str) -> Result<Field, ArrowError> {
        let mut field = Field::new(name, DataType::Binary, true);
        field.try_with_extension_type(GeoArrowWkbType {
            metadata: GeoArrowMetadata::default(),
        })?;
        Ok(field)
    }

    #[test]
    fn extension_only_variant_is_not_orderable() -> Result<(), ArrowError> {
        let variant = extension_only_variant("v")?;
        assert!(is_orderable(variant.data_type()));
        assert!(!is_orderable_field(&variant));
        Ok(())
    }

    #[test]
    fn extension_only_variant_nested_in_a_struct_is_not_orderable() -> Result<(), ArrowError> {
        let nested = Field::new(
            "s",
            DataType::Struct(Fields::from(vec![extension_only_variant("v")?])),
            true,
        );
        assert!(!is_orderable_field(&nested));
        Ok(())
    }

    #[test]
    fn geometry_is_not_orderable() -> Result<(), ArrowError> {
        let geometry = geometry("g")?;
        assert!(is_orderable(geometry.data_type()));
        assert!(!is_orderable_field(&geometry));
        Ok(())
    }

    #[test]
    fn an_unmarked_struct_with_variant_field_names_is_orderable() {
        let shaped = Field::new(
            "s",
            DataType::Struct(Fields::from(vec![
                Field::new("metadata", DataType::Binary, false),
                Field::new("value", DataType::Binary, true),
            ])),
            true,
        );
        assert!(is_orderable_field(&shaped));
    }
}
