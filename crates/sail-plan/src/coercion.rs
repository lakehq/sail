use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion_expr::type_coercion::binary::type_union_resolution;

/// The type Spark widens a pair of NUMERIC types to, for the places that pick one type for several
/// branches: `CASE`, `IF`, and the set operations.
///
/// DataFusion's `type_union_resolution` is close but not Spark's `findWiderTypeForTwo`, and the two
/// places it differs both LOSE data:
///   - a fractional beside a DECIMAL is a DOUBLE (`TypeCoercionHelper.scala:194-195`), where the
///     union resolution answers a DECIMAL that a large DOUBLE overflows;
///   - an integral beside a FLOAT is a DOUBLE, "to avoid potential precision loss on converting the
///     Integral type as Float type" (`AnsiTypeCoercion.scala:113-123`), where the union resolution
///     answers FLOAT and `16777217` comes back as `16777216`.
///
/// Returns `None` when the pair is not numeric, or when DataFusion finds no common type: the caller
/// then leaves the branches as they are.
pub(crate) fn spark_wider_numeric_type(left: &DataType, right: &DataType) -> Option<DataType> {
    if !left.is_numeric() || !right.is_numeric() {
        return None;
    }
    let is_decimal = |data_type: &DataType| {
        matches!(
            data_type,
            DataType::Decimal32(_, _)
                | DataType::Decimal64(_, _)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
        )
    };
    let is_fractional = |data_type: &DataType| {
        matches!(
            data_type,
            DataType::Float16 | DataType::Float32 | DataType::Float64
        )
    };
    if (is_decimal(left) && is_fractional(right)) || (is_fractional(left) && is_decimal(right)) {
        return Some(DataType::Float64);
    }
    let wider = type_union_resolution(&[left.clone(), right.clone()])?;
    if matches!(wider, DataType::Float32 | DataType::Float16) {
        return Some(DataType::Float64);
    }
    Some(wider)
}

/// The type a whole set of branches widens to, folding [`spark_wider_numeric_type`] over them.
pub(crate) fn spark_wider_numeric_type_of(data_types: &[DataType]) -> Option<DataType> {
    let mut common = data_types.first()?.clone();
    for data_type in data_types.iter().skip(1) {
        common = spark_wider_numeric_type(&common, data_type)?;
    }
    Some(common)
}

/// The type Spark widens a PAIR of types to, containers included: `findWiderTypeForTwo` recurses
/// into an array, a map and a struct, widening leaf by leaf and keeping the LEFT side's field names
/// (`TypeCoercionHelper.scala:141`). `coalesce` alone cannot type those pairs -- an array of structs
/// whose leaves widen, or a map whose values need a promotion, fails at analysis or at runtime --
/// so the caller casts both sides to this type first.
///
/// Returns `None` when there is no common type, when a struct pair has a different number of fields,
/// or when the pair is one `coalesce` already handles on its own.
pub(crate) fn spark_wider_type(left: &DataType, right: &DataType) -> Option<DataType> {
    if left == right {
        return Some(left.clone());
    }
    if left.is_null() {
        return Some(right.clone());
    }
    if right.is_null() {
        return Some(left.clone());
    }
    let list_element = |field: &Field, other: &Field| -> Option<Field> {
        let data_type = spark_wider_type(field.data_type(), other.data_type())?;
        Some(
            Field::new(
                field.name(),
                data_type,
                field.is_nullable() || other.is_nullable(),
            )
            .with_metadata(field.metadata().clone()),
        )
    };
    match (left, right) {
        (
            DataType::List(left)
            | DataType::LargeList(left)
            | DataType::FixedSizeList(left, _)
            | DataType::ListView(left)
            | DataType::LargeListView(left),
            DataType::List(right)
            | DataType::LargeList(right)
            | DataType::FixedSizeList(right, _)
            | DataType::ListView(right)
            | DataType::LargeListView(right),
        ) => Some(DataType::List(Arc::new(list_element(left, right)?))),
        (DataType::Map(left, sorted), DataType::Map(right, _)) => {
            Some(DataType::Map(Arc::new(list_element(left, right)?), *sorted))
        }
        (DataType::Struct(left), DataType::Struct(right)) if left.len() == right.len() => {
            let fields = left
                .iter()
                .zip(right.iter())
                .map(|(left, right)| Some(Arc::new(list_element(left, right)?)))
                .collect::<Option<Fields>>()?;
            Some(DataType::Struct(fields))
        }
        (left, right) if left.is_nested() || right.is_nested() => None,
        // Only a NUMERIC leaf pair is widened here. A string beside a number is Spark's string
        // promotion, and casting the string side to the number would raise on a value that is not
        // one (`nvl(array(1), array('a'))` answers in Spark); that pair keeps the route it had.
        (left, right) => spark_wider_numeric_type(left, right),
    }
}

/// Returns true if the cast from `from` to `to` involves a Struct
/// (possibly nested in a List/LargeList/FixedSizeList/Map) whose field names
/// don't share enough overlap for DataFusion's struct cast validator.
pub(crate) fn needs_struct_field_rename(from: &DataType, to: &DataType) -> bool {
    match (from, to) {
        (DataType::Struct(a), DataType::Struct(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b.iter())
                    .any(|(fa, fb)| fa.name() != fb.name())
        }
        (DataType::List(a), DataType::List(b))
        | (DataType::LargeList(a), DataType::LargeList(b)) => {
            needs_struct_field_rename(a.data_type(), b.data_type())
        }
        (DataType::FixedSizeList(a, sa), DataType::FixedSizeList(b, sb)) if sa == sb => {
            needs_struct_field_rename(a.data_type(), b.data_type())
        }
        (DataType::Map(a, _), DataType::Map(b, _)) => {
            needs_struct_field_rename(a.data_type(), b.data_type())
        }
        _ => false,
    }
}

/// Build a target type that has the names from `to` but the data types from
/// `from`. The result is what `SparkStructRename` produces; the subsequent
/// regular CAST then handles any leaf-type conversion.
pub(crate) fn build_rename_target_type(from: &DataType, to: &DataType) -> DataType {
    match (from, to) {
        (DataType::Struct(src_fields), DataType::Struct(tgt_fields))
            if src_fields.len() == tgt_fields.len() =>
        {
            let fields: Fields = src_fields
                .iter()
                .zip(tgt_fields.iter())
                .map(|(src, tgt)| {
                    Arc::new(
                        Field::new(
                            tgt.name(),
                            build_rename_target_type(src.data_type(), tgt.data_type()),
                            src.is_nullable(),
                        )
                        .with_metadata(src.metadata().clone()),
                    )
                })
                .collect();
            DataType::Struct(fields)
        }
        (DataType::List(src), DataType::List(tgt)) => DataType::List(Arc::new(
            Field::new(
                tgt.name(),
                build_rename_target_type(src.data_type(), tgt.data_type()),
                src.is_nullable(),
            )
            .with_metadata(src.metadata().clone()),
        )),
        (DataType::LargeList(src), DataType::LargeList(tgt)) => DataType::LargeList(Arc::new(
            Field::new(
                tgt.name(),
                build_rename_target_type(src.data_type(), tgt.data_type()),
                src.is_nullable(),
            )
            .with_metadata(src.metadata().clone()),
        )),
        (DataType::FixedSizeList(src, sa), DataType::FixedSizeList(tgt, _)) => {
            DataType::FixedSizeList(
                Arc::new(
                    Field::new(
                        tgt.name(),
                        build_rename_target_type(src.data_type(), tgt.data_type()),
                        src.is_nullable(),
                    )
                    .with_metadata(src.metadata().clone()),
                ),
                *sa,
            )
        }
        (DataType::Map(src, sorted), DataType::Map(tgt, _)) => DataType::Map(
            Arc::new(
                Field::new(
                    tgt.name(),
                    build_rename_target_type(src.data_type(), tgt.data_type()),
                    src.is_nullable(),
                )
                .with_metadata(src.metadata().clone()),
            ),
            *sorted,
        ),
        // Leaves: keep the source data type unchanged.
        _ => from.clone(),
    }
}
