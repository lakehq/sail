use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields, IntervalUnit, TimeUnit};
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchemaRef, Result as DataFusionResult};
use datafusion_expr::type_coercion::binary::type_union_resolution;
use datafusion_expr::{ExprSchemable, expr};
use sail_common::spec;

/// The type Spark widens a pair of NUMERIC types to, for the places that pick one type for several
/// branches: `CASE`, `IF`, and the set operations.
///
/// DataFusion's `type_union_resolution` is close but not Spark's `findWiderTypeForTwo`, and the two
/// places it differs both LOSE data:
///   - a fractional beside a DECIMAL is a DOUBLE (`TypeCoercionHelper.scala:194-195`), where the
///     union resolution answers a DECIMAL that a large DOUBLE overflows;
///   - an integral beside a FLOAT is a DOUBLE, "to avoid potential precision loss on converting the
///     Integral type as Float type" (`AnsiTypeCoercion.scala:113-123`), where the union resolution
///     answers FLOAT and `16777217` comes back as `16777216`. That clause is ANSI's alone: the
///     default mode takes `numericPrecedence` (`TypeCoercion.scala:89-92`) and keeps the FLOAT.
///
/// Returns `None` when the pair is not numeric, or when DataFusion finds no common type: the caller
/// then leaves the branches as they are.
pub(crate) fn spark_wider_numeric_type(
    left: &DataType,
    right: &DataType,
    ansi_mode: bool,
) -> Option<DataType> {
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
    if ansi_mode && matches!(wider, DataType::Float32 | DataType::Float16) {
        return Some(DataType::Float64);
    }
    Some(wider)
}

/// The type a whole set of branches widens to, folding [`spark_wider_numeric_type`] over them.
pub(crate) fn spark_wider_numeric_type_of(
    data_types: &[DataType],
    ansi_mode: bool,
) -> Option<DataType> {
    let mut common = data_types.first()?.clone();
    for data_type in data_types.iter().skip(1) {
        common = spark_wider_numeric_type(&common, data_type, ansi_mode)?;
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
pub(crate) fn spark_wider_type(
    left: &DataType,
    right: &DataType,
    ansi_mode: bool,
) -> Option<DataType> {
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
        let data_type = spark_wider_type(field.data_type(), other.data_type(), ansi_mode)?;
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
        // `findTypeForComplex` pairs struct fields through `SQLConf.get.resolver` and gives up when
        // a pair of names does not match (`TypeCoercionHelper.scala:164-176`), so Spark refuses the
        // pair rather than renaming it. The default resolver is case-insensitive.
        (DataType::Struct(left), DataType::Struct(right)) if left.len() == right.len() => {
            let fields = left
                .iter()
                .zip(right.iter())
                .map(|(left, right)| {
                    left.name()
                        .eq_ignore_ascii_case(right.name())
                        .then(|| Some(Arc::new(list_element(left, right)?)))?
                })
                .collect::<Option<Fields>>()?;
            Some(DataType::Struct(fields))
        }
        (left, right) if left.is_nested() || right.is_nested() => None,
        (left, right) => spark_wider_string_type(left, right, ansi_mode)
            .or_else(|| spark_wider_numeric_type(left, right, ansi_mode)),
    }
}

/// Spark applies its STRING promotion before returning from `findWiderTypeForTwo`, including when
/// that call came recursively from an ARRAY, MAP, or STRUCT. Legacy coercion promotes the atomic
/// peer to STRING; ANSI promotes the STRING to BIGINT, DOUBLE, or the temporal peer
/// (`TypeCoercion.scala:105-122`; `AnsiStringPromotionTypeCoercion.scala:92-106).
fn spark_wider_string_type(left: &DataType, right: &DataType, ansi_mode: bool) -> Option<DataType> {
    let (string, other) = if left.is_string() {
        (left, right)
    } else if right.is_string() {
        (right, left)
    } else {
        return None;
    };
    if !ansi_mode {
        return matches!(
            other,
            DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)
        )
        .then(|| string.clone())
        .or_else(|| other.is_numeric().then(|| string.clone()));
    }
    match other {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            Some(DataType::Int64)
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            Some(DataType::Int64)
        }
        DataType::Float16 | DataType::Float32 | DataType::Float64 => Some(DataType::Float64),
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) => Some(other.clone()),
        _ => None,
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

/// Whether two types are struct pairs Spark refuses to type: `findTypeForComplex` returns `None`
/// when the two structs have a different number of fields or a pair of names its resolver does not
/// match (`TypeCoercionHelper.scala:164-176`), and `Coalesce.checkInputDataTypes` then raises
/// `DATATYPE_MISMATCH.DATA_DIFF_TYPES` (`nullExpressions.scala:78-86`). Recurses into an array and a
/// map so a list of such structs is refused too.
pub(crate) fn struct_pair_spark_refuses(left: &DataType, right: &DataType) -> bool {
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
        )
        | (DataType::Map(left, _), DataType::Map(right, _)) => {
            struct_pair_spark_refuses(left.data_type(), right.data_type())
        }
        (DataType::Struct(left), DataType::Struct(right)) => {
            left.len() != right.len()
                || left.iter().zip(right.iter()).any(|(left, right)| {
                    !left.name().eq_ignore_ascii_case(right.name())
                        || struct_pair_spark_refuses(left.data_type(), right.data_type())
                })
        }
        (DataType::Struct(_), other) | (other, DataType::Struct(_)) => !other.is_null(),
        _ => false,
    }
}

pub(crate) fn spark_interval_metadata_for_expression(
    expression: &expr::Expr,
    schema: &DFSchemaRef,
) -> DataFusionResult<Option<spec::SparkIntervalMetadata>> {
    let field = expression.to_field(schema.as_ref())?.1;
    if !matches!(
        field.data_type(),
        DataType::Duration(TimeUnit::Microsecond) | DataType::Interval(IntervalUnit::YearMonth)
    ) {
        return Ok(None);
    }

    let interval_type = field.data_type().clone();
    let mut combined = None::<spec::SparkIntervalMetadata>;
    expression.apply(|candidate| {
        // Interval scaling returns the default DAY TO SECOND range. Its input range
        // must not leak into a later string conversion (MultiplyDTInterval/DivideDTInterval).
        if let expr::Expr::ScalarFunction(function) = candidate
            && matches!(
                function.func.name(),
                "spark_multiply_dt_interval" | "spark_divide_dt_interval"
            )
        {
            let default = spec::SparkIntervalMetadata::DayTime {
                start_field: spec::DayTimeIntervalField::Day,
                end_field: spec::DayTimeIntervalField::Second,
            };
            combined = Some(match combined {
                Some(current) => current.wider(default).unwrap_or(default),
                None => default,
            });
            return Ok(TreeNodeRecursion::Jump);
        }
        let field = candidate.to_field(schema.as_ref())?.1;
        if field.data_type() != &interval_type {
            return Ok(TreeNodeRecursion::Jump);
        }
        let Some(value) = field.metadata().get(spec::SAIL_SPARK_INTERVAL_METADATA_KEY) else {
            return Ok(TreeNodeRecursion::Continue);
        };
        let candidate = spec::SparkIntervalMetadata::from_json(value)
            .map_err(|error| datafusion_common::DataFusionError::Plan(error.to_string()))?;
        combined = Some(match combined {
            None => candidate,
            Some(current) => current.wider(candidate).ok_or_else(|| {
                datafusion_common::DataFusionError::Plan(
                    "incompatible Spark interval metadata in expression".to_string(),
                )
            })?,
        });
        Ok(TreeNodeRecursion::Jump)
    })?;
    Ok(combined)
}

/// Temporary identity of DATE - DATE while its physical value remains an INT day count.
pub(crate) const SAIL_DATE_DIFFERENCE_METADATA_KEY: &str = "SAIL::spark::date_difference";
