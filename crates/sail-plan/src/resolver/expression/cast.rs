use std::ops::{Div, Mul};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef, Fields, IntervalUnit, TimeUnit};
use datafusion_common::{DFSchemaRef, ScalarValue};
use datafusion_expr::{ExprSchemable, ScalarUDF, cast, expr, lit, try_cast};
use sail_common::spec;
use sail_common::utils::datetime::time_unit_to_multiplier;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::plan::PlanService;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_common_datafusion::variant::{is_marked_variant_storage_type, is_variant_storage_field};
use sail_function::scalar::datetime::convert_tz::ConvertTz;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_interval::{
    SparkCalendarInterval, SparkDayTimeInterval, SparkYearMonthInterval, YearMonthIntervalMonths,
};
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::spark_cast_string_to_int32::SparkCastStringToInt32;
use sail_function::scalar::spark_struct_rename::SparkStructRename;
use sail_function::scalar::spark_to_string::{SparkToLargeUtf8, SparkToUtf8, SparkToUtf8View};
use sail_function::scalar::variant::spark_cast_to_variant::SparkCastToVariant;
use sail_function::scalar::variant::spark_variant_get::SparkVariantGet;
use sail_function::scalar::variant::spark_variant_to_json::SparkVariantToJsonUdf;

use crate::error::{PlanError, PlanResult};
use crate::function::is_spark_compatible_arrow_fixed_offset;
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
use crate::resolver::expression::predicate::spark_interval_metadata_for_expression;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) async fn resolve_expression_cast(
        &self,
        expr: spec::Expr,
        cast_to_type: spec::DataType,
        _rename: bool,
        is_try: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        // CAST(expr AS VARIANT) → rewrite to SparkCastToVariant UDF
        // Must intercept before resolve_data_type converts Variant to Struct.
        if matches!(cast_to_type, spec::DataType::Variant) {
            let NamedExpr { expr, name, .. } =
                self.resolve_named_expression(expr, schema, state).await?;
            let name = if need_rename_cast(&expr) {
                let prefix = if is_try { "TRY_" } else { "" };
                vec![format!("{}CAST({} AS VARIANT)", prefix, name.one()?)]
            } else {
                name
            };
            let expr = ScalarUDF::new_from_impl(SparkCastToVariant::new()).call(vec![expr]);
            return Ok(NamedExpr::new(name, expr));
        }

        // Extract the DayTimeInterval field unit before resolving to Arrow type,
        // since it determines the multiplier for numeric-to-interval casts.
        // Spark uses the end field (or start field for single-field intervals)
        // to interpret the numeric value: e.g. DayTimeIntervalType(DAY, DAY) treats
        // the value as days, while DayTimeIntervalType(DAY, SECOND) treats it as seconds.
        let day_time_interval_field = match &cast_to_type {
            spec::DataType::Interval {
                interval_unit: spec::IntervalUnit::DayTime,
                start_field,
                end_field,
            } => end_field.or(*start_field),
            _ => None,
        };
        let spark_interval_metadata = match &cast_to_type {
            spec::DataType::Interval {
                interval_unit,
                start_field,
                end_field,
            } => spec::SparkIntervalMetadata::try_new(*interval_unit, *start_field, *end_field)?
                .map(spec::SparkIntervalMetadata::to_json)
                .transpose()?,
            _ => None,
        };
        let cast_to_type = self.resolve_data_type(&cast_to_type, state)?;
        let NamedExpr { expr, name, .. } =
            self.resolve_named_expression(expr, schema, state).await?;
        let expr_field = expr.to_field(schema)?.1;
        let name = if need_rename_cast(&expr) {
            let service = self.ctx.extension::<PlanService>()?;
            let data_type_string = service
                .plan_formatter()
                .data_type_to_simple_string(&cast_to_type)?;
            vec![format!(
                "{}CAST({} AS {})",
                if is_try { "TRY_" } else { "" },
                name.one()?,
                data_type_string.to_ascii_uppercase()
            )]
        } else {
            name
        };
        let expr = self.cast_to_spark_type(
            expr,
            &expr_field,
            cast_to_type,
            is_try,
            day_time_interval_field,
            schema,
        )?;
        Ok(match spark_interval_metadata {
            Some(metadata) => {
                // Nested expressions consume the Expr without its NamedExpr metadata.
                // Keep the target qualifier on the cast field as well as the projection.
                let field = expr.to_field(schema)?.1;
                let mut field_metadata = field.metadata().clone();
                field_metadata.insert(
                    spec::SAIL_SPARK_INTERVAL_METADATA_KEY.to_string(),
                    metadata.clone(),
                );
                let field = Arc::new(field.as_ref().clone().with_metadata(field_metadata));
                let expr = match expr {
                    expr::Expr::Cast(cast) => {
                        expr::Expr::Cast(expr::Cast::new_from_field(cast.expr, field))
                    }
                    expr::Expr::TryCast(cast) => {
                        expr::Expr::TryCast(expr::TryCast::new_from_field(cast.expr, field))
                    }
                    expr => expr::Expr::Cast(expr::Cast::new_from_field(Box::new(expr), field)),
                };
                NamedExpr::new(name, expr).with_metadata(vec![(
                    spec::SAIL_SPARK_INTERVAL_METADATA_KEY.to_string(),
                    metadata,
                )])
            }
            None => NamedExpr::new(name, expr),
        })
    }

    /// Casts a resolved expression the way a Spark `Cast` does, for ANSI mode and the types
    /// whose Arrow cast differs from Spark's.
    ///
    /// TODO: a Spark cast is nullable where its input is or where the cast can turn a value into
    ///   NULL (`Cast.nullable`, `Cast.forceNullable`), so `CAST('1' AS DECIMAL(2,0))` is nullable.
    ///   A DataFusion cast keeps the nullability of its input. Set operations declare it with a
    ///   `CASE` (`cast_force_nullable` in `query/set_op.rs`); doing it here would add that `CASE`
    ///   to every cast in every plan, so it is left to its own change.
    ///
    /// TODO: with ANSI mode a string that is not a number fails with DataFusion's own message
    ///   (`Cannot cast string 'abc' to value of Int64 type`) rather than `CAST_INVALID_INPUT`.
    pub(in crate::resolver) fn cast_to_spark_type(
        &self,
        expr: expr::Expr,
        expr_field: &Field,
        cast_to_type: DataType,
        is_try: bool,
        day_time_interval_field: Option<spec::IntervalFieldType>,
        schema: &DFSchemaRef,
    ) -> PlanResult<expr::Expr> {
        let expr_type = expr_field.data_type().clone();
        let expr_is_variant = is_variant_storage_field(expr_field);
        let override_string_cast = matches!(
            expr_type,
            DataType::Date32
                | DataType::Date64
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Duration(_)
                | DataType::Interval(_)
                | DataType::Timestamp(_, _)
                | DataType::List(_)
                | DataType::LargeList(_)
                | DataType::FixedSizeList(_, _)
                | DataType::ListView(_)
                | DataType::LargeListView(_)
                | DataType::Struct(_)
                | DataType::Map(_, _)
        );
        let expr = match (expr_type, cast_to_type.clone(), is_try) {
            // A variant is stored as a struct, which only its own conversion can build.
            (_, to, _) if !expr_is_variant && is_marked_variant_storage_type(&to) => {
                ScalarUDF::new_from_impl(SparkCastToVariant::new()).call(vec![expr])
            }
            (
                DataType::Timestamp(_, None),
                DataType::Timestamp(TimeUnit::Microsecond, Some(timezone)),
                is_try,
            ) => {
                if is_spark_compatible_arrow_fixed_offset(timezone.as_ref()) {
                    if is_try {
                        try_cast(expr, cast_to_type)
                    } else {
                        cast(expr, cast_to_type)
                    }
                } else {
                    let timestamp_ntz =
                        cast(expr, DataType::Timestamp(TimeUnit::Microsecond, None));
                    let instant = ScalarUDF::from(ConvertTz::new(false)).call(vec![
                        lit(timezone.to_string()),
                        lit("UTC"),
                        timestamp_ntz,
                    ]);
                    cast(cast(instant, DataType::Int64), cast_to_type)
                }
            }
            (_, DataType::Utf8, _) if expr_is_variant => cast(
                ScalarUDF::new_from_impl(SparkVariantToJsonUdf::new()).call(vec![expr]),
                DataType::Utf8,
            ),
            (_, DataType::LargeUtf8, _) if expr_is_variant => cast(
                ScalarUDF::new_from_impl(SparkVariantToJsonUdf::new()).call(vec![expr]),
                DataType::LargeUtf8,
            ),
            (_, DataType::Utf8View, _) if expr_is_variant => {
                ScalarUDF::new_from_impl(SparkVariantToJsonUdf::new()).call(vec![expr])
            }
            (_, to, is_try) if expr_is_variant => {
                let service = self.ctx.extension::<PlanService>()?;
                let data_type_string = service.plan_formatter().data_type_to_simple_string(&to)?;
                ScalarUDF::new_from_impl(SparkVariantGet::new(is_try)).call(vec![
                    expr,
                    lit("$"),
                    lit(data_type_string),
                ])
            }
            (DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View, DataType::Int32, false)
                if !self.config.ansi_mode =>
            {
                ScalarUDF::new_from_impl(SparkCastStringToInt32::new()).call(vec![expr])
            }
            (from, DataType::Timestamp(time_unit, _) | DataType::Duration(time_unit), _)
                if from.is_numeric() =>
            {
                let multiplier = match (day_time_interval_field, &cast_to_type) {
                    (Some(field), DataType::Duration(_)) => day_time_field_to_microseconds(field),
                    _ => time_unit_to_multiplier(&time_unit),
                };
                cast(expr.mul(lit(multiplier)), cast_to_type)
            }
            (DataType::Timestamp(time_unit, _) | DataType::Duration(time_unit), to, _)
                if to.is_numeric() =>
            {
                cast(
                    lit(1.0)
                        .div(lit(time_unit_to_multiplier(&time_unit)))
                        .mul(cast(expr, DataType::Int64)),
                    to,
                )
            }
            (DataType::Interval(IntervalUnit::YearMonth), to, is_try) if to.is_integer() => {
                let interval_metadata = expr_field
                    .metadata()
                    .get(spec::SAIL_SPARK_INTERVAL_METADATA_KEY)
                    .map(|value| spec::SparkIntervalMetadata::from_json(value))
                    .transpose()?;
                let months = ScalarUDF::from(YearMonthIntervalMonths::new()).call(vec![expr]);
                let value = if matches!(
                    interval_metadata,
                    Some(spec::SparkIntervalMetadata::YearMonth {
                        end_field: spec::YearMonthIntervalField::Year,
                        ..
                    })
                ) {
                    months / lit(12_i32)
                } else {
                    months
                };
                if is_try {
                    try_cast(value, to)
                } else {
                    cast(value, to)
                }
            }
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                DataType::Interval(IntervalUnit::YearMonth),
                _,
            ) => ScalarUDF::new_from_impl(SparkYearMonthInterval::new()).call(vec![expr]),
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                DataType::Duration(TimeUnit::Microsecond),
                _,
            ) => ScalarUDF::new_from_impl(SparkDayTimeInterval::new()).call(vec![expr]),
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                DataType::Interval(IntervalUnit::MonthDayNano),
                _,
            ) => ScalarUDF::new_from_impl(SparkCalendarInterval::new()).call(vec![expr]),
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                DataType::Date32,
                is_try,
            ) => ScalarUDF::new_from_impl(SparkDate::new(is_try || !self.config.ansi_mode))
                .call(vec![expr]),
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                DataType::Timestamp(TimeUnit::Microsecond, tz),
                is_try,
            ) => Arc::new(ScalarUDF::new_from_impl(SparkTimestamp::try_new(
                tz,
                self.config.ansi_mode,
                is_try,
            )?))
            .call(vec![expr]),
            (_, DataType::Utf8, _) if override_string_cast => {
                ScalarUDF::new_from_impl(SparkToUtf8::new())
                    .call(spark_string_cast_arguments(expr, schema)?)
            }
            (_, DataType::LargeUtf8, _) if override_string_cast => {
                ScalarUDF::new_from_impl(SparkToLargeUtf8::new())
                    .call(spark_string_cast_arguments(expr, schema)?)
            }
            (_, DataType::Utf8View, _) if override_string_cast => {
                ScalarUDF::new_from_impl(SparkToUtf8View::new())
                    .call(spark_string_cast_arguments(expr, schema)?)
            }
            (DataType::Date32 | DataType::Date64, to, _)
                if to.is_numeric() || matches!(to, DataType::Boolean) =>
            {
                if !is_try && self.config.ansi_mode {
                    return Err(PlanError::invalid(format!("cannot cast date to {to}")));
                }
                lit(ScalarValue::try_from(&to)?)
            }
            (from, to, _)
                if struct_repeats_a_field_name(&to) && struct_arity_aligns(&from, &to) =>
            {
                // A struct that names two fields the same cannot be cast by name, so the fields
                // are named by their position for the conversion and renamed to what the target
                // asks for afterwards. Both renames are metadata only, so only the middle cast
                // converts a value, and it pairs the fields in order as Spark does.
                let positional_from = build_positional_names_type(&from);
                let positional_to = build_positional_names_type(&to);
                let renamed =
                    ScalarUDF::new_from_impl(SparkStructRename::new(positional_from.clone()))
                        .call(vec![expr]);
                let converted = if positional_from == positional_to {
                    renamed
                } else if is_try {
                    try_cast(renamed, positional_to)
                } else {
                    cast(renamed, positional_to)
                };
                ScalarUDF::new_from_impl(SparkStructRename::new(to)).call(vec![converted])
            }
            (from, to, _) if needs_struct_field_rename(&from, &to) => {
                // Pre-rename the source struct fields positionally so the cast
                // becomes a no-op or a valid name-matched one (see
                // `needs_struct_field_rename`).
                let renamed_target = build_rename_target_type(&from, &to);
                let renamed =
                    ScalarUDF::new_from_impl(SparkStructRename::new(renamed_target.clone()))
                        .call(vec![expr]);
                if renamed_target == to {
                    renamed
                } else if is_try {
                    try_cast(renamed, to)
                } else {
                    cast(renamed, to)
                }
            }
            (_, to, true) => try_cast(expr, to),
            (_, to, _) => cast(expr, to),
        };
        Ok(expr)
    }
}

fn spark_string_cast_arguments(
    expr: expr::Expr,
    schema: &DFSchemaRef,
) -> PlanResult<Vec<expr::Expr>> {
    let interval = spark_interval_metadata_for_expression(&expr, schema)?;
    let mut arguments = vec![expr];
    if let Some(interval) = interval {
        // Physical expression serialization does not preserve intermediate field metadata.
        arguments.push(lit(interval.to_json()?));
    }
    Ok(arguments)
}

/// Returns true where a struct at any level of the type names two of its fields the same.
/// DataFusion picks the source child of a struct cast with `column_by_name`
/// (`datafusion_common::nested_struct::cast_struct_column`), which answers with the first field of
/// that name, so a cast that converts a leaf would read one field twice and lose the value of the
/// other. Spark pairs the fields by position instead (`Cast.castStruct`).
fn struct_repeats_a_field_name(data_type: &DataType) -> bool {
    match data_type {
        DataType::Struct(fields) => {
            fields
                .iter()
                .enumerate()
                .any(|(i, x)| fields.iter().take(i).any(|y| y.name() == x.name()))
                || fields
                    .iter()
                    .any(|x| struct_repeats_a_field_name(x.data_type()))
        }
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            struct_repeats_a_field_name(field.data_type())
        }
        DataType::Map(field, _) => struct_repeats_a_field_name(field.data_type()),
        _ => false,
    }
}

/// Returns true where both types hold the same containers with the same number of struct fields at
/// every level, which is what pairing the fields by position needs.
fn struct_arity_aligns(from: &DataType, to: &DataType) -> bool {
    match (from, to) {
        (DataType::Struct(a), DataType::Struct(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b.iter())
                    .all(|(x, y)| struct_arity_aligns(x.data_type(), y.data_type()))
        }
        (DataType::List(a), DataType::List(b))
        | (DataType::LargeList(a), DataType::LargeList(b)) => {
            struct_arity_aligns(a.data_type(), b.data_type())
        }
        (DataType::FixedSizeList(a, sa), DataType::FixedSizeList(b, sb)) if sa == sb => {
            struct_arity_aligns(a.data_type(), b.data_type())
        }
        (DataType::Map(a, sa), DataType::Map(b, sb)) if sa == sb => {
            struct_arity_aligns(a.data_type(), b.data_type())
        }
        // A container on one side and something else on the other never align, so the names are
        // left as they are: renaming them would reach a cast that cannot be done anyway, and the
        // positional names would be what the failure names.
        (from, to) if is_nested(from) || is_nested(to) => false,
        _ => true,
    }
}

fn is_nested(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Struct(_)
            | DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Map(_, _)
    )
}

/// The same type with every struct field named after its position. Applied to both sides of a cast
/// it names the fields unambiguously and pairs them in order, which is the conversion Spark does.
/// The names are rewritten in full, so they cannot collide with one another.
fn build_positional_names_type(data_type: &DataType) -> DataType {
    let renamed_field = |field: &FieldRef| {
        Arc::new(
            Field::new(
                field.name(),
                build_positional_names_type(field.data_type()),
                field.is_nullable(),
            )
            .with_metadata(field.metadata().clone()),
        )
    };
    match data_type {
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .enumerate()
                .map(|(i, field)| {
                    Arc::new(
                        Field::new(
                            format!("col{}", i + 1),
                            build_positional_names_type(field.data_type()),
                            field.is_nullable(),
                        )
                        .with_metadata(field.metadata().clone()),
                    )
                })
                .collect::<Fields>(),
        ),
        DataType::List(field) => DataType::List(renamed_field(field)),
        DataType::LargeList(field) => DataType::LargeList(renamed_field(field)),
        DataType::FixedSizeList(field, size) => {
            DataType::FixedSizeList(renamed_field(field), *size)
        }
        DataType::Map(field, sorted) => DataType::Map(renamed_field(field), *sorted),
        _ => data_type.clone(),
    }
}

/// Returns true if the cast from `from` to `to` involves a Struct
/// (possibly nested in a List/LargeList/FixedSizeList/Map) whose field names
/// don't share enough overlap for DataFusion's struct cast validator.
fn needs_struct_field_rename(from: &DataType, to: &DataType) -> bool {
    match (from, to) {
        // Spark casts a struct by position at every level, so a field whose own type needs a
        // rename counts as well.
        (DataType::Struct(a), DataType::Struct(b)) => {
            a.len() == b.len()
                && a.iter().zip(b.iter()).any(|(fa, fb)| {
                    fa.name() != fb.name()
                        || needs_struct_field_rename(fa.data_type(), fb.data_type())
                })
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
fn build_rename_target_type(from: &DataType, to: &DataType) -> DataType {
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

fn day_time_field_to_microseconds(field: spec::IntervalFieldType) -> i64 {
    match field {
        spec::IntervalFieldType::Day => 86_400_000_000,
        spec::IntervalFieldType::Hour => 3_600_000_000,
        spec::IntervalFieldType::Minute => 60_000_000,
        // Second, or Year/Month (shouldn't appear for DayTime intervals)
        _ => 1_000_000,
    }
}

fn need_rename_cast(expr: &expr::Expr) -> bool {
    match expr {
        expr::Expr::Alias(_) | expr::Expr::Column(_) | expr::Expr::OuterReferenceColumn(..) => {
            false
        }
        expr::Expr::Cast(cast) => need_rename_cast(cast.expr.as_ref()),
        expr::Expr::TryCast(try_cast) => need_rename_cast(try_cast.expr.as_ref()),
        _ => true,
    }
}
