use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::extension::EXTENSION_TYPE_NAME_KEY;
use datafusion::functions::core::expr_fn::get_field;
use datafusion::functions_window::row_number::row_number_udwf;
use datafusion::logical_expr::expr::NullTreatment;
use datafusion_common::arrow::datatypes::{DataType, Field, FieldRef, Fields, IntervalUnit};
use datafusion_common::{Column, DFSchemaRef, JoinType, NullEquality, ScalarValue};
use datafusion_expr::builder::project;
use datafusion_expr::expr::{FieldMetadata, WindowFunctionParams};
use datafusion_expr::{
    Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Projection, ScalarUDF, WindowFrame,
    WindowFunctionDefinition, cast, expr, lit, when,
};
use sail_common::spec;
use sail_common::utils::string::to_lowercase;
use sail_common_datafusion::variant::{is_marked_variant_storage_type, is_variant_storage_field};
use sail_function::scalar::struct_function::StructFunction;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::expression::attribute::quote_identifier_name;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_set_operation(
        &self,
        op: spec::SetOperation,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        use spec::SetOpType;

        let spec::SetOperation {
            left,
            right,
            set_op_type,
            is_all,
            by_name,
            allow_missing_columns,
        } = op;
        let left = self.resolve_query_plan(*left, state).await?;
        let right = self.resolve_query_plan(*right, state).await?;
        match set_op_type {
            SetOpType::Intersect => {
                let (left_schema, right_schema) = (left.schema().clone(), right.schema().clone());
                let operator = if is_all { "INTERSECT ALL" } else { "INTERSECT" };
                self.check_set_operation_types(
                    operator,
                    false,
                    false,
                    &left_schema,
                    &right_schema,
                )?;
                let (left, right) =
                    self.widen_set_operation_inputs(left, right, false, false, (false, false))?;
                self.reject_map_column_in_set_output(left.schema(), right.schema(), state)?;
                // TODO: an intersection can hold a NULL only where both inputs can
                //   (`Intersect.mergeChildOutputs`), and DataFusion keeps the nullability of the
                //   first input. Narrowing it with an expression is unsafe while Sail can declare a
                //   column non-nullable that holds NULL (an empty scalar subquery), since the
                //   expression would then replace a real NULL.
                let plan = LogicalPlanBuilder::intersect(left, right, is_all)?;
                self.keep_left_metadata(plan, &left_schema, &right_schema, false, false)
            }
            SetOpType::Union => {
                let (left, right) = if by_name {
                    let left_names = Self::get_field_names(left.schema(), state)?;
                    let right_names = Self::get_field_names(right.schema(), state)?;
                    self.reject_duplicate_column_names(&left_names)?;
                    self.reject_duplicate_column_names(&right_names)?;
                    // Each column of the first input is matched in turn, and the first one that
                    // fails decides the error (`ResolveUnion.compareAndAddFields`).
                    let (mut left_reordered_columns, mut right_reordered_columns): (
                        Vec<Expr>,
                        Vec<Expr>,
                    ) = left_names
                        .iter()
                        .enumerate()
                        .map(|(left_idx, left_name)| {
                            match right_names
                                .iter()
                                .position(|right_name| self.match_identifier(left_name, right_name))
                            {
                                Some(right_idx) => {
                                    let left_field = left.schema().field(left_idx);
                                    let right_field = right.schema().field(right_idx);
                                    if !allow_missing_columns {
                                        self.check_nested_fields_by_name(
                                            left_field.data_type(),
                                            right_field.data_type(),
                                        )?;
                                    }
                                    let left_column = Expr::Column(Column::from(
                                        left.schema().qualified_field(left_idx),
                                    ));
                                    let right_column = Expr::Column(Column::from(
                                        right.schema().qualified_field(right_idx),
                                    ));
                                    // The fields of a struct are rebuilt in the order of the first
                                    // input, and with `allowMissingColumns` the first input takes
                                    // the fields only the second one has (`ResolveUnion`).
                                    let (right_expr, right_type) = match self
                                        .rebuild_struct_by_name(
                                            right_column.clone(),
                                            right_field,
                                            left_field.data_type(),
                                            allow_missing_columns,
                                        )? {
                                        Some((expr, data_type)) => {
                                            (expr.alias(right_field.name()), data_type)
                                        }
                                        None => (right_column, right_field.data_type().clone()),
                                    };
                                    let left_expr = match allow_missing_columns
                                        .then(|| {
                                            self.rebuild_struct_by_name(
                                                left_column.clone(),
                                                left_field,
                                                &right_type,
                                                true,
                                            )
                                        })
                                        .transpose()?
                                        .flatten()
                                    {
                                        Some((expr, _)) => expr.alias(left_field.name()),
                                        None => left_column,
                                    };
                                    Ok((left_expr, right_expr))
                                }
                                None if allow_missing_columns => Ok((
                                    Expr::Column(Column::from(
                                        left.schema().qualified_field(left_idx),
                                    )),
                                    // The padded column keeps the type of the side that has it.
                                    cast(
                                        lit(ScalarValue::Null),
                                        left.schema().field(left_idx).data_type().clone(),
                                    )
                                    .alias(state.register_field_name(left_name)),
                                )),
                                None => Err(PlanError::AnalysisError(format!(
                                    "[UNRESOLVED_COLUMN_AMONG_FIELD_NAMES] Cannot resolve column \
                                     name \"{left_name}\" among ({}).",
                                    right_names.join(", ")
                                ))),
                            }
                        })
                        .collect::<PlanResult<Vec<(Expr, Expr)>>>()?
                        .into_iter()
                        .unzip();
                    if allow_missing_columns {
                        let (left_extra_columns, right_extra_columns): (Vec<Expr>, Vec<Expr>) =
                            right_names
                                .into_iter()
                                .enumerate()
                                .filter(|(_, right_name)| {
                                    !left_names.iter().any(|left_name| {
                                        self.match_identifier(left_name, right_name)
                                    })
                                })
                                .map(|(right_idx, right_name)| {
                                    (
                                        cast(
                                            lit(ScalarValue::Null),
                                            right.schema().field(right_idx).data_type().clone(),
                                        )
                                        .alias(state.register_field_name(right_name)),
                                        Expr::Column(Column::from(
                                            right.schema().qualified_field(right_idx),
                                        )),
                                    )
                                })
                                .collect::<Vec<(Expr, Expr)>>()
                                .into_iter()
                                .unzip();
                        right_reordered_columns.extend(right_extra_columns);
                        left_reordered_columns.extend(left_extra_columns);
                        (
                            project(left, left_reordered_columns)?,
                            project(right, right_reordered_columns)?,
                        )
                    } else {
                        // A column of the second input with no match in the first is appended
                        // to it rather than dropped (`ResolveUnion`), so it fails the count.
                        if right_names.len() != left_names.len() {
                            return Err(num_columns_mismatch_error(
                                "UNION",
                                left_names.len(),
                                right_names.len(),
                            ));
                        }
                        (left, project(right, right_reordered_columns)?)
                    }
                } else {
                    (left, right)
                };
                let left_schema = left.schema().clone();
                let right_schema = right.schema().clone();
                self.check_set_operation_types(
                    "UNION",
                    true,
                    by_name,
                    &left_schema,
                    &right_schema,
                )?;
                let (left, right) =
                    self.widen_set_operation_inputs(left, right, true, by_name, (true, true))?;
                let (widened_left, widened_right) = (left.schema().clone(), right.schema().clone());
                let left = self.widen_union_nullability(left, &widened_right, by_name)?;
                let plan = LogicalPlanBuilder::new(left).union(right)?.build()?;
                let plan =
                    self.keep_left_metadata(plan, &left_schema, &right_schema, true, by_name)?;
                if is_all {
                    Ok(plan)
                } else {
                    // An operation that compares whole rows rejects a column that holds a map.
                    self.reject_map_column_in_set_output(&widened_left, &widened_right, state)?;
                    // The metadata is written below the `Distinct`, so that DataFusion still
                    // flattens a chain of unions.
                    Ok(LogicalPlanBuilder::from(plan).distinct()?.build()?)
                }
            }
            SetOpType::Except => {
                let (left_schema, right_schema) = (left.schema().clone(), right.schema().clone());
                let operator = if is_all { "EXCEPT ALL" } else { "EXCEPT" };
                self.check_set_operation_types(
                    operator,
                    false,
                    false,
                    &left_schema,
                    &right_schema,
                )?;
                let (left, right) =
                    self.widen_set_operation_inputs(left, right, false, false, (true, false))?;
                self.reject_map_column_in_set_output(left.schema(), right.schema(), state)?;

                let mut join_keys = left
                    .schema()
                    .fields()
                    .iter()
                    .zip(right.schema().fields().iter())
                    .map(|(left_field, right_field)| {
                        (
                            Column::from_name(left_field.name()),
                            Column::from_name(right_field.name()),
                        )
                    })
                    .collect::<Vec<_>>();

                let plan = if is_all {
                    let left_row_number_alias = state.register_field_name("row_num");
                    let right_row_number_alias = state.register_field_name("row_num");
                    let left_row_number_window =
                        Expr::WindowFunction(Box::new(expr::WindowFunction {
                            fun: WindowFunctionDefinition::WindowUDF(row_number_udwf()),
                            params: WindowFunctionParams {
                                args: vec![],
                                partition_by: left
                                    .schema()
                                    .fields()
                                    .iter()
                                    .map(|field| Expr::Column(Column::from_name(field.name())))
                                    .collect::<Vec<_>>(),
                                order_by: vec![],
                                window_frame: WindowFrame::new(None),
                                filter: None,
                                null_treatment: Some(NullTreatment::RespectNulls),
                                distinct: false,
                            },
                        }))
                        .alias(left_row_number_alias.as_str());
                    let right_row_number_window =
                        Expr::WindowFunction(Box::new(expr::WindowFunction {
                            fun: WindowFunctionDefinition::WindowUDF(row_number_udwf()),
                            params: WindowFunctionParams {
                                args: vec![],
                                partition_by: right
                                    .schema()
                                    .fields()
                                    .iter()
                                    .map(|field| Expr::Column(Column::from_name(field.name())))
                                    .collect::<Vec<_>>(),
                                order_by: vec![],
                                window_frame: WindowFrame::new(None),
                                filter: None,
                                null_treatment: Some(NullTreatment::RespectNulls),
                                distinct: false,
                            },
                        }))
                        .alias(right_row_number_alias.as_str());
                    let left = LogicalPlanBuilder::from(left)
                        .window(vec![left_row_number_window])?
                        .build()?;
                    let right = LogicalPlanBuilder::from(right)
                        .window(vec![right_row_number_window])?
                        .build()?;
                    let left_join_columns = join_keys
                        .iter()
                        .map(|(left_col, _)| left_col.clone())
                        .collect::<Vec<_>>();
                    join_keys.push((
                        Column::from_name(left_row_number_alias),
                        Column::from_name(right_row_number_alias),
                    ));
                    LogicalPlanBuilder::from(left)
                        .join_detailed(
                            right,
                            JoinType::LeftAnti,
                            join_keys.into_iter().unzip(),
                            None,
                            NullEquality::NullEqualsNull,
                        )?
                        .project(left_join_columns)?
                        .build()
                } else {
                    LogicalPlanBuilder::from(left)
                        .distinct()?
                        .join_detailed(
                            right,
                            JoinType::LeftAnti,
                            join_keys.into_iter().unzip(),
                            None,
                            NullEquality::NullEqualsNull,
                        )?
                        .build()
                }?;
                self.keep_left_metadata(plan, &left_schema, &right_schema, false, false)
            }
        }
    }
}

/// The family of a type as Spark's widening sees it. `Other` is a type the check does not model,
/// which is left to DataFusion as before.
#[derive(Clone, Copy, PartialEq, Eq)]
enum TypeFamily<'a> {
    Null,
    Boolean,
    Integral,
    Fractional,
    Decimal,
    String,
    Binary,
    Date,
    TimestampNtz,
    Timestamp,
    Time,
    YearMonthInterval,
    DayTimeInterval,
    Array(&'a FieldRef),
    Map(&'a FieldRef, &'a FieldRef),
    Struct(&'a Fields),
    /// A VARIANT, an atomic type to Spark stored as a marked struct.
    Variant,
    /// A user-defined type, known by the field metadata; compatible only with itself.
    Udt,
    /// A CALENDAR INTERVAL, which is not atomic and is compatible only with itself.
    CalendarInterval,
    Other,
}

impl<'a> TypeFamily<'a> {
    fn of(field: &'a FieldRef) -> Self {
        use DataType::*;

        if field
            .metadata()
            .contains_key(spec::SAIL_SPARK_UDT_METADATA_KEY)
        {
            return Self::Udt;
        }
        // A variant carries the Arrow extension type or marks its storage fields.
        if is_variant_storage_field(field) {
            return Self::Variant;
        }
        if field.metadata().contains_key(EXTENSION_TYPE_NAME_KEY) {
            return Self::Other;
        }
        match field.data_type() {
            Null => Self::Null,
            Boolean => Self::Boolean,
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 => Self::Integral,
            Float16 | Float32 | Float64 => Self::Fractional,
            Decimal32(..) | Decimal64(..) | Decimal128(..) | Decimal256(..) => Self::Decimal,
            Utf8 | LargeUtf8 | Utf8View => Self::String,
            Binary | LargeBinary | BinaryView | FixedSizeBinary(_) => Self::Binary,
            Date32 | Date64 => Self::Date,
            Timestamp(_, None) => Self::TimestampNtz,
            Timestamp(_, Some(_)) => Self::Timestamp,
            Time32(_) | Time64(_) => Self::Time,
            Interval(IntervalUnit::YearMonth) => Self::YearMonthInterval,
            Duration(_) => Self::DayTimeInterval,
            Interval(IntervalUnit::MonthDayNano) => Self::CalendarInterval,
            List(field)
            | LargeList(field)
            | FixedSizeList(field, _)
            | ListView(field)
            | LargeListView(field) => Self::Array(field),
            Map(field, _) => match field.data_type() {
                Struct(fields) if fields.len() == 2 => Self::Map(&fields[0], &fields[1]),
                _ => Self::Other,
            },
            Struct(fields) => Self::Struct(fields),
            _ => Self::Other,
        }
    }

    fn is_numeric(self) -> bool {
        matches!(self, Self::Integral | Self::Fractional | Self::Decimal)
    }

    fn is_datetime(self) -> bool {
        matches!(self, Self::Date | Self::TimestampNtz | Self::Timestamp)
    }

    fn is_atomic(self) -> bool {
        !matches!(
            self,
            Self::Null
                | Self::Array(_)
                | Self::Map(..)
                | Self::Struct(_)
                | Self::Udt
                | Self::CalendarInterval
                | Self::Other
        )
    }
}

/// Whether either of two checks holds: true when one does, false when both fail, and `None` when
/// neither holds and one of them is not modeled.
fn either(a: Option<bool>, b: impl FnOnce() -> Option<bool>) -> Option<bool> {
    if a == Some(true) {
        return a;
    }
    match (a, b()) {
        (_, Some(true)) => Some(true),
        (Some(false), Some(false)) => Some(false),
        _ => None,
    }
}

/// Whether every check holds: false when one fails, and `None` when none fails and one of them
/// is not modeled.
fn every(answers: impl IntoIterator<Item = Option<bool>>) -> Option<bool> {
    let mut result = Some(true);
    for answer in answers {
        match answer {
            Some(false) => return Some(false),
            None => result = None,
            Some(true) => {}
        }
    }
    result
}

/// The ordinal Spark writes for a position counted from zero (`ordinalNumber`).
fn ordinal_number(i: usize) -> String {
    match i {
        0 => "first".to_string(),
        1 => "second".to_string(),
        2 => "third".to_string(),
        _ => format!("{}th", i + 1),
    }
}

/// Whether two fields hold the same user-defined type.
fn same_udt(left: &FieldRef, right: &FieldRef) -> bool {
    left.data_type().equals_datatype(right.data_type())
        && left.metadata().get(spec::SAIL_SPARK_UDT_METADATA_KEY)
            == right.metadata().get(spec::SAIL_SPARK_UDT_METADATA_KEY)
}

/// Whether a cast from one type to the other can turn a value into NULL (`Cast.forceNullable`).
fn cast_force_nullable(from: &DataType, to: &DataType) -> bool {
    use DataType::*;

    let is_string = |t: &DataType| matches!(t, Utf8 | LargeUtf8 | Utf8View);
    let is_integral = |t: &DataType| {
        matches!(
            t,
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64
        )
    };
    let decimal = |t: &DataType| match t {
        Decimal32(p, s) | Decimal64(p, s) | Decimal128(p, s) | Decimal256(p, s) => {
            Some((i32::from(*p), i32::from(*s)))
        }
        _ => None,
    };
    // `canNullSafeCastToDecimal`: the decimal holds every value of the other type.
    let null_safe_to_decimal = |from: &DataType, (p, s): (i32, i32)| {
        let wider_than = |(q, t): (i32, i32)| p - s >= q - t && s >= t;
        match from {
            Boolean => wider_than((1, 0)),
            Int8 | UInt8 => wider_than((3, 0)),
            Int16 | UInt16 => wider_than((5, 0)),
            Int32 | UInt32 => wider_than((10, 0)),
            Int64 | UInt64 => wider_than((20, 0)),
            other => match decimal(other) {
                Some(d) => wider_than(d) || p - s > d.0 - d.1,
                None => false,
            },
        }
    };
    if from.is_null() || same_type_ignoring_nullability(from, to) {
        return false;
    }
    match (from, to) {
        (f, t) if is_string(f) => !(is_string(t) || matches!(t, Binary)),
        (_, t) if is_string(t) => false,
        (Timestamp(_, Some(_)), Int8 | Int16 | Int32) => true,
        (Time32(_) | Time64(_), Int8 | Int16) => true,
        (Float32 | Float64, Timestamp(_, Some(_))) => true,
        (Timestamp(_, Some(_)), Date32 | Date64) => false,
        (_, Date32 | Date64) => true,
        (Date32 | Date64, Timestamp(_, Some(_))) => false,
        (Date32 | Date64, _) => true,
        (_, Interval(IntervalUnit::MonthDayNano)) => true,
        (f, t) if decimal(t).is_some() => decimal(t).is_some_and(|d| !null_safe_to_decimal(f, d)),
        (f, t) => (f.is_floating() || decimal(f).is_some()) && is_integral(t),
    }
}

/// Whether two types are the same once the nullability of what they nest is ignored.
fn same_type_ignoring_nullability(left: &DataType, right: &DataType) -> bool {
    fn nullable(data_type: &DataType) -> DataType {
        let field = |f: &FieldRef| {
            Arc::new(
                Field::new(f.name(), nullable(f.data_type()), true)
                    .with_metadata(Default::default()),
            )
        };
        match data_type {
            DataType::List(f) => DataType::List(field(f)),
            DataType::LargeList(f) => DataType::LargeList(field(f)),
            DataType::FixedSizeList(f, n) => DataType::FixedSizeList(field(f), *n),
            DataType::Map(f, sorted) => DataType::Map(field(f), *sorted),
            DataType::Struct(fields) => {
                DataType::Struct(fields.iter().map(field).collect::<Vec<_>>().into())
            }
            other => other.clone(),
        }
    }
    nullable(left) == nullable(right)
}

/// The error for two inputs with a different number of columns (`numColumnsMismatch`).
fn num_columns_mismatch_error(operator: &str, left: usize, right: usize) -> PlanError {
    PlanError::AnalysisError(format!(
        "[NUM_COLUMNS_MISMATCH] {operator} can only be performed on inputs with the same number \
         of columns, but the first input has {left} columns and the second input has {right} \
         columns."
    ))
}

impl PlanResolver<'_> {
    /// Whether Spark finds a type two columns widen to (`findWiderTypeForTwo`), which differs with
    /// ANSI mode (`AnsiTypeCoercion`) only in how a string is promoted and in FLOAT and integral.
    fn has_wider_type(
        &self,
        left: &FieldRef,
        right: &FieldRef,
        ansi: bool,
        by_name: bool,
    ) -> Option<bool> {
        use TypeFamily as F;

        let (l, r) = (F::of(left), F::of(right));
        if l == F::Other || r == F::Other {
            return None;
        }
        // Arrow compares two structs without the names of their fields, which Spark widens only
        // when they match, so a complex type is walked below instead.
        if l == F::Null
            || r == F::Null
            || (l.is_atomic() && left.data_type().equals_datatype(right.data_type()))
        {
            return Some(true);
        }
        let answer = match (l, r) {
            // A user-defined type widens only to itself, and so does a calendar interval.
            (F::Udt, F::Udt) => return same_udt(left, right).then_some(true),
            (F::CalendarInterval, F::CalendarInterval) => true,
            (F::Udt | F::CalendarInterval, _) | (_, F::Udt | F::CalendarInterval) => false,
            // A string or a binary value is one type to Spark however Arrow stores it.
            (F::String, F::String) | (F::Binary, F::Binary) => true,
            (a, b) if a.is_numeric() && b.is_numeric() => true,
            (a, b) if a.is_datetime() && b.is_datetime() => true,
            (F::YearMonthInterval, F::YearMonthInterval)
            | (F::DayTimeInterval, F::DayTimeInterval) => true,
            // A time only widens to itself, and two precisions are not modeled.
            (F::Time, F::Time) => return None,
            (F::String, other) | (other, F::String) => {
                if ansi {
                    // `AnsiStringPromotionTypeCoercion.findWiderTypeForString`
                    other.is_atomic() && !matches!(other, F::YearMonthInterval | F::DayTimeInterval)
                } else {
                    // `TypeCoercion.stringPromotion`
                    other.is_atomic() && !matches!(other, F::Binary | F::Boolean)
                }
            }
            (F::Array(a), F::Array(b)) => return self.has_wider_type(a, b, ansi, by_name),
            (F::Map(lk, lv), F::Map(rk, rv)) => {
                // A key that the cast to the wider type could turn NULL has no wider type
                // (`findTypeForComplex`).
                let key = if lk.data_type().equals_datatype(rk.data_type()) {
                    Some(true)
                } else {
                    match self.wider_type(lk, rk, ansi, by_name) {
                        Some(t) => Some(
                            !cast_force_nullable(lk.data_type(), &t)
                                && !cast_force_nullable(rk.data_type(), &t),
                        ),
                        None => match self.has_wider_type(lk, rk, ansi, by_name) {
                            Some(true) => None,
                            other => other,
                        },
                    }
                };
                return match key {
                    Some(true) => self.has_wider_type(lv, rv, ansi, by_name),
                    other => other,
                };
            }
            (F::Struct(a), F::Struct(b)) => match self.struct_field_pairs(a, b, by_name) {
                // Spark widens two structs only when their fields match by name.
                Some(pairs)
                    if pairs
                        .iter()
                        .all(|(x, y)| self.match_identifier(x.name(), y.name())) =>
                {
                    return every(
                        pairs
                            .into_iter()
                            .map(|(x, y)| self.has_wider_type(x, y, ansi, by_name)),
                    );
                }
                _ => false,
            },
            _ => false,
        };
        Some(answer)
    }

    /// The type two columns widen to (`findWiderTypeForTwo`), written as the Arrow type Sail uses
    /// for it, or `None` when there is none or the pair is not modeled. It follows the same
    /// families as `has_wider_type`.
    fn wider_type(
        &self,
        left: &FieldRef,
        right: &FieldRef,
        ansi: bool,
        by_name: bool,
    ) -> Option<DataType> {
        use TypeFamily as F;

        let (l, r) = (F::of(left), F::of(right));
        let (lt, rt) = (left.data_type(), right.data_type());
        if l == F::Other || r == F::Other {
            return None;
        }
        if r == F::Null {
            return Some(lt.clone());
        }
        if l == F::Null {
            return Some(rt.clone());
        }
        if l.is_atomic() && lt.equals_datatype(rt) {
            return Some(lt.clone());
        }
        // A nested field of the wider type can hold NULL where either side can, or where the
        // cast to it can make one, and carries no metadata (`findTypeForComplex`).
        let merge_field = |x: &FieldRef, y: &FieldRef| {
            self.wider_type(x, y, ansi, by_name).map(|t| {
                let nullable = x.is_nullable()
                    || y.is_nullable()
                    || cast_force_nullable(x.data_type(), &t)
                    || cast_force_nullable(y.data_type(), &t);
                Arc::new(Field::new(x.name(), t, nullable))
            })
        };
        match (l, r) {
            (F::Udt, F::Udt) if same_udt(left, right) => Some(lt.clone()),
            (F::CalendarInterval, F::CalendarInterval) => Some(lt.clone()),
            (F::Udt | F::CalendarInterval, _) | (_, F::Udt | F::CalendarInterval) => None,
            (F::String, F::String) | (F::Binary, F::Binary) => Some(lt.clone()),
            (a, b) if a.is_numeric() && b.is_numeric() => Self::wider_numeric_type(lt, rt, ansi),
            // A date, then a timestamp without a time zone, then one with.
            (a, b) if a.is_datetime() && b.is_datetime() => {
                let rank = |f: F| match f {
                    F::Date => 0,
                    F::TimestampNtz => 1,
                    _ => 2,
                };
                Some(if rank(b) > rank(a) { rt } else { lt }.clone())
            }
            (F::YearMonthInterval, F::YearMonthInterval)
            | (F::DayTimeInterval, F::DayTimeInterval) => Some(lt.clone()),
            (F::String, other) | (other, F::String) => {
                let (string, other_type) = if l == F::String { (lt, rt) } else { (rt, lt) };
                if !other.is_atomic() {
                    None
                } else if !ansi {
                    // The string wins, except against a binary or a boolean value.
                    (!matches!(other, F::Binary | F::Boolean)).then(|| string.clone())
                } else {
                    match other {
                        F::YearMonthInterval | F::DayTimeInterval => None,
                        F::Integral => Some(DataType::Int64),
                        F::Fractional | F::Decimal => Some(DataType::Float64),
                        // A string is cast to the plain binary type rather than to a view, which
                        // the client cannot read back.
                        F::Binary => Some(DataType::Binary),
                        _ => Some(other_type.clone()),
                    }
                }
            }
            (F::Array(a), F::Array(b)) => {
                let element = merge_field(a, b)?;
                Some(match lt {
                    DataType::LargeList(_) => DataType::LargeList(element),
                    _ => DataType::List(element),
                })
            }
            (F::Map(lk, lv), F::Map(rk, rv)) => {
                // Only a key the check models as widening without turning a value into NULL.
                if self.has_wider_type(left, right, ansi, by_name) != Some(true) {
                    return None;
                }
                let key = if lk.data_type().equals_datatype(rk.data_type()) {
                    lk.clone()
                } else {
                    merge_field(lk, rk)?
                };
                let value = merge_field(lv, rv)?;
                let DataType::Map(entries, sorted) = lt else {
                    return None;
                };
                let entries = entries
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::Struct(vec![key, value].into()));
                Some(DataType::Map(Arc::new(entries), *sorted))
            }
            (F::Struct(a), F::Struct(b)) => {
                let pairs = self.struct_field_pairs(a, b, by_name)?;
                if !pairs
                    .iter()
                    .all(|(x, y)| self.match_identifier(x.name(), y.name()))
                {
                    return None;
                }
                let fields = pairs
                    .into_iter()
                    .map(|(x, y)| merge_field(x, y))
                    .collect::<Option<Vec<_>>>()?;
                Some(DataType::Struct(fields.into()))
            }
            _ => None,
        }
    }

    /// The wider of two numeric types: the one that comes later in `numericPrecedence`, a
    /// decimal that holds both (`widerDecimalType`), or DOUBLE for a decimal and a floating
    /// type. With ANSI mode an integral type and FLOAT widen to DOUBLE.
    fn wider_numeric_type(left: &DataType, right: &DataType, ansi: bool) -> Option<DataType> {
        use DataType::*;

        let decimal = |t: &DataType| match t {
            Decimal32(p, s) | Decimal64(p, s) | Decimal128(p, s) | Decimal256(p, s) => {
                Some((i32::from(*p), i32::from(*s)))
            }
            Int8 | UInt8 => Some((3, 0)),
            Int16 | UInt16 => Some((5, 0)),
            Int32 | UInt32 => Some((10, 0)),
            Int64 | UInt64 => Some((20, 0)),
            _ => None,
        };
        let is_decimal = |t: &DataType| {
            matches!(
                t,
                Decimal32(..) | Decimal64(..) | Decimal128(..) | Decimal256(..)
            )
        };
        if is_decimal(left) || is_decimal(right) {
            if left.is_floating() || right.is_floating() {
                return Some(Float64);
            }
            let ((p1, s1), (p2, s2)) = (decimal(left)?, decimal(right)?);
            let scale = s1.max(s2);
            let precision = scale + (p1 - s1).max(p2 - s2);
            // Past the maximum precision the integral digits are kept and the fraction is cut
            // (`DecimalType.boundedPreferIntegralDigits`).
            let (precision, scale) = if precision <= 38 {
                (precision, scale)
            } else {
                (38, (scale - (precision - 38)).max(0))
            };
            return Some(Decimal128(
                u8::try_from(precision).ok()?,
                i8::try_from(scale).ok()?,
            ));
        }
        let rank = |t: &DataType| match t {
            Int8 | UInt8 => Some(0),
            Int16 | UInt16 => Some(1),
            Int32 | UInt32 => Some(2),
            Int64 | UInt64 => Some(3),
            Float16 | Float32 => Some(4),
            Float64 => Some(5),
            _ => None,
        };
        let (l, r) = (rank(left)?, rank(right)?);
        let wider = if r > l { right } else { left };
        if ansi && matches!(wider, Float16 | Float32) && l.min(r) < 4 {
            return Some(Float64);
        }
        Some(wider.clone())
    }

    /// Whether Spark widens the inputs of a set operation at all. It does only for an operator
    /// that is not `resolved`: one where a column differs in more than nullability, or for a union
    /// in more than the names of what it nests (`Union.allChildrenCompatible`).
    fn set_operation_widens(
        &self,
        left: &DFSchemaRef,
        right: &DFSchemaRef,
        is_union: bool,
        by_name: bool,
    ) -> bool {
        left.fields()
            .iter()
            .zip(right.fields().iter())
            .any(|(l, r)| {
                if is_union {
                    self.equals_structurally(l, r, by_name) == Some(false)
                } else {
                    !same_type_ignoring_nullability(l.data_type(), r.data_type())
                }
            })
    }

    /// Spark widens the inputs of a set operation to one type per column before it combines
    /// them (`WidenSetOperationTypes`), casting each side where the type is not its own, even
    /// where only the nullability of what it nests differs. A union whose inputs are the same but
    /// for the names of what they nest is not widened at all; the second input then takes the
    /// names of the first, which is what DataFusion needs.
    fn widen_set_operation_inputs(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
        is_union: bool,
        by_name: bool,
        nullable_casts: (bool, bool),
    ) -> PlanResult<(LogicalPlan, LogicalPlan)> {
        let ansi = self.config.ansi_mode;
        let widens = self.set_operation_widens(left.schema(), right.schema(), is_union, by_name);
        let targets = left
            .schema()
            .fields()
            .iter()
            .zip(right.schema().fields().iter())
            .map(|(l, r)| match self.wider_type(l, r, ansi, by_name) {
                // A cast renames the fields of a struct by position. `unionByName` has rebuilt
                // the structs it matches by name, but not those inside an array, which are
                // left to DataFusion, which matches them by name too.
                _ if by_name && !self.struct_names_aligned(l.data_type(), r.data_type()) => {
                    (None, None)
                }
                Some(t) => (Some(t.clone()), Some(t)),
                None if is_union
                    && self.equals_structurally(l, r, by_name) == Some(true)
                    && !same_type_ignoring_nullability(l.data_type(), r.data_type()) =>
                {
                    // With the nullability of both, since the output can hold a NULL wherever
                    // either input can.
                    (
                        None,
                        Some(self.union_like_merge(l.data_type(), r.data_type(), by_name)),
                    )
                }
                None => (None, None),
            })
            .collect::<Vec<_>>();
        let (left_targets, right_targets): (Vec<_>, Vec<_>) = targets.into_iter().unzip();
        let left = self.cast_set_operation_input(left, left_targets, nullable_casts.0, widens)?;
        let right =
            self.cast_set_operation_input(right, right_targets, nullable_casts.1, widens)?;
        Ok((left, right))
    }

    /// Casts the columns of one input of a set operation to the types given, where they differ
    /// in more than nullability, or in nullability too once the operation `widens`, and keeps
    /// their names.
    ///
    /// A cast that can turn a value into NULL is nullable (`Cast.nullable`). That is declared
    /// only where the output reads it: both inputs of a union, and the first input of a
    /// difference. An intersection takes the nullability both inputs share, which Sail does
    /// not narrow to (see the TODO there), so declaring it would only widen it further.
    fn cast_set_operation_input(
        &self,
        plan: LogicalPlan,
        targets: Vec<Option<DataType>>,
        declare_nullable_casts: bool,
        widens: bool,
    ) -> PlanResult<LogicalPlan> {
        let schema = plan.schema().clone();
        let needs_cast = |field: &FieldRef, target: &Option<DataType>| {
            target.as_ref().is_some_and(|t| {
                !same_type_ignoring_nullability(field.data_type(), t)
                    || (widens && !field.data_type().equals_datatype(t))
            })
        };
        if !schema
            .fields()
            .iter()
            .zip(&targets)
            .any(|(field, target)| needs_cast(field, target))
        {
            return Ok(plan);
        }
        let expr = schema
            .columns()
            .into_iter()
            .zip(schema.fields().iter())
            .zip(targets)
            .map(|((column, field), target)| match target {
                Some(t) if needs_cast(field, &Some(t.clone())) => {
                    let force_nullable =
                        declare_nullable_casts && cast_force_nullable(field.data_type(), &t);
                    let casted = self.cast_to_spark_type(
                        Expr::Column(column),
                        field,
                        t.clone(),
                        false,
                        None,
                    )?;
                    // The `CASE` only declares the column nullable, and never changes a value.
                    let casted = if force_nullable && !casted.nullable(&schema)? {
                        when(lit(true), casted).otherwise(lit(ScalarValue::try_from(&t)?))?
                    } else {
                        casted
                    };
                    Ok(casted.alias(field.name()))
                }
                _ => Ok(Expr::Column(column)),
            })
            .collect::<PlanResult<Vec<_>>>()?;
        Ok(project(plan, expr)?)
    }

    /// The fields of two structs, paired the way Spark compares them: by position, or by name for
    /// `unionByName`, which reorders the fields of a nested struct to match the first input
    /// (`ResolveUnion.mergeFields`). `None` when the two have a different number of fields.
    fn struct_field_pairs<'a>(
        &self,
        left: &'a Fields,
        right: &'a Fields,
        by_name: bool,
    ) -> Option<Vec<(&'a FieldRef, &'a FieldRef)>> {
        if left.len() != right.len() {
            return None;
        }
        let by_position = || left.iter().zip(right.iter()).collect::<Vec<_>>();
        if !by_name {
            return Some(by_position());
        }
        let by_name = left
            .iter()
            .map(|x| {
                right
                    .iter()
                    .find(|y| self.match_identifier(x.name(), y.name()))
                    .map(|y| (x, y))
            })
            .collect::<Option<Vec<_>>>();
        Some(by_name.unwrap_or_else(by_position))
    }

    /// Whether two types are the same once the names of what they nest and their nullability are
    /// ignored (`DataType.equalsStructurally`).
    fn equals_structurally(
        &self,
        left: &FieldRef,
        right: &FieldRef,
        by_name: bool,
    ) -> Option<bool> {
        use TypeFamily as F;

        match (F::of(left), F::of(right)) {
            (F::Other, _) | (_, F::Other) => None,
            (F::Udt, F::Udt) => Some(same_udt(left, right)),
            (F::Udt, _) | (_, F::Udt) => Some(false),
            (F::Array(a), F::Array(b)) => self.equals_structurally(a, b, by_name),
            (F::Map(ak, av), F::Map(bk, bv)) => every([
                self.equals_structurally(ak, bk, by_name),
                self.equals_structurally(av, bv, by_name),
            ]),
            (F::Struct(a), F::Struct(b)) => match self.struct_field_pairs(a, b, by_name) {
                Some(pairs) => every(
                    pairs
                        .into_iter()
                        .map(|(x, y)| self.equals_structurally(x, y, by_name)),
                ),
                None => Some(false),
            },
            // A family that is a single Spark type is one type however Arrow stores it: the unit
            // of a timestamp and its time zone, for example, are not part of the Spark type.
            (
                a @ (F::String
                | F::Binary
                | F::Boolean
                | F::Date
                | F::TimestampNtz
                | F::Timestamp
                | F::YearMonthInterval
                | F::DayTimeInterval
                | F::CalendarInterval
                | F::Variant),
                b,
            ) => Some(a == b),
            _ => Some(left.data_type().equals_datatype(right.data_type())),
        }
    }

    /// Whether a column of the second input of a set operation is compatible with the one of the
    /// first, after both are widened (`CheckAnalysis`, `getDataTypesAreCompatibleFn`). A union
    /// accepts two types that are the same but for names, and every other operation checks the
    /// widening without ANSI mode.
    fn is_compatible(
        &self,
        left: &FieldRef,
        right: &FieldRef,
        is_union: bool,
        by_name: bool,
        ansi: bool,
    ) -> Option<bool> {
        either(self.has_wider_type(left, right, ansi, by_name), || {
            if is_union {
                self.equals_structurally(left, right, by_name)
            } else {
                self.has_wider_type(left, right, false, by_name)
            }
        })
    }

    /// Spark refuses a set operation where a column of the second input has no type in common
    /// with the one of the first (`CheckAnalysis`, `INCOMPATIBLE_COLUMN_TYPE`).
    fn check_set_operation_types(
        &self,
        operator: &str,
        is_union: bool,
        by_name: bool,
        left: &DFSchemaRef,
        right: &DFSchemaRef,
    ) -> PlanResult<()> {
        // The number of columns is checked first (`numColumnsMismatch`).
        let (left_len, right_len) = (left.fields().len(), right.fields().len());
        if left_len != right_len {
            return Err(num_columns_mismatch_error(operator, left_len, right_len));
        }
        let ansi = self.config.ansi_mode;
        let pairs = left.fields().iter().zip(right.fields().iter());
        let Some((ordinal, (left_field, right_field))) = pairs
            .clone()
            .enumerate()
            .find(|(_, (l, r))| self.is_compatible(l, r, is_union, by_name, ansi) == Some(false))
        else {
            return Ok(());
        };
        // With ANSI mode Spark suggests turning it off when that alone would accept every column
        // (`getHintForOperatorCoercion`). It judges the plan ANSI mode has already widened, so a
        // column ANSI mode widens counts as accepted.
        let hint = if ansi
            && pairs.into_iter().all(|(l, r)| {
                self.has_wider_type(l, r, true, by_name) == Some(true)
                    || self.is_compatible(l, r, is_union, by_name, false) != Some(false)
            }) {
            "\nTo fix the error, you might need to add explicit type casts. If necessary set \
             spark.sql.ansi.enabled to false to bypass this error."
        } else {
            ""
        };
        Err(PlanError::AnalysisError(format!(
            "[INCOMPATIBLE_COLUMN_TYPE] {operator} can only be performed on tables with \
             compatible column types. The {} column of the second table is {} type which is not \
             compatible with {} at the same column of the first table.{hint}.",
            ordinal_number(ordinal),
            self.quoted_spark_type_name(right_field)?,
            self.quoted_spark_type_name(left_field)?,
        )))
    }

    /// A type as `toSQLType` quotes it in a message: a user-defined type is written as
    /// `UDT("<its storage type>")`.
    fn quoted_spark_type_name(&self, field: &FieldRef) -> PlanResult<String> {
        let name = self.spark_type_name(field.data_type())?;
        Ok(if TypeFamily::of(field) == TypeFamily::Udt {
            format!("UDT(\"{name}\")")
        } else {
            format!("\"{name}\"")
        })
    }

    /// Whether Spark casts the left input of a set operation: it widens the two inputs to one type
    /// first (`WidenSetOperationTypes`), and the left one is cast where that type is not its own.
    /// Where there is no wider type nothing is cast, and a container is cast where something it
    /// holds is. The nullability of what a container holds does not count: Spark keeps the
    /// metadata of an array whose elements only the right input can hold NULL in.
    fn left_is_cast(&self, left: &FieldRef, right: &FieldRef, ansi: bool, by_name: bool) -> bool {
        use TypeFamily as F;

        if self.has_wider_type(left, right, ansi, by_name) != Some(true) {
            return false;
        }
        match (F::of(left), F::of(right)) {
            (F::Array(a), F::Array(b)) => self.left_is_cast(a, b, ansi, by_name),
            (F::Map(lk, lv), F::Map(rk, rv)) => {
                self.left_is_cast(lk, rk, ansi, by_name) || self.left_is_cast(lv, rv, ansi, by_name)
            }
            (F::Struct(a), F::Struct(b)) => self
                .struct_field_pairs(a, b, by_name)
                .into_iter()
                .flatten()
                .any(|(x, y)| self.left_is_cast(x, y, ansi, by_name)),
            _ => Self::atomic_left_is_cast(left.data_type(), right.data_type(), ansi),
        }
    }

    /// The same for two atomic types that have a wider type. The widening is Spark's
    /// (`findWiderTypeForTwo`), taken family by family, since DataFusion's disagrees with it on
    /// decimals, timestamps and binary.
    fn atomic_left_is_cast(left: &DataType, right: &DataType, ansi: bool) -> bool {
        use DataType::*;

        let is_string = |t: &DataType| matches!(t, Utf8 | LargeUtf8 | Utf8View);
        let is_binary =
            |t: &DataType| matches!(t, Binary | LargeBinary | BinaryView | FixedSizeBinary(_));
        // The digits a decimal needs to hold every value of an integral type.
        let integral_digits = |t: &DataType| match t {
            Int8 | UInt8 => Some(3),
            Int16 | UInt16 => Some(5),
            Int32 | UInt32 => Some(10),
            Int64 | UInt64 => Some(20),
            _ => None,
        };
        // The precedence of the numeric types other than decimal (`numericPrecedence`).
        let rank = |t: &DataType| match t {
            Int8 | UInt8 => Some(0),
            Int16 | UInt16 => Some(1),
            Int32 | UInt32 => Some(2),
            Int64 | UInt64 => Some(3),
            Float16 | Float32 => Some(4),
            Float64 => Some(5),
            _ => None,
        };
        let decimal = |t: &DataType| match t {
            Decimal32(p, s) | Decimal64(p, s) | Decimal128(p, s) | Decimal256(p, s) => {
                Some((i32::from(*p), i32::from(*s)))
            }
            _ => None,
        };
        // A date, then a timestamp without a time zone, then one with (`findWiderDateTimeType`).
        let datetime_rank = |t: &DataType| match t {
            Date32 | Date64 => Some(0),
            Timestamp(_, None) => Some(1),
            Timestamp(_, Some(_)) => Some(2),
            _ => None,
        };

        // Two strings or two binary values are one type to Spark however Arrow stores them. NULL
        // widens to the other side.
        if left.equals_datatype(right)
            || (is_string(left) && is_string(right))
            || (is_binary(left) && is_binary(right))
            || right.is_null()
        {
            return false;
        }
        if left.is_null() {
            return true;
        }
        // With ANSI mode a string is cast to the other side, an integral type becoming BIGINT and a
        // fractional one DOUBLE (`AnsiStringPromotionTypeCoercion.findWiderTypeForString`);
        // without it the string wins (`TypeCoercion.stringPromotion`).
        if is_string(left) {
            return ansi;
        }
        if is_string(right) {
            return if !ansi {
                true
            } else if integral_digits(left).is_some() {
                left != &Int64
            } else if left.is_floating() || decimal(left).is_some() {
                left != &Float64
            } else {
                false
            };
        }
        if let (Some(l), Some(r)) = (rank(left), rank(right)) {
            // With ANSI mode an integral type and FLOAT widen to DOUBLE, so both are cast.
            let float_and_integral = |a: &DataType, b: &DataType| {
                matches!(a, Float16 | Float32) && integral_digits(b).is_some()
            };
            if ansi && (float_and_integral(left, right) || float_and_integral(right, left)) {
                return true;
            }
            return l < r;
        }
        if let Some((p, s)) = decimal(left) {
            if decimal(right).is_some() {
                return Self::wider_numeric_type(left, right, ansi).and_then(|t| decimal(&t))
                    != Some((p, s));
            }
            if let Some(digits) = integral_digits(right) {
                // A decimal that holds every value of the integral type is the wider one.
                return p - s < digits;
            }
            // A decimal and a floating type widen to DOUBLE.
            return right.is_floating();
        }
        if decimal(right).is_some() {
            // An integral type is cast to a decimal, and a floating type widens to DOUBLE.
            return left != &Float64;
        }
        if let (Some(l), Some(r)) = (datetime_rank(left), datetime_rank(right)) {
            return l < r;
        }
        // Two intervals of one kind widen to the one that spans both, which Arrow does not tell
        // apart from either of them.
        false
    }

    /// Spark reports the metadata of the first input for a set operation, after it widens the
    /// inputs. DataFusion merges the metadata of every input instead: it skips an input that has
    /// none, and drops a key two inputs give different values (`intersect_metadata_for_union`). So
    /// the metadata Spark reports is written on every column where the merge disagrees with it.
    fn keep_left_metadata(
        &self,
        plan: LogicalPlan,
        left_schema: &DFSchemaRef,
        right_schema: &DFSchemaRef,
        is_union: bool,
        by_name: bool,
    ) -> PlanResult<LogicalPlan> {
        let ansi = self.config.ansi_mode;
        let widens = self.set_operation_widens(left_schema, right_schema, is_union, by_name);
        // Spark widens the children first, casting a column narrower than the result as
        // `Alias(Cast(...))` (`WidenSetOperationTypes.widenTypes`), and a cast carries no metadata
        // (`Alias.metadata`). So a column reports the metadata of its left input where that input
        // was not cast, and none where it was. Whether it was cast is read off the type the two
        // inputs widen to rather than off the output, since the type Sail reports for a set
        // operation is the one of its first input.
        let reported = |left_field: &FieldRef, right_field: &FieldRef| {
            let cast = self.left_is_cast(left_field, right_field, ansi, by_name)
                || (widens
                    && self
                        .wider_type(left_field, right_field, ansi, by_name)
                        .is_some_and(|t| {
                            same_type_ignoring_nullability(left_field.data_type(), &t)
                                && !left_field.data_type().equals_datatype(&t)
                        }));
            let mut metadata = if cast {
                HashMap::new()
            } else {
                left_field.metadata().clone()
            };
            // The key is written even when the left input has none, so that the metadata of the
            // right input does not survive the merge in its place.
            metadata
                .entry(spec::SPARK_METADATA_JSON_KEY.to_string())
                .or_insert_with(|| "{}".to_string());
            metadata
        };
        // A missing key reads as the empty metadata Spark reports for no metadata at all.
        let agrees = |field: &FieldRef, reported: &HashMap<String, String>| {
            reported.iter().all(|(key, value)| {
                let current = field.metadata().get(key).map(|x| x.as_str());
                if key == spec::SPARK_METADATA_JSON_KEY {
                    current.unwrap_or("{}") == value
                } else {
                    current == Some(value.as_str())
                }
            })
        };
        let output = plan.schema().clone();
        let reported = output
            .fields()
            .iter()
            .zip(left_schema.fields().iter())
            .zip(right_schema.fields().iter())
            .map(|((field, left_field), right_field)| {
                let reported = reported(left_field, right_field);
                (!agrees(field, &reported)).then_some(reported)
            })
            .collect::<Vec<_>>();
        if reported.iter().all(Option::is_none) {
            return Ok(plan);
        }
        let expr = output
            .columns()
            .into_iter()
            .zip(reported)
            .map(|(column, reported)| match reported {
                None => Expr::Column(column),
                Some(metadata) => {
                    let name = column.name.clone();
                    Expr::Column(column)
                        .alias_with_metadata(name, Some(FieldMetadata::from(metadata)))
                }
            })
            .collect::<Vec<_>>();
        Ok(LogicalPlan::Projection(Projection::try_new(
            expr,
            Arc::new(plan),
        )?))
    }

    /// A union can hold a NULL wherever either input can, at every level a column nests
    /// (`Union.mergeChildOutputs`, `StructType.unionLikeMerge`). DataFusion does so only at the top
    /// level and keeps what the first input nests, so the first input is cast to its own type
    /// with the nullability of both where they differ.
    fn widen_union_nullability(
        &self,
        left: LogicalPlan,
        right: &DFSchemaRef,
        by_name: bool,
    ) -> PlanResult<LogicalPlan> {
        let merged = left
            .schema()
            .fields()
            .iter()
            .zip(right.fields().iter())
            .map(|(l, r)| self.union_like_merge(l.data_type(), r.data_type(), by_name))
            .collect::<Vec<_>>();
        if left
            .schema()
            .fields()
            .iter()
            .zip(&merged)
            .all(|(field, merged)| field.data_type() == merged)
        {
            return Ok(left);
        }
        let expr = left
            .schema()
            .columns()
            .into_iter()
            .zip(left.schema().fields().iter())
            .zip(merged)
            .map(|((column, field), merged)| {
                if field.data_type() == &merged {
                    Expr::Column(column)
                } else {
                    cast(Expr::Column(column), merged).alias(field.name())
                }
            })
            .collect::<Vec<_>>();
        Ok(project(left, expr)?)
    }

    /// The type of the first input with the nullability of both at every level they share.
    fn union_like_merge(&self, left: &DataType, right: &DataType, by_name: bool) -> DataType {
        let merge_field = |l: &FieldRef, r: &FieldRef| {
            Arc::new(
                l.as_ref()
                    .clone()
                    .with_data_type(self.union_like_merge(l.data_type(), r.data_type(), by_name))
                    .with_nullable(l.is_nullable() || r.is_nullable()),
            )
        };
        match (left, right) {
            (DataType::List(l), DataType::List(r)) => DataType::List(merge_field(l, r)),
            (DataType::LargeList(l), DataType::LargeList(r)) => {
                DataType::LargeList(merge_field(l, r))
            }
            (DataType::FixedSizeList(l, n), DataType::FixedSizeList(r, _)) => {
                DataType::FixedSizeList(merge_field(l, r), *n)
            }
            (DataType::Map(l, sorted), DataType::Map(r, _)) => {
                match (l.data_type(), r.data_type()) {
                    (DataType::Struct(lf), DataType::Struct(rf))
                        if lf.len() == 2 && rf.len() == 2 =>
                    {
                        let entries = DataType::Struct(
                            vec![merge_field(&lf[0], &rf[0]), merge_field(&lf[1], &rf[1])].into(),
                        );
                        DataType::Map(
                            Arc::new(l.as_ref().clone().with_data_type(entries)),
                            *sorted,
                        )
                    }
                    _ => left.clone(),
                }
            }
            (DataType::Struct(l), DataType::Struct(r)) if !is_marked_variant_storage_type(left) => {
                match self.struct_field_pairs(l, r, by_name) {
                    Some(pairs) => DataType::Struct(
                        pairs
                            .into_iter()
                            .map(|(x, y)| merge_field(x, y))
                            .collect::<Vec<_>>()
                            .into(),
                    ),
                    None => left.clone(),
                }
            }
            _ => left.clone(),
        }
    }

    /// Rebuilds a struct with the fields of `target` in their order, reading each one by name,
    /// and appends the fields only `target` lacks (`ResolveUnion.addFields`). A field `target`
    /// has and the struct lacks is NULL, which is only reached with `allowMissingColumns`, since
    /// the names were checked before. `None` when the fields already line up by position.
    fn rebuild_struct_by_name(
        &self,
        expr: Expr,
        field: &Field,
        target: &DataType,
        allow_missing: bool,
    ) -> PlanResult<Option<(Expr, DataType)>> {
        let (DataType::Struct(from), DataType::Struct(to)) = (field.data_type(), target) else {
            return Ok(None);
        };
        if self.struct_names_aligned(field.data_type(), target) {
            return Ok(None);
        }
        // A field read out of a struct that can be NULL can be NULL too (`GetStructField`).
        let read = |from_field: &FieldRef| {
            from_field
                .as_ref()
                .clone()
                .with_nullable(from_field.is_nullable() || field.is_nullable())
        };
        let mut values = Vec::new();
        let mut fields = Vec::new();
        for target_field in to.iter() {
            match from
                .iter()
                .find(|x| self.match_identifier(x.name(), target_field.name()))
            {
                Some(from_field) => {
                    let from_field = read(from_field);
                    let value = get_field(expr.clone(), from_field.name().to_string());
                    let (value, data_type) = match self.rebuild_struct_by_name(
                        value.clone(),
                        &from_field,
                        target_field.data_type(),
                        allow_missing,
                    )? {
                        Some(rebuilt) => rebuilt,
                        None => (value, from_field.data_type().clone()),
                    };
                    values.push(value);
                    fields.push(Field::new(
                        target_field.name(),
                        data_type,
                        from_field.is_nullable(),
                    ));
                }
                None if allow_missing => {
                    values.push(cast(
                        lit(ScalarValue::Null),
                        target_field.data_type().clone(),
                    ));
                    fields.push(Field::new(
                        target_field.name(),
                        target_field.data_type().clone(),
                        true,
                    ));
                }
                None => {
                    return Err(PlanError::internal(format!(
                        "missing struct field {} for unionByName",
                        target_field.name()
                    )));
                }
            }
        }
        for from_field in from.iter() {
            if !to
                .iter()
                .any(|x| self.match_identifier(x.name(), from_field.name()))
            {
                values.push(get_field(expr.clone(), from_field.name().to_string()));
                fields.push(read(from_field));
            }
        }
        // Built like `struct`, so that each field keeps the nullability of what it reads.
        let names = fields.iter().map(|x| x.name().clone()).collect::<Vec<_>>();
        let data_type = DataType::Struct(fields.into());
        let rebuilt = ScalarUDF::from(StructFunction::new(names)).call(values);
        let rebuilt = if field.is_nullable() {
            when(expr.is_null(), lit(ScalarValue::try_from(&data_type)?)).otherwise(rebuilt)?
        } else {
            rebuilt
        };
        Ok(Some((rebuilt, data_type)))
    }

    /// Whether the fields of two types line up by position with the same names, in structs and
    /// in arrays of them. A struct inside a map is compared by position anyway
    /// (`ResolveUnion.mergeFields` does not reach into maps), so a map always lines up.
    ///
    /// TODO: `unionByName` rebuilds a struct inside an array by name too, with an
    ///   `ArrayTransform` (`ResolveUnion.transformArray`), which also fills a missing nested field
    ///   with NULL under `allowMissingColumns`. Such a struct is left to DataFusion here, which
    ///   matches the names but cannot fill a missing field, because the plan has no way yet to build
    ///   a lambda over a resolved expression.
    fn struct_names_aligned(&self, left: &DataType, right: &DataType) -> bool {
        match (left, right) {
            (DataType::Struct(l), DataType::Struct(r)) => {
                l.len() == r.len()
                    && l.iter().zip(r.iter()).all(|(x, y)| {
                        self.match_identifier(x.name(), y.name())
                            && self.struct_names_aligned(x.data_type(), y.data_type())
                    })
            }
            (
                DataType::List(l) | DataType::LargeList(l) | DataType::FixedSizeList(l, _),
                DataType::List(r) | DataType::LargeList(r) | DataType::FixedSizeList(r, _),
            ) => self.struct_names_aligned(l.data_type(), r.data_type()),
            _ => true,
        }
    }

    /// `unionByName` refuses an input that names two columns the same (`ResolveUnion`,
    /// `checkColumnNames`), comparing the names without case unless the analysis is case
    /// sensitive, and reports the name as compared.
    fn reject_duplicate_column_names(&self, names: &[String]) -> PlanResult<()> {
        let mut normalized = names
            .iter()
            .map(|x| {
                if self.config.case_sensitive {
                    x.clone()
                } else {
                    to_lowercase(x)
                }
            })
            .collect::<Vec<_>>();
        // Spark reports the first duplicate in order of the names (`checkColumnNameDuplication`).
        normalized.sort();
        match normalized.windows(2).find(|pair| pair[0] == pair[1]) {
            Some(pair) => Err(PlanError::AnalysisError(format!(
                "[COLUMN_ALREADY_EXISTS] The column {} already exists. Choose another name or \
                 rename the existing column.",
                quote_identifier_name(&pair[0])
            ))),
            None => Ok(()),
        }
    }

    /// Without `allowMissingColumns`, `unionByName` matches the fields of a nested struct by name,
    /// in structs and in arrays of them but not in maps, and refuses a field of the first input
    /// that the second one lacks (`ResolveUnion.addFields`).
    fn check_nested_fields_by_name(&self, left: &DataType, right: &DataType) -> PlanResult<()> {
        match (left, right) {
            (DataType::Struct(left_fields), DataType::Struct(right_fields)) => {
                for left_field in left_fields.iter() {
                    match right_fields
                        .iter()
                        .find(|x| self.match_identifier(x.name(), left_field.name()))
                    {
                        Some(right_field) => self.check_nested_fields_by_name(
                            left_field.data_type(),
                            right_field.data_type(),
                        )?,
                        None => {
                            return Err(PlanError::AnalysisError(format!(
                                "[FIELD_NOT_FOUND] No such struct field {} in {}.",
                                quote_identifier_name(left_field.name()),
                                right_fields
                                    .iter()
                                    .map(|x| quote_identifier_name(x.name()))
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            )));
                        }
                    }
                }
                Ok(())
            }
            (
                DataType::List(l) | DataType::LargeList(l) | DataType::FixedSizeList(l, _),
                DataType::List(r) | DataType::LargeList(r) | DataType::FixedSizeList(r, _),
            ) => self.check_nested_fields_by_name(l.data_type(), r.data_type()),
            _ => Ok(()),
        }
    }

    /// An operation that compares whole rows rejects a column that holds a map, since a map has
    /// no order of its own. Spark looks for it in the output, after the types are checked and
    /// widened (`CheckAnalysis`, `mapColumnInSetOperation`), so a NULL column of the first input
    /// holds a map when the second input has one there, and keeps its own name.
    fn reject_map_column_in_set_output(
        &self,
        left: &DFSchemaRef,
        right: &DFSchemaRef,
        state: &PlanResolverState,
    ) -> PlanResult<()> {
        for (left_field, right_field) in left.fields().iter().zip(right.fields().iter()) {
            if left_field.data_type().is_null() {
                let widened = Arc::new(right_field.as_ref().clone().with_name(left_field.name()));
                self.reject_map_column_in_set_operation_for_field(&widened, state)?;
            } else {
                self.reject_map_column_in_set_operation_for_field(left_field, state)?;
            }
        }
        // A variant has no order either, and is looked for after every map
        // (`variantColumnInSetOperation`).
        for field in left.fields().iter() {
            if contains_variant(field.data_type()) {
                let name = state.get_field_info(field.name())?.name().to_string();
                return Err(PlanError::AnalysisError(format!(
                    "[UNSUPPORTED_FEATURE.SET_OPERATION_ON_VARIANT_TYPE] The feature is not \
                     supported: Cannot have VARIANT type columns in DataFrame which calls set \
                     operations (INTERSECT, EXCEPT, etc.), but the type of column {} is \"{}\".",
                    quote_identifier_name(&name),
                    self.spark_type_name(field.data_type())?
                )));
            }
        }
        Ok(())
    }
}

/// Whether a type is a variant or holds one at any depth.
fn contains_variant(data_type: &DataType) -> bool {
    match data_type {
        DataType::Struct(_) if is_marked_variant_storage_type(data_type) => true,
        DataType::Struct(fields) => fields.iter().any(|x| contains_variant(x.data_type())),
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::FixedSizeList(f, _)
        | DataType::ListView(f)
        | DataType::LargeListView(f)
        | DataType::Map(f, _) => contains_variant(f.data_type()),
        _ => false,
    }
}
