//! Spark's casts between TIMESTAMP_NTZ and TIMESTAMP, which use the session time zone.

use std::cell::OnceCell;
use std::sync::Arc;

use chrono::{DateTime, Offset, TimeZone, Utc};
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::datatypes::{DataType, FieldRef, TimeUnit};
use datafusion::common::config::ConfigOptions;
use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchema, Result, ScalarValue};
use datafusion_expr::expr::{Cast, InList};
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::utils::merge_schema;
use datafusion_expr::{
    BinaryExpr, Expr, ExprSchemable, LogicalPlan, Operator, ScalarUDF, Volatility, cast, lit,
};
use sail_common::utils::datetime::is_fixed_zero_offset_timezone;
use sail_function::scalar::datetime::convert_tz::{ConvertTz, has_timestamp_ntz};

/// Converts the casts of TIMESTAMP_NTZ to TIMESTAMP, which type coercion adds, like Spark.
///
/// A comparison with a constant uses the constant's wall-clock time in the time zone of the cast
/// when that time casts back to the constant, like Spark's `UnwrapCastInBinaryComparison`, and
/// stays a plain column comparison that Parquet can prune with. DataFusion would instead unwrap the
/// cast by reading the constant's UTC epoch as a wall-clock time.
///
/// Other casts to the session time zone convert like Spark, which moves a wall-clock time in a DST
/// gap forward and resolves one in an overlap to the earlier offset. Arrow's cast fails for them,
/// and the optimizer would unwrap it by reading the constant's UTC epoch as a wall-clock time,
/// whereas the optimizer unwraps the conversion like Spark when a comparison forms later.
pub(crate) fn convert_session_timestamp_casts(
    plan: LogicalPlan,
    config: &ConfigOptions,
) -> Result<LogicalPlan> {
    let Some(timezone) = config.execution.time_zone.as_deref() else {
        return Ok(plan);
    };
    let rewriter = Rewriter {
        timezone,
        converts: !is_fixed_zero_offset_timezone(timezone) && !timezone.starts_with(['+', '-']),
        config,
        simplifier: OnceCell::new(),
    };
    plan.transform_up_with_subqueries(|plan| {
        if !has_timestamp_cast(&plan)? {
            return Ok(Transformed::no(plan));
        }
        let schema = merge_schema(&plan.inputs());
        let name_preserver = NamePreserver::new(&plan);
        plan.map_expressions(|expr| {
            let name = name_preserver.save(&expr);
            expr.transform_down(|expr| rewriter.rewrite(expr, &schema))
                .map(|expr| expr.update_data(|expr| name.restore(expr)))
        })
    })
    .data()
}

/// Spark's cast of TIMESTAMP to TIMESTAMP_NTZ `data_type`, which takes the wall-clock time of the
/// value in the session time zone. Arrow's cast takes the wall-clock time in UTC.
pub(crate) fn session_wall_clock(expr: Expr, data_type: &DataType, timezone: &str) -> Expr {
    let timestamp_ntz = DataType::Timestamp(TimeUnit::Microsecond, None);
    let utc = cast(expr, timestamp_ntz.clone());
    let wall = if is_fixed_zero_offset_timezone(timezone) {
        utc
    } else {
        ScalarUDF::from(ConvertTz::cast(false)).call(vec![lit("UTC"), lit(timezone), utc])
    };
    if *data_type == timestamp_ntz {
        wall
    } else {
        cast(wall, data_type.clone())
    }
}

struct Rewriter<'a> {
    timezone: &'a str,
    /// Whether casts to the session time zone convert, which they need for DST gaps and overlaps.
    /// Arrow's cast is exact for a fixed offset.
    converts: bool,
    config: &'a ConfigOptions,
    /// Built on first use, since most plans have no constant to evaluate.
    simplifier: OnceCell<ExprSimplifier>,
}

impl Rewriter<'_> {
    fn rewrite(&self, expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) if is_comparison(op) => {
                if let Some(expr) = self.unwrap_date_cast(&left, op, &right, schema) {
                    Ok(Transformed::yes(expr))
                } else if let Some(expr) = op
                    .swap()
                    .and_then(|op| self.unwrap_date_cast(&right, op, &left, schema))
                {
                    Ok(Transformed::yes(expr))
                } else if let Some((value, wall)) = self.unwrap_pair(&left, &right, schema) {
                    Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
                        Box::new(value),
                        op,
                        Box::new(wall),
                    ))))
                } else if let Some((value, wall)) = self.unwrap_pair(&right, &left, schema) {
                    Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
                        Box::new(wall),
                        op,
                        Box::new(value),
                    ))))
                } else {
                    Ok(Transformed::no(Expr::BinaryExpr(BinaryExpr {
                        left,
                        op,
                        right,
                    })))
                }
            }
            // Spark rewrites a one-value IN to an equality before unwrapping the cast.
            // Longer lists of constants unwrap the same way.
            Expr::InList(InList {
                expr,
                list,
                negated,
            }) => match list
                .iter()
                .map(|value| self.unwrap_pair(&expr, value, schema))
                .collect::<Option<Vec<_>>>()
            {
                Some(pairs) if !pairs.is_empty() => {
                    let value = pairs[0].0.clone();
                    let walls = pairs.into_iter().map(|(_, wall)| wall).collect();
                    Ok(Transformed::yes(Expr::InList(InList::new(
                        Box::new(value),
                        walls,
                        negated,
                    ))))
                }
                _ => Ok(Transformed::no(Expr::InList(InList {
                    expr,
                    list,
                    negated,
                }))),
            },
            Expr::Cast(Cast { expr: value, field }) => {
                if self.converts
                    && let DataType::Timestamp(unit, Some(label)) = field.data_type()
                    && label.as_ref() == self.timezone
                    && matches!(value.get_type(schema)?, DataType::Timestamp(_, None))
                {
                    let instant = self.session_timestamp(
                        cast(*value, DataType::Timestamp(TimeUnit::Microsecond, None)),
                        label,
                    );
                    Ok(Transformed::yes(if *unit == TimeUnit::Microsecond {
                        instant
                    } else {
                        cast(instant, field.data_type().clone())
                    }))
                } else if self.converts
                    && let Some(types) = nested_session_cast_types(
                        &value.get_type(schema)?,
                        field.data_type(),
                        self.timezone,
                    )
                    && types.converted
                {
                    // The TIMESTAMP_NTZ values convert in a nested value too, and the other
                    // values cast to their target type first.
                    let instant = self.session_timestamp(
                        cast(*value, types.timestamp_ntz),
                        &Arc::from(self.timezone),
                    );
                    Ok(Transformed::yes(if types.timestamp_is_target {
                        instant
                    } else {
                        cast(instant, field.data_type().clone())
                    }))
                } else if *field.data_type() == DataType::Date32
                    && let DataType::Timestamp(unit, Some(label)) = value.get_type(schema)?
                    && label.as_ref() != self.timezone
                {
                    // Spark takes the date in the session time zone, whatever zone labels the
                    // values. Arrow takes it in the zone of the label, which keeps the instants.
                    Ok(Transformed::yes(cast(
                        cast(
                            *value,
                            DataType::Timestamp(unit, Some(Arc::from(self.timezone))),
                        ),
                        DataType::Date32,
                    )))
                } else {
                    Ok(Transformed::no(Expr::Cast(Cast { expr: value, field })))
                }
            }
            expr => Ok(Transformed::no(expr)),
        }
    }

    /// The TIMESTAMP_NTZ values, which are microseconds, as TIMESTAMP in the session time zone.
    fn session_timestamp(&self, value: Expr, label: &Arc<str>) -> Expr {
        ScalarUDF::from(ConvertTz::cast(false).with_output_timezone(Some(Arc::clone(label))))
            .call(vec![lit(self.timezone), lit("UTC"), value])
    }

    /// Returns the TIMESTAMP_NTZ value under `cast_expr` and the wall-clock time of `constant`
    /// in the time zone of the cast, when that time casts back to the constant.
    fn unwrap_pair(
        &self,
        cast_expr: &Expr,
        constant: &Expr,
        schema: &DFSchema,
    ) -> Option<(Expr, Expr)> {
        let Expr::Cast(Cast { expr: value, field }) = cast_expr else {
            return None;
        };
        let DataType::Timestamp(unit, Some(timezone)) = field.data_type() else {
            return None;
        };
        // A constant value is converted like any other cast, and the optimizer folds the
        // comparison, like Spark's constant folding before it unwraps casts.
        if is_foldable_constant(value) {
            return None;
        }
        let DataType::Timestamp(value_unit, None) = value.get_type(schema).ok()? else {
            return None;
        };
        let instant = self.timestamp_constant(constant, *unit)?;
        let tz = timezone.parse::<Tz>().ok()?;
        let utc = instant.naive_utc();
        let wall = utc.checked_add_offset(tz.offset_from_utc_datetime(&utc).fix())?;
        if tz.from_local_datetime(&wall).earliest()?.naive_utc() != utc {
            return None;
        }
        let wall = from_datetime(wall.and_utc(), *unit)?;
        let value = if value_unit == *unit {
            value.as_ref().clone()
        } else {
            cast(value.as_ref().clone(), DataType::Timestamp(*unit, None))
        };
        Some((value, lit(timestamp_scalar(wall, *unit))))
    }

    /// Spark's comparison of the date of a TIMESTAMP with a DATE constant, which compares the
    /// TIMESTAMP with the start of the day in the session time zone (`unwrapDateToTimestamp`), so
    /// that Parquet can prune with it. A TIMESTAMP in the session time zone casts like Spark
    /// already, and the optimizer unwraps the cast.
    fn unwrap_date_cast(
        &self,
        cast_expr: &Expr,
        op: Operator,
        constant: &Expr,
        schema: &DFSchema,
    ) -> Option<Expr> {
        let Expr::Cast(Cast { expr: value, field }) = cast_expr else {
            return None;
        };
        if *field.data_type() != DataType::Date32 {
            return None;
        }
        let DataType::Timestamp(unit, Some(label)) = value.get_type(schema).ok()? else {
            return None;
        };
        if label.as_ref() == self.timezone {
            return None;
        }
        let Some(ScalarValue::Date32(Some(days))) = self.constant_value(constant) else {
            return None;
        };
        let start = self.day_start(days, unit, &label)?;
        let end = self.day_start(days.checked_add(1)?, unit, &label)?;
        let value = value.as_ref().clone();
        Some(match op {
            Operator::Eq => value.clone().gt_eq(start).and(value.lt(end)),
            Operator::NotEq => value.clone().lt(start).or(value.gt_eq(end)),
            Operator::Lt => value.lt(start),
            Operator::LtEq => value.lt(end),
            Operator::Gt => value.gt_eq(end),
            Operator::GtEq => value.gt_eq(start),
            _ => return None,
        })
    }

    /// The start of the day `days` in the session time zone, labelled with `label`. Returns `None`
    /// when midnight is in a DST gap.
    fn day_start(&self, days: i32, unit: TimeUnit, label: &Arc<str>) -> Option<Expr> {
        let tz = self.timezone.parse::<Tz>().ok()?;
        let midnight = DateTime::UNIX_EPOCH
            .date_naive()
            .checked_add_signed(chrono::TimeDelta::try_days(i64::from(days))?)?
            .and_hms_opt(0, 0, 0)?;
        let micros = tz
            .from_local_datetime(&midnight)
            .earliest()?
            .timestamp_micros();
        let value = match unit {
            TimeUnit::Second => micros / 1_000_000,
            TimeUnit::Millisecond => micros / 1_000,
            TimeUnit::Microsecond => micros,
            TimeUnit::Nanosecond => micros.checked_mul(1_000)?,
        };
        let label = Some(Arc::clone(label));
        Some(lit(match unit {
            TimeUnit::Second => ScalarValue::TimestampSecond(Some(value), label),
            TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(Some(value), label),
            TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(Some(value), label),
            TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(value), label),
        }))
    }

    /// The value of a constant expression, which the optimizer evaluates like Spark's constant
    /// folding.
    fn constant_value(&self, constant: &Expr) -> Option<ScalarValue> {
        if !is_foldable_constant(constant) {
            return None;
        }
        let constant = match constant {
            Expr::Literal(..) => constant.clone(),
            _ => self
                .simplifier
                .get_or_init(|| {
                    ExprSimplifier::new(
                        SimplifyContext::builder()
                            .with_schema(Arc::new(DFSchema::empty()))
                            .with_config_options(Arc::new(self.config.clone()))
                            .build(),
                    )
                })
                .simplify(constant.clone())
                .ok()?,
        };
        match constant {
            Expr::Literal(value, _) => Some(value),
            _ => None,
        }
    }

    /// The instant of a TIMESTAMP constant in `unit`, evaluating a constant expression such as
    /// `TIMESTAMP '...' - INTERVAL 1 DAY` like Spark's constant folding.
    fn timestamp_constant(&self, constant: &Expr, unit: TimeUnit) -> Option<DateTime<Utc>> {
        let value = self.constant_value(constant)?;
        match (value, unit) {
            (ScalarValue::TimestampSecond(Some(v), Some(_)), TimeUnit::Second) => {
                DateTime::from_timestamp(v, 0)
            }
            (ScalarValue::TimestampMillisecond(Some(v), Some(_)), TimeUnit::Millisecond) => {
                DateTime::from_timestamp_millis(v)
            }
            (ScalarValue::TimestampMicrosecond(Some(v), Some(_)), TimeUnit::Microsecond) => {
                DateTime::from_timestamp_micros(v)
            }
            (ScalarValue::TimestampNanosecond(Some(v), Some(_)), TimeUnit::Nanosecond) => {
                Some(DateTime::from_timestamp_nanos(v))
            }
            _ => None,
        }
    }
}

/// Whether the plan node has a cast to TIMESTAMP, which type coercion adds when TIMESTAMP_NTZ
/// meets TIMESTAMP.
fn has_timestamp_cast(plan: &LogicalPlan) -> Result<bool> {
    let mut found = false;
    plan.apply_expressions(|expr| {
        expr.apply(|expr| {
            found = matches!(
                expr,
                Expr::Cast(Cast { field, .. })
                    if has_timestamp(field.data_type()) || *field.data_type() == DataType::Date32
            );
            Ok(if found {
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })
    })?;
    Ok(found)
}

/// Whether the type is or has TIMESTAMP values.
fn has_timestamp(data_type: &DataType) -> bool {
    match data_type {
        DataType::Timestamp(_, Some(_)) => true,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => has_timestamp(field.data_type()),
        DataType::Struct(fields) => fields.iter().any(|field| has_timestamp(field.data_type())),
        _ => false,
    }
}

/// The types that a nested cast of TIMESTAMP_NTZ values to TIMESTAMP in the session time zone
/// takes, in which the converted values are microseconds as TIMESTAMP_NTZ and as TIMESTAMP.
struct NestedSessionCastTypes {
    timestamp_ntz: DataType,
    timestamp: DataType,
    /// Whether any value converts.
    converted: bool,
    /// Whether `timestamp` is the target type.
    timestamp_is_target: bool,
}

/// Returns `None` when a TIMESTAMP_NTZ value stays TIMESTAMP_NTZ, which `ConvertTz` would convert.
fn nested_session_cast_types(
    from: &DataType,
    to: &DataType,
    timezone: &str,
) -> Option<NestedSessionCastTypes> {
    let with_type = |field: &FieldRef, data_type: DataType| {
        Arc::new(field.as_ref().clone().with_data_type(data_type))
    };
    let nested = |from: &FieldRef, to: &FieldRef, f: &dyn Fn(FieldRef) -> DataType| {
        let types = nested_session_cast_types(from.data_type(), to.data_type(), timezone)?;
        Some(NestedSessionCastTypes {
            timestamp_ntz: f(with_type(to, types.timestamp_ntz)),
            timestamp: f(with_type(to, types.timestamp)),
            converted: types.converted,
            timestamp_is_target: types.timestamp_is_target,
        })
    };
    match (from, to) {
        (DataType::Timestamp(_, None), DataType::Timestamp(unit, Some(label)))
            if label.as_ref() == timezone =>
        {
            Some(NestedSessionCastTypes {
                timestamp_ntz: DataType::Timestamp(TimeUnit::Microsecond, None),
                timestamp: DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::clone(label))),
                converted: true,
                timestamp_is_target: *unit == TimeUnit::Microsecond,
            })
        }
        (DataType::List(from), DataType::List(to)) => nested(from, to, &DataType::List),
        (DataType::LargeList(from), DataType::LargeList(to)) => {
            nested(from, to, &DataType::LargeList)
        }
        (DataType::FixedSizeList(from, size), DataType::FixedSizeList(to, to_size))
            if size == to_size =>
        {
            nested(from, to, &|field| DataType::FixedSizeList(field, *size))
        }
        (DataType::Map(from, _), DataType::Map(to, sorted)) => {
            nested(from, to, &|field| DataType::Map(field, *sorted))
        }
        (DataType::Struct(from), DataType::Struct(to)) if from.len() == to.len() => {
            let fields = from
                .iter()
                .zip(to.iter())
                .map(|(from, to)| {
                    nested_session_cast_types(from.data_type(), to.data_type(), timezone)
                        .map(|types| (to, types))
                })
                .collect::<Option<Vec<_>>>()?;
            let struct_type = |f: &dyn Fn(&NestedSessionCastTypes) -> DataType| {
                DataType::Struct(
                    fields
                        .iter()
                        .map(|(to, types)| with_type(to, f(types)))
                        .collect(),
                )
            };
            Some(NestedSessionCastTypes {
                timestamp_ntz: struct_type(&|types| types.timestamp_ntz.clone()),
                timestamp: struct_type(&|types| types.timestamp.clone()),
                converted: fields.iter().any(|(_, types)| types.converted),
                timestamp_is_target: fields.iter().all(|(_, types)| types.timestamp_is_target),
            })
        }
        (_, to) if has_timestamp_ntz(to) => None,
        (_, to) => Some(NestedSessionCastTypes {
            timestamp_ntz: to.clone(),
            timestamp: to.clone(),
            converted: false,
            timestamp_is_target: true,
        }),
    }
}

fn is_comparison(op: Operator) -> bool {
    matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom
    )
}

/// Whether the expression is a constant that the optimizer evaluates before comparing it.
fn is_foldable_constant(expr: &Expr) -> bool {
    !expr
        .exists(|expr| {
            Ok(match expr {
                Expr::Literal(..)
                | Expr::Cast(_)
                | Expr::TryCast(_)
                | Expr::BinaryExpr(_)
                | Expr::Negative(_) => false,
                // Functions without arguments, such as `current_timestamp()`, depend on the query
                // execution time.
                Expr::ScalarFunction(function) => match function.func.signature().volatility {
                    Volatility::Immutable => false,
                    Volatility::Stable => function.args.is_empty(),
                    Volatility::Volatile => true,
                },
                _ => true,
            })
        })
        .unwrap_or(true)
}

fn from_datetime(value: DateTime<Utc>, unit: TimeUnit) -> Option<i64> {
    match unit {
        TimeUnit::Second => Some(value.timestamp()),
        TimeUnit::Millisecond => Some(value.timestamp_millis()),
        TimeUnit::Microsecond => Some(value.timestamp_micros()),
        TimeUnit::Nanosecond => value.timestamp_nanos_opt(),
    }
}

fn timestamp_scalar(value: i64, unit: TimeUnit) -> ScalarValue {
    match unit {
        TimeUnit::Second => ScalarValue::TimestampSecond(Some(value), None),
        TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(Some(value), None),
        TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(Some(value), None),
        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(value), None),
    }
}
