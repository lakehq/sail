use std::sync::Arc;

use datafusion::arrow::datatypes::{
    DataType, IntervalDayTimeType, IntervalUnit, IntervalYearMonthType, TimeUnit,
};
use datafusion::functions::expr_fn;
use datafusion_common::types::NativeType;
use datafusion_common::{DFSchemaRef, ScalarValue};
use datafusion_expr::expr::{self, Expr};
use datafusion_expr::{BinaryExpr, ExprSchemable, Operator, ScalarUDF, cast, lit, try_cast, when};
use datafusion_functions::core::expr_ext::FieldAccessor;
use datafusion_spark::function::datetime::make_dt_interval::SparkMakeDtInterval;
use datafusion_spark::function::datetime::make_interval::SparkMakeInterval;
use sail_common::datetime::time_unit_to_multiplier;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::datetime::convert_tz::ConvertTz;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_date_part::SparkDatePart;
use sail_function::scalar::datetime::spark_date_trunc::SparkDateTrunc;
use sail_function::scalar::datetime::spark_last_day::SparkLastDay;
use sail_function::scalar::datetime::spark_make_time::SparkMakeTime;
use sail_function::scalar::datetime::spark_make_timestamp_ntz::SparkMakeTimestampNtz;
use sail_function::scalar::datetime::spark_make_ym_interval::SparkMakeYmInterval;
use sail_function::scalar::datetime::spark_next_day::SparkNextDay;
use sail_function::scalar::datetime::spark_time::SparkTime;
use sail_function::scalar::datetime::spark_time_diff::SparkTimeDiff;
use sail_function::scalar::datetime::spark_time_trunc::SparkTimeTrunc;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::datetime::spark_to_chrono_fmt::SparkToChronoFmt;
use sail_function::scalar::datetime::spark_trunc_level::{SparkTruncLevel, TRUNC_LEVEL_NULL};
use sail_function::scalar::datetime::spark_unix_timestamp::SparkUnixTimestamp;
use sail_function::scalar::datetime::spark_window_buckets::SparkWindowBuckets;
use sail_function::scalar::datetime::spark_year::SparkYear;
use sail_function::scalar::datetime::timestamp_now::TimestampNow;
use sail_function::scalar::explode::{Explode, ExplodeKind};
use sail_sql_analyzer::literal::interval::IntervalValue;
use sail_sql_analyzer::parser::parse_interval;

use crate::config::DefaultTimestampType;
use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

fn integer_part(expr: Expr, part: &str) -> Expr {
    cast(
        expr_fn::date_part(lit(part.to_uppercase()), expr),
        DataType::Int32,
    )
}

fn years(arg: Expr) -> Expr {
    integer_part(arg, "YEAR")
}

/// The granularities `date_trunc` truncates to. The ALIASES each one accepts live in
/// `SparkTruncLevel`, which is the single place a unit is matched -- whether it arrives as a
/// literal or in a column -- so the two paths cannot drift apart.
const DATE_TRUNC_LEVELS: &[&str] = &[
    "year",
    "quarter",
    "month",
    "week",
    "day",
    "hour",
    "minute",
    "second",
    "millisecond",
    "microsecond",
];

/// `trunc` is a different Spark expression (`TruncDate`) and truncates only to a date-level unit.
/// A finer unit is not an error: it names a granularity this list omits, so it falls through to
/// NULL exactly like a unit that matches nothing.
const TRUNC_LEVELS: &[&str] = &["year", "quarter", "month", "week"];

/// Spark resolves the truncation unit **per row** and yields NULL for a unit it does not
/// recognize, whereas DataFusion's `date_trunc` demands a literal unit and errors otherwise.
///
/// So enumerate the units in the plan rather than inspecting the argument: every branch calls the
/// kernel with a literal unit, which makes a unit arriving in a column behave exactly like a
/// literal one, and lets an unrecognized, NULL, or non-string unit fall through to NULL. Matching
/// the unit lives here and nowhere else, so the two comparisons cannot drift apart.
///
/// A literal STRING unit is resolved here and emits the single call it names, so the common case
/// costs nothing: no branch, and nothing per-unit shipped to the workers. A literal of any other
/// type is measured as its string form, which this stage cannot compute, so it takes the dispatch
/// below like a column would.
///
/// A unit that only exists as a column is dispatched by a `CASE` over the resolved level. That
/// `CASE` repeats the VALUE subtree once per branch -- ten for `date_trunc`, four for `trunc`,
/// plus the unmatched one -- and the whole thing travels to every worker in the physical plan.
/// Runtime cost is bounded (`CaseExpr` evaluates each branch on a disjoint slice of rows, so the
/// value is still computed at most once per row), but plan SIZE is not. Collapsing it would mean
/// passing the level as a column to a single call, which would also make the value an ordinary
/// argument -- always evaluated -- and that is exactly what Spark's short circuit forbids.
///
/// Whether a unit that matches nothing converts the value is not the same in the two cases, and
/// Spark's codegen is where the difference lives (`TruncInstant.codeGenHelper`):
///
/// * a **literal** unit that names no granularity short-circuits to NULL and never evaluates the
///   value, so `null` is a bare NULL literal;
/// * a unit **in a column** is evaluated together with the value before the level is checked, so
///   `unmatched` has to convert the value and only then yield NULL -- a bare literal there would
///   swallow the error Spark raises under ANSI for a value it cannot convert.
fn truncate_by_unit(
    unit: Expr,
    levels: &[&str],
    truncate: impl Fn(&str) -> Expr,
    null: Expr,
    unmatched: Expr,
    null_unit_converts_value: bool,
) -> Expr {
    // A NULL unit matches nothing whatever its type, and Spark folds the cast that types it --
    // `date_trunc(CAST(NULL AS STRING), ...)` short-circuits just like a bare NULL.
    if is_folded_null(&unit) {
        return null;
    }
    // Spark's short circuit keys off `foldable`, not off being a literal: it resolves anything
    // without an input at compile time, so `concat('BOG','US')` skips the value too. Sail cannot
    // name the granularity for those at plan-build time -- the optimizer that would evaluate them
    // runs later -- but it can still decline to convert the value, which is the part that shows.
    //
    // KNOWN GAP -- a scalar subquery is treated as foldable here, and that is not always right.
    // `ScalarSubquery.foldable` is false in Spark; what makes the short circuit happen is Spark's
    // own optimizer folding the subquery into a literal before codegen. Whether it does depends
    // on the subquery, and this check cannot tell: it sees the UNOPTIMIZED plan.
    //
    // Measured on JVM 4.1.1. `(SELECT 'BOGUS')` and a one-row temporary view are both folded, and
    // Spark returns NULL for them -- Sail agrees. A subquery over a relation (`... FROM range(1)`,
    // a table) is NOT folded, and Spark raises `CAST_INVALID_INPUT` where Sail still returns NULL.
    // That case is reproducible and covered by a `@sail-bug` scenario in `date_trunc.feature`.
    //
    // The boundary IS detectable from here: Spark folds exactly when every leaf of the subquery
    // is a one-row relation. `(SELECT 'BOGUS')`, a view over a constant and `FROM (SELECT 1)` all
    // fold; `FROM (VALUES ('x'))`, `FROM range(1)` and a table do not. Left alone anyway, because
    // that boundary is a quirk of which optimizer rule fires rather than a rule of the language --
    // nothing explains why `VALUES` of one row is not folded while `SELECT 1` is -- so encoding it
    // buys one exotic case and pins Sail to an internal detail of a specific Spark version.
    let foldable = unit.column_refs().is_empty() && !unit.is_volatile() && !unit.contains_outer();
    let unmatched = if foldable { null.clone() } else { unmatched };
    // A NULL unit and a unit that matches nothing are NOT the same case, and which one skips the
    // value follows the ARGUMENT ORDER of the Spark expression. `nullSafeCodeGen` evaluates the
    // left child, and only evaluates the right one inside the null check: `TruncTimestamp` is
    // `(format, timestamp)`, so a NULL format never reaches the timestamp; `TruncDate` is
    // `(date, format)`, so the date is always evaluated. Verified on JVM 4.1.1.
    let null_unit = if null_unit_converts_value {
        unmatched.clone()
    } else {
        null.clone()
    };
    // The plan builder runs before types are resolved, so only a literal can be folded here.
    // Only a STRING literal resolves here. Any other literal is measured as its string form, and
    // that form is not known at this stage: `X'59454152'` is the string `YEAR` once Spark coerces
    // it, so treating a non-string literal as "matches nothing" would answer NULL for a unit that
    // names a granularity. Let it fall through to the dispatch instead, where the coercion
    // happens -- a literal is foldable, so the unmatched branch there is the bare NULL anyway and
    // the value is still not converted.
    if let Some(
        ScalarValue::Utf8(Some(literal))
        | ScalarValue::LargeUtf8(Some(literal))
        | ScalarValue::Utf8View(Some(literal)),
    ) = folded_literal(&unit)
    {
        // The same matcher the columnar path runs, so a literal and a column resolve a unit
        // identically. A granularity this function does not accept is filtered out here for the
        // same reason the dispatch below omits it.
        let granularity = SparkTruncLevel::level(literal).filter(|level| levels.contains(level));
        return granularity.map_or(null, truncate);
    }
    // The unit resolves to its granularity in ONE evaluation. Doing it with expressions cannot:
    // the dispatch has to tell three outcomes apart -- named a granularity, was NULL, matched
    // nothing -- and `CASE x WHEN NULL` never matches, so a NULL selector shares the `ELSE` with
    // an unmatched one. Splitting them in the plan (`coalesce`, `IS NULL`) mentions the unit
    // twice, and DataFusion does not share a volatile subtree between the mentions, so a unit
    // like `IF(rand() < 0.5, NULL, 'YEAR')` gets drawn once per mention.
    let level = ScalarUDF::from(SparkTruncLevel::new()).call(vec![unit]);
    Expr::Case(expr::Case {
        expr: Some(Box::new(level)),
        when_then_expr: levels
            .iter()
            .map(|level| (Box::new(lit(*level)), Box::new(truncate(level))))
            .chain(std::iter::once((
                Box::new(lit(TRUNC_LEVEL_NULL)),
                Box::new(null_unit),
            )))
            .collect(),
        // A granularity this function does not accept -- `trunc` takes only the date-level ones --
        // resolves to a level that no arm above lists, and lands here with the unmatched units.
        else_expr: Some(Box::new(unmatched)),
    })
}

/// The branch a unit that matches nothing takes when the unit is NOT a literal: Spark has already
/// evaluated the value by then, so the conversion has to happen and only then yield NULL.
///
/// `IS NULL` evaluates the value **once** and throws the answer away; both branches are the same
/// NULL, so the row is NULL whatever the value held. `nullif(value, value)` would evaluate it
/// TWICE — DataFusion does not hoist a volatile subtree into a common expression
/// (`common_subexpr_eliminate.rs`, `!node.is_volatile_node()`), so a value built from `rand()`
/// draws a different number per copy, the two do not compare equal, and the row keeps a value
/// where Spark yields NULL.
///
/// This leans on the value being nullable: against a non-nullable one the simplifier rewrites
/// `IsNull(e)` to `false` and can then drop `e` with the branch, taking the conversion — and the
/// ANSI error that rides on it — with it. Every value reaching here is the result of a UDF call
/// or a cast, both nullable, so it holds today; a future path that converts to a non-nullable
/// value would need a different guard.
fn null_after_converting(value: Expr, null: Expr) -> Expr {
    Expr::Case(expr::Case {
        expr: None,
        when_then_expr: vec![(Box::new(value.is_null()), Box::new(null.clone()))],
        else_expr: Some(Box::new(null)),
    })
}

/// The literal an expression folds to, looking through the casts that only give it a type. Spark
/// folds those too, so `CAST(NULL AS STRING)` and an integer unit resolve here rather than
/// becoming a per-row match.
fn folded_literal(expr: &Expr) -> Option<&ScalarValue> {
    match expr {
        Expr::Literal(value, _) => Some(value),
        Expr::Cast(expr::Cast { expr, .. }) | Expr::TryCast(expr::TryCast { expr, .. }) => {
            folded_literal(expr)
        }
        _ => None,
    }
}

/// Whether an expression is a NULL that Spark would fold. `is_null_literal` sees the bare literal
/// alone, and a unit written as `CAST(NULL AS STRING)` has to short-circuit just like a bare NULL.
fn is_folded_null(expr: &Expr) -> bool {
    folded_literal(expr).is_some_and(ScalarValue::is_null)
}

/// Spark reports an argument it cannot use for its TYPE while it analyzes the query, as
/// `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE`. A Spark client matches on that error class, so the
/// rejection has to carry it rather than a plain invalid-argument error.
fn unexpected_input_type_err(
    function: &str,
    parameter: &str,
    required: &str,
    actual: &DataType,
) -> PlanError {
    // `AnalysisError`, not `invalid`: Spark raises this while it analyzes the query, and a Spark
    // client sees the throwable class. `PlanError::InvalidArgument` surfaces as
    // `IllegalArgumentException`, where Spark raises `AnalysisException`.
    PlanError::AnalysisError(format!(
        "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve `{function}` due to data type \
         mismatch: The {parameter} parameter requires the \"{required}\" type, however the input \
         has the type \"{actual}\"."
    ))
}

/// Spark requires a STRING unit and rejects a complex one while it analyzes the query. Any other
/// atomic type is measured as its string form and simply matches no granularity.
///
/// A unit that is already a string is handed back untouched. Casting it would rewrite a
/// `Utf8View` -- what Parquet produces by default -- into `Utf8`, copying every value and giving
/// up the view fast path in `upper`, all to reach a type it already had.
fn coerce_unit(
    function: &str,
    parameter: &str,
    unit: Expr,
    schema: &DFSchemaRef,
) -> PlanResult<Expr> {
    let data_type = unit.get_type(schema)?;
    match (&data_type).into() {
        // Only the plain string types go through untouched. An encoding that merely WRAPS a
        // string still has to be unwrapped: the matcher's signature brings a dictionary down to a
        // string but not a run-end-encoded one, and rejecting a unit for its layout is not a
        // thing Spark does. Casting a `Utf8View` here would copy every value to reach a type it
        // already is, which is why the plain ones are excluded.
        //
        // The run-end-encoded case is traced, not exercised: the tests reach a dictionary through
        // a pandas categorical, and nothing in the Python client produces a run-end-encoded
        // column. It mirrors the value path, which IS covered.
        NativeType::String
            if matches!(
                data_type,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) =>
        {
            Ok(unit)
        }
        NativeType::String => Ok(cast(unit, DataType::Utf8)),
        NativeType::List(_)
        | NativeType::FixedSizeList(_, _)
        | NativeType::Struct(_)
        | NativeType::Map(_)
        | NativeType::Union(_) => Err(unexpected_input_type_err(
            function, parameter, "STRING", &data_type,
        )),
        // `try_cast`, not `cast`: Spark's coercion to string never fails -- `Cast(binary, string)`
        // is `UTF8String.fromBytes`, which does not validate UTF-8 -- so an argument Arrow refuses
        // to convert has to become a unit that matches nothing, not an error. Spark returns NULL
        // for `date_trunc(X'FF', ts)`.
        _ => Ok(try_cast(unit, DataType::Utf8)),
    }
}

fn trunc(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ansi_mode = input.function_context.plan_config.ansi_mode;
    let schema = input.function_context.schema;
    let (date, part) = input.arguments.two()?;
    let part = coerce_unit("trunc", "second", part, schema)?;
    // Spark's `trunc` coerces its value argument to DateType, and does so before it validates the
    // unit: the conversion errors under ANSI and is NULL when ANSI is off, whether or not the
    // unit names a granularity. The type is gated first, and not only for the error class: Arrow
    // REINTERPRETS an integer as a day count, so `trunc(1, 'YEAR')` would quietly answer
    // 1970-01-01 where Spark rejects the query.
    let data_type = date.get_type(schema)?;
    let date = match (&data_type).into() {
        // Strings parse with Spark's own grammar, which takes `2009-2-12` and a bare `2024` that
        // Arrow's fixed-position parser rejects. This is the parser Sail's `CAST(… AS DATE)`
        // already uses, so the two surfaces agree.
        NativeType::String => ScalarUDF::from(SparkDate::new(!ansi_mode)).call(vec![date]),
        NativeType::Null | NativeType::Date | NativeType::Timestamp(_, _) => {
            if ansi_mode {
                cast(date, DataType::Date32)
            } else {
                try_cast(date, DataType::Date32)
            }
        }
        _ => {
            return Err(unexpected_input_type_err(
                "trunc", "first", "DATE", &data_type,
            ));
        }
    };
    // Truncate through a naive microsecond timestamp rather than handing the DATE straight to the
    // kernel. `SparkDateTrunc` only takes over for a microsecond timestamp; a DATE falls through
    // to DataFusion, which coerces it to NANOsecond and multiplies days out with an unchecked
    // `unary`, so `trunc(DATE '2300-01-01', 'YEAR')` wraps around into the past instead of
    // truncating. A DATE carries no zone, so converting it to a naive timestamp is exact.
    let naive = cast(date, DataType::Timestamp(TimeUnit::Microsecond, None));
    Ok(truncate_by_unit(
        part,
        TRUNC_LEVELS,
        // Truncating goes through the same kernel `date_trunc` uses, which carries Spark's
        // nullability: `trunc` is nullable whatever its arguments are, because a unit that names
        // no granularity turns the row NULL.
        |granularity| {
            cast(
                ScalarUDF::from(SparkDateTrunc::new()).call(vec![lit(granularity), naive.clone()]),
                DataType::Date32,
            )
        },
        lit(ScalarValue::Date32(None)),
        null_after_converting(naive.clone(), lit(ScalarValue::Date32(None))),
        // `TruncDate` is `(date, format)`: the date is the left child, so it is evaluated even
        // when the format is NULL.
        true,
    ))
}

fn date_trunc(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let session_tz = input.function_context.plan_config.session_timezone.clone();
    let ansi_mode = input.function_context.plan_config.ansi_mode;
    let schema = input.function_context.schema;
    let (part, timestamp) = input.arguments.two()?;
    let part = coerce_unit("date_trunc", "first", part, schema)?;
    // A string that is not a timestamp errors under ANSI, and is NULL when ANSI is off.
    let to_timestamp = |expr: Expr| {
        let timestamp = DataType::Timestamp(TimeUnit::Microsecond, Some(session_tz.clone()));
        if ansi_mode {
            cast(expr, timestamp)
        } else {
            try_cast(expr, timestamp)
        }
    };
    // Spark's date_trunc coerces its value argument to TimestampType and always returns
    // TimestampType, whether the input is DATE, TIMESTAMP, TIMESTAMP_NTZ, or a string.
    // Leave other types untouched so they are rejected like Spark instead of silently coerced.
    // The match is on the LOGICAL type, so an encoding that only wraps one of these -- a
    // dictionary or a run-end-encoded string -- is accepted rather than rejected for its layout.
    let data_type = timestamp.get_type(schema)?;
    let timestamp = match (&data_type).into() {
        // A DATE and a TIMESTAMP_NTZ are both wall clocks, and turning a wall clock into an
        // instant is the conversion Arrow's cast gets wrong: it resolves the local time with
        // `.single()`, which has no answer inside a DST gap or inside the repeated hour of a
        // fall-back. Spark moves the first forward and takes the earlier offset for the second,
        // which is what `SparkTimestamp` does. A DATE reaches it as a naive timestamp, a
        // conversion that is pure arithmetic and involves no zone: midnight of that date. Zones
        // whose transition happens AT midnight (`America/Havana`) make the DATE path fail
        // exactly like the NTZ one.
        NativeType::Date | NativeType::Timestamp(_, None) => {
            let naive = cast(timestamp, DataType::Timestamp(TimeUnit::Microsecond, None));
            ScalarUDF::from(SparkTimestamp::try_new(
                Some(session_tz.clone()),
                ansi_mode,
                false,
            )?)
            .call(vec![naive])
        }
        // Strings parse with Spark's own grammar, which takes `2009-2-12` and a bare `2024`.
        // Arrow's parser demands a fixed-position `YYYY-MM-DD` and at least 10 bytes, so it
        // rejects both -- and Sail's `CAST(… AS TIMESTAMP)` already routes here, so leaving this
        // on the Arrow parser made the two surfaces disagree with each other.
        //
        // `SparkTimestamp` declares a user-defined signature, which does no coercion of its own,
        // so an encoding that merely WRAPS a string -- a dictionary, a run-end-encoded column --
        // has to be unwrapped here or the call is rejected for its layout. Only those are cast:
        // casting a `Utf8View` would copy every value to reach a type it already is.
        NativeType::String => {
            let value = if matches!(
                data_type,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) {
                timestamp
            } else {
                cast(timestamp, DataType::Utf8)
            };
            ScalarUDF::from(SparkTimestamp::try_new(
                Some(session_tz.clone()),
                ansi_mode,
                false,
            )?)
            .call(vec![value])
        }
        NativeType::Null | NativeType::Timestamp(_, _) => to_timestamp(timestamp),
        // Spark rejects the value for its type when it analyzes the query, so the rejection
        // must not depend on the unit: an unrecognized or NULL unit does not turn it into NULL.
        _ => {
            return Err(unexpected_input_type_err(
                "date_trunc",
                "second",
                "TIMESTAMP",
                &data_type,
            ));
        }
    };
    let truncated = truncate_by_unit(
        part,
        DATE_TRUNC_LEVELS,
        |granularity| {
            ScalarUDF::from(SparkDateTrunc::new()).call(vec![lit(granularity), timestamp.clone()])
        },
        lit(ScalarValue::TimestampMicrosecond(
            None,
            Some(session_tz.clone()),
        )),
        null_after_converting(
            timestamp.clone(),
            lit(ScalarValue::TimestampMicrosecond(
                None,
                Some(session_tz.clone()),
            )),
        ),
        // `TruncTimestamp` is `(format, timestamp)`: the format is the left child, so a NULL
        // format short-circuits and the timestamp is never evaluated.
        false,
    );
    let truncated = match truncated.get_type(input.function_context.schema)? {
        DataType::Timestamp(TimeUnit::Microsecond, _) => truncated,
        DataType::Timestamp(_, tz) => {
            cast(truncated, DataType::Timestamp(TimeUnit::Microsecond, tz))
        }
        other => Err(PlanError::InternalError(format!(
            "date_trunc expected a timestamp result, got {other:?}"
        )))?,
    };
    Ok(truncated)
}

fn interval_arithmetic(input: ScalarFunctionInput, unit: &str, op: Operator) -> PlanResult<Expr> {
    let (date, interval) = input.arguments.two()?;

    let interval = match unit.to_lowercase().as_str() {
        "years" | "year" => match interval {
            Expr::Literal(ScalarValue::Int32(Some(years)), metadata) => Expr::Literal(
                ScalarValue::IntervalYearMonth(Some(IntervalYearMonthType::make_value(years, 0))),
                metadata,
            ),
            _ => cast(
                format_interval(interval, "years"),
                DataType::Interval(IntervalUnit::YearMonth),
            ),
        },
        "months" | "month" => match interval {
            Expr::Literal(ScalarValue::Int32(Some(months)), metadata) => Expr::Literal(
                ScalarValue::IntervalYearMonth(Some(IntervalYearMonthType::make_value(0, months))),
                metadata,
            ),
            _ => cast(
                format_interval(interval, "months"),
                DataType::Interval(IntervalUnit::YearMonth),
            ),
        },
        "days" | "day" => match interval {
            Expr::Literal(ScalarValue::Int32(Some(days)), metadata) => Expr::Literal(
                ScalarValue::IntervalDayTime(Some(IntervalDayTimeType::make_value(days, 0))),
                metadata,
            ),
            _ => cast(
                format_interval(interval, "days"),
                DataType::Interval(IntervalUnit::DayTime),
            ),
        },
        _ => {
            return Err(PlanError::invalid(format!(
                "add_interval does not support interval unit type '{unit}'"
            )));
        }
    };
    Ok(Expr::BinaryExpr(BinaryExpr {
        left: Box::new(cast(date, DataType::Date32)),
        op,
        right: Box::new(interval),
    }))
}

fn format_interval(interval: Expr, unit: &str) -> Expr {
    Expr::BinaryExpr(BinaryExpr {
        left: Box::new(interval),
        op: Operator::StringConcat,
        right: Box::new(lit(format!(" {unit}"))),
    })
}

fn timestampadd_interval(unit: &str, quantity: Expr) -> PlanResult<Expr> {
    let zero_i32 = || lit(0_i32);
    let zero_f64 = || lit(0_f64);
    let quantity_i32 = || cast(quantity.clone(), DataType::Int32);
    let quantity_f64 = || cast(quantity.clone(), DataType::Float64);
    let make_interval = |args: Vec<Expr>| ScalarUDF::from(SparkMakeInterval::new()).call(args);
    let make_dt_interval = |args: Vec<Expr>| ScalarUDF::from(SparkMakeDtInterval::new()).call(args);

    let normalized = unit.trim().to_uppercase();
    match normalized.as_str() {
        "YEAR" => Ok(make_interval(vec![quantity_i32()])),
        "QUARTER" => Ok(make_interval(vec![
            zero_i32(),
            cast(quantity.clone() * lit(3_i32), DataType::Int32),
        ])),
        "MONTH" => Ok(make_interval(vec![zero_i32(), quantity_i32()])),
        "WEEK" => Ok(make_dt_interval(vec![
            cast(quantity.clone() * lit(7_i32), DataType::Int32),
            zero_i32(),
            zero_i32(),
            zero_f64(),
        ])),
        "DAY" | "DAYOFYEAR" => Ok(make_dt_interval(vec![
            quantity_i32(),
            zero_i32(),
            zero_i32(),
            zero_f64(),
        ])),
        "HOUR" => Ok(make_dt_interval(vec![
            zero_i32(),
            quantity_i32(),
            zero_i32(),
            zero_f64(),
        ])),
        "MINUTE" => Ok(make_dt_interval(vec![
            zero_i32(),
            zero_i32(),
            quantity_i32(),
            zero_f64(),
        ])),
        "SECOND" => Ok(make_dt_interval(vec![
            zero_i32(),
            zero_i32(),
            zero_i32(),
            quantity_f64(),
        ])),
        "MILLISECOND" => Ok(make_dt_interval(vec![
            zero_i32(),
            zero_i32(),
            zero_i32(),
            quantity_f64() / lit(1_000_f64),
        ])),
        "MICROSECOND" => Ok(make_dt_interval(vec![
            zero_i32(),
            zero_i32(),
            zero_i32(),
            quantity_f64() / lit(1_000_000_f64),
        ])),
        _ => Err(PlanError::invalid(format!(
            "timestampadd does not support interval unit type '{unit}'"
        ))),
    }
}

fn timestampadd(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let (unit, quantity, timestamp) = input.arguments.three()?;
    let unit = match &unit {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => s.clone(),
        Expr::Column(col) => col.name().to_string(),
        _ => {
            return Err(PlanError::invalid(
                "timestampadd unit must be a string literal or keyword",
            ));
        }
    };
    let interval = timestampadd_interval(&unit, quantity)?;
    Ok(cast(
        timestamp,
        DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(input.function_context.plan_config.session_timezone.clone()),
        ),
    ) + interval)
}

fn make_date(year: Expr, month: Expr, day: Expr) -> Expr {
    match (&year, &month, &day) {
        (Expr::Literal(ScalarValue::Null, metadata), _, _)
        | (_, Expr::Literal(ScalarValue::Null, metadata), _)
        | (_, _, Expr::Literal(ScalarValue::Null, metadata)) => {
            Expr::Literal(ScalarValue::Null, metadata.clone())
        }
        _ => expr_fn::make_date(year, month, day),
    }
}

fn date_days_arithmetic(dt1: Expr, dt2: Expr, op: Operator) -> Expr {
    let (dt1, dt2) = match (&dt1, &dt2) {
        (Expr::Literal(ScalarValue::Date32(_), _), Expr::Literal(ScalarValue::Date32(_), _)) => {
            (dt1, dt2)
        }
        _ => (cast(dt1, DataType::Date32), cast(dt2, DataType::Date32)),
    };
    let dt1 = cast(dt1, DataType::Int64);
    let dt2 = cast(dt2, DataType::Int64);
    Expr::BinaryExpr(BinaryExpr {
        left: Box::new(dt1),
        op,
        right: Box::new(dt2),
    })
}

fn datediff(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let args = input.arguments;
    match args.len() {
        2 => {
            let [start, end] = <[Expr; 2]>::try_from(args)
                .map_err(|_| PlanError::invalid("datediff requires 2 or 3 arguments"))?;
            Ok(date_days_arithmetic(start, end, Operator::Minus))
        }
        3 => {
            let [unit, start, end] = <[Expr; 3]>::try_from(args)
                .map_err(|_| PlanError::invalid("datediff requires 2 or 3 arguments"))?;
            let unit_str = match &unit {
                Expr::Literal(ScalarValue::Utf8(Some(s)), _)
                | Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => s.to_uppercase(),
                Expr::Column(col) => col.name().to_uppercase(),
                _ => {
                    return Err(PlanError::invalid(
                        "datediff unit must be a string literal or keyword",
                    ));
                }
            };
            match unit_str.as_str() {
                "DAY" => Ok(date_days_arithmetic(end, start, Operator::Minus)),
                "HOUR" | "MINUTE" | "SECOND" | "MONTH" | "YEAR" | "WEEK" | "QUARTER" => {
                    let start_ts = cast(start, DataType::Timestamp(TimeUnit::Microsecond, None));
                    let end_ts = cast(end, DataType::Timestamp(TimeUnit::Microsecond, None));
                    let diff_seconds = cast(
                        Expr::BinaryExpr(BinaryExpr {
                            left: Box::new(cast(end_ts, DataType::Int64)),
                            op: Operator::Minus,
                            right: Box::new(cast(start_ts, DataType::Int64)),
                        }),
                        DataType::Int64,
                    );
                    let divisor = match unit_str.as_str() {
                        "SECOND" => 1_000_000i64,
                        "MINUTE" => 60_000_000i64,
                        "HOUR" => 3_600_000_000i64,
                        "WEEK" => 7 * 24 * 3_600_000_000i64,
                        "MONTH" => 30 * 24 * 3_600_000_000i64,
                        "YEAR" => 365 * 24 * 3_600_000_000i64,
                        "QUARTER" => 91 * 24 * 3_600_000_000i64,
                        _ => 1i64,
                    };
                    Ok(Expr::BinaryExpr(BinaryExpr {
                        left: Box::new(diff_seconds),
                        op: Operator::Divide,
                        right: Box::new(lit(divisor)),
                    }))
                }
                other => Err(PlanError::unsupported(format!("datediff unit: {other}"))),
            }
        }
        n => Err(PlanError::invalid(format!(
            "datediff requires 2 or 3 arguments, got {n}"
        ))),
    }
}

fn session_timezone(input: &ScalarFunctionInput) -> Expr {
    lit(input
        .function_context
        .plan_config
        .session_timezone
        .to_string())
}

fn current_timezone(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let session_tz = session_timezone(&input);
    input.arguments.zero()?;
    Ok(session_tz)
}

fn to_chrono_fmt(format: Expr) -> Expr {
    ScalarUDF::from(SparkToChronoFmt::new()).call(vec![format])
}

fn to_date(input: ScalarFunctionInput) -> PlanResult<Expr> {
    if input.arguments.len() == 1 {
        // If format is not supplied, the function is a synonym for cast(expr AS DATE).
        crate::function::scalar::conversion::cast_to_date(input)
    } else if input.arguments.len() == 2 {
        let (expr, format) = input.arguments.two()?;
        let expr_type = expr.get_type(input.function_context.schema);
        if let Ok(DataType::Timestamp(_, _)) = expr_type {
            let expr = expr_fn::to_local_time(vec![expr]);
            return Ok(cast(expr, DataType::Date32)); // In case of data type timestamp, ignore format
        }
        let expr = match expr_type {
            Ok(_other) => expr,
            Err(_) => cast(expr, DataType::Utf8), // In case of error, cast to string
        };
        let format = to_chrono_fmt(format);
        Ok(expr_fn::to_date(vec![expr, format]))
    } else {
        Err(PlanError::invalid("to_date requires 1 or 2 arguments"))
    }
}

fn unix_timestamp(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let timezone = input.function_context.plan_config.session_timezone.clone();
    if input.arguments.is_empty() {
        let expr = ScalarUDF::from(TimestampNow::new(timezone, TimeUnit::Second)).call(vec![]);
        Ok(cast(expr, DataType::Int64))
    } else if input.arguments.len() == 1 {
        Ok(ScalarUDF::from(SparkUnixTimestamp::new(timezone)).call(input.arguments))
    } else if input.arguments.len() == 2 {
        let (expr, format) = input.arguments.two()?;
        let format = to_chrono_fmt(format);
        Ok(ScalarUDF::from(SparkUnixTimestamp::new(timezone)).call(vec![expr, format]))
    } else {
        Err(PlanError::invalid(
            "unix_timestamp requires 1 or 2 arguments",
        ))
    }
}

fn to_unix_timestamp(input: ScalarFunctionInput) -> PlanResult<Expr> {
    if input.arguments.is_empty() {
        Err(PlanError::invalid(
            "to_unix_timestamp requires 1 or 2 arguments",
        ))
    } else {
        unix_timestamp(input)
    }
}

/// Dispatch for `next_day(date, day_of_week)`.
///
/// Reads `PlanConfig::ansi_mode` at planning time and bakes it into the UDF
/// so the runtime path chooses between erroring (ANSI=true) and returning
/// NULL (ANSI=false) on malformed day-of-week strings.
fn next_day(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ansi_mode = input.function_context.plan_config.ansi_mode;
    let udf = ScalarUDF::from(SparkNextDay::new(ansi_mode));
    Ok(udf.call(input.arguments))
}

pub(super) fn date_format(expr: Expr, format: Expr) -> Expr {
    // Handle standalone fractional seconds format (e.g., 'SSS' for milliseconds).
    // Chrono's %.Nf always includes a leading dot (e.g., ".000"), so for standalone
    // S-patterns we strip the dot using substr.
    if let Expr::Literal(sv, _) = &format
        && let Some(Some(fmt)) = sv.try_as_str()
        && !fmt.is_empty()
        && fmt.chars().all(|c| c == 'S')
    {
        let n = fmt.len();
        let chrono_fmt = format!("%.{n}f");
        let result = expr_fn::to_char(expr, lit(chrono_fmt));
        return expr_fn::substr(result, lit(2i64));
    }
    let format = to_chrono_fmt(format);
    expr_fn::to_char(expr, format)
}

fn timestamp_data_type(input: &ScalarFunctionInput, timestamp_ntz: bool) -> DataType {
    let timezone = if timestamp_ntz {
        None
    } else {
        Some(input.function_context.plan_config.session_timezone.clone())
    };
    DataType::Timestamp(TimeUnit::Microsecond, timezone)
}

fn timestamp_null(input: &ScalarFunctionInput, timestamp_ntz: bool) -> Expr {
    let timezone = if timestamp_ntz {
        None
    } else {
        Some(input.function_context.plan_config.session_timezone.clone())
    };
    lit(ScalarValue::TimestampMicrosecond(None, timezone))
}

fn is_null_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(value, _) if value.is_null())
}

fn to_timestamp(input: ScalarFunctionInput, timestamp_ntz: bool) -> PlanResult<Expr> {
    timestamp_with_try(input, timestamp_ntz, false)
}

fn to_time(input: ScalarFunctionInput) -> PlanResult<Expr> {
    time_with_try(input, false)
}

fn try_to_time(input: ScalarFunctionInput) -> PlanResult<Expr> {
    time_with_try(input, true)
}

/// Shared `to_time` / `try_to_time` planner. Routes through `SparkTime`, which
/// parses strings (with an optional chrono format) or casts time/timestamp args.
/// `to_time` errors on failure (Spark's `ToTime` is ANSI-invariant); `try_to_time`
/// (`is_try`) returns NULL.
fn time_with_try(input: ScalarFunctionInput, is_try: bool) -> PlanResult<Expr> {
    let udf = ScalarUDF::from(SparkTime::new(is_try));
    if input.arguments.len() == 1 {
        Ok(udf.call(input.arguments))
    } else if input.arguments.len() == 2 {
        // Pass `expr` through unchanged so `SparkTime::coerce_types` validates it
        // and the kernel dispatches by type (strings parse with the format,
        // TIME/TIMESTAMP cast directly), exactly as in the 1-arg form. Forcing a
        // cast to Utf8 here would route non-string inputs through string parsing,
        // bypassing the coercion checks and diverging from the 1-arg behavior.
        let (expr, format) = input.arguments.two()?;
        let format = to_chrono_fmt(format);
        Ok(udf.call(vec![expr, format]))
    } else {
        let name = if is_try { "try_to_time" } else { "to_time" };
        Err(PlanError::invalid(format!(
            "{name} requires 1 or 2 arguments"
        )))
    }
}

fn try_to_timestamp(input: ScalarFunctionInput, timestamp_ntz: bool) -> PlanResult<Expr> {
    timestamp_with_try(input, timestamp_ntz, true)
}

/// Shared `to_timestamp` / `try_to_timestamp` (+ `_ntz`) planner.
///
/// The 1-arg form goes through `cast` / `try_cast`, which route strings to
/// `SparkTimestamp` (honoring ANSI for the strict variant) and cast other types.
/// The 2-arg form parses the value with the given format via `SparkTimestamp`.
fn timestamp_with_try(
    input: ScalarFunctionInput,
    timestamp_ntz: bool,
    is_try: bool,
) -> PlanResult<Expr> {
    let data_type = timestamp_data_type(&input, timestamp_ntz);
    let ansi_mode = input.function_context.plan_config.ansi_mode;
    let timezone = if timestamp_ntz {
        None
    } else {
        Some(input.function_context.plan_config.session_timezone.clone())
    };
    if input.arguments.len() == 1 {
        let expr = input.arguments.one()?;
        let expr_type = expr.get_type(input.function_context.schema)?;
        if matches!(
            expr_type,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ) {
            // Strings parse through SparkTimestamp, which honors ANSI (errors
            // under ANSI, NULL otherwise) for the strict variant.
            let udf = ScalarUDF::from(SparkTimestamp::try_new(timezone, ansi_mode, is_try)?);
            Ok(udf.call(vec![expr]))
        } else {
            // Timestamp-with-tz is re-based to the session zone; other types cast.
            let expr = match expr_type {
                DataType::Timestamp(_, Some(_)) => expr_fn::to_local_time(vec![expr]),
                _ => expr,
            };
            if is_try {
                Ok(try_cast(expr, data_type))
            } else {
                Ok(cast(expr, data_type))
            }
        }
    } else if input.arguments.len() == 2 {
        let null = timestamp_null(&input, timestamp_ntz);
        let (expr, format) = input.arguments.two()?;
        if is_null_literal(&expr) || is_null_literal(&format) {
            return Ok(null);
        }
        let udf = ScalarUDF::from(SparkTimestamp::try_new(timezone, ansi_mode, is_try)?);
        Ok(udf.call(vec![cast(expr, DataType::Utf8), to_chrono_fmt(format)]))
    } else {
        let name = match (is_try, timestamp_ntz) {
            (false, false) => "to_timestamp",
            (true, false) => "try_to_timestamp",
            (false, true) => "to_timestamp_ntz",
            (true, true) => "try_to_timestamp_ntz",
        };
        Err(PlanError::invalid(format!(
            "{name} requires 1 or 2 arguments"
        )))
    }
}

fn from_unixtime(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let (expr, format) = if input.arguments.len() == 1 {
        let expr = input.arguments.one()?;
        // default format is "yyyy-MM-dd HH:mm:ss"
        Ok((expr, lit("yyyy-MM-dd HH:mm:ss")))
    } else if input.arguments.len() == 2 {
        input.arguments.two()
    } else {
        return Err(PlanError::invalid(
            "from_unixtime requires 1 or 2 arguments",
        ));
    }?;

    let timezone = input.function_context.plan_config.session_timezone.clone();
    let format = to_chrono_fmt(format);
    let expr = cast(expr, DataType::Timestamp(TimeUnit::Second, Some(timezone)));
    Ok(expr_fn::to_char(expr, format))
}

fn unix_time_unit(input: ScalarFunctionInput, time_unit: TimeUnit) -> PlanResult<Expr> {
    let arg = input.arguments.one()?;
    Ok(cast(
        cast(
            arg,
            DataType::Timestamp(
                time_unit,
                Some(input.function_context.plan_config.session_timezone.clone()),
            ),
        ),
        DataType::Int64,
    ))
}

fn current_timestamp_microseconds(input: ScalarFunctionInput) -> PlanResult<Expr> {
    if input.arguments.is_empty() {
        let timezone = input.function_context.plan_config.session_timezone.clone();
        Ok(ScalarUDF::from(TimestampNow::new(timezone, TimeUnit::Microsecond)).call(vec![]))
    } else {
        Err(PlanError::invalid(format!(
            "current_timestamp takes 0 arguments, got {:?}",
            input.arguments
        )))
    }
}

fn current_localtimestamp_microseconds(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let expr = current_timestamp_microseconds(input)?;
    Ok(expr_fn::to_local_time(vec![expr]))
}

fn convert_tz(from_tz: Expr, to_tz: Expr, ts: Expr, classic: bool) -> Expr {
    ScalarUDF::from(ConvertTz::new(classic)).call(vec![from_tz, to_tz, ts])
}

/// A helper function for processing the input NTZ timestamp.
fn ntz_timestamp_and_unit(
    ts: Expr,
    schema: &DFSchemaRef,
    ansi_mode: bool,
) -> PlanResult<(Expr, TimeUnit)> {
    match ts.get_type(schema)? {
        DataType::Timestamp(unit, Some(_)) => Ok((expr_fn::to_local_time(vec![ts]), unit)),
        DataType::Timestamp(unit, None) => Ok((ts, unit)),
        DataType::Date32 | DataType::Date64 => {
            let unit = TimeUnit::Microsecond;
            Ok((cast(ts, DataType::Timestamp(unit, None)), unit))
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            let unit = TimeUnit::Microsecond;
            let ts =
                ScalarUDF::from(SparkTimestamp::try_new(None, ansi_mode, false)?).call(vec![ts]);
            Ok((ts, unit))
        }
        x => Err(PlanError::invalid(format!(
            "invalid NTZ timestamp type: {x:?}"
        ))),
    }
}

fn convert_timezone(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let session_tz = input.function_context.plan_config.session_timezone.clone();
    let args = input.arguments;
    let (from_tz, to_tz, ts) = match args.len() {
        3 => Ok(args.three()?),
        2 => {
            let (to_tz, ts) = args.two()?;
            Ok((lit(session_tz.to_string()), to_tz, ts))
        }
        _ => Err(PlanError::invalid(format!(
            "convert_timezone takes 2 or 3 arguments, got {args:?}"
        ))),
    }?;
    let (ts, _unit) = ntz_timestamp_and_unit(
        ts,
        input.function_context.schema,
        input.function_context.plan_config.ansi_mode,
    )?;
    Ok(convert_tz(from_tz, to_tz, ts, true))
}

/// A helper function for processing the input timestamp for
/// `from_utc_timestamp` and `to_utc_timestamp` functions.
/// These functions expect timestamps with time zone, but consider the value
/// relative to the UTC time zone.
fn utc_ntz_timestamp_and_unit(
    ts: Expr,
    schema: &DFSchemaRef,
    session_tz: &Arc<str>,
) -> PlanResult<(Expr, TimeUnit)> {
    let (ts, unit) = match ts.get_type(schema)? {
        DataType::Timestamp(unit, Some(_)) => (ts, unit),
        DataType::Timestamp(unit, None) => {
            let ts = cast(ts, DataType::Timestamp(unit, Some(session_tz.clone())));
            (ts, unit)
        }
        DataType::Date32
        | DataType::Date64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View => {
            let unit = TimeUnit::Microsecond;
            let ts = cast(ts, DataType::Timestamp(unit, Some(session_tz.clone())));
            (ts, unit)
        }
        x => {
            return Err(PlanError::invalid(format!(
                "invalid UTC NTZ timestamp type: {x:?}"
            )));
        }
    };
    let ts = cast(ts, DataType::Timestamp(unit, None));
    Ok((ts, unit))
}

fn from_utc_timestamp(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let session_tz = input.function_context.plan_config.session_timezone.clone();
    let (ts, to_tz) = input.arguments.two()?;
    let (ts, unit) = utc_ntz_timestamp_and_unit(ts, input.function_context.schema, &session_tz)?;
    let ts = convert_tz(lit("UTC"), to_tz, ts, false);
    let ts = cast(ts, DataType::Timestamp(unit, Some(Arc::from("UTC"))));
    Ok(cast(ts, DataType::Timestamp(unit, Some(session_tz))))
}

fn to_utc_timestamp(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let session_tz = input.function_context.plan_config.session_timezone.clone();
    let (ts, from_tz) = input.arguments.two()?;
    let (ts, unit) = utc_ntz_timestamp_and_unit(ts, input.function_context.schema, &session_tz)?;
    let ts = convert_tz(from_tz, lit("UTC"), ts, false);
    let ts = cast(ts, DataType::Timestamp(unit, Some(Arc::from("UTC"))));
    Ok(cast(ts, DataType::Timestamp(unit, Some(session_tz))))
}

fn make_timestamp_ltz(args: Vec<Expr>, session_tz: &Arc<str>, is_try: bool) -> PlanResult<Expr> {
    let ntz_ts = if args.len() == 2 || args.len() == 6 {
        ScalarUDF::from(SparkMakeTimestampNtz::new(is_try)).call(args)
    } else if args.len() == 3 || args.len() == 7 {
        let mut args = args;
        let Some(from_tz) = args.pop() else {
            unreachable!()
        };
        let ntz_ts = ScalarUDF::from(SparkMakeTimestampNtz::new(is_try)).call(args);
        convert_tz(from_tz, lit(session_tz.to_string()), ntz_ts, true)
    } else {
        return Err(PlanError::invalid(format!(
            "{}make_timestamp_ltz requires 2, 3, 6 or 7 arguments, got {:?}",
            if is_try { "try_" } else { "" },
            args
        )));
    };
    Ok(cast(
        ntz_ts,
        DataType::Timestamp(TimeUnit::Microsecond, Some(session_tz.clone())),
    ))
}

fn make_timestamp_ntz(args: Vec<Expr>, is_try: bool) -> PlanResult<Expr> {
    if args.len() == 2 || args.len() == 6 {
        Ok(ScalarUDF::from(SparkMakeTimestampNtz::new(is_try)).call(args))
    } else {
        Err(PlanError::invalid(format!(
            "{}make_timestamp_ntz requires 2 or 6 arguments, got {:?}",
            if is_try { "try_" } else { "" },
            args
        )))
    }
}

fn make_timestamp(input: ScalarFunctionInput, is_try: bool) -> PlanResult<Expr> {
    let session_tz = &input.function_context.plan_config.session_timezone;
    let mut args = input.arguments;
    if args.len() == 1 {
        args.push(lit(ScalarValue::Time64Microsecond(Some(0))));
    }
    match input.function_context.plan_config.default_timestamp_type {
        DefaultTimestampType::TimestampLtz => make_timestamp_ltz(args, session_tz, is_try),
        DefaultTimestampType::TimestampNtz => {
            if args.len() == 3 || args.len() == 7 {
                args.pop();
            }
            make_timestamp_ntz(args, is_try)
        }
    }
}

fn date_part(part: Expr, date: Expr) -> Expr {
    ScalarUDF::from(SparkDatePart::new()).call(vec![part, date])
}

fn months_between(input: ScalarFunctionInput) -> PlanResult<Expr> {
    // args extraction:
    let ScalarFunctionInput {
        mut arguments,
        function_context,
    } = input;
    let round_off = (arguments.len() == 3)
        .then(|| arguments.pop())
        .flatten()
        .unwrap_or(lit(true));
    let (date1, date2) = arguments.two()?;

    // consts:
    let seconds_per_day: i64 = 24 * 60 * 60;
    let seconds_in_month = cast(lit(31 * seconds_per_day), DataType::Float64);

    // helper functions:
    let ensure_timestamp = |dt: Expr| match dt.get_type(function_context.schema) {
        Ok(DataType::Timestamp(time_unit, _tz)) => (dt.clone(), time_unit),
        _ => (
            cast(dt.clone(), DataType::Timestamp(TimeUnit::Microsecond, None)),
            TimeUnit::Microsecond,
        ),
    };

    let date_to_months =
        |dt: Expr| integer_part(dt.clone(), "YEAR") * lit(12) + integer_part(dt, "MONTH");

    let is_last_day = |dt: Expr| {
        ScalarUDF::from(SparkLastDay::new())
            .call(vec![cast(dt.clone(), DataType::Date32)])
            .eq(cast(dt, DataType::Date32))
    };

    let seconds_in_day = |dt: Expr, tu: TimeUnit| {
        (cast(dt.clone(), DataType::Int64)
            - cast(expr_fn::date_trunc(lit("DAY"), dt), DataType::Int64))
            / lit(time_unit_to_multiplier(&tu))
    };

    // prerequisites
    let (date1, tu1) = ensure_timestamp(date1.clone());
    let (date2, tu2) = ensure_timestamp(date2.clone());

    // calculations:
    let days1 = integer_part(date1.clone(), "DAY");
    let days2 = integer_part(date2.clone(), "DAY");

    let month_diff = cast(
        date_to_months(date1.clone()) - date_to_months(date2.clone()),
        DataType::Float64,
    );

    let seconds_diff = (days1.clone() - days2.clone()) * lit(seconds_per_day)
        + seconds_in_day(date1.clone(), tu1)
        - seconds_in_day(date2.clone(), tu2);

    let months_between = when(
        days1
            .eq(days2)
            .or(is_last_day(date1).and(is_last_day(date2))),
        month_diff.clone(),
    )
    .when(lit(true), month_diff + seconds_diff / seconds_in_month)
    .end()?;

    Ok(when(
        round_off,
        expr_fn::round(vec![months_between.clone(), lit(8)]),
    )
    .when(lit(true), months_between)
    .end()?)
}

const MICROS_PER_DAY: i64 = 24 * 60 * 60 * 1_000_000;

/// Parses a `window` duration/start-time argument (interval string, day-time
/// interval, or integer number of microseconds, matching Spark's
/// `TimeWindow.parseExpression`) into microseconds. Months/years are rejected
/// (non-constant length).
fn window_interval_micros(expr: &Expr) -> PlanResult<i64> {
    let Expr::Literal(value, _) = expr else {
        return Err(PlanError::invalid(
            "window durations and start time must be literal strings, intervals, or integers",
        ));
    };
    if let Some(s) = value.try_as_str().flatten() {
        return match parse_interval(s)
            .map_err(|e| PlanError::invalid(format!("invalid window interval {s:?}: {e}")))?
        {
            IntervalValue::Microsecond { microseconds } => Ok(microseconds),
            _ => Err(PlanError::invalid(format!(
                "window interval must not contain months or years: {s:?}"
            ))),
        };
    }
    match value {
        // Spark interprets integer literals as microseconds.
        ScalarValue::Int32(Some(v)) => Ok(*v as i64),
        ScalarValue::Int64(Some(v)) => Ok(*v),
        ScalarValue::DurationMicrosecond(Some(v)) => Ok(*v),
        ScalarValue::DurationMillisecond(Some(v)) => Ok(*v * 1_000),
        ScalarValue::DurationSecond(Some(v)) => Ok(*v * 1_000_000),
        ScalarValue::DurationNanosecond(Some(v)) => Ok(*v / 1_000),
        ScalarValue::IntervalDayTime(Some(v)) => {
            Ok(v.days as i64 * MICROS_PER_DAY + v.milliseconds as i64 * 1_000)
        }
        ScalarValue::IntervalMonthDayNano(Some(v)) if v.months == 0 => {
            Ok(v.days as i64 * MICROS_PER_DAY + v.nanoseconds / 1_000)
        }
        _ => Err(PlanError::invalid(
            "window durations and start time must be literal strings, day-time intervals, or integers",
        )),
    }
}

/// The parsed durations (in microseconds) of a Spark `window` call.
#[derive(Debug, Clone, Copy)]
struct WindowSpec {
    window_duration: i64,
    slide_duration: i64,
    start_time: i64,
}

/// Bound on `ceil(windowDuration / slideDuration)`
const MAX_OVERLAPPING_WINDOWS: i64 = 1_000_000;

/// Parses and validates the `window` durations from the full argument list
/// (`args[0]` is the time column; `args[1..]` are window/slide/start).
fn parse_window_spec(args: &[Expr]) -> PlanResult<WindowSpec> {
    if !(2..=4).contains(&args.len()) {
        return Err(PlanError::invalid(format!(
            "window requires 2 to 4 arguments, got {}",
            args.len()
        )));
    }
    let window_duration = window_interval_micros(&args[1])?;
    let slide_duration = match args.get(2) {
        Some(arg) => window_interval_micros(arg)?,
        None => window_duration,
    };
    let start_time = match args.get(3) {
        Some(arg) => window_interval_micros(arg)?,
        None => 0,
    };
    if window_duration <= 0 {
        return Err(PlanError::invalid(
            "window: the window duration must be greater than 0",
        ));
    }
    if slide_duration <= 0 {
        return Err(PlanError::invalid(
            "window: the slide duration must be greater than 0",
        ));
    }
    if slide_duration > window_duration {
        return Err(PlanError::invalid(
            "window: the slide duration must be less than or equal to the window duration",
        ));
    }
    if start_time >= slide_duration || start_time <= -slide_duration {
        return Err(PlanError::invalid(format!(
            "The `abs(start_time)`({start_time}L) must be < the `slide_duration`({slide_duration}L)."
        )));
    }
    let overlapping = (window_duration + slide_duration - 1) / slide_duration;
    if overlapping > MAX_OVERLAPPING_WINDOWS {
        return Err(PlanError::invalid(format!(
            "window: ceil(windowDuration / slideDuration) = {overlapping} exceeds the limit of {MAX_OVERLAPPING_WINDOWS}"
        )));
    }
    Ok(WindowSpec {
        window_duration,
        slide_duration,
        start_time,
    })
}

/// The `window` struct field type: a microsecond timestamp (non-timestamp inputs
/// are cast, matching Spark's cast of the time column to `TimestampType`).
fn window_field_type(
    time_type: &DataType,
    session_tz: &std::sync::Arc<str>,
) -> PlanResult<DataType> {
    Ok(match time_type {
        DataType::Timestamp(_, tz) => DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
        // Spark casts dates and strings to `TimestampType` (session time zone), so a
        // date becomes midnight in the session time zone, not a naive timestamp.
        DataType::Date32
        | DataType::Date64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View => {
            DataType::Timestamp(TimeUnit::Microsecond, Some(session_tz.clone()))
        }
        other => {
            return Err(PlanError::invalid(format!(
                "window requires a timestamp time column, got {other:?}"
            )));
        }
    })
}

/// The Spark `window` time function: buckets a timestamp into `struct<start, end>`
/// windows. The candidate enumeration is deferred to the `SparkWindowBuckets` UDF
/// so the plan stays bounded regardless of the `window/slide` ratio.
fn window(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let schema = input.function_context.schema;
    let args = input.arguments;
    let spec = parse_window_spec(&args)?;
    let session_tz = input.function_context.plan_config.session_timezone.clone();
    let time = args
        .into_iter()
        .next()
        .ok_or_else(|| PlanError::internal("window missing time column"))?;
    let field_type = window_field_type(&time.get_type(schema)?, &session_tz)?;
    let time_ts = cast(time, field_type);
    let buckets = ScalarUDF::from(SparkWindowBuckets::new(
        spec.window_duration,
        spec.slide_duration,
        spec.start_time,
    ))
    .call(vec![time_ts]);
    Ok(ScalarUDF::from(Explode::new(ExplodeKind::Explode)).call(vec![buckets]))
}

/// The Spark `window_time` function: the event-time of a time window, defined as
/// `window.end - 1 microsecond`. Spark validates the argument via column metadata
/// markers; we approximate that with a structural check on the window struct type.
fn window_time(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let schema = input.function_context.schema;
    let arg = input.arguments.one()?;
    let end_type = match arg.get_type(schema)? {
        DataType::Struct(fields)
            if fields.len() == 2
                && fields[0].name() == "start"
                && fields[1].name() == "end"
                && matches!(
                    fields[0].data_type(),
                    DataType::Timestamp(TimeUnit::Microsecond, _)
                )
                && matches!(
                    fields[1].data_type(),
                    DataType::Timestamp(TimeUnit::Microsecond, _)
                ) =>
        {
            fields[1].data_type().clone()
        }
        other => {
            return Err(PlanError::invalid(format!(
                "window_time requires a window column (struct with start and end timestamps), got {other:?}"
            )));
        }
    };
    Ok(cast(
        cast(arg.field("end"), DataType::Int64) - lit(1_i64),
        end_type,
    ))
}

pub(super) fn list_built_in_datetime_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        (
            "add_years",
            F::custom(|input| interval_arithmetic(input, "years", Operator::Plus)),
        ),
        (
            "add_months",
            F::custom(|input| interval_arithmetic(input, "months", Operator::Plus)),
        ),
        (
            "add_days",
            F::custom(|input| interval_arithmetic(input, "days", Operator::Plus)),
        ),
        ("convert_timezone", F::custom(convert_timezone)),
        ("curdate", F::nullary(expr_fn::current_date)),
        ("current_date", F::nullary(expr_fn::current_date)),
        ("current_time", F::nullary(expr_fn::current_time)),
        (
            "current_timestamp",
            F::custom(current_timestamp_microseconds),
        ),
        ("current_timezone", F::custom(current_timezone)),
        (
            "date_add",
            F::custom(|input| interval_arithmetic(input, "days", Operator::Plus)),
        ),
        ("date_diff", F::custom(datediff)),
        ("date_format", F::binary(date_format)),
        ("date_from_unix_date", F::cast(DataType::Date32)),
        ("date_part", F::binary(date_part)),
        (
            "date_sub",
            F::custom(|input| interval_arithmetic(input, "days", Operator::Minus)),
        ),
        ("date_trunc", F::custom(date_trunc)),
        (
            "dateadd",
            F::custom(|input| interval_arithmetic(input, "days", Operator::Plus)),
        ),
        ("datediff", F::custom(datediff)),
        ("datepart", F::binary(date_part)),
        ("day", F::unary(|arg| integer_part(arg, "DAY"))),
        ("dayname", F::unary(|arg| expr_fn::to_char(arg, lit("%a")))),
        ("dayofmonth", F::unary(|arg| integer_part(arg, "DAY"))),
        (
            "dayofweek",
            F::unary(|arg| integer_part(arg, "DOW") + lit(1)),
        ),
        ("dayofyear", F::unary(|arg| integer_part(arg, "DOY"))),
        ("extract", F::binary(date_part)),
        ("from_unixtime", F::custom(from_unixtime)),
        ("from_utc_timestamp", F::custom(from_utc_timestamp)),
        ("hour", F::unary(|arg| integer_part(arg, "HOUR"))),
        ("last_day", F::udf(SparkLastDay::new())),
        (
            "localtimestamp",
            F::custom(current_localtimestamp_microseconds),
        ),
        ("make_date", F::ternary(make_date)),
        ("make_dt_interval", F::udf(SparkMakeDtInterval::new())),
        ("make_interval", F::udf(SparkMakeInterval::new())),
        ("make_time", F::udf(SparkMakeTime::new())),
        (
            "make_timestamp",
            F::custom(|input| make_timestamp(input, false)),
        ),
        (
            "make_timestamp_ltz",
            F::custom(|input| {
                make_timestamp_ltz(
                    input.arguments,
                    &input.function_context.plan_config.session_timezone,
                    false,
                )
            }),
        ),
        (
            "make_timestamp_ntz",
            F::custom(|input| make_timestamp_ntz(input.arguments, false)),
        ),
        ("make_ym_interval", F::udf(SparkMakeYmInterval::new())),
        ("minute", F::unary(|arg| integer_part(arg, "MINUTE"))),
        ("month", F::unary(|arg| integer_part(arg, "MONTH"))),
        (
            "monthname",
            F::unary(|arg| expr_fn::to_char(arg, lit("%b"))),
        ),
        ("months_between", F::custom(months_between)),
        ("next_day", F::custom(next_day)),
        ("now", F::custom(current_timestamp_microseconds)),
        ("quarter", F::unary(|arg| integer_part(arg, "QUARTER"))),
        ("second", F::unary(|arg| integer_part(arg, "SECOND"))),
        ("session_window", F::unknown("session_window")),
        (
            "timestamp_micros",
            F::cast(DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into()),
            )),
        ),
        (
            "timestamp_millis",
            F::unary(|arg| {
                cast(
                    cast(arg, DataType::Int64) * lit(1_000_i64),
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                )
            }),
        ),
        (
            "timestamp_seconds",
            F::unary(|arg| {
                cast(
                    cast(arg, DataType::Int64) * lit(1_000_000_i64),
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                )
            }),
        ),
        ("timestampadd", F::custom(timestampadd)),
        ("timestamp_add", F::custom(timestampadd)),
        ("timestampdiff", F::custom(datediff)),
        ("timestamp_diff", F::custom(datediff)),
        ("to_date", F::custom(to_date)),
        ("to_time", F::custom(to_time)),
        ("try_to_time", F::custom(try_to_time)),
        (
            "to_timestamp",
            F::custom(|input| {
                let timestamp_ntz = matches!(
                    input.function_context.plan_config.default_timestamp_type,
                    DefaultTimestampType::TimestampNtz
                );
                to_timestamp(input, timestamp_ntz)
            }),
        ),
        // The description for `to_timestamp_ltz` and `to_timestamp_ntz` are the same:
        //  "Parses the timestamp with the format to a timestamp without time zone. Returns null with invalid input."
        // https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_timestamp_ltz.html
        // https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_timestamp_ntz.html
        (
            "to_timestamp_ltz",
            F::custom(|input| to_timestamp(input, false)),
        ),
        (
            "to_timestamp_ntz",
            F::custom(|input| to_timestamp(input, true)),
        ),
        ("to_unix_timestamp", F::custom(to_unix_timestamp)),
        ("to_utc_timestamp", F::custom(to_utc_timestamp)),
        ("trunc", F::custom(trunc)),
        ("try_make_interval", F::unknown("try_make_interval")),
        (
            "try_make_timestamp",
            F::custom(|input| make_timestamp(input, true)),
        ),
        (
            "try_make_timestamp_ltz",
            F::custom(|input| {
                make_timestamp_ltz(
                    input.arguments,
                    &input.function_context.plan_config.session_timezone,
                    true,
                )
            }),
        ),
        (
            "try_make_timestamp_ntz",
            F::custom(|input| make_timestamp_ntz(input.arguments, true)),
        ),
        (
            "try_to_timestamp",
            F::custom(|input| {
                let timestamp_ntz = matches!(
                    input.function_context.plan_config.default_timestamp_type,
                    DefaultTimestampType::TimestampNtz
                );
                try_to_timestamp(input, timestamp_ntz)
            }),
        ),
        ("time_diff", F::udf(SparkTimeDiff::new())),
        ("time_trunc", F::udf(SparkTimeTrunc::new())),
        (
            "unix_date",
            F::unary(|arg| cast(cast(arg, DataType::Date32), DataType::Int32)),
        ),
        (
            "unix_micros",
            F::custom(|input| unix_time_unit(input, TimeUnit::Microsecond)),
        ),
        (
            "unix_millis",
            F::custom(|input| unix_time_unit(input, TimeUnit::Millisecond)),
        ),
        (
            "unix_seconds",
            F::custom(|input| unix_time_unit(input, TimeUnit::Second)),
        ),
        ("unix_timestamp", F::custom(unix_timestamp)),
        ("weekday", F::unary(|arg| integer_part(arg, "DOW") - lit(1))),
        (
            "weekofyear",
            F::unary(|arg| cast(expr_fn::to_char(arg, lit("%V")), DataType::Int32)),
        ),
        ("window", F::custom(window)),
        ("window_time", F::custom(window_time)),
        ("year", F::udf(SparkYear::new())),
        ("years", F::unary(years)),
    ]
}
