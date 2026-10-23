//! Query-analysis policy, shared by native resolution and kernel field validation.

use std::sync::Arc;

use arrow_schema::{DataType, Field, IntervalUnit};
use datafusion_common::{DataFusionError, Result};

pub fn coerce_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if arg_types.len() != 2 {
        return Err(DataFusionError::Plan(format!(
            "percentile_disc expects 2 arguments, got {}",
            arg_types.len()
        )));
    }
    // arg[0]: ORDER BY column. Spark accepts every numeric type and STRING
    // (cast-to-double semantics, always returns `double`) and ANSI day-time
    // intervals (returns the interval type). Numerics route through `Float64`;
    // STRING and INTERVAL/DURATION are left UNCHANGED so the accumulator can
    // handle them (string→double cast with the ANSI flag; intervals stored as
    // `i64` and reconstructed to the original type).
    let order_by = match &arg_types[0] {
        dt if dt.is_numeric() => DataType::Float64,
        // Leave STRING types UNCHANGED so the planner inserts no cast. The
        // string→double cast is performed inside the accumulator with a
        // safe/strict flag driven by ANSI mode: under ANSI a non-numeric
        // string errors, otherwise it becomes NULL and is ignored. Forcing
        // `Float64` here would make DataFusion insert a strict planning-time
        // cast that fails before the accumulator runs.
        dt @ (DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) => dt.clone(),
        // Calendar intervals (month-day-nano, e.g. `make_interval`) are NOT
        // orderable in Spark — reject them like Spark's INVALID_ORDERING_TYPE.
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            return Err(DataFusionError::Plan(
                "percentile_disc: calendar INTERVAL (month-day-nano) is not orderable; \
                 use an ANSI day-time interval"
                    .into(),
            ));
        }
        // Keep ANSI day-time/year-month INTERVAL and DURATION so the discrete
        // value (and its type) survives to the accumulator; Spark returns the
        // same interval type.
        dt @ (DataType::Interval(_) | DataType::Duration(_)) => dt.clone(),
        other => {
            return Err(DataFusionError::Plan(format!(
                "percentile_disc: ORDER BY column must be numeric, string, or interval, got {other}"
            )));
        }
    };
    // arg[1]: percentile literal — either a single value (scalar form) or
    // a `List`/`LargeList`/`FixedSizeList` (array form). Coerce to
    // `Float64` / `List<Float64>` respectively. The accepted-type
    // matrix mirrors what Spark validates at planning time:
    //
    //                       scalar       array element
    //   numeric              ✓            ✓
    //   STRING (Utf8 etc.)   ✓ (lazy)     ✗
    //   `Null` (untyped)     ✗            ✓ (empty `array()`)
    //   bool/date/timestamp/binary/etc. ✗  ✗
    //
    // STRING is permitted only for the scalar form because Spark accepts
    // it via implicit cast there but rejects `array<string>` at planning.
    // `Null` element is permitted for the array form because that is the
    // type of an empty `array()` literal (and of `array(NULL)`, which
    // Spark also accepts treating NULL as `0.0`).
    let percentile = match &arg_types[1] {
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            let elem = field.data_type();
            if !elem.is_numeric() && !matches!(elem, DataType::Null) {
                return Err(DataFusionError::Plan(format!(
                    "percentile_disc: percentile array elements must be numeric, got {elem}"
                )));
            }
            DataType::List(Arc::new(Field::new("item", DataType::Float64, true)))
        }
        dt if dt.is_numeric() => DataType::Float64,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DataType::Float64,
        other => {
            return Err(DataFusionError::Plan(format!(
                "percentile_disc: percentile must be numeric, got {other}"
            )));
        }
    };
    Ok(vec![order_by, percentile])
}

pub fn return_type(arg_types: &[DataType]) -> Result<DataType> {
    if matches!(arg_types.get(1), Some(DataType::List(_))) {
        // INTERVAL/DURATION inputs yield `array<interval>`; everything else
        // yields `array<double>`.
        let item = if matches!(arg_types[0], DataType::Interval(_) | DataType::Duration(_)) {
            arg_types[0].clone()
        } else {
            DataType::Float64
        };
        return Ok(DataType::List(Arc::new(Field::new("item", item, true))));
    }
    // STRING ORDER BY columns are left unchanged by `coerce_types` (no
    // planning-time cast), but Spark's `percentile_disc` always returns
    // `double` for string input, so report `Float64` here.
    if matches!(
        arg_types[0],
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) {
        return Ok(DataType::Float64);
    }
    Ok(arg_types[0].clone())
}
