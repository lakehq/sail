use std::fmt::{Debug, Formatter};
use std::mem::{size_of, size_of_val};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, ArrowNumericType, AsArray, Float64Array, Int64Array, ListArray, PrimitiveArray,
};
use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::{
    DataType, Decimal128Type, Decimal256Type, Field, FieldRef, Float16Type, Float32Type,
    Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, IntervalUnit, UInt16Type, UInt32Type,
    UInt64Type, UInt8Type,
};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, GroupsAccumulator, Signature, Volatility,
};

use crate::aggregate::percentile_disc_groups::PercentileDiscGroupsAccumulator;
use crate::aggregate::utils::{
    calculate_percentile_disc, cast_to_type_with_safe, extract_percentile_literal,
    extract_percentiles_array, percentile_disc_index,
};

macro_rules! dispatch_numeric_type {
    ($input_dt:expr, $helper:ident, $err_msg:expr) => {
        match &$input_dt {
            DataType::Int8 => $helper!(Int8Type, $input_dt),
            DataType::Int16 => $helper!(Int16Type, $input_dt),
            DataType::Int32 => $helper!(Int32Type, $input_dt),
            DataType::Int64 => $helper!(Int64Type, $input_dt),
            DataType::UInt8 => $helper!(UInt8Type, $input_dt),
            DataType::UInt16 => $helper!(UInt16Type, $input_dt),
            DataType::UInt32 => $helper!(UInt32Type, $input_dt),
            DataType::UInt64 => $helper!(UInt64Type, $input_dt),
            DataType::Float16 => $helper!(Float16Type, $input_dt),
            DataType::Float32 => $helper!(Float32Type, $input_dt),
            DataType::Float64 => $helper!(Float64Type, $input_dt),
            DataType::Decimal128(_, _) => $helper!(Decimal128Type, $input_dt),
            DataType::Decimal256(_, _) => $helper!(Decimal256Type, $input_dt),
            _ => Err(DataFusionError::NotImplemented(format!(
                "{} not supported for {}",
                $err_msg, $input_dt,
            ))),
        }
    };
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PercentileDisc {
    signature: Signature,
    /// When `true`, casting a non-numeric STRING ORDER BY column to `f64` is
    /// strict (invalid values raise an error). When `false`, the cast is safe
    /// (invalid values become NULL and are ignored). Mirrors
    /// `spark.sql.ansi.enabled`.
    ansi_mode: bool,
}

impl Default for PercentileDisc {
    fn default() -> Self {
        Self::new(false)
    }
}

impl PercentileDisc {
    pub fn new(ansi_mode: bool) -> Self {
        // `user_defined` so the planner calls our `coerce_types` (see there).
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            ansi_mode,
        }
    }

    /// `true` when running under `spark.sql.ansi.enabled = true`.
    pub fn ansi_mode(&self) -> bool {
        self.ansi_mode
    }

    /// `true` when the (single) `ORDER BY` direction is `DESC`.
    fn is_descending(args: &AccumulatorArgs) -> bool {
        args.order_bys
            .first()
            .map(|sort_expr| sort_expr.options.descending)
            .unwrap_or(false)
    }

    /// Spark requires exactly one `ORDER BY` expression inside `WITHIN GROUP`
    /// (`INVALID_WITHIN_GROUP_EXPRESSION.WRONG_NUM_ORDERINGS`). The discrete
    /// percentile is defined over a single ordered dimension, so reject anything
    /// else rather than silently using only the first expression.
    fn ensure_single_order_by(args: &AccumulatorArgs) -> Result<()> {
        if args.order_bys.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "percentile_disc requires exactly one ORDER BY expression within \
                 the WITHIN GROUP clause, got {}",
                args.order_bys.len()
            )));
        }
        Ok(())
    }

    /// Extract the scalar percentile literal. We do NOT invert for `DESC` here:
    /// `percentile_disc`'s discrete index is asymmetric, so the descending
    /// adjustment is applied inside `calculate_percentile_disc` instead.
    fn resolve_percentile(args: &AccumulatorArgs) -> Result<f64> {
        extract_percentile_literal(&args.exprs[1])
    }

    /// Returns `Some(percentiles)` if `args.exprs[1]` is a list literal of
    /// percentile values, `None` if it's a scalar.
    fn try_resolve_percentiles_array(args: &AccumulatorArgs) -> Result<Option<Vec<f64>>> {
        // `coerce_types` normalizes any list flavor to `List(Float64)`, so we
        // only need to check for `List`.
        if !matches!(args.exprs[1].data_type(args.schema)?, DataType::List(_)) {
            return Ok(None);
        }
        let percentiles = extract_percentiles_array(&args.exprs[1])?;
        Ok(Some(percentiles))
    }
}

impl AggregateUDFImpl for PercentileDisc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "percentile_disc"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn supports_within_group_clause(&self) -> bool {
        // `percentile_disc` is an ordered-set aggregate (`WITHIN GROUP`).
        // Tell DataFusion so it can format the schema name correctly
        // (`WITHIN GROUP (ORDER BY ...)` instead of `ORDER BY ...`) and skip
        // the duplicated leading arg when rendering.
        true
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
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
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _) => {
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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
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

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let is_array_percentiles = matches!(
            args.input_fields.get(1).map(|f| f.data_type()),
            Some(DataType::List(_))
        );
        let input_type = args.input_fields[0].data_type();
        let value_type = if matches!(input_type, DataType::Interval(_) | DataType::Duration(_)) {
            // Intervals are stored as their `i64` representation (scalar and array forms).
            DataType::Int64
        } else if is_array_percentiles {
            DataType::Float64
        } else if matches!(
            input_type,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ) {
            // STRING input is left uncoerced at plan time but stored as `f64`
            // inside the accumulator.
            DataType::Float64
        } else {
            input_type.clone()
        };
        let field = Field::new_list_field(value_type, true);
        let state_name = if args.is_distinct {
            "distinct_percentile_disc"
        } else {
            "percentile_disc"
        };

        Ok(vec![Field::new(
            format_state_name(args.name, state_name),
            DataType::List(Arc::new(field)),
            true,
        )
        .into()])
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        // Spark rejects `DISTINCT` on `WITHIN GROUP` aggregates with
        // `INVALID_WITHIN_GROUP_EXPRESSION.DISTINCT_UNSUPPORTED`. The DISTINCT
        // keyword is meaningless here (the function's value is a position in
        // the sorted population, not a count of distinct elements) — surface a
        // matching error rather than silently de-duplicate the input.
        if args.is_distinct {
            return Err(DataFusionError::Plan(
                "percentile_disc does not support DISTINCT with WITHIN GROUP".into(),
            ));
        }
        Self::ensure_single_order_by(&args)?;

        let ansi_mode = self.ansi_mode;

        // Array-of-percentiles form: `percentile_disc(array(p1, p2, ...))`.
        if let Some(percentiles) = Self::try_resolve_percentiles_array(&args)? {
            let descending = Self::is_descending(&args);
            let input_dt = args.exprs[0].data_type(args.schema)?;
            // INTERVAL/DURATION array form returns `array<interval>`, not doubles.
            if matches!(input_dt, DataType::Interval(_) | DataType::Duration(_)) {
                return Ok(Box::new(MultiIntervalPercentileDiscAccumulator {
                    all_values: vec![],
                    percentiles,
                    descending,
                    data_type: input_dt,
                }));
            }
            return Ok(Box::new(MultiPercentileDiscAccumulator {
                all_values: vec![],
                percentiles,
                descending,
                ansi_mode,
            }));
        }

        // Scalar percentile form: `percentile_disc(p)`.
        let percentile = Self::resolve_percentile(&args)?;
        let descending = Self::is_descending(&args);

        // DISTINCT was rejected above, so there's only one accumulator shape.
        macro_rules! helper {
            ($t:ty, $dt:expr) => {
                Ok(Box::new(PercentileDiscAccumulator::<$t> {
                    data_type: $dt.clone(),
                    all_values: vec![],
                    percentile,
                    descending,
                    ansi_mode,
                }))
            };
        }

        let input_dt = args.exprs[0].data_type(args.schema)?;
        // STRING input is stored as `f64`; the string→double cast happens in
        // `update_batch` with a safe/strict flag driven by ANSI mode.
        if matches!(
            input_dt,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ) {
            return Ok(Box::new(PercentileDiscAccumulator::<Float64Type> {
                data_type: DataType::Float64,
                all_values: vec![],
                percentile,
                descending,
                ansi_mode,
            }));
        }
        // INTERVAL/DURATION input: store the `i64` representation and reconstruct
        // the interval type on output (Spark returns the interval type).
        if matches!(input_dt, DataType::Interval(_) | DataType::Duration(_)) {
            return Ok(Box::new(IntervalPercentileDiscAccumulator {
                all_values: vec![],
                percentile,
                descending,
                data_type: input_dt.clone(),
            }));
        }
        dispatch_numeric_type!(input_dt, helper, "PercentileDiscAccumulator")
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        if args.is_distinct {
            return false;
        }
        // Array-of-percentiles and INTERVAL/DURATION inputs are not supported by
        // the groups accumulator fast path; fall back to the regular per-group
        // accumulator above (which handles both).
        let arr_form = args
            .exprs
            .get(1)
            .and_then(|e| e.data_type(args.schema).ok())
            .map(|dt| matches!(dt, DataType::List(_)))
            .unwrap_or(false);
        let interval_input = args
            .exprs
            .first()
            .and_then(|e| e.data_type(args.schema).ok())
            .map(|dt| matches!(dt, DataType::Interval(_) | DataType::Duration(_)))
            .unwrap_or(false);
        !arr_form && !interval_input
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Self::ensure_single_order_by(&args)?;
        let percentile = Self::resolve_percentile(&args)?;
        let descending = Self::is_descending(&args);
        let ansi_mode = self.ansi_mode;

        macro_rules! helper {
            ($t:ty, $dt:expr) => {
                Ok(Box::new(PercentileDiscGroupsAccumulator::<$t>::new(
                    $dt, percentile, descending, ansi_mode,
                )))
            };
        }

        let input_dt = args.exprs[0].data_type(args.schema)?;
        // STRING input is stored as `f64`; the string→double cast happens in
        // `update_batch` with a safe/strict flag driven by ANSI mode.
        if matches!(
            input_dt,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ) {
            return Ok(Box::new(
                PercentileDiscGroupsAccumulator::<Float64Type>::new(
                    DataType::Float64,
                    percentile,
                    descending,
                    ansi_mode,
                ),
            ));
        }
        dispatch_numeric_type!(input_dt, helper, "PercentileDiscGroupsAccumulator")
    }
}

struct PercentileDiscAccumulator<T: ArrowNumericType> {
    data_type: DataType,
    all_values: Vec<T::Native>,
    percentile: f64,
    descending: bool,
    /// When `true`, the input→storage cast is strict (invalid string values
    /// error). When `false`, it is safe (invalid values become NULL and are
    /// ignored). Mirrors `spark.sql.ansi.enabled`.
    ansi_mode: bool,
}

impl<T: ArrowNumericType> Debug for PercentileDiscAccumulator<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PercentileDiscAccumulator({}, percentile={}, descending={})",
            self.data_type, self.percentile, self.descending
        )
    }
}

impl<T: ArrowNumericType> Accumulator for PercentileDiscAccumulator<T> {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, self.all_values.len() as i32]));

        let values_array = PrimitiveArray::<T>::new(
            ScalarBuffer::from(std::mem::take(&mut self.all_values)),
            None,
        )
        .with_data_type(self.data_type.clone());

        let list_array = ListArray::new(
            Arc::new(Field::new_list_field(self.data_type.clone(), true)),
            offsets,
            Arc::new(values_array),
            None,
        );

        Ok(vec![ScalarValue::List(Arc::new(list_array))])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values_array = cast_to_type_with_safe(&values[0], &self.data_type, !self.ansi_mode)?;

        let null_count = values_array.null_count();
        let values = values_array.as_primitive::<T>();
        self.all_values.reserve(values.len() - null_count);
        self.all_values.extend(values.iter().flatten());
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let array = states[0].as_list::<i32>();
        for v in array.iter().flatten() {
            self.update_batch(&[v])?
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let d = std::mem::take(&mut self.all_values);
        let value = calculate_percentile_disc::<T>(d, self.percentile, self.descending);
        ScalarValue::new_primitive::<T>(value, &self.data_type)
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.all_values.capacity() * size_of::<T::Native>()
    }
}

/// Accumulator for the array-of-percentiles form: stores values as `f64`
/// and computes each percentile from the same buffer.
///
/// Output is `List<Float64>` matching Spark's `array<double>` return type.
struct MultiPercentileDiscAccumulator {
    all_values: Vec<f64>,
    percentiles: Vec<f64>,
    descending: bool,
    /// When `true`, the input→`f64` cast is strict (invalid string values
    /// error). When `false`, it is safe (invalid values become NULL and are
    /// ignored). Mirrors `spark.sql.ansi.enabled`.
    ansi_mode: bool,
}

impl Debug for MultiPercentileDiscAccumulator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MultiPercentileDiscAccumulator(percentiles={:?}, len={}, descending={})",
            self.percentiles,
            self.all_values.len(),
            self.descending
        )
    }
}

impl Accumulator for MultiPercentileDiscAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![
            0_i32,
            self.all_values.len() as i32,
        ]));
        let values_array = Float64Array::new(
            ScalarBuffer::from(std::mem::take(&mut self.all_values)),
            None,
        );
        let list_array = ListArray::new(
            Arc::new(Field::new_list_field(DataType::Float64, true)),
            offsets,
            Arc::new(values_array),
            None,
        );
        Ok(vec![ScalarValue::List(Arc::new(list_array))])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values_array = cast_to_type_with_safe(&values[0], &DataType::Float64, !self.ansi_mode)?;
        let null_count = values_array.null_count();
        let values = values_array.as_primitive::<Float64Type>();
        self.all_values.reserve(values.len() - null_count);
        self.all_values.extend(values.iter().flatten());
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let array = states[0].as_list::<i32>();
        for v in array.iter().flatten() {
            self.update_batch(&[v])?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // When the input is empty OR the percentile array is empty (e.g.
        // callers passed `array()`), return a NULL list — matches Spark, which
        // collapses `percentile_disc(array()) WITHIN GROUP (ORDER BY x)` to
        // NULL rather than an empty `array<double>`.
        if self.all_values.is_empty() || self.percentiles.is_empty() {
            let field = Arc::new(Field::new_list_field(DataType::Float64, true));
            return Ok(ScalarValue::List(Arc::new(ListArray::new_null(field, 1))));
        }
        // Sort once (O(n log n)) and index into the sorted slice for each
        // percentile (O(1) per pick). This avoids cloning the full value
        // buffer for every requested percentile, which was O(k·n) for `k`
        // percentiles and `n` values. f64 uses total_cmp to keep NaN ordering
        // consistent with `calculate_percentile_disc`'s ArrowNativeTypeOp.
        let mut sorted = std::mem::take(&mut self.all_values);
        sorted.sort_unstable_by(|a, b| a.total_cmp(b));
        let len = sorted.len();
        let results: Vec<Option<f64>> = self
            .percentiles
            .iter()
            .map(|&p| Some(sorted[percentile_disc_index(len, p, self.descending)]))
            .collect();
        let values_array = Float64Array::from_iter(results);
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, values_array.len() as i32]));
        let list_array = ListArray::new(
            Arc::new(Field::new_list_field(DataType::Float64, true)),
            offsets,
            Arc::new(values_array),
            None,
        );
        Ok(ScalarValue::List(Arc::new(list_array)))
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + self.all_values.capacity() * size_of::<f64>()
            + self.percentiles.capacity() * size_of::<f64>()
    }
}

/// Accumulator for ANSI interval / duration ORDER BY columns. Each value is
/// stored as its `i64` representation; on output we pick the discrete percentile
/// and reconstruct the original interval/duration type. Mirrors `percentile.rs`'s
/// interval handling but selects discretely (no interpolation).
struct IntervalPercentileDiscAccumulator {
    all_values: Vec<i64>,
    percentile: f64,
    descending: bool,
    data_type: DataType,
}

impl Debug for IntervalPercentileDiscAccumulator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "IntervalPercentileDiscAccumulator({}, percentile={}, descending={})",
            self.data_type, self.percentile, self.descending
        )
    }
}

/// Convert one interval/duration array element to its scalar `i64` representation.
fn interval_to_i64(data_type: &DataType, array: &ArrayRef, row: usize) -> Result<i64> {
    use datafusion::arrow::datatypes::{IntervalUnit, TimeUnit};
    let v = match data_type {
        DataType::Interval(IntervalUnit::YearMonth) => array
            .as_primitive::<datafusion::arrow::datatypes::IntervalYearMonthType>()
            .value(row) as i64,
        DataType::Interval(IntervalUnit::DayTime) => {
            let val = array
                .as_primitive::<datafusion::arrow::datatypes::IntervalDayTimeType>()
                .value(row);
            val.days as i64 * 86_400_000 + val.milliseconds as i64
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let val = array
                .as_primitive::<datafusion::arrow::datatypes::IntervalMonthDayNanoType>()
                .value(row);
            val.months as i64 * 2_592_000_000_000_000
                + val.days as i64 * 86_400_000_000_000
                + val.nanoseconds
        }
        DataType::Duration(TimeUnit::Second) => array
            .as_primitive::<datafusion::arrow::datatypes::DurationSecondType>()
            .value(row),
        DataType::Duration(TimeUnit::Millisecond) => array
            .as_primitive::<datafusion::arrow::datatypes::DurationMillisecondType>()
            .value(row),
        DataType::Duration(TimeUnit::Microsecond) => array
            .as_primitive::<datafusion::arrow::datatypes::DurationMicrosecondType>()
            .value(row),
        DataType::Duration(TimeUnit::Nanosecond) => array
            .as_primitive::<datafusion::arrow::datatypes::DurationNanosecondType>()
            .value(row),
        other => {
            return Err(DataFusionError::Internal(format!(
                "percentile_disc interval accumulator does not support {other}"
            )));
        }
    };
    Ok(v)
}

/// Reconstruct a `ScalarValue` of `data_type` from an `i64`, or a typed NULL.
fn i64_to_interval_scalar(data_type: &DataType, value: Option<i64>) -> Result<ScalarValue> {
    use datafusion::arrow::datatypes::{
        IntervalDayTime, IntervalMonthDayNano, IntervalUnit, TimeUnit,
    };
    Ok(match data_type {
        DataType::Interval(IntervalUnit::YearMonth) => {
            ScalarValue::IntervalYearMonth(value.map(|v| v as i32))
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            ScalarValue::IntervalDayTime(value.map(|v| IntervalDayTime {
                days: (v / 86_400_000) as i32,
                milliseconds: (v % 86_400_000) as i32,
            }))
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            ScalarValue::IntervalMonthDayNano(value.map(|v| {
                let months = (v / 2_592_000_000_000_000) as i32;
                let rem = v % 2_592_000_000_000_000;
                IntervalMonthDayNano {
                    months,
                    days: (rem / 86_400_000_000_000) as i32,
                    nanoseconds: rem % 86_400_000_000_000,
                }
            }))
        }
        DataType::Duration(TimeUnit::Second) => ScalarValue::DurationSecond(value),
        DataType::Duration(TimeUnit::Millisecond) => ScalarValue::DurationMillisecond(value),
        DataType::Duration(TimeUnit::Microsecond) => ScalarValue::DurationMicrosecond(value),
        DataType::Duration(TimeUnit::Nanosecond) => ScalarValue::DurationNanosecond(value),
        other => {
            return Err(DataFusionError::Internal(format!(
                "percentile_disc interval accumulator does not support {other}"
            )));
        }
    })
}

impl Accumulator for IntervalPercentileDiscAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![
            0_i32,
            self.all_values.len() as i32,
        ]));
        let values_array = Int64Array::from(std::mem::take(&mut self.all_values));
        let list_array = ListArray::new(
            Arc::new(Field::new_list_field(DataType::Int64, true)),
            offsets,
            Arc::new(values_array),
            None,
        );
        Ok(vec![ScalarValue::List(Arc::new(list_array))])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = &values[0];
        self.all_values.reserve(array.len() - array.null_count());
        for row in 0..array.len() {
            if !array.is_null(row) {
                let v = interval_to_i64(&self.data_type, array, row)?;
                self.all_values.push(v);
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let array = states[0].as_list::<i32>();
        for v in array.iter().flatten() {
            self.all_values
                .extend(v.as_primitive::<Int64Type>().iter().flatten());
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.all_values.is_empty() {
            return i64_to_interval_scalar(&self.data_type, None);
        }
        let mut sorted = std::mem::take(&mut self.all_values);
        sorted.sort_unstable();
        let idx = percentile_disc_index(sorted.len(), self.percentile, self.descending);
        i64_to_interval_scalar(&self.data_type, Some(sorted[idx]))
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.all_values.capacity() * size_of::<i64>()
    }
}

/// Array-of-percentiles form for interval/duration inputs. Stores values as `i64`,
/// computes each percentile discretely, and reconstructs a `List<interval>`
/// (matching Spark's `array<interval>` return, not `array<double>`).
struct MultiIntervalPercentileDiscAccumulator {
    all_values: Vec<i64>,
    percentiles: Vec<f64>,
    descending: bool,
    data_type: DataType,
}

impl Debug for MultiIntervalPercentileDiscAccumulator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MultiIntervalPercentileDiscAccumulator({}, percentiles={:?}, descending={})",
            self.data_type, self.percentiles, self.descending
        )
    }
}

impl Accumulator for MultiIntervalPercentileDiscAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![
            0_i32,
            self.all_values.len() as i32,
        ]));
        let values_array = Int64Array::from(std::mem::take(&mut self.all_values));
        let list_array = ListArray::new(
            Arc::new(Field::new_list_field(DataType::Int64, true)),
            offsets,
            Arc::new(values_array),
            None,
        );
        Ok(vec![ScalarValue::List(Arc::new(list_array))])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = &values[0];
        self.all_values.reserve(array.len() - array.null_count());
        for row in 0..array.len() {
            if !array.is_null(row) {
                let v = interval_to_i64(&self.data_type, array, row)?;
                self.all_values.push(v);
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let array = states[0].as_list::<i32>();
        for v in array.iter().flatten() {
            self.all_values
                .extend(v.as_primitive::<Int64Type>().iter().flatten());
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // Empty input or empty percentile array → NULL list (matches the numeric form).
        if self.all_values.is_empty() || self.percentiles.is_empty() {
            let field = Arc::new(Field::new_list_field(self.data_type.clone(), true));
            return Ok(ScalarValue::List(Arc::new(ListArray::new_null(field, 1))));
        }
        let mut sorted = std::mem::take(&mut self.all_values);
        sorted.sort_unstable();
        let len = sorted.len();
        let scalars: Vec<ScalarValue> = self
            .percentiles
            .iter()
            .map(|&p| {
                let idx = percentile_disc_index(len, p, self.descending);
                i64_to_interval_scalar(&self.data_type, Some(sorted[idx]))
            })
            .collect::<Result<Vec<_>>>()?;
        let values_array = ScalarValue::iter_to_array(scalars)?;
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, values_array.len() as i32]));
        let list_array = ListArray::new(
            Arc::new(Field::new_list_field(self.data_type.clone(), true)),
            offsets,
            values_array,
            None,
        );
        Ok(ScalarValue::List(Arc::new(list_array)))
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + self.all_values.capacity() * size_of::<i64>()
            + self.percentiles.capacity() * size_of::<f64>()
    }
}

pub fn percentile_disc_udaf(ansi_mode: bool) -> Arc<datafusion::logical_expr::AggregateUDF> {
    Arc::new(datafusion::logical_expr::AggregateUDF::from(
        PercentileDisc::new(ansi_mode),
    ))
}
