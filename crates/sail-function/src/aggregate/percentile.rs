use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, RecordBatch, RecordBatchOptions};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::cast::{as_float64_array, as_string_array};
use datafusion::common::ScalarValue;
use datafusion::error::Result;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_common::DataFusionError;
use datafusion_expr_common::columnar_value::ColumnarValue;
use ordered_float::OrderedFloat;
use sail_common_datafusion::literal::LiteralValue;

/// The `PercentileFunction` calculates the exact percentile (quantile) from a set of values.
///
/// Unlike `approx_percentile_cont`, this function calculates the exact percentile by:
/// 1. Collecting all values
/// 2. Sorting them
/// 3. Computing the exact percentile using linear interpolation (for numeric types)
///    or selecting the value at the position (for string types)
///
/// This is computationally more expensive than the approximate version but provides exact results.
///
/// For the special case of percentile 0.5 (median), this is equivalent to the `median` function.
///
/// Supported types:
/// - Numeric types (Int, Float, etc.) -> returns Float64 with linear interpolation
/// - String types (Utf8, Utf8View, LargeUtf8) -> returns the same string type
#[derive(PartialEq, Eq, Hash)]
pub struct PercentileFunction {
    signature: Signature,
}

impl Debug for PercentileFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PercentileFunction")
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for PercentileFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl PercentileFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for PercentileFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "percentile"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::Utf8 => Ok(DataType::Utf8),
            DataType::Utf8View => Ok(DataType::Utf8View),
            DataType::LargeUtf8 => Ok(DataType::LargeUtf8),
            dt @ DataType::Interval(_) => Ok(dt.clone()),
            dt @ DataType::Duration(_) => Ok(dt.clone()),
            _ => Ok(DataType::Float64),
        }
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let data_type = &acc_args.exprs[0].data_type(acc_args.schema)?;

        let expr: &Arc<dyn PhysicalExpr> = acc_args.exprs.get(1).ok_or_else(|| {
            DataFusionError::Execution(
                "percentile() requires a second argument (percentile value)".into(),
            )
        })?;

        let percentile: f64 = extract_literal(expr).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to extract percentile value in percentile(): {}",
                e
            ))
        })?;

        // Validate that percentile is within [0.0, 1.0]
        if !(0.0..=1.0).contains(&percentile) {
            return Err(DataFusionError::Execution(format!(
                "Percentile value {} is out of range [0.0, 1.0]",
                percentile
            )));
        }

        match data_type {
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => Ok(Box::new(
                StringPercentileAccumulator::new(percentile, data_type.clone()),
            )),
            DataType::Interval(_) | DataType::Duration(_) => Ok(Box::new(
                IntervalPercentileAccumulator::new(percentile, data_type.clone()),
            )),
            _ => Ok(Box::new(NumericPercentileAccumulator::new(percentile))),
        }
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<arrow::datatypes::FieldRef>> {
        let value_type = args.input_fields[0].data_type().clone();

        let storage_type = match &value_type {
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => DataType::Utf8,
            DataType::Interval(_) | DataType::Duration(_) => DataType::Int64,
            _ => DataType::Float64,
        };

        let values_list_type = DataType::List(Arc::new(Field::new("item", storage_type, true)));

        Ok(vec![Field::new("values", values_list_type, true).into()])
    }
}

/// Accumulator for numeric types (with linear interpolation)
///
/// Optimization: Uses HashMap to track value counts for O(1) retract operations
/// instead of O(n) removal from Vec.
#[derive(Debug)]
pub struct NumericPercentileAccumulator {
    /// Map from value to count (how many times it appears)
    value_counts: HashMap<OrderedFloat<f64>, usize>,
    /// Total number of values (sum of all counts)
    total_count: usize,
    percentile: f64,
}

impl NumericPercentileAccumulator {
    pub fn new(percentile: f64) -> Self {
        Self {
            value_counts: HashMap::new(),
            total_count: 0,
            percentile,
        }
    }

    /// Calculate percentile with linear interpolation (Spark's formula)
    fn calculate_percentile(&self, sorted_values: &[f64]) -> Option<f64> {
        if sorted_values.is_empty() {
            return None;
        }

        if sorted_values.len() == 1 {
            return Some(sorted_values[0]);
        }

        let n: usize = sorted_values.len();
        let pos: f64 = (n - 1) as f64 * self.percentile;
        let lower_idx: usize = pos.floor() as usize;
        let upper_idx: usize = pos.ceil() as usize;

        if lower_idx == upper_idx {
            Some(sorted_values[lower_idx])
        } else {
            let lower_val: f64 = sorted_values[lower_idx];
            let upper_val: f64 = sorted_values[upper_idx];
            let fraction: f64 = pos - lower_idx as f64;
            Some(lower_val + fraction * (upper_val - lower_val))
        }
    }

    /// Convert HashMap to sorted Vec
    fn get_sorted_values(&self) -> Vec<f64> {
        let mut values = Vec::with_capacity(self.total_count);

        for (ordered_val, &count) in &self.value_counts {
            let val = ordered_val.into_inner();
            for _ in 0..count {
                values.push(val);
            }
        }

        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        values
    }
}

impl Accumulator for NumericPercentileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let array = &values[0];

        let float_array = arrow::compute::cast(array, &DataType::Float64)?;
        let float_array = as_float64_array(&float_array)?;

        for value in float_array.iter().flatten() {
            let ordered_val = OrderedFloat(value);
            *self.value_counts.entry(ordered_val).or_insert(0) += 1;
            self.total_count += 1;
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.total_count == 0 {
            return Ok(ScalarValue::Float64(None));
        }

        let sorted_values = self.get_sorted_values();

        match self.calculate_percentile(&sorted_values) {
            Some(result) => Ok(ScalarValue::Float64(Some(result))),
            None => Ok(ScalarValue::Float64(None)),
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(&self.value_counts)
            + self.value_counts.capacity() * std::mem::size_of::<(OrderedFloat<f64>, usize)>()
            + std::mem::size_of::<usize>()
            + std::mem::size_of::<f64>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let sorted_values = self.get_sorted_values();

        let values_scalar = ScalarValue::new_list_nullable(
            &sorted_values
                .iter()
                .map(|&v| ScalarValue::Float64(Some(v)))
                .collect::<Vec<_>>(),
            &DataType::Float64,
        );

        Ok(vec![ScalarValue::List(values_scalar)])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let values_list = &states[0];

        if let Some(list_array) = values_list.as_list_opt::<i32>() {
            for list_idx in 0..list_array.len() {
                if !list_array.is_null(list_idx) {
                    let value_array = list_array.value(list_idx);
                    let float_array = as_float64_array(&value_array)?;

                    for value in float_array.iter().flatten() {
                        let ordered_val = OrderedFloat(value);
                        *self.value_counts.entry(ordered_val).or_insert(0) += 1;
                        self.total_count += 1;
                    }
                }
            }
        }

        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let array = &values[0];
        let float_array = arrow::compute::cast(array, &DataType::Float64)?;
        let float_array = as_float64_array(&float_array)?;

        // O(1) removal using HashMap
        for v in float_array.iter().flatten() {
            let ordered_val = OrderedFloat(v);
            if let Some(count) = self.value_counts.get_mut(&ordered_val) {
                *count -= 1;
                self.total_count -= 1;

                // Remove entry if count reaches 0
                if *count == 0 {
                    self.value_counts.remove(&ordered_val);
                }
            }
        }

        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

/// Accumulator for string types (returns the value at the percentile position)
///
/// Optimization: Uses HashMap to track string counts for O(1) retract operations
#[derive(Debug)]
pub struct StringPercentileAccumulator {
    value_counts: HashMap<String, usize>,
    total_count: usize,
    percentile: f64,
    data_type: DataType,
}

impl StringPercentileAccumulator {
    pub fn new(percentile: f64, data_type: DataType) -> Self {
        Self {
            value_counts: HashMap::new(),
            total_count: 0,
            percentile,
            data_type,
        }
    }

    /// Calculate percentile for strings (no interpolation, uses rounding)
    fn calculate_percentile(&self, sorted_values: &[String]) -> Option<String> {
        if sorted_values.is_empty() {
            return None;
        }

        if sorted_values.len() == 1 {
            return Some(sorted_values[0].clone());
        }

        let n: usize = sorted_values.len();
        let pos: f64 = (n - 1) as f64 * self.percentile;
        let idx: usize = pos.round() as usize;

        Some(sorted_values[idx].clone())
    }

    /// Convert HashMap to sorted Vec
    fn get_sorted_values(&self) -> Vec<String> {
        let mut values = Vec::with_capacity(self.total_count);

        for (val, &count) in &self.value_counts {
            for _ in 0..count {
                values.push(val.clone());
            }
        }

        values.sort();
        values
    }
}

impl Accumulator for StringPercentileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let array = &values[0];

        // Convert input to Utf8 and collect values
        let string_array = arrow::compute::cast(array, &DataType::Utf8)?;
        let string_array = as_string_array(&string_array)?;

        for value in string_array.iter().flatten() {
            let val = value.to_string();
            *self.value_counts.entry(val).or_insert(0) += 1;
            self.total_count += 1;
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.total_count == 0 {
            return match_string_type!(&self.data_type, None);
        }

        let sorted_values = self.get_sorted_values();

        match self.calculate_percentile(&sorted_values) {
            Some(result) => match_string_type!(&self.data_type, Some(result)),
            None => match_string_type!(&self.data_type, None),
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(&self.value_counts)
            + self
                .value_counts
                .keys()
                .map(|k| k.capacity() + std::mem::size_of::<usize>())
                .sum::<usize>()
            + std::mem::size_of::<usize>()
            + std::mem::size_of::<f64>()
            + std::mem::size_of::<DataType>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let sorted_values = self.get_sorted_values();

        let values_scalar = ScalarValue::new_list_nullable(
            &sorted_values
                .iter()
                .map(|v| ScalarValue::Utf8(Some(v.clone())))
                .collect::<Vec<_>>(),
            &DataType::Utf8,
        );

        Ok(vec![ScalarValue::List(values_scalar)])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let values_list = &states[0];

        if let Some(list_array) = values_list.as_list_opt::<i32>() {
            for list_idx in 0..list_array.len() {
                if !list_array.is_null(list_idx) {
                    let value_array = list_array.value(list_idx);
                    let string_array = as_string_array(&value_array)?;

                    for value in string_array.iter().flatten() {
                        let val = value.to_string();
                        *self.value_counts.entry(val).or_insert(0) += 1;
                        self.total_count += 1;
                    }
                }
            }
        }

        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let array = &values[0];

        let string_array = arrow::compute::cast(array, &DataType::Utf8)?;
        let string_array = as_string_array(&string_array)?;

        // O(1) removal using HashMap
        for v in string_array.iter().flatten() {
            let val = v.to_string();
            if let Some(count) = self.value_counts.get_mut(&val) {
                *count -= 1;
                self.total_count -= 1;

                if *count == 0 {
                    self.value_counts.remove(&val);
                }
            }
        }

        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

/// Accumulator for interval/duration types
///
/// Optimization: Uses HashMap to track interval counts for O(1) retract operations
#[derive(Debug)]
pub struct IntervalPercentileAccumulator {
    value_counts: HashMap<i64, usize>,
    total_count: usize,
    percentile: f64,
    data_type: DataType,
}

impl IntervalPercentileAccumulator {
    pub fn new(percentile: f64, data_type: DataType) -> Self {
        Self {
            value_counts: HashMap::new(),
            total_count: 0,
            percentile,
            data_type,
        }
    }

    /// Calculate percentile for i64 with interpolation
    fn calculate_percentile(&self, sorted_values: &[i64]) -> Option<i64> {
        if sorted_values.is_empty() {
            return None;
        }

        if sorted_values.len() == 1 {
            return Some(sorted_values[0]);
        }

        let n: usize = sorted_values.len();
        let pos: f64 = (n - 1) as f64 * self.percentile;
        let lower_idx: usize = pos.floor() as usize;
        let upper_idx: usize = pos.ceil() as usize;

        if lower_idx == upper_idx {
            Some(sorted_values[lower_idx])
        } else {
            let lower: f64 = sorted_values[lower_idx] as f64;
            let upper: f64 = sorted_values[upper_idx] as f64;
            let fraction: f64 = pos - lower_idx as f64;
            Some((lower + fraction * (upper - lower)).round() as i64)
        }
    }

    /// Convert HashMap to sorted Vec
    fn get_sorted_values(&self) -> Vec<i64> {
        let mut values = Vec::with_capacity(self.total_count);

        for (&val, &count) in &self.value_counts {
            for _ in 0..count {
                values.push(val);
            }
        }

        values.sort_unstable();
        values
    }
}

impl Accumulator for IntervalPercentileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let array = &values[0];

        for row_idx in 0..array.len() {
            if array.is_null(row_idx) {
                continue;
            }

            // Convert interval/duration to i64 for calculation
            let value = match &self.data_type {
                DataType::Interval(unit) => {
                    use datafusion::arrow::datatypes::IntervalUnit;
                    match unit {
                        IntervalUnit::YearMonth => {
                            let arr = array.as_primitive::<datafusion::arrow::datatypes::IntervalYearMonthType>();
                            arr.value(row_idx) as i64
                        }
                        IntervalUnit::DayTime => {
                            let arr = array
                                .as_primitive::<datafusion::arrow::datatypes::IntervalDayTimeType>(
                                );
                            let val = arr.value(row_idx);
                            // IntervalDayTime is stored as (days, milliseconds)
                            // Convert to total milliseconds for interpolation
                            val.days as i64 * 86_400_000 + val.milliseconds as i64
                        }
                        IntervalUnit::MonthDayNano => {
                            let arr = array.as_primitive::<datafusion::arrow::datatypes::IntervalMonthDayNanoType>();
                            let val = arr.value(row_idx);
                            // Convert to nanoseconds for interpolation
                            val.months as i64 * 2_592_000_000_000_000 // ~30 days in nanos
                                + val.days as i64 * 86_400_000_000_000 // days to nanos
                                + val.nanoseconds
                        }
                    }
                }
                DataType::Duration(unit) => {
                    use datafusion::arrow::datatypes::TimeUnit;
                    match unit {
                        TimeUnit::Second => array
                            .as_primitive::<datafusion::arrow::datatypes::DurationSecondType>()
                            .value(row_idx),
                        TimeUnit::Millisecond => array
                            .as_primitive::<datafusion::arrow::datatypes::DurationMillisecondType>()
                            .value(row_idx),
                        TimeUnit::Microsecond => array
                            .as_primitive::<datafusion::arrow::datatypes::DurationMicrosecondType>()
                            .value(row_idx),
                        TimeUnit::Nanosecond => array
                            .as_primitive::<datafusion::arrow::datatypes::DurationNanosecondType>()
                            .value(row_idx),
                    }
                }
                _ => {
                    return Err(DataFusionError::Execution(format!(
                        "IntervalPercentileAccumulator does not support type {:?}",
                        self.data_type
                    )));
                }
            };

            *self.value_counts.entry(value).or_insert(0) += 1;
            self.total_count += 1;
        }

        Ok(())
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.total_count == 0 {
            return match &self.data_type {
                DataType::Interval(unit) => interval_none!(unit),
                DataType::Duration(unit) => duration_none!(unit),
                _ => Err(DataFusionError::Execution(format!(
                    "Unsupported type {:?}",
                    self.data_type
                ))),
            };
        }

        let sorted_values = self.get_sorted_values();

        match self.calculate_percentile(&sorted_values) {
            Some(result_i64) => match &self.data_type {
                DataType::Interval(unit) => {
                    use datafusion::arrow::datatypes::IntervalUnit;
                    match unit {
                        IntervalUnit::YearMonth => {
                            Ok(ScalarValue::IntervalYearMonth(Some(result_i64 as i32)))
                        }
                        IntervalUnit::DayTime => {
                            // Convert back from milliseconds to (days, milliseconds)
                            use datafusion::arrow::datatypes::IntervalDayTime;
                            let days = (result_i64 / 86_400_000) as i32;
                            let milliseconds = (result_i64 % 86_400_000) as i32;
                            Ok(ScalarValue::IntervalDayTime(Some(IntervalDayTime {
                                days,
                                milliseconds,
                            })))
                        }
                        IntervalUnit::MonthDayNano => {
                            // Convert back from nanoseconds
                            use datafusion::arrow::datatypes::IntervalMonthDayNano;
                            let months = (result_i64 / 2_592_000_000_000_000) as i32;
                            let remaining = result_i64 % 2_592_000_000_000_000;
                            let days = (remaining / 86_400_000_000_000) as i32;
                            let nanoseconds = remaining % 86_400_000_000_000;
                            Ok(ScalarValue::IntervalMonthDayNano(Some(
                                IntervalMonthDayNano {
                                    months,
                                    days,
                                    nanoseconds,
                                },
                            )))
                        }
                    }
                }
                DataType::Duration(unit) => {
                    use datafusion::arrow::datatypes::TimeUnit;
                    match unit {
                        TimeUnit::Second => Ok(ScalarValue::DurationSecond(Some(result_i64))),
                        TimeUnit::Millisecond => {
                            Ok(ScalarValue::DurationMillisecond(Some(result_i64)))
                        }
                        TimeUnit::Microsecond => {
                            Ok(ScalarValue::DurationMicrosecond(Some(result_i64)))
                        }
                        TimeUnit::Nanosecond => {
                            Ok(ScalarValue::DurationNanosecond(Some(result_i64)))
                        }
                    }
                }
                _ => Err(DataFusionError::Execution(format!(
                    "Unsupported type {:?}",
                    self.data_type
                ))),
            },
            None => match &self.data_type {
                DataType::Interval(unit) => interval_none!(unit),
                DataType::Duration(unit) => duration_none!(unit),
                _ => Err(DataFusionError::Execution(format!(
                    "Unsupported type {:?}",
                    self.data_type
                ))),
            },
        }
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(&self.value_counts)
            + self.value_counts.capacity() * std::mem::size_of::<(i64, usize)>()
            + std::mem::size_of::<usize>()
            + std::mem::size_of::<f64>()
            + std::mem::size_of::<DataType>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let sorted_values = self.get_sorted_values();

        let values_scalar = ScalarValue::new_list_nullable(
            &sorted_values
                .iter()
                .map(|&v| ScalarValue::Int64(Some(v)))
                .collect::<Vec<_>>(),
            &DataType::Int64,
        );

        Ok(vec![ScalarValue::List(values_scalar)])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let values_list = states[0].as_list::<i32>();

        for list_idx in 0..values_list.len() {
            if values_list.is_null(list_idx) {
                continue;
            }

            let values_array = values_list.value(list_idx);
            let int_array = values_array.as_primitive::<datafusion::arrow::datatypes::Int64Type>();

            for elem_idx in 0..int_array.len() {
                if !int_array.is_null(elem_idx) {
                    let value = int_array.value(elem_idx);
                    *self.value_counts.entry(value).or_insert(0) += 1;
                    self.total_count += 1;
                }
            }
        }

        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let array = &values[0];

        for row_idx in 0..array.len() {
            if array.is_null(row_idx) {
                continue;
            }

            let val_i64 = match &self.data_type {
                DataType::Interval(unit) => {
                    use datafusion::arrow::datatypes::IntervalUnit;
                    match unit {
                        IntervalUnit::YearMonth => {
                            let arr = array.as_primitive::<datafusion::arrow::datatypes::IntervalYearMonthType>();
                            arr.value(row_idx) as i64
                        }
                        IntervalUnit::DayTime => {
                            let arr = array
                                .as_primitive::<datafusion::arrow::datatypes::IntervalDayTimeType>(
                                );
                            let v = arr.value(row_idx);
                            v.days as i64 * 86_400_000 + v.milliseconds as i64
                        }
                        IntervalUnit::MonthDayNano => {
                            let arr = array.as_primitive::<datafusion::arrow::datatypes::IntervalMonthDayNanoType>();
                            let v = arr.value(row_idx);
                            v.months as i64 * 2_592_000_000_000_000
                                + v.days as i64 * 86_400_000_000_000
                                + v.nanoseconds
                        }
                    }
                }
                DataType::Duration(unit) => {
                    use datafusion::arrow::datatypes::TimeUnit;
                    match unit {
                        TimeUnit::Second => array
                            .as_primitive::<datafusion::arrow::datatypes::DurationSecondType>()
                            .value(row_idx),
                        TimeUnit::Millisecond => array
                            .as_primitive::<datafusion::arrow::datatypes::DurationMillisecondType>()
                            .value(row_idx),
                        TimeUnit::Microsecond => array
                            .as_primitive::<datafusion::arrow::datatypes::DurationMicrosecondType>()
                            .value(row_idx),
                        TimeUnit::Nanosecond => array
                            .as_primitive::<datafusion::arrow::datatypes::DurationNanosecondType>()
                            .value(row_idx),
                    }
                }
                _ => {
                    return Err(DataFusionError::Execution(format!(
                        "IntervalPercentileAccumulator does not support type {:?}",
                        self.data_type
                    )));
                }
            };

            // O(1) removal using HashMap
            if let Some(count) = self.value_counts.get_mut(&val_i64) {
                *count -= 1;
                self.total_count -= 1;

                if *count == 0 {
                    self.value_counts.remove(&val_i64);
                }
            }
        }

        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

/// Extract a literal f64 value from a PhysicalExpr (for percentile parameter)
fn extract_literal(expr: &Arc<dyn PhysicalExpr>) -> Result<f64, DataFusionError> {
    // Create empty batch to evaluate the physical expression
    let fields: Vec<Field> = vec![];
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new_with_options(
        schema,
        vec![],
        &RecordBatchOptions::default().with_row_count(Some(1)),
    )?;

    // Evaluate the expression to get a scalar value
    let col_val = expr.evaluate(&batch)?;
    let scalar = match col_val {
        ColumnarValue::Scalar(s) => s,
        ColumnarValue::Array(arr) => ScalarValue::try_from_array(arr.as_ref(), 0)?,
    };

    // Convert scalar to f64, supporting numeric types and decimals
    let percentile: f64 = match &scalar {
        // Try native f64 types first
        ScalarValue::Float64(Some(v)) => *v,
        ScalarValue::Float32(Some(v)) => *v as f64,
        ScalarValue::Float16(Some(v)) => f64::from(*v),
        // Try integers (use LiteralValue for validation)
        ScalarValue::Int8(_)
        | ScalarValue::Int16(_)
        | ScalarValue::Int32(_)
        | ScalarValue::Int64(_)
        | ScalarValue::UInt8(_)
        | ScalarValue::UInt16(_)
        | ScalarValue::UInt32(_)
        | ScalarValue::UInt64(_) => {
            // LiteralValue will validate and convert
            let int_val = LiteralValue(&scalar).try_to_i64().map_err(|e| {
                DataFusionError::Execution(format!(
                    "Cannot convert percentile literal {:?} to integer: {}",
                    scalar, e
                ))
            })?;
            int_val as f64
        }
        // Handle decimals
        ScalarValue::Decimal128(Some(v), _precision, scale) => {
            (*v as f64) / 10f64.powi(*scale as i32)
        }
        _ => {
            return Err(DataFusionError::Execution(format!(
                "Cannot convert percentile literal {:?} to f64",
                scalar
            )))
        }
    };

    Ok(percentile)
}
