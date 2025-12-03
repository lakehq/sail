use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow;
use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::cast::{as_float64_array, as_string_array};
use datafusion::common::ScalarValue;
use datafusion::error::Result;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_common::DataFusionError;

use crate::aggregate::util::percentile::{extract_literal, return_type, state_fields};

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
        return_type(arg_types)
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
        state_fields(args)
    }
}

#[derive(Debug)]
pub struct NumericPercentileAccumulator {
    values: Vec<f64>,
    percentile: f64,
}

impl NumericPercentileAccumulator {
    pub fn new(percentile: f64) -> Self {
        Self {
            values: Vec::new(),
            percentile,
        }
    }

    fn calculate_percentile(&self, sorted_values: &[f64], percentile: f64) -> Option<f64> {
        if sorted_values.is_empty() {
            return None;
        }

        if sorted_values.len() == 1 {
            return Some(sorted_values[0]);
        }

        let n = sorted_values.len();

        // Spark uses (n - 1) * percentile for the position
        let pos = (n - 1) as f64 * percentile;
        let lower_idx = pos.floor() as usize;
        let upper_idx = pos.ceil() as usize;

        if lower_idx == upper_idx {
            Some(sorted_values[lower_idx])
        } else {
            let lower_val = sorted_values[lower_idx];
            let upper_val = sorted_values[upper_idx];
            let fraction = pos - lower_idx as f64;
            Some(lower_val + fraction * (upper_val - lower_val))
        }
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
            self.values.push(value);
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.values.is_empty() {
            return Ok(ScalarValue::Float64(None));
        }

        let mut sorted_values = self.values.clone();
        sorted_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        match self.calculate_percentile(&sorted_values, self.percentile) {
            Some(result) => Ok(ScalarValue::Float64(Some(result))),
            None => Ok(ScalarValue::Float64(None)),
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(&self.values)
            + self.values.capacity() * std::mem::size_of::<f64>()
            + std::mem::size_of::<f64>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let values_scalar = ScalarValue::new_list_nullable(
            &self
                .values
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
            for i in 0..list_array.len() {
                if !list_array.is_null(i) {
                    let value_array = list_array.value(i);
                    let float_array = as_float64_array(&value_array)?;

                    for value in float_array.iter().flatten() {
                        self.values.push(value);
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

        for v in float_array.iter().flatten() {
            if let Some(pos) = self.values.iter().position(|x| *x == v) {
                self.values.remove(pos);
            }
        }

        Ok(())
    }
    fn supports_retract_batch(&self) -> bool {
        true
    }
}

/// Accumulator for string types (returns the value at the percentile position)
#[derive(Debug)]
pub struct StringPercentileAccumulator {
    values: Vec<String>,
    percentile: f64,
    data_type: DataType,
}

impl StringPercentileAccumulator {
    pub fn new(percentile: f64, data_type: DataType) -> Self {
        Self {
            values: Vec::new(),
            percentile,
            data_type,
        }
    }

    /// Calculate the string at the percentile position
    /// For strings, we can't interpolate, so we return the value at the closest index
    fn calculate_percentile(&self, sorted_values: &[String], percentile: f64) -> Option<String> {
        if sorted_values.is_empty() {
            return None;
        }

        if sorted_values.len() == 1 {
            return Some(sorted_values[0].clone());
        }

        let n = sorted_values.len();

        // Use the same position calculation as Spark
        let pos = (n - 1) as f64 * percentile;
        let idx = pos.round() as usize;

        Some(sorted_values[idx].clone())
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
            self.values.push(value.to_string());
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.values.is_empty() {
            return match_string_type!(&self.data_type, None);
        }

        // Sort values alphabetically
        let mut sorted_values = self.values.clone();
        sorted_values.sort();

        match self.calculate_percentile(&sorted_values, self.percentile) {
            Some(result) => match_string_type!(&self.data_type, Some(result)),
            None => match_string_type!(&self.data_type, None),
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(&self.values)
            + self.values.iter().map(|s| s.capacity()).sum::<usize>()
            + std::mem::size_of::<f64>()
            + std::mem::size_of::<DataType>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let values_scalar = ScalarValue::new_list_nullable(
            &self
                .values
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
            for i in 0..list_array.len() {
                if !list_array.is_null(i) {
                    let value_array = list_array.value(i);
                    let string_array = as_string_array(&value_array)?;

                    for value in string_array.iter().flatten() {
                        self.values.push(value.to_string());
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

        for v in string_array.iter().flatten() {
            if let Some(pos) = self.values.iter().position(|s| s == v) {
                self.values.remove(pos);
            }
        }

        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

/// Accumulator for interval/duration types
#[derive(Debug)]
pub struct IntervalPercentileAccumulator {
    values: Vec<i64>,
    percentile: f64,
    data_type: DataType,
}

impl IntervalPercentileAccumulator {
    pub fn new(percentile: f64, data_type: DataType) -> Self {
        Self {
            values: Vec::new(),
            percentile,
            data_type,
        }
    }

    fn calculate_percentile(&self, sorted_values: &[i64], percentile: f64) -> Option<i64> {
        if sorted_values.is_empty() {
            return None;
        }

        if sorted_values.len() == 1 {
            return Some(sorted_values[0]);
        }

        let n = sorted_values.len();
        let pos = (n - 1) as f64 * percentile;
        let lower_idx = pos.floor() as usize;
        let upper_idx = pos.ceil() as usize;

        if lower_idx == upper_idx {
            Some(sorted_values[lower_idx])
        } else {
            let lower = sorted_values[lower_idx] as f64;
            let upper = sorted_values[upper_idx] as f64;
            let fraction = pos - lower_idx as f64;
            Some((lower + fraction * (upper - lower)).round() as i64)
        }
    }
}

impl Accumulator for IntervalPercentileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let array = &values[0];

        for i in 0..array.len() {
            if array.is_null(i) {
                continue;
            }

            // Convert interval/duration to i64 for calculation
            let value = match &self.data_type {
                DataType::Interval(unit) => {
                    use datafusion::arrow::datatypes::IntervalUnit;
                    match unit {
                        IntervalUnit::YearMonth => {
                            let arr = array.as_primitive::<datafusion::arrow::datatypes::IntervalYearMonthType>();
                            arr.value(i) as i64
                        }
                        IntervalUnit::DayTime => {
                            let arr = array
                                .as_primitive::<datafusion::arrow::datatypes::IntervalDayTimeType>(
                                );
                            let val = arr.value(i);
                            // IntervalDayTime is stored as (days, milliseconds)
                            // Convert to total milliseconds for interpolation
                            val.days as i64 * 86400000 + val.milliseconds as i64
                        }
                        IntervalUnit::MonthDayNano => {
                            let arr = array.as_primitive::<datafusion::arrow::datatypes::IntervalMonthDayNanoType>();
                            let val = arr.value(i);
                            // Convert to nanoseconds for interpolation
                            val.months as i64 * 2592000000000000 // ~30 days in nanos
                                + val.days as i64 * 86400000000000 // days to nanos
                                + val.nanoseconds
                        }
                    }
                }
                DataType::Duration(unit) => {
                    use datafusion::arrow::datatypes::TimeUnit;
                    match unit {
                        TimeUnit::Second => array
                            .as_primitive::<datafusion::arrow::datatypes::DurationSecondType>()
                            .value(i),
                        TimeUnit::Millisecond => array
                            .as_primitive::<datafusion::arrow::datatypes::DurationMillisecondType>()
                            .value(i),
                        TimeUnit::Microsecond => array
                            .as_primitive::<datafusion::arrow::datatypes::DurationMicrosecondType>()
                            .value(i),
                        TimeUnit::Nanosecond => array
                            .as_primitive::<datafusion::arrow::datatypes::DurationNanosecondType>()
                            .value(i),
                    }
                }
                _ => {
                    return Err(DataFusionError::Execution(format!(
                        "IntervalPercentileAccumulator does not support type {:?}",
                        self.data_type
                    )));
                }
            };

            self.values.push(value);
        }

        Ok(())
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.values.is_empty() {
            return match &self.data_type {
                DataType::Interval(unit) => interval_none!(unit),
                DataType::Duration(unit) => duration_none!(unit),
                _ => Err(DataFusionError::Execution(format!(
                    "Unsupported type {:?}",
                    self.data_type
                ))),
            };
        }

        let mut sorted_values = self.values.clone();
        sorted_values.sort();

        match self.calculate_percentile(&sorted_values, self.percentile) {
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
                            let days = (result_i64 / 86400000) as i32;
                            let milliseconds = (result_i64 % 86400000) as i32;
                            Ok(ScalarValue::IntervalDayTime(Some(IntervalDayTime {
                                days,
                                milliseconds,
                            })))
                        }
                        IntervalUnit::MonthDayNano => {
                            // Convert back from nanoseconds
                            use datafusion::arrow::datatypes::IntervalMonthDayNano;
                            let months = (result_i64 / 2592000000000000) as i32;
                            let remaining = result_i64 % 2592000000000000;
                            let days = (remaining / 86400000000000) as i32;
                            let nanoseconds = remaining % 86400000000000;
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
        std::mem::size_of_val(&self.values)
            + self.values.capacity() * std::mem::size_of::<i64>()
            + std::mem::size_of::<f64>()
            + std::mem::size_of::<DataType>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let values_scalar = ScalarValue::new_list_nullable(
            &self
                .values
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

        for i in 0..values_list.len() {
            if values_list.is_null(i) {
                continue;
            }

            let values_array = values_list.value(i);
            let int_array = values_array.as_primitive::<datafusion::arrow::datatypes::Int64Type>();

            for j in 0..int_array.len() {
                if !int_array.is_null(j) {
                    self.values.push(int_array.value(j));
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

        for i in 0..array.len() {
            if array.is_null(i) {
                continue;
            }

            let val_i64 = match &self.data_type {
                DataType::Interval(unit) => {
                    use datafusion::arrow::datatypes::IntervalUnit;
                    match unit {
                        IntervalUnit::YearMonth => {
                            let arr = array.as_primitive::<datafusion::arrow::datatypes::IntervalYearMonthType>();
                            arr.value(i) as i64
                        }
                        IntervalUnit::DayTime => {
                            let arr = array
                                .as_primitive::<datafusion::arrow::datatypes::IntervalDayTimeType>(
                                );
                            let v = arr.value(i);
                            v.days as i64 * 86_400_000 + v.milliseconds as i64
                        }
                        IntervalUnit::MonthDayNano => {
                            let arr = array.as_primitive::<datafusion::arrow::datatypes::IntervalMonthDayNanoType>();
                            let v = arr.value(i);
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
                            .value(i),
                        TimeUnit::Millisecond => array
                            .as_primitive::<datafusion::arrow::datatypes::DurationMillisecondType>()
                            .value(i),
                        TimeUnit::Microsecond => array
                            .as_primitive::<datafusion::arrow::datatypes::DurationMicrosecondType>()
                            .value(i),
                        TimeUnit::Nanosecond => array
                            .as_primitive::<datafusion::arrow::datatypes::DurationNanosecondType>()
                            .value(i),
                    }
                }
                _ => {
                    return Err(DataFusionError::Execution(format!(
                        "IntervalPercentileAccumulator does not support type {:?}",
                        self.data_type
                    )));
                }
            };

            if let Some(pos) = self.values.iter().position(|x| *x == val_i64) {
                self.values.remove(pos);
            }
        }

        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}
