use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow;
use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::datatypes::{
    DataType, DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType,
    DurationSecondType, IntervalDayTimeType, IntervalMonthDayNanoType, IntervalUnit,
};
use datafusion::common::cast::{as_float64_array, as_string_array};
use datafusion::common::ScalarValue;
use datafusion::error::Result;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_common::DataFusionError;

use crate::aggregate::percentile::extract_literal;

/// The `PercentileDiscFunction` calculates the discrete percentile (quantile) from a set of values.
///
/// Unlike `percentile_cont` which uses linear interpolation, `percentile_disc` returns an actual
/// value from the dataset (no interpolation). It uses the LOWER method for index calculation:
/// - Position: ceil(percentile * n) - 1
/// - This ensures we always select an existing value from the sorted list
///
/// Supported types:
/// - Numeric types (Int, Float, etc.) -> returns the same numeric type (cast to Float64)
/// - String types (Utf8, Utf8View, LargeUtf8) -> returns the same string type
/// - INTERVAL types (YearMonth, DayTime, MonthDayNano) -> returns the same interval type
/// - Duration types (Second, Millisecond, Microsecond, Nanosecond) -> returns the same duration type
#[derive(PartialEq, Eq, Hash)]
pub struct PercentileDiscFunction {
    signature: Signature,
}

impl Debug for PercentileDiscFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PercentileDiscFunction")
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for PercentileDiscFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl PercentileDiscFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for PercentileDiscFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "percentile_disc"
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

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<arrow::datatypes::FieldRef>> {
        let value_type = args.input_fields[0].data_type().clone();

        let storage_type = match &value_type {
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => DataType::Utf8,
            DataType::Interval(_) | DataType::Duration(_) => DataType::Int64,
            _ => DataType::Float64,
        };

        let values_list_type = DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            storage_type,
            true,
        )));

        Ok(vec![
            arrow::datatypes::Field::new("values", values_list_type, true).into(),
            arrow::datatypes::Field::new("percentile", DataType::Float64, false).into(),
            arrow::datatypes::Field::new("data_type_id", DataType::UInt8, false).into(),
        ])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let data_type = &acc_args.exprs[0].data_type(acc_args.schema)?;

        let expr: &Arc<dyn PhysicalExpr> = acc_args.exprs.get(1).ok_or_else(|| {
            DataFusionError::Execution(
                "percentile_disc() requires a second argument (percentile value)".into(),
            )
        })?;

        let percentile: f64 = extract_literal(expr).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to extract percentile value in percentile_disc(): {}",
                e
            ))
        })?;

        match data_type {
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => Ok(Box::new(
                StringPercentileDiscAccumulator::new(percentile, data_type.clone()),
            )),
            DataType::Interval(_) | DataType::Duration(_) => Ok(Box::new(
                IntervalPercentileDiscAccumulator::new(percentile, data_type.clone()),
            )),
            _ => Ok(Box::new(NumericPercentileDiscAccumulator::new(percentile))),
        }
    }
}

#[derive(Debug)]
pub struct NumericPercentileDiscAccumulator {
    values: Vec<f64>,
    percentile: f64,
}

impl NumericPercentileDiscAccumulator {
    pub fn new(percentile: f64) -> Self {
        Self {
            values: Vec::new(),
            percentile,
        }
    }

    fn calculate_percentile_disc(&self, sorted_values: &[f64], percentile: f64) -> Option<f64> {
        if sorted_values.is_empty() {
            return None;
        }

        if sorted_values.len() == 1 {
            return Some(sorted_values[0]);
        }

        let n = sorted_values.len();

        // Discrete percentile: use ceil method to always return an existing value
        // Position = ceil(percentile * n) - 1
        let pos = (percentile * n as f64).ceil() as usize;
        let idx = if pos == 0 { 0 } else { pos - 1 };
        let idx = idx.min(n - 1);

        Some(sorted_values[idx])
    }
}

impl Accumulator for NumericPercentileDiscAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let array = &values[0];

        if values.len() >= 2 {
            if let Some(percentile_array) =
                values[1].as_primitive_opt::<arrow::datatypes::Float64Type>()
            {
                if !percentile_array.is_empty() && !percentile_array.is_null(0) {
                    self.percentile = percentile_array.value(0);
                }
            }
        }

        let float_array = arrow::compute::cast(array, &DataType::Float64)?;
        let float_array = as_float64_array(&float_array)?;

        for value in float_array.iter().flatten() {
            self.values.push(value);
        }

        Ok(())
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

        Ok(vec![
            ScalarValue::List(values_scalar),
            ScalarValue::Float64(Some(self.percentile)),
            ScalarValue::UInt8(Some(0)), // 0 = numeric type
        ])
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

        if states.len() >= 2 {
            let percentile_array = as_float64_array(&states[1])?;
            if !percentile_array.is_empty() && !percentile_array.is_null(0) {
                self.percentile = percentile_array.value(0);
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.values.is_empty() {
            return Ok(ScalarValue::Float64(None));
        }

        let mut sorted_values = self.values.clone();
        sorted_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        match self.calculate_percentile_disc(&sorted_values, self.percentile) {
            Some(result) => Ok(ScalarValue::Float64(Some(result))),
            None => Ok(ScalarValue::Float64(None)),
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.values.capacity() * std::mem::size_of::<f64>()
    }
}

#[derive(Debug)]
pub struct StringPercentileDiscAccumulator {
    values: Vec<String>,
    percentile: f64,
    data_type: DataType,
}

impl StringPercentileDiscAccumulator {
    pub fn new(percentile: f64, data_type: DataType) -> Self {
        Self {
            values: Vec::new(),
            percentile,
            data_type,
        }
    }

    fn calculate_percentile_disc(
        &self,
        sorted_values: &[String],
        percentile: f64,
    ) -> Option<String> {
        if sorted_values.is_empty() {
            return None;
        }

        if sorted_values.len() == 1 {
            return Some(sorted_values[0].clone());
        }

        let n = sorted_values.len();

        // Discrete percentile: use ceil method
        let pos = (percentile * n as f64).ceil() as usize;
        let idx = if pos == 0 { 0 } else { pos - 1 };
        let idx = idx.min(n - 1);

        Some(sorted_values[idx].clone())
    }
}

impl Accumulator for StringPercentileDiscAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let array = &values[0];

        if values.len() >= 2 {
            if let Some(percentile_array) =
                values[1].as_primitive_opt::<arrow::datatypes::Float64Type>()
            {
                if !percentile_array.is_empty() && !percentile_array.is_null(0) {
                    self.percentile = percentile_array.value(0);
                }
            }
        }

        let string_array = as_string_array(&array)?;

        for value in string_array.iter().flatten() {
            self.values.push(value.to_string());
        }

        Ok(())
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

        Ok(vec![
            ScalarValue::List(values_scalar),
            ScalarValue::Float64(Some(self.percentile)),
            ScalarValue::UInt8(Some(1)), // 1 = string type
        ])
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

        if states.len() >= 2 {
            let percentile_array = as_float64_array(&states[1])?;
            if !percentile_array.is_empty() && !percentile_array.is_null(0) {
                self.percentile = percentile_array.value(0);
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.values.is_empty() {
            return Ok(ScalarValue::Utf8(None));
        }

        let mut sorted_values = self.values.clone();
        sorted_values.sort();

        match self.calculate_percentile_disc(&sorted_values, self.percentile) {
            Some(result) => match &self.data_type {
                DataType::Utf8View => Ok(ScalarValue::Utf8View(Some(result))),
                DataType::LargeUtf8 => Ok(ScalarValue::LargeUtf8(Some(result))),
                _ => Ok(ScalarValue::Utf8(Some(result))),
            },
            None => Ok(ScalarValue::Utf8(None)),
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self
                .values
                .iter()
                .map(|s| std::mem::size_of_val(s) + s.len())
                .sum::<usize>()
    }
}

#[derive(Debug)]
pub struct IntervalPercentileDiscAccumulator {
    values: Vec<i64>,
    percentile: f64,
    data_type: DataType,
}

impl IntervalPercentileDiscAccumulator {
    pub fn new(percentile: f64, data_type: DataType) -> Self {
        Self {
            values: Vec::new(),
            percentile,
            data_type,
        }
    }

    fn calculate_percentile_disc(&self, sorted_values: &[i64], percentile: f64) -> Option<i64> {
        if sorted_values.is_empty() {
            return None;
        }

        if sorted_values.len() == 1 {
            return Some(sorted_values[0]);
        }

        let n = sorted_values.len();

        // Discrete percentile: use ceil method
        let pos = (percentile * n as f64).ceil() as usize;
        let idx = if pos == 0 { 0 } else { pos - 1 };
        let idx = idx.min(n - 1);

        Some(sorted_values[idx])
    }
}

impl Accumulator for IntervalPercentileDiscAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let array = &values[0];

        if values.len() >= 2 {
            if let Some(percentile_array) =
                values[1].as_primitive_opt::<arrow::datatypes::Float64Type>()
            {
                if !percentile_array.is_empty() && !percentile_array.is_null(0) {
                    self.percentile = percentile_array.value(0);
                }
            }
        }

        match &self.data_type {
            DataType::Interval(IntervalUnit::YearMonth) => {
                let interval_array =
                    array.as_primitive::<arrow::datatypes::IntervalYearMonthType>();
                for value in interval_array.iter().flatten() {
                    self.values.push(value as i64);
                }
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                let interval_array = array.as_primitive::<IntervalDayTimeType>();
                for value in interval_array.iter().flatten() {
                    let total_millis = value.days as i64 * 86_400_000 + value.milliseconds as i64;
                    self.values.push(total_millis);
                }
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                let interval_array = array.as_primitive::<IntervalMonthDayNanoType>();
                for value in interval_array.iter().flatten() {
                    let total_nanos = value.months as i64 * 2_592_000_000_000_000
                        + value.days as i64 * 86_400_000_000_000
                        + value.nanoseconds;
                    self.values.push(total_nanos);
                }
            }
            DataType::Duration(unit) => match unit {
                arrow::datatypes::TimeUnit::Second => {
                    let duration_array = array.as_primitive::<DurationSecondType>();
                    for value in duration_array.iter().flatten() {
                        self.values.push(value);
                    }
                }
                arrow::datatypes::TimeUnit::Millisecond => {
                    let duration_array = array.as_primitive::<DurationMillisecondType>();
                    for value in duration_array.iter().flatten() {
                        self.values.push(value);
                    }
                }
                arrow::datatypes::TimeUnit::Microsecond => {
                    let duration_array = array.as_primitive::<DurationMicrosecondType>();
                    for value in duration_array.iter().flatten() {
                        self.values.push(value);
                    }
                }
                arrow::datatypes::TimeUnit::Nanosecond => {
                    let duration_array = array.as_primitive::<DurationNanosecondType>();
                    for value in duration_array.iter().flatten() {
                        self.values.push(value);
                    }
                }
            },
            _ => {}
        }

        Ok(())
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

        Ok(vec![
            ScalarValue::List(values_scalar),
            ScalarValue::Float64(Some(self.percentile)),
            ScalarValue::UInt8(Some(2)), // 2 = interval/duration type
        ])
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
                    let int64_array = value_array.as_primitive::<arrow::datatypes::Int64Type>();

                    for value in int64_array.iter().flatten() {
                        self.values.push(value);
                    }
                }
            }
        }

        if states.len() >= 2 {
            let percentile_array = as_float64_array(&states[1])?;
            if !percentile_array.is_empty() && !percentile_array.is_null(0) {
                self.percentile = percentile_array.value(0);
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.values.is_empty() {
            return match &self.data_type {
                DataType::Interval(IntervalUnit::YearMonth) => {
                    Ok(ScalarValue::IntervalYearMonth(None))
                }
                DataType::Interval(IntervalUnit::DayTime) => Ok(ScalarValue::IntervalDayTime(None)),
                DataType::Interval(IntervalUnit::MonthDayNano) => {
                    Ok(ScalarValue::IntervalMonthDayNano(None))
                }
                DataType::Duration(unit) => match unit {
                    arrow::datatypes::TimeUnit::Second => Ok(ScalarValue::DurationSecond(None)),
                    arrow::datatypes::TimeUnit::Millisecond => {
                        Ok(ScalarValue::DurationMillisecond(None))
                    }
                    arrow::datatypes::TimeUnit::Microsecond => {
                        Ok(ScalarValue::DurationMicrosecond(None))
                    }
                    arrow::datatypes::TimeUnit::Nanosecond => {
                        Ok(ScalarValue::DurationNanosecond(None))
                    }
                },
                _ => Ok(ScalarValue::Int64(None)),
            };
        }

        let mut sorted_values = self.values.clone();
        sorted_values.sort();

        match self.calculate_percentile_disc(&sorted_values, self.percentile) {
            Some(result) => match &self.data_type {
                DataType::Interval(IntervalUnit::YearMonth) => {
                    Ok(ScalarValue::IntervalYearMonth(Some(result as i32)))
                }
                DataType::Interval(IntervalUnit::DayTime) => {
                    use datafusion::arrow::datatypes::IntervalDayTime;
                    let days = (result / 86_400_000) as i32;
                    let milliseconds = (result % 86_400_000) as i32;
                    Ok(ScalarValue::IntervalDayTime(Some(IntervalDayTime {
                        days,
                        milliseconds,
                    })))
                }
                DataType::Interval(IntervalUnit::MonthDayNano) => {
                    use datafusion::arrow::datatypes::IntervalMonthDayNano;
                    let months = (result / 2_592_000_000_000_000) as i32;
                    let remaining = result % 2_592_000_000_000_000;
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
                DataType::Duration(unit) => match unit {
                    arrow::datatypes::TimeUnit::Second => {
                        Ok(ScalarValue::DurationSecond(Some(result)))
                    }
                    arrow::datatypes::TimeUnit::Millisecond => {
                        Ok(ScalarValue::DurationMillisecond(Some(result)))
                    }
                    arrow::datatypes::TimeUnit::Microsecond => {
                        Ok(ScalarValue::DurationMicrosecond(Some(result)))
                    }
                    arrow::datatypes::TimeUnit::Nanosecond => {
                        Ok(ScalarValue::DurationNanosecond(Some(result)))
                    }
                },
                _ => Ok(ScalarValue::Int64(Some(result))),
            },
            None => Ok(ScalarValue::Int64(None)),
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.values.capacity() * std::mem::size_of::<i64>()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Float64Array, StringArray};

    use super::*;

    #[test]
    fn test_numeric_percentile_disc_basic() -> Result<()> {
        let mut acc = NumericPercentileDiscAccumulator::new(0.5);

        let array = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]));
        acc.update_batch(&[array])?;

        let result = acc.evaluate()?;
        assert_eq!(result, ScalarValue::Float64(Some(3.0)));
        Ok(())
    }

    #[test]
    fn test_numeric_percentile_disc_basic_2() -> Result<()> {
        let mut acc = NumericPercentileDiscAccumulator::new(0.5);

        let array = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0]));
        acc.update_batch(&[array])?;

        let result = acc.evaluate()?;
        assert_eq!(result, ScalarValue::Float64(Some(2.0)));
        Ok(())
    }

    #[test]
    fn test_numeric_percentile_disc_25() -> Result<()> {
        let mut acc = NumericPercentileDiscAccumulator::new(0.25);

        let array = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]));
        acc.update_batch(&[array])?;

        let result = acc.evaluate()?;
        // ceil(0.25 * 5) - 1 = ceil(1.25) - 1 = 2 - 1 = 1, so value at index 1 = 2.0
        assert_eq!(result, ScalarValue::Float64(Some(2.0)));
        Ok(())
    }

    #[test]
    fn test_numeric_percentile_disc_75() -> Result<()> {
        let mut acc = NumericPercentileDiscAccumulator::new(0.75);

        let array = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]));
        acc.update_batch(&[array])?;

        let result = acc.evaluate()?;
        // ceil(0.75 * 5) - 1 = ceil(3.75) - 1 = 4 - 1 = 3, so value at index 3 = 4.0
        assert_eq!(result, ScalarValue::Float64(Some(4.0)));
        Ok(())
    }

    #[test]
    fn test_string_percentile_disc() -> Result<()> {
        let mut acc = StringPercentileDiscAccumulator::new(0.5, DataType::Utf8);

        let array = Arc::new(StringArray::from(vec!["apple", "banana", "cherry"]));
        acc.update_batch(&[array])?;

        let result = acc.evaluate()?;
        // Sorted: apple, banana, cherry. ceil(0.5 * 3) - 1 = 2 - 1 = 1, so "banana"
        assert_eq!(result, ScalarValue::Utf8(Some("banana".to_string())));
        Ok(())
    }

    #[test]
    fn test_interval_year_month_percentile_disc() -> Result<()> {
        let mut acc = IntervalPercentileDiscAccumulator::new(
            0.5,
            DataType::Interval(IntervalUnit::YearMonth),
        );

        let array = Arc::new(arrow::array::IntervalYearMonthArray::from(vec![0, 10, 20]));
        acc.update_batch(&[array])?;

        let result = acc.evaluate()?;
        // Values: [0, 10, 20]. ceil(0.5 * 3) - 1 = 2 - 1 = 1, so 10 months
        assert_eq!(result, ScalarValue::IntervalYearMonth(Some(10)));
        Ok(())
    }

    #[test]
    fn test_empty_values() -> Result<()> {
        let mut acc = NumericPercentileDiscAccumulator::new(0.5);
        let result = acc.evaluate()?;
        assert_eq!(result, ScalarValue::Float64(None));
        Ok(())
    }

    #[test]
    fn test_single_value() -> Result<()> {
        let mut acc = NumericPercentileDiscAccumulator::new(0.5);

        let array = Arc::new(Float64Array::from(vec![42.0]));
        acc.update_batch(&[array])?;

        let result = acc.evaluate()?;
        assert_eq!(result, ScalarValue::Float64(Some(42.0)));
        Ok(())
    }
}
