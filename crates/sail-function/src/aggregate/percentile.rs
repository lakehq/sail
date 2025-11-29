use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, RecordBatch, RecordBatchOptions};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::cast::{as_float64_array, as_string_array};
use datafusion::common::ScalarValue;
use datafusion::error::Result;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_common::DataFusionError;
use datafusion_expr_common::columnar_value::ColumnarValue;

macro_rules! match_string_type {
    ($data_type:expr, $value:expr) => {
        match $data_type {
            DataType::Utf8View => Ok(ScalarValue::Utf8View($value)),
            DataType::LargeUtf8 => Ok(ScalarValue::LargeUtf8($value)),
            _ => Ok(ScalarValue::Utf8($value)),
        }
    };
}

macro_rules! interval_none {
    ($unit:expr) => {
        match $unit {
            datafusion::arrow::datatypes::IntervalUnit::YearMonth => {
                Ok(ScalarValue::IntervalYearMonth(None))
            }
            datafusion::arrow::datatypes::IntervalUnit::DayTime => {
                Ok(ScalarValue::IntervalDayTime(None))
            }
            datafusion::arrow::datatypes::IntervalUnit::MonthDayNano => {
                Ok(ScalarValue::IntervalMonthDayNano(None))
            }
        }
    };
}

/// Macro to handle Duration type matching for None values
macro_rules! duration_none {
    ($unit:expr) => {
        match $unit {
            datafusion::arrow::datatypes::TimeUnit::Second => Ok(ScalarValue::DurationSecond(None)),
            datafusion::arrow::datatypes::TimeUnit::Millisecond => {
                Ok(ScalarValue::DurationMillisecond(None))
            }
            datafusion::arrow::datatypes::TimeUnit::Microsecond => {
                Ok(ScalarValue::DurationMicrosecond(None))
            }
            datafusion::arrow::datatypes::TimeUnit::Nanosecond => {
                Ok(ScalarValue::DurationNanosecond(None))
            }
        }
    };
}

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

        // El percentil debe venir SIEMPRE como segundo parámetro
        let expr: &Arc<dyn PhysicalExpr> = acc_args.exprs.get(1).ok_or_else(|| {
            DataFusionError::Execution(
                "percentile() requires a second argument (percentile value)".into(),
            )
        })?;

        fn dummy_batch() -> RecordBatch {
            let fields: Vec<Field> = Vec::new();
            let schema: SchemaRef = Arc::new(Schema::new(fields));

            RecordBatch::try_new_with_options(
                schema,
                Vec::new(),
                &RecordBatchOptions::default().with_row_count(Some(1)),
            )
            .expect("failed to create dummy batch for percentile literal")
        }
        let batch = dummy_batch();

        let col_val = expr.evaluate(&batch)?;

        let scalar = match col_val {
            ColumnarValue::Scalar(s) => s,
            ColumnarValue::Array(arr) => ScalarValue::try_from_array(arr.as_ref(), 0)?,
        };
        fn scalar_to_f64(sv: &ScalarValue) -> Option<f64> {
            match sv {
                ScalarValue::Float64(Some(v)) => Some(*v),
                ScalarValue::Float32(Some(v)) => Some(*v as f64),

                ScalarValue::Int64(Some(v)) => Some(*v as f64),
                ScalarValue::UInt64(Some(v)) => Some(*v as f64),
                ScalarValue::Int32(Some(v)) => Some(*v as f64),
                ScalarValue::UInt32(Some(v)) => Some(*v as f64),

                ScalarValue::Decimal128(Some(v), _precision, scale) => {
                    Some((*v as f64) / 10f64.powi(*scale as i32))
                }

                _ => None,
            }
        }

        let percentile: f64 = scalar_to_f64(&scalar).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Cannot convert percentile literal {:?} to f64",
                scalar
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

        // Update percentile value if provided as second argument
        if values.len() >= 2 {
            if let Some(percentile_array) =
                values[1].as_primitive_opt::<arrow::datatypes::Float64Type>()
            {
                if !percentile_array.is_empty() && !percentile_array.is_null(0) {
                    self.percentile = percentile_array.value(0);
                }
            }
        }

        // Convert input to Utf8 and collect values
        let string_array = arrow::compute::cast(array, &DataType::Utf8)?;
        let string_array = as_string_array(&string_array)?;

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

        // Update percentile from state if present
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
                    return Err(datafusion::error::DataFusionError::Execution(format!(
                        "IntervalPercentileAccumulator does not support type {:?}",
                        self.data_type
                    )));
                }
            };

            self.values.push(value);
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let values_list = states[0].as_list::<i32>();
        let percentile_array =
            states[1].as_primitive::<datafusion::arrow::datatypes::Float64Type>();

        for i in 0..values_list.len() {
            if values_list.is_null(i) {
                continue;
            }

            if !percentile_array.is_null(i) {
                self.percentile = percentile_array.value(i);
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

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.values.is_empty() {
            return match &self.data_type {
                DataType::Interval(unit) => interval_none!(unit),
                DataType::Duration(unit) => duration_none!(unit),
                _ => Err(datafusion::error::DataFusionError::Execution(format!(
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
                _ => Err(datafusion::error::DataFusionError::Execution(format!(
                    "Unsupported type {:?}",
                    self.data_type
                ))),
            },
            None => match &self.data_type {
                DataType::Interval(unit) => interval_none!(unit),
                DataType::Duration(unit) => duration_none!(unit),
                _ => Err(datafusion::error::DataFusionError::Execution(format!(
                    "Unsupported type {:?}",
                    self.data_type
                ))),
            },
        }
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
            ScalarValue::UInt8(Some(2)), // 2 = interval type
        ])
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(&self.values)
            + self.values.capacity() * std::mem::size_of::<i64>()
            + std::mem::size_of::<f64>()
            + std::mem::size_of::<DataType>()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Float64Array, StringArray};

    use super::*;

    #[test]
    fn test_percentile_0() -> Result<()> {
        let mut acc = NumericPercentileAccumulator::new(0.0);

        let values: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(5.0),
            Some(1.0),
            Some(9.0),
            Some(3.0),
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        // 0th percentile is the minimum value
        assert_eq!(result, ScalarValue::Float64(Some(1.0)));
        Ok(())
    }

    #[test]
    fn test_percentile_25() -> Result<()> {
        let mut acc = NumericPercentileAccumulator::new(0.25);

        let values: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(0.0),
            Some(1.0),
            Some(2.5),
            Some(3.5),
            Some(5.0),
            Some(6.0),
            Some(7.5),
            Some(8.5),
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        // For 8 values, 25th percentile position = (8-1) * 0.25 = 1.75
        // Interpolate between index 1 (1.0) and index 2 (2.5)
        // Result = 1.0 + 0.75 * (2.5 - 1.0) = 1.0 + 0.75 * 1.5 = 2.125
        assert_eq!(result, ScalarValue::Float64(Some(2.125)));
        Ok(())
    }

    #[test]
    fn test_percentile_median() -> Result<()> {
        let mut acc = NumericPercentileAccumulator::new(0.5);

        // Test data from the Ibis test
        let values: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(0.0),
            Some(1.0),
            Some(2.5),
            Some(3.5),
            Some(5.0),
            Some(6.0),
            Some(7.5),
            Some(8.5),
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        // Expected median: (3.5 + 5.0) / 2 = 4.25
        assert_eq!(result, ScalarValue::Float64(Some(4.25)));
        Ok(())
    }

    #[test]
    fn test_percentile_75() -> Result<()> {
        let mut acc = NumericPercentileAccumulator::new(0.75);

        let values: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(0.0),
            Some(1.0),
            Some(2.5),
            Some(3.5),
            Some(5.0),
            Some(6.0),
            Some(7.5),
            Some(8.5),
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        // For 8 values, 75th percentile position = (8-1) * 0.75 = 5.25
        // Interpolate between index 5 (6.0) and index 6 (7.5)
        // Result = 6.0 + 0.25 * (7.5 - 6.0) = 6.0 + 0.375 = 6.375
        assert_eq!(result, ScalarValue::Float64(Some(6.375)));
        Ok(())
    }

    #[test]
    fn test_percentile_100() -> Result<()> {
        let mut acc = NumericPercentileAccumulator::new(1.0);

        let values: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(5.0),
            Some(1.0),
            Some(9.0),
            Some(3.0),
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        // 100th percentile is the maximum value
        assert_eq!(result, ScalarValue::Float64(Some(9.0)));
        Ok(())
    }

    #[test]
    fn test_percentile_empty() -> Result<()> {
        let mut acc = NumericPercentileAccumulator::new(0.5);
        let result = acc.evaluate()?;
        assert_eq!(result, ScalarValue::Float64(None));
        Ok(())
    }

    #[test]
    fn test_percentile_single_value() -> Result<()> {
        let mut acc = NumericPercentileAccumulator::new(0.5);
        let values: ArrayRef = Arc::new(Float64Array::from(vec![Some(42.0)]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, ScalarValue::Float64(Some(42.0)));
        Ok(())
    }

    #[test]
    fn test_percentile_with_nulls() -> Result<()> {
        let mut acc = NumericPercentileAccumulator::new(0.5);

        let values: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(1.0),
            None,
            Some(2.0),
            None,
            Some(3.0),
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        // Median of [1.0, 2.0, 3.0] is 2.0
        assert_eq!(result, ScalarValue::Float64(Some(2.0)));
        Ok(())
    }

    #[test]
    fn test_string_percentile_median() -> Result<()> {
        let mut acc = StringPercentileAccumulator::new(0.5, DataType::Utf8);

        // Test with repeated value "a"
        let values: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), Some("a"), Some("a")]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        // Expected: "a" (median of all "a"s is "a")
        assert_eq!(result, ScalarValue::Utf8(Some("a".to_string())));
        Ok(())
    }

    #[test]
    fn test_string_percentile_sorted() -> Result<()> {
        let mut acc = StringPercentileAccumulator::new(0.5, DataType::Utf8);

        // Test with different strings
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            Some("date"),
            Some("banana"),
            Some("cherry"),
            Some("apple"),
            Some("elderberry"),
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        // Sorted: ["apple", "banana", "cherry", "date", "elderberry"]
        // Median position: (5-1) * 0.5 = 2.0 -> index 2 = "cherry"
        assert_eq!(result, ScalarValue::Utf8(Some("cherry".to_string())));
        Ok(())
    }
    #[test]
    fn test_string_percentile_sorted_null() -> Result<()> {
        let mut acc = StringPercentileAccumulator::new(0.5, DataType::Utf8);

        // Test with different strings
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            Some("date"),
            Some("banana"),
            Some("cherry"),
            None,
            Some("apple"),
            Some("elderberry"),
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        // Sorted: ["apple", "banana", "cherry", "date", "elderberry"]
        // Median position: (5-1) * 0.5 = 2.0 -> index 2 = "cherry"
        assert_eq!(result, ScalarValue::Utf8(Some("cherry".to_string())));
        Ok(())
    }

    #[test]
    fn test_string_percentile_empty() -> Result<()> {
        let mut acc = StringPercentileAccumulator::new(0.5, DataType::Utf8);
        let result = acc.evaluate()?;
        assert_eq!(result, ScalarValue::Utf8(None));
        Ok(())
    }

    #[test]
    fn test_percentile_merge_batches() -> Result<()> {
        use datafusion::arrow::array::UInt8Array;
        use datafusion_common::DataFusionError;

        // Create first accumulator with some values
        let mut acc1 = NumericPercentileAccumulator::new(0.5);
        let values1: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.0), Some(2.0), Some(3.0)]));
        acc1.update_batch(&[values1])?;

        // Create second accumulator with more values
        let mut acc2 = NumericPercentileAccumulator::new(0.5);
        let values2: ArrayRef = Arc::new(Float64Array::from(vec![Some(4.0), Some(5.0), Some(6.0)]));
        acc2.update_batch(&[values2])?;

        // Get state from acc2
        let state2 = acc2.state()?;

        // Convert state to ArrayRef for merging — no `panic!`
        let values_list: ArrayRef = match &state2[0] {
            ScalarValue::List(list_array) => Arc::clone(list_array) as ArrayRef,
            other => {
                return Err(DataFusionError::Internal(format!(
                    "Expected List in percentile accumulator state[0], got: {:?}",
                    other
                )))
            }
        };

        let percentile_array: ArrayRef = Arc::new(Float64Array::from(vec![state2[1]
            .clone()
            .try_into()
            .unwrap_or(0.5_f64)]));

        let data_type_array: ArrayRef = Arc::new(UInt8Array::from(vec![state2[2]
            .clone()
            .try_into()
            .unwrap_or(0u8)]));

        // Merge state2 into acc1
        acc1.merge_batch(&[values_list, percentile_array, data_type_array])?;

        // After merge, acc1 should have [1, 2, 3, 4, 5, 6]
        // Median = (3 + 4) / 2 = 3.5
        let result = acc1.evaluate()?;
        assert_eq!(result, ScalarValue::Float64(Some(3.5)));

        Ok(())
    }

    #[test]
    fn test_percentile_group_by_scenario() -> Result<()> {
        // Simulate the scenario from the user's test case with GROUP BY
        // Group 1: Java earnings [20000, 22000, 30000] -> median should be 22000
        let mut acc_java = NumericPercentileAccumulator::new(0.5);
        let java_values: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(20000.0),
            Some(22000.0),
            Some(30000.0),
        ]));
        acc_java.update_batch(&[java_values])?;
        let java_result = acc_java.evaluate()?;
        assert_eq!(java_result, ScalarValue::Float64(Some(22000.0)));

        // Group 2: dotNET earnings [5000, 10000, 48000] -> median should be 10000
        let mut acc_dotnet = NumericPercentileAccumulator::new(0.5);
        let dotnet_values: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(5000.0),
            Some(10000.0),
            Some(48000.0),
        ]));
        acc_dotnet.update_batch(&[dotnet_values])?;
        let dotnet_result = acc_dotnet.evaluate()?;
        assert_eq!(dotnet_result, ScalarValue::Float64(Some(10000.0)));

        Ok(())
    }

    #[test]
    fn test_percentile_group_by_scenario_2() -> Result<()> {
        let mut acc_java = NumericPercentileAccumulator::new(0.5);
        let java_values: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(2.0),
            Some(2.0),
            Some(22000.0),
            Some(30000.0),
        ]));
        acc_java.update_batch(&[java_values])?;
        let java_result = acc_java.evaluate()?;
        assert_eq!(java_result, ScalarValue::Float64(Some(11001.0)));

        Ok(())
    }

    #[test]
    fn test_percentile_two_values() -> Result<()> {
        let mut acc = NumericPercentileAccumulator::new(0.5);

        let values: ArrayRef = Arc::new(Float64Array::from(vec![Some(10.0), Some(20.0)]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        // Median of [10, 20] with interpolation: 10 + 0.5 * (20 - 10) = 15
        assert_eq!(result, ScalarValue::Float64(Some(15.0)));
        Ok(())
    }

    #[test]
    fn test_interval_year_month_percentile_50() -> Result<()> {
        use datafusion::arrow::array::IntervalYearMonthArray;

        let mut acc = IntervalPercentileAccumulator::new(
            0.5,
            DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::YearMonth),
        );

        // VALUES: INTERVAL '0' MONTH, INTERVAL '10' MONTH
        // Expected: INTERVAL '5' MONTH (0-5 in YEAR TO MONTH format)
        let values: ArrayRef = Arc::new(IntervalYearMonthArray::from(vec![Some(0), Some(10)]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        // Percentile 0.5 of [0, 10] = 0 + 0.5 * (10 - 0) = 5 months
        assert_eq!(result, ScalarValue::IntervalYearMonth(Some(5)));
        Ok(())
    }
    use datafusion_common::{DataFusionError, Result};

    #[test]
    fn test_interval_year_month_percentile_25() -> Result<()> {
        use datafusion::arrow::array::IntervalYearMonthArray;

        let mut acc = IntervalPercentileAccumulator::new(
            0.25,
            DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::YearMonth),
        );

        let values: ArrayRef = Arc::new(IntervalYearMonthArray::from(vec![Some(0), Some(10)]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        match result {
            ScalarValue::IntervalYearMonth(Some(v)) => {
                assert!((2..=3).contains(&v));
                Ok(())
            }
            other => Err(DataFusionError::Internal(format!(
                "Expected IntervalYearMonth, got: {:?}",
                other
            ))),
        }
    }

    #[test]
    fn test_interval_day_time_percentile_50() -> Result<()> {
        use datafusion::arrow::array::IntervalDayTimeArray;
        use datafusion::arrow::datatypes::IntervalDayTime;

        let mut acc = IntervalPercentileAccumulator::new(
            0.5,
            DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::DayTime),
        );

        let val1 = IntervalDayTime {
            days: 0,
            milliseconds: 0,
        };
        let val2 = IntervalDayTime {
            days: 0,
            milliseconds: 10000,
        };

        let values: ArrayRef = Arc::new(IntervalDayTimeArray::from(vec![Some(val1), Some(val2)]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        match result {
            ScalarValue::IntervalDayTime(Some(interval)) => {
                assert_eq!(interval.days, 0);
                assert_eq!(interval.milliseconds, 5000);
                Ok(())
            }
            other => Err(DataFusionError::Internal(format!(
                "Expected IntervalDayTime, got: {:?}",
                other
            ))),
        }
    }

    #[test]
    fn test_interval_year_month_multiple_values() -> Result<()> {
        use datafusion::arrow::array::IntervalYearMonthArray;

        let mut acc = IntervalPercentileAccumulator::new(
            0.5,
            DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::YearMonth),
        );

        let values: ArrayRef = Arc::new(IntervalYearMonthArray::from(vec![
            Some(0),
            Some(1),
            Some(2),
            Some(10),
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        match result {
            ScalarValue::IntervalYearMonth(Some(v)) => {
                assert!((1..=2).contains(&v));
                Ok(())
            }
            other => Err(DataFusionError::Internal(format!(
                "Expected IntervalYearMonth, got: {:?}",
                other
            ))),
        }
    }

    #[test]
    fn test_interval_with_nulls() -> Result<()> {
        use datafusion::arrow::array::IntervalYearMonthArray;

        let mut acc = IntervalPercentileAccumulator::new(
            0.5,
            DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::YearMonth),
        );

        let values: ArrayRef = Arc::new(IntervalYearMonthArray::from(vec![
            Some(0),
            None,
            Some(10),
            None,
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, ScalarValue::IntervalYearMonth(Some(5)));
        Ok(())
    }

    #[test]
    fn test_interval_empty() -> Result<()> {
        let mut acc = IntervalPercentileAccumulator::new(
            0.5,
            DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::YearMonth),
        );

        let result = acc.evaluate()?;

        assert_eq!(result, ScalarValue::IntervalYearMonth(None));
        Ok(())
    }

    #[test]
    fn test_interval_single_value() -> Result<()> {
        use datafusion::arrow::array::IntervalYearMonthArray;

        let mut acc = IntervalPercentileAccumulator::new(
            0.5,
            DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::YearMonth),
        );

        let values: ArrayRef = Arc::new(IntervalYearMonthArray::from(vec![Some(12)]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, ScalarValue::IntervalYearMonth(Some(12)));
        Ok(())
    }

    #[test]
    fn test_duration_second_percentile() -> Result<()> {
        use datafusion::arrow::array::DurationSecondArray;

        let mut acc = IntervalPercentileAccumulator::new(
            0.5,
            DataType::Duration(datafusion::arrow::datatypes::TimeUnit::Second),
        );

        let values: ArrayRef = Arc::new(DurationSecondArray::from(vec![Some(0), Some(100)]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, ScalarValue::DurationSecond(Some(50)));
        Ok(())
    }

    #[test]
    fn test_interval_day_time_with_days() -> Result<()> {
        use datafusion::arrow::array::IntervalDayTimeArray;
        use datafusion::arrow::datatypes::IntervalDayTime;

        let mut acc = IntervalPercentileAccumulator::new(
            0.5,
            DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::DayTime),
        );

        let val1 = IntervalDayTime {
            days: 0,
            milliseconds: 0,
        };
        let val2 = IntervalDayTime {
            days: 2,
            milliseconds: 0,
        };

        let values: ArrayRef = Arc::new(IntervalDayTimeArray::from(vec![Some(val1), Some(val2)]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        match result {
            ScalarValue::IntervalDayTime(Some(interval)) => {
                assert_eq!(interval.days, 1);
                assert_eq!(interval.milliseconds, 0);
                Ok(())
            }
            other => Err(DataFusionError::Internal(format!(
                "Expected IntervalDayTime, got: {:?}",
                other
            ))),
        }
    }

    #[test]
    fn test_interval_percentile_0() -> Result<()> {
        use datafusion::arrow::array::IntervalYearMonthArray;

        let mut acc = IntervalPercentileAccumulator::new(
            0.0,
            DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::YearMonth),
        );

        let values: ArrayRef = Arc::new(IntervalYearMonthArray::from(vec![
            Some(5),
            Some(10),
            Some(15),
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, ScalarValue::IntervalYearMonth(Some(5)));
        Ok(())
    }

    #[test]
    fn test_interval_percentile_100() -> Result<()> {
        use datafusion::arrow::array::IntervalYearMonthArray;

        let mut acc = IntervalPercentileAccumulator::new(
            1.0,
            DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::YearMonth),
        );

        let values: ArrayRef = Arc::new(IntervalYearMonthArray::from(vec![
            Some(5),
            Some(10),
            Some(15),
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, ScalarValue::IntervalYearMonth(Some(15)));
        Ok(())
    }
}
