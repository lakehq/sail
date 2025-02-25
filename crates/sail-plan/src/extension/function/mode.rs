use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

/// [Credit]: <https://github.com/datafusion-contrib/datafusion-functions-extra/blob/5fa184df2589f09e90035c5e6a0d2c88c57c298a/src/mode.rs>
use datafusion::arrow;
use datafusion::arrow::array::{ArrayAccessor, ArrayIter, ArrayRef, ArrowPrimitiveType, AsArray};
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Date64Type, Field, Float16Type, Float32Type, Float64Type, Int16Type,
    Int32Type, Int64Type, Int8Type, Time32MillisecondType, Time32SecondType, Time64MicrosecondType,
    Time64NanosecondType, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use datafusion::common::cast::{as_primitive_array, as_string_array};
use datafusion::common::not_impl_err;
use datafusion::error::Result;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion::physical_expr::aggregate::utils::Hashable;
use datafusion::scalar::ScalarValue;

/// The `ModeFunction` calculates the mode (most frequent value) from a set of values.
///
/// - Null values are ignored during the calculation.
/// - If multiple values have the same frequency, the MAX value with the highest frequency is returned.
pub struct ModeFunction {
    signature: Signature,
}

impl Debug for ModeFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ModeFunction")
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for ModeFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl ModeFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for ModeFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "mode"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        let value_type = args.input_types[0].clone();

        Ok(vec![
            Field::new("values", value_type, true),
            Field::new("frequencies", DataType::UInt64, true),
        ])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let data_type = &acc_args.exprs[0].data_type(acc_args.schema)?;

        let accumulator: Box<dyn Accumulator> = match data_type {
            DataType::Int8 => Box::new(PrimitiveModeAccumulator::<Int8Type>::new(data_type)),
            DataType::Int16 => Box::new(PrimitiveModeAccumulator::<Int16Type>::new(data_type)),
            DataType::Int32 => Box::new(PrimitiveModeAccumulator::<Int32Type>::new(data_type)),
            DataType::Int64 => Box::new(PrimitiveModeAccumulator::<Int64Type>::new(data_type)),
            DataType::UInt8 => Box::new(PrimitiveModeAccumulator::<UInt8Type>::new(data_type)),
            DataType::UInt16 => Box::new(PrimitiveModeAccumulator::<UInt16Type>::new(data_type)),
            DataType::UInt32 => Box::new(PrimitiveModeAccumulator::<UInt32Type>::new(data_type)),
            DataType::UInt64 => Box::new(PrimitiveModeAccumulator::<UInt64Type>::new(data_type)),

            DataType::Date32 => Box::new(PrimitiveModeAccumulator::<Date32Type>::new(data_type)),
            DataType::Date64 => Box::new(PrimitiveModeAccumulator::<Date64Type>::new(data_type)),
            DataType::Time32(TimeUnit::Millisecond) => Box::new(PrimitiveModeAccumulator::<
                Time32MillisecondType,
            >::new(data_type)),
            DataType::Time32(TimeUnit::Second) => {
                Box::new(PrimitiveModeAccumulator::<Time32SecondType>::new(data_type))
            }
            DataType::Time64(TimeUnit::Microsecond) => Box::new(PrimitiveModeAccumulator::<
                Time64MicrosecondType,
            >::new(data_type)),
            DataType::Time64(TimeUnit::Nanosecond) => Box::new(PrimitiveModeAccumulator::<
                Time64NanosecondType,
            >::new(data_type)),
            DataType::Timestamp(TimeUnit::Microsecond, _) => Box::new(PrimitiveModeAccumulator::<
                TimestampMicrosecondType,
            >::new(data_type)),
            DataType::Timestamp(TimeUnit::Millisecond, _) => Box::new(PrimitiveModeAccumulator::<
                TimestampMillisecondType,
            >::new(data_type)),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => Box::new(PrimitiveModeAccumulator::<
                TimestampNanosecondType,
            >::new(data_type)),
            DataType::Timestamp(TimeUnit::Second, _) => Box::new(PrimitiveModeAccumulator::<
                TimestampSecondType,
            >::new(data_type)),

            DataType::Float16 => Box::new(FloatModeAccumulator::<Float16Type>::new(data_type)),
            DataType::Float32 => Box::new(FloatModeAccumulator::<Float32Type>::new(data_type)),
            DataType::Float64 => Box::new(FloatModeAccumulator::<Float64Type>::new(data_type)),

            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => {
                Box::new(BytesModeAccumulator::new(data_type))
            }
            _ => {
                return not_impl_err!("Unsupported data type: {:?} for mode function", data_type);
            }
        };

        Ok(accumulator)
    }
}

/// [Credit]: <https://github.com/datafusion-contrib/datafusion-functions-extra/blob/5fa184df2589f09e90035c5e6a0d2c88c57c298a/src/common/mode/native.rs>
#[derive(Debug)]
pub struct PrimitiveModeAccumulator<T>
where
    T: ArrowPrimitiveType + Send,
    T::Native: Eq + Hash,
{
    value_counts: HashMap<T::Native, i64>,
    data_type: DataType,
}

impl<T> PrimitiveModeAccumulator<T>
where
    T: ArrowPrimitiveType + Send,
    T::Native: Eq + Hash + Clone,
{
    pub fn new(data_type: &DataType) -> Self {
        Self {
            value_counts: HashMap::default(),
            data_type: data_type.clone(),
        }
    }
}

impl<T> Accumulator for PrimitiveModeAccumulator<T>
where
    T: ArrowPrimitiveType + Send + Debug,
    T::Native: Eq + Hash + Clone + PartialOrd + Debug,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let arr = as_primitive_array::<T>(&values[0])?;

        for value in arr.iter().flatten() {
            let counter = self.value_counts.entry(value).or_insert(0);
            *counter += 1;
        }

        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let values: Vec<ScalarValue> = self
            .value_counts
            .keys()
            .map(|key| ScalarValue::new_primitive::<T>(Some(*key), &self.data_type))
            .collect::<Result<Vec<_>>>()?;

        let frequencies: Vec<ScalarValue> = self
            .value_counts
            .values()
            .map(|count| ScalarValue::from(*count))
            .collect();

        let values_scalar = ScalarValue::new_list_nullable(&values, &self.data_type.clone());
        let frequencies_scalar = ScalarValue::new_list_nullable(&frequencies, &DataType::Int64);

        Ok(vec![
            ScalarValue::List(values_scalar),
            ScalarValue::List(frequencies_scalar),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let values_array = as_primitive_array::<T>(&states[0])?;
        let counts_array = as_primitive_array::<arrow::datatypes::Int64Type>(&states[1])?;

        for i in 0..values_array.len() {
            let value = values_array.value(i);
            let count = counts_array.value(i);
            let entry = self.value_counts.entry(value).or_insert(0);
            *entry += count;
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let mut max_value: Option<T::Native> = None;
        let mut max_count: i64 = 0;

        self.value_counts.iter().for_each(|(value, &count)| {
            match count.cmp(&max_count) {
                std::cmp::Ordering::Greater => {
                    max_value = Some(*value);
                    max_count = count;
                }
                std::cmp::Ordering::Equal => {
                    max_value = match max_value {
                        Some(ref current_max_value) if value > current_max_value => Some(*value),
                        Some(ref current_max_value) => Some(*current_max_value),
                        None => Some(*value),
                    };
                }
                _ => {} // Do nothing if count is less than max_count
            }
        });

        match max_value {
            Some(val) => ScalarValue::new_primitive::<T>(Some(val), &self.data_type),
            None => ScalarValue::new_primitive::<T>(None, &self.data_type),
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(&self.value_counts)
            + self.value_counts.len() * std::mem::size_of::<(T::Native, i64)>()
    }
}

#[derive(Debug)]
pub struct FloatModeAccumulator<T>
where
    T: ArrowPrimitiveType,
{
    value_counts: HashMap<Hashable<T::Native>, i64>,
    data_type: DataType,
}

impl<T> FloatModeAccumulator<T>
where
    T: ArrowPrimitiveType,
{
    pub fn new(data_type: &DataType) -> Self {
        Self {
            value_counts: HashMap::default(),
            data_type: data_type.clone(),
        }
    }
}

impl<T> Accumulator for FloatModeAccumulator<T>
where
    T: ArrowPrimitiveType + Send + Debug,
    T::Native: PartialOrd + Debug + Clone,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let arr = as_primitive_array::<T>(&values[0])?;

        for value in arr.iter().flatten() {
            let counter = self.value_counts.entry(Hashable(value)).or_insert(0);
            *counter += 1;
        }

        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let values: Vec<ScalarValue> = self
            .value_counts
            .keys()
            .map(|key| ScalarValue::new_primitive::<T>(Some(key.0), &self.data_type))
            .collect::<Result<Vec<_>>>()?;

        let frequencies: Vec<ScalarValue> = self
            .value_counts
            .values()
            .map(|count| ScalarValue::from(*count))
            .collect();

        let values_scalar = ScalarValue::new_list_nullable(&values, &self.data_type.clone());
        let frequencies_scalar = ScalarValue::new_list_nullable(&frequencies, &DataType::Int64);

        Ok(vec![
            ScalarValue::List(values_scalar),
            ScalarValue::List(frequencies_scalar),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let values_array = as_primitive_array::<T>(&states[0])?;
        let counts_array = as_primitive_array::<arrow::datatypes::Int64Type>(&states[1])?;

        for i in 0..values_array.len() {
            let count = counts_array.value(i);
            let entry = self
                .value_counts
                .entry(Hashable(values_array.value(i)))
                .or_insert(0);
            *entry += count;
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let mut max_value: Option<T::Native> = None;
        let mut max_count: i64 = 0;

        self.value_counts.iter().for_each(|(value, &count)| {
            match count.cmp(&max_count) {
                std::cmp::Ordering::Greater => {
                    max_value = Some(value.0);
                    max_count = count;
                }
                std::cmp::Ordering::Equal => {
                    max_value = match max_value {
                        Some(current_max_value) if value.0 > current_max_value => Some(value.0),
                        Some(current_max_value) => Some(current_max_value),
                        None => Some(value.0),
                    };
                }
                _ => {} // Do nothing if count is less than max_count
            }
        });

        match max_value {
            Some(val) => ScalarValue::new_primitive::<T>(Some(val), &self.data_type),
            None => ScalarValue::new_primitive::<T>(None, &self.data_type),
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(&self.value_counts)
            + self.value_counts.len() * std::mem::size_of::<(Hashable<T::Native>, i64)>()
    }
}

/// [Credit]: <https://github.com/datafusion-contrib/datafusion-functions-extra/blob/5fa184df2589f09e90035c5e6a0d2c88c57c298a/src/common/mode/bytes.rs>
#[derive(Debug)]
pub struct BytesModeAccumulator {
    value_counts: HashMap<String, i64>,
    data_type: DataType,
}

impl BytesModeAccumulator {
    pub fn new(data_type: &DataType) -> Self {
        Self {
            value_counts: HashMap::new(),
            data_type: data_type.clone(),
        }
    }

    fn update_counts<'a, V>(&mut self, array: V)
    where
        V: ArrayAccessor<Item = &'a str>,
    {
        for value in ArrayIter::new(array).flatten() {
            let key = value;
            if let Some(count) = self.value_counts.get_mut(key) {
                *count += 1;
            } else {
                self.value_counts.insert(key.to_string(), 1);
            }
        }
    }
}

impl Accumulator for BytesModeAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        match &self.data_type {
            DataType::Utf8View => {
                let array = values[0].as_string_view();
                self.update_counts(array);
            }
            _ => {
                let array = values[0].as_string::<i32>();
                self.update_counts(array);
            }
        };

        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let values: Vec<ScalarValue> = self
            .value_counts
            .keys()
            .map(|key| ScalarValue::Utf8(Some(key.to_string())))
            .collect();

        let frequencies: Vec<ScalarValue> = self
            .value_counts
            .values()
            .map(|&count| ScalarValue::Int64(Some(count)))
            .collect();

        let values_scalar = ScalarValue::new_list_nullable(&values, &DataType::Utf8);
        let frequencies_scalar = ScalarValue::new_list_nullable(&frequencies, &DataType::Int64);

        Ok(vec![
            ScalarValue::List(values_scalar),
            ScalarValue::List(frequencies_scalar),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let values_array = as_string_array(&states[0])?;
        let counts_array = as_primitive_array::<arrow::datatypes::Int64Type>(&states[1])?;

        for (i, value_option) in values_array.iter().enumerate() {
            if let Some(value) = value_option {
                let count = counts_array.value(i);
                let entry = self.value_counts.entry(value.to_string()).or_insert(0);
                *entry += count;
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.value_counts.is_empty() {
            return match &self.data_type {
                DataType::Utf8View => Ok(ScalarValue::Utf8View(None)),
                _ => Ok(ScalarValue::Utf8(None)),
            };
        }

        let mode = self
            .value_counts
            .iter()
            .max_by(|a, b| {
                // First compare counts
                a.1.cmp(b.1)
                    // If counts are equal, compare keys in reverse order to get the maximum string
                    .then_with(|| b.0.cmp(a.0))
            })
            .map(|(value, _)| value.to_string());

        match mode {
            Some(result) => match &self.data_type {
                DataType::Utf8View => Ok(ScalarValue::Utf8View(Some(result))),
                _ => Ok(ScalarValue::Utf8(Some(result))),
            },
            None => match &self.data_type {
                DataType::Utf8View => Ok(ScalarValue::Utf8View(None)),
                _ => Ok(ScalarValue::Utf8(None)),
            },
        }
    }

    fn size(&self) -> usize {
        self.value_counts.capacity() * std::mem::size_of::<(String, i64)>()
            + std::mem::size_of_val(&self.data_type)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{ArrayRef, GenericByteViewArray, StringArray};

    use super::*;

    #[test]
    fn test_mode_accumulator_single_mode_utf8() -> Result<()> {
        let mut acc = BytesModeAccumulator::new(&DataType::Utf8);
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            Some("apple"),
            Some("banana"),
            Some("apple"),
            Some("orange"),
            Some("banana"),
            Some("apple"),
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, ScalarValue::Utf8(Some("apple".to_string())));
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_tie_utf8() -> Result<()> {
        let mut acc = BytesModeAccumulator::new(&DataType::Utf8);
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            Some("apple"),
            Some("banana"),
            Some("apple"),
            Some("orange"),
            Some("banana"),
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, ScalarValue::Utf8(Some("apple".to_string())));
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_all_nulls_utf8() -> Result<()> {
        let mut acc = BytesModeAccumulator::new(&DataType::Utf8);
        let values: ArrayRef = Arc::new(StringArray::from(vec![None as Option<&str>, None, None]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, ScalarValue::Utf8(None));
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_with_nulls_utf8() -> Result<()> {
        let mut acc = BytesModeAccumulator::new(&DataType::Utf8);
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            Some("apple"),
            None,
            Some("banana"),
            Some("apple"),
            None,
            None,
            None,
            Some("banana"),
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, ScalarValue::Utf8(Some("apple".to_string())));
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_single_mode_utf8view() -> Result<()> {
        let mut acc = BytesModeAccumulator::new(&DataType::Utf8View);
        let values: ArrayRef = Arc::new(GenericByteViewArray::from(vec![
            Some("apple"),
            Some("banana"),
            Some("apple"),
            Some("orange"),
            Some("banana"),
            Some("apple"),
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, ScalarValue::Utf8View(Some("apple".to_string())));
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_tie_utf8view() -> Result<()> {
        let mut acc = BytesModeAccumulator::new(&DataType::Utf8View);
        let values: ArrayRef = Arc::new(GenericByteViewArray::from(vec![
            Some("apple"),
            Some("banana"),
            Some("apple"),
            Some("orange"),
            Some("banana"),
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, ScalarValue::Utf8View(Some("apple".to_string())));
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_all_nulls_utf8view() -> Result<()> {
        let mut acc = BytesModeAccumulator::new(&DataType::Utf8View);
        let values: ArrayRef = Arc::new(GenericByteViewArray::from(vec![
            None as Option<&str>,
            None,
            None,
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, ScalarValue::Utf8View(None));
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_with_nulls_utf8view() -> Result<()> {
        let mut acc = BytesModeAccumulator::new(&DataType::Utf8View);
        let values: ArrayRef = Arc::new(GenericByteViewArray::from(vec![
            Some("apple"),
            None,
            Some("banana"),
            Some("apple"),
            None,
            None,
            None,
            Some("banana"),
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, ScalarValue::Utf8View(Some("apple".to_string())));
        Ok(())
    }
}
