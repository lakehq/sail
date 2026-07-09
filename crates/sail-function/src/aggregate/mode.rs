use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

/// [Credit]: <https://github.com/datafusion-contrib/datafusion-functions-extra/blob/5fa184df2589f09e90035c5e6a0d2c88c57c298a/src/mode.rs>
use datafusion::arrow::array::{
    Array, ArrayAccessor, ArrayIter, ArrayRef, ArrowPrimitiveType, AsArray,
};
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Date64Type, Field, FieldRef, Float16Type, Float32Type, Float64Type,
    Int8Type, Int16Type, Int32Type, Int64Type, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use datafusion::common::cast::{as_list_array, as_primitive_array, as_string_array};
use datafusion::common::{exec_err, not_impl_err};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion::physical_expr::aggregate::utils::Hashable;
use datafusion::scalar::ScalarValue;

use super::utils::get_scalar_value;

/// The `ModeFunction` calculates the mode (most frequent value) from a set of values.
///
/// - Null values are ignored during the calculation.
/// - If multiple values have the same frequency, the MAX value with the highest frequency is returned.
/// - If the optional second argument `deterministic` is true (Spark's `mode(col, deterministic)`),
///   the LOWEST value among the most frequent values is returned instead.
/// - Spark's `mode() WITHIN GROUP (ORDER BY col)` SQL syntax is rewritten by the plan
///   resolver into the `"lowest"` (ascending) / `"highest"` (descending) tie-break
///   sentinels as the second argument; see [`TieBreak`].
#[derive(PartialEq, Eq, Hash)]
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
    fn name(&self) -> &str {
        "mode"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let data_type = &acc_args.exprs[0].data_type(acc_args.schema)?;
        let tie_break = resolve_tie_break(&acc_args)?;

        let accumulator: Box<dyn Accumulator> = match data_type {
            DataType::Int8 => Box::new(PrimitiveModeAccumulator::<Int8Type>::new(
                data_type, tie_break,
            )),
            DataType::Int16 => Box::new(PrimitiveModeAccumulator::<Int16Type>::new(
                data_type, tie_break,
            )),
            DataType::Int32 => Box::new(PrimitiveModeAccumulator::<Int32Type>::new(
                data_type, tie_break,
            )),
            DataType::Int64 => Box::new(PrimitiveModeAccumulator::<Int64Type>::new(
                data_type, tie_break,
            )),
            DataType::UInt8 => Box::new(PrimitiveModeAccumulator::<UInt8Type>::new(
                data_type, tie_break,
            )),
            DataType::UInt16 => Box::new(PrimitiveModeAccumulator::<UInt16Type>::new(
                data_type, tie_break,
            )),
            DataType::UInt32 => Box::new(PrimitiveModeAccumulator::<UInt32Type>::new(
                data_type, tie_break,
            )),
            DataType::UInt64 => Box::new(PrimitiveModeAccumulator::<UInt64Type>::new(
                data_type, tie_break,
            )),

            DataType::Date32 => Box::new(PrimitiveModeAccumulator::<Date32Type>::new(
                data_type, tie_break,
            )),
            DataType::Date64 => Box::new(PrimitiveModeAccumulator::<Date64Type>::new(
                data_type, tie_break,
            )),
            DataType::Time32(TimeUnit::Millisecond) => {
                Box::new(PrimitiveModeAccumulator::<Time32MillisecondType>::new(
                    data_type, tie_break,
                ))
            }
            DataType::Time32(TimeUnit::Second) => Box::new(PrimitiveModeAccumulator::<
                Time32SecondType,
            >::new(data_type, tie_break)),
            DataType::Time64(TimeUnit::Microsecond) => {
                Box::new(PrimitiveModeAccumulator::<Time64MicrosecondType>::new(
                    data_type, tie_break,
                ))
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                Box::new(PrimitiveModeAccumulator::<Time64NanosecondType>::new(
                    data_type, tie_break,
                ))
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                Box::new(PrimitiveModeAccumulator::<TimestampMicrosecondType>::new(
                    data_type, tie_break,
                ))
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                Box::new(PrimitiveModeAccumulator::<TimestampMillisecondType>::new(
                    data_type, tie_break,
                ))
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                Box::new(PrimitiveModeAccumulator::<TimestampNanosecondType>::new(
                    data_type, tie_break,
                ))
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                Box::new(PrimitiveModeAccumulator::<TimestampSecondType>::new(
                    data_type, tie_break,
                ))
            }

            DataType::Float16 => Box::new(FloatModeAccumulator::<Float16Type>::new(
                data_type, tie_break,
            )),
            DataType::Float32 => Box::new(FloatModeAccumulator::<Float32Type>::new(
                data_type, tie_break,
            )),
            DataType::Float64 => Box::new(FloatModeAccumulator::<Float64Type>::new(
                data_type, tie_break,
            )),

            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => {
                Box::new(BytesModeAccumulator::new(data_type, tie_break))
            }
            _ => {
                return not_impl_err!("Unsupported data type: {:?} for mode function", data_type);
            }
        };

        Ok(accumulator)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        // The accumulators emit their state as lists in `state()`, so the state fields
        // must be declared as list types for the partial aggregation output schema to
        // match. `BytesModeAccumulator` stores its state values as `Utf8` regardless of
        // the input string type, and all accumulators emit `Int64` frequencies.
        let value_type = match args.input_fields[0].data_type() {
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => DataType::Utf8,
            other => other.clone(),
        };

        Ok(vec![
            Field::new(
                "values",
                DataType::List(Arc::new(Field::new_list_field(value_type, true))),
                true,
            )
            .into(),
            Field::new(
                "frequencies",
                DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true))),
                true,
            )
            .into(),
        ])
    }
}

/// How ties between equally frequent values are broken.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TieBreak {
    /// No particular value is required among the most frequent values.
    Any,
    /// Return the lowest of the most frequent values.
    Lowest,
    /// Return the highest of the most frequent values.
    Highest,
}

/// Resolves the tie-breaking strategy from the optional second argument.
///
/// Spark passes `mode(col, deterministic)` with a boolean literal, where true means
/// the lowest value among the most frequent values is returned. The plan resolver
/// rewrites `mode() WITHIN GROUP (ORDER BY col)` to the string sentinels `"lowest"`
/// (ascending order) and `"highest"` (descending order).
fn resolve_tie_break(args: &AccumulatorArgs) -> Result<TieBreak> {
    let Some(expr) = args.exprs.get(1) else {
        return Ok(TieBreak::Any);
    };
    let value = get_scalar_value(expr).map_err(|_| {
        DataFusionError::Plan("mode requires deterministic to be a constant boolean".to_string())
    })?;
    match value {
        ScalarValue::Boolean(Some(false)) => Ok(TieBreak::Any),
        ScalarValue::Boolean(Some(true)) => Ok(TieBreak::Lowest),
        ScalarValue::Utf8(Some(value))
        | ScalarValue::LargeUtf8(Some(value))
        | ScalarValue::Utf8View(Some(value)) => match value.as_str() {
            "lowest" => Ok(TieBreak::Lowest),
            "highest" => Ok(TieBreak::Highest),
            value => exec_err!("invalid tie-break strategy for mode: {value}"),
        },
        value => exec_err!(
            "mode requires deterministic to be a non-null boolean literal, got {}",
            value.data_type()
        ),
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
    tie_break: TieBreak,
}

impl<T> PrimitiveModeAccumulator<T>
where
    T: ArrowPrimitiveType + Send,
    T::Native: Eq + Hash + Clone,
{
    pub fn new(data_type: &DataType, tie_break: TieBreak) -> Self {
        Self {
            value_counts: HashMap::default(),
            data_type: data_type.clone(),
            tie_break,
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
                        Some(ref current_max_value)
                            if self.tie_break == TieBreak::Lowest && value < current_max_value =>
                        {
                            Some(*value)
                        }
                        Some(ref current_max_value)
                            if self.tie_break != TieBreak::Lowest && value > current_max_value =>
                        {
                            Some(*value)
                        }
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
        size_of_val(&self.value_counts) + self.value_counts.len() * size_of::<(T::Native, i64)>()
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

        let values_list = as_list_array(&states[0])?;
        let counts_list = as_list_array(&states[1])?;

        for i in 0..values_list.len() {
            if values_list.is_null(i) || counts_list.is_null(i) {
                continue;
            }
            let values = values_list.value(i);
            let counts = counts_list.value(i);
            let values_array = as_primitive_array::<T>(&values)?;
            let counts_array = as_primitive_array::<Int64Type>(&counts)?;

            for j in 0..values_array.len() {
                let value = values_array.value(j);
                let count = counts_array.value(j);
                let entry = self.value_counts.entry(value).or_insert(0);
                *entry += count;
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct FloatModeAccumulator<T>
where
    T: ArrowPrimitiveType,
{
    value_counts: HashMap<Hashable<T::Native>, i64>,
    data_type: DataType,
    tie_break: TieBreak,
}

impl<T> FloatModeAccumulator<T>
where
    T: ArrowPrimitiveType,
{
    pub fn new(data_type: &DataType, tie_break: TieBreak) -> Self {
        Self {
            value_counts: HashMap::default(),
            data_type: data_type.clone(),
            tie_break,
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
                        Some(current_max_value)
                            if self.tie_break == TieBreak::Lowest
                                && value.0 < current_max_value =>
                        {
                            Some(value.0)
                        }
                        Some(current_max_value)
                            if self.tie_break != TieBreak::Lowest
                                && value.0 > current_max_value =>
                        {
                            Some(value.0)
                        }
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
        size_of_val(&self.value_counts)
            + self.value_counts.len() * size_of::<(Hashable<T::Native>, i64)>()
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

        let values_list = as_list_array(&states[0])?;
        let counts_list = as_list_array(&states[1])?;

        for i in 0..values_list.len() {
            if values_list.is_null(i) || counts_list.is_null(i) {
                continue;
            }
            let values = values_list.value(i);
            let counts = counts_list.value(i);
            let values_array = as_primitive_array::<T>(&values)?;
            let counts_array = as_primitive_array::<Int64Type>(&counts)?;

            for j in 0..values_array.len() {
                let count = counts_array.value(j);
                let entry = self
                    .value_counts
                    .entry(Hashable(values_array.value(j)))
                    .or_insert(0);
                *entry += count;
            }
        }

        Ok(())
    }
}

/// [Credit]: <https://github.com/datafusion-contrib/datafusion-functions-extra/blob/5fa184df2589f09e90035c5e6a0d2c88c57c298a/src/common/mode/bytes.rs>
#[derive(Debug)]
pub struct BytesModeAccumulator {
    value_counts: HashMap<String, i64>,
    data_type: DataType,
    tie_break: TieBreak,
}

impl BytesModeAccumulator {
    pub fn new(data_type: &DataType, tie_break: TieBreak) -> Self {
        Self {
            value_counts: HashMap::new(),
            data_type: data_type.clone(),
            tie_break,
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
                a.1.cmp(b.1).then_with(|| match self.tie_break {
                    TieBreak::Highest => a.0.cmp(b.0),
                    // If counts are equal, compare keys in reverse order to get the maximum string
                    TieBreak::Any | TieBreak::Lowest => b.0.cmp(a.0),
                })
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
        self.value_counts.capacity() * size_of::<(String, i64)>() + size_of_val(&self.data_type)
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

        let values_list = as_list_array(&states[0])?;
        let counts_list = as_list_array(&states[1])?;

        for i in 0..values_list.len() {
            if values_list.is_null(i) || counts_list.is_null(i) {
                continue;
            }
            let values = values_list.value(i);
            let counts = counts_list.value(i);
            let values_array = as_string_array(&values)?;
            let counts_array = as_primitive_array::<Int64Type>(&counts)?;

            for (j, value_option) in values_array.iter().enumerate() {
                if let Some(value) = value_option {
                    let count = counts_array.value(j);
                    let entry = self.value_counts.entry(value.to_string()).or_insert(0);
                    *entry += count;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{ArrayRef, GenericByteViewArray, StringArray};

    use super::*;

    #[test]
    fn test_mode_accumulator_single_mode_utf8() -> Result<()> {
        let mut acc = BytesModeAccumulator::new(&DataType::Utf8, TieBreak::Any);
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
        let mut acc = BytesModeAccumulator::new(&DataType::Utf8, TieBreak::Any);
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
        let mut acc = BytesModeAccumulator::new(&DataType::Utf8, TieBreak::Any);
        let values: ArrayRef = Arc::new(StringArray::from(vec![None as Option<&str>, None, None]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, ScalarValue::Utf8(None));
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_with_nulls_utf8() -> Result<()> {
        let mut acc = BytesModeAccumulator::new(&DataType::Utf8, TieBreak::Any);
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
        let mut acc = BytesModeAccumulator::new(&DataType::Utf8View, TieBreak::Any);
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
        let mut acc = BytesModeAccumulator::new(&DataType::Utf8View, TieBreak::Any);
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
        let mut acc = BytesModeAccumulator::new(&DataType::Utf8View, TieBreak::Any);
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
        let mut acc = BytesModeAccumulator::new(&DataType::Utf8View, TieBreak::Any);
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
