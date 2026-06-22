use std::fmt::{Debug, Formatter};
use std::mem::{size_of, size_of_val};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, ArrowNumericType, AsArray, ListArray, PrimitiveArray,
};
use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::{
    DataType, Decimal128Type, Decimal256Type, DurationMicrosecondType, DurationMillisecondType,
    DurationNanosecondType, DurationSecondType, Field, FieldRef, Float16Type, Float32Type,
    Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, IntervalUnit, IntervalYearMonthType,
    TimeUnit, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion::physical_expr::PhysicalExpr;

use crate::aggregate::percentile::{extract_literal, extract_percentiles_array};
use crate::aggregate::utils::{calculate_percentile_disc, cast_to_type};
use crate::error::invalid_arg_count_exec_err;

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
            // Spark also supports ANSI interval types. Year-month intervals are
            // backed by an i32 and day-time intervals are mapped to durations
            // (i64); both are ordinal, so the discrete nearest-rank selection
            // works directly on the underlying primitive.
            DataType::Interval(IntervalUnit::YearMonth) => {
                $helper!(IntervalYearMonthType, $input_dt)
            }
            DataType::Duration(TimeUnit::Second) => $helper!(DurationSecondType, $input_dt),
            DataType::Duration(TimeUnit::Millisecond) => {
                $helper!(DurationMillisecondType, $input_dt)
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                $helper!(DurationMicrosecondType, $input_dt)
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                $helper!(DurationNanosecondType, $input_dt)
            }
            _ => Err(DataFusionError::NotImplemented(format!(
                "{} not supported for {}",
                $err_msg, $input_dt,
            ))),
        }
    };
}

/// Spark-compatible `approx_percentile` / `percentile_approx` aggregate.
///
/// ```text
/// percentile_approx(col, percentage, accuracy)
/// ```
///
/// Unlike DataFusion's `approx_percentile_cont`, this matches Spark semantics:
///
/// - `percentage` may be a single number in `[0.0, 1.0]` (returning a scalar) or
///   an array of such numbers (returning `array<input_type>`).
/// - The result preserves the input numeric type (e.g. `int -> int`,
///   `decimal(10,2) -> decimal(10,2)`).
/// - Percentiles are selected with the discrete nearest-rank method
///   (`x[ceil(q * N) - 1]`, with `q = 0` returning the minimum), the same
///   selection Spark's `percentile_approx` converges to. The optional `accuracy`
///   argument is validated (positive integer) but does not affect the result,
///   since the selection is exact.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkApproxPercentile {
    signature: Signature,
}

impl Default for SparkApproxPercentile {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkApproxPercentile {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    /// Extracts the requested percentiles and whether they were given as an array.
    fn resolve_percentiles(exprs: &[Arc<dyn PhysicalExpr>]) -> Result<(Vec<f64>, bool)> {
        let percentage_expr = exprs.get(1).ok_or_else(|| {
            DataFusionError::Execution(
                "approx_percentile requires a percentage argument".to_string(),
            )
        })?;

        let (percentiles, is_array) = match extract_percentiles_array(percentage_expr) {
            Ok(percentiles) => (percentiles, true),
            Err(_) => (vec![extract_literal(percentage_expr)?], false),
        };
        for percentile in &percentiles {
            if !(0.0..=1.0).contains(percentile) {
                return Err(DataFusionError::Execution(format!(
                    "The percentage must be between [0.0, 1.0], but got {percentile}"
                )));
            }
        }

        // Spark accepts an optional accuracy argument that must be a positive
        // integer. The discrete selection is exact, so the value is only
        // validated, not used.
        if let Some(expr) = exprs.get(2) {
            let accuracy = extract_literal(expr)?;
            if accuracy < 1.0 {
                return Err(DataFusionError::Execution(format!(
                    "The accuracy must be between (0, 2147483647], but got {accuracy}"
                )));
            }
        }

        Ok((percentiles, is_array))
    }
}

impl AggregateUDFImpl for SparkApproxPercentile {
    fn name(&self) -> &str {
        "approx_percentile"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() < 2 || arg_types.len() > 3 {
            return Err(invalid_arg_count_exec_err(
                "percentile_approx",
                (2, 3),
                arg_types.len(),
            ));
        }
        let input = &arg_types[0];
        let supported = input.is_numeric()
            || matches!(
                input,
                DataType::Interval(IntervalUnit::YearMonth) | DataType::Duration(_)
            );
        if !supported {
            return Err(DataFusionError::Plan(format!(
                "percentile_approx requires a numeric or interval input type, got {input}"
            )));
        }
        // The percentage is a single Float64 or an array of Float64; the input
        // type is preserved (Spark returns the input type) and the optional
        // accuracy is an integer.
        let percentage = match &arg_types[1] {
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
                DataType::List(Arc::new(Field::new_list_field(DataType::Float64, false)))
            }
            _ => DataType::Float64,
        };
        let mut coerced = vec![input.clone(), percentage];
        if arg_types.len() == 3 {
            coerced.push(DataType::Int32);
        }
        Ok(coerced)
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // `coerce_types` has already validated the input type and rewritten the
        // percentage argument to Float64 (scalar) or List<Float64> (array).
        // When the percentage is an array, the result is an array of percentiles
        // whose element type matches the input; Spark reports it as non-nullable.
        if matches!(arg_types.get(1), Some(DataType::List(_))) {
            Ok(DataType::List(Arc::new(Field::new(
                "item",
                arg_types[0].clone(),
                false,
            ))))
        } else {
            Ok(arg_types[0].clone())
        }
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let (percentiles, is_array) = Self::resolve_percentiles(args.exprs)?;

        macro_rules! helper {
            ($t:ty, $dt:expr) => {
                Ok(Box::new(ApproxPercentileAccumulator::<$t> {
                    data_type: $dt.clone(),
                    all_values: vec![],
                    percentiles: percentiles.clone(),
                    is_array,
                }))
            };
        }

        let input_dt = args.exprs[0].data_type(args.schema)?;
        dispatch_numeric_type!(input_dt, helper, "ApproxPercentileAccumulator")
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let input_type = args.input_fields[0].data_type().clone();
        let field = Field::new_list_field(input_type, true);
        Ok(vec![Field::new(
            format_state_name(args.name, "approx_percentile"),
            DataType::List(Arc::new(field)),
            true,
        )
        .into()])
    }
}

/// Accumulator backing [`SparkApproxPercentile`]. It collects the input values
/// (preserving their native type) and selects each requested percentile with the
/// discrete nearest-rank method on `evaluate`.
struct ApproxPercentileAccumulator<T: ArrowNumericType> {
    data_type: DataType,
    all_values: Vec<T::Native>,
    percentiles: Vec<f64>,
    is_array: bool,
}

impl<T: ArrowNumericType> Debug for ApproxPercentileAccumulator<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ApproxPercentileAccumulator({}, percentiles={:?})",
            self.data_type, self.percentiles
        )
    }
}

impl<T: ArrowNumericType> Accumulator for ApproxPercentileAccumulator<T> {
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
        let values_array = cast_to_type(&values[0], &self.data_type)?;
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
        let values = std::mem::take(&mut self.all_values);
        if self.is_array {
            let element_type = DataType::List(Arc::new(Field::new_list_field(
                self.data_type.clone(),
                false,
            )));
            let mut natives: Vec<T::Native> = Vec::with_capacity(self.percentiles.len());
            for percentile in &self.percentiles {
                match calculate_percentile_disc::<T>(values.clone(), *percentile) {
                    Some(value) => natives.push(value),
                    // No (non-null) input rows: Spark returns NULL for the whole array.
                    None => return ScalarValue::try_from(&element_type),
                }
            }
            let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, natives.len() as i32]));
            let values_array = PrimitiveArray::<T>::new(ScalarBuffer::from(natives), None)
                .with_data_type(self.data_type.clone());
            let list_array = ListArray::new(
                Arc::new(Field::new_list_field(self.data_type.clone(), false)),
                offsets,
                Arc::new(values_array),
                None,
            );
            Ok(ScalarValue::List(Arc::new(list_array)))
        } else {
            let value = calculate_percentile_disc::<T>(values, self.percentiles[0]);
            ScalarValue::new_primitive::<T>(value, &self.data_type)
        }
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + self.all_values.capacity() * size_of::<T::Native>()
            + self.percentiles.capacity() * size_of::<f64>()
    }
}
