use std::fmt::{Debug, Formatter};
use std::mem::{size_of, size_of_val};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Float64Array, Int64Array, ListArray, StructArray, make_array,
};
use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Date64Type, Decimal128Type, Decimal256Type, DurationMicrosecondType,
    DurationMillisecondType, DurationNanosecondType, DurationSecondType, Field, FieldRef,
    Float16Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, IntervalUnit,
    IntervalYearMonthType, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    i256,
};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion::physical_expr::PhysicalExpr;
use half::f16;

use crate::aggregate::percentile::{extract_literal, extract_percentiles_array};
use crate::aggregate::quantile_summaries::{QuantileSummaries, Stats};
use crate::aggregate::utils::cast_to_type;
use crate::error::invalid_arg_count_exec_err;

/// Spark's `ApproximatePercentile.DEFAULT_PERCENTILE_ACCURACY`.
const DEFAULT_PERCENTILE_ACCURACY: i64 = 10_000;

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
/// - The result preserves the input type (e.g. `int -> int`,
///   `decimal(10,2) -> decimal(10,2)`, `date -> date`).
/// - Values are accumulated in a Greenwald-Khanna sketch
///   ([`QuantileSummaries`]) whose relative error is `1 / accuracy`, so memory
///   stays bounded, and the `relativeError` short-circuits that return the
///   minimum and the maximum behave as they do in Spark.
///
/// Like Spark, every input is widened to `f64` on the way in and narrowed back
/// to the input type on the way out, so wide `bigint` and `decimal` inputs lose
/// precision exactly as they do in Spark.
///
/// Results are identical to Spark's whenever the sketch is lossless (small
/// inputs, or an `accuracy` high enough that nothing is merged). Once the
/// sketch actually compresses, the selected quantile depends on the order and
/// partitioning of the input, so Sail and Spark agree only within the
/// `1 / accuracy` bound. That is inherent to the algorithm rather than a gap:
/// Spark does not agree with itself either, and returns a different quantile
/// for the same query at a different input partitioning.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkApproxPercentile {
    signature: Signature,
}

impl Default for SparkApproxPercentile {
    fn default() -> Self {
        Self::new()
    }
}

/// The requested percentiles and the accuracy driving the sketch's relative
/// error.
struct ResolvedArgs {
    percentiles: Vec<f64>,
    accuracy: i64,
}

impl SparkApproxPercentile {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    fn resolve_args(exprs: &[Arc<dyn PhysicalExpr>], is_array: bool) -> Result<ResolvedArgs> {
        let percentage_expr = exprs.get(1).ok_or_else(|| {
            DataFusionError::Execution(
                "approx_percentile requires a percentage argument".to_string(),
            )
        })?;

        // `coerce_types` has already normalized the percentage to `Float64` or
        // `List<Float64>`, so the shape is known: extracting the wrong one would
        // replace a precise error with a misleading one.
        let percentiles = if is_array {
            extract_percentiles_array(percentage_expr)?
        } else {
            vec![extract_literal(percentage_expr)?]
        };
        for percentile in &percentiles {
            if !(0.0..=1.0).contains(percentile) {
                return Err(DataFusionError::Execution(format!(
                    "The percentage must be between [0.0, 1.0], but got {percentile}"
                )));
            }
        }

        let accuracy = match exprs.get(2) {
            Some(expr) => extract_literal(expr)? as i64,
            None => DEFAULT_PERCENTILE_ACCURACY,
        };
        if accuracy <= 0 || accuracy > i32::MAX as i64 {
            return Err(DataFusionError::Execution(format!(
                "The accuracy must be between (0, 2147483647], but got {accuracy}"
            )));
        }

        Ok(ResolvedArgs {
            percentiles,
            accuracy,
        })
    }

    /// The element type of the state's `sampled` list, mirroring Spark's
    /// serialized `Stats(value, g, delta)`.
    fn stats_fields() -> Vec<FieldRef> {
        vec![
            Field::new("value", DataType::Float64, false).into(),
            Field::new("g", DataType::Int64, false).into(),
            Field::new("delta", DataType::Int64, false).into(),
        ]
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
        // Spark accepts NumericType, DateType, TimestampType, TimestampNTZType
        // and both ANSI interval types, since all of them are numeric
        // internally.
        let supported = input.is_numeric()
            || matches!(
                input,
                DataType::Date32
                    | DataType::Date64
                    | DataType::Timestamp(_, _)
                    | DataType::Interval(IntervalUnit::YearMonth)
                    | DataType::Duration(_)
            );
        if !supported {
            return Err(DataFusionError::Plan(format!(
                "percentile_approx requires a numeric, date, timestamp or interval input type, got {input}"
            )));
        }
        // The percentage is a single Float64 or an array of Float64; the input
        // type is preserved (Spark returns the input type). Accuracy is read as
        // an Int64 so that values above `i32::MAX` are rejected rather than
        // wrapping, matching Spark's `accuracy > Int.MaxValue` check.
        let percentage = match &arg_types[1] {
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
                DataType::List(Arc::new(Field::new_list_field(DataType::Float64, false)))
            }
            _ => DataType::Float64,
        };
        let mut coerced = vec![input.clone(), percentage];
        if arg_types.len() == 3 {
            coerced.push(DataType::Int64);
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
        let is_array = matches!(
            args.exprs.get(1).map(|e| e.data_type(args.schema)),
            Some(Ok(DataType::List(_)))
        );
        let ResolvedArgs {
            percentiles,
            accuracy,
        } = Self::resolve_args(args.exprs, is_array)?;

        let data_type = args
            .exprs
            .first()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "approx_percentile requires an input argument".to_string(),
                )
            })?
            .data_type(args.schema)?;

        Ok(Box::new(ApproxPercentileAccumulator {
            data_type,
            summaries: QuantileSummaries::new(1.0 / accuracy as f64),
            relative_error: 1.0 / accuracy as f64,
            percentiles,
            is_array,
        }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let stats = DataType::Struct(Self::stats_fields().into());
        Ok(vec![
            Field::new(
                format_state_name(args.name, "count"),
                DataType::Int64,
                false,
            )
            .into(),
            Field::new(
                format_state_name(args.name, "sampled"),
                DataType::List(Arc::new(Field::new_list_field(stats, false))),
                false,
            )
            .into(),
        ])
    }
}

/// Accumulator backing [`SparkApproxPercentile`].
///
/// Mirrors Spark's `PercentileDigest`: observations are inserted into the
/// sketch as `f64`, the sketch is compressed before it is read or merged, and
/// the queried quantiles are narrowed back to the input type.
struct ApproxPercentileAccumulator {
    data_type: DataType,
    summaries: QuantileSummaries,
    relative_error: f64,
    percentiles: Vec<f64>,
    is_array: bool,
}

impl Debug for ApproxPercentileAccumulator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ApproxPercentileAccumulator({}, percentiles={:?})",
            self.data_type, self.percentiles
        )
    }
}

impl ApproxPercentileAccumulator {
    /// Spark's `PercentileDigest.quantileSummaries`: compress lazily, so the
    /// sketch is readable without discarding anything.
    fn compressed(&mut self) -> &QuantileSummaries {
        if !self.summaries.is_compressed() {
            self.summaries.compress();
        }
        &self.summaries
    }
}

impl Accumulator for ApproxPercentileAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let summaries = self.compressed();
        let count = summaries.count();
        let sampled = summaries.sampled();

        let values = Float64Array::from_iter_values(sampled.iter().map(|s| s.value));
        let g = Int64Array::from_iter_values(sampled.iter().map(|s| s.g));
        let delta = Int64Array::from_iter_values(sampled.iter().map(|s| s.delta));
        let stats = StructArray::new(
            SparkApproxPercentile::stats_fields().into(),
            vec![Arc::new(values), Arc::new(g), Arc::new(delta)],
            None,
        );

        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![
            0_i32,
            i32::try_from(stats.len()).map_err(|_| {
                DataFusionError::Execution(
                    "approx_percentile sketch exceeded the maximum list length".to_string(),
                )
            })?,
        ]));
        let list = ListArray::new(
            Arc::new(Field::new_list_field(
                DataType::Struct(SparkApproxPercentile::stats_fields().into()),
                false,
            )),
            offsets,
            Arc::new(stats),
            None,
        );

        Ok(vec![
            ScalarValue::Int64(Some(count)),
            ScalarValue::List(Arc::new(list)),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(input) = values.first() else {
            return Ok(());
        };
        let doubles = spark_internal_as_f64(input)?;
        for value in doubles.as_primitive::<Float64Type>().iter().flatten() {
            self.summaries.insert(value);
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let (Some(counts), Some(sampled)) = (states.first(), states.get(1)) else {
            return Ok(());
        };
        let counts = counts.as_primitive::<Int64Type>();
        let sampled = sampled.as_list::<i32>();

        // Spark compresses the receiving digest before merging.
        if !self.summaries.is_compressed() {
            self.summaries.compress();
        }
        for index in 0..sampled.len() {
            if sampled.is_null(index) {
                continue;
            }
            let entry = sampled.value(index);
            let Some(entry) = entry.as_any().downcast_ref::<StructArray>() else {
                return Err(DataFusionError::Execution(
                    "approx_percentile state is not a struct list".to_string(),
                ));
            };
            let (Some(values), Some(g), Some(delta)) = (
                entry.column_by_name("value"),
                entry.column_by_name("g"),
                entry.column_by_name("delta"),
            ) else {
                return Err(DataFusionError::Execution(
                    "approx_percentile state is missing a sketch column".to_string(),
                ));
            };
            let values = values.as_primitive::<Float64Type>();
            let g = g.as_primitive::<Int64Type>();
            let delta = delta.as_primitive::<Int64Type>();

            let stats = (0..entry.len())
                .map(|i| Stats {
                    value: values.value(i),
                    g: g.value(i),
                    delta: delta.value(i),
                })
                .collect();
            let other =
                QuantileSummaries::from_parts(self.relative_error, stats, counts.value(index));
            self.summaries.merge_with(&other);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // Reads the sketch without consuming it: DataFusion calls `evaluate`
        // once per row on a shared accumulator for window frames.
        let data_type = self.data_type.clone();
        let percentiles = std::mem::take(&mut self.percentiles);
        let queried = self.compressed().query(&percentiles);
        self.percentiles = percentiles;

        if self.is_array {
            let element_type =
                DataType::List(Arc::new(Field::new_list_field(data_type.clone(), false)));
            // No (non-null) input rows: Spark returns NULL for the whole array.
            let Some(values) = queried else {
                return ScalarValue::try_from(&element_type);
            };
            let scalars = values
                .into_iter()
                .map(|value| f64_to_input_type(value, &data_type))
                .collect::<Result<Vec<_>>>()?;
            let values_array = ScalarValue::iter_to_array(scalars)?;
            let offsets =
                OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, values_array.len() as i32]));
            let list_array = ListArray::new(
                Arc::new(Field::new_list_field(data_type, false)),
                offsets,
                values_array,
                None,
            );
            Ok(ScalarValue::List(Arc::new(list_array)))
        } else {
            match queried.as_ref().and_then(|values| values.first()) {
                Some(value) => f64_to_input_type(*value, &data_type),
                None => ScalarValue::try_from(&data_type),
            }
        }
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + size_of_val(self.summaries.sampled())
            + self.percentiles.capacity() * size_of::<f64>()
    }
}

/// Widens an input array to `f64` using the same internal representation Spark
/// reads in `ApproximatePercentile.update`: dates and year-month intervals are
/// their `i32` backing value, timestamps and day-time intervals their `i64`.
fn spark_internal_as_f64(array: &ArrayRef) -> Result<ArrayRef> {
    let backing = match array.data_type() {
        DataType::Date32 | DataType::Interval(IntervalUnit::YearMonth) => Some(DataType::Int32),
        DataType::Date64 | DataType::Timestamp(_, _) | DataType::Duration(_) => {
            Some(DataType::Int64)
        }
        _ => None,
    };
    let array = match backing {
        // Zero-copy reinterpretation: same buffers, same width, numeric view.
        Some(data_type) => {
            let data = array
                .to_data()
                .into_builder()
                .data_type(data_type)
                .build()?;
            make_array(data)
        }
        None => Arc::clone(array),
    };
    cast_to_type(&array, &DataType::Float64)
}

/// Narrows a quantile back to the input type, mirroring the match in Spark's
/// `ApproximatePercentile.eval`.
///
/// Scala narrows `Double` to `Byte`/`Short` via `Int`, keeping the low bits
/// rather than saturating, so the `as i32 as i8` chain below is deliberate.
fn f64_to_input_type(value: f64, data_type: &DataType) -> Result<ScalarValue> {
    match data_type {
        DataType::Int8 => {
            ScalarValue::new_primitive::<Int8Type>(Some(value as i32 as i8), data_type)
        }
        DataType::Int16 => {
            ScalarValue::new_primitive::<Int16Type>(Some(value as i32 as i16), data_type)
        }
        DataType::Int32 => ScalarValue::new_primitive::<Int32Type>(Some(value as i32), data_type),
        DataType::Int64 => ScalarValue::new_primitive::<Int64Type>(Some(value as i64), data_type),
        DataType::UInt8 => {
            ScalarValue::new_primitive::<UInt8Type>(Some(value as u32 as u8), data_type)
        }
        DataType::UInt16 => {
            ScalarValue::new_primitive::<UInt16Type>(Some(value as u32 as u16), data_type)
        }
        DataType::UInt32 => ScalarValue::new_primitive::<UInt32Type>(Some(value as u32), data_type),
        DataType::UInt64 => ScalarValue::new_primitive::<UInt64Type>(Some(value as u64), data_type),
        DataType::Float16 => {
            ScalarValue::new_primitive::<Float16Type>(Some(f16::from_f64(value)), data_type)
        }
        DataType::Float32 => {
            ScalarValue::new_primitive::<Float32Type>(Some(value as f32), data_type)
        }
        DataType::Float64 => ScalarValue::new_primitive::<Float64Type>(Some(value), data_type),
        DataType::Decimal128(_, scale) => ScalarValue::new_primitive::<Decimal128Type>(
            Some(decimal_unscaled(value, *scale) as i128),
            data_type,
        ),
        DataType::Decimal256(_, scale) => {
            let unscaled = i256::from_f64(decimal_unscaled(value, *scale)).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "percentile_approx result {value} is out of range for {data_type}"
                ))
            })?;
            ScalarValue::new_primitive::<Decimal256Type>(Some(unscaled), data_type)
        }
        DataType::Date32 => ScalarValue::new_primitive::<Date32Type>(Some(value as i32), data_type),
        DataType::Date64 => ScalarValue::new_primitive::<Date64Type>(Some(value as i64), data_type),
        DataType::Timestamp(TimeUnit::Second, _) => {
            ScalarValue::new_primitive::<TimestampSecondType>(Some(value as i64), data_type)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            ScalarValue::new_primitive::<TimestampMillisecondType>(Some(value as i64), data_type)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            ScalarValue::new_primitive::<TimestampMicrosecondType>(Some(value as i64), data_type)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            ScalarValue::new_primitive::<TimestampNanosecondType>(Some(value as i64), data_type)
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            ScalarValue::new_primitive::<IntervalYearMonthType>(Some(value as i32), data_type)
        }
        DataType::Duration(TimeUnit::Second) => {
            ScalarValue::new_primitive::<DurationSecondType>(Some(value as i64), data_type)
        }
        DataType::Duration(TimeUnit::Millisecond) => {
            ScalarValue::new_primitive::<DurationMillisecondType>(Some(value as i64), data_type)
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            ScalarValue::new_primitive::<DurationMicrosecondType>(Some(value as i64), data_type)
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            ScalarValue::new_primitive::<DurationNanosecondType>(Some(value as i64), data_type)
        }
        other => Err(DataFusionError::NotImplemented(format!(
            "percentile_approx not supported for {other}"
        ))),
    }
}

/// Rescales a quantile to a decimal's unscaled representation.
///
/// `f64::round` rounds half away from zero, which is the `ROUND_HALF_UP` Spark
/// applies when narrowing the `Double` result to the declared decimal type.
fn decimal_unscaled(value: f64, scale: i8) -> f64 {
    (value * 10f64.powi(scale as i32)).round()
}
