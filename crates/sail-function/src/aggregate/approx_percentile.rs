use std::collections::{HashMap, VecDeque};
use std::mem::size_of;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, ListArray};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute::CastOptions;
use datafusion::arrow::datatypes::{
    DataType, Field, FieldRef, Float64Type, IntervalUnit, UInt64Type,
};
use datafusion::common::{DataFusionError, HashSet, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use sail_common::spec::SAIL_SPARK_INTERVAL_METADATA_KEY;
use sail_common_datafusion::display::spark_f64_to_string;

use crate::aggregate::utils::{get_scalar_value, scalar_to_f64};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ApproxPercentile {
    signature: Signature,
}

impl Default for ApproxPercentile {
    fn default() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ApproxPercentile {
    /// Validate foldable parameters both during planning and when constructing an accumulator.
    pub fn validate_parameters(
        percentage: ScalarValue,
        accuracy: ScalarValue,
    ) -> Result<(Vec<f64>, i64)> {
        if percentage.is_null() {
            return Err(DataFusionError::Plan("percentage must not be null".into()));
        }
        let percentages = match percentage {
            ScalarValue::List(array) => {
                let values = array.value(0);
                (0..values.len())
                    .map(|i| {
                        // Spark's ArrayData.toDoubleArray reads a null DOUBLE slot as zero.
                        if values.is_null(i) {
                            Ok(0.0)
                        } else {
                            scalar_to_f64(&ScalarValue::try_from_array(&values, i)?)
                        }
                    })
                    .collect::<Result<Vec<_>>>()?
            }
            value => vec![scalar_to_f64(&value)?],
        };
        if percentages.iter().any(|p| *p < 0.0 || *p > 1.0) {
            return Err(DataFusionError::Plan(
                "percentage must be between 0.0 and 1.0".into(),
            ));
        }
        let accuracy = match accuracy {
            ScalarValue::Int64(Some(value)) if (1..=i32::MAX as i64).contains(&value) => value,
            _ => {
                return Err(DataFusionError::Plan(
                    "accuracy must be a non-null integer in (0, 2147483647]".into(),
                ));
            }
        };
        Ok((percentages, accuracy))
    }

    fn make_accumulator(
        &self,
        args: AccumulatorArgs,
        sliding: bool,
    ) -> Result<Box<dyn Accumulator>> {
        let percentage = get_scalar_value(&args.exprs[1])?;
        let accuracy = args
            .exprs
            .get(2)
            .map(get_scalar_value)
            .transpose()?
            .unwrap_or(ScalarValue::Int64(Some(10000)));
        let (percentages, accuracy) = Self::validate_parameters(percentage, accuracy)?;
        Ok(Box::new(ApproxPercentileAccumulator {
            summary: QuantileSummary::new(1.0 / accuracy as f64),
            percentages,
            input_type: args.exprs[0].data_type(args.schema)?,
            return_type: args.return_field.data_type().clone(),
            window: sliding.then(VecDeque::new),
            distinct: args.is_distinct.then(HashSet::new),
        }))
    }
}

// TODO: DataFusion aggregate schema names omit argument casts, so selecting
// percentile_approx(d, p) alongside percentile_approx(CAST(d AS TIMESTAMP_NTZ), p)
// collides. This also affects existing aggregates; fix the shared naming contract.
impl AggregateUDFImpl for ApproxPercentile {
    fn name(&self) -> &str {
        "percentile_approx"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        if !(2..=3).contains(&types.len()) {
            return Err(DataFusionError::Plan(
                "percentile_approx expects 2 or 3 arguments".into(),
            ));
        }
        let input = match &types[0] {
            DataType::Null => DataType::Float64,
            dt if dt.is_numeric() => dt.clone(),
            dt @ (DataType::Date32
            | DataType::Timestamp(_, _)
            | DataType::Duration(_)
            | DataType::Interval(IntervalUnit::YearMonth)) => dt.clone(),
            dt => {
                return Err(DataFusionError::Plan(format!(
                    "percentile_approx requires numeric, date, timestamp or ANSI interval input, got {dt}"
                )));
            }
        };
        let percentage = match &types[1] {
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _) => {
                // Spark accepts already-double arrays with nullable elements, but its
                // implicit array cast to DOUBLE requires non-nullable source elements.
                if (field.is_nullable() && field.data_type() != &DataType::Float64)
                    || (!field.data_type().is_numeric() && field.data_type() != &DataType::Null)
                {
                    return Err(DataFusionError::Plan(
                        "percentage array must contain numbers".into(),
                    ));
                }
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Float64,
                    field.is_nullable(),
                )))
            }
            dt if dt.is_numeric() || dt == &DataType::Null => DataType::Float64,
            dt => {
                return Err(DataFusionError::Plan(format!(
                    "percentage must be numeric, got {dt}"
                )));
            }
        };
        let mut result = vec![input, percentage];
        if let Some(accuracy) = types.get(2) {
            if !accuracy.is_integer() && accuracy != &DataType::Null {
                return Err(DataFusionError::Plan("accuracy must be an integer".into()));
            }
            result.push(DataType::Int64);
        }
        Ok(result)
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        let types = self.coerce_types(types)?;
        Ok(if matches!(types[1], DataType::List(_)) {
            DataType::List(Arc::new(Field::new("item", types[0].clone(), false)))
        } else {
            types[0].clone()
        })
    }

    fn return_field(&self, fields: &[FieldRef]) -> Result<FieldRef> {
        let types = fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        let data_type = self.return_type(&types)?;
        // MONTH/SECOND and other ANSI interval qualifiers are logical type metadata.
        // Spark preserves the child type, including qualifiers, for scalar and array results.
        let metadata = fields[0]
            .metadata()
            .get(SAIL_SPARK_INTERVAL_METADATA_KEY)
            .map(|value| {
                HashMap::from([(SAIL_SPARK_INTERVAL_METADATA_KEY.to_string(), value.clone())])
            })
            .unwrap_or_default();
        Ok(Arc::new(match data_type {
            DataType::List(item) => Field::new(
                self.name(),
                DataType::List(Arc::new(item.as_ref().clone().with_metadata(metadata))),
                true,
            ),
            data_type => Field::new(self.name(), data_type, true).with_metadata(metadata),
        }))
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.make_accumulator(args, false)
    }

    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.make_accumulator(args, true)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        if args.is_distinct {
            return Ok(vec![Arc::new(Field::new(
                format_state_name(args.name, "distinct_values"),
                DataType::List(Arc::new(Field::new(
                    "item",
                    args.input_fields[0].data_type().clone(),
                    false,
                ))),
                false,
            ))]);
        }
        Ok([
            ("values", DataType::Float64),
            ("g", DataType::UInt64),
            ("delta", DataType::UInt64),
        ]
        .into_iter()
        .map(|(name, dt)| {
            Arc::new(Field::new(
                format_state_name(args.name, name),
                DataType::List(Arc::new(Field::new("item", dt, false))),
                false,
            ))
        })
        .chain(std::iter::once(Arc::new(Field::new(
            format_state_name(args.name, "count"),
            DataType::UInt64,
            false,
        ))))
        .collect())
    }
}

#[derive(Clone, Copy, Debug)]
struct Sample {
    value: f64,
    g: u64,
    delta: u64,
}

/// Spark's QuantileSummaries (Greenwald–Khanna), including its head-buffer and
/// compression thresholds. A t-digest or an exact rank calculation produces
/// different results, particularly at low accuracy and after partial merges.
/// TODO: Match Spark's scan and partial-aggregation partition topology. Sail may
/// repartition even explicit single-partition ranges, changing approximate ranks.
/// IEEE NaN comparisons also make summary merges order-dependent, so differing
/// LocalRelation partitions can select different NaN results.
#[derive(Debug)]
struct QuantileSummary {
    relative_error: f64,
    count: u64,
    sampled: Vec<Sample>,
    head: Vec<f64>,
    compressed: bool,
}

impl QuantileSummary {
    fn new(relative_error: f64) -> Self {
        Self {
            relative_error,
            count: 0,
            sampled: vec![],
            head: vec![],
            compressed: true,
        }
    }

    fn insert(&mut self, value: f64) {
        self.compressed = false;
        self.head.push(value);
        if self.head.len() >= 50000 {
            self.insert_head();
            if self.sampled.len() >= 10000 {
                self.compress();
            }
        }
    }

    fn insert_head(&mut self) {
        if self.head.is_empty() {
            return;
        }
        // Java's double sort orders NaN last and negative zero before positive zero.
        self.head.sort_by(|a, b| match (a.is_nan(), b.is_nan()) {
            (true, true) => std::cmp::Ordering::Equal,
            (true, false) => std::cmp::Ordering::Greater,
            (false, true) => std::cmp::Ordering::Less,
            _ => a.total_cmp(b),
        });
        let mut inserted = Vec::with_capacity(self.sampled.len() + self.head.len());
        let mut index = 0;
        for (i, &value) in self.head.iter().enumerate() {
            while index < self.sampled.len() && self.sampled[index].value <= value {
                inserted.push(self.sampled[index]);
                index += 1;
            }
            self.count += 1;
            let delta = if inserted.is_empty()
                || (index == self.sampled.len() && i + 1 == self.head.len())
            {
                0
            } else {
                (2.0 * self.relative_error * self.count as f64).floor() as u64
            };
            inserted.push(Sample { value, g: 1, delta });
        }
        inserted.extend_from_slice(&self.sampled[index..]);
        self.sampled = inserted;
        self.head.clear();
    }

    fn compress(&mut self) {
        if self.compressed {
            return;
        }
        self.insert_head();
        self.compressed = true;
        let Some(&last) = self.sampled.last() else {
            return;
        };
        let mut head = last;
        let mut compressed = Vec::with_capacity(self.sampled.len());
        let threshold = 2.0 * self.relative_error * self.count as f64;
        for &sample in self.sampled[1..self.sampled.len().max(2) - 1].iter().rev() {
            if ((sample.g + head.g + head.delta) as f64) < threshold {
                head.g += sample.g;
            } else {
                compressed.push(head);
                head = sample;
            }
        }
        compressed.push(head);
        if self.sampled.len() > 1 && self.sampled[0].value <= head.value {
            compressed.push(self.sampled[0]);
        }
        compressed.reverse();
        self.sampled = compressed;
    }

    fn merge(&mut self, other: Self) {
        self.compress();
        if other.count == 0 {
            return;
        }
        if self.count == 0 {
            *self = other;
            return;
        }
        let self_delta = (2.0 * other.relative_error * other.count as f64).floor() as u64;
        let other_delta = (2.0 * self.relative_error * self.count as f64).floor() as u64;
        let mut merged = Vec::with_capacity(self.sampled.len() + other.sampled.len());
        let (mut left, mut right) = (0, 0);
        while left < self.sampled.len() && right < other.sampled.len() {
            let mut sample;
            if self.sampled[left].value < other.sampled[right].value {
                sample = self.sampled[left];
                left += 1;
                if right > 0 {
                    sample.delta += self_delta;
                }
            } else {
                sample = other.sampled[right];
                right += 1;
                if left > 0 {
                    sample.delta += other_delta;
                }
            }
            merged.push(sample);
        }
        merged.extend_from_slice(&self.sampled[left..]);
        merged.extend_from_slice(&other.sampled[right..]);
        self.sampled = merged;
        self.compressed = false;
        self.count += other.count;
        self.relative_error = self.relative_error.max(other.relative_error);
        self.compress();
    }

    fn query(&mut self, percentages: &[f64]) -> Vec<f64> {
        self.compress();
        if self.sampled.is_empty() {
            return vec![];
        }
        let error = self
            .sampled
            .iter()
            .map(|s| s.g + s.delta)
            .max()
            .unwrap_or(0)
            / 2;
        let mut ordered: Vec<_> = percentages.iter().copied().enumerate().collect();
        ordered.sort_by(|a, b| a.1.total_cmp(&b.1));
        let mut result = vec![0.0; percentages.len()];
        let mut index = 0;
        let mut min_rank = self.sampled[0].g;
        for (pos, percentage) in ordered {
            if percentage <= self.relative_error {
                result[pos] = self.sampled[0].value;
            } else if percentage >= 1.0 - self.relative_error {
                result[pos] = self.sampled[self.sampled.len() - 1].value;
            } else {
                let rank = (percentage * self.count as f64).ceil();
                while index + 1 < self.sampled.len() {
                    let sample = self.sampled[index];
                    if (min_rank + sample.delta) as f64 - error as f64 <= rank
                        && rank <= (min_rank + error) as f64
                    {
                        break;
                    }
                    index += 1;
                    min_rank += self.sampled[index].g;
                }
                result[pos] = self.sampled[index].value;
                if index + 1 == self.sampled.len() {
                    min_rank = 0;
                }
            }
        }
        result
    }
}

#[derive(Debug)]
struct ApproxPercentileAccumulator {
    summary: QuantileSummary,
    percentages: Vec<f64>,
    input_type: DataType,
    return_type: DataType,
    // Spark recomputes bounded windows because compressed summaries cannot retract.
    window: Option<VecDeque<f64>>,
    // DISTINCT must retain original values: GK summaries cannot remove duplicates
    // across partial aggregates, and different BIGINTs can round to the same double.
    distinct: Option<HashSet<ScalarValue>>,
}

fn as_double(value: ScalarValue) -> Result<f64> {
    match value {
        ScalarValue::Date32(Some(v)) | ScalarValue::IntervalYearMonth(Some(v)) => Ok(v as f64),
        ScalarValue::TimestampSecond(Some(v), _)
        | ScalarValue::TimestampMillisecond(Some(v), _)
        | ScalarValue::TimestampMicrosecond(Some(v), _)
        | ScalarValue::TimestampNanosecond(Some(v), _)
        | ScalarValue::DurationSecond(Some(v))
        | ScalarValue::DurationMillisecond(Some(v))
        | ScalarValue::DurationMicrosecond(Some(v))
        | ScalarValue::DurationNanosecond(Some(v)) => Ok(v as f64),
        value @ (ScalarValue::Decimal32(_, _, _)
        | ScalarValue::Decimal64(_, _, _)
        | ScalarValue::Decimal128(_, _, _)
        | ScalarValue::Decimal256(_, _, _)) => {
            // BigDecimal.doubleValue rounds the decimal itself, rather than first
            // rounding the unscaled integer and then dividing in floating point.
            value
                .to_string()
                .parse::<f64>()
                .map_err(|e| DataFusionError::Execution(e.to_string()))
        }
        value => scalar_to_f64(&value.cast_to(&DataType::Float64)?),
    }
}

fn from_double(value: f64, data_type: &DataType) -> Result<ScalarValue> {
    Ok(match data_type {
        DataType::Int8 => ScalarValue::Int8(Some((value as i32) as i8)),
        DataType::Int16 => ScalarValue::Int16(Some((value as i32) as i16)),
        DataType::Int32 => ScalarValue::Int32(Some(value as i32)),
        DataType::Int64 => ScalarValue::Int64(Some(value as i64)),
        DataType::Date32 => ScalarValue::Date32(Some(value as i32)),
        DataType::Interval(IntervalUnit::YearMonth) => {
            ScalarValue::IntervalYearMonth(Some(value as i32))
        }
        DataType::Timestamp(_, _) | DataType::Duration(_) => {
            ScalarValue::Int64(Some(value as i64)).cast_to(data_type)?
        }
        DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => {
            // TODO: Spark's SQL string rendering observes the Decimal's runtime scale,
            // while Arrow has only the declared scale. Preserve materialized values here;
            // matching that rendering requires a separate runtime decimal representation.
            // Spark constructs Decimal(Double) from the shortest decimal representation,
            // then rounds to the input scale. Multiplying a double by 10^scale first
            // introduces binary rounding artifacts for high-precision decimal inputs.
            let text = spark_f64_to_string(value);
            // Arrow's decimal parser does not reliably retain mantissa digits when
            // the requested scale is smaller than an exponent-form string's fraction.
            let text = if let Some((mantissa, exponent)) = text.split_once('E') {
                let exponent = exponent
                    .parse::<i32>()
                    .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                let (sign, mantissa) = mantissa
                    .strip_prefix('-')
                    .map_or(("", mantissa), |value| ("-", value));
                let point = mantissa.find('.').unwrap_or(mantissa.len()) as i32 + exponent;
                let digits = mantissa.replace('.', "");
                if point <= 0 {
                    format!("{sign}0.{}{digits}", "0".repeat((-point) as usize))
                } else if point as usize >= digits.len() {
                    format!(
                        "{sign}{digits}{}",
                        "0".repeat(point as usize - digits.len())
                    )
                } else {
                    let (integer, fraction) = digits.split_at(point as usize);
                    format!("{sign}{integer}.{fraction}")
                }
            } else {
                text
            };
            let scale = text
                .split_once('.')
                .map_or(0, |(_, fraction)| fraction.len()) as i8;
            let options = CastOptions {
                safe: true,
                ..Default::default()
            };
            ScalarValue::Utf8(Some(text))
                .cast_to_with_options(&DataType::Decimal256(76, scale), &options)?
                .cast_to_with_options(data_type, &options)?
        }
        _ => ScalarValue::Float64(Some(value)).cast_to(data_type)?,
    })
}

impl Accumulator for ApproxPercentileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for i in 0..values[0].len() {
            if values[0].is_null(i) {
                continue;
            }
            let mut scalar = ScalarValue::try_from_array(&values[0], i)?;
            if let Some(distinct) = &mut self.distinct {
                // Spark normalizes floating grouping keys used by DISTINCT.
                scalar = match scalar {
                    ScalarValue::Float32(Some(v)) => ScalarValue::Float32(Some(if v == 0.0 {
                        0.0
                    } else if v.is_nan() {
                        f32::NAN
                    } else {
                        v
                    })),
                    ScalarValue::Float64(Some(v)) => ScalarValue::Float64(Some(if v == 0.0 {
                        0.0
                    } else if v.is_nan() {
                        f64::NAN
                    } else {
                        v
                    })),
                    value => value,
                };
                if !distinct.insert(scalar.clone()) {
                    continue;
                }
            }
            let value = as_double(scalar)?;
            if let Some(window) = &mut self.window {
                window.push_back(value);
            } else {
                self.summary.insert(value);
            }
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let window = self
            .window
            .as_mut()
            .ok_or_else(|| DataFusionError::Internal("cannot retract a quantile summary".into()))?;
        window.drain(..values[0].len() - values[0].null_count());
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        self.window.is_some()
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if let Some(window) = &self.window {
            self.summary = QuantileSummary::new(self.summary.relative_error);
            for &value in window {
                self.summary.insert(value);
            }
        }
        if (self.summary.count > 0 || !self.summary.head.is_empty())
            && self.percentages.iter().any(|p| p.is_nan())
        {
            return Err(DataFusionError::Execution(
                "percentage must be between 0.0 and 1.0".into(),
            ));
        }
        let result = self.summary.query(&self.percentages);
        if result.is_empty() {
            return ScalarValue::try_from(&self.return_type);
        }
        let values = result
            .into_iter()
            .map(|value| from_double(value, &self.input_type))
            .collect::<Result<Vec<_>>>()?;
        Ok(if let DataType::List(field) = &self.return_type {
            // TODO: Spark permits decimal-overflow NULL elements despite declaring
            // containsNull=false. Arrow forbids this inconsistent list representation;
            // supporting it requires a separate logical/physical nullability model.
            if values.iter().any(ScalarValue::is_null) {
                return Err(DataFusionError::Execution("decimal overflow in percentile_approx array cannot be represented by a non-nullable Arrow list".into()));
            }
            ScalarValue::List(Arc::new(ListArray::try_new(
                Arc::clone(field),
                OffsetBuffer::from_lengths([values.len()]),
                ScalarValue::iter_to_array(values)?,
                None,
            )?))
        } else {
            values[0].clone()
        })
    }

    fn size(&self) -> usize {
        self.distinct
            .as_ref()
            .map_or(0, |values| values.capacity() * size_of::<ScalarValue>())
            + size_of::<Self>()
            + self.summary.sampled.capacity() * size_of::<Sample>()
            + (self.summary.head.capacity()
                + self.percentages.capacity()
                + self.window.as_ref().map_or(0, VecDeque::capacity))
                * size_of::<f64>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        if let Some(distinct) = &self.distinct {
            let mut values = distinct.iter().cloned().collect::<Vec<_>>();
            values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            return Ok(vec![ScalarValue::List(ScalarValue::new_list(
                &values,
                &self.input_type,
                false,
            ))]);
        }
        self.summary.compress();
        let values: Vec<_> = self
            .summary
            .sampled
            .iter()
            .map(|s| ScalarValue::Float64(Some(s.value)))
            .collect();
        let gs: Vec<_> = self
            .summary
            .sampled
            .iter()
            .map(|s| ScalarValue::UInt64(Some(s.g)))
            .collect();
        let deltas: Vec<_> = self
            .summary
            .sampled
            .iter()
            .map(|s| ScalarValue::UInt64(Some(s.delta)))
            .collect();
        Ok(vec![
            ScalarValue::List(ScalarValue::new_list(&values, &DataType::Float64, false)),
            ScalarValue::List(ScalarValue::new_list(&gs, &DataType::UInt64, false)),
            ScalarValue::List(ScalarValue::new_list(&deltas, &DataType::UInt64, false)),
            ScalarValue::UInt64(Some(self.summary.count)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if self.distinct.is_some() {
            let values = states[0].as_list::<i32>();
            for i in 0..values.len() {
                self.update_batch(&[values.value(i)])?;
            }
            return Ok(());
        }
        let values = states[0].as_list::<i32>();
        let gs = states[1].as_list::<i32>();
        let deltas = states[2].as_list::<i32>();
        let counts = states[3].as_primitive::<UInt64Type>();
        for i in 0..values.len() {
            let (vs, gs, ds) = (values.value(i), gs.value(i), deltas.value(i));
            let (vs, gs, ds) = (
                vs.as_primitive::<Float64Type>(),
                gs.as_primitive::<UInt64Type>(),
                ds.as_primitive::<UInt64Type>(),
            );
            let sampled = (0..vs.len())
                .map(|j| Sample {
                    value: vs.value(j),
                    g: gs.value(j),
                    delta: ds.value(j),
                })
                .collect();
            self.summary.merge(QuantileSummary {
                relative_error: self.summary.relative_error,
                count: counts.value(i),
                sampled,
                head: vec![],
                compressed: true,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::QuantileSummary;

    #[test]
    fn spark_summary_compression_boundaries() {
        // Source Spark 4.1.1: approx_percentile(id, array(0D,.01D,.25D,.5D,.75D,.99D,1D), accuracy)
        // FROM range(0, count, 1, 1). These cross the 50,000-observation head-buffer boundary.
        let cases = [
            (49999, 2.0, [0.0, 0.0, 0.0, 0.0, 49998.0, 49998.0, 49998.0]),
            (
                49999,
                7.0,
                [0.0, 0.0, 10964.0, 19638.0, 31632.0, 49998.0, 49998.0],
            ),
            (
                49999,
                10000.0,
                [0.0, 495.0, 12494.0, 24997.0, 37494.0, 49494.0, 49998.0],
            ),
            (50000, 2.0, [0.0, 0.0, 0.0, 0.0, 49999.0, 49999.0, 49999.0]),
            (
                50000,
                7.0,
                [0.0, 0.0, 10966.0, 19640.0, 31633.0, 49999.0, 49999.0],
            ),
            (
                50000,
                10000.0,
                [0.0, 495.0, 12494.0, 24997.0, 37494.0, 49494.0, 49999.0],
            ),
            (50001, 2.0, [0.0, 0.0, 0.0, 0.0, 50000.0, 50000.0, 50000.0]),
            (
                50001,
                7.0,
                [0.0, 0.0, 10966.0, 19640.0, 31633.0, 50000.0, 50000.0],
            ),
            (
                50001,
                10000.0,
                [0.0, 495.0, 12501.0, 24997.0, 37496.0, 49495.0, 50000.0],
            ),
            (
                100003,
                2.0,
                [0.0, 0.0, 0.0, 0.0, 100002.0, 100002.0, 100002.0],
            ),
            (
                100003,
                7.0,
                [0.0, 0.0, 10966.0, 49999.0, 63265.0, 100002.0, 100002.0],
            ),
            (
                100003,
                10000.0,
                [0.0, 999.0, 24997.0, 49999.0, 74992.0, 98992.0, 100002.0],
            ),
        ];
        let percentages = [0.0, 0.01, 0.25, 0.5, 0.75, 0.99, 1.0];
        for (count, accuracy, expected) in cases {
            let mut summary = QuantileSummary::new(1.0 / accuracy);
            for value in 0..count {
                summary.insert(value as f64);
            }
            assert_eq!(
                summary.query(&percentages),
                expected,
                "count={count}, accuracy={accuracy}"
            );
            assert_eq!(
                summary.query(&percentages),
                expected,
                "repeated query must preserve compressed state"
            );
        }
    }

    #[test]
    fn spark_summary_empty_singleton_and_partial_merge() {
        let mut left = QuantileSummary::new(0.0001);
        assert!(left.query(&[0.5]).is_empty());
        left.insert(0.0);
        assert_eq!(left.query(&[0.0, 0.5, 1.0]), [0.0, 0.0, 0.0]);
        left.insert(2.0);
        let mut right = QuantileSummary::new(0.0001);
        right.insert(1.0);
        right.insert(3.0);
        right.compress();
        left.merge(right);
        assert_eq!(left.query(&[1.0, 0.5, 0.0, 0.5]), [3.0, 1.0, 0.0, 1.0]);
    }
}
