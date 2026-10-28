use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Float32Array, Float64Array};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::functions_aggregate::average::Avg;
use datafusion::functions_aggregate::count::Count;
use datafusion::functions_aggregate::min_max;
use datafusion::functions_aggregate::sum::Sum;
use datafusion::physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, plan_err};
use datafusion_expr::Accumulator;

use super::state::{Row, RowMultiset};

/// Unlike sliding-window extrema, CDC accumulators must allow arbitrary retraction order.
#[derive(Debug)]
pub(super) struct AccumulatorFactory {
    expression: Arc<AggregateFunctionExpr>,
    distinct: bool,
    extremum: bool,
    floating_sum: bool,
}

impl AccumulatorFactory {
    pub fn try_new(expression: &AggregateFunctionExpr, schema: SchemaRef) -> Result<Self> {
        let function = expression.fun().inner();
        let extremum = function.is::<min_max::Min>() || function.is::<min_max::Max>();
        let floating_sum = function.is::<Sum>() || function.is::<Avg>();
        if !extremum && !floating_sum && !function.is::<Count>() {
            return plan_err!(
                "streaming aggregate {} does not support arbitrary retractions; \
                 supported functions are COUNT, SUM, AVG, MIN and MAX, including DISTINCT",
                expression.name()
            );
        }
        // DISTINCT is handled by reference counts, including for types whose upstream
        // sliding DISTINCT accumulator is unavailable (e.g. decimal SUM and AVG).
        let base =
            AggregateExprBuilder::new(Arc::new(expression.fun().clone()), expression.expressions())
                .schema(schema)
                .alias(expression.name())
                .with_ignore_nulls(expression.ignore_nulls())
                .with_distinct(false)
                .build()?;
        let factory = Self {
            expression: Arc::new(base),
            distinct: expression.is_distinct(),
            extremum,
            floating_sum,
        };
        factory.create()?;
        Ok(factory)
    }

    pub fn create(&self) -> Result<StreamAccumulator> {
        let accumulator = if self.extremum {
            self.expression.create_accumulator()?
        } else {
            self.expression.create_sliding_accumulator()?
        };
        Ok(StreamAccumulator {
            expression: self.expression.clone(),
            accumulator,
            values: (self.distinct || self.extremum).then(ValueCounts::default),
            extremum: self.extremum,
            invalidated: false,
            floating_sum: self.floating_sum,
            finite_count: 0,
            non_finite: [0; 3],
        })
    }
}

#[derive(Debug, Default)]
struct ValueCounts {
    counts: HashMap<Row, usize>,
    bytes: usize,
}

impl ValueCounts {
    /// Returns true only on a 0 -> 1 or 1 -> 0 transition.
    fn apply(&mut self, mut value: Row, retracted: bool) -> Result<bool> {
        if retracted {
            let count = self.counts.get_mut(&value).ok_or_else(|| {
                exec_datafusion_err!("stream retracted an aggregate value not present in its state")
            })?;
            *count -= 1;
            if *count != 0 {
                return Ok(false);
            }
            let (stored, _) = self.counts.remove_entry(&value).ok_or_else(|| {
                datafusion_common::internal_datafusion_err!("missing aggregate value")
            })?;
            // Equal values may have different backing allocations (e.g. struct arrays).
            self.bytes -= RowMultiset::row_size(&stored);
            if self.counts.is_empty() || self.counts.len() < self.counts.capacity() / 4 {
                self.counts.shrink_to_fit();
            }
            Ok(true)
        } else if let Some(count) = self.counts.get_mut(&value) {
            *count = checked_count(*count, false)?;
            Ok(false)
        } else {
            // Nested scalar slices otherwise keep the entire input array alive.
            value.iter_mut().for_each(ScalarValue::compact);
            self.bytes += RowMultiset::row_size(&value);
            self.counts.insert(value, 1);
            Ok(true)
        }
    }
}

#[derive(Debug)]
pub(super) struct StreamAccumulator {
    expression: Arc<AggregateFunctionExpr>,
    accumulator: Box<dyn Accumulator>,
    values: Option<ValueCounts>,
    extremum: bool,
    invalidated: bool,
    floating_sum: bool,
    finite_count: usize,
    // NaN, +infinity, -infinity must be retractable without poisoning the finite sum.
    non_finite: [usize; 3],
}

impl StreamAccumulator {
    pub fn apply(&mut self, values: &[ArrayRef], index: usize, retracted: bool) -> Result<()> {
        // All supported aggregates ignore NULL arguments. COUNT(*) has a literal argument.
        if values.iter().any(|array| array.is_null(index)) {
            return Ok(());
        }
        let value = if let Some(counts) = &mut self.values {
            let value = values
                .iter()
                .map(|array| ScalarValue::try_from_array(array, index))
                .collect::<Result<Row>>()?;
            if !counts.apply(value.clone(), retracted)? {
                return Ok(());
            }
            Some(value)
        } else {
            None
        };
        if self.extremum && retracted {
            // Re-scan only distinct aggregate values when the cached extremum disappears.
            // Never retain or replay full input rows, nor use FIFO window retractions.
            if let Some(value) = value {
                self.invalidated |= self.accumulator.evaluate()? == value[0];
            }
            return Ok(());
        }
        if self.extremum && self.invalidated {
            return Ok(());
        }
        if self.floating_sum {
            let value = values.first().and_then(|array| {
                if let Some(array) = array.as_any().downcast_ref::<Float64Array>() {
                    Some(array.value(index))
                } else {
                    array
                        .as_any()
                        .downcast_ref::<Float32Array>()
                        .map(|array| f64::from(array.value(index)))
                }
            });
            if let Some(value) = value.filter(|value| !value.is_finite()) {
                let slot = if value.is_nan() {
                    0
                } else if value.is_sign_positive() {
                    1
                } else {
                    2
                };
                self.non_finite[slot] = checked_count(self.non_finite[slot], retracted)?;
                return Ok(());
            }
        }
        let values = values
            .iter()
            .map(|array| array.slice(index, 1))
            .collect::<Vec<_>>();
        if retracted {
            self.finite_count = checked_count(self.finite_count, true)?;
            if self.finite_count == 0 {
                // Reset floating-point residuals and NULL state after the last contribution.
                self.accumulator = self.expression.create_sliding_accumulator()?;
            } else {
                self.accumulator.retract_batch(&values)?;
            }
        } else {
            self.finite_count = checked_count(self.finite_count, false)?;
            self.accumulator.update_batch(&values)?;
        }
        Ok(())
    }

    pub fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.invalidated {
            self.accumulator = self.expression.create_accumulator()?;
            if let Some(values) = &self.values {
                let mut values = values.counts.keys();
                loop {
                    let chunk = values
                        .by_ref()
                        .take(1024)
                        .map(|row| row[0].clone())
                        .collect::<Vec<_>>();
                    if chunk.is_empty() {
                        break;
                    }
                    self.accumulator
                        .update_batch(&[ScalarValue::iter_to_array(chunk)?])?;
                }
            }
            self.invalidated = false;
        }
        let [nan, positive, negative] = self.non_finite;
        let special = if nan > 0 || (positive > 0 && negative > 0) {
            Some(f64::NAN)
        } else if positive > 0 {
            Some(f64::INFINITY)
        } else if negative > 0 {
            Some(f64::NEG_INFINITY)
        } else {
            None
        };
        if let Some(value) = special {
            return ScalarValue::Float64(Some(value)).cast_to(self.expression.field().data_type());
        }
        self.accumulator.evaluate()
    }

    pub fn size(&self) -> usize {
        size_of_val(self)
            + self.accumulator.size()
            + self.values.as_ref().map_or(0, |values| values.bytes)
    }
}

pub(super) fn checked_count(count: usize, retracted: bool) -> Result<usize> {
    if retracted {
        count
            .checked_sub(1)
            .ok_or_else(|| exec_datafusion_err!("stream retracted from an empty aggregate"))
    } else {
        count
            .checked_add(1)
            .ok_or_else(|| exec_datafusion_err!("stream aggregate count overflow"))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Int64Array, StructArray};
    use datafusion::arrow::datatypes::{DataType, Field};

    use super::*;

    #[test]
    fn retraction_releases_the_stored_value_allocation() -> Result<()> {
        let mut counts = ValueCounts::default();
        let value = ScalarValue::Int64(Some(1));
        counts.apply(vec![value.clone()], false)?;
        let mut larger = Vec::with_capacity(32);
        larger.push(value);
        counts.apply(larger, true)?;
        assert_eq!(counts.bytes, 0);
        assert!(counts.counts.is_empty());
        Ok(())
    }

    #[test]
    fn nested_distinct_values_do_not_retain_input_buffers() -> Result<()> {
        let array = StructArray::from(vec![(
            Arc::new(Field::new("v", DataType::Int64, false)),
            Arc::new(Int64Array::from(vec![1; 10000])) as ArrayRef,
        )]);
        let mut counts = ValueCounts::default();
        counts.apply(vec![ScalarValue::try_from_array(&array, 0)?], false)?;
        assert!(counts.bytes < 4096);
        counts.apply(vec![ScalarValue::try_from_array(&array, 9999)?], true)?;
        assert_eq!(counts.bytes, 0);
        Ok(())
    }
}
