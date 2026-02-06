use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, ListArray, StructArray};
use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};

use crate::aggregate::utils::{cast_to_type, get_scalar_value};
use crate::scalar::math::java_random::JavaRandom;

/// A histogram bin represented as a (center, count) pair.
#[derive(Debug, Clone)]
struct Coord {
    x: f64,
    y: f64,
}

impl PartialEq for Coord {
    fn eq(&self, other: &Self) -> bool {
        self.x.total_cmp(&other.x).is_eq()
    }
}

impl Eq for Coord {}

impl PartialOrd for Coord {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Coord {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.x.total_cmp(&other.x)
    }
}

/// Streaming approximate histogram using the Ben-Haim/Tom-Tov algorithm.
///
/// Ported from Spark's `NumericHistogram.java`.
#[derive(Debug, Clone)]
struct NumericHistogram {
    nbins: usize,
    bins: Vec<Coord>,
    rng: JavaRandom,
}

impl NumericHistogram {
    fn new(nbins: usize) -> Self {
        Self {
            nbins,
            bins: Vec::new(),
            rng: JavaRandom::new(31183),
        }
    }

    fn is_empty(&self) -> bool {
        self.bins.is_empty()
    }

    fn add(&mut self, v: f64) {
        let pos = self.bins.binary_search_by(|c| c.x.total_cmp(&v));
        match pos {
            Ok(idx) => {
                self.bins[idx].y += 1.0;
            }
            Err(idx) => {
                self.bins.insert(idx, Coord { x: v, y: 1.0 });
                if self.bins.len() > self.nbins {
                    self.trim();
                }
            }
        }
    }

    fn merge(&mut self, other: &NumericHistogram) {
        if other.bins.is_empty() {
            return;
        }
        if self.bins.is_empty() {
            self.nbins = other.nbins;
            self.bins = other.bins.clone();
            return;
        }
        let mut combined = Vec::with_capacity(self.bins.len() + other.bins.len());
        combined.extend(self.bins.iter().cloned());
        combined.extend(other.bins.iter().cloned());
        combined.sort();
        self.bins = combined;
        self.trim();
    }

    fn trim(&mut self) {
        while self.bins.len() > self.nbins {
            let mut smallest_diff = self.bins[1].x - self.bins[0].x;
            let mut smallest_diff_loc = 0usize;
            let mut smallest_diff_count = 1usize;

            for i in 1..self.bins.len() - 1 {
                let diff = self.bins[i + 1].x - self.bins[i].x;
                if diff < smallest_diff {
                    smallest_diff = diff;
                    smallest_diff_loc = i;
                    smallest_diff_count = 1;
                } else if diff == smallest_diff {
                    smallest_diff_count += 1;
                    if self.rng.next_f64() <= 1.0 / smallest_diff_count as f64 {
                        smallest_diff_loc = i;
                    }
                }
            }

            let d = self.bins[smallest_diff_loc].y + self.bins[smallest_diff_loc + 1].y;
            self.bins[smallest_diff_loc].x =
                self.bins[smallest_diff_loc].x * self.bins[smallest_diff_loc].y / d
                    + self.bins[smallest_diff_loc + 1].x * self.bins[smallest_diff_loc + 1].y / d;
            self.bins[smallest_diff_loc].y = d;
            self.bins.remove(smallest_diff_loc + 1);
        }
    }
}

/// Computes an approximate equi-height histogram on numeric data.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct HistogramNumericFunction {
    signature: Signature,
}

impl Default for HistogramNumericFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl HistogramNumericFunction {
    /// Creates a new `HistogramNumericFunction`.
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }

    fn extract_nbins(args: &AccumulatorArgs) -> Result<usize> {
        let scalar = get_scalar_value(&args.exprs[1])?;
        match scalar {
            ScalarValue::Int8(Some(v)) => Ok(v as usize),
            ScalarValue::Int16(Some(v)) => Ok(v as usize),
            ScalarValue::Int32(Some(v)) => Ok(v as usize),
            ScalarValue::Int64(Some(v)) => Ok(v as usize),
            other => Err(DataFusionError::Plan(format!(
                "histogram_numeric requires an integer literal for nbins, got {}",
                other.data_type()
            ))),
        }
    }
}

impl AggregateUDFImpl for HistogramNumericFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "histogram_numeric"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let x_type = arg_types[0].clone();
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(
                vec![
                    Field::new("x", x_type, true),
                    Field::new("y", DataType::Float64, true),
                ]
                .into(),
            ),
            true,
        ))))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Field::new(
                "xs",
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                true,
            )
            .into(),
            Field::new(
                "ys",
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                true,
            )
            .into(),
            Field::new("nbins", DataType::Int32, true).into(),
        ])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let nbins = Self::extract_nbins(&acc_args)?;
        let input_type = acc_args.exprs[0].data_type(acc_args.schema)?;
        Ok(Box::new(HistogramNumericAccumulator::new(
            nbins, input_type,
        )))
    }
}

/// Accumulator for the `histogram_numeric` aggregate function.
#[derive(Debug)]
struct HistogramNumericAccumulator {
    histogram: NumericHistogram,
    input_type: DataType,
}

impl HistogramNumericAccumulator {
    fn new(nbins: usize, input_type: DataType) -> Self {
        Self {
            histogram: NumericHistogram::new(nbins),
            input_type,
        }
    }
}

impl Accumulator for HistogramNumericAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values_f64 = cast_to_type(&values[0], &DataType::Float64)?;
        let array = values_f64
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal("Failed to cast to Float64Array".to_string())
            })?;
        for value in array.iter().flatten() {
            self.histogram.add(value);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.histogram.is_empty() {
            let struct_type = DataType::Struct(
                vec![
                    Field::new("x", self.input_type.clone(), true),
                    Field::new("y", DataType::Float64, true),
                ]
                .into(),
            );
            return Ok(ScalarValue::List(Arc::new(ListArray::new_null(
                Arc::new(Field::new("item", struct_type, true)),
                1,
            ))));
        }

        let xs_f64: Vec<f64> = self.histogram.bins.iter().map(|c| c.x).collect();
        let ys: Vec<f64> = self.histogram.bins.iter().map(|c| c.y).collect();
        let len = xs_f64.len();

        let xs_array: ArrayRef = Arc::new(Float64Array::from(xs_f64));
        let xs_casted = cast_to_type(&xs_array, &self.input_type)?;

        let ys_array: ArrayRef = Arc::new(Float64Array::from(ys));

        let struct_fields: Vec<Arc<Field>> = vec![
            Arc::new(Field::new("x", self.input_type.clone(), true)),
            Arc::new(Field::new("y", DataType::Float64, true)),
        ];
        let struct_array = StructArray::try_new(
            struct_fields.clone().into(),
            vec![xs_casted, ys_array],
            None,
        )?;

        let field = Arc::new(Field::new(
            "item",
            DataType::Struct(struct_fields.into()),
            true,
        ));
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0i32, len as i32]));
        let list_array = ListArray::new(field, offsets, Arc::new(struct_array), None);

        Ok(ScalarValue::List(Arc::new(list_array)))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.histogram.bins.capacity() * std::mem::size_of::<Coord>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let xs: Vec<ScalarValue> = self
            .histogram
            .bins
            .iter()
            .map(|c| ScalarValue::Float64(Some(c.x)))
            .collect();
        let ys: Vec<ScalarValue> = self
            .histogram
            .bins
            .iter()
            .map(|c| ScalarValue::Float64(Some(c.y)))
            .collect();

        let xs_list = ScalarValue::new_list_nullable(&xs, &DataType::Float64);
        let ys_list = ScalarValue::new_list_nullable(&ys, &DataType::Float64);

        Ok(vec![
            ScalarValue::List(xs_list),
            ScalarValue::List(ys_list),
            ScalarValue::Int32(Some(self.histogram.nbins as i32)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let xs_list = states[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("Expected ListArray for xs state".to_string())
            })?;
        let ys_list = states[1]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("Expected ListArray for ys state".to_string())
            })?;
        let nbins_array = states[2]
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .ok_or_else(|| {
                DataFusionError::Internal("Expected Int32Array for nbins state".to_string())
            })?;

        for i in 0..xs_list.len() {
            if xs_list.is_null(i) {
                continue;
            }
            let xs_values = xs_list.value(i);
            let ys_values = ys_list.value(i);
            let nbins = nbins_array.value(i) as usize;

            let xs = xs_values
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("Expected Float64Array for xs".to_string())
                })?;
            let ys = ys_values
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("Expected Float64Array for ys".to_string())
                })?;

            let mut other = NumericHistogram::new(nbins);
            for j in 0..xs.len() {
                other.bins.push(Coord {
                    x: xs.value(j),
                    y: ys.value(j),
                });
            }
            self.histogram.merge(&other);
        }
        Ok(())
    }
}
