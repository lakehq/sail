use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Float32Array, GenericListArray, ListArray, OffsetSizeTrait,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::cast::{
    as_float32_array, as_int64_array, as_large_list_array, as_list_array,
};
use datafusion_common::{Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{AggregateUDFImpl, Signature, Volatility};
use datafusion_expr_common::accumulator::Accumulator;

use crate::scalar::vector::is_float_vector;

fn float_vector_field() -> FieldRef {
    Arc::new(Field::new_list_field(DataType::Float32, false))
}

/// The `ARRAY<FLOAT>` type with non-nullable elements that Spark returns.
fn float_vector_type() -> DataType {
    DataType::List(float_vector_field())
}

fn float_vector_scalar(vector: Option<&[f32]>) -> Result<ScalarValue> {
    let array = match vector {
        None => ListArray::new_null(float_vector_field(), 1),
        Some(vector) => ListArray::try_new(
            float_vector_field(),
            OffsetBuffer::from_lengths([vector.len()]),
            Arc::new(Float32Array::from(vector.to_vec())),
            None,
        )?,
    };
    Ok(ScalarValue::List(Arc::new(array)))
}

fn check_dimension(expected: usize, actual: usize) -> Result<()> {
    if expected == actual {
        Ok(())
    } else {
        exec_err!(
            "vector_avg requires vectors with matching dimensions, got {expected} and {actual}"
        )
    }
}

/// Accumulator for `vector_avg`.
///
/// Spark keeps a single-precision running mean rather than a sum and a count,
/// so the same formulas are reproduced here to match its rounding behavior.
/// The state is `(avg: ARRAY<FLOAT>, count: BIGINT)`, where a null vector and
/// a zero count represent an empty partial result.
#[derive(Debug, Default)]
struct VectorAvgAccumulator {
    avg: Option<Vec<f32>>,
    count: i64,
}

impl VectorAvgAccumulator {
    fn update_vectors<O: OffsetSizeTrait>(&mut self, vectors: &GenericListArray<O>) -> Result<()> {
        for vector in vectors.iter().flatten() {
            let vector = as_float32_array(&vector)?;
            // Spark skips the whole vector when any element is null.
            if vector.null_count() > 0 {
                continue;
            }
            self.update_vector(vector.values())?;
        }
        Ok(())
    }

    fn update_vector(&mut self, input: &[f32]) -> Result<()> {
        let Some(avg) = &mut self.avg else {
            self.avg = Some(input.to_vec());
            self.count = 1;
            return Ok(());
        };
        check_dimension(avg.len(), input.len())?;
        self.count += 1;
        // Spark computes `1.0f / newCount` once, in single precision.
        let inv_count = 1.0 / self.count as f32;
        for (avg, value) in avg.iter_mut().zip(input) {
            *avg += (*value - *avg) * inv_count;
        }
        Ok(())
    }

    fn merge_vector(&mut self, other: &[f32], other_count: i64) -> Result<()> {
        let Some(avg) = &mut self.avg else {
            self.avg = Some(other.to_vec());
            self.count = other_count;
            return Ok(());
        };
        check_dimension(avg.len(), other.len())?;
        let total = (self.count + other_count) as f32;
        let left_weight = self.count as f32 / total;
        let right_weight = other_count as f32 / total;
        for (avg, value) in avg.iter_mut().zip(other) {
            *avg = *avg * left_weight + *value * right_weight;
        }
        self.count += other_count;
        Ok(())
    }
}

impl Accumulator for VectorAvgAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let [vectors] = values else {
            return exec_err!("vector_avg expects one argument, got {}", values.len());
        };
        match vectors.data_type() {
            DataType::List(_) => self.update_vectors(as_list_array(vectors)?),
            DataType::LargeList(_) => self.update_vectors(as_large_list_array(vectors)?),
            data_type => {
                exec_err!("vector_avg expects an ARRAY<FLOAT> argument, got {data_type:?}")
            }
        }
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        float_vector_scalar(self.avg.as_deref())
    }

    fn size(&self) -> usize {
        let avg = self.avg.as_ref().map_or(0, Vec::capacity);
        std::mem::size_of_val(self) + avg * std::mem::size_of::<f32>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?, ScalarValue::Int64(Some(self.count))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let [vectors, counts] = states else {
            return exec_err!("vector_avg expects two state columns, got {}", states.len());
        };
        let vectors = as_list_array(vectors)?;
        let counts = as_int64_array(counts)?;
        for (vector, count) in vectors.iter().zip(counts) {
            if let (Some(vector), Some(count)) = (vector, count)
                && count > 0
            {
                self.merge_vector(as_float32_array(&vector)?.values(), count)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct VectorAvg {
    signature: Signature,
}

impl Default for VectorAvg {
    fn default() -> Self {
        Self::new()
    }
}

impl VectorAvg {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for VectorAvg {
    fn name(&self) -> &str {
        "vector_avg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // Spark 4.2 only accepts `ARRAY<FLOAT>` and does not implicitly cast
        // other element types, so wider element types are rejected here as well.
        match arg_types {
            [vector] if is_float_vector(vector) => Ok(float_vector_type()),
            _ => plan_err!("vector_avg expects an ARRAY<FLOAT> argument, got {arg_types:?}"),
        }
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(VectorAvgAccumulator::default()))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Field::new(
                format_state_name(args.name, "avg"),
                float_vector_type(),
                true,
            )
            .into(),
            Field::new(
                format_state_name(args.name, "count"),
                DataType::Int64,
                false,
            )
            .into(),
        ])
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Int64Array, LargeListArray};
    use datafusion::arrow::datatypes::Float32Type;

    use super::*;

    fn vectors(rows: Vec<Option<Vec<Option<f32>>>>) -> ArrayRef {
        Arc::new(ListArray::from_iter_primitive::<Float32Type, _, _>(rows))
    }

    fn result_vector(scalar: &ScalarValue) -> Result<Option<Vec<f32>>> {
        let ScalarValue::List(list) = scalar else {
            return exec_err!("expected a list scalar, got {scalar:?}");
        };
        if list.is_null(0) {
            return Ok(None);
        }
        Ok(Some(as_float32_array(&list.value(0))?.values().to_vec()))
    }

    #[test]
    fn averages_vectors_element_wise() -> Result<()> {
        let mut acc = VectorAvgAccumulator::default();
        acc.update_batch(&[vectors(vec![
            Some(vec![Some(1.0), Some(2.0)]),
            Some(vec![Some(3.0), Some(4.0)]),
        ])])?;
        assert_eq!(result_vector(&acc.evaluate()?)?, Some(vec![2.0, 3.0]));
        Ok(())
    }

    #[test]
    fn skips_null_vectors_and_vectors_with_null_elements() -> Result<()> {
        let mut acc = VectorAvgAccumulator::default();
        acc.update_batch(&[vectors(vec![
            None,
            Some(vec![Some(1.0), None]),
            Some(vec![Some(2.0), Some(4.0)]),
            None,
        ])])?;
        assert_eq!(result_vector(&acc.evaluate()?)?, Some(vec![2.0, 4.0]));
        Ok(())
    }

    #[test]
    fn empty_group_returns_null() -> Result<()> {
        let mut acc = VectorAvgAccumulator::default();
        acc.update_batch(&[vectors(vec![None, Some(vec![Some(1.0), None])])])?;
        let result = acc.evaluate()?;
        assert_eq!(result.data_type(), float_vector_type());
        assert_eq!(result_vector(&result)?, None);
        Ok(())
    }

    #[test]
    fn empty_vectors_average_to_an_empty_vector() -> Result<()> {
        let mut acc = VectorAvgAccumulator::default();
        acc.update_batch(&[vectors(vec![Some(vec![]), Some(vec![])])])?;
        assert_eq!(result_vector(&acc.evaluate()?)?, Some(vec![]));
        Ok(())
    }

    #[test]
    fn rejects_dimension_mismatch() {
        let mut acc = VectorAvgAccumulator::default();
        let result = acc.update_batch(&[vectors(vec![
            Some(vec![Some(1.0), Some(2.0)]),
            Some(vec![Some(3.0)]),
        ])]);
        assert!(matches!(
            result,
            Err(error) if error.to_string().contains("matching dimensions, got 2 and 1")
        ));
    }

    #[test]
    fn matches_spark_running_mean_rounding() -> Result<()> {
        // The same input as the `vector_avg.feature` precision scenario. Spark's
        // incremental float mean gives 5.0500016 here, while sum / count gives 5.05.
        let rows = (1..=100)
            .map(|i| Some(vec![Some(i as f32 * 0.1)]))
            .collect();
        let mut acc = VectorAvgAccumulator::default();
        acc.update_batch(&[vectors(rows)])?;
        assert_eq!(result_vector(&acc.evaluate()?)?, Some(vec![5.050_001_6]));
        Ok(())
    }

    #[test]
    fn supports_large_list_input() -> Result<()> {
        let rows = vec![
            Some(vec![Some(1.0), Some(2.0)]),
            Some(vec![Some(3.0), Some(6.0)]),
        ];
        let large = Arc::new(LargeListArray::from_iter_primitive::<Float32Type, _, _>(
            rows,
        )) as ArrayRef;
        let mut acc = VectorAvgAccumulator::default();
        acc.update_batch(&[large])?;
        assert_eq!(result_vector(&acc.evaluate()?)?, Some(vec![2.0, 4.0]));
        Ok(())
    }

    #[test]
    fn merges_partial_states_with_count_weights() -> Result<()> {
        let mut left = VectorAvgAccumulator::default();
        left.update_batch(&[vectors(vec![
            Some(vec![Some(1.0), Some(2.0)]),
            Some(vec![Some(3.0), Some(4.0)]),
            Some(vec![Some(5.0), Some(6.0)]),
        ])])?;
        let mut right = VectorAvgAccumulator::default();
        right.update_batch(&[vectors(vec![Some(vec![Some(7.0), Some(8.0)])])])?;
        let mut empty = VectorAvgAccumulator::default();

        let states = [left.state()?, right.state()?, empty.state()?];
        let columns = (0..2)
            .map(|i| ScalarValue::iter_to_array(states.iter().map(|state| state[i].clone())))
            .collect::<Result<Vec<_>>>()?;

        let mut merged = VectorAvgAccumulator::default();
        merged.merge_batch(&columns)?;
        assert_eq!(merged.count, 4);
        assert_eq!(result_vector(&merged.evaluate()?)?, Some(vec![4.0, 5.0]));
        Ok(())
    }

    #[test]
    fn merge_rejects_dimension_mismatch() -> Result<()> {
        let mut acc = VectorAvgAccumulator::default();
        acc.update_batch(&[vectors(vec![Some(vec![Some(1.0), Some(2.0)])])])?;
        let other_vector = vectors(vec![Some(vec![Some(1.0)])]);
        let other_count = Arc::new(Int64Array::from(vec![1])) as ArrayRef;
        let result = acc.merge_batch(&[other_vector, other_count]);
        assert!(matches!(
            result,
            Err(error) if error.to_string().contains("matching dimensions, got 2 and 1")
        ));
        Ok(())
    }

    #[test]
    fn rejects_non_float_vectors() {
        let udaf = VectorAvg::new();
        let list =
            |element: DataType| DataType::List(Arc::new(Field::new_list_field(element, true)));
        assert!(udaf.return_type(&[DataType::Float32]).is_err());
        assert!(udaf.return_type(&[list(DataType::Float64)]).is_err());
        assert_eq!(
            udaf.return_type(&[list(DataType::Float32)]).ok(),
            Some(float_vector_type())
        );
    }
}
