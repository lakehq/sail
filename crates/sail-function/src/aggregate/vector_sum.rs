use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Float32Array, GenericListArray, ListArray, OffsetSizeTrait,
    as_large_list_array, as_list_array,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Float32Type};
use datafusion_common::{Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use ordered_float::OrderedFloat;

use crate::aggregate::vector::normalize;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct VectorSum {
    signature: Signature,
}

impl Default for VectorSum {
    fn default() -> Self {
        Self::new()
    }
}

impl VectorSum {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }

    fn data_type() -> DataType {
        DataType::List(Arc::new(Field::new("item", DataType::Float32, false)))
    }
}

impl AggregateUDFImpl for VectorSum {
    fn name(&self) -> &str {
        "vector_sum"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if !matches!(arg_types, [DataType::List(field) | DataType::LargeList(field)]
            if field.data_type() == &DataType::Float32)
        {
            return plan_err!("vector_sum expects one ARRAY<FLOAT> argument, got {arg_types:?}");
        }
        Ok(Self::data_type())
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let state_type = if args.is_distinct {
            DataType::List(Arc::new(Field::new("item", Self::data_type(), false)))
        } else {
            Self::data_type()
        };
        Ok(vec![Arc::new(Field::new(
            if args.is_distinct {
                "distinct_vector_sum"
            } else {
                "vector_sum"
            },
            state_type,
            true,
        ))])
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(VectorSumAccumulator::new(args.is_distinct)))
    }

    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if args.is_distinct {
            return plan_err!("DISTINCT window functions are not supported: vector_sum");
        }
        Ok(Box::new(VectorSumAccumulator {
            window_vectors: Some(VecDeque::new()),
            ..Default::default()
        }))
    }
}

#[derive(Debug, Default)]
struct VectorSumAccumulator {
    sum: Option<Vec<f32>>,
    seen: Option<HashSet<Vec<OrderedFloat<f32>>>>,
    vectors: Vec<Vec<f32>>,
    window_vectors: Option<VecDeque<Vec<f32>>>,
}

impl VectorSumAccumulator {
    fn new(is_distinct: bool) -> Self {
        Self {
            seen: is_distinct.then(HashSet::new),
            ..Self::default()
        }
    }

    fn add(&mut self, vector: &[f32]) -> Result<()> {
        if let Some(sum) = &self.sum
            && sum.len() != vector.len()
        {
            return exec_err!(
                "vector_sum requires vectors with matching dimensions, got {} and {}",
                sum.len(),
                vector.len()
            );
        }
        let normalized = self
            .seen
            .as_ref()
            .map(|_| vector.iter().copied().map(normalize).collect::<Vec<_>>());
        let vector = normalized.as_deref().unwrap_or(vector);
        if let Some(seen) = &mut self.seen {
            if !seen.insert(vector.iter().copied().map(OrderedFloat).collect()) {
                return Ok(());
            }
            self.vectors.push(vector.to_vec());
        }
        match &mut self.sum {
            None => self.sum = Some(vector.to_vec()),
            Some(sum) => {
                for (total, value) in sum.iter_mut().zip(vector) {
                    *total += value;
                }
            }
        }
        Ok(())
    }

    fn update<O: OffsetSizeTrait>(&mut self, vectors: &GenericListArray<O>) -> Result<()> {
        for row in 0..vectors.len() {
            if vectors.is_null(row) {
                continue;
            }
            let vector = vectors.value(row);
            let vector = vector.as_primitive::<Float32Type>();
            if vector.null_count() == 0 {
                if let Some(window_vectors) = &mut self.window_vectors {
                    window_vectors.push_back(vector.values().to_vec());
                } else {
                    self.add(vector.values())?;
                }
            }
        }
        Ok(())
    }

    fn retract<O: OffsetSizeTrait>(&mut self, vectors: &GenericListArray<O>) -> Result<()> {
        let Some(window_vectors) = &mut self.window_vectors else {
            return exec_err!("vector_sum cannot retract from a non-sliding accumulator");
        };
        for row in 0..vectors.len() {
            if vectors.is_null(row) {
                continue;
            }
            let vector = vectors.value(row);
            let vector = vector.as_primitive::<Float32Type>();
            if vector.null_count() > 0 {
                continue;
            }
            let Some(departing) = window_vectors.pop_front() else {
                return exec_err!("vector_sum cannot retract a vector from an empty window");
            };
            if !departing
                .iter()
                .map(|value| value.to_bits())
                .eq(vector.values().iter().map(|value| value.to_bits()))
            {
                return exec_err!("vector_sum retracted vector does not match the window input");
            }
        }
        Ok(())
    }

    fn recompute_window_sum(&mut self) -> Result<()> {
        if let Some(window_vectors) = &self.window_vectors {
            let mut accumulator = Self::default();
            for vector in window_vectors {
                accumulator.add(vector)?;
            }
            self.sum = accumulator.sum;
        }
        Ok(())
    }

    fn result(&self) -> Result<ScalarValue> {
        let field = Arc::new(Field::new("item", DataType::Float32, false));
        let list = match &self.sum {
            None => ListArray::new_null(field, 1),
            Some(sum) => {
                let len = i32::try_from(sum.len()).map_err(|_| {
                    datafusion_common::DataFusionError::Execution(
                        "vector_sum dimension exceeds the maximum list size".to_string(),
                    )
                })?;
                ListArray::new(
                    field,
                    OffsetBuffer::new(vec![0, len].into()),
                    Arc::new(Float32Array::from(sum.clone())),
                    None,
                )
            }
        };
        Ok(ScalarValue::List(Arc::new(list)))
    }

    fn distinct_state(&self) -> Result<ScalarValue> {
        let field = Arc::new(Field::new("item", VectorSum::data_type(), false));
        if self.sum.is_none() {
            return Ok(ScalarValue::List(Arc::new(ListArray::new_null(field, 1))));
        }

        let mut offsets = Vec::with_capacity(self.vectors.len() + 1);
        let mut values = Vec::new();
        let mut offset = 0_i32;
        offsets.push(offset);
        for vector in &self.vectors {
            let len = i32::try_from(vector.len()).map_err(|_| {
                datafusion_common::DataFusionError::Execution(
                    "vector_sum dimension exceeds the maximum list size".to_string(),
                )
            })?;
            offset = offset.checked_add(len).ok_or_else(|| {
                datafusion_common::DataFusionError::Execution(
                    "vector_sum state exceeds the maximum list size".to_string(),
                )
            })?;
            offsets.push(offset);
            values.extend_from_slice(vector);
        }
        let count = i32::try_from(self.vectors.len()).map_err(|_| {
            datafusion_common::DataFusionError::Execution(
                "vector_sum state exceeds the maximum list size".to_string(),
            )
        })?;
        let vectors = ListArray::new(
            Arc::new(Field::new("item", DataType::Float32, false)),
            OffsetBuffer::new(offsets.into()),
            Arc::new(Float32Array::from(values)),
            None,
        );
        Ok(ScalarValue::List(Arc::new(ListArray::new(
            field,
            OffsetBuffer::new(vec![0, count].into()),
            Arc::new(vectors),
            None,
        ))))
    }
}

impl Accumulator for VectorSumAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        match values {
            [array] => match array.data_type() {
                DataType::List(_) => self.update(as_list_array(array)),
                DataType::LargeList(_) => self.update(as_large_list_array(array)),
                other => exec_err!("vector_sum expects an ARRAY<FLOAT>, got {other:?}"),
            },
            _ => exec_err!("vector_sum needs exactly one argument"),
        }
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.recompute_window_sum()?;
        self.result()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.recompute_window_sum()?;
        Ok(vec![if self.seen.is_some() {
            self.distinct_state()?
        } else {
            self.result()?
        }])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        match states {
            [array]
                if self.seen.is_some()
                    && matches!(array.data_type(), DataType::List(field) if field.data_type() == &VectorSum::data_type()) =>
            {
                let states = as_list_array(array);
                for row in 0..states.len() {
                    if !states.is_null(row) {
                        self.update(as_list_array(&states.value(row)))?;
                    }
                }
                Ok(())
            }
            [array] if self.seen.is_none() && matches!(array.data_type(), DataType::List(_)) => {
                self.update(as_list_array(array))
            }
            _ => exec_err!("vector_sum expects one list state"),
        }
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + self
                .sum
                .as_ref()
                .map_or(0, |sum| sum.capacity() * size_of::<f32>())
            + self.vectors.capacity() * size_of::<Vec<f32>>()
            + self
                .vectors
                .iter()
                .map(|v| v.capacity() * size_of::<f32>())
                .sum::<usize>()
            + self.seen.as_ref().map_or(0, |seen| {
                seen.capacity() * size_of::<Vec<OrderedFloat<f32>>>()
                    + seen
                        .iter()
                        .map(|v| v.capacity() * size_of::<OrderedFloat<f32>>())
                        .sum::<usize>()
            })
            + self.window_vectors.as_ref().map_or(0, |vectors| {
                vectors.capacity() * size_of::<Vec<f32>>()
                    + vectors
                        .iter()
                        .map(|v| v.capacity() * size_of::<f32>())
                        .sum::<usize>()
            })
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        match values {
            [array] => match array.data_type() {
                DataType::List(_) => self.retract(as_list_array(array)),
                DataType::LargeList(_) => self.retract(as_large_list_array(array)),
                other => exec_err!("vector_sum expects an ARRAY<FLOAT>, got {other:?}"),
            },
            _ => exec_err!("vector_sum needs exactly one argument"),
        }
    }

    fn supports_retract_batch(&self) -> bool {
        self.window_vectors.is_some()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::LargeListArray;

    use super::*;

    #[test]
    fn skips_null_vectors_and_elements_and_tracks_empty_vectors() -> Result<()> {
        let vectors = ListArray::from_iter_primitive::<Float32Type, _, _>(vec![
            None,
            Some(vec![Some(9.0), None, Some(10.0)]),
            Some(vec![Some(1.0), Some(2.0)]),
            Some(vec![Some(3.0), Some(4.0)]),
        ]);
        let mut acc = VectorSumAccumulator::default();
        acc.update_batch(&[Arc::new(vectors)])?;
        assert_eq!(acc.sum, Some(vec![4.0, 6.0]));
        assert_eq!(
            acc.evaluate()?
                .to_array()?
                .as_list::<i32>()
                .value(0)
                .data_type(),
            &DataType::Float32
        );

        let empty = ListArray::from_iter_primitive::<Float32Type, _, _>(vec![Some(vec![])]);
        let mut acc = VectorSumAccumulator::default();
        assert!(acc.evaluate()?.is_null());
        acc.update_batch(&[Arc::new(empty)])?;
        assert!(!acc.evaluate()?.is_null());
        assert_eq!(
            acc.evaluate()?.to_array()?.as_list::<i32>().value(0).len(),
            0
        );
        Ok(())
    }

    #[test]
    fn merges_partial_states_and_rejects_dimension_mismatch() -> Result<()> {
        let mut left = VectorSumAccumulator::default();
        left.add(&[1.0, 2.0])?;
        let mut right = VectorSumAccumulator::default();
        right.add(&[3.0, 4.0])?;
        let state = right.state()?[0].to_array()?;
        left.merge_batch(&[state])?;
        assert_eq!(left.sum, Some(vec![4.0, 6.0]));
        assert!(matches!(
            left.add(&[1.0]),
            Err(error) if error.to_string().contains("matching dimensions")
        ));
        let mut empty = VectorSumAccumulator {
            sum: Some(vec![]),
            ..Default::default()
        };
        assert!(left.merge_batch(&[empty.state()?[0].to_array()?]).is_err());
        Ok(())
    }

    #[test]
    fn accepts_large_list_inputs() -> Result<()> {
        let vectors = LargeListArray::from_iter_primitive::<Float32Type, _, _>(vec![
            Some(vec![Some(1.0), Some(2.0)]),
            Some(vec![Some(3.0), Some(4.0)]),
        ]);
        let mut acc = VectorSumAccumulator::default();
        acc.update_batch(&[Arc::new(vectors)])?;
        assert_eq!(acc.sum, Some(vec![4.0, 6.0]));
        Ok(())
    }

    #[test]
    fn distinct_states_deduplicate_across_partitions() -> Result<()> {
        let vectors = ListArray::from_iter_primitive::<Float32Type, _, _>(vec![
            None,
            Some(vec![Some(8.0), None]),
            Some(vec![Some(1.0), Some(2.0)]),
            Some(vec![Some(1.0), Some(2.0)]),
        ]);
        let mut left = VectorSumAccumulator::new(true);
        left.update_batch(&[Arc::new(vectors)])?;
        let mut right = VectorSumAccumulator::new(true);
        right.add(&[1.0, 2.0])?;
        right.add(&[3.0, 4.0])?;
        left.merge_batch(&[right.state()?[0].to_array()?])?;
        assert_eq!(left.sum, Some(vec![4.0, 6.0]));
        assert_eq!(left.vectors.len(), 2);
        assert!(left.merge_batch(&[right.state()?[0].to_array()?]).is_ok());
        assert_eq!(left.sum, Some(vec![4.0, 6.0]));

        let mut empty = VectorSumAccumulator::new(true);
        left.merge_batch(&[empty.state()?[0].to_array()?])?;
        assert_eq!(left.sum, Some(vec![4.0, 6.0]));
        empty.add(&[])?;
        assert!(left.merge_batch(&[empty.state()?[0].to_array()?]).is_err());
        let mut empty_group = VectorSumAccumulator::new(true);
        empty_group.merge_batch(&[empty.state()?[0].to_array()?])?;
        assert!(!empty_group.evaluate()?.is_null());
        assert_eq!(empty_group.sum, Some(vec![]));
        Ok(())
    }

    #[test]
    fn distinct_normalizes_signed_zero_in_inputs_and_merged_states() -> Result<()> {
        for (first, second) in [(-0.0, 0.0), (0.0, -0.0)] {
            let mut left = VectorSumAccumulator::new(true);
            left.add(&[first])?;
            left.add(&[second])?;
            assert_eq!(
                left.sum.as_ref().map(|sum| sum[0].to_bits()),
                Some(0.0_f32.to_bits())
            );
            assert_eq!(left.vectors[0][0].to_bits(), 0.0_f32.to_bits());

            let mut right = VectorSumAccumulator::new(true);
            right.add(&[second])?;
            let state = right.state()?[0].to_array()?;
            left.merge_batch(&[state])?;
            assert_eq!(left.vectors.len(), 1);
            assert_eq!(
                left.sum.as_ref().map(|sum| sum[0].to_bits()),
                Some(0.0_f32.to_bits())
            );

            let mut merged = VectorSumAccumulator::new(true);
            merged.merge_batch(&[left.state()?[0].to_array()?])?;
            assert_eq!(merged.vectors[0][0].to_bits(), 0.0_f32.to_bits());
        }

        let mut nan = VectorSumAccumulator::new(true);
        nan.add(&[f32::from_bits(0x7fc0_0001)])?;
        nan.add(&[f32::from_bits(0xffc0_0002)])?;
        assert_eq!(nan.vectors.len(), 1);
        assert_eq!(nan.vectors[0][0].to_bits(), f32::NAN.to_bits());

        let mut ordinary = VectorSumAccumulator::default();
        ordinary.add(&[-0.0])?;
        assert_eq!(
            ordinary.sum.as_ref().map(|sum| sum[0].to_bits()),
            Some((-0.0_f32).to_bits())
        );
        Ok(())
    }

    #[test]
    fn sliding_window_retracts_valid_vectors_and_preserves_empty_frames() -> Result<()> {
        let mut acc = VectorSumAccumulator {
            window_vectors: Some(VecDeque::new()),
            ..Default::default()
        };
        let first = Arc::new(ListArray::from_iter_primitive::<Float32Type, _, _>(vec![
            None,
            Some(vec![Some(9.0), None]),
            Some(vec![Some(1.0)]),
        ])) as ArrayRef;
        acc.update_batch(std::slice::from_ref(&first))?;
        assert_eq!(acc.sum, None);
        assert_eq!(
            acc.evaluate()?.to_array()?.as_list::<i32>().value(0).len(),
            1
        );
        let next = Arc::new(ListArray::from_iter_primitive::<Float32Type, _, _>(vec![
            Some(vec![Some(2.0), Some(3.0)]),
        ])) as ArrayRef;
        acc.update_batch(&[next])?;
        acc.retract_batch(&[first])?;
        assert_eq!(
            acc.evaluate()?.to_array()?.as_list::<i32>().value(0).len(),
            2
        );
        let last = Arc::new(ListArray::from_iter_primitive::<Float32Type, _, _>(vec![
            Some(vec![Some(2.0), Some(3.0)]),
        ])) as ArrayRef;
        acc.retract_batch(&[last])?;
        assert!(acc.evaluate()?.is_null());

        let empty = Arc::new(ListArray::from_iter_primitive::<Float32Type, _, _>(vec![
            Some(vec![]),
        ])) as ArrayRef;
        acc.update_batch(std::slice::from_ref(&empty))?;
        assert!(!acc.evaluate()?.is_null());
        acc.retract_batch(&[empty])?;
        assert!(acc.evaluate()?.is_null());
        Ok(())
    }
}
