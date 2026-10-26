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

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            "vector_sum",
            Self::data_type(),
            true,
        ))])
    }

    fn accumulator(&self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(VectorSumAccumulator::default()))
    }
}

#[derive(Debug, Default)]
struct VectorSumAccumulator {
    sum: Option<Vec<f32>>,
}

impl VectorSumAccumulator {
    fn add(&mut self, vector: &[f32]) -> Result<()> {
        match &mut self.sum {
            None => self.sum = Some(vector.to_vec()),
            Some(sum) => {
                if sum.len() != vector.len() {
                    return exec_err!(
                        "vector_sum requires vectors with matching dimensions, got {} and {}",
                        sum.len(),
                        vector.len()
                    );
                }
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
                self.add(vector.values())?;
            }
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
        self.result()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.result()?])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        match states {
            [array] if matches!(array.data_type(), DataType::List(_)) => {
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
        let mut empty = VectorSumAccumulator { sum: Some(vec![]) };
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
}
