use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray, ListArray, new_empty_array};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute::concat;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::logical_expr::Accumulator;
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::function::StateFieldsArgs;
use datafusion_expr::utils::format_state_name;

pub trait BatchAggregator: Send + Sync {
    fn call(&self, args: &[ArrayRef]) -> Result<ArrayRef>;
}

/// An accumulator that stores all batches in the state and apply aggregation at the end.
pub struct BatchAggregateAccumulator {
    input_types: Vec<DataType>,
    inputs: Vec<VecDeque<ArrayRef>>,
    num_rows: usize,
    output_type: DataType,
    aggregator: Box<dyn BatchAggregator>,
    /// The number of arguments the Python function actually accepts.
    /// May be less than `input_types.len()` when a dummy argument was injected
    /// to satisfy DataFusion's requirement that every aggregate has at least one input.
    actual_arg_count: usize,
}

impl Debug for BatchAggregateAccumulator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchAggregateAccumulator")
            .field("input_types", &self.input_types)
            .field("inputs", &self.inputs)
            .field("output_type", &self.output_type)
            .finish()
    }
}

impl BatchAggregateAccumulator {
    pub fn new(
        input_types: Vec<DataType>,
        output_type: DataType,
        aggregator: Box<dyn BatchAggregator>,
        actual_arg_count: usize,
    ) -> Self {
        let num_inputs = input_types.len();
        Self {
            input_types,
            inputs: vec![VecDeque::new(); num_inputs],
            num_rows: 0,
            output_type,
            aggregator,
            actual_arg_count,
        }
    }

    pub fn state_fields(args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        // We accumulate the inputs in the state.
        // Each state field corresponds to an input argument.
        let fields = args
            .input_fields
            .iter()
            .enumerate()
            .map(|(i, input_field)| {
                let name = format_state_name(args.name, &i.to_string());
                let field = Field::new_list_field(input_field.data_type().clone(), true);
                Field::new(name, DataType::List(Arc::new(field)), true).into()
            })
            .collect();
        Ok(fields)
    }
}

impl Accumulator for BatchAggregateAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.len() != self.inputs.len() {
            return exec_err!(
                "expected {} arguments, got {}",
                self.inputs.len(),
                values.len()
            );
        }
        let rows = values.first().map_or(0, |value| value.len());
        if values.iter().any(|value| value.len() != rows) {
            return exec_err!("aggregate arguments must have the same number of rows");
        }
        if rows == 0 {
            return Ok(());
        }
        for (input, value) in self.inputs.iter_mut().zip(values.iter()) {
            input.push_back(value.clone());
        }
        self.num_rows += rows;
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.len() != self.inputs.len() {
            return exec_err!(
                "expected {} arguments, got {}",
                self.inputs.len(),
                values.len()
            );
        }
        let rows = values.first().map_or(0, |value| value.len());
        if values.iter().any(|value| value.len() != rows) {
            return exec_err!("retracted arguments must have the same number of rows");
        }
        if self.num_rows < rows {
            return exec_err!("cannot retract more rows than the accumulator contains");
        }
        for input in &mut self.inputs {
            let mut remaining = rows;
            while remaining > 0
                && let Some(batch) = input.front_mut()
            {
                if remaining < batch.len() {
                    *batch = batch.slice(remaining, batch.len() - remaining);
                    break;
                }
                remaining -= batch.len();
                input.pop_front();
            }
        }
        self.num_rows -= rows;
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let inputs = self
            .inputs
            .iter()
            .zip(&self.input_types)
            .map(|(input, data_type)| {
                let input = match input.len() {
                    0 => new_empty_array(data_type),
                    1 => Arc::clone(&input[0]),
                    _ => concat(&input.iter().map(|x| x.as_ref()).collect::<Vec<_>>())?,
                };
                Ok(input)
            })
            .collect::<Result<Vec<_>>>()?;
        let array = self.aggregator.call(&inputs[..self.actual_arg_count])?;
        if array.len() != 1 {
            return exec_err!("expected a single value, got {}", array.len());
        }
        let scalar = ScalarValue::try_from_array(&array, 0)?;
        Ok(scalar)
    }

    fn size(&self) -> usize {
        let mut size = size_of_val(self);
        size += size_of::<VecDeque<ArrayRef>>() * self.inputs.capacity();
        for input in &self.inputs {
            size += size_of::<ArrayRef>() * input.capacity();
            for array in input {
                size += array.get_array_memory_size();
            }
        }
        size += self.output_type.size();
        size -= size_of_val(&self.output_type);
        size
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let state = self
            .inputs
            .iter()
            .zip(&self.input_types)
            .map(|(input, data_type)| {
                let field = Arc::new(Field::new_list_field(data_type.clone(), true));
                let input = input.iter().map(|x| x.as_ref()).collect::<Vec<_>>();
                let input = if input.is_empty() {
                    ListArray::new_null(field, 0)
                } else {
                    let values = concat(&input)?;
                    let offsets = OffsetBuffer::from_lengths([values.len()]);
                    ListArray::new(field, offsets, values, None)
                };
                Ok(ScalarValue::List(Arc::new(input)))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(state)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.len() != self.inputs.len() {
            return exec_err!(
                "expected {} arguments in the states, got {}",
                self.inputs.len(),
                states.len()
            );
        }
        let batches = states
            .iter()
            .map(|state| {
                state
                    .as_list::<i32>()
                    .iter()
                    .flatten()
                    .filter(|array| !array.is_empty())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let rows = batches.first().map_or(0, |batches| {
            batches.iter().map(|batch| batch.len()).sum::<usize>()
        });
        if batches
            .iter()
            .any(|batches| batches.iter().map(|batch| batch.len()).sum::<usize>() != rows)
        {
            return exec_err!("aggregate states must have the same number of rows");
        }
        for (input, batches) in self.inputs.iter_mut().zip(batches) {
            input.extend(batches);
        }
        self.num_rows += rows;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Int32Array, StructArray};

    use super::*;
    use crate::array::build_singleton_list_array;

    struct CollectInputs;

    impl BatchAggregator for CollectInputs {
        fn call(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
            let fields = vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Int32, true),
            ];
            let rows = StructArray::try_new(fields.into(), args.to_vec(), None)?;
            Ok(build_singleton_list_array(Arc::new(rows)))
        }
    }

    fn arrays(a: &[Option<i32>], b: &[Option<i32>]) -> Vec<ArrayRef> {
        vec![
            Arc::new(Int32Array::from(a.to_vec())),
            Arc::new(Int32Array::from(b.to_vec())),
        ]
    }

    fn accumulator() -> Result<BatchAggregateAccumulator> {
        let output = CollectInputs.call(&arrays(&[], &[]))?;
        Ok(BatchAggregateAccumulator::new(
            vec![DataType::Int32; 2],
            output.data_type().clone(),
            Box::new(CollectInputs),
            2,
        ))
    }

    fn assert_frame(
        accumulator: &mut BatchAggregateAccumulator,
        a: &[Option<i32>],
        b: &[Option<i32>],
    ) -> Result<()> {
        let expected = CollectInputs.call(&arrays(a, b))?;
        assert_eq!(
            accumulator.evaluate()?,
            ScalarValue::try_from_array(&expected, 0)?
        );
        Ok(())
    }

    #[test]
    fn retract_preserves_fifo_rows_across_batches() -> Result<()> {
        let mut accumulator = accumulator()?;
        assert!(accumulator.supports_retract_batch());
        accumulator.update_batch(&arrays(&[], &[]))?;
        accumulator.update_batch(&arrays(
            &[Some(1), None, Some(1)],
            &[None, Some(2), Some(3)],
        ))?;
        accumulator.update_batch(&arrays(&[], &[]))?;
        accumulator.update_batch(&arrays(&[Some(4), None], &[Some(5), Some(6)]))?;
        accumulator.update_batch(&arrays(&[Some(7)], &[None]))?;
        accumulator.retract_batch(&arrays(&[Some(1)], &[None]))?;
        assert_frame(
            &mut accumulator,
            &[None, Some(1), Some(4), None, Some(7)],
            &[Some(2), Some(3), Some(5), Some(6), None],
        )?;
        accumulator.retract_batch(&arrays(
            &[None, Some(1), Some(4)],
            &[Some(2), Some(3), Some(5)],
        ))?;
        assert_frame(&mut accumulator, &[None, Some(7)], &[Some(6), None])?;
        accumulator.retract_batch(&arrays(&[None, Some(7)], &[Some(6), None]))?;
        accumulator.retract_batch(&arrays(&[], &[]))?;
        assert_frame(&mut accumulator, &[], &[])?;
        accumulator.update_batch(&arrays(&[Some(8)], &[Some(9)]))?;
        assert_frame(&mut accumulator, &[Some(8)], &[Some(9)])
    }

    #[test]
    fn invalid_retraction_preserves_state() -> Result<()> {
        let mut accumulator = accumulator()?;
        accumulator.update_batch(&arrays(&[Some(1), None], &[Some(2), Some(3)]))?;
        assert!(accumulator.retract_batch(&[]).is_err());
        assert!(accumulator.retract_batch(&arrays(&[None], &[])).is_err());
        assert!(
            accumulator
                .retract_batch(&arrays(&[None; 3], &[None; 3]))
                .is_err()
        );
        assert_frame(&mut accumulator, &[Some(1), None], &[Some(2), Some(3)])
    }

    #[test]
    fn state_merge_retains_only_the_current_frame() -> Result<()> {
        let mut source = accumulator()?;
        source.update_batch(&arrays(&[Some(1), Some(2)], &[Some(3), None]))?;
        source.retract_batch(&arrays(&[Some(1)], &[Some(3)]))?;
        let states = source
            .state()?
            .iter()
            .map(ScalarValue::to_array)
            .collect::<Result<Vec<_>>>()?;
        let mut target = accumulator()?;
        target.merge_batch(&states)?;
        assert_frame(&mut target, &[Some(2)], &[None])?;
        target.retract_batch(&arrays(&[Some(2)], &[None]))?;
        assert_frame(&mut target, &[], &[])
    }
}
