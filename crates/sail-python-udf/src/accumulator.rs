use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::array::{new_empty_array, ArrayRef, AsArray, ListArray};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute::concat;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::logical_expr::Accumulator;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::function::StateFieldsArgs;
use datafusion_expr::utils::format_state_name;

pub trait BatchAggregator: Send + Sync {
    fn call(&self, args: &[ArrayRef]) -> Result<ArrayRef>;
}

/// An accumulator that stores all batches in the state and apply aggregation at the end.
pub struct BatchAggregateAccumulator {
    input_types: Vec<DataType>,
    inputs: Vec<Vec<ArrayRef>>,
    output_type: DataType,
    aggregator: Box<dyn BatchAggregator>,
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
    ) -> Self {
        let num_inputs = input_types.len();
        Self {
            input_types,
            inputs: vec![vec![]; num_inputs],
            output_type,
            aggregator,
        }
    }

    pub fn state_fields(args: StateFieldsArgs) -> Result<Vec<Field>> {
        // We accumulate the inputs in the state.
        // Each state field corresponds to an input argument.
        let fields = args
            .input_types
            .iter()
            .enumerate()
            .map(|(i, dt)| {
                let name = format_state_name(args.name, &i.to_string());
                let field = Field::new_list_field(dt.clone(), true);
                Field::new(name, DataType::List(Arc::new(field)), true)
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
        for (input, value) in self.inputs.iter_mut().zip(values.iter()) {
            input.push(value.clone());
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let inputs = self
            .inputs
            .iter()
            .zip(&self.input_types)
            .map(|(input, data_type)| {
                let input = input.iter().map(|x| x.as_ref()).collect::<Vec<_>>();
                let input = if input.is_empty() {
                    new_empty_array(data_type)
                } else {
                    concat(&input)?
                };
                Ok(input)
            })
            .collect::<Result<Vec<_>>>()?;
        let array = self.aggregator.call(&inputs)?;
        if array.len() != 1 {
            return exec_err!("expected a single value, got {}", array.len());
        }
        let scalar = ScalarValue::try_from_array(&array, 0)?;
        Ok(scalar)
    }

    fn size(&self) -> usize {
        let mut size = size_of_val(self);
        size += size_of::<Vec<ArrayRef>>() * self.inputs.capacity();
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
        for (input, state) in self.inputs.iter_mut().zip(states.iter()) {
            let state = state.as_list::<i32>();
            for v in state.iter().flatten() {
                input.push(v);
            }
        }
        Ok(())
    }
}
