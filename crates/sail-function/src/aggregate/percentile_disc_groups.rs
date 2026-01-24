use std::fmt::{Debug, Formatter};
use std::mem::{size_of, size_of_val};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, ArrowNumericType, AsArray, BooleanArray, ListArray, PrimitiveArray,
    PrimitiveBuilder,
};
use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{DataFusionError, HashSet, Result, ScalarValue};
use datafusion::logical_expr::{Accumulator, EmitTo, GroupsAccumulator};
use datafusion::physical_expr::aggregate::utils::Hashable;

use crate::aggregate::utils::{calculate_percentile_disc, cast_to_type, filtered_null_mask};

#[derive(Debug)]
pub struct PercentileDiscGroupsAccumulator<T: ArrowNumericType + Send> {
    data_type: DataType,
    group_values: Vec<Vec<T::Native>>,
    percentile: f64,
}

impl<T: ArrowNumericType + Send> PercentileDiscGroupsAccumulator<T> {
    pub fn new(data_type: DataType, percentile: f64) -> Self {
        Self {
            data_type,
            group_values: Vec::new(),
            percentile,
        }
    }

    /// Accumulates values with nulls using chunked bitmap iteration.
    ///
    /// This processes data in 64-element chunks, checking the null bitmap via
    /// bitwise operations rather than per-element validity calls. The pattern
    /// is borrowed from DataFusion's `percentile_cont` implementation.
    fn accumulate_with_nulls_chunked(
        &mut self,
        group_indices: &[usize],
        values: &PrimitiveArray<T>,
    ) {
        let data = values.values();
        // SAFETY: his function represents an infallible situation.
        // When the null count is greater than zero, the null buffer must exist.
        #[allow(clippy::expect_used)]
        let nulls = values
            .nulls()
            .expect("null buffer must exist when null_count > 0");

        let group_indices_chunks = group_indices.chunks_exact(64);
        let data_chunks = data.chunks_exact(64);
        let bit_chunks = nulls.inner().bit_chunks();

        let group_indices_remainder = group_indices_chunks.remainder();
        let data_remainder = data_chunks.remainder();

        group_indices_chunks
            .zip(data_chunks)
            .zip(bit_chunks.iter())
            .for_each(|((group_index_chunk, data_chunk), mask)| {
                let mut index_mask = 1;
                group_index_chunk.iter().zip(data_chunk.iter()).for_each(
                    |(&group_index, &new_value)| {
                        let is_valid = (mask & index_mask) != 0;
                        if is_valid {
                            self.group_values[group_index].push(new_value);
                        }
                        index_mask <<= 1;
                    },
                )
            });

        let remainder_bits = bit_chunks.remainder_bits();
        group_indices_remainder
            .iter()
            .zip(data_remainder.iter())
            .enumerate()
            .for_each(|(i, (&group_index, &new_value))| {
                let is_valid = remainder_bits & (1 << i) != 0;
                if is_valid {
                    self.group_values[group_index].push(new_value);
                }
            });
    }
}

impl<T: ArrowNumericType + Send> GroupsAccumulator for PercentileDiscGroupsAccumulator<T> {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let values_array = cast_to_type(&values[0], &self.data_type)?;
        let values = values_array.as_primitive::<T>();

        self.group_values.resize(total_num_groups, Vec::new());

        let data: &[T::Native] = values.values();
        assert_eq!(data.len(), group_indices.len());

        match (values.null_count() > 0, opt_filter) {
            (false, None) => {
                for (&group_index, &new_value) in group_indices.iter().zip(data.iter()) {
                    self.group_values[group_index].push(new_value);
                }
            }
            (true, None) => {
                self.accumulate_with_nulls_chunked(group_indices, values);
            }
            (false, Some(filter)) => {
                assert_eq!(filter.len(), group_indices.len());
                group_indices
                    .iter()
                    .zip(data.iter())
                    .zip(filter.iter())
                    .for_each(|((&group_index, &new_value), filter_value)| {
                        if let Some(true) = filter_value {
                            self.group_values[group_index].push(new_value);
                        }
                    })
            }
            (true, Some(filter)) => {
                assert_eq!(filter.len(), group_indices.len());
                filter
                    .iter()
                    .zip(group_indices.iter())
                    .zip(values.iter())
                    .for_each(|((filter_value, &group_index), new_value)| {
                        if let Some(true) = filter_value {
                            if let Some(new_value) = new_value {
                                self.group_values[group_index].push(new_value)
                            }
                        }
                    })
            }
        }

        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        _opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let input_group_values = values[0].as_list::<i32>();

        self.group_values.resize(total_num_groups, Vec::new());

        group_indices
            .iter()
            .zip(input_group_values.iter())
            .for_each(|(&group_index, values_opt)| {
                if let Some(values) = values_opt {
                    let values = values.as_primitive::<T>();
                    self.group_values[group_index].extend(values.values().iter());
                }
            });

        Ok(())
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let emit_group_values = emit_to.take_needed(&mut self.group_values);

        let mut offsets = Vec::with_capacity(self.group_values.len() + 1);
        offsets.push(0);
        let mut cur_len = 0_i32;
        for group_value in &emit_group_values {
            cur_len += group_value.len() as i32;
            offsets.push(cur_len);
        }
        let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));

        let flatten_group_values = emit_group_values.into_iter().flatten().collect::<Vec<_>>();
        let group_values_array =
            PrimitiveArray::<T>::new(ScalarBuffer::from(flatten_group_values), None)
                .with_data_type(self.data_type.clone());

        let result_list_array = ListArray::new(
            Arc::new(Field::new_list_field(self.data_type.clone(), true)),
            offsets,
            Arc::new(group_values_array),
            None,
        );

        Ok(vec![Arc::new(result_list_array)])
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let emit_group_values = emit_to.take_needed(&mut self.group_values);

        let mut evaluate_result_builder =
            PrimitiveBuilder::<T>::new().with_data_type(self.data_type.clone());
        for values in emit_group_values {
            let value = calculate_percentile_disc::<T>(values, self.percentile);
            evaluate_result_builder.append_option(value);
        }

        Ok(Arc::new(evaluate_result_builder.finish()))
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let values_array = cast_to_type(&values[0], &self.data_type)?;
        let input_array = values_array.as_primitive::<T>();

        let values = PrimitiveArray::<T>::new(input_array.values().clone(), None)
            .with_data_type(self.data_type.clone());

        let offset_end = i32::try_from(input_array.len()).map_err(|e| {
            DataFusionError::Internal(format!(
                "cast array_len to i32 failed in convert_to_state of group percentile_disc, err:{e:?}"
            ))
        })?;
        let offsets = (0..=offset_end).collect::<Vec<_>>();
        let offsets = unsafe { OffsetBuffer::new_unchecked(ScalarBuffer::from(offsets)) };

        let nulls = filtered_null_mask(opt_filter, input_array);

        let converted_list_array = ListArray::new(
            Arc::new(Field::new_list_field(self.data_type.clone(), true)),
            offsets,
            Arc::new(values),
            nulls,
        );

        Ok(vec![Arc::new(converted_list_array)])
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + self
                .group_values
                .iter()
                .map(|values| values.capacity() * size_of::<T::Native>())
                .sum::<usize>()
            + self.group_values.capacity() * size_of::<Vec<T::Native>>()
    }
}

pub struct DistinctPercentileDiscAccumulator<T: ArrowNumericType> {
    pub data_type: DataType,
    pub distinct_values: HashSet<Hashable<T::Native>>,
    pub percentile: f64,
}

impl<T: ArrowNumericType> Debug for DistinctPercentileDiscAccumulator<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DistinctPercentileDiscAccumulator({}, percentile={})",
            self.data_type, self.percentile
        )
    }
}

impl<T: ArrowNumericType> Accumulator for DistinctPercentileDiscAccumulator<T> {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let all_values = self
            .distinct_values
            .iter()
            .map(|x| ScalarValue::new_primitive::<T>(Some(x.0), &self.data_type))
            .collect::<Result<Vec<_>>>()?;

        let arr = ScalarValue::new_list_nullable(&all_values, &self.data_type);
        Ok(vec![ScalarValue::List(arr)])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let values = cast_to_type(&values[0], &self.data_type)?;
        let array = values.as_primitive::<T>();
        match array.nulls().filter(|x| x.null_count() > 0) {
            Some(n) => {
                for idx in n.valid_indices() {
                    self.distinct_values.insert(Hashable(array.value(idx)));
                }
            }
            None => array.values().iter().for_each(|x| {
                self.distinct_values.insert(Hashable(*x));
            }),
        }
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
        let d = std::mem::take(&mut self.distinct_values)
            .into_iter()
            .map(|v| v.0)
            .collect::<Vec<_>>();
        let value = calculate_percentile_disc::<T>(d, self.percentile);
        ScalarValue::new_primitive::<T>(value, &self.data_type)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.distinct_values.capacity() * size_of::<T::Native>()
    }
}
