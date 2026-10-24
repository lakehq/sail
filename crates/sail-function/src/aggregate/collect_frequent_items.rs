use std::fmt::Debug;
use std::mem::size_of_val;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::cast::{as_int64_array, as_list_array};
use datafusion::common::{Result, ScalarValue, exec_err};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use indexmap::IndexMap;

use super::utils::get_scalar_value;

/// The frequent items of a column, as Spark's `CollectFrequentItems` computes them for
/// `DataFrame.stat.freqItems`. The second argument is the number of items to keep counts for.
///
/// The algorithm is the frequent element count of Karp, Schenker and Papadimitriou.
/// Every item that occurs in more than `1 / size` of the rows is in the result, which can
/// also contain other items. Null values are counted as an item, as in Spark.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CollectFrequentItems {
    signature: Signature,
}

impl Default for CollectFrequentItems {
    fn default() -> Self {
        Self::new()
    }
}

impl CollectFrequentItems {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for CollectFrequentItems {
    fn name(&self) -> &str {
        "collect_frequent_items"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new_list_field(
            arg_types[0].clone(),
            true,
        ))))
    }

    fn is_nullable(&self) -> bool {
        false
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let [_, size] = args.exprs else {
            return exec_err!("collect_frequent_items expects 2 arguments");
        };
        let size = match get_scalar_value(size)? {
            ScalarValue::Int32(Some(size)) if size > 0 => size as usize,
            ScalarValue::Int64(Some(size)) if size > 0 => size as usize,
            other => {
                return exec_err!(
                    "collect_frequent_items expects a positive integer size, got {other}"
                );
            }
        };
        Ok(Box::new(FrequentItemsAccumulator {
            data_type: args.expr_fields[0].data_type().clone(),
            size,
            counts: IndexMap::new(),
        }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let item_type = args.input_fields[0].data_type().clone();
        Ok(vec![
            Field::new(
                "items",
                DataType::List(Arc::new(Field::new_list_field(item_type, true))),
                true,
            )
            .into(),
            Field::new(
                "counts",
                DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true))),
                true,
            )
            .into(),
        ])
    }
}

#[derive(Debug)]
struct FrequentItemsAccumulator {
    data_type: DataType,
    size: usize,
    counts: IndexMap<ScalarValue, i64>,
}

impl FrequentItemsAccumulator {
    /// Port of `CollectFrequentItems.add` in Spark.
    fn add(&mut self, item: ScalarValue, count: i64) {
        if let Some(value) = self.counts.get_mut(&item) {
            *value += count;
        } else if self.counts.len() < self.size {
            self.counts.insert(item, count);
        } else {
            let min_count = self.counts.values().copied().min().unwrap_or(0);
            if count >= min_count {
                // Something will get kicked out, so the item can be added.
                self.counts.insert(item, count);
                self.counts.retain(|_, value| *value > min_count);
                self.counts
                    .values_mut()
                    .for_each(|value| *value -= min_count);
            } else {
                self.counts.values_mut().for_each(|value| *value -= count);
            }
        }
    }
}

impl Accumulator for FrequentItemsAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = &values[0];
        for i in 0..array.len() {
            self.add(ScalarValue::try_from_array(array, i)?, 1);
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let items = as_list_array(&states[0])?;
        let counts = as_list_array(&states[1])?;
        for row in 0..items.len() {
            if items.is_null(row) || counts.is_null(row) {
                continue;
            }
            let row_items = items.value(row);
            let row_counts = counts.value(row);
            let row_counts = as_int64_array(&row_counts)?;
            for i in 0..row_items.len() {
                self.add(
                    ScalarValue::try_from_array(&row_items, i)?,
                    row_counts.value(i),
                );
            }
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let items = self.counts.keys().cloned().collect::<Vec<_>>();
        let counts = self
            .counts
            .values()
            .map(|count| ScalarValue::Int64(Some(*count)))
            .collect::<Vec<_>>();
        Ok(vec![
            ScalarValue::List(ScalarValue::new_list_nullable(&items, &self.data_type)),
            ScalarValue::List(ScalarValue::new_list_nullable(&counts, &DataType::Int64)),
        ])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let items = self.counts.keys().cloned().collect::<Vec<_>>();
        Ok(ScalarValue::List(ScalarValue::new_list_nullable(
            &items,
            &self.data_type,
        )))
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + self
                .counts
                .keys()
                .map(|item| item.size() + size_of_val(&0i64))
                .sum::<usize>()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Int64Array;

    use super::*;

    fn accumulator(size: usize) -> FrequentItemsAccumulator {
        FrequentItemsAccumulator {
            data_type: DataType::Int64,
            size,
            counts: IndexMap::new(),
        }
    }

    fn items(acc: &mut FrequentItemsAccumulator) -> Result<Vec<Option<i64>>> {
        let ScalarValue::List(list) = acc.evaluate()? else {
            return exec_err!("expected a list");
        };
        Ok(as_int64_array(list.values())?.iter().collect())
    }

    #[test]
    fn frequent_items_are_kept_and_rare_items_evicted() -> Result<()> {
        // When the map is full, 3 evicts 2, and 3 comes back after 1 is counted again.
        let mut acc = accumulator(2);
        let values: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 1, 3, 1, 3]));
        acc.update_batch(&[values])?;
        assert_eq!(items(&mut acc)?, vec![Some(1), Some(3)]);
        Ok(())
    }

    #[test]
    fn merging_states_combines_counts() -> Result<()> {
        let mut left = accumulator(2);
        left.update_batch(&[Arc::new(Int64Array::from(vec![Some(7), None, Some(7)]))])?;
        let mut right = accumulator(2);
        right.update_batch(&[Arc::new(Int64Array::from(vec![Some(7), Some(8)]))])?;
        let state = left
            .state()?
            .into_iter()
            .map(|value| value.to_array())
            .collect::<Result<Vec<_>>>()?;
        right.merge_batch(&state)?;
        // The null item is counted, but it is evicted together with 8 when merged.
        assert_eq!(items(&mut right)?, vec![Some(7)]);
        Ok(())
    }
}
