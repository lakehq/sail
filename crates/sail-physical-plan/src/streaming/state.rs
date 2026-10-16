use std::collections::HashMap;

use datafusion::arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::memory_pool::MemoryReservation;
use datafusion_common::{Result, ScalarValue, exec_datafusion_err};

pub(super) type Row = Vec<ScalarValue>;

pub(super) fn row_at(batch: &RecordBatch, index: usize) -> Result<Row> {
    batch
        .columns()
        .iter()
        .map(|array| ScalarValue::try_from_array(array, index))
        .collect()
}

pub(super) fn rows_to_batch(schema: SchemaRef, rows: &[Row]) -> Result<RecordBatch> {
    let columns = (0..schema.fields().len())
        .map(|i| {
            if rows.is_empty() {
                Ok(datafusion::arrow::array::new_empty_array(
                    schema.field(i).data_type(),
                ))
            } else {
                ScalarValue::iter_to_array(rows.iter().map(|row| row[i].clone()))
            }
        })
        .collect::<Result<Vec<ArrayRef>>>()?;
    Ok(RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(rows.len())),
    )?)
}

/// A bag of live rows. Retractions must match an existing occurrence.
#[derive(Default)]
pub(super) struct RowMultiset {
    rows: HashMap<Row, usize>,
}

impl RowMultiset {
    pub fn apply(
        &mut self,
        row: Row,
        retracted: bool,
        memory: &mut MemoryReservation,
    ) -> Result<()> {
        if retracted {
            let count = self.rows.get_mut(&row).ok_or_else(|| {
                exec_datafusion_err!("stream retracted a row that is not present in its state")
            })?;
            *count -= 1;
            if *count == 0 {
                self.rows.remove(&row);
                if self.rows.is_empty() || self.rows.len() < self.rows.capacity() / 4 {
                    self.rows.shrink_to_fit();
                }
                memory.shrink(Self::row_size(&row));
            }
        } else if let Some(count) = self.rows.get_mut(&row) {
            *count = count
                .checked_add(1)
                .ok_or_else(|| exec_datafusion_err!("stream row count overflow"))?;
        } else {
            memory.try_grow(Self::row_size(&row))?;
            self.rows.insert(row, 1);
        }
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn rows(&self) -> impl Iterator<Item = &Row> {
        self.rows
            .iter()
            .flat_map(|(row, count)| std::iter::repeat_n(row, *count))
    }

    pub fn into_rows(self) -> impl Iterator<Item = Row> {
        self.rows
            .into_iter()
            .flat_map(|(row, count)| std::iter::repeat_n(row, count))
    }

    pub fn row_size(row: &Row) -> usize {
        // Include the hash table slot, multiplicity, and allocation overhead.
        ScalarValue::size_of_vec(row) + 256
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};

    use super::*;

    #[test]
    fn retractions_preserve_multiplicity_and_release_memory() -> Result<()> {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(4096));
        let mut memory = MemoryConsumer::new("test").register(&pool);
        let mut rows = RowMultiset::default();
        let row = vec![
            ScalarValue::Utf8(Some("same".into())),
            ScalarValue::Int64(None),
        ];
        rows.apply(row.clone(), false, &mut memory)?;
        rows.apply(row.clone(), false, &mut memory)?;
        rows.apply(row.clone(), true, &mut memory)?;
        assert_eq!(rows.rows().count(), 1);
        assert!(memory.size() > 0);
        rows.apply(row.clone(), true, &mut memory)?;
        assert!(rows.is_empty());
        assert_eq!(memory.size(), 0);
        assert!(matches!(
            rows.apply(row, true, &mut memory),
            Err(datafusion_common::DataFusionError::Execution(_))
        ));
        Ok(())
    }

    #[test]
    fn live_rows_respect_the_memory_pool() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1));
        let mut memory = MemoryConsumer::new("test").register(&pool);
        let mut rows = RowMultiset::default();
        assert!(
            rows.apply(vec![ScalarValue::Int64(Some(1))], false, &mut memory)
                .is_err()
        );
        assert!(rows.is_empty());
        assert_eq!(memory.size(), 0);
    }
}
