// SPDX-License-Identifier: Apache-2.0

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use chrono::NaiveDate;
use datafusion::arrow::array::{
    ArrayAccessor, ArrayRef, RecordBatch, StringArray, UInt32Array, downcast_dictionary_array,
};
use datafusion::arrow::compute::take_record_batch;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion_common::cast::{
    as_boolean_array, as_date32_array, as_date64_array, as_float16_array, as_float32_array,
    as_float64_array, as_int8_array, as_int16_array, as_int32_array, as_int64_array,
    as_large_string_array, as_string_array, as_string_view_array, as_uint8_array, as_uint16_array,
    as_uint32_array, as_uint64_array,
};
use datafusion_common::{Result, exec_datafusion_err, not_impl_err, plan_err};
use object_store::path::Path;

use crate::formats::parquet::writer::task::{DirectoryWriter, FileFactory};
use crate::listing::commit::WrittenFile;

/// The values of the partition columns, formatted for the directory names.
type PartitionKey = Vec<String>;

/// Splits batches by the values of the partition columns.
pub(super) struct PartitionRouter {
    /// The partition columns. The data types are not used,
    /// since the types are taken from the batches.
    columns: Vec<(String, DataType)>,
    /// The indices of the partition columns in the input schema.
    column_indices: Vec<usize>,
    /// The indices of the columns that are written to the files.
    file_indices: Vec<usize>,
    file_schema: SchemaRef,
    converter: Option<RowConverter>,
}

impl PartitionRouter {
    pub fn try_new(input_schema: &SchemaRef, partition_columns: &[String]) -> Result<Self> {
        let column_indices = partition_columns
            .iter()
            .map(|name| {
                input_schema.index_of(name).or_else(|_| {
                    plan_err!("partition column {name} does not exist in schema {input_schema}")
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let file_indices = input_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| !partition_columns.contains(field.name()))
            .map(|(i, _)| i)
            .collect::<Vec<_>>();
        let file_schema = Arc::new(input_schema.project(&file_indices)?);
        Ok(Self {
            columns: partition_columns
                .iter()
                .map(|name| (name.clone(), DataType::Null))
                .collect(),
            column_indices,
            file_indices,
            file_schema,
            converter: None,
        })
    }

    /// The schema of the files, which is the input schema without the partition columns.
    pub fn file_schema(&self) -> &SchemaRef {
        &self.file_schema
    }

    /// Splits a non-empty batch into one batch per distinct value of the partition columns.
    /// The returned batches do not have the partition columns.
    pub fn split(&mut self, batch: &RecordBatch) -> Result<Vec<(PartitionKey, RecordBatch)>> {
        // Format the first row before anything else, so that an unsupported
        // partition column type is reported by the formatting function.
        let first_key = self.key(batch, 0)?;
        let columns: Vec<ArrayRef> = self
            .column_indices
            .iter()
            .map(|i| Arc::clone(batch.column(*i)))
            .collect();
        let converter = match &mut self.converter {
            Some(converter) => converter,
            converter @ None => {
                let fields = columns
                    .iter()
                    .map(|column| SortField::new(column.data_type().clone()))
                    .collect();
                converter.insert(RowConverter::new(fields)?)
            }
        };
        let rows = converter.convert_columns(&columns)?;
        let data = batch.project(&self.file_indices)?;
        let first = rows.row(0);
        if rows.iter().all(|row| row == first) {
            return Ok(vec![(first_key, data)]);
        }
        let mut indices: Vec<Vec<u32>> = vec![];
        let mut groups = HashMap::new();
        for (i, row) in rows.iter().enumerate() {
            let group = *groups.entry(row).or_insert_with(|| {
                indices.push(vec![]);
                indices.len() - 1
            });
            indices[group].push(i as u32);
        }
        indices
            .into_iter()
            .map(|indices| {
                let key = self.key(batch, indices[0] as usize)?;
                let data = take_record_batch(&data, &UInt32Array::from(indices))?;
                Ok((key, data))
            })
            .collect()
    }

    fn key(&self, batch: &RecordBatch, row: usize) -> Result<PartitionKey> {
        let row = batch.slice(row, 1);
        let values = compute_partition_keys_by_row(&row, &self.columns)?;
        Ok(values.iter().map(|x| x[0].to_string()).collect())
    }

    /// Returns the directory of the files for the partition key.
    fn directory(&self, table_path: &Path, key: &PartitionKey) -> Path {
        let mut path = table_path.clone();
        for ((column, _), value) in self.columns.iter().zip(key) {
            path = path.join(format!("{column}={value}"));
        }
        path
    }
}

// SPDX-SnippetBegin
// SPDX-License-Identifier: Apache-2.0
// SPDX-SnippetComment: Credit: https://github.com/apache/datafusion/blob/500744d5c25499055e3b2ca46b022bc425989203/datafusion/datasource/src/write/demux.rs#L373-L541
#[expect(clippy::unwrap_used)]
fn compute_partition_keys_by_row<'a>(
    rb: &'a RecordBatch,
    partition_by: &'a [(String, DataType)],
) -> Result<Vec<Vec<Cow<'a, str>>>> {
    let mut all_partition_values = vec![];

    const EPOCH_DAYS_FROM_CE: i32 = 719_163;

    // For the purposes of writing partitioned data, we can rely on schema inference
    // to determine the type of the partition cols in order to provide a more ergonomic
    // UI which does not require specifying DataTypes manually. So, we ignore the
    // DataType within the partition_by array and infer the correct type from the
    // batch schema instead.
    let schema = rb.schema();
    for (col, _) in partition_by.iter() {
        let mut partition_values = vec![];

        let dtype = schema.field_with_name(col)?.data_type();
        let col_array = rb.column_by_name(col).ok_or(exec_datafusion_err!(
            "PartitionBy Column {} does not exist in source data! Got schema {schema}.",
            col
        ))?;

        match dtype {
            DataType::Utf8 => {
                let array = as_string_array(col_array)?;
                for i in 0..rb.num_rows() {
                    partition_values.push(Cow::from(array.value(i)));
                }
            }
            DataType::LargeUtf8 => {
                let array = as_large_string_array(col_array)?;
                for i in 0..rb.num_rows() {
                    partition_values.push(Cow::from(array.value(i)));
                }
            }
            DataType::Utf8View => {
                let array = as_string_view_array(col_array)?;
                for i in 0..rb.num_rows() {
                    partition_values.push(Cow::from(array.value(i)));
                }
            }
            DataType::Boolean => {
                let array = as_boolean_array(col_array)?;
                for i in 0..rb.num_rows() {
                    partition_values.push(Cow::from(array.value(i).to_string()));
                }
            }
            DataType::Date32 => {
                let array = as_date32_array(col_array)?;
                // ISO-8601/RFC3339 format - yyyy-mm-dd
                let format = "%Y-%m-%d";
                for i in 0..rb.num_rows() {
                    let date =
                        NaiveDate::from_num_days_from_ce_opt(EPOCH_DAYS_FROM_CE + array.value(i))
                            .unwrap()
                            .format(format)
                            .to_string();
                    partition_values.push(Cow::from(date));
                }
            }
            DataType::Date64 => {
                let array = as_date64_array(col_array)?;
                // ISO-8601/RFC3339 format - yyyy-mm-dd
                let format = "%Y-%m-%d";
                for i in 0..rb.num_rows() {
                    let date = NaiveDate::from_num_days_from_ce_opt(
                        EPOCH_DAYS_FROM_CE + (array.value(i) / 86_400_000) as i32,
                    )
                    .unwrap()
                    .format(format)
                    .to_string();
                    partition_values.push(Cow::from(date));
                }
            }
            DataType::Int8 => {
                let array = as_int8_array(col_array)?;
                for i in 0..rb.num_rows() {
                    partition_values.push(Cow::from(array.value(i).to_string()));
                }
            }
            DataType::Int16 => {
                let array = as_int16_array(col_array)?;
                for i in 0..rb.num_rows() {
                    partition_values.push(Cow::from(array.value(i).to_string()));
                }
            }
            DataType::Int32 => {
                let array = as_int32_array(col_array)?;
                for i in 0..rb.num_rows() {
                    partition_values.push(Cow::from(array.value(i).to_string()));
                }
            }
            DataType::Int64 => {
                let array = as_int64_array(col_array)?;
                for i in 0..rb.num_rows() {
                    partition_values.push(Cow::from(array.value(i).to_string()));
                }
            }
            DataType::UInt8 => {
                let array = as_uint8_array(col_array)?;
                for i in 0..rb.num_rows() {
                    partition_values.push(Cow::from(array.value(i).to_string()));
                }
            }
            DataType::UInt16 => {
                let array = as_uint16_array(col_array)?;
                for i in 0..rb.num_rows() {
                    partition_values.push(Cow::from(array.value(i).to_string()));
                }
            }
            DataType::UInt32 => {
                let array = as_uint32_array(col_array)?;
                for i in 0..rb.num_rows() {
                    partition_values.push(Cow::from(array.value(i).to_string()));
                }
            }
            DataType::UInt64 => {
                let array = as_uint64_array(col_array)?;
                for i in 0..rb.num_rows() {
                    partition_values.push(Cow::from(array.value(i).to_string()));
                }
            }
            DataType::Float16 => {
                let array = as_float16_array(col_array)?;
                for i in 0..rb.num_rows() {
                    partition_values.push(Cow::from(array.value(i).to_string()));
                }
            }
            DataType::Float32 => {
                let array = as_float32_array(col_array)?;
                for i in 0..rb.num_rows() {
                    partition_values.push(Cow::from(array.value(i).to_string()));
                }
            }
            DataType::Float64 => {
                let array = as_float64_array(col_array)?;
                for i in 0..rb.num_rows() {
                    partition_values.push(Cow::from(array.value(i).to_string()));
                }
            }
            DataType::Dictionary(_, _) => {
                downcast_dictionary_array!(
                    col_array =>  {
                        let array = col_array.downcast_dict::<StringArray>()
                            .ok_or(exec_datafusion_err!("it is not yet supported to write to hive partitions with datatype {}",
                            dtype))?;

                        for i in 0..rb.num_rows() {
                            partition_values.push(Cow::from(array.value(i)));
                        }
                    },
                    _ => unreachable!(),
                )
            }
            _ => {
                return not_impl_err!(
                    "it is not yet supported to write to hive partitions with datatype {}",
                    dtype
                );
            }
        }

        all_partition_values.push(partition_values);
    }

    Ok(all_partition_values)
}
// SPDX-SnippetEnd

struct OpenDirectory {
    writer: DirectoryWriter,
    /// The value of the clock when the directory was last written to.
    last_used: u64,
}

/// Writes the files of a writer task into one directory per partition key.
///
/// The number of directories with an open file is limited.
/// When the limit is reached, the file that has not been written to for the
/// longest time is completed. More rows for its partition key go to a new file.
pub(super) struct PartitionedWriter {
    router: PartitionRouter,
    table_path: Path,
    max_open_files: usize,
    open: HashMap<PartitionKey, OpenDirectory>,
    /// The directories whose file was completed to stay within the limit.
    closed: HashMap<PartitionKey, DirectoryWriter>,
    clock: u64,
}

impl PartitionedWriter {
    pub fn new(router: PartitionRouter, table_path: Path, max_open_files: usize) -> Self {
        Self {
            router,
            table_path,
            max_open_files,
            open: HashMap::new(),
            closed: HashMap::new(),
            clock: 0,
        }
    }

    pub async fn write(
        &mut self,
        factory: &FileFactory,
        batch: &RecordBatch,
        written: &mut Vec<WrittenFile>,
    ) -> Result<()> {
        for (key, data) in self.router.split(batch)? {
            self.clock += 1;
            if !self.open.contains_key(&key) {
                if self.open.len() >= self.max_open_files {
                    self.close_least_recently_used(written).await?;
                }
                let writer = match self.closed.remove(&key) {
                    Some(writer) => writer,
                    None => DirectoryWriter::new(self.router.directory(&self.table_path, &key)),
                };
                self.open.insert(
                    key.clone(),
                    OpenDirectory {
                        writer,
                        last_used: self.clock,
                    },
                );
            }
            let directory = self
                .open
                .get_mut(&key)
                .ok_or_else(|| exec_datafusion_err!("missing writer for partition {key:?}"))?;
            directory.last_used = self.clock;
            directory.writer.write(factory, &data, written).await?;
        }
        Ok(())
    }

    async fn close_least_recently_used(&mut self, written: &mut Vec<WrittenFile>) -> Result<()> {
        let key = self
            .open
            .iter()
            .min_by_key(|(_, directory)| directory.last_used)
            .map(|(key, _)| key.clone());
        if let Some((key, mut directory)) = key.and_then(|key| self.open.remove_entry(&key)) {
            directory.writer.close(written).await?;
            self.closed.insert(key, directory.writer);
        }
        Ok(())
    }

    pub fn memory_size(&self) -> usize {
        self.open
            .values()
            .map(|directory| directory.writer.memory_size())
            .sum()
    }

    pub async fn close(&mut self, written: &mut Vec<WrittenFile>) -> Result<()> {
        for (_, mut directory) in self.open.drain() {
            directory.writer.close(written).await?;
        }
        Ok(())
    }
}
