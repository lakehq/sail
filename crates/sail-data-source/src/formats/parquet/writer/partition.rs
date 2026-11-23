// SPDX-License-Identifier: Apache-2.0

use std::borrow::Cow;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};

use chrono::NaiveDate;
use datafusion::arrow::array::{
    Array, ArrayAccessor, ArrayRef, AsArray, FixedSizeListArray, GenericListArray,
    GenericListViewArray, MapArray, OffsetSizeTrait, RecordBatch, RecordBatchOptions, Scalar,
    StringArray, StructArray, UInt32Array, downcast_dictionary_array,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use datafusion::arrow::compute::kernels::cmp::not_distinct;
use datafusion::arrow::compute::kernels::zip::zip;
use datafusion::arrow::compute::{can_cast_types, cast, is_not_null, take_record_batch};
use datafusion::arrow::datatypes::{ArrowNativeType, DataType, Field, Schema, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::common::runtime::SpawnedTask;
use datafusion::execution::memory_pool::{MemoryPool, MemoryReservation};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::metrics::Count;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{EmptyRecordBatchStream, ExecutionPlan};
use datafusion_common::cast::{
    as_boolean_array, as_date32_array, as_date64_array, as_float16_array, as_float32_array,
    as_float64_array, as_int8_array, as_int16_array, as_int32_array, as_int64_array,
    as_large_string_array, as_string_array, as_string_view_array, as_uint8_array, as_uint16_array,
    as_uint32_array, as_uint64_array,
};
use datafusion_common::{
    DataFusionError, Result, ScalarValue, exec_datafusion_err, exec_err, internal_datafusion_err,
    internal_err, not_impl_err, plan_err,
};
use futures::StreamExt;
use object_store::path::Path;
use sail_common_datafusion::data_type::{contains_dictionary, without_dictionaries};
use tokio::sync::mpsc::{Receiver, Sender, channel};

use crate::formats::parquet::writer::task::{
    ClosingFile, DirectoryWriter, FileFactory, reserve_or_flush,
};
use crate::listing::partition::DEFAULT_PARTITION_NAME;
use crate::listing::staging::StagedFile;

/// The values of the partition columns, formatted for the directory names.
type PartitionKey = Vec<String>;

/// The rows of a batch that have the same values of the partition columns.
pub(super) struct KeyRows {
    key: PartitionKey,
    /// The indices of the rows, or nothing when these are all rows of the batch.
    rows: Option<UInt32Array>,
}

impl KeyRows {
    fn take(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        match &self.rows {
            Some(rows) => Ok(take_record_batch(batch, rows)?),
            None => Ok(batch.clone()),
        }
    }
}

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

    /// Returns the rows of a non-empty batch for each distinct value of the partition columns.
    ///
    /// The rows without a value of a partition column are returned separately from
    /// the rows with the empty string, and both have the key of the default partition.
    pub fn split(&mut self, batch: &RecordBatch) -> Result<Vec<KeyRows>> {
        let keys = batch.project(&self.column_indices)?;
        if self.converter.is_none() {
            // The formatting function reports a partition column type that it
            // does not support, and it does so for a batch without rows. It is
            // asked before anything else is done with the columns of the first
            // batch, so that the error does not depend on the values of the rows.
            compute_partition_keys_by_row(&keys.slice(0, 0), &self.columns)?;
        }
        let values = KeyValues::try_new(&keys)?;
        if has_one_key(&keys) {
            return Ok(vec![KeyRows {
                key: self.key(&values, 0)?,
                rows: None,
            }]);
        }
        let converter = match &mut self.converter {
            Some(converter) => converter,
            converter @ None => {
                let fields = keys
                    .columns()
                    .iter()
                    .map(|column| SortField::new(column.data_type().clone()))
                    .collect();
                converter.insert(RowConverter::new(fields)?)
            }
        };
        let rows = converter.convert_columns(keys.columns())?;
        let first = rows.row(0);
        if rows.iter().all(|row| row == first) {
            return Ok(vec![KeyRows {
                key: self.key(&values, 0)?,
                rows: None,
            }]);
        }
        let mut indices: Vec<Vec<u32>> = vec![];
        let mut groups = HashMap::new();
        for (i, row) in rows.iter().enumerate() {
            let group = *groups.entry(row).or_insert_with(|| {
                indices.push(vec![]);
                indices.len() - 1
            });
            if let Some(indices) = indices.get_mut(group) {
                indices.push(i as u32);
            }
        }
        indices
            .into_iter()
            .map(|indices| {
                let row = indices.first().copied().unwrap_or_default();
                Ok(KeyRows {
                    key: self.key(&values, row as usize)?,
                    rows: Some(UInt32Array::from(indices)),
                })
            })
            .collect()
    }

    /// Returns the columns of the batch that are written to the files.
    fn file_columns(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        Ok(batch.project(&self.file_indices)?)
    }

    /// Returns the values of the partition columns at the row, formatted for
    /// the directory names. A row without a value and a row with the empty string
    /// are in the default partition, as they are for Spark.
    fn key(&self, values: &KeyValues, row: usize) -> Result<PartitionKey> {
        let present = values.present.slice(row, 1);
        let formatted = compute_partition_keys_by_row(&present, &self.columns)?;
        formatted
            .iter()
            .zip(&values.valid)
            .map(|(value, valid)| {
                let value = match value.first() {
                    Some(value) if valid.as_ref().is_none_or(|x| x.is_valid(row)) => value,
                    _ => return Ok(DEFAULT_PARTITION_NAME.to_string()),
                };
                match value.is_empty() {
                    true => Ok(DEFAULT_PARTITION_NAME.to_string()),
                    false => Ok(value.to_string()),
                }
            })
            .collect()
    }

    /// Returns whether the rows with the same values of the partition columns
    /// are next to each other in an input that has the ordering.
    /// This is the case when the ordering starts with the partition columns.
    fn is_clustered_by(&self, ordering: &LexOrdering) -> bool {
        let leading: Vec<_> = ordering
            .iter()
            .take(self.column_indices.len())
            .filter_map(|x| x.expr.downcast_ref::<Column>().map(|x| x.index()))
            .collect();
        self.column_indices.iter().all(|x| leading.contains(x))
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

/// Returns whether all rows of a non-empty batch are known to have the values
/// of the partition columns that the first row has.
///
/// This is the case for every batch of a task that writes one partition
/// directory, and it is found with a comparison of each column, which costs
/// less than the conversion of the rows that tells the keys of a batch apart.
/// A missing value is equal to a missing value only. The answer is no for
/// a column type that the comparison does not support.
fn has_one_key(keys: &RecordBatch) -> bool {
    keys.columns().iter().all(|column| {
        let first = Scalar::new(column.slice(0, 1));
        not_distinct(column, &first).is_ok_and(|same| same.true_count() == column.len())
    })
}

/// The partition columns of a batch, prepared for the formatting of their values.
struct KeyValues {
    /// The partition columns, with a value in every row.
    present: RecordBatch,
    /// The rows of each partition column that have a value,
    /// or nothing when all rows of the column have a value.
    valid: Vec<Option<NullBuffer>>,
}

impl KeyValues {
    fn try_new(keys: &RecordBatch) -> Result<Self> {
        let present = keys
            .columns()
            .iter()
            .map(fill_missing_values)
            .collect::<Result<Vec<ArrayRef>>>()?;
        Ok(Self {
            present: RecordBatch::try_new(keys.schema(), present)?,
            valid: keys.columns().iter().map(|x| x.logical_nulls()).collect(),
        })
    }
}

/// Returns the partition column with the default value of its type
/// in place of each missing value.
///
/// The formatting function reads the content of the array at every row,
/// and the content of the array at a row without a value is arbitrary.
fn fill_missing_values(column: &ArrayRef) -> Result<ArrayRef> {
    if column.logical_null_count() == 0 {
        return Ok(Arc::clone(column));
    }
    let values = match column.data_type() {
        DataType::Dictionary(_, value_type) => cast(column, value_type)?,
        _ => Arc::clone(column),
    };
    let default = ScalarValue::new_default(values.data_type())?.to_scalar()?;
    let values = zip(&is_not_null(&values)?, &values, &default)?;
    Ok(cast(&values, column.data_type())?)
}

// SPDX-SnippetBegin
// SPDX-License-Identifier: Apache-2.0
// SPDX-SnippetComment: Credit: https://github.com/apache/datafusion/blob/500744d5c25499055e3b2ca46b022bc425989203/datafusion/datasource/src/write/demux.rs#L373-L541
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
                            .ok_or_else(|| {
                                exec_datafusion_err!(
                                    "the date of a hive partition is out of range: {}",
                                    array.value(i)
                                )
                            })?
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
                    .ok_or_else(|| {
                        exec_datafusion_err!(
                            "the date of a hive partition is out of range: {}",
                            array.value(i)
                        )
                    })?
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
                    _ => {
                        return internal_err!(
                            "the hive partition column with datatype {} is not a dictionary array",
                            dtype
                        );
                    }
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

/// The capacity of the channels through which the rows of the overflow
/// are sent to the sort and received from it. The sort and the writer
/// take the batches as they come, so a channel only has to hold the batch
/// that is handed over.
const OVERFLOW_CHANNEL_CAPACITY: usize = 1;

/// The name of the column that identifies the partition key of a row
/// while the row is sorted. The column is added after the columns of the input
/// and is referred to by its index, so the name does not have to be unique.
const OVERFLOW_KEY_COLUMN: &str = "partition_key";

/// The input of the sort of the overflow, which is read once.
struct OverflowInput {
    schema: SchemaRef,
    stream: Mutex<Option<SendableRecordBatchStream>>,
}

impl fmt::Debug for OverflowInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OverflowInput").finish_non_exhaustive()
    }
}

impl PartitionStream for OverflowInput {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let stream = self
            .stream
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        match stream {
            Some(stream) => stream,
            None => Box::pin(EmptyRecordBatchStream::new(Arc::clone(&self.schema))),
        }
    }
}

/// The rows of the partition keys that have no open file because the task has
/// its number of open files. The rows are sorted by their partition key while
/// the task reads its input, and they are written when the input has ended.
///
/// The sort uses the memory pool and the temporary files of the session,
/// so it writes its rows to temporary files when the memory pool is limited.
///
/// The sort combines the batches that it receives. Batches with dictionaries
/// of their own cannot always be combined, since the dictionary of the combined
/// batch can have more values than the type of the keys can refer to. So the
/// rows are sorted with the values in place of each dictionary, and
/// [`with_dictionaries`] returns the rows to the data types of the input.
struct Overflow {
    /// The schema of the rows that are sorted, which is the schema of the input
    /// without dictionaries and with the column of the partition key.
    schema: SchemaRef,
    /// The input of the sort, until the sorted rows are read.
    sender: Option<Sender<Result<RecordBatch>>>,
    sorted: Receiver<Result<RecordBatch>>,
    /// The task that sorts the rows, until it has ended.
    /// The task is canceled when this is dropped.
    sort: Option<SpawnedTask<()>>,
    /// The number of each partition key in the order of their first rows.
    ids: HashMap<PartitionKey, u32>,
    keys: Vec<PartitionKey>,
    /// The number of rows that have been added and have not been read.
    rows: usize,
}

impl Overflow {
    fn try_new(
        input_schema: &SchemaRef,
        ordering: Option<&LexOrdering>,
        context: &Arc<TaskContext>,
    ) -> Result<Self> {
        // An ordering on an expression is evaluated for the data types of the input,
        // so the rows of such an input are sorted as they are.
        let plain = ordering.is_none_or(|ordering| {
            ordering
                .iter()
                .all(|x| x.expr.downcast_ref::<Column>().is_some())
        });
        let mut fields: Vec<_> = input_schema
            .fields()
            .iter()
            .map(|field| match without_dictionary_type(field.data_type()) {
                Some(data_type) if plain => {
                    Arc::new(field.as_ref().clone().with_data_type(data_type))
                }
                _ => Arc::clone(field),
            })
            .collect();
        fields.push(Arc::new(Field::new(
            OVERFLOW_KEY_COLUMN,
            DataType::UInt32,
            false,
        )));
        let key_index = fields.len() - 1;
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            input_schema.metadata().clone(),
        ));
        let builder =
            RecordBatchReceiverStream::builder(Arc::clone(&schema), OVERFLOW_CHANNEL_CAPACITY);
        let sender = builder.tx();
        let input = OverflowInput {
            schema: Arc::clone(&schema),
            stream: Mutex::new(Some(builder.build())),
        };
        let input = StreamingTableExec::try_new(
            Arc::clone(&schema),
            vec![Arc::new(input)],
            None,
            [],
            false,
            None,
        )?;
        // The rows of a partition key keep the order that the input is required to have.
        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new(OVERFLOW_KEY_COLUMN, key_index));
        let order = std::iter::once(PhysicalSortExpr::new_default(key))
            .chain(ordering.into_iter().flat_map(|x| x.iter().cloned()));
        let order = LexOrdering::new(order)
            .ok_or_else(|| internal_datafusion_err!("empty ordering for partition keys"))?;
        let mut stream = SortExec::new(order, Arc::new(input)).execute(0, Arc::clone(context))?;
        let (output, sorted) = channel(OVERFLOW_CHANNEL_CAPACITY);
        let sort = SpawnedTask::spawn(async move {
            while let Some(batch) = stream.next().await {
                if output.send(batch).await.is_err() {
                    break;
                }
            }
        });
        Ok(Self {
            schema,
            sender: Some(sender),
            sorted,
            sort: Some(sort),
            ids: HashMap::new(),
            keys: vec![],
            rows: 0,
        })
    }

    /// Adds rows of the input, which all have the partition key.
    async fn add(&mut self, key: PartitionKey, rows: RecordBatch) -> Result<()> {
        let id = match self.ids.get(&key) {
            Some(id) => *id,
            None => {
                let id = u32::try_from(self.keys.len())
                    .map_err(|_| exec_datafusion_err!("too many partition keys"))?;
                self.ids.insert(key.clone(), id);
                self.keys.push(key);
                id
            }
        };
        let mut columns = rows
            .columns()
            .iter()
            .zip(self.schema.fields())
            .map(
                |(column, field)| match column.data_type() == field.data_type() {
                    true => Ok(Arc::clone(column)),
                    false => cast(column, field.data_type()),
                },
            )
            .collect::<Result<Vec<_>, _>>()?;
        columns.push(Arc::new(UInt32Array::from(vec![id; rows.num_rows()])));
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), columns)?;
        let Some(sender) = &self.sender else {
            return internal_err!("rows are added to the sort after its rows have been read");
        };
        if sender.send(Ok(batch)).await.is_ok() {
            self.rows += rows.num_rows();
            return Ok(());
        }
        // The sort has ended, which it does before the end of its input only when it fails.
        while self.next().await?.is_some() {}
        exec_err!("the sort of the partitioned rows ended before the end of its input")
    }

    /// Returns the next sorted rows with the number of their partition key in
    /// the last column, or nothing when all rows that were added have been returned.
    /// No rows can be added once this has been called.
    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        // The sort returns its rows when its input has ended.
        self.sender = None;
        if let Some(batch) = self.sorted.recv().await {
            let batch = batch?;
            self.rows = self.rows.checked_sub(batch.num_rows()).ok_or_else(|| {
                internal_datafusion_err!("the sort of the partitioned rows returned other rows")
            })?;
            return Ok(Some(batch));
        }
        // The rows end without an error also when the task of the sort
        // has panicked or has been canceled, so the task is asked how it ended.
        if let Some(sort) = self.sort.take() {
            sort.join()
                .await
                .map_err(|e| DataFusionError::ExecutionJoin(Box::new(e)))?;
        }
        match self.rows {
            0 => Ok(None),
            rows => internal_err!("the sort of the partitioned rows did not return {rows} rows"),
        }
    }

    /// Returns the partition key that has the number.
    fn key(&self, id: u32) -> Result<&PartitionKey> {
        self.keys
            .get(id as usize)
            .ok_or_else(|| internal_datafusion_err!("unknown partition key in the sorted rows"))
    }
}

/// Returns the data type with the type of the values in place of each
/// dictionary at any depth, if the data type has a dictionary and its arrays
/// can be converted to that data type and back.
fn without_dictionary_type(data_type: &DataType) -> Option<DataType> {
    if !contains_dictionary(data_type) {
        return None;
    }
    let plain = without_dictionaries(data_type);
    (can_cast_types(data_type, &plain) && can_cast_types(&plain, data_type)).then_some(plain)
}

/// Returns the rows with the data types of the schema, which differ from
/// the data types of the rows in their dictionaries only.
///
/// The rows are returned in several batches when one dictionary cannot refer
/// to the values of all rows. One row always has a dictionary, since the row
/// had one in the input.
pub(super) fn with_dictionaries(rows: RecordBatch, schema: &SchemaRef) -> Result<Vec<RecordBatch>> {
    if rows.schema_ref().fields() == schema.fields() {
        return Ok(vec![rows]);
    }
    let mut batches = vec![];
    // The rows that are not converted, with the last rows first.
    let mut rest = vec![rows];
    while let Some(rows) = rest.pop() {
        let columns = rows
            .columns()
            .iter()
            .zip(schema.fields())
            .map(|(column, field)| with_dictionary(column, field.data_type()))
            .collect::<Result<Vec<_>, _>>();
        match columns {
            Ok(columns) => {
                let options = RecordBatchOptions::new().with_row_count(Some(rows.num_rows()));
                let schema = Arc::clone(schema);
                batches.push(RecordBatch::try_new_with_options(
                    schema, columns, &options,
                )?);
            }
            Err(ArrowError::DictionaryKeyOverflowError) if rows.num_rows() > 1 => {
                let half = rows.num_rows() / 2;
                rest.push(rows.slice(half, rows.num_rows() - half));
                rest.push(rows.slice(0, half));
            }
            Err(e) => return Err(e.into()),
        }
    }
    Ok(batches)
}

/// Returns the array with the data type, which differs from the data type
/// of the array in its dictionaries only.
///
/// A dictionary is created for the values that the rows of the array refer to,
/// and not for all values of an array that the array is a slice of. The values
/// of an array of runs are not divided, so all its values are in the dictionary.
fn with_dictionary(array: &ArrayRef, data_type: &DataType) -> Result<ArrayRef, ArrowError> {
    if array.data_type() == data_type {
        return Ok(Arc::clone(array));
    }
    match data_type {
        DataType::List(field) => list_with_dictionary::<i32>(array, field),
        DataType::LargeList(field) => list_with_dictionary::<i64>(array, field),
        DataType::ListView(field) => list_view_with_dictionary::<i32>(array, field),
        DataType::LargeListView(field) => list_view_with_dictionary::<i64>(array, field),
        DataType::FixedSizeList(field, size) => {
            let list = array.as_fixed_size_list();
            let values = with_dictionary(list.values(), field.data_type())?;
            let list = FixedSizeListArray::try_new_with_length(
                Arc::clone(field),
                *size,
                values,
                list.nulls().cloned(),
                list.len(),
            )?;
            Ok(Arc::new(list))
        }
        DataType::Map(field, sorted) => {
            let map = array.as_map();
            let (start, offsets) = rebased_offsets(map.offsets());
            let length = offsets.last().map_or(0, |x| x.as_usize());
            let entries: ArrayRef = Arc::new(map.entries().slice(start, length));
            let entries = with_dictionary(&entries, field.data_type())?;
            let map = MapArray::try_new(
                Arc::clone(field),
                offsets,
                entries.as_struct().clone(),
                map.nulls().cloned(),
                *sorted,
            )?;
            Ok(Arc::new(map))
        }
        DataType::Struct(fields) => {
            let values = array.as_struct();
            let columns = values
                .columns()
                .iter()
                .zip(fields)
                .map(|(column, field)| with_dictionary(column, field.data_type()))
                .collect::<Result<Vec<_>, _>>()?;
            let values = StructArray::try_new_with_length(
                fields.clone(),
                columns,
                values.nulls().cloned(),
                values.len(),
            )?;
            Ok(Arc::new(values))
        }
        _ => cast(array, data_type),
    }
}

/// Returns the offset of the first value of a list array in the array of its
/// values, and the offsets of the lists from that value on.
fn rebased_offsets<O: OffsetSizeTrait>(offsets: &OffsetBuffer<O>) -> (usize, OffsetBuffer<O>) {
    let start = offsets.first().map_or(0, |x| x.as_usize());
    (start, OffsetBuffer::from_lengths(offsets.lengths()))
}

fn list_with_dictionary<O: OffsetSizeTrait>(
    array: &ArrayRef,
    field: &Arc<Field>,
) -> Result<ArrayRef, ArrowError> {
    let list = array.as_list::<O>();
    let (start, offsets) = rebased_offsets(list.offsets());
    let length = offsets.last().map_or(0, |x| x.as_usize());
    let values = list.values().slice(start, length);
    let values = with_dictionary(&values, field.data_type())?;
    let list =
        GenericListArray::try_new(Arc::clone(field), offsets, values, list.nulls().cloned())?;
    Ok(Arc::new(list))
}

fn list_view_with_dictionary<O: OffsetSizeTrait>(
    array: &ArrayRef,
    field: &Arc<Field>,
) -> Result<ArrayRef, ArrowError> {
    let list = array.as_list_view::<O>();
    // The lists can be anywhere in the array of their values.
    let ranges = || list.offsets().iter().zip(list.sizes().iter());
    let start = ranges().map(|(x, _)| x.as_usize()).min().unwrap_or(0);
    let end = ranges()
        .map(|(x, size)| x.as_usize() + size.as_usize())
        .max()
        .unwrap_or(0);
    let values = list.values().slice(start, end - start);
    let values = with_dictionary(&values, field.data_type())?;
    let offsets: ScalarBuffer<O> = list
        .offsets()
        .iter()
        .map(|x| O::usize_as(x.as_usize() - start))
        .collect();
    let list = GenericListViewArray::try_new(
        Arc::clone(field),
        offsets,
        list.sizes().clone(),
        values,
        list.nulls().cloned(),
    )?;
    Ok(Arc::new(list))
}

/// Writes the files of a writer task into one directory per partition key.
///
/// The task writes one file at a time to each directory, and it writes to
/// a limited number of directories at the same time. The rows of the other
/// directories are sorted by their partition key, and they are written one
/// directory after the other when the input has ended. No file is completed
/// to make room for another one, so the number of files does not depend
/// on the order of the rows.
///
/// When the rows of each directory are next to each other in the input,
/// the task writes to one directory at a time and nothing is sorted.
pub(super) struct PartitionedWriter {
    router: PartitionRouter,
    table_path: Path,
    max_open_files: usize,
    /// Whether the rows of each partition key are next to each other in the input.
    clustered: bool,
    /// The ordering that the input is required to have.
    ordering: Option<LexOrdering>,
    context: Arc<TaskContext>,
    rows_sorted: Count,
    open: HashMap<PartitionKey, DirectoryWriter>,
    /// The number of files that the task has written for an input in which the rows
    /// of each partition key are next to each other. The files of such an input are
    /// numbered across the directories, so that the files of a directory have
    /// different names also when the directory has rows in several places of the
    /// input, as the default partition has for the missing value and the empty string.
    clustered_files: u32,
    /// The files that are being completed, in the order in which they received
    /// their last rows. The number of these files and of the directories that
    /// receive rows together is at most the number of directories that can be open.
    closing: VecDeque<ClosingFile>,
    overflow: Option<Overflow>,
}

impl PartitionedWriter {
    pub fn new(
        router: PartitionRouter,
        table_path: Path,
        max_open_files: usize,
        ordering: Option<LexOrdering>,
        context: Arc<TaskContext>,
        rows_sorted: Count,
    ) -> Self {
        let clustered = ordering
            .as_ref()
            .is_some_and(|ordering| router.is_clustered_by(ordering));
        Self {
            router,
            table_path,
            max_open_files,
            clustered,
            ordering,
            context,
            rows_sorted,
            open: HashMap::new(),
            clustered_files: 0,
            closing: VecDeque::new(),
            overflow: None,
        }
    }

    pub async fn write(
        &mut self,
        factory: &FileFactory,
        batch: &RecordBatch,
        written: &mut Vec<StagedFile>,
    ) -> Result<()> {
        let (parts, data) = {
            let _timer = factory.elapsed_compute().timer();
            (self.router.split(batch)?, self.router.file_columns(batch)?)
        };
        for part in parts {
            if !self.open.contains_key(&part.key) {
                if self.clustered {
                    // The rows of the open directory have ended,
                    // and the directory of these rows is opened next.
                    self.close_open(factory, written, 1).await?;
                } else if self.open.len() >= self.max_open_files {
                    let rows = part.take(batch)?;
                    self.rows_sorted.add(rows.num_rows());
                    let overflow = match &mut self.overflow {
                        Some(overflow) => overflow,
                        overflow @ None => overflow.insert(Overflow::try_new(
                            &batch.schema(),
                            self.ordering.as_ref(),
                            &self.context,
                        )?),
                    };
                    overflow.add(part.key, rows).await?;
                    continue;
                }
            }
            let rows = part.take(&data)?;
            self.directory(part.key)
                .write(factory, &rows, written)
                .await?;
        }
        Ok(())
    }

    /// Returns the open directory of the partition key, which is opened if it is not open.
    fn directory(&mut self, key: PartitionKey) -> &mut DirectoryWriter {
        match self.open.entry(key) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let directory = self.router.directory(&self.table_path, entry.key());
                entry.insert(DirectoryWriter::starting_at(
                    directory,
                    self.clustered_files,
                ))
            }
        }
    }

    /// Starts to complete the files of the open directories, before the given
    /// number of other directories receive rows.
    async fn close_open(
        &mut self,
        factory: &FileFactory,
        written: &mut Vec<StagedFile>,
        others: usize,
    ) -> Result<()> {
        let open = std::mem::take(&mut self.open);
        for mut writer in open.into_values() {
            if self.clustered {
                self.clustered_files = writer.next_counter();
            }
            self.start_close(&mut writer, factory, written, others)
                .await?;
        }
        Ok(())
    }

    /// Starts to complete the files of a directory that is not among the open directories.
    ///
    /// The files are completed while the task writes to other directories, so that
    /// the task does not wait for each file. The task waits for the files that
    /// have been completing the longest when it has its number of open files,
    /// which are these files, the open directories, and the given number of
    /// other directories that receive rows.
    async fn start_close(
        &mut self,
        writer: &mut DirectoryWriter,
        factory: &FileFactory,
        written: &mut Vec<StagedFile>,
        others: usize,
    ) -> Result<()> {
        self.closing.extend(writer.start_close(factory));
        let limit = self.max_open_files.saturating_sub(self.open.len() + others);
        self.wait_for_closing(limit, written).await
    }

    /// Waits for the files that are being completed until at most the given number remains.
    async fn wait_for_closing(
        &mut self,
        limit: usize,
        written: &mut Vec<StagedFile>,
    ) -> Result<()> {
        while self.closing.len() > limit {
            if let Some(file) = self.closing.pop_front() {
                written.push(file.wait().await?);
            }
        }
        Ok(())
    }

    /// Adds the rows of the files of the open directories to the files as row
    /// groups, and waits for the files that are being completed, so that
    /// no file keeps rows or row groups in memory.
    pub async fn flush(&mut self, written: &mut Vec<StagedFile>) -> Result<()> {
        for writer in self.open.values_mut() {
            writer.flush(written).await?;
        }
        self.wait_for_closing(0, written).await
    }

    /// The memory of the files in the open directories and of the files
    /// that are being completed.
    pub fn memory_size(&self) -> usize {
        let open: usize = self.open.values().map(|writer| writer.memory_size()).sum();
        let closing: usize = self.closing.iter().map(|file| file.memory_size()).sum();
        open + closing
    }

    /// Completes the files of the open directories, and writes the rows that were
    /// sorted. The reservation follows the memory of the files while these rows are written.
    pub async fn close(
        &mut self,
        factory: &FileFactory,
        written: &mut Vec<StagedFile>,
        reservation: &MemoryReservation,
        pool: &Arc<dyn MemoryPool>,
    ) -> Result<()> {
        // The sorted rows are written to one directory at a time.
        let others = usize::from(self.overflow.is_some());
        self.close_open(factory, written, others).await?;
        let Some(mut overflow) = self.overflow.take() else {
            return self.wait_for_closing(0, written).await;
        };
        // The rows of a partition key are next to each other, so one directory is open at a time.
        let mut current: Option<(u32, DirectoryWriter)> = None;
        while let Some(batch) = overflow.next().await? {
            let ids = match batch.columns().last() {
                Some(ids) => as_uint32_array(ids)?.clone(),
                None => return internal_err!("missing partition keys in the sorted rows"),
            };
            let data = self.router.file_columns(&batch)?;
            let mut start = 0;
            while start < ids.len() {
                let id = ids.value(start);
                let length = ids
                    .values()
                    .iter()
                    .skip(start)
                    .take_while(|x| **x == id)
                    .count();
                let writer = match &mut current {
                    Some((current, writer)) if *current == id => writer,
                    current => {
                        if let Some((_, mut writer)) = current.take() {
                            // The directory of the next rows is open as well.
                            self.start_close(&mut writer, factory, written, 1).await?;
                        }
                        let directory = self.router.directory(&self.table_path, overflow.key(id)?);
                        &mut current.insert((id, DirectoryWriter::new(directory))).1
                    }
                };
                let rows = data.slice(start, length);
                for rows in with_dictionaries(rows, self.router.file_schema())? {
                    writer.write(factory, &rows, written).await?;
                }
                start += length;
            }
            let size = |writer: &Option<(u32, DirectoryWriter)>, files: &Self| {
                files.memory_size() + writer.as_ref().map_or(0, |(_, x)| x.memory_size())
            };
            let total = size(&current, self);
            let flush = async {
                if let Some((_, writer)) = &mut current {
                    writer.flush(written).await?;
                }
                self.wait_for_closing(0, written).await
            };
            if reserve_or_flush(reservation, pool, total, flush).await? {
                reservation.try_resize(size(&current, self))?;
            }
        }
        if let Some((_, mut writer)) = current {
            self.start_close(&mut writer, factory, written, 0).await?;
        }
        self.wait_for_closing(0, written).await
    }
}
