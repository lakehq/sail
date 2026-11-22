// SPDX-License-Identifier: Apache-2.0

use std::borrow::Cow;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};

use chrono::NaiveDate;
use datafusion::arrow::array::{
    ArrayAccessor, ArrayRef, RecordBatch, StringArray, UInt32Array, downcast_dictionary_array,
};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::compute::kernels::zip::zip;
use datafusion::arrow::compute::{cast, is_not_null, take_record_batch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::common::runtime::SpawnedTask;
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
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::task::JoinHandle;

use crate::formats::parquet::writer::task::{DirectoryWriter, FileFactory};
use crate::listing::commit::WrittenFile;
use crate::listing::partition::DEFAULT_PARTITION_NAME;

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
        let values = KeyValues::try_new(&keys)?;
        // Format the first row before anything else, so that an unsupported
        // partition column type is reported by the formatting function.
        self.key(&values, 0)?;
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
            .zip(&values.nulls)
            .map(|(value, nulls)| {
                let value = match value.first() {
                    Some(value) if nulls.as_ref().is_none_or(|x| x.is_valid(row)) => value,
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

/// The partition columns of a batch, prepared for the formatting of their values.
struct KeyValues {
    /// The partition columns, with a value in every row.
    present: RecordBatch,
    /// The rows of each partition column that have a value,
    /// or nothing when all rows of the column have a value.
    nulls: Vec<Option<NullBuffer>>,
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
            nulls: keys.columns().iter().map(|x| x.logical_nulls()).collect(),
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
struct Overflow {
    /// The schema of the input with the column of the partition key.
    schema: SchemaRef,
    sender: Sender<Result<RecordBatch>>,
    sorted: Receiver<Result<RecordBatch>>,
    /// The task that sorts the rows. The task is canceled when this is dropped.
    _sort: SpawnedTask<()>,
    /// The number of each partition key in the order of their first rows.
    ids: HashMap<PartitionKey, u32>,
    keys: Vec<PartitionKey>,
}

impl Overflow {
    fn try_new(
        input_schema: &SchemaRef,
        ordering: Option<&LexOrdering>,
        context: &Arc<TaskContext>,
    ) -> Result<Self> {
        let mut fields = input_schema.fields().to_vec();
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
            sender,
            sorted,
            _sort: sort,
            ids: HashMap::new(),
            keys: vec![],
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
        let mut columns = rows.columns().to_vec();
        columns.push(Arc::new(UInt32Array::from(vec![id; rows.num_rows()])));
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), columns)?;
        if self.sender.send(Ok(batch)).await.is_ok() {
            return Ok(());
        }
        // The sort has ended, which it does before the end of its input only when it fails.
        while let Some(batch) = self.sorted.recv().await {
            batch?;
        }
        exec_err!("the sort of the partitioned rows ended before the end of its input")
    }
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
    /// The directories whose file was completed when the rows of another partition key
    /// followed. More rows for such a directory go to a new file.
    closed: HashMap<PartitionKey, DirectoryWriter>,
    /// The files that are being completed, in the order in which they received
    /// their last rows. The number of these files and of the open directories
    /// together is at most the number of directories that can be open.
    closing: VecDeque<JoinHandle<Result<WrittenFile>>>,
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
            closed: HashMap::new(),
            closing: VecDeque::new(),
            overflow: None,
        }
    }

    pub async fn write(
        &mut self,
        factory: &FileFactory,
        batch: &RecordBatch,
        written: &mut Vec<WrittenFile>,
    ) -> Result<()> {
        let (parts, data) = {
            let _timer = factory.elapsed_compute().timer();
            (self.router.split(batch)?, self.router.file_columns(batch)?)
        };
        for part in parts {
            if !self.open.contains_key(&part.key) {
                if self.clustered {
                    // The rows of the open directory have ended.
                    self.close_open(factory, written).await?;
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
                let writer = match self.closed.remove(entry.key()) {
                    Some(writer) => writer,
                    None => {
                        DirectoryWriter::new(self.router.directory(&self.table_path, entry.key()))
                    }
                };
                entry.insert(writer)
            }
        }
    }

    /// Starts to complete the files of the open directories.
    async fn close_open(
        &mut self,
        factory: &FileFactory,
        written: &mut Vec<WrittenFile>,
    ) -> Result<()> {
        let keys: Vec<_> = self.open.keys().cloned().collect();
        for key in keys {
            if let Some(mut writer) = self.open.remove(&key) {
                self.start_close(&mut writer, factory, written).await?;
                self.closed.insert(key, writer);
            }
        }
        Ok(())
    }

    /// Starts to complete the files of a directory that is not among the open directories.
    ///
    /// The files are completed while the task writes to other directories, so that
    /// the task does not wait for each file. The task waits for the files that
    /// have been completing the longest when it has its number of open files.
    async fn start_close(
        &mut self,
        writer: &mut DirectoryWriter,
        factory: &FileFactory,
        written: &mut Vec<WrittenFile>,
    ) -> Result<()> {
        self.closing.extend(writer.start_close(factory));
        let limit = self.max_open_files.saturating_sub(self.open.len());
        self.wait_for_closing(limit, written).await
    }

    /// Waits for the files that are being completed until at most the given number remains.
    async fn wait_for_closing(
        &mut self,
        limit: usize,
        written: &mut Vec<WrittenFile>,
    ) -> Result<()> {
        while self.closing.len() > limit {
            if let Some(task) = self.closing.pop_front() {
                let file = task
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))??;
                written.push(file);
            }
        }
        Ok(())
    }

    pub fn memory_size(&self) -> usize {
        self.open.values().map(|writer| writer.memory_size()).sum()
    }

    pub async fn close(
        &mut self,
        factory: &FileFactory,
        written: &mut Vec<WrittenFile>,
    ) -> Result<()> {
        self.close_open(factory, written).await?;
        let Some(overflow) = self.overflow.take() else {
            return self.wait_for_closing(0, written).await;
        };
        let Overflow {
            sender,
            mut sorted,
            _sort,
            keys,
            ..
        } = overflow;
        // The sort returns its rows when its input has ended.
        drop(sender);
        // The rows of a partition key are next to each other, so one directory is open at a time.
        let mut current: Option<(u32, DirectoryWriter)> = None;
        while let Some(batch) = sorted.recv().await {
            let batch = batch?;
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
                            self.start_close(&mut writer, factory, written).await?;
                        }
                        let key = keys.get(id as usize).ok_or_else(|| {
                            internal_datafusion_err!("unknown partition key in the sorted rows")
                        })?;
                        let writer =
                            DirectoryWriter::new(self.router.directory(&self.table_path, key));
                        &mut current.insert((id, writer)).1
                    }
                };
                writer
                    .write(factory, &data.slice(start, length), written)
                    .await?;
                start += length;
            }
        }
        if let Some((_, mut writer)) = current {
            self.start_close(&mut writer, factory, written).await?;
        }
        self.wait_for_closing(0, written).await
    }
}
