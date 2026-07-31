// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use chrono::NaiveDate;
use datafusion::arrow::array::builder::UInt64Builder;
use datafusion::arrow::array::cast::AsArray;
use datafusion::arrow::array::{ArrayAccessor, RecordBatch, StructArray};
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::cast::{
    as_boolean_array, as_date32_array, as_date64_array, as_float16_array, as_float32_array,
    as_float64_array, as_int8_array, as_int16_array, as_int32_array, as_int64_array,
    as_large_string_array, as_string_array, as_string_view_array, as_uint8_array, as_uint16_array,
    as_uint32_array, as_uint64_array,
};
use datafusion::common::runtime::SpawnedTask;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion_common::{
    Result, exec_datafusion_err, internal_datafusion_err, not_impl_err, plan_datafusion_err,
};
use datafusion_datasource::ListingTableUrl;
use datafusion_datasource::file_sink_config::FileSinkConfig;
use futures::StreamExt;
use object_store::path::Path;
use tokio::sync::mpsc::{self, Receiver, Sender, UnboundedReceiver, UnboundedSender};

type FileStreamReceiver = UnboundedReceiver<(Path, Receiver<RecordBatch>)>;

pub(crate) fn start_demuxer_task(
    config: &FileSinkConfig,
    data: SendableRecordBatchStream,
    context: &Arc<TaskContext>,
    write_id: String,
) -> Result<(SpawnedTask<Result<()>>, FileStreamReceiver)> {
    let base_output_path = config
        .table_paths
        .first()
        .cloned()
        .ok_or_else(|| plan_datafusion_err!("Parquet sink requires one output path"))?;
    let (sender, receiver) = mpsc::unbounded_channel();
    let context = Arc::clone(context);
    let file_extension = config.file_extension.clone();
    let task = if config.table_partition_cols.is_empty() {
        let single_file_output = config
            .file_output_mode
            .single_file_output(&base_output_path);
        SpawnedTask::spawn(row_count_demuxer(
            sender,
            data,
            context,
            base_output_path,
            file_extension,
            single_file_output,
            write_id,
        ))
    } else {
        SpawnedTask::spawn(hive_style_partitions_demuxer(
            sender,
            data,
            context,
            config.table_partition_cols.clone(),
            base_output_path,
            file_extension,
            config.keep_partition_by_columns,
            write_id,
        ))
    };
    Ok((task, receiver))
}

async fn row_count_demuxer(
    mut sender: UnboundedSender<(Path, Receiver<RecordBatch>)>,
    mut input: SendableRecordBatchStream,
    context: Arc<TaskContext>,
    base_output_path: ListingTableUrl,
    file_extension: String,
    single_file_output: bool,
    write_id: String,
) -> Result<()> {
    let execution = &context.session_config().options().execution;
    let mut minimum_parallel_files = execution.minimum_parallel_output_files.max(1);
    let mut max_rows_per_file = execution.soft_max_rows_per_output_file;
    let max_buffered_batches = execution.max_buffered_batches_per_output_file.max(1);
    if single_file_output {
        minimum_parallel_files = 1;
        max_rows_per_file = usize::MAX;
    }

    let mut next_part_index = 0;
    let mut open_file_streams = Vec::with_capacity(minimum_parallel_files);
    let mut row_counts = Vec::with_capacity(minimum_parallel_files);
    let mut next_stream = 0;
    let mut received_batch = false;

    if single_file_output {
        open_file_streams.push(create_file_stream(
            &base_output_path,
            &write_id,
            next_part_index,
            &file_extension,
            true,
            max_buffered_batches,
            &mut sender,
        )?);
        row_counts.push(0);
        next_part_index += 1;
    }

    let schema = input.schema();
    while let Some(batch) = input.next().await.transpose()? {
        received_batch = true;
        if open_file_streams.len() < minimum_parallel_files {
            open_file_streams.push(create_file_stream(
                &base_output_path,
                &write_id,
                next_part_index,
                &file_extension,
                false,
                max_buffered_batches,
                &mut sender,
            )?);
            row_counts.push(0);
            next_part_index += 1;
        } else if row_counts[next_stream] >= max_rows_per_file {
            row_counts[next_stream] = 0;
            open_file_streams[next_stream] = create_file_stream(
                &base_output_path,
                &write_id,
                next_part_index,
                &file_extension,
                false,
                max_buffered_batches,
                &mut sender,
            )?;
            next_part_index += 1;
        }
        row_counts[next_stream] = row_counts[next_stream].saturating_add(batch.num_rows());
        open_file_streams[next_stream]
            .send(batch)
            .await
            .map_err(|_| exec_datafusion_err!("failed to send batch to Parquet writer"))?;
        next_stream = (next_stream + 1) % minimum_parallel_files;
    }

    if single_file_output && !received_batch {
        open_file_streams
            .first_mut()
            .ok_or_else(|| internal_datafusion_err!("missing single-file output stream"))?
            .send(RecordBatch::new_empty(schema))
            .await
            .map_err(|_| exec_datafusion_err!("failed to send empty Parquet batch"))?;
    }
    Ok(())
}

fn create_file_stream(
    base_output_path: &ListingTableUrl,
    write_id: &str,
    part_index: usize,
    file_extension: &str,
    single_file_output: bool,
    max_buffered_batches: usize,
    sender: &mut UnboundedSender<(Path, Receiver<RecordBatch>)>,
) -> Result<Sender<RecordBatch>> {
    let path = if single_file_output {
        base_output_path.prefix().to_owned()
    } else {
        base_output_path
            .prefix()
            .clone()
            .join(format!("{write_id}_{part_index}.{file_extension}"))
    };
    let (file_sender, file_receiver) = mpsc::channel(max_buffered_batches.div_ceil(2).max(1));
    sender
        .send((path, file_receiver))
        .map_err(|_| exec_datafusion_err!("failed to create Parquet file stream"))?;
    Ok(file_sender)
}

async fn hive_style_partitions_demuxer(
    sender: UnboundedSender<(Path, Receiver<RecordBatch>)>,
    mut input: SendableRecordBatchStream,
    context: Arc<TaskContext>,
    partition_by: Vec<(String, DataType)>,
    base_output_path: ListingTableUrl,
    file_extension: String,
    keep_partition_by_columns: bool,
    write_id: String,
) -> Result<()> {
    let max_buffered_batches = context
        .session_config()
        .options()
        .execution
        .max_buffered_batches_per_output_file
        .max(1);
    let mut partition_streams: HashMap<Vec<String>, Sender<RecordBatch>> = HashMap::new();

    while let Some(batch) = input.next().await.transpose()? {
        let partition_values = compute_partition_keys_by_row(&batch, &partition_by)?;
        let take_arrays = compute_take_arrays(&batch, &partition_values);
        for (partition_key, mut builder) in take_arrays {
            let take_indices = builder.finish();
            let struct_array: StructArray = batch.clone().into();
            let partition_batch = RecordBatch::from(
                datafusion::arrow::compute::take(&struct_array, &take_indices, None)?.as_struct(),
            );
            let partition_sender = match partition_streams.get_mut(&partition_key) {
                Some(partition_sender) => partition_sender,
                None => {
                    let (partition_sender, partition_receiver) =
                        mpsc::channel(max_buffered_batches);
                    let file_path = hive_style_file_path(
                        &partition_key,
                        &partition_by,
                        &write_id,
                        &file_extension,
                        &base_output_path,
                    );
                    sender
                        .send((file_path, partition_receiver))
                        .map_err(|_| exec_datafusion_err!("failed to create partition writer"))?;
                    partition_streams.insert(partition_key.clone(), partition_sender);
                    partition_streams.get_mut(&partition_key).ok_or_else(|| {
                        internal_datafusion_err!("new partition writer was not retained")
                    })?
                }
            };
            let partition_batch = if keep_partition_by_columns {
                partition_batch
            } else {
                remove_partition_columns(&partition_batch, &partition_by)?
            };
            partition_sender
                .send(partition_batch)
                .await
                .map_err(|_| internal_datafusion_err!("failed to send partition batch"))?;
        }
    }
    Ok(())
}

fn compute_partition_keys_by_row<'a>(
    batch: &'a RecordBatch,
    partition_by: &'a [(String, DataType)],
) -> Result<Vec<Vec<Cow<'a, str>>>> {
    const EPOCH_DAYS_FROM_CE: i32 = 719_163;

    let schema = batch.schema();
    let mut all_partition_values = Vec::with_capacity(partition_by.len());
    for (column, _) in partition_by {
        let data_type = schema.field_with_name(column)?.data_type();
        let array = batch.column_by_name(column).ok_or_else(|| {
            exec_datafusion_err!("partition column {column} does not exist in schema {schema}")
        })?;
        let mut values = Vec::with_capacity(batch.num_rows());
        match data_type {
            DataType::Utf8 => {
                let array = as_string_array(array)?;
                for index in 0..batch.num_rows() {
                    values.push(Cow::from(array.value(index)));
                }
            }
            DataType::LargeUtf8 => {
                let array = as_large_string_array(array)?;
                for index in 0..batch.num_rows() {
                    values.push(Cow::from(array.value(index)));
                }
            }
            DataType::Utf8View => {
                let array = as_string_view_array(array)?;
                for index in 0..batch.num_rows() {
                    values.push(Cow::from(array.value(index)));
                }
            }
            DataType::Boolean => {
                let array = as_boolean_array(array)?;
                for index in 0..batch.num_rows() {
                    values.push(Cow::from(array.value(index).to_string()));
                }
            }
            DataType::Date32 => {
                let array = as_date32_array(array)?;
                for index in 0..batch.num_rows() {
                    let days = EPOCH_DAYS_FROM_CE
                        .checked_add(array.value(index))
                        .ok_or_else(|| {
                            internal_datafusion_err!("Date32 partition value is out of range")
                        })?;
                    let date = NaiveDate::from_num_days_from_ce_opt(days).ok_or_else(|| {
                        internal_datafusion_err!("Date32 partition value is out of range")
                    })?;
                    values.push(Cow::from(date.format("%Y-%m-%d").to_string()));
                }
            }
            DataType::Date64 => {
                let array = as_date64_array(array)?;
                for index in 0..batch.num_rows() {
                    let epoch_days =
                        i32::try_from(array.value(index) / 86_400_000).map_err(|_| {
                            internal_datafusion_err!("Date64 partition value is out of range")
                        })?;
                    let days = EPOCH_DAYS_FROM_CE.checked_add(epoch_days).ok_or_else(|| {
                        internal_datafusion_err!("Date64 partition value is out of range")
                    })?;
                    let date = NaiveDate::from_num_days_from_ce_opt(days).ok_or_else(|| {
                        internal_datafusion_err!("Date64 partition value is out of range")
                    })?;
                    values.push(Cow::from(date.format("%Y-%m-%d").to_string()));
                }
            }
            DataType::Int8 => push_display_values(as_int8_array(array)?, &mut values, batch),
            DataType::Int16 => push_display_values(as_int16_array(array)?, &mut values, batch),
            DataType::Int32 => push_display_values(as_int32_array(array)?, &mut values, batch),
            DataType::Int64 => push_display_values(as_int64_array(array)?, &mut values, batch),
            DataType::UInt8 => push_display_values(as_uint8_array(array)?, &mut values, batch),
            DataType::UInt16 => push_display_values(as_uint16_array(array)?, &mut values, batch),
            DataType::UInt32 => push_display_values(as_uint32_array(array)?, &mut values, batch),
            DataType::UInt64 => push_display_values(as_uint64_array(array)?, &mut values, batch),
            DataType::Float16 => push_display_values(as_float16_array(array)?, &mut values, batch),
            DataType::Float32 => push_display_values(as_float32_array(array)?, &mut values, batch),
            DataType::Float64 => push_display_values(as_float64_array(array)?, &mut values, batch),
            DataType::Dictionary(_, _) => {
                let strings = datafusion::arrow::compute::cast(array, &DataType::Utf8)?;
                let strings = as_string_array(&strings)?;
                for index in 0..batch.num_rows() {
                    values.push(Cow::from(strings.value(index).to_string()));
                }
            }
            _ => {
                return not_impl_err!(
                    "writing Hive partitions with data type {data_type} is not supported"
                );
            }
        }
        all_partition_values.push(values);
    }
    Ok(all_partition_values)
}

fn push_display_values<'a, T>(array: T, values: &mut Vec<Cow<'a, str>>, batch: &RecordBatch)
where
    T: ArrayAccessor,
    T::Item: ToString,
{
    for index in 0..batch.num_rows() {
        values.push(Cow::from(array.value(index).to_string()));
    }
}

fn compute_take_arrays(
    batch: &RecordBatch,
    all_partition_values: &[Vec<Cow<'_, str>>],
) -> HashMap<Vec<String>, UInt64Builder> {
    let mut take_arrays = HashMap::new();
    for row in 0..batch.num_rows() {
        let partition_key = all_partition_values
            .iter()
            .map(|values| values[row].clone().into_owned())
            .collect::<Vec<_>>();
        take_arrays
            .entry(partition_key)
            .or_insert_with(UInt64Builder::new)
            .append_value(row as u64);
    }
    take_arrays
}

fn remove_partition_columns(
    batch: &RecordBatch,
    partition_by: &[(String, DataType)],
) -> Result<RecordBatch> {
    let partition_names = partition_by
        .iter()
        .map(|(name, _)| name)
        .collect::<Vec<_>>();
    let (columns, fields): (Vec<_>, Vec<_>) = batch
        .columns()
        .iter()
        .zip(batch.schema().fields())
        .filter(|(_, field)| !partition_names.contains(&field.name()))
        .map(|(array, field)| (Arc::clone(array), field.as_ref().clone()))
        .unzip();
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

fn hive_style_file_path(
    partition_key: &[String],
    partition_by: &[(String, DataType)],
    write_id: &str,
    file_extension: &str,
    base_output_path: &ListingTableUrl,
) -> Path {
    let mut path = base_output_path.prefix().clone();
    for (index, value) in partition_key.iter().enumerate() {
        path = path.join(format!("{}={value}", partition_by[index].0));
    }
    path.join(format!("{write_id}.{file_extension}"))
}
