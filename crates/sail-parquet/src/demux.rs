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

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::builder::UInt64Builder;
use datafusion::arrow::array::cast::AsArray;
use datafusion::arrow::array::{RecordBatch, StructArray};
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::runtime::SpawnedTask;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion_common::{
    Result, exec_datafusion_err, internal_datafusion_err, not_impl_err, plan_datafusion_err,
};
use datafusion_datasource::ListingTableUrl;
use datafusion_datasource::file_sink_config::FileSinkConfig;
use futures::StreamExt;
use object_store::path::{Path, PathPart};
use sail_common_datafusion::hive_partition::{format_partition_values, partition_path_segment};
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
                    )?;
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

fn compute_partition_keys_by_row(
    batch: &RecordBatch,
    partition_by: &[(String, DataType)],
) -> Result<Vec<Vec<String>>> {
    let schema = batch.schema();
    let mut all_partition_values = Vec::with_capacity(partition_by.len());
    for (column, _) in partition_by {
        let data_type = schema.field_with_name(column)?.data_type();
        if !supports_hive_partition_type(data_type) {
            return not_impl_err!(
                "writing Hive partitions with data type {data_type} is not supported"
            );
        }
        let array = batch.column_by_name(column).ok_or_else(|| {
            exec_datafusion_err!("partition column {column} does not exist in schema {schema}")
        })?;
        all_partition_values.push(format_partition_values(array.as_ref())?);
    }
    Ok(all_partition_values)
}

fn supports_hive_partition_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _)
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Interval(_) => true,
        DataType::Dictionary(_, value_type) => supports_hive_partition_type(value_type),
        _ => false,
    }
}

fn compute_take_arrays(
    batch: &RecordBatch,
    all_partition_values: &[Vec<String>],
) -> HashMap<Vec<String>, UInt64Builder> {
    let mut take_arrays = HashMap::new();
    for row in 0..batch.num_rows() {
        let partition_key = all_partition_values
            .iter()
            .map(|values| values[row].clone())
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
) -> Result<Path> {
    let mut path = base_output_path.prefix().clone();
    for (index, value) in partition_key.iter().enumerate() {
        let segment = partition_path_segment(&partition_by[index].0, value);
        path = path.join(PathPart::parse(&segment).map_err(|error| {
            exec_datafusion_err!("invalid Hive partition path segment {segment:?}: {error}")
        })?);
    }
    Ok(path.join(format!("{write_id}.{file_extension}")))
}
