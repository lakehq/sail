use std::sync::Arc;

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ipc::convert::fb_to_schema;
use datafusion::arrow::ipc::reader::read_footer_length;
use datafusion::arrow::ipc::{root_as_footer, root_as_message};
use datafusion::catalog::Session;
use datafusion::datasource::physical_plan::ArrowSource;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::union::UnionExec;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{Result, internal_datafusion_err};
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::source::DataSourceExec;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt};
use sail_common_datafusion::array::record_batch::retag_schema_timestamp_timezone;

use crate::listing::source::{
    ListingFileSample, ListingScanInput, ReadFormat, retag_timestamp_plan,
};

#[derive(Debug, Default, Clone)]
pub struct ArrowReadFormat;

const ARROW_MAGIC: [u8; 6] = *b"ARROW1";
const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

async fn is_arrow_file(store: &dyn ObjectStore, object_location: &Path) -> Result<bool> {
    let options = GetOptions {
        range: Some(GetRange::Bounded(0..6)),
        ..Default::default()
    };
    let bytes = store
        .get_opts(object_location, options)
        .await?
        .bytes()
        .await?;
    Ok(bytes.as_ref() == ARROW_MAGIC)
}

async fn read_arrow_schema(
    store: &dyn ObjectStore,
    object_location: &Path,
    file_format: bool,
) -> Result<SchemaRef> {
    if file_format {
        let footer = store
            .get_opts(
                object_location,
                GetOptions {
                    range: Some(GetRange::Suffix(10)),
                    ..Default::default()
                },
            )
            .await?
            .bytes()
            .await?;
        let footer_len = read_footer_length(footer.as_ref().try_into().map_err(|error| {
            ArrowError::ParseError(format!("Unable to read IPC footer length: {error:?}"))
        })?)?;
        let suffix_len = u64::try_from(footer_len).map_err(|error| {
            ArrowError::ParseError(format!("IPC footer length does not fit u64: {error:?}"))
        })?;
        let footer = store
            .get_opts(
                object_location,
                GetOptions {
                    range: Some(GetRange::Suffix(10 + suffix_len)),
                    ..Default::default()
                },
            )
            .await?
            .bytes()
            .await?;
        let footer_bytes = footer.get(..footer_len).ok_or_else(|| {
            ArrowError::ParseError(format!(
                "IPC footer declares {footer_len} bytes, but only {} are available",
                footer.len()
            ))
        })?;
        let footer = root_as_footer(footer_bytes).map_err(|error| {
            ArrowError::ParseError(format!("Unable to read IPC footer: {error:?}"))
        })?;
        return Ok(Arc::new(fb_to_schema(footer.schema().ok_or_else(
            || ArrowError::IpcError("Unable to read IPC footer schema".to_string()),
        )?)));
    }

    // IPC streams place the schema message at the start. Read only that
    // message instead of downloading the whole object during planning.
    let mut bytes = store.get_range(object_location, 0..16).await?.to_vec();
    let preamble_len = ipc_preamble_len(&bytes);
    let meta_len = ipc_metadata_len(&bytes, preamble_len)?;
    let required = preamble_len + 4 + meta_len;
    if bytes.len() < required {
        bytes.extend_from_slice(
            &store
                .get_range(object_location, bytes.len() as u64..required as u64)
                .await?,
        );
    }
    schema_from_ipc_message(&bytes, preamble_len)
}

async fn read_arrow_info(
    store: &dyn ObjectStore,
    object_location: &Path,
) -> Result<(bool, SchemaRef)> {
    let file_format = is_arrow_file(store, object_location).await?;
    let schema = read_arrow_schema(store, object_location, file_format).await?;
    Ok((file_format, schema))
}

fn build_scan_config(
    input: ListingScanInput,
    physical_schema: SchemaRef,
    file_format: bool,
) -> Result<FileScanConfig> {
    // Decode with the physical IPC schema first. The Arrow reader constructs batches
    // directly with this schema and cannot accept a canonicalized timezone in its place.
    let physical_table_schema = datafusion_datasource::TableSchema::new(
        physical_schema,
        input.schema.table_partition_cols().clone(),
    );
    let source = if file_format {
        ArrowSource::new_file_source(physical_table_schema)
    } else {
        ArrowSource::new_stream_file_source(physical_table_schema)
    };
    Ok(
        FileScanConfigBuilder::new(input.object_store_url, Arc::new(source))
            .with_file_groups(input.file_groups)
            .with_constraints(input.constraints)
            .with_statistics(input.statistics)
            .with_projection_indices(input.projection)?
            .with_limit(input.limit)
            .with_output_ordering(input.output_ordering)
            .with_preserve_order(input.preserve_order)
            .with_partitioned_by_file_group(input.partitioned_by_file_group)
            .build(),
    )
}

#[derive(Debug)]
struct ArrowScanGroup {
    file_format: bool,
    schema: SchemaRef,
    files_by_group: Vec<Vec<PartitionedFile>>,
}

#[async_trait::async_trait]
impl ReadFormat for ArrowReadFormat {
    async fn infer_compression(
        &self,
        _ctx: &dyn Session,
        _files: &[ListingFileSample<'_>],
    ) -> Result<CompressionTypeVariant> {
        Ok(CompressionTypeVariant::UNCOMPRESSED)
    }

    async fn infer_schema(
        &self,
        ctx: &dyn Session,
        files: &[ListingFileSample<'_>],
        _compression: CompressionTypeVariant,
    ) -> Result<SchemaRef> {
        let mut objects = vec![];
        for group in files {
            for object in &group.objects {
                objects.push((Arc::clone(&group.store), object.location.clone()));
            }
        }
        let mut formats = futures::stream::iter(objects)
            .map(|(store, location)| async move {
                let (file_format, schema) = read_arrow_info(store.as_ref(), &location).await?;
                Ok::<_, datafusion_common::DataFusionError>((location, file_format, schema))
            })
            .boxed()
            .buffer_unordered(ctx.config_options().execution.meta_fetch_concurrency)
            .try_collect::<Vec<_>>()
            .await?;
        formats.sort_unstable_by(|(left, _, _), (right, _, _)| left.cmp(right));

        let file_format = formats.first().map(|(_, format, _)| *format);
        if formats
            .iter()
            .any(|(_, format, _)| Some(*format) != file_format)
        {
            return Err(internal_datafusion_err!(
                "Mixed Arrow IPC file and stream formats are not supported in one scan"
            ));
        }
        let mut canonical_schema = None;
        for (_, _, schema) in formats {
            let schema = retag_schema_timestamp_timezone(schema.as_ref(), "UTC")?;
            if canonical_schema
                .as_ref()
                .is_some_and(|expected| expected != &schema)
            {
                return Err(internal_datafusion_err!(
                    "Arrow IPC files have schemas that differ beyond timestamp timezone metadata"
                ));
            }
            canonical_schema = Some(schema);
        }
        Ok(Arc::new(canonical_schema.unwrap_or_else(Schema::empty)))
    }

    async fn scan(&self, ctx: &dyn Session, input: ListingScanInput) -> Result<FileScanConfig> {
        let object_store = ctx.runtime_env().object_store(&input.object_store_url)?;
        let first = input
            .file_groups
            .iter()
            .flat_map(|group| group.files())
            .next()
            .ok_or_else(|| internal_datafusion_err!("No files found in file group"))?;
        let (file_format, physical_schema) =
            read_arrow_info(object_store.as_ref(), &first.object_meta.location).await?;
        build_scan_config(input, physical_schema, file_format)
    }

    async fn scan_plan(
        &self,
        ctx: &dyn Session,
        input: ListingScanInput,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let object_store = ctx.runtime_env().object_store(&input.object_store_url)?;
        let mut files = vec![];
        for (group, file_group) in input.file_groups.iter().enumerate() {
            for (position, file) in file_group.files().iter().enumerate() {
                files.push((group, position, file.clone()));
            }
        }
        let mut files = futures::stream::iter(files)
            .map(|(original_group, position, file)| {
                let object_store = Arc::clone(&object_store);
                async move {
                    let (file_format, schema) =
                        read_arrow_info(object_store.as_ref(), &file.object_meta.location).await?;
                    Ok::<_, datafusion_common::DataFusionError>((
                        original_group,
                        position,
                        file,
                        file_format,
                        schema,
                    ))
                }
            })
            .boxed()
            .buffer_unordered(ctx.config_options().execution.meta_fetch_concurrency)
            .try_collect::<Vec<_>>()
            .await?;
        files.sort_unstable_by_key(|(group, position, _, _, _)| (*group, *position));

        let file_format = files.first().map(|(_, _, _, format, _)| *format);
        if files
            .iter()
            .any(|(_, _, _, format, _)| Some(*format) != file_format)
        {
            return Err(internal_datafusion_err!(
                "Mixed Arrow IPC file and stream formats are not supported in one scan"
            ));
        }

        let original_group_count = input.file_groups.len();
        let preserve_file_order = input.preserve_order || !input.output_ordering.is_empty();
        let mut groups: Vec<ArrowScanGroup> = vec![];
        for (original_group, _, file, file_format, schema) in files {
            let index = if preserve_file_order {
                groups
                    .last()
                    .is_some_and(|group| group.file_format == file_format && group.schema == schema)
                    .then_some(groups.len() - 1)
            } else {
                groups
                    .iter()
                    .position(|group| group.file_format == file_format && group.schema == schema)
            }
            .unwrap_or_else(|| {
                groups.push(ArrowScanGroup {
                    file_format,
                    schema: Arc::clone(&schema),
                    files_by_group: vec![vec![]; original_group_count],
                });
                groups.len() - 1
            });
            groups[index].files_by_group[original_group].push(file);
        }

        let ListingScanInput {
            object_store_url,
            file_groups: original_file_groups,
            constraints,
            projection,
            limit,
            preserve_order,
            output_ordering,
            statistics,
            partitioned_by_file_group,
            schema,
            compression,
        } = input;
        for group in &groups {
            let canonical = retag_schema_timestamp_timezone(group.schema.as_ref(), "UTC")?;
            if &canonical != schema.file_schema().as_ref() {
                return Err(internal_datafusion_err!(
                    "Arrow IPC files have schemas that differ beyond timestamp timezone metadata"
                ));
            }
        }

        let group_count = groups.len();
        let mut plans = Vec::with_capacity(group_count);
        for group in groups {
            let file_groups = if group_count == 1 {
                original_file_groups.clone()
            } else {
                group
                    .files_by_group
                    .into_iter()
                    .filter(|files| !files.is_empty())
                    .map(FileGroup::new)
                    .collect()
            };
            // Whole-scan statistics cannot be assigned to every heterogeneous child without
            // making the union multiply them. Per-file statistics remain attached to the files.
            let child_statistics = if group_count == 1 {
                statistics.clone()
            } else {
                datafusion_common::Statistics::new_unknown(schema.table_schema())
            };
            let config = build_scan_config(
                ListingScanInput {
                    object_store_url: object_store_url.clone(),
                    file_groups,
                    constraints: constraints.clone(),
                    projection: projection.clone(),
                    limit,
                    preserve_order,
                    output_ordering: output_ordering.clone(),
                    statistics: child_statistics,
                    partitioned_by_file_group,
                    schema: schema.clone(),
                    compression,
                },
                group.schema,
                group.file_format,
            )?;
            plans.push(retag_timestamp_plan(
                DataSourceExec::from_data_source(config),
                &Arc::from("UTC"),
            )?);
        }
        UnionExec::try_new(plans)
    }

    fn adapt_scan_plan(
        &self,
        input: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    ) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        retag_timestamp_plan(input, &Arc::from("UTC"))
    }
}

fn ipc_preamble_len(bytes: &[u8]) -> usize {
    // The preamble length is everything before the metadata length
    if bytes.starts_with(&ARROW_MAGIC) {
        // File format starts with magic number "ARROW1"
        if bytes.get(8..12) == Some(CONTINUATION_MARKER.as_slice()) {
            // Continuation marker was added in v0.15.0
            12
        } else {
            // File format before v0.15.0
            8
        }
    } else if bytes.starts_with(&CONTINUATION_MARKER) {
        // Stream format after v0.15.0 starts with continuation marker
        4
    } else {
        // Stream format before v0.15.0 does not have a preamble
        0
    }
}

fn ipc_metadata_len(bytes: &[u8], preamble_len: usize) -> Result<usize> {
    let end = preamble_len.checked_add(4).ok_or_else(|| {
        ArrowError::ParseError("IPC message metadata offset overflowed".to_string())
    })?;
    let meta_len_bytes: [u8; 4] = bytes
        .get(preamble_len..end)
        .ok_or_else(|| {
            ArrowError::ParseError(format!(
                "IPC message is truncated before its metadata length at byte {preamble_len}"
            ))
        })?
        .try_into()
        .map_err(|error| {
            ArrowError::ParseError(format!(
                "Unable to read IPC message metadata length: {error:?}"
            ))
        })?;

    let meta_len = i32::from_le_bytes(meta_len_bytes);
    if meta_len < 0 {
        return Err(
            ArrowError::ParseError("IPC message metadata length is negative".to_string()).into(),
        );
    }
    Ok(meta_len as usize)
}

fn schema_from_ipc_message(bytes: &[u8], preamble_len: usize) -> Result<SchemaRef> {
    let metadata_offset = preamble_len.checked_add(4).ok_or_else(|| {
        ArrowError::ParseError("IPC message metadata offset overflowed".to_string())
    })?;
    let metadata = bytes.get(metadata_offset..).ok_or_else(|| {
        ArrowError::ParseError(format!(
            "IPC message is truncated before its metadata at byte {metadata_offset}"
        ))
    })?;
    let message = root_as_message(metadata).map_err(|err| {
        ArrowError::ParseError(format!("Unable to read IPC message metadata: {err:?}"))
    })?;
    let fb_schema = message
        .header_as_schema()
        .ok_or_else(|| ArrowError::IpcError("Unable to read IPC message schema".to_string()))?;
    Ok(Arc::new(fb_to_schema(fb_schema)))
}

#[cfg(test)]
#[expect(clippy::panic)]
mod tests {
    use datafusion::arrow::array::{AsArray, TimestampMicrosecondArray};
    use datafusion::arrow::datatypes::{DataType, Field, TimeUnit, TimestampMicrosecondType};
    use datafusion::arrow::ipc::writer::{FileWriter, StreamWriter};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;
    use datafusion_common::{Constraints, ScalarValue, Statistics};
    use datafusion_datasource::file_groups::FileGroup;
    use datafusion_datasource::{ListingTableUrl, PartitionedFile, TableSchema};
    use object_store::ObjectMeta;
    use object_store::memory::InMemory;

    use super::*;

    fn timestamp_data(timezone: &str, value: i64) -> Result<(SchemaRef, RecordBatch)> {
        let timezone = Arc::<str>::from(timezone);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::clone(&timezone))),
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(
                TimestampMicrosecondArray::from(vec![value]).with_timezone(timezone),
            )],
        )?;
        Ok((schema, batch))
    }

    fn stream_bytes(timezone: &str, value: i64) -> Result<Vec<u8>> {
        let (schema, batch) = timestamp_data(timezone, value)?;
        let mut bytes = Vec::new();
        let mut writer = StreamWriter::try_new(&mut bytes, &schema)?;
        writer.write(&batch)?;
        writer.finish()?;
        drop(writer);
        Ok(bytes)
    }

    fn file_bytes(timezone: &str, value: i64) -> Result<Vec<u8>> {
        let (schema, batch) = timestamp_data(timezone, value)?;
        let mut bytes = Vec::new();
        let mut writer = FileWriter::try_new(&mut bytes, &schema)?;
        writer.write(&batch)?;
        writer.finish()?;
        drop(writer);
        Ok(bytes)
    }

    fn scan_input(
        url: &ListingTableUrl,
        objects: Vec<ObjectMeta>,
        schema: SchemaRef,
    ) -> ListingScanInput {
        ListingScanInput {
            object_store_url: url.object_store(),
            file_groups: vec![FileGroup::from(
                objects
                    .into_iter()
                    .map(PartitionedFile::from)
                    .collect::<Vec<_>>(),
            )],
            constraints: Constraints::default(),
            projection: None,
            limit: None,
            preserve_order: false,
            output_ordering: vec![],
            statistics: Statistics::new_unknown(&schema),
            partitioned_by_file_group: false,
            schema: TableSchema::new(schema, vec![]),
            compression: CompressionTypeVariant::UNCOMPRESSED,
        }
    }

    #[tokio::test]
    async fn inferred_arrow_ltz_schema_is_canonical_utc() -> Result<()> {
        let bytes = stream_bytes("+01:02:03", -3_723_000_000)?;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let location = Path::from("input.arrow");
        store.put(&location, bytes.into()).await?;
        let object = store.head(&location).await?;
        let url = ListingTableUrl::parse("memory://bucket/input.arrow")?;
        let samples = [ListingFileSample {
            url: &url,
            store: Arc::clone(&store),
            objects: vec![object.clone()],
        }];
        let context = SessionContext::new();

        let schema = ArrowReadFormat
            .infer_schema(
                &context.state(),
                &samples,
                CompressionTypeVariant::UNCOMPRESSED,
            )
            .await?;
        assert_eq!(
            schema.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
        );

        let object_store_url = url.object_store();
        context
            .runtime_env()
            .register_object_store(object_store_url.as_ref(), Arc::clone(&store));
        let plan = ArrowReadFormat
            .scan_plan(
                &context.state(),
                scan_input(&url, vec![object], Arc::clone(&schema)),
            )
            .await?;
        let batches = collect(plan, context.task_ctx()).await?;
        let [batch] = batches.as_slice() else {
            panic!("expected exactly one Arrow batch, got {}", batches.len());
        };
        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
        );
        let timestamps = batch.column(0).as_primitive::<TimestampMicrosecondType>();
        assert_eq!(timestamps.value(0), -3_723_000_000);
        Ok(())
    }

    #[tokio::test]
    async fn scans_different_physical_timezones_as_utc() -> Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let first = Path::from("first.arrow");
        let second = Path::from("second.arrow");
        store
            .put(&first, stream_bytes("+01:02:03", 1)?.into())
            .await?;
        store.put(&second, stream_bytes("UTC", 2)?.into()).await?;
        let objects = vec![store.head(&first).await?, store.head(&second).await?];
        let url = ListingTableUrl::parse("memory://bucket/")?;
        let samples = [ListingFileSample {
            url: &url,
            store: Arc::clone(&store),
            objects: objects.clone(),
        }];

        let context = SessionContext::new();
        let schema = ArrowReadFormat
            .infer_schema(
                &context.state(),
                &samples,
                CompressionTypeVariant::UNCOMPRESSED,
            )
            .await?;
        assert_eq!(
            schema.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
        );

        let object_store_url = url.object_store();
        context
            .runtime_env()
            .register_object_store(object_store_url.as_ref(), Arc::clone(&store));
        let partition_field = Arc::new(Field::new("source", DataType::Utf8, false));
        let mut input = scan_input(&url, objects.clone(), Arc::clone(&schema));
        input.file_groups = vec![FileGroup::new(vec![
            PartitionedFile::from(objects[0].clone())
                .with_partition_values(vec![ScalarValue::Utf8(Some("offset".to_string()))]),
            PartitionedFile::from(objects[1].clone())
                .with_partition_values(vec![ScalarValue::Utf8(Some("utc".to_string()))]),
        ])];
        input.schema = TableSchema::new(Arc::clone(&schema), vec![partition_field]);
        input.statistics = Statistics::new_unknown(input.schema.table_schema());
        input.projection = Some(vec![1, 0]);
        let plan = ArrowReadFormat.scan_plan(&context.state(), input).await?;
        let batches = collect(plan, context.task_ctx()).await?;
        assert!(batches.iter().all(|batch| {
            batch.schema().field(1).data_type()
                == &DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
        }));
        let mut rows = vec![];
        for batch in &batches {
            let sources = batch.column(0).as_string::<i32>();
            let timestamps = batch.column(1).as_primitive::<TimestampMicrosecondType>();
            for row in 0..batch.num_rows() {
                rows.push((sources.value(row).to_string(), timestamps.value(row)));
            }
        }
        rows.sort_unstable();
        assert_eq!(rows, [("offset".to_string(), 1), ("utc".to_string(), 2)]);
        Ok(())
    }

    #[tokio::test]
    async fn inference_rejects_mixed_ipc_file_and_stream_formats() -> Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let stream = Path::from("stream.arrow");
        let file = Path::from("file.arrow");
        store.put(&stream, stream_bytes("UTC", 1)?.into()).await?;
        store.put(&file, file_bytes("UTC", 2)?.into()).await?;
        let objects = vec![store.head(&stream).await?, store.head(&file).await?];
        let url = ListingTableUrl::parse("memory://bucket/")?;
        let context = SessionContext::new();
        let samples = [ListingFileSample {
            url: &url,
            store: Arc::clone(&store),
            objects,
        }];

        let result = ArrowReadFormat
            .infer_schema(
                &context.state(),
                &samples,
                CompressionTypeVariant::UNCOMPRESSED,
            )
            .await;
        let Err(error) = result else {
            panic!("mixed Arrow IPC formats should be rejected");
        };
        assert!(
            error
                .to_string()
                .contains("Mixed Arrow IPC file and stream formats are not supported in one scan"),
            "unexpected scan error: {error}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn schema_probe_rejects_truncated_ipc() -> Result<()> {
        let store = InMemory::new();

        let stream = Path::from("truncated-stream.arrow");
        store.put(&stream, vec![0xff].into()).await?;
        assert!(read_arrow_schema(&store, &stream, false).await.is_err());

        let file = Path::from("corrupt-file.arrow");
        let mut bytes = 100_u32.to_le_bytes().to_vec();
        bytes.extend_from_slice(&ARROW_MAGIC);
        store.put(&file, bytes.into()).await?;
        assert!(read_arrow_schema(&store, &file, true).await.is_err());
        Ok(())
    }
}
