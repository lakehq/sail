use std::collections::{HashMap, HashSet};
use std::fmt::Formatter;
use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::array::{ArrayRef, RecordBatch, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{Distribution, EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::{DataFusionError, Result, exec_err, internal_err, plan_err};
use futures::{StreamExt, TryStreamExt};
use log::warn;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt, PutPayload};
use sail_common_datafusion::listing_write::{
    ListingWriteFile, ListingWriteTaskManifest, decode_listing_write_manifests,
    listing_write_manifest_schema,
};
use sail_parquet::ParquetWriterExec;

#[derive(Debug, Clone)]
pub struct ListingWriteCommitExec {
    input: Arc<dyn ExecutionPlan>,
    object_store_url: ObjectStoreUrl,
    target_prefix: Path,
    staging_prefix: Path,
    write_id: String,
    overwrite: bool,
    expected_task_count: usize,
    properties: Arc<PlanProperties>,
}

impl ListingWriteCommitExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        object_store_url: ObjectStoreUrl,
        target_prefix: Path,
        staging_prefix: Path,
        write_id: String,
        overwrite: bool,
        expected_task_count: usize,
    ) -> Result<Self> {
        if input.schema() != listing_write_manifest_schema() {
            return plan_err!("listing write commit input has an invalid manifest schema");
        }
        if write_id.is_empty() {
            return plan_err!("listing write commit requires a write ID");
        }
        let expected_staging_prefix = target_prefix
            .clone()
            .join("_temporary")
            .join("sail")
            .join(write_id.as_str());
        if staging_prefix != expected_staging_prefix {
            return plan_err!(
                "listing write staging prefix {staging_prefix} does not match {expected_staging_prefix}"
            );
        }
        let expected_task_count = match parquet_writer_snapshot(&input)? {
            Some((writer_write_id, task_count)) => {
                if writer_write_id != write_id {
                    return plan_err!(
                        "listing write commit has write ID {write_id}, but its Parquet writer has {writer_write_id}"
                    );
                }
                task_count
            }
            None => expected_task_count,
        };
        if expected_task_count == 0 {
            return plan_err!("listing write commit requires at least one writer task");
        }
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(count_schema()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            object_store_url,
            target_prefix,
            staging_prefix,
            write_id,
            overwrite,
            expected_task_count,
            properties,
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn object_store_url(&self) -> &ObjectStoreUrl {
        &self.object_store_url
    }

    pub fn target_prefix(&self) -> &Path {
        &self.target_prefix
    }

    pub fn staging_prefix(&self) -> &Path {
        &self.staging_prefix
    }

    pub fn write_id(&self) -> &str {
        &self.write_id
    }

    pub fn overwrite(&self) -> bool {
        self.overwrite
    }

    pub fn expected_task_count(&self) -> usize {
        self.expected_task_count
    }
}

fn parquet_writer_snapshot(plan: &Arc<dyn ExecutionPlan>) -> Result<Option<(String, usize)>> {
    let mut snapshot = plan.downcast_ref::<ParquetWriterExec>().map(|writer| {
        (
            writer.write_id().to_string(),
            writer.properties().output_partitioning().partition_count(),
        )
    });
    for child in plan.children() {
        if let Some(child_snapshot) = parquet_writer_snapshot(child)? {
            if let Some(current) = &snapshot {
                if current != &child_snapshot {
                    return plan_err!(
                        "listing write commit input contains inconsistent Parquet writer snapshots"
                    );
                }
            } else {
                snapshot = Some(child_snapshot);
            }
        }
    }
    Ok(snapshot)
}

impl DisplayAs for ListingWriteCommitExec {
    fn fmt_as(&self, _display_type: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "ListingWriteCommitExec: output={}, mode={}, expected_tasks={}",
            self.target_prefix,
            if self.overwrite {
                "overwrite"
            } else {
                "append"
            },
            self.expected_task_count
        )
    }
}

impl ExecutionPlan for ListingWriteCommitExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else {
            return internal_err!("ListingWriteCommitExec requires exactly one child");
        };
        Ok(Arc::new(Self::try_new(
            Arc::clone(input),
            self.object_store_url.clone(),
            self.target_prefix.clone(),
            self.staging_prefix.clone(),
            self.write_id.clone(),
            self.overwrite,
            self.expected_task_count,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return exec_err!("ListingWriteCommitExec can only execute partition 0");
        }
        let input_partitions = self.input.output_partitioning().partition_count();
        if input_partitions != 1 {
            return exec_err!(
                "ListingWriteCommitExec requires one input partition, got {input_partitions}"
            );
        }
        let mut input = self.input.execute(0, Arc::clone(&context))?;
        let object_store_url = self.object_store_url.clone();
        let target_prefix = self.target_prefix.clone();
        let staging_prefix = self.staging_prefix.clone();
        let write_id = self.write_id.clone();
        let overwrite = self.overwrite;
        let expected_task_count = self.expected_task_count;
        let schema = self.schema();
        let stream_schema = Arc::clone(&schema);
        let output = futures::stream::once(async move {
            let mut manifests = Vec::with_capacity(expected_task_count);
            while let Some(batch) = input.next().await.transpose()? {
                manifests.extend(decode_listing_write_manifests(&batch)?);
            }
            let store = context.runtime_env().object_store(&object_store_url)?;
            let row_count = commit_listing_write(
                store,
                target_prefix,
                staging_prefix,
                write_id,
                overwrite,
                expected_task_count,
                manifests,
            )
            .await?;
            count_batch(row_count)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            output,
        )))
    }
}

#[derive(Debug)]
struct ValidatedWriteFile {
    staging_path: Path,
    final_path: Path,
    metadata: ListingWriteFile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilePublishState {
    Staged,
    Committed,
}

async fn commit_listing_write(
    store: Arc<dyn ObjectStore>,
    target_prefix: Path,
    staging_prefix: Path,
    write_id: String,
    overwrite: bool,
    expected_task_count: usize,
    manifests: Vec<ListingWriteTaskManifest>,
) -> Result<u64> {
    let (row_count, files, accepted_attempt_prefixes) = validate_manifests(
        &target_prefix,
        &staging_prefix,
        &write_id,
        expected_task_count,
        manifests,
    )?;

    let mut files_to_publish = Vec::new();
    for file in &files {
        if file_publish_state(store.as_ref(), file).await? == FilePublishState::Staged {
            files_to_publish.push(file);
        }
    }

    let final_paths = files
        .iter()
        .map(|file| file.final_path.clone())
        .collect::<HashSet<_>>();
    let old_paths = if overwrite {
        store
            .list(Some(&target_prefix))
            .try_filter_map(|object| {
                let keep = !final_paths.contains(&object.location)
                    && !is_temporary_path(&target_prefix, &object.location);
                futures::future::ready(Ok(keep.then_some(object.location)))
            })
            .try_collect::<Vec<_>>()
            .await?
    } else {
        Vec::new()
    };

    let success_path = target_prefix.clone().join("_SUCCESS");
    let had_success_marker = match store.head(&success_path).await {
        Ok(_) => true,
        Err(object_store::Error::NotFound { .. }) => false,
        Err(error) => return Err(DataFusionError::ObjectStore(Box::new(error))),
    };
    delete_paths(store.as_ref(), vec![success_path.clone()]).await?;

    let mut published_paths = Vec::with_capacity(files_to_publish.len());
    let publish_result = async {
        for file in files_to_publish {
            // Record the destination before the copy because an object-store error can be
            // ambiguous about whether a server-side copy became visible.
            published_paths.push(file.final_path.clone());
            store.copy(&file.staging_path, &file.final_path).await?;
            let committed = store.head(&file.final_path).await?;
            if committed.size != file.metadata.size {
                return exec_err!(
                    "committed listing file {} has size {}, expected {}",
                    file.final_path,
                    committed.size,
                    file.metadata.size
                );
            }
        }
        Ok(())
    }
    .await;
    if let Err(error) = publish_result {
        match delete_paths(store.as_ref(), published_paths).await {
            Ok(()) if had_success_marker => {
                if let Err(marker_error) = put_success_marker(store.as_ref(), &success_path).await {
                    warn!(
                        "failed to restore listing success marker {success_path}: {marker_error}"
                    );
                }
            }
            Ok(()) => {}
            Err(rollback_error) => {
                warn!("failed to roll back published listing files: {rollback_error}");
            }
        }
        return Err(error);
    }

    delete_paths(store.as_ref(), old_paths).await?;
    put_success_marker(store.as_ref(), &success_path).await?;
    for attempt_prefix in accepted_attempt_prefixes {
        if let Err(error) = delete_prefix(store.as_ref(), &attempt_prefix).await {
            warn!("failed to clean listing write staging path {attempt_prefix}: {error}");
        }
    }
    Ok(row_count)
}

fn validate_manifests(
    target_prefix: &Path,
    staging_prefix: &Path,
    write_id: &str,
    expected_task_count: usize,
    manifests: Vec<ListingWriteTaskManifest>,
) -> Result<(u64, Vec<ValidatedWriteFile>, Vec<Path>)> {
    if manifests.len() != expected_task_count {
        return exec_err!(
            "listing write produced {} task manifests, expected {expected_task_count}",
            manifests.len()
        );
    }
    let mut tasks = HashMap::with_capacity(manifests.len());
    let mut job_stage = None;
    let mut row_count = 0_u64;
    let mut final_paths = HashSet::new();
    let mut files = Vec::new();
    let mut accepted_attempt_prefixes = Vec::with_capacity(manifests.len());
    for manifest in manifests {
        if manifest.write_id != write_id {
            return exec_err!(
                "listing write manifest has write ID {}, expected {write_id}",
                manifest.write_id
            );
        }
        let partition = usize::try_from(manifest.partition)
            .map_err(|_| DataFusionError::Execution("writer partition is too large".to_string()))?;
        if partition >= expected_task_count {
            return exec_err!(
                "listing write manifest partition {partition} is outside expected task count {expected_task_count}"
            );
        }
        if tasks.insert(partition, manifest.attempt).is_some() {
            return exec_err!("duplicate listing write manifest for partition {partition}");
        }
        match job_stage {
            Some(expected) if expected != (manifest.job_id, manifest.stage) => {
                return exec_err!("listing write manifests came from different jobs or stages");
            }
            None => job_stage = Some((manifest.job_id, manifest.stage)),
            _ => {}
        }
        let file_rows = manifest.files.iter().try_fold(0_u64, |total, file| {
            total
                .checked_add(file.row_count)
                .ok_or_else(|| DataFusionError::Execution("file row count overflow".to_string()))
        })?;
        if file_rows != manifest.row_count {
            return exec_err!(
                "listing write task {} reported {} rows but its files contain {} rows",
                manifest.partition,
                manifest.row_count,
                file_rows
            );
        }
        row_count = row_count
            .checked_add(manifest.row_count)
            .ok_or_else(|| DataFusionError::Execution("write row count overflow".to_string()))?;
        let attempt_prefix = staging_prefix.clone().join(format!(
            "job-{}-stage-{}-part-{}-attempt-{}",
            manifest.job_id, manifest.stage, manifest.partition, manifest.attempt
        ));
        accepted_attempt_prefixes.push(attempt_prefix.clone());
        for file in manifest.files {
            let staging_path = Path::parse(&file.staging_path).map_err(|error| {
                DataFusionError::Execution(format!("invalid staging path: {error}"))
            })?;
            if !staging_path.prefix_matches(&attempt_prefix) {
                return exec_err!(
                    "listing staging file {staging_path} is outside task attempt path {attempt_prefix}"
                );
            }
            let relative = Path::parse(&file.final_relative_path).map_err(|error| {
                DataFusionError::Execution(format!("invalid final relative path: {error}"))
            })?;
            if relative.is_root() {
                return exec_err!("listing final relative path must name a file");
            }
            let mut final_path = target_prefix.clone();
            final_path.extend(relative.parts());
            if is_temporary_path(target_prefix, &final_path) {
                return exec_err!("listing final path must not be under _temporary");
            }
            if !final_paths.insert(final_path.clone()) {
                return exec_err!("duplicate listing final path {final_path}");
            }
            files.push(ValidatedWriteFile {
                staging_path,
                final_path,
                metadata: file,
            });
        }
    }
    for partition in 0..expected_task_count {
        if !tasks.contains_key(&partition) {
            return exec_err!("missing listing write manifest for partition {partition}");
        }
    }
    files.sort_by(|left, right| left.final_path.cmp(&right.final_path));
    accepted_attempt_prefixes.sort();
    Ok((row_count, files, accepted_attempt_prefixes))
}

async fn file_publish_state(
    store: &dyn ObjectStore,
    file: &ValidatedWriteFile,
) -> Result<FilePublishState> {
    let committed = match store.head(&file.final_path).await {
        Ok(object) if validate_object_size(&object, &file.metadata).is_ok() => {
            return Ok(FilePublishState::Committed);
        }
        Ok(object) => Some(object),
        Err(object_store::Error::NotFound { .. }) => None,
        Err(error) => return Err(DataFusionError::ObjectStore(Box::new(error))),
    };
    match store.head(&file.staging_path).await {
        Ok(object) => {
            validate_object_metadata(&object, &file.metadata)?;
            Ok(FilePublishState::Staged)
        }
        Err(object_store::Error::NotFound { .. }) => match committed {
            Some(object) => {
                validate_object_size(&object, &file.metadata)?;
                Ok(FilePublishState::Committed)
            }
            None => {
                let object = store.head(&file.final_path).await?;
                validate_object_size(&object, &file.metadata)?;
                Ok(FilePublishState::Committed)
            }
        },
        Err(error) => Err(DataFusionError::ObjectStore(Box::new(error))),
    }
}

fn validate_object_metadata(object: &ObjectMeta, expected: &ListingWriteFile) -> Result<()> {
    validate_object_size(object, expected)?;
    if let (Some(actual), Some(expected)) = (&object.e_tag, &expected.e_tag)
        && actual != expected
    {
        return exec_err!(
            "listing write file {} has an unexpected e-tag",
            object.location
        );
    }
    if let (Some(actual), Some(expected)) = (&object.version, &expected.version)
        && actual != expected
    {
        return exec_err!(
            "listing write file {} has an unexpected version",
            object.location
        );
    }
    Ok(())
}

fn validate_object_size(object: &ObjectMeta, expected: &ListingWriteFile) -> Result<()> {
    if object.size != expected.size {
        return exec_err!(
            "listing write file {} has size {}, expected {}",
            object.location,
            object.size,
            expected.size
        );
    }
    Ok(())
}

fn is_temporary_path(target_prefix: &Path, path: &Path) -> bool {
    path.prefix_match(target_prefix)
        .and_then(|mut parts| parts.next())
        .is_some_and(|part| part.as_ref() == "_temporary")
}

async fn delete_prefix(store: &dyn ObjectStore, prefix: &Path) -> Result<()> {
    let paths = store
        .list(Some(prefix))
        .map_ok(|object| object.location)
        .try_collect::<Vec<_>>()
        .await?;
    delete_paths(store, paths).await
}

pub async fn clean_up_listing_write_staging(
    context: &TaskContext,
    object_store_url: &ObjectStoreUrl,
    staging_prefix: &Path,
) -> Result<()> {
    let store = context.runtime_env().object_store(object_store_url)?;
    delete_prefix(store.as_ref(), staging_prefix).await
}

async fn delete_paths(store: &dyn ObjectStore, paths: Vec<Path>) -> Result<()> {
    for path in paths {
        match store.delete(&path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(DataFusionError::ObjectStore(Box::new(error))),
        }
    }
    Ok(())
}

async fn put_success_marker(store: &dyn ObjectStore, path: &Path) -> Result<()> {
    store
        .put(path, PutPayload::from(Bytes::new()))
        .await
        .map(|_| ())
        .map_err(|error| DataFusionError::ObjectStore(Box::new(error)))
}

fn count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![datafusion::arrow::datatypes::Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

fn count_batch(count: u64) -> Result<RecordBatch> {
    let values = Arc::new(UInt64Array::from(vec![count])) as ArrayRef;
    Ok(RecordBatch::try_from_iter_with_nullable(vec![(
        "count", values, false,
    )])?)
}

#[cfg(test)]
mod tests {
    use std::fmt::{Display, Formatter};

    use async_trait::async_trait;
    use datafusion::arrow::datatypes::Field;
    use datafusion::common::config::TableParquetOptions;
    use datafusion::datasource::listing::ListingTableUrl;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::logical_expr::dml::InsertOp;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion_datasource::file_sink_config::{FileOutputMode, FileSinkConfig};
    use futures::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, PutMultipartOptions,
        PutOptions, PutResult,
    };
    use sail_parquet::{ParquetWriteExecutionOptions, ParquetWriterExec};

    use super::*;

    async fn put(store: &InMemory, path: &str, value: &'static [u8]) -> Result<ObjectMeta> {
        let path = Path::parse(path)
            .map_err(|error| DataFusionError::Execution(format!("invalid test path: {error}")))?;
        store
            .put(&path, PutPayload::from(Bytes::from_static(value)))
            .await?;
        Ok(store.head(&path).await?)
    }

    fn manifest(meta: &ObjectMeta, final_relative_path: &str) -> ListingWriteTaskManifest {
        ListingWriteTaskManifest {
            write_id: "write-id".to_string(),
            job_id: 1,
            stage: 2,
            partition: 0,
            attempt: 3,
            row_count: 2,
            files: vec![ListingWriteFile {
                staging_path: meta.location.to_string(),
                final_relative_path: final_relative_path.to_string(),
                size: meta.size,
                row_count: 2,
                e_tag: meta.e_tag.clone(),
                version: meta.version.clone(),
            }],
        }
    }

    async fn exists(store: &InMemory, path: &str) -> bool {
        let Ok(path) = Path::parse(path) else {
            return false;
        };
        store.head(&path).await.is_ok()
    }

    #[derive(Debug)]
    struct FailingCopyStore {
        store: Arc<InMemory>,
        fail_destination: Path,
    }

    impl Display for FailingCopyStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            f.write_str("failing copy store")
        }
    }

    #[async_trait]
    impl ObjectStore for FailingCopyStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.store.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.store.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.store.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.store.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.store.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.store.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            if to == &self.fail_destination {
                return Err(object_store::Error::Generic {
                    store: "test",
                    source: Box::new(std::io::Error::other("injected copy failure")),
                });
            }
            self.store.copy_opts(from, to, options).await
        }
    }

    #[test]
    fn derives_writer_task_count_after_physical_repartitioning() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let input = MemorySourceConfig::try_new_exec(
            &[vec![], vec![], vec![], vec![]],
            Arc::clone(&schema),
            None,
        )?;
        let sink_config = FileSinkConfig {
            original_url: "memory:///table".to_string(),
            object_store_url: ObjectStoreUrl::parse("memory://")?,
            file_group: Default::default(),
            table_paths: vec![ListingTableUrl::parse("memory:///table")?],
            output_schema: schema,
            table_partition_cols: vec![],
            insert_op: InsertOp::Append,
            keep_partition_by_columns: false,
            file_extension: "parquet".to_string(),
            file_output_mode: FileOutputMode::Automatic,
        };
        let context = TaskContext::default();
        let writer = ParquetWriterExec::try_new_with_write_id(
            input,
            sink_config,
            TableParquetOptions::default(),
            ParquetWriteExecutionOptions::from(&context.session_config().options().execution),
            None,
            "write-id".to_string(),
        )?;
        let commit = ListingWriteCommitExec::try_new(
            Arc::new(CoalescePartitionsExec::new(Arc::new(writer))),
            ObjectStoreUrl::parse("memory://")?,
            Path::from("table"),
            Path::from("table/_temporary/sail/write-id"),
            "write-id".to_string(),
            false,
            1,
        )?;

        assert_eq!(commit.expected_task_count(), 4);
        Ok(())
    }

    #[tokio::test]
    async fn append_publishes_files_success_marker_and_cleans_staging() -> Result<()> {
        let store = Arc::new(InMemory::new());
        put(store.as_ref(), "table/old.parquet", b"old").await?;
        let staged = put(
            store.as_ref(),
            "table/_temporary/sail/write-id/job-1-stage-2-part-0-attempt-3/p=a%2Fb/part.parquet",
            b"parquet",
        )
        .await?;
        let task_manifest = manifest(&staged, "p=a%2Fb/part.parquet");

        let rows = commit_listing_write(
            store.clone(),
            Path::from("table"),
            Path::from("table/_temporary/sail/write-id"),
            "write-id".to_string(),
            false,
            1,
            vec![task_manifest.clone()],
        )
        .await?;

        assert_eq!(rows, 2);
        assert!(exists(store.as_ref(), "table/old.parquet").await);
        assert!(exists(store.as_ref(), "table/p=a%2Fb/part.parquet").await);
        assert!(exists(store.as_ref(), "table/_SUCCESS").await);
        assert!(
            store
                .list(Some(&Path::from("table/_temporary/sail/write-id")))
                .next()
                .await
                .is_none()
        );

        // A retried driver attempt accepts already-published files after staging cleanup.
        commit_listing_write(
            store.clone(),
            Path::from("table"),
            Path::from("table/_temporary/sail/write-id"),
            "write-id".to_string(),
            false,
            1,
            vec![task_manifest],
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn overwrite_deletes_old_files_only_after_new_files_are_available() -> Result<()> {
        let store = Arc::new(InMemory::new());
        put(store.as_ref(), "table/old.parquet", b"old").await?;
        let staged = put(
            store.as_ref(),
            "table/_temporary/sail/write-id/job-1-stage-2-part-0-attempt-3/part.parquet",
            b"new parquet",
        )
        .await?;

        commit_listing_write(
            store.clone(),
            Path::from("table"),
            Path::from("table/_temporary/sail/write-id"),
            "write-id".to_string(),
            true,
            1,
            vec![manifest(&staged, "part.parquet")],
        )
        .await?;

        assert!(!exists(store.as_ref(), "table/old.parquet").await);
        assert!(exists(store.as_ref(), "table/part.parquet").await);
        assert!(exists(store.as_ref(), "table/_SUCCESS").await);
        Ok(())
    }

    #[tokio::test]
    async fn missing_staging_file_does_not_delete_existing_output() -> Result<()> {
        let store = Arc::new(InMemory::new());
        put(store.as_ref(), "table/old.parquet", b"old").await?;
        let missing = ObjectMeta {
            location: Path::from(
                "table/_temporary/sail/write-id/job-1-stage-2-part-0-attempt-3/missing.parquet",
            ),
            last_modified: chrono::Utc::now(),
            size: 7,
            e_tag: None,
            version: None,
        };

        let result = commit_listing_write(
            store.clone(),
            Path::from("table"),
            Path::from("table/_temporary/sail/write-id"),
            "write-id".to_string(),
            true,
            1,
            vec![manifest(&missing, "part.parquet")],
        )
        .await;

        assert!(result.is_err());
        assert!(exists(store.as_ref(), "table/old.parquet").await);
        assert!(!exists(store.as_ref(), "table/part.parquet").await);
        assert!(!exists(store.as_ref(), "table/_SUCCESS").await);
        Ok(())
    }

    #[tokio::test]
    async fn publish_failure_rolls_back_new_files_and_restores_success_marker() -> Result<()> {
        let store = Arc::new(InMemory::new());
        put(store.as_ref(), "table/old.parquet", b"old").await?;
        put(store.as_ref(), "table/_SUCCESS", b"").await?;
        let first = put(
            store.as_ref(),
            "table/_temporary/sail/write-id/job-1-stage-2-part-0-attempt-3/part-1.parquet",
            b"first",
        )
        .await?;
        let second = put(
            store.as_ref(),
            "table/_temporary/sail/write-id/job-1-stage-2-part-0-attempt-3/part-2.parquet",
            b"second",
        )
        .await?;
        let mut task_manifest = manifest(&first, "part-1.parquet");
        task_manifest.row_count = 4;
        task_manifest.files.push(ListingWriteFile {
            staging_path: second.location.to_string(),
            final_relative_path: "part-2.parquet".to_string(),
            size: second.size,
            row_count: 2,
            e_tag: second.e_tag,
            version: second.version,
        });
        let failing: Arc<dyn ObjectStore> = Arc::new(FailingCopyStore {
            store: Arc::clone(&store),
            fail_destination: Path::from("table/part-2.parquet"),
        });

        let result = commit_listing_write(
            failing,
            Path::from("table"),
            Path::from("table/_temporary/sail/write-id"),
            "write-id".to_string(),
            true,
            1,
            vec![task_manifest],
        )
        .await;

        assert!(result.is_err());
        assert!(exists(store.as_ref(), "table/old.parquet").await);
        assert!(exists(store.as_ref(), "table/_SUCCESS").await);
        assert!(!exists(store.as_ref(), "table/part-1.parquet").await);
        assert!(!exists(store.as_ref(), "table/part-2.parquet").await);
        Ok(())
    }

    #[tokio::test]
    async fn terminal_cleanup_removes_all_write_attempts() -> Result<()> {
        let store = Arc::new(InMemory::new());
        put(
            store.as_ref(),
            "table/_temporary/sail/write-id/job-1-stage-2-part-0-attempt-3/part.parquet",
            b"accepted",
        )
        .await?;
        put(
            store.as_ref(),
            "table/_temporary/sail/write-id/job-1-stage-2-part-0-attempt-4/part.parquet",
            b"orphan",
        )
        .await?;
        let context = TaskContext::default();
        let object_store_url = ObjectStoreUrl::parse("memory://")?;
        context
            .runtime_env()
            .register_object_store(object_store_url.as_ref(), store.clone());

        clean_up_listing_write_staging(
            &context,
            &object_store_url,
            &Path::from("table/_temporary/sail/write-id"),
        )
        .await?;

        assert!(
            store
                .list(Some(&Path::from("table/_temporary/sail/write-id")))
                .next()
                .await
                .is_none()
        );
        Ok(())
    }

    #[test]
    fn rejects_file_from_a_different_task_attempt() {
        let file = ObjectMeta {
            location: Path::from(
                "table/_temporary/sail/write-id/job-1-stage-2-part-0-attempt-4/part.parquet",
            ),
            last_modified: chrono::Utc::now(),
            size: 7,
            e_tag: None,
            version: None,
        };

        let result = validate_manifests(
            &Path::from("table"),
            &Path::from("table/_temporary/sail/write-id"),
            "write-id",
            1,
            vec![manifest(&file, "part.parquet")],
        );

        assert!(result.is_err());
    }
}
