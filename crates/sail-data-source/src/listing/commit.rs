// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet};
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, RecordBatch, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{Distribution, EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion_common::cast::{as_string_array, as_uint64_array};
use datafusion_common::{DataFusionError, Result, exec_err, internal_datafusion_err, internal_err};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt, stream};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use sail_common_datafusion::error::InputIncompleteError;
use uuid::Uuid;

use crate::listing::part_file::parse_part_file_name;

/// The name of the empty file that marks a completed write.
const SUCCESS_FILE_NAME: &str = "_SUCCESS";

/// A data file that a writer task has completed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrittenFile {
    pub location: Path,
    pub size: u64,
    pub num_rows: u64,
}

/// The schema of the manifest that each writer task returns.
/// The manifest has one row per completed file.
/// A task that completed no file returns a single row with a null path.
/// The row is required since the commit must see every writer partition,
/// and an exchange between the writers and the commit drops empty batches.
pub fn manifest_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("partition", DataType::UInt64, false),
        Field::new("partitions", DataType::UInt64, false),
        Field::new("attempt", DataType::UInt64, false),
        Field::new("path", DataType::Utf8, true),
        Field::new("size", DataType::UInt64, false),
        Field::new("num_rows", DataType::UInt64, false),
    ]))
}

/// Builds the manifest of the writer task for one of the writer partitions.
pub fn manifest_batch(
    partition: usize,
    partitions: usize,
    attempt: usize,
    files: &[WrittenFile],
) -> Result<RecordBatch> {
    let rows = files.len().max(1);
    let mut paths: Vec<Option<String>> = files
        .iter()
        .map(|file| Some(file.location.to_string()))
        .collect();
    paths.resize(rows, None);
    let mut sizes: Vec<u64> = files.iter().map(|file| file.size).collect();
    sizes.resize(rows, 0);
    let mut num_rows: Vec<u64> = files.iter().map(|file| file.num_rows).collect();
    num_rows.resize(rows, 0);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(vec![partition as u64; rows])),
        Arc::new(UInt64Array::from(vec![partitions as u64; rows])),
        Arc::new(UInt64Array::from(vec![attempt as u64; rows])),
        Arc::new(StringArray::from(paths)),
        Arc::new(UInt64Array::from(sizes)),
        Arc::new(UInt64Array::from(num_rows)),
    ];
    Ok(RecordBatch::try_new(manifest_schema(), columns)?)
}

/// The manifests of all writer tasks of a write.
#[derive(Debug, Default)]
struct Manifests {
    /// The number of writer partitions.
    partitions: Option<u64>,
    /// The attempt number of each writer partition.
    attempts: HashMap<u64, u64>,
    /// The partitions that completed no file.
    empty_partitions: HashSet<u64>,
    files: HashSet<Path>,
    num_rows: u64,
}

impl Manifests {
    fn add(&mut self, manifest: &RecordBatch) -> Result<()> {
        let partition = as_uint64_array(manifest.column(0))?;
        let partitions = as_uint64_array(manifest.column(1))?;
        let attempt = as_uint64_array(manifest.column(2))?;
        let path = as_string_array(manifest.column(3))?;
        let num_rows = as_uint64_array(manifest.column(5))?;
        for i in 0..manifest.num_rows() {
            let partition = partition.value(i);
            let partitions = partitions.value(i);
            let attempt = attempt.value(i);
            if *self.partitions.get_or_insert(partitions) != partitions {
                return internal_err!("writer tasks disagree on the number of partitions");
            }
            let known = self.attempts.insert(partition, attempt);
            if known.is_some_and(|known| known != attempt) {
                return internal_err!("writer partition {partition} has more than one attempt");
            }
            // Each task returns its manifest once, so a partition has either
            // one row without a file or one row for each of its files.
            let duplicate = if path.is_valid(i) {
                let path = Path::parse(path.value(i))
                    .map_err(|e| internal_datafusion_err!("invalid path in the manifest: {e}"))?;
                self.empty_partitions.contains(&partition) || !self.files.insert(path)
            } else {
                known.is_some() || !self.empty_partitions.insert(partition)
            };
            if duplicate {
                return internal_err!("writer partition {partition} has more than one manifest");
            }
            self.num_rows += num_rows.value(i);
        }
        Ok(())
    }

    /// Checks that the manifests of all writer partitions were received.
    /// The end of the input does not show this, since the output of a failed
    /// or canceled writer task can end in the same way as a complete output.
    ///
    /// The error says that the input is incomplete, so that the error of a writer
    /// task that has failed is reported for the write instead of this error.
    fn validate(&self) -> Result<()> {
        let incomplete =
            |message: String| DataFusionError::External(Box::new(InputIncompleteError(message)));
        let Some(partitions) = self.partitions else {
            return Err(incomplete(
                "the write is incomplete: no writer task has reported its files".to_string(),
            ));
        };
        let received = self.attempts.len() as u64;
        if received != partitions || self.attempts.keys().any(|x| *x >= partitions) {
            return Err(incomplete(format!(
                "the write is incomplete: {received} of {partitions} writer tasks have reported their files"
            )));
        }
        Ok(())
    }

    fn max_attempt(&self) -> u64 {
        self.attempts.values().copied().max().unwrap_or(0)
    }
}

/// Deletes the files. A file that does not exist is not an error.
pub(crate) async fn delete_files(store: &Arc<dyn ObjectStore>, files: Vec<Path>) -> Result<()> {
    delete_file_stream(store, stream::iter(files.into_iter().map(Ok)).boxed()).await
}

/// Deletes the files of the stream. A file that does not exist is not an error.
async fn delete_file_stream(
    store: &Arc<dyn ObjectStore>,
    files: BoxStream<'static, object_store::Result<Path>>,
) -> Result<()> {
    let mut deleted = store.delete_stream(files);
    while let Some(result) = deleted.next().await {
        match result {
            Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

/// Completes a write and returns the number of rows written.
///
/// A write that overwrites replaces the content of the table directory when
/// all writer tasks have completed their files. The files that were in the
/// directory before the write are kept until then, so that a write that fails
/// does not remove them. The `_SUCCESS` file of the earlier content is removed
/// before the writer tasks start, so that the file is present only when
/// the directory has the files of exactly one completed write.
async fn commit_file_write(
    open_input: impl FnOnce() -> Result<SendableRecordBatchStream>,
    store: Arc<dyn ObjectStore>,
    table_path: Path,
    write_id: Uuid,
    overwrite: bool,
) -> Result<u64> {
    let success_file = table_path.clone().join(SUCCESS_FILE_NAME);
    if overwrite {
        delete_files(&store, vec![success_file.clone()]).await?;
    }
    let mut input = open_input()?;
    let mut manifests = Manifests::default();
    let mut result = Ok(());
    while let Some(manifest) = input.next().await {
        result = manifest.and_then(|manifest| manifests.add(&manifest));
        if result.is_err() {
            break;
        }
    }
    let result = result.and_then(|()| manifests.validate());
    if let Err(e) = result {
        // Stop the writer tasks that are still running, and remove the files
        // of the tasks that have completed. A writer task removes its own files
        // unless its manifest was delivered, so the commit is only responsible
        // for the files in the manifests that it has received. These files have
        // names that no other attempt uses, so a later attempt of the write is
        // not affected.
        drop(input);
        let files = manifests.files.into_iter().collect();
        if let Err(cleanup) = delete_files(&store, files).await {
            log::warn!("failed to delete the files of a failed write to {table_path}: {cleanup}");
        }
        return Err(e);
    }

    // The write has all its files from here on, and none of them is deleted
    // if the rest of the commit fails.
    //
    // The write has files that are not in a manifest only if a writer task was
    // attempted more than once. These are the files whose name has another
    // attempt than the attempt that the writer partition has reported.
    // The files of later attempts are kept, since such an attempt may be
    // running when this attempt of the commit was canceled but has not
    // stopped yet. A write that overwrites also removes everything that
    // is not a file of the write.
    //
    // The files of the write are recognized by their names and not by the
    // locations in the manifests, so that a file of the write is kept even
    // when the object store lists its location in another form than the form
    // in which the writer task has reported it.
    let max_attempt = manifests.max_attempt();
    if overwrite || max_attempt > 0 {
        let attempts = std::mem::take(&mut manifests.attempts);
        let files = store
            .list(Some(&table_path))
            .try_filter_map(move |meta| {
                let name = meta
                    .location
                    .filename()
                    .and_then(parse_part_file_name)
                    .filter(|name| name.write_id == write_id);
                let delete = match name {
                    Some(name) => {
                        let attempt = name.attempt as u64;
                        attempt <= max_attempt
                            && attempts.get(&(name.partition as u64)) != Some(&attempt)
                    }
                    None => overwrite,
                };
                futures::future::ready(Ok(delete.then_some(meta.location)))
            })
            .boxed();
        delete_file_stream(&store, files).await?;
    }

    store.put(&success_file, PutPayload::default()).await?;
    Ok(manifests.num_rows)
}

/// Completes a listing table write after all writer tasks have finished.
///
/// The input is the manifests of the writer tasks.
/// The files of failed or superseded writer task attempts are deleted,
/// and then an empty `_SUCCESS` file is written to the table directory.
/// A write that overwrites also deletes everything else in the table directory
/// that is not a file of the write, before the `_SUCCESS` file is written.
/// The output is a single row with the number of rows written.
#[derive(Debug)]
pub struct FileWriteCommitExec {
    input: Arc<dyn ExecutionPlan>,
    object_store_url: ObjectStoreUrl,
    table_path: Path,
    write_id: Uuid,
    /// Whether the write replaces the content of the table directory.
    overwrite: bool,
    properties: Arc<PlanProperties>,
}

impl FileWriteCommitExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        object_store_url: ObjectStoreUrl,
        table_path: Path,
        write_id: Uuid,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(count_schema()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            input,
            object_store_url,
            table_path,
            write_id,
            overwrite: false,
            properties,
        }
    }

    /// Sets whether the write replaces the content of the table directory.
    pub fn with_overwrite(mut self, overwrite: bool) -> Self {
        self.overwrite = overwrite;
        self
    }

    pub fn overwrite(&self) -> bool {
        self.overwrite
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn object_store_url(&self) -> &ObjectStoreUrl {
        &self.object_store_url
    }

    pub fn table_path(&self) -> &Path {
        &self.table_path
    }

    pub fn write_id(&self) -> &Uuid {
        &self.write_id
    }
}

/// The schema of the commit output, which is the same as the output schema
/// of the DataFusion data sink.
fn count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

impl DisplayAs for FileWriteCommitExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}: path={}", self.name(), self.table_path)?;
        if self.overwrite {
            write!(f, ", overwrite")?;
        }
        Ok(())
    }
}

impl ExecutionPlan for FileWriteCommitExec {
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

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    #[expect(deprecated)]
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: datafusion::physical_plan::ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_new_children(children)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else {
            return internal_err!("{} must have exactly one child", self.name());
        };
        Ok(Arc::new(
            Self::new(
                Arc::clone(input),
                self.object_store_url.clone(),
                self.table_path.clone(),
                self.write_id,
            )
            .with_overwrite(self.overwrite),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return exec_err!(
                "{} expects only partition 0 but got {}",
                self.name(),
                partition
            );
        }
        let input = Arc::clone(&self.input);
        let store = context.runtime_env().object_store(&self.object_store_url)?;
        let table_path = self.table_path.clone();
        let write_id = self.write_id;
        let overwrite = self.overwrite;
        let schema = self.schema();
        let output = stream::once(async move {
            // The writer tasks can start when the input is executed, which must
            // not be before the commit has prepared the table directory.
            let open_input = move || input.execute(0, context);
            let count =
                commit_file_write(open_input, store, table_path, write_id, overwrite).await?;
            let count: ArrayRef = Arc::new(UInt64Array::from(vec![count]));
            Ok(RecordBatch::try_new(count_schema(), vec![count])?)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, output)))
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Display;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        PutMultipartOptions, PutOptions, PutResult,
    };

    use super::*;
    use crate::listing::part_file::part_file_name;

    const WRITE_ID: Uuid = Uuid::from_u128(1);
    const OTHER_WRITE_ID: Uuid = Uuid::from_u128(2);

    /// An object store that records the requests it receives,
    /// and that fails one of the requests when it is told to.
    #[derive(Debug)]
    struct RecordingStore {
        inner: InMemory,
        requests: Arc<Mutex<Vec<String>>>,
        failure: Arc<Mutex<Option<(&'static str, usize)>>>,
    }

    impl RecordingStore {
        fn new() -> Self {
            Self {
                inner: InMemory::new(),
                requests: Arc::new(Mutex::new(vec![])),
                failure: Arc::new(Mutex::new(None)),
            }
        }
    }

    fn record(requests: &Mutex<Vec<String>>, request: String) {
        if let Ok(mut requests) = requests.lock() {
            requests.push(request);
        }
    }

    /// Returns an error if the request of the kind is the request that fails.
    fn fail(
        failure: &Mutex<Option<(&'static str, usize)>>,
        kind: &'static str,
    ) -> object_store::Result<()> {
        let Ok(mut failure) = failure.lock() else {
            return Ok(());
        };
        match failure.as_mut() {
            Some((failing, remaining)) if *failing == kind => {
                if *remaining > 0 {
                    *remaining -= 1;
                    return Ok(());
                }
            }
            _ => return Ok(()),
        }
        *failure = None;
        Err(object_store::Error::Generic {
            store: "RecordingStore",
            source: format!("{kind} failure").into(),
        })
    }

    impl Display for RecordingStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "RecordingStore")
        }
    }

    #[async_trait]
    impl ObjectStore for RecordingStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            record(
                &self.requests,
                format!("PUT {location} {}", payload.content_length()),
            );
            fail(&self.failure, "PUT")?;
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            let requests = Arc::clone(&self.requests);
            let failure = Arc::clone(&self.failure);
            let locations = locations
                .map(move |location| {
                    let location = location?;
                    record(&requests, format!("DELETE {location}"));
                    fail(&failure, "DELETE")?;
                    Ok(location)
                })
                .boxed();
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            let prefix_text = prefix.map(|x| x.to_string()).unwrap_or_default();
            record(&self.requests, format!("LIST {prefix_text}"));
            if let Err(e) = fail(&self.failure, "LIST") {
                return stream::once(async move { Err(e) }).boxed();
            }
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    struct TestWrite {
        recording: Arc<RecordingStore>,
        store: Arc<dyn ObjectStore>,
    }

    impl TestWrite {
        fn new() -> Self {
            let recording = Arc::new(RecordingStore::new());
            let store: Arc<dyn ObjectStore> = recording.clone();
            Self { recording, store }
        }

        /// Creates a file without recording the request.
        async fn create(&self, path: &str) -> Result<WrittenFile> {
            let location = Path::from(path);
            self.recording
                .inner
                .put(&location, Bytes::from_static(b"data").into())
                .await?;
            Ok(WrittenFile {
                location,
                size: 4,
                num_rows: 10,
            })
        }

        async fn commit(&self, manifests: Vec<Result<RecordBatch>>) -> Result<u64> {
            self.commit_with(manifests, false).await
        }

        async fn commit_with(
            &self,
            manifests: Vec<Result<RecordBatch>>,
            overwrite: bool,
        ) -> Result<u64> {
            let requests = Arc::clone(&self.recording.requests);
            let open_input = move || {
                record(&requests, "INPUT".to_string());
                let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
                    manifest_schema(),
                    stream::iter(manifests),
                ));
                Ok(input)
            };
            commit_file_write(
                open_input,
                Arc::clone(&self.store),
                Path::from("table"),
                WRITE_ID,
                overwrite,
            )
            .await
        }

        /// Makes a later request of the kind fail,
        /// after the given number of requests of the kind.
        fn fail(&self, kind: &'static str, after: usize) {
            if let Ok(mut failure) = self.recording.failure.lock() {
                *failure = Some((kind, after));
            }
        }

        /// The requests to the object store, and `INPUT` for the point
        /// at which the commit has started the writer tasks.
        fn requests_and_input(&self) -> Vec<String> {
            self.recording
                .requests
                .lock()
                .map(|x| x.clone())
                .unwrap_or_default()
        }

        fn requests(&self) -> Vec<String> {
            let mut requests = self.requests_and_input();
            requests.retain(|x| x != "INPUT");
            requests
        }

        async fn files(&self) -> Result<Vec<String>> {
            let mut files: Vec<_> = self
                .recording
                .inner
                .list(None)
                .map_ok(|meta| meta.location.to_string())
                .try_collect()
                .await?;
            files.sort();
            Ok(files)
        }
    }

    fn name(partition: usize, write_id: &Uuid, counter: u32, attempt: usize) -> String {
        part_file_name(partition, write_id, counter, attempt, ".parquet")
    }

    #[test]
    fn test_manifest_batch() -> Result<()> {
        let files = vec![
            WrittenFile {
                location: Path::from("table/a"),
                size: 1,
                num_rows: 2,
            },
            WrittenFile {
                location: Path::from("table/b"),
                size: 3,
                num_rows: 4,
            },
        ];
        let manifest = manifest_batch(5, 8, 6, &files)?;
        assert_eq!(manifest.schema(), manifest_schema());
        assert_eq!(manifest.num_rows(), 2);
        let mut manifests = Manifests::default();
        manifests.add(&manifest)?;
        manifests.add(&manifest_batch(7, 8, 0, &[])?)?;
        assert_eq!(manifests.attempts, HashMap::from([(5, 6), (7, 0)]));
        assert_eq!(
            manifests.files,
            files.into_iter().map(|x| x.location).collect()
        );
        assert_eq!(manifests.empty_partitions, HashSet::from([7]));
        assert_eq!(manifests.num_rows, 6);
        assert_eq!(manifests.max_attempt(), 6);
        assert_eq!(manifests.partitions, Some(8));
        assert!(manifests.validate().is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_commit_without_retry_does_not_list() -> Result<()> {
        let write = TestWrite::new();
        let a = write
            .create(&format!("table/{}", name(0, &WRITE_ID, 0, 0)))
            .await?;
        let b = write
            .create(&format!("table/{}", name(0, &WRITE_ID, 1, 0)))
            .await?;
        let c = write
            .create(&format!("table/x=1/{}", name(2, &WRITE_ID, 0, 0)))
            .await?;
        // This file is not in a manifest, but no task was retried,
        // so the directory is not inspected.
        write
            .create(&format!("table/{}", name(9, &WRITE_ID, 0, 0)))
            .await?;
        let count = write
            .commit(vec![
                manifest_batch(0, 3, 0, &[a, b]),
                manifest_batch(1, 3, 0, &[]),
                manifest_batch(2, 3, 0, &[c]),
            ])
            .await?;
        assert_eq!(count, 30);
        assert_eq!(write.requests(), vec!["PUT table/_SUCCESS 0"]);
        assert_eq!(write.files().await?.len(), 5);
        Ok(())
    }

    #[tokio::test]
    async fn test_commit_of_empty_write() -> Result<()> {
        let write = TestWrite::new();
        let count = write.commit(vec![manifest_batch(0, 1, 0, &[])]).await?;
        assert_eq!(count, 0);
        assert_eq!(write.requests(), vec!["PUT table/_SUCCESS 0"]);
        assert_eq!(write.files().await?, vec!["table/_SUCCESS"]);
        Ok(())
    }

    #[tokio::test]
    async fn test_commit_after_retry_deletes_stale_files() -> Result<()> {
        let write = TestWrite::new();
        let stale = [
            format!("table/{}", name(0, &WRITE_ID, 0, 0)),
            format!("table/{}", name(1, &WRITE_ID, 0, 0)),
            format!("table/{}", name(1, &WRITE_ID, 1, 1)),
            format!("table/x=1/y=2/{}", name(0, &WRITE_ID, 0, 0)),
        ];
        for path in &stale {
            write.create(path).await?;
        }
        let kept = [
            // a later attempt that may still be running
            format!("table/{}", name(0, &WRITE_ID, 0, 3)),
            format!("table/{}", name(0, &OTHER_WRITE_ID, 0, 0)),
            format!("table/{}", name(0, &OTHER_WRITE_ID, 0, 1)),
            "table/data.parquet".to_string(),
            format!("other/{}", name(0, &WRITE_ID, 0, 0)),
        ];
        for path in &kept {
            write.create(path).await?;
        }
        let a = write
            .create(&format!("table/{}", name(0, &WRITE_ID, 0, 2)))
            .await?;
        let b = write
            .create(&format!("table/x=1/y=2/{}", name(0, &WRITE_ID, 0, 2)))
            .await?;
        // The task of this partition was not retried.
        let c = write
            .create(&format!("table/{}", name(2, &WRITE_ID, 0, 0)))
            .await?;
        let committed = [a.clone(), b.clone(), c.clone()];
        let count = write
            .commit(vec![
                manifest_batch(0, 3, 2, &[a, b]),
                manifest_batch(1, 3, 2, &[]),
                manifest_batch(2, 3, 0, &[c]),
            ])
            .await?;
        assert_eq!(count, 30);

        let requests = write.requests();
        assert_eq!(requests.first().map(|x| x.as_str()), Some("LIST table"));
        assert_eq!(
            requests.last().map(|x| x.as_str()),
            Some("PUT table/_SUCCESS 0")
        );
        let mut deleted = requests[1..requests.len() - 1].to_vec();
        deleted.sort();
        let mut expected: Vec<_> = stale.iter().map(|x| format!("DELETE {x}")).collect();
        expected.sort();
        assert_eq!(deleted, expected);

        let mut expected: Vec<_> = kept
            .iter()
            .cloned()
            .chain(committed.iter().map(|x| x.location.to_string()))
            .chain(["table/_SUCCESS".to_string()])
            .collect();
        expected.sort();
        assert_eq!(write.files().await?, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_commit_input_error_deletes_received_files() -> Result<()> {
        let write = TestWrite::new();
        let a = write
            .create(&format!("table/{}", name(0, &WRITE_ID, 0, 1)))
            .await?;
        let other = format!("table/{}", name(1, &WRITE_ID, 0, 1));
        write.create(&other).await?;
        let result = write
            .commit(vec![
                manifest_batch(0, 2, 1, std::slice::from_ref(&a)),
                exec_err!("writer failure"),
                manifest_batch(1, 2, 1, &[]),
            ])
            .await;
        assert!(matches!(&result, Err(e) if e.to_string().contains("writer failure")));
        assert_eq!(write.requests(), vec![format!("DELETE {}", a.location)]);
        assert_eq!(write.files().await?, vec![other]);
        Ok(())
    }

    #[tokio::test]
    async fn test_commit_rejects_missing_manifests() -> Result<()> {
        // The input ends without an error, but one writer partition has not reported.
        let write = TestWrite::new();
        let a = write
            .create(&format!("table/{}", name(0, &WRITE_ID, 0, 0)))
            .await?;
        let result = write
            .commit(vec![
                manifest_batch(0, 3, 0, std::slice::from_ref(&a)),
                manifest_batch(2, 3, 0, &[]),
            ])
            .await;
        assert!(
            matches!(&result, Err(e) if e.to_string().contains("2 of 3 writer tasks")),
            "{result:?}"
        );
        // The error says that the input is incomplete, so that the error
        // of a failed writer task is reported for the write instead.
        let is_incomplete = |result: &Result<u64>| matches!(result, Err(DataFusionError::External(e)) if e.is::<InputIncompleteError>());
        assert!(is_incomplete(&result), "{result:?}");
        // The files that the commit has received are deleted.
        assert_eq!(write.requests(), vec![format!("DELETE {}", a.location)]);
        assert_eq!(write.files().await?, Vec::<String>::new());

        // no manifest at all
        let write = TestWrite::new();
        let result = write.commit(vec![]).await;
        assert!(
            matches!(&result, Err(e) if e.to_string().contains("no writer task has reported")),
            "{result:?}"
        );
        assert!(is_incomplete(&result), "{result:?}");
        assert_eq!(write.files().await?, Vec::<String>::new());

        // a partition index outside the range
        let write = TestWrite::new();
        let result = write
            .commit(vec![
                manifest_batch(0, 2, 0, &[]),
                manifest_batch(2, 2, 0, &[]),
            ])
            .await;
        assert!(
            matches!(&result, Err(e) if e.to_string().contains("the write is incomplete")),
            "{result:?}"
        );

        // different numbers of partitions
        let write = TestWrite::new();
        let result = write
            .commit(vec![
                manifest_batch(0, 2, 0, &[]),
                manifest_batch(1, 3, 0, &[]),
            ])
            .await;
        assert!(
            matches!(&result, Err(e) if e.to_string().contains("disagree on the number of partitions")),
            "{result:?}"
        );
        assert!(!is_incomplete(&result), "{result:?}");
        assert_eq!(write.files().await?, Vec::<String>::new());
        Ok(())
    }

    #[tokio::test]
    async fn test_commit_rejects_inconsistent_manifests() -> Result<()> {
        async fn file(write: &TestWrite, counter: u32, attempt: usize) -> Result<WrittenFile> {
            let path = format!("table/{}", name(0, &WRITE_ID, counter, attempt));
            write.create(&path).await
        }

        // two attempts of the same partition
        let write = TestWrite::new();
        let a = file(&write, 0, 0).await?;
        let b = file(&write, 0, 1).await?;
        let result = write
            .commit(vec![
                manifest_batch(0, 1, 0, &[a]),
                manifest_batch(0, 1, 1, &[b]),
            ])
            .await;
        assert!(
            matches!(&result, Err(e) if e.to_string().contains("more than one attempt")),
            "{result:?}"
        );
        assert!(!write.files().await?.contains(&"table/_SUCCESS".to_string()));

        // the same manifest twice
        let write = TestWrite::new();
        let a = file(&write, 0, 0).await?;
        let result = write
            .commit(vec![
                manifest_batch(0, 1, 0, std::slice::from_ref(&a)),
                manifest_batch(0, 1, 0, &[a]),
            ])
            .await;
        assert!(
            matches!(&result, Err(e) if e.to_string().contains("more than one manifest")),
            "{result:?}"
        );

        // the same manifest twice for a partition without files
        let write = TestWrite::new();
        let result = write
            .commit(vec![
                manifest_batch(0, 1, 0, &[]),
                manifest_batch(0, 1, 0, &[]),
            ])
            .await;
        assert!(
            matches!(&result, Err(e) if e.to_string().contains("more than one manifest")),
            "{result:?}"
        );

        // a manifest with files and a manifest without files
        let write = TestWrite::new();
        let a = file(&write, 0, 0).await?;
        for manifests in [
            vec![
                manifest_batch(0, 1, 0, std::slice::from_ref(&a)),
                manifest_batch(0, 1, 0, &[]),
            ],
            vec![manifest_batch(0, 1, 0, &[]), manifest_batch(0, 1, 0, &[a])],
        ] {
            let result = write.commit(manifests).await;
            assert!(
                matches!(&result, Err(e) if e.to_string().contains("more than one manifest")),
                "{result:?}"
            );
        }
        assert!(!write.files().await?.contains(&"table/_SUCCESS".to_string()));
        Ok(())
    }

    /// The content of a table directory before a write that overwrites,
    /// and a file outside the directory.
    const OLD_FILES: [&str; 6] = [
        "table/_SUCCESS",
        "table/.hidden",
        "table/data.json",
        "table/old.parquet",
        "table/x=1/y=2/old.parquet",
        "other/file",
    ];

    async fn create_old_files(write: &TestWrite) -> Result<Vec<String>> {
        let mut files: Vec<String> = OLD_FILES.iter().map(|x| x.to_string()).collect();
        // The files of an earlier write have the names of files of a write.
        files.push(format!("table/{}", name(0, &OTHER_WRITE_ID, 0, 0)));
        files.push(format!("table/x=1/{}", name(0, &OTHER_WRITE_ID, 0, 1)));
        for file in &files {
            write.create(file).await?;
        }
        files.sort();
        Ok(files)
    }

    fn sorted(mut files: Vec<String>) -> Vec<String> {
        files.sort();
        files
    }

    #[tokio::test]
    async fn test_overwrite_replaces_files_at_commit() -> Result<()> {
        let write = TestWrite::new();
        let old = create_old_files(&write).await?;
        let a = write
            .create(&format!("table/{}", name(0, &WRITE_ID, 0, 0)))
            .await?;
        let b = write
            .create(&format!("table/x=1/y=2/{}", name(1, &WRITE_ID, 0, 0)))
            .await?;
        let new = vec![a.location.to_string(), b.location.to_string()];
        let count = write
            .commit_with(
                vec![manifest_batch(0, 2, 0, &[a]), manifest_batch(1, 2, 0, &[b])],
                true,
            )
            .await?;
        assert_eq!(count, 20);

        // The `_SUCCESS` file of the earlier content is deleted before the
        // writer tasks start, and the rest is deleted after they have ended.
        let requests = write.requests_and_input();
        assert_eq!(
            requests[..3],
            ["DELETE table/_SUCCESS", "INPUT", "LIST table"]
        );
        assert_eq!(
            requests.last().map(|x| x.as_str()),
            Some("PUT table/_SUCCESS 0")
        );
        let deleted = sorted(requests[3..requests.len() - 1].to_vec());
        let expected: Vec<_> = old
            .iter()
            .filter(|x| x.starts_with("table/") && *x != "table/_SUCCESS")
            .map(|x| format!("DELETE {x}"))
            .collect();
        assert_eq!(deleted, sorted(expected));

        let mut expected = new;
        expected.extend(["other/file".to_string(), "table/_SUCCESS".to_string()]);
        assert_eq!(write.files().await?, sorted(expected));
        Ok(())
    }

    #[tokio::test]
    async fn test_overwrite_of_empty_directory() -> Result<()> {
        let write = TestWrite::new();
        let count = write
            .commit_with(vec![manifest_batch(0, 1, 0, &[])], true)
            .await?;
        assert_eq!(count, 0);
        assert_eq!(
            write.requests_and_input(),
            vec![
                "DELETE table/_SUCCESS",
                "INPUT",
                "LIST table",
                "PUT table/_SUCCESS 0"
            ]
        );
        assert_eq!(write.files().await?, vec!["table/_SUCCESS"]);
        Ok(())
    }

    #[tokio::test]
    async fn test_failed_overwrite_keeps_old_files() -> Result<()> {
        // The input fails, or the input ends before all writer tasks have reported.
        for failed_input in [true, false] {
            let write = TestWrite::new();
            let old = create_old_files(&write).await?;
            let a = write
                .create(&format!("table/{}", name(0, &WRITE_ID, 0, 0)))
                .await?;
            let mut manifests = vec![manifest_batch(0, 2, 0, std::slice::from_ref(&a))];
            if failed_input {
                manifests.push(exec_err!("writer failure"));
            }
            let result = write.commit_with(manifests, true).await;
            let message = if failed_input {
                "writer failure"
            } else {
                "1 of 2 writer tasks"
            };
            assert!(
                matches!(&result, Err(e) if e.to_string().contains(message)),
                "{result:?}"
            );
            // The directory is not listed, and only the file of the write is deleted.
            assert_eq!(
                write.requests_and_input(),
                vec![
                    "DELETE table/_SUCCESS".to_string(),
                    "INPUT".to_string(),
                    format!("DELETE {}", a.location)
                ]
            );
            let expected: Vec<_> = old
                .iter()
                .filter(|x| *x != "table/_SUCCESS")
                .cloned()
                .collect();
            assert_eq!(write.files().await?, expected);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_overwrite_does_not_start_writers_when_success_file_is_not_deleted() -> Result<()>
    {
        let write = TestWrite::new();
        let old = create_old_files(&write).await?;
        write.fail("DELETE", 0);
        let result = write
            .commit_with(vec![manifest_batch(0, 1, 0, &[])], true)
            .await;
        assert!(
            matches!(&result, Err(e) if e.to_string().contains("DELETE failure")),
            "{result:?}"
        );
        assert_eq!(write.requests_and_input(), vec!["DELETE table/_SUCCESS"]);
        assert_eq!(write.files().await?, old);
        Ok(())
    }

    #[tokio::test]
    async fn test_overwrite_keeps_new_files_when_commit_fails_after_writers() -> Result<()> {
        // The listing fails, a deletion of an old file fails,
        // or the `_SUCCESS` file is not written.
        for (kind, after) in [("LIST", 0), ("DELETE", 1), ("DELETE", 3), ("PUT", 0)] {
            let write = TestWrite::new();
            let old = create_old_files(&write).await?;
            let a = write
                .create(&format!("table/{}", name(0, &WRITE_ID, 0, 0)))
                .await?;
            let b = write
                .create(&format!("table/x=1/y=2/{}", name(1, &WRITE_ID, 0, 0)))
                .await?;
            let new = [a.location.to_string(), b.location.to_string()];
            write.fail(kind, after);
            let result = write
                .commit_with(
                    vec![manifest_batch(0, 2, 0, &[a]), manifest_batch(1, 2, 0, &[b])],
                    true,
                )
                .await;
            assert!(
                matches!(&result, Err(e) if e.to_string().contains(&format!("{kind} failure"))),
                "{kind} {result:?}"
            );
            // No file of the write is deleted, and there is no `_SUCCESS` file.
            let requests = write.requests();
            for file in &new {
                assert!(!requests.contains(&format!("DELETE {file}")), "{kind}");
            }
            let files = write.files().await?;
            for file in &new {
                assert!(files.contains(file), "{kind} {files:?}");
            }
            assert!(!files.contains(&"table/_SUCCESS".to_string()), "{kind}");
            let old_files = files
                .iter()
                .filter(|x| x.starts_with("table/") && !new.contains(*x))
                .count();
            match (kind, after) {
                // Nothing of the earlier content is deleted but its `_SUCCESS` file.
                ("LIST", _) => assert_eq!(old_files, old.len() - 2),
                // The deletion that fails is the second or the fourth deletion
                // after the deletion of the `_SUCCESS` file.
                ("DELETE", after) => assert!(old_files > old.len() - 2 - after - 1, "{files:?}"),
                _ => assert_eq!(old_files, 0),
            }
            assert!(files.contains(&"other/file".to_string()));
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_overwrite_after_retry_lists_once() -> Result<()> {
        let write = TestWrite::new();
        let old = create_old_files(&write).await?;
        let stale = [
            format!("table/{}", name(0, &WRITE_ID, 0, 0)),
            format!("table/x=1/{}", name(1, &WRITE_ID, 0, 0)),
        ];
        for path in &stale {
            write.create(path).await?;
        }
        // a later attempt that may still be running
        let later = format!("table/{}", name(0, &WRITE_ID, 0, 2));
        write.create(&later).await?;
        let a = write
            .create(&format!("table/{}", name(0, &WRITE_ID, 0, 1)))
            .await?;
        let b = write
            .create(&format!("table/x=1/{}", name(1, &WRITE_ID, 0, 1)))
            .await?;
        let new = vec![a.location.to_string(), b.location.to_string()];
        write
            .commit_with(
                vec![manifest_batch(0, 2, 1, &[a]), manifest_batch(1, 2, 1, &[b])],
                true,
            )
            .await?;
        let requests = write.requests();
        assert_eq!(requests.iter().filter(|x| x.starts_with("LIST")).count(), 1);
        let deleted = sorted(
            requests
                .iter()
                .filter(|x| x.starts_with("DELETE") && *x != "DELETE table/_SUCCESS")
                .cloned()
                .collect(),
        );
        let expected: Vec<_> = old
            .iter()
            .filter(|x| x.starts_with("table/") && *x != "table/_SUCCESS")
            .chain(stale.iter())
            .map(|x| format!("DELETE {x}"))
            .collect();
        assert_eq!(deleted, sorted(expected));
        let mut expected = new;
        expected.extend([
            later,
            "other/file".to_string(),
            "table/_SUCCESS".to_string(),
        ]);
        assert_eq!(write.files().await?, sorted(expected));
        Ok(())
    }

    #[tokio::test]
    async fn test_commit_keeps_files_in_directories_with_special_names() -> Result<()> {
        use object_store::local::LocalFileSystem;

        // The files of the write are kept whatever the form is in which
        // the object store lists the locations of the files.
        let directory = tempfile::tempdir()?;
        let stores: [Arc<dyn ObjectStore>; 2] = [
            Arc::new(InMemory::new()),
            Arc::new(LocalFileSystem::new_with_prefix(directory.path())?),
        ];
        for store in stores {
            for (overwrite, attempt) in [(true, 0), (true, 1), (false, 1)] {
                let table = Path::from("table");
                let mut written = vec![];
                for (partition, value) in ["x=50% off", "x=a b", "x=é/y=%20", "x=a%2Fb"]
                    .into_iter()
                    .enumerate()
                {
                    let mut location = table.clone();
                    for part in value.split('/') {
                        location = location.join(part);
                    }
                    let location = location.join(name(partition, &WRITE_ID, 0, attempt));
                    store
                        .put(&location, Bytes::from_static(b"data").into())
                        .await?;
                    written.push(WrittenFile {
                        location,
                        size: 4,
                        num_rows: 1,
                    });
                }
                let old = table.clone().join("x=50% off").join("old.parquet");
                store.put(&old, Bytes::from_static(b"old").into()).await?;
                let stale = table
                    .clone()
                    .join("x=a b")
                    .join(name(1, &WRITE_ID, 0, attempt + 1));
                store.put(&stale, Bytes::from_static(b"old").into()).await?;

                let manifests: Vec<_> = written
                    .iter()
                    .enumerate()
                    .map(|(partition, file)| {
                        manifest_batch(partition, 4, attempt, std::slice::from_ref(file))
                    })
                    .collect();
                let open_input = move || {
                    let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
                        manifest_schema(),
                        stream::iter(manifests),
                    ));
                    Ok(input)
                };
                let count = commit_file_write(
                    open_input,
                    Arc::clone(&store),
                    table.clone(),
                    WRITE_ID,
                    overwrite,
                )
                .await?;
                assert_eq!(count, 4);
                for file in &written {
                    let data = store.get(&file.location).await?.bytes().await?;
                    assert_eq!(data.as_ref(), b"data", "{store} {}", file.location);
                }
                // The file of a later attempt is kept, and the file that is not
                // a file of the write is kept unless the write overwrites.
                assert!(store.head(&stale).await.is_ok());
                assert_eq!(store.head(&old).await.is_ok(), !overwrite);
                let files: Vec<_> = store.list(Some(&table)).try_collect().await?;
                assert_eq!(files.len(), if overwrite { 6 } else { 7 }, "{files:?}");
                delete_files(&store, files.into_iter().map(|x| x.location).collect()).await?;
            }
        }
        Ok(())
    }
}
