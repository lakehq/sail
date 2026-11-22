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
    let files = stream::iter(files.into_iter().map(Ok)).boxed();
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
async fn commit_file_write(
    mut input: SendableRecordBatchStream,
    store: Arc<dyn ObjectStore>,
    table_path: Path,
    write_id: Uuid,
) -> Result<u64> {
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

    // A file that no manifest refers to exists only if a writer task was
    // attempted more than once. The files of later attempts are kept, since
    // such an attempt may be running when this attempt of the commit was
    // canceled but has not stopped yet.
    let max_attempt = manifests.max_attempt();
    if max_attempt > 0 {
        let stale = store
            .list(Some(&table_path))
            .try_filter_map(|meta| {
                let stale = meta
                    .location
                    .filename()
                    .and_then(parse_part_file_name)
                    .is_some_and(|name| {
                        name.write_id == write_id && name.attempt as u64 <= max_attempt
                    })
                    && !manifests.files.contains(&meta.location);
                futures::future::ready(Ok(stale.then_some(meta.location)))
            })
            .try_collect::<Vec<_>>()
            .await?;
        delete_files(&store, stale).await?;
    }

    store
        .put(&table_path.join(SUCCESS_FILE_NAME), PutPayload::default())
        .await?;
    Ok(manifests.num_rows)
}

/// Completes a listing table write after all writer tasks have finished.
///
/// The input is the manifests of the writer tasks.
/// The files of failed or superseded writer task attempts are deleted,
/// and then an empty `_SUCCESS` file is written to the table directory.
/// The output is a single row with the number of rows written.
#[derive(Debug)]
pub struct FileWriteCommitExec {
    input: Arc<dyn ExecutionPlan>,
    object_store_url: ObjectStoreUrl,
    table_path: Path,
    write_id: Uuid,
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
            properties,
        }
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
        write!(f, "{}: path={}", self.name(), self.table_path)
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
        Ok(Arc::new(Self::new(
            Arc::clone(input),
            self.object_store_url.clone(),
            self.table_path.clone(),
            self.write_id,
        )))
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
        let input = self.input.execute(0, Arc::clone(&context))?;
        let store = context.runtime_env().object_store(&self.object_store_url)?;
        let table_path = self.table_path.clone();
        let write_id = self.write_id;
        let schema = self.schema();
        let output = stream::once(async move {
            let count = commit_file_write(input, store, table_path, write_id).await?;
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

    /// An object store that records the requests it receives.
    #[derive(Debug)]
    struct RecordingStore {
        inner: InMemory,
        requests: Arc<Mutex<Vec<String>>>,
    }

    impl RecordingStore {
        fn new() -> Self {
            Self {
                inner: InMemory::new(),
                requests: Arc::new(Mutex::new(vec![])),
            }
        }
    }

    fn record(requests: &Mutex<Vec<String>>, request: String) {
        if let Ok(mut requests) = requests.lock() {
            requests.push(request);
        }
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
            let locations = locations
                .inspect_ok(move |location| record(&requests, format!("DELETE {location}")))
                .boxed();
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            let prefix_text = prefix.map(|x| x.to_string()).unwrap_or_default();
            record(&self.requests, format!("LIST {prefix_text}"));
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
            let input = Box::pin(RecordBatchStreamAdapter::new(
                manifest_schema(),
                stream::iter(manifests),
            ));
            commit_file_write(
                input,
                Arc::clone(&self.store),
                Path::from("table"),
                WRITE_ID,
            )
            .await
        }

        fn requests(&self) -> Vec<String> {
            self.recording
                .requests
                .lock()
                .map(|x| x.clone())
                .unwrap_or_default()
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
}
