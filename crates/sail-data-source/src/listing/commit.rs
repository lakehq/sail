// SPDX-License-Identifier: Apache-2.0

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt::Formatter;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, PoisonError};

use datafusion::arrow::array::{
    Array, ArrayRef, ListBuilder, RecordBatch, StringArray, StringBuilder, UInt64Array,
};
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
use datafusion_common::cast::{as_list_array, as_string_array, as_uint64_array};
use datafusion_common::{DataFusionError, Result, exec_err, internal_datafusion_err, internal_err};
use futures::{FutureExt, StreamExt, TryStreamExt, future, stream};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use sail_common_datafusion::error::CommitStateUnknownError;
use sail_common_datafusion::extension::{TaskAttempt, task_context_with_extension};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::listing::staging::{
    PUBLISH_CONCURRENCY, Publish, Publisher, StagedFile, is_staged_name,
};

/// Stops the writer tasks of a write that run in the process of its commit.
///
/// The commit gives this to its input in the task context, and the writer
/// tasks of the input stop when the commit has failed or is canceled.
/// The writer tasks on other workers do not have it. The driver stops them.
#[derive(Debug)]
pub struct WriteCancellation(CancellationToken);

impl WriteCancellation {
    /// Returns the cancellation of the write whose commit has created
    /// the task context, if there is one.
    pub fn of(context: &TaskContext) -> Option<Arc<Self>> {
        context.session_config().get_extension::<Self>()
    }

    /// Waits until the write is canceled.
    pub async fn canceled(&self) {
        self.0.cancelled().await
    }
}

/// The name of the empty file that marks a completed write.
const SUCCESS_FILE_NAME: &str = "_SUCCESS";

/// The schema of the manifest that each writer task returns.
///
/// The manifest has one row per file that the task has staged, with the way
/// in which the file is published: `staged` is the location of the object
/// that is renamed, or `upload_id` and `parts` identify the multipart upload
/// that is completed. A task that staged no file returns a single row with
/// a null path. The row is required since the commit must see every writer
/// partition, and an exchange between the writers and the commit drops empty
/// batches.
///
/// Each row has the number of writer partitions and the number of files of
/// its task, so that the commit can tell whether it has received every row
/// of every task. The end of the input does not show this.
pub fn manifest_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("partition", DataType::UInt64, false),
        Field::new("partitions", DataType::UInt64, false),
        Field::new("files", DataType::UInt64, false),
        Field::new("path", DataType::Utf8, true),
        Field::new("size", DataType::UInt64, false),
        Field::new("num_rows", DataType::UInt64, false),
        Field::new("staged", DataType::Utf8, true),
        Field::new("upload_id", DataType::Utf8, true),
        Field::new(
            "parts",
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            true,
        ),
    ]))
}

/// Builds the manifest of the writer task for one of the writer partitions.
pub fn manifest_batch(
    partition: usize,
    partitions: usize,
    files: &[StagedFile],
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
    let mut staged = StringBuilder::new();
    let mut upload_ids = StringBuilder::new();
    let mut parts = ListBuilder::new(StringBuilder::new());
    for file in files {
        match &file.publish {
            Publish::Rename { staged: location } => {
                staged.append_value(location.as_ref());
                upload_ids.append_null();
                parts.append_null();
            }
            Publish::Multipart {
                upload_id,
                parts: ids,
            } => {
                staged.append_null();
                upload_ids.append_value(upload_id);
                for id in ids {
                    parts.values().append_value(id);
                }
                parts.append(true);
            }
        }
    }
    if files.is_empty() {
        staged.append_null();
        upload_ids.append_null();
        parts.append_null();
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(vec![partition as u64; rows])),
        Arc::new(UInt64Array::from(vec![partitions as u64; rows])),
        Arc::new(UInt64Array::from(vec![files.len() as u64; rows])),
        Arc::new(StringArray::from(paths)),
        Arc::new(UInt64Array::from(sizes)),
        Arc::new(UInt64Array::from(num_rows)),
        Arc::new(staged.finish()),
        Arc::new(upload_ids.finish()),
        Arc::new(parts.finish()),
    ];
    Ok(RecordBatch::try_new(manifest_schema(), columns)?)
}

/// Returns the staged files of the manifest of a writer task.
#[cfg(test)]
pub(crate) fn manifest_files(manifest: &RecordBatch) -> Result<Vec<StagedFile>> {
    let mut files = vec![];
    Manifests::default().add(manifest, &mut files)?;
    Ok(files)
}

/// What a writer partition has reported so far.
#[derive(Debug, Clone, Copy)]
struct ReportedPartition {
    /// The number of files of the manifest of the writer task.
    files: u64,
    /// The number of rows of the manifest that were received.
    rows: u64,
}

/// The manifests of all writer tasks of a write.
#[derive(Debug, Default)]
struct Manifests {
    /// The number of writer partitions.
    partitions: Option<u64>,
    reported: HashMap<u64, ReportedPartition>,
    locations: HashSet<Path>,
    num_rows: u64,
}

impl Manifests {
    /// Adds a manifest. The files of the manifest are added to `files`
    /// also when the manifest is rejected, so that they can be removed.
    fn add(&mut self, manifest: &RecordBatch, files: &mut Vec<StagedFile>) -> Result<()> {
        let [
            partition,
            partitions,
            file_count,
            path,
            size,
            num_rows,
            staged,
            upload_id,
            parts,
        ] = manifest.columns()
        else {
            return internal_err!("invalid columns in the manifest");
        };
        let partition = as_uint64_array(partition)?;
        let partitions = as_uint64_array(partitions)?;
        let file_count = as_uint64_array(file_count)?;
        let path = as_string_array(path)?;
        let size = as_uint64_array(size)?;
        let num_rows = as_uint64_array(num_rows)?;
        let staged = as_string_array(staged)?;
        let upload_id = as_string_array(upload_id)?;
        let parts = as_list_array(parts)?;
        let parse = |path: &str| {
            Path::parse(path)
                .map_err(|e| internal_datafusion_err!("invalid path in the manifest: {e}"))
        };
        // The file of a row, if the row has one.
        let file = |i: usize| -> Result<Option<StagedFile>> {
            if path.is_null(i) {
                return Ok(None);
            }
            let publish = if staged.is_valid(i) {
                Publish::Rename {
                    staged: parse(staged.value(i))?,
                }
            } else if upload_id.is_valid(i) && parts.is_valid(i) {
                let ids = parts.value(i);
                Publish::Multipart {
                    upload_id: upload_id.value(i).to_string(),
                    parts: as_string_array(&ids)?
                        .iter()
                        .map(|id| id.map(|id| id.to_string()))
                        .collect::<Option<_>>()
                        .ok_or_else(|| internal_datafusion_err!("missing part in the manifest"))?,
                }
            } else {
                return internal_err!("a file of the manifest cannot be published");
            };
            Ok(Some(StagedFile {
                location: parse(path.value(i))?,
                size: size.value(i),
                num_rows: num_rows.value(i),
                publish,
            }))
        };
        // All rows are read also when a row is rejected, so that the files
        // of the other rows are known.
        let mut result = Ok(());
        for i in 0..manifest.num_rows() {
            let check = file(i).and_then(|file| {
                let check = self.check_row(
                    partition.value(i),
                    partitions.value(i),
                    file_count.value(i),
                    file.as_ref(),
                );
                files.extend(file);
                check
            });
            result = result.and(check);
        }
        result
    }

    fn check_row(
        &mut self,
        partition: u64,
        partitions: u64,
        files: u64,
        file: Option<&StagedFile>,
    ) -> Result<()> {
        if *self.partitions.get_or_insert(partitions) != partitions {
            return internal_err!("writer tasks disagree on the number of partitions");
        }
        if partition >= partitions {
            return internal_err!(
                "writer partition {partition} is not one of the {partitions} partitions of the write"
            );
        }
        // Each task returns its manifest once, so a partition has either
        // one row without a file or one row for each of its files.
        let reported = self
            .reported
            .entry(partition)
            .or_insert(ReportedPartition { files, rows: 0 });
        reported.rows += 1;
        let duplicate = reported.files != files
            || reported.rows > files.max(1)
            || file.is_some_and(|file| !self.locations.insert(file.location.clone()));
        if duplicate {
            return internal_err!("writer partition {partition} has more than one manifest");
        }
        if file.is_some() != (files > 0) {
            return internal_err!("writer partition {partition} has an invalid manifest");
        }
        self.num_rows += file.map_or(0, |file| file.num_rows);
        Ok(())
    }

    /// Checks that all rows of the manifests of all writer partitions were
    /// received. The end of the input does not show this, since the output
    /// of a failed or canceled writer task can end in the same way as
    /// a complete output. Nothing is published for a write that is incomplete.
    /// The error is the description of what is missing.
    fn validate(&self) -> Result<(), String> {
        let Some(partitions) = self.partitions else {
            return Err(
                "the write is incomplete: no writer task has reported its files. \
                A writer task has failed or was canceled, and its error is in the logs of the task."
                    .to_string(),
            );
        };
        let complete = self
            .reported
            .values()
            .filter(|reported| reported.rows == reported.files.max(1))
            .count() as u64;
        if complete != partitions {
            return Err(format!(
                "the write is incomplete: {complete} of {partitions} writer tasks have reported all their files. \
                A writer task has failed or was canceled, and its error is in the logs of the task."
            ));
        }
        Ok(())
    }
}

/// Deletes the files. A file that does not exist is not an error.
/// All files are tried, and the first error is returned.
async fn delete_files(store: &Arc<dyn ObjectStore>, files: Vec<Path>) -> Result<()> {
    let mut deleted = store.delete_stream(stream::iter(files.into_iter().map(Ok)).boxed());
    let mut result = Ok(());
    while let Some(file) = deleted.next().await {
        match file {
            Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => result = result.and(Err(e.into())),
        }
    }
    result
}

/// The writes of a job whose tasks can be attempted again.
///
/// A task of such a job must not publish the files of a write. The tasks of
/// a job run at the same time, and the output of a task that has failed or
/// was canceled can end for its readers in the same way as a complete output.
/// The writer tasks can therefore stage files that miss rows of a task that
/// has failed before them, and the commit can receive the manifests of all
/// writer tasks, before the job has learned of the failure. The task of
/// the commit can also be attempted again after it has ended. Only the end of
/// the job shows whether the files have all the rows: a job succeeds when
/// the latest attempt of each of its tasks has succeeded.
///
/// The job runner therefore gives each job this object in the task context of
/// the job. All attempts of the commit of a write run in the process of the job
/// runner, with that task context. An attempt records here the staged files
/// that it has received, and it does not publish them. The attempt with the
/// greatest number replaces the others, whose files are removed. The job
/// runner publishes the files of the writes when the job has succeeded,
/// and removes them when the job has failed or is canceled. This is the same
/// for every way in which the tasks of a job exchange their data.
///
/// A plan that is executed without such a job runner is attempted once, in
/// one process, where an input that has failed returns its error and does
/// not end. The commit of its write publishes the files itself when its
/// input has ended.
#[derive(Default)]
pub struct JobWrites {
    state: Mutex<JobWritesState>,
}

enum JobWritesState {
    /// The job is running. The value has the latest attempt of the commit
    /// of each write that has recorded its files.
    Running(HashMap<Uuid, PreparedWrite>),
    /// The job has ended, and its writes are published or removed.
    Ended,
}

impl Default for JobWritesState {
    fn default() -> Self {
        Self::Running(HashMap::new())
    }
}

/// What an attempt of the commit of a write has recorded for the end of the job.
struct PreparedWrite {
    /// The number of the attempt of the task of the commit.
    attempt: usize,
    commit: WriteCommit,
    received: Received,
}

impl PreparedWrite {
    /// Makes the write visible. This must run to its end once it has started.
    async fn publish(self) -> Result<()> {
        match self.received {
            Received::Complete(received) => self.commit.publish(received.files).await,
            // The job has succeeded although a writer task has not reported
            // its files, which the job runner rules out.
            Received::Incomplete(message) => exec_err!("{message}"),
        }
    }

    /// Removes the staged files of the attempt, which are not published.
    async fn remove(self) {
        if let Received::Complete(received) = self.received {
            self.commit.abort(received.files).await;
        }
    }
}

impl JobWrites {
    /// Returns the writes of the job that the task context belongs to,
    /// if the plan is executed for a job whose tasks can be attempted again.
    pub fn of(context: &TaskContext) -> Option<Arc<Self>> {
        context.session_config().get_extension::<Self>()
    }

    fn state(&self) -> std::sync::MutexGuard<'_, JobWritesState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Whether no write of the job has recorded its files.
    pub fn is_empty(&self) -> bool {
        match &*self.state() {
            JobWritesState::Running(writes) => writes.is_empty(),
            JobWritesState::Ended => true,
        }
    }

    /// Records what an attempt of the commit of a write has received.
    /// Returns whether the attempt is recorded, and the attempt whose staged
    /// files the caller removes: the earlier attempt that is replaced, or
    /// the given attempt when a later attempt is recorded already or when
    /// the job has ended.
    fn prepare(&self, write: PreparedWrite) -> (bool, Option<PreparedWrite>) {
        let mut state = self.state();
        let JobWritesState::Running(writes) = &mut *state else {
            return (false, Some(write));
        };
        match writes.entry(write.commit.write_id) {
            Entry::Occupied(entry) if entry.get().attempt >= write.attempt => (false, Some(write)),
            Entry::Occupied(mut entry) => (true, Some(entry.insert(write))),
            Entry::Vacant(entry) => {
                entry.insert(write);
                (true, None)
            }
        }
    }

    /// Ends the job, and returns what its writes have recorded.
    /// An attempt that comes later is not recorded.
    fn end(&self) -> Vec<PreparedWrite> {
        match std::mem::replace(&mut *self.state(), JobWritesState::Ended) {
            JobWritesState::Running(writes) => {
                let mut writes: Vec<_> = writes.into_values().collect();
                writes.sort_by_key(|write| write.commit.write_id);
                writes
            }
            JobWritesState::Ended => vec![],
        }
    }

    /// Publishes the files of the writes of the job. The job runner calls
    /// this once, when the job has succeeded, and reports the error to
    /// the client of the job.
    ///
    /// The files are published by a separate task, which is not canceled when
    /// the caller is, so that the table directory is not left with a part of
    /// a write. A write that is canceled at that time is reported as canceled
    /// to its client, and it is committed nevertheless when the process keeps
    /// running. If a write is not published, the staged files of the writes
    /// after it are removed.
    pub async fn commit(&self) -> Result<()> {
        let writes = self.end();
        if writes.is_empty() {
            return Ok(());
        }
        run_to_end(async move {
            let mut result = Ok(());
            for write in writes {
                if result.is_ok() {
                    result = write.publish().await;
                } else {
                    write.remove().await;
                }
            }
            result
        })
        .await
    }

    /// Removes the staged files of the writes of the job. The job runner
    /// calls this when the job has failed or is canceled.
    ///
    /// The files are removed by a separate task, which has started when this
    /// returns, whether or not the returned work is awaited. The work ends
    /// when the files are removed.
    pub fn abort(&self) -> impl Future<Output = ()> + Send + 'static {
        let writes = self.end();
        let removal = match tokio::runtime::Handle::try_current() {
            Ok(runtime) if !writes.is_empty() => Some(runtime.spawn(async move {
                for write in writes {
                    write.remove().await;
                }
            })),
            Ok(_) => None,
            Err(e) => {
                log::warn!("the staged files of the writes of a job are not removed: {e}");
                None
            }
        };
        async move {
            if let Some(removal) = removal {
                let _ = removal.await;
            }
        }
    }
}

/// Runs the work that makes a write visible in a task of its own, which is
/// not canceled when the caller is. The state of the write is unknown if
/// the task does not end.
async fn run_to_end(work: impl Future<Output = Result<()>> + Send + 'static) -> Result<()> {
    match tokio::spawn(work).await {
        Ok(result) => result,
        Err(e) => Err(commit_state_unknown(
            "the task that publishes the files of the write has not ended",
            Some(DataFusionError::External(Box::new(e))),
        )),
    }
}

/// The staged files of the manifests of all writer tasks of a write.
struct ReceivedManifests {
    /// The number of rows written.
    num_rows: u64,
    files: Vec<StagedFile>,
}

/// What the input of a commit has delivered when it has ended without an error.
enum Received {
    /// All rows of the manifests of all writer tasks.
    Complete(ReceivedManifests),
    /// Not all rows of the manifests, with the description of what is missing.
    /// The staged files of the rows that were received are removed.
    Incomplete(String),
}

/// Reads the manifests of all writer tasks to the end of the input, and
/// gives their staged files to the commit.
///
/// A writer task does not own its staged files any longer when its manifest
/// is taken, so every manifest that reaches the input must be read. This is
/// therefore a task of its own, which starts when the commit is executed and
/// which is not canceled when the commit is. A manifest can wait in the input
/// for a long time, since the input can collect the manifests of many writer
/// tasks before it returns one.
///
/// The input ends when all writer tasks have ended. The first error stops
/// the writer tasks that run in this process, and the driver stops the writer
/// tasks on the workers when the commit has failed or is canceled.
/// The staged files are removed here when the input has failed, when
/// a manifest is rejected, when the write is incomplete, or when the commit
/// is gone at the end of the input. The commit receives the result after that.
async fn read_manifests(
    mut input: SendableRecordBatchStream,
    publisher: Publisher,
    table_path: Path,
    write_id: Uuid,
    cancel: CancellationToken,
    commit: oneshot::Sender<Result<Received>>,
) {
    let mut manifests = Manifests::default();
    let mut files = vec![];
    let mut result = Ok(());
    // The input is read after an error as well, since the manifests of
    // the writer tasks that have ended can follow the error of another task.
    // A writer task that runs in this process can panic while the input is
    // read. The panic is the error of the write then, and the input is not
    // read again.
    loop {
        let next = AssertUnwindSafe(input.next()).catch_unwind().await;
        let panicked = next.is_err();
        let added = match next {
            Ok(Some(manifest)) => {
                manifest.and_then(|manifest| manifests.add(&manifest, &mut files))
            }
            Ok(None) => break,
            Err(panic) => {
                let message = panic
                    .downcast_ref::<&str>()
                    .map(|x| x.to_string())
                    .or_else(|| panic.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "unknown panic".to_string());
                internal_err!("a writer task of the write has panicked: {message}")
            }
        };
        if added.is_err() && result.is_ok() {
            cancel.cancel();
            result = added;
        }
        if panicked {
            break;
        }
    }
    if std::panic::catch_unwind(AssertUnwindSafe(|| drop(input))).is_err() {
        log::warn!(
            "the input of the commit of the write {write_id} has panicked when it was dropped"
        );
    }
    let remove = |files: Vec<StagedFile>| async move {
        if !files.is_empty() {
            log::warn!(
                "removing {} staged files of the write {write_id} to {table_path}, \
                which are not published",
                files.len(),
            );
            publisher.abort_all(&files).await;
        }
    };
    let result = result.map(|()| manifests.validate());
    match result {
        Ok(Ok(())) => {
            let received = ReceivedManifests {
                num_rows: manifests.num_rows,
                files,
            };
            // The commit owns the files, unless it was canceled.
            if let Err(Ok(Received::Complete(received))) =
                commit.send(Ok(Received::Complete(received)))
            {
                remove(received.files).await;
            }
        }
        Ok(Err(incomplete)) => {
            remove(files).await;
            let _ = commit.send(Ok(Received::Incomplete(incomplete)));
        }
        Err(e) => {
            remove(files).await;
            let _ = commit.send(Err(e));
        }
    }
}

/// The end of the commit that receives what [`read_manifests`] has read.
///
/// The commit can be canceled when the manifests are sent and not yet
/// received. Their staged files have no owner then, so they are removed
/// when this is dropped.
struct ManifestsReceiver {
    receiver: oneshot::Receiver<Result<Received>>,
    publisher: Publisher,
}

impl Drop for ManifestsReceiver {
    fn drop(&mut self) {
        // Manifests that are sent after this are removed by their sender.
        self.receiver.close();
        let Ok(Ok(Received::Complete(received))) = self.receiver.try_recv() else {
            return;
        };
        if received.files.is_empty() {
            return;
        }
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                let publisher = self.publisher.clone();
                runtime.spawn(async move { publisher.abort_all(&received.files).await });
            }
            Err(e) => log::warn!(
                "{} staged files of a write that is canceled are not removed: {e}",
                received.files.len()
            ),
        }
    }
}

/// The work of the commit of a write.
struct WriteCommit {
    publisher: Publisher,
    table_path: Path,
    write_id: Uuid,
    /// Whether the write replaces the content of the table directory.
    overwrite: bool,
    /// Whether the `_SUCCESS` file is written.
    write_success_file: bool,
}

impl WriteCommit {
    /// Starts to read the manifests of the writer tasks from the input, and
    /// returns the work that completes the write with the number of rows
    /// written. `cancel` stops the writer tasks of the input that run in
    /// this process. They are stopped when the work ends or is dropped.
    ///
    /// Without a job, the work publishes the files when the input has ended.
    /// For a job, with the number of the attempt of the task of the commit,
    /// the work records the files for the end of the job, see [`JobWrites`].
    /// It then has no number of rows, and no error, when the manifests are
    /// incomplete: a writer task has failed or was canceled then, so the job
    /// fails with the error of that task or attempts its tasks again.
    fn start(
        self,
        input: SendableRecordBatchStream,
        cancel: CancellationToken,
        job: Option<(Arc<JobWrites>, usize)>,
    ) -> Result<impl Future<Output = Result<Option<u64>>> + Send + 'static> {
        let runtime = tokio::runtime::Handle::try_current().map_err(|e| {
            internal_datafusion_err!("the commit of a write is executed outside of a runtime: {e}")
        })?;
        let (sender, manifests) = oneshot::channel();
        runtime.spawn(read_manifests(
            input,
            self.publisher.clone(),
            self.table_path.clone(),
            self.write_id,
            cancel.clone(),
            sender,
        ));
        let mut manifests = ManifestsReceiver {
            receiver: manifests,
            publisher: self.publisher.clone(),
        };
        let cancel = cancel.drop_guard();
        Ok(async move {
            let _cancel = cancel;
            let received = match (&mut manifests.receiver).await {
                Ok(received) => received?,
                Err(_) => return internal_err!("the manifests of the write were not read"),
            };
            match (job, received) {
                (Some((job, attempt)), received) => self.prepare(&job, attempt, received).await,
                (None, Received::Complete(received)) => {
                    let count = received.num_rows;
                    run_to_end(self.publish(received.files)).await?;
                    Ok(Some(count))
                }
                (None, Received::Incomplete(message)) => exec_err!("{message}"),
            }
        })
    }

    /// Records what the attempt of the commit has received for the end of
    /// the job, and returns the number of rows written if the manifests
    /// are complete.
    async fn prepare(
        self,
        job: &JobWrites,
        attempt: usize,
        received: Received,
    ) -> Result<Option<u64>> {
        let write_id = self.write_id;
        let count = match &received {
            Received::Complete(received) => Some(received.num_rows),
            Received::Incomplete(_) => None,
        };
        let (recorded, other) = job.prepare(PreparedWrite {
            attempt,
            commit: self,
            received,
        });
        if let Some(other) = other {
            other.remove().await;
        }
        if !recorded {
            return internal_err!(
                "attempt {attempt} of the commit of the write {write_id} is not \
                the latest attempt of a job that is running"
            );
        }
        Ok(count)
    }

    /// Removes staged files that are not published. The files are removed
    /// by a separate task, which is not canceled when the commit is.
    async fn abort(&self, files: Vec<StagedFile>) {
        if files.is_empty() {
            return;
        }
        log::warn!(
            "removing {} staged files of the write {} to {}, which are not published",
            files.len(),
            self.write_id,
            self.table_path
        );
        let publisher = self.publisher.clone();
        // The files are removed whether or not the task is awaited.
        let _ = tokio::spawn(async move { publisher.abort_all(&files).await }).await;
    }

    /// Lists the content of the table directory that an overwrite replaces.
    /// Returns the locations of the objects and whether the directory has
    /// a `_SUCCESS` file, which is not among the locations.
    ///
    /// This is everything in the directory except for the objects that
    /// the writes to the table have staged, since these can belong to a write
    /// that is running, and except for the locations of the files of this
    /// write, which are never deleted as old content.
    async fn list_old_content(&self, files: &[StagedFile]) -> Result<(Vec<Path>, bool)> {
        let own: HashSet<&Path> = files.iter().map(|file| &file.location).collect();
        let success_file = self.table_path.clone().join(SUCCESS_FILE_NAME);
        let mut has_success_file = false;
        let mut locations = vec![];
        let mut objects = self.publisher.store().list(Some(&self.table_path));
        while let Some(object) = objects.try_next().await? {
            if object.location == success_file {
                has_success_file = true;
            } else if !object.location.filename().is_some_and(is_staged_name)
                && !own.contains(&object.location)
            {
                locations.push(object.location);
            }
        }
        Ok((locations, has_success_file))
    }

    /// Publishes the files at the indices. Returns the indices of the files
    /// that are not published, each with its error.
    async fn publish_files(
        &self,
        files: &[StagedFile],
        indices: impl Iterator<Item = usize>,
    ) -> Vec<(usize, DataFusionError)> {
        stream::iter(indices)
            .map(|i| async move {
                let result = match files.get(i) {
                    Some(file) => self.publisher.publish(file).await,
                    None => internal_err!("missing staged file"),
                };
                result.err().map(|e| (i, e))
            })
            .buffer_unordered(PUBLISH_CONCURRENCY)
            .filter_map(futures::future::ready)
            .collect()
            .await
    }

    /// Makes the write visible. This must run to its end once it has started.
    ///
    /// The steps are in this order:
    ///
    ///   1. A write that overwrites lists the old content of the table
    ///      directory and deletes its `_SUCCESS` file. A failure up to here
    ///      leaves the table as it was, and the staged files are removed.
    ///   2. The files are published. The files that are not published
    ///      at first are tried once more when the other files have been
    ///      tried, since the files are staged durably and a file can be
    ///      published any number of times. If files remain, the files
    ///      that were published are deleted, the others are removed,
    ///      and the `_SUCCESS` file is written again if it was deleted.
    ///      The table is then as it was, unless a reader has seen some of
    ///      the files in the meantime, or unless the object store keeps
    ///      deleted objects as versions.
    ///   3. A write that overwrites deletes the old content.
    ///   4. The `_SUCCESS` file is written.
    ///
    /// A failure after the files were published, or a failure to undo
    /// a partial publication, leaves the table in a state that is reported
    /// as unknown to the client of the write. The directory then
    /// has no `_SUCCESS` file of this write. A write that appends does not
    /// touch the `_SUCCESS` file of an earlier write before it writes its own,
    /// as in Spark, so that file can be present while the directory has
    /// a part of the files of this write, for example when the process is
    /// stopped while the files are published. It is deleted when a partial
    /// publication cannot be undone, if the object store allows it.
    ///
    /// A write that overwrites deletes the old content only after the new
    /// files are published. This is an intentional deviation from Spark, which
    /// deletes the old content before the write starts. A Spark overwrite that
    /// fails has lost the old content, and its readers see an empty or partial
    /// table while it runs. Here the old content is kept unless the write
    /// succeeds, and the readers see it until the files are published.
    /// The cost is that a reader that lists the directory between steps 2
    /// and 3 sees the old and the new rows together. This lasts as long as
    /// it takes to publish the files and to delete the old content, which is
    /// one small request for each file, with many requests at the same time.
    async fn publish(self, files: Vec<StagedFile>) -> Result<()> {
        let store = Arc::clone(self.publisher.store());
        let success_file = self.table_path.clone().join(SUCCESS_FILE_NAME);

        let prepared = async {
            if let Some(file) = files.iter().find(|file| !self.publisher.accepts(file)) {
                return internal_err!(
                    "the file {} is staged in another way than the commit publishes files: \
                    the sessions of the writer tasks and of the commit do not have \
                    the same kind of object store",
                    file.location
                );
            }
            if !self.overwrite {
                return Ok((vec![], false));
            }
            let (old_content, has_success_file) = self.list_old_content(&files).await?;
            if has_success_file {
                delete_files(&store, vec![success_file.clone()]).await?;
            }
            Ok((old_content, has_success_file))
        };
        let (old_content, had_success_file) = match prepared.await {
            Ok(prepared) => prepared,
            Err(e) => {
                self.abort(files).await;
                return Err(e);
            }
        };

        let mut failed = self.publish_files(&files, 0..files.len()).await;
        if !failed.is_empty() {
            let indices: Vec<usize> = failed.iter().map(|(i, _)| *i).collect();
            failed = self.publish_files(&files, indices.into_iter()).await;
        }
        if !failed.is_empty() {
            return Err(self.undo(files, failed, had_success_file).await);
        }

        delete_files(&store, old_content).await.map_err(|e| {
            commit_state_unknown(
                "the files of the write are published, but the old content \
                of the table directory could not be deleted",
                Some(e),
            )
        })?;
        if self.write_success_file {
            store
                .put(&success_file, PutPayload::default())
                .await
                .map_err(|e| {
                    commit_state_unknown(
                        "the files of the write are published, \
                        but the _SUCCESS file could not be written",
                        Some(e.into()),
                    )
                })?;
        }
        Ok(())
    }

    /// Undoes a publication that has failed for some of the files,
    /// and returns the error of the write.
    async fn undo(
        &self,
        files: Vec<StagedFile>,
        failed: Vec<(usize, DataFusionError)>,
        restore_success_file: bool,
    ) -> DataFusionError {
        let store = self.publisher.store();
        let success_file = self.table_path.clone().join(SUCCESS_FILE_NAME);
        let unpublished: HashSet<usize> = failed.iter().map(|(i, _)| *i).collect();
        let error = match failed.into_iter().next() {
            Some((_, error)) => error,
            None => internal_datafusion_err!("missing error of a file that is not published"),
        };
        log::warn!(
            "{} of {} files of the write {} to {} could not be published, \
            and the write is undone: {error}",
            unpublished.len(),
            files.len(),
            self.write_id,
            self.table_path
        );
        // A request that has failed can still take effect later, so the locations
        // of all files are deleted, also of the files that are not published.
        let locations: Vec<Path> = files.iter().map(|file| file.location.clone()).collect();
        let staged: Vec<_> = files
            .into_iter()
            .enumerate()
            .filter(|(i, _)| unpublished.contains(i))
            .map(|(_, file)| file)
            .collect();
        self.publisher.abort_all(&staged).await;
        let undone = async {
            delete_files(store, locations).await?;
            if restore_success_file {
                store.put(&success_file, PutPayload::default()).await?;
            }
            Ok::<_, DataFusionError>(())
        };
        match undone.await {
            Ok(()) => error.context(
                "the files of the write could not be published, \
                and the table directory is as it was before the write",
            ),
            Err(e) => {
                // The directory can have some of the files of the write,
                // so it must not have a `_SUCCESS` file.
                if let Err(e) = delete_files(store, vec![success_file]).await {
                    log::warn!(
                        "failed to delete the _SUCCESS file of {}: {e}",
                        self.table_path
                    );
                }
                commit_state_unknown(
                    format!(
                        "the files of the write could not be published ({error}), \
                        and the files that were published could not be deleted"
                    ),
                    Some(e),
                )
            }
        }
    }
}

/// Returns the error of a commit that has changed the table directory and
/// that has not completed. The client of the write receives the error, and nothing
/// attempts the write again.
fn commit_state_unknown(
    message: impl Into<String>,
    source: Option<DataFusionError>,
) -> DataFusionError {
    let message = message.into();
    DataFusionError::External(Box::new(match source {
        Some(source) => CommitStateUnknownError::with_source(message, source),
        None => CommitStateUnknownError::from(message),
    }))
}

/// Completes a listing table write after all writer tasks have finished.
///
/// The input is the manifests of the writer tasks, which have staged
/// the files of the write. Nothing of the write is visible to the readers
/// of the table until the commit has received the manifests of all writer
/// tasks. The commit then publishes the files, and writes an empty `_SUCCESS`
/// file to the table directory, unless the write is not to have the file.
/// A write that overwrites deletes the old content of the table directory
/// when the files are published. A write that fails or is canceled before
/// the files are published leaves the content of the table as it was.
/// The output is a single row with the number of rows written.
///
/// The manifests are read from the time the plan is executed, to the end of
/// the input, by a task that is not canceled with the commit. The staged
/// files of every manifest that the input delivers are therefore published
/// or removed.
///
/// The commit is not stopped once it has started to publish the files.
/// A write that is canceled at that time is reported as canceled to its
/// client, and it is committed nevertheless when the process keeps running.
/// If the process stops then, the directory has a part of the files.
///
/// When the plan is executed for a job whose tasks can be attempted again,
/// the commit does not publish the files. It records them in the [`JobWrites`]
/// of the job, and the job runner publishes them when the job has succeeded.
/// The output is the same row, and the client of the job receives the end of
/// the output only after the files are published. The output has no row when
/// a writer task has not reported its files, which is not an error of
/// the commit: the job then fails with the error of the task that has failed.
#[derive(Debug, Clone)]
pub struct FileWriteCommitExec {
    input: Arc<dyn ExecutionPlan>,
    object_store_url: ObjectStoreUrl,
    table_path: Path,
    write_id: Uuid,
    /// Whether the write replaces the content of the table directory.
    overwrite: bool,
    /// Whether the `_SUCCESS` file is written.
    write_success_file: bool,
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
            write_success_file: true,
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

    /// Sets whether the `_SUCCESS` file is written when the write is completed.
    pub fn with_success_file(mut self, write_success_file: bool) -> Self {
        self.write_success_file = write_success_file;
        self
    }

    pub fn write_success_file(&self) -> bool {
        self.write_success_file
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
            .with_overwrite(self.overwrite)
            .with_success_file(self.write_success_file),
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
        let job = match (JobWrites::of(&context), TaskAttempt::of(&context)) {
            (Some(job), Some(attempt)) => Some((job, attempt)),
            (Some(_), None) => {
                return internal_err!(
                    "{} is executed for a job without the attempt of its task",
                    self.name()
                );
            }
            (None, _) => None,
        };
        let commit = WriteCommit {
            publisher: Publisher::try_new(&context, &self.object_store_url)?,
            table_path: self.table_path.clone(),
            write_id: self.write_id,
            overwrite: self.overwrite,
            write_success_file: self.write_success_file,
        };
        // The writer tasks of the input that run in this process are given
        // the means to stop them.
        let cancel = CancellationToken::new();
        let input_context =
            task_context_with_extension(&context, Arc::new(WriteCancellation(cancel.clone())));
        let commit = commit.start(self.input.execute(0, input_context)?, cancel, job)?;
        let schema = self.schema();
        let output = stream::once(async move {
            let Some(count) = commit.await? else {
                return Ok(None);
            };
            let count: ArrayRef = Arc::new(UInt64Array::from(vec![count]));
            Ok(Some(RecordBatch::try_new(count_schema(), vec![count])?))
        })
        .try_filter_map(|batch| future::ready(Ok(batch)));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, output)))
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::future::BoxFuture;
    use futures::stream::BoxStream;

    use super::*;
    use crate::listing::testing::{FaultStore, Mode};

    const WRITE_ID: Uuid = Uuid::from_u128(1);
    const OTHER_WRITE_ID: Uuid = Uuid::from_u128(2);

    struct TestWrite {
        store: Arc<FaultStore>,
        mode: Mode,
        publisher: Publisher,
        /// Whether the commit writes the `_SUCCESS` file.
        success_file: bool,
    }

    impl TestWrite {
        fn new(mode: Mode) -> Self {
            let store = FaultStore::new();
            let publisher = store.publisher(mode);
            Self {
                store,
                mode,
                publisher,
                success_file: true,
            }
        }

        /// Stages a file of four bytes and ten rows as a writer task does.
        async fn stage(&self, path: &str) -> Result<StagedFile> {
            self.stage_with(path, "data", &self.publisher).await
        }

        async fn stage_with(
            &self,
            path: &str,
            data: &'static str,
            publisher: &Publisher,
        ) -> Result<StagedFile> {
            let location = Path::from(path);
            // The file has two parts.
            let mut upload =
                publisher.upload(&Path::from("table"), &WRITE_ID, location.clone(), 2, 4);
            upload.put(Bytes::from_static(data.as_bytes())).await?;
            let publish = upload.finish().await?;
            Ok(StagedFile {
                location,
                size: data.len() as u64,
                num_rows: 10,
                publish,
            })
        }

        /// Creates an object that is in the table directory before the write.
        async fn put(&self, path: &str) -> Result<()> {
            let store = self.store.inner();
            store.put(&Path::from(path), "old".into()).await?;
            Ok(())
        }

        async fn read(&self, path: &str) -> Result<Bytes> {
            let store = self.store.inner();
            Ok(store.get(&Path::from(path)).await?.bytes().await?)
        }

        fn commit_future(
            &self,
            manifests: Vec<Result<RecordBatch>>,
            overwrite: bool,
        ) -> BoxFuture<'static, Result<u64>> {
            self.commit_stream(stream::iter(manifests).boxed(), overwrite)
        }

        fn commit_stream(
            &self,
            manifests: BoxStream<'static, Result<RecordBatch>>,
            overwrite: bool,
        ) -> BoxFuture<'static, Result<u64>> {
            self.commit_input(|_| manifests, overwrite)
        }

        /// Starts the commit, which reads the manifests from then on.
        /// The manifests are created with the means to stop the writer tasks.
        fn commit_input(
            &self,
            manifests: impl FnOnce(CancellationToken) -> BoxStream<'static, Result<RecordBatch>>,
            overwrite: bool,
        ) -> BoxFuture<'static, Result<u64>> {
            self.start(manifests, overwrite, None)
                .map(|count| match count? {
                    Some(count) => Ok(count),
                    None => internal_err!("the commit has no number of rows"),
                })
                .boxed()
        }

        /// Starts an attempt of the commit for a job, which reads the manifests
        /// from then on, and which has no number of rows when they are incomplete.
        fn prepare(
            &self,
            job: &Arc<JobWrites>,
            attempt: usize,
            manifests: Vec<Result<RecordBatch>>,
            overwrite: bool,
        ) -> BoxFuture<'static, Result<Option<u64>>> {
            let job = Some((Arc::clone(job), attempt));
            self.start(|_| stream::iter(manifests).boxed(), overwrite, job)
        }

        fn start(
            &self,
            manifests: impl FnOnce(CancellationToken) -> BoxStream<'static, Result<RecordBatch>>,
            overwrite: bool,
            job: Option<(Arc<JobWrites>, usize)>,
        ) -> BoxFuture<'static, Result<Option<u64>>> {
            // Only the requests of the commit are of interest.
            self.store.clear_requests();
            let commit = WriteCommit {
                publisher: self.publisher.clone(),
                table_path: Path::from("table"),
                write_id: WRITE_ID,
                overwrite,
                write_success_file: self.success_file,
            };
            let cancel = CancellationToken::new();
            let manifests = manifests(cancel.clone());
            let input = Box::pin(RecordBatchStreamAdapter::new(manifest_schema(), manifests));
            match commit.start(input, cancel, job) {
                Ok(commit) => commit.boxed(),
                Err(e) => futures::future::ready(Err(e)).boxed(),
            }
        }

        async fn commit(&self, manifests: Vec<Result<RecordBatch>>) -> Result<u64> {
            self.commit_future(manifests, false).await
        }

        async fn overwrite(&self, manifests: Vec<Result<RecordBatch>>) -> Result<u64> {
            self.commit_future(manifests, true).await
        }

        /// The requests of the commit, with the requests that publish files
        /// in the order of their locations. These requests are concurrent.
        fn requests(&self) -> Vec<String> {
            let mut requests = self.store.requests();
            let publish = format!("{} ", self.mode.publish_request());
            let mut published: Vec<_> = requests
                .iter()
                .filter(|x| x.starts_with(&publish))
                .cloned()
                .collect();
            published.sort();
            let mut published = published.into_iter();
            for request in requests.iter_mut() {
                if request.starts_with(&publish)
                    && let Some(next) = published.next()
                {
                    *request = next;
                }
            }
            requests
        }

        /// The request that publishes the file at the path.
        fn publish_request(&self, path: &str) -> String {
            match self.mode {
                Mode::Rename => format!("RENAME table/_staged-* {path}"),
                Mode::Multipart => format!("COMPLETE {path}"),
            }
        }

        /// The request that removes the staged file for the path.
        fn abort_request(&self, path: &str) -> String {
            match self.mode {
                Mode::Rename => "DELETE table/_staged-*".to_string(),
                Mode::Multipart => format!("ABORT {path}"),
            }
        }

        /// The objects that the staged files occupy before they are published.
        fn staged_objects(&self, files: usize) -> Vec<&'static str> {
            match self.mode {
                Mode::Rename => vec!["table/_staged-*"; files],
                Mode::Multipart => vec![],
            }
        }
    }

    fn manifest(partition: usize, partitions: usize, files: &[&StagedFile]) -> Result<RecordBatch> {
        let files: Vec<StagedFile> = files.iter().map(|x| (*x).clone()).collect();
        manifest_batch(partition, partitions, &files)
    }

    fn is_state_unknown(error: &DataFusionError) -> bool {
        matches!(error, DataFusionError::External(e) if e.is::<CommitStateUnknownError>())
    }

    #[test]
    fn test_job_writes_of_task_context() {
        use datafusion::prelude::SessionConfig;

        // The writes of a job are recorded in the task context of the job.
        let job = Arc::new(JobWrites::default());
        let config = SessionConfig::new().with_extension(Arc::clone(&job));
        let context = TaskContext::default().with_session_config(config);
        assert!(matches!(JobWrites::of(&context), Some(x) if Arc::ptr_eq(&x, &job)));
        // A plan that is executed without a job publishes the files of its write.
        assert!(JobWrites::of(&TaskContext::default()).is_none());
    }

    #[test]
    fn test_manifest_round_trip() -> Result<()> {
        let files = vec![
            StagedFile {
                location: Path::from("table/a.parquet"),
                size: 3,
                num_rows: 4,
                publish: Publish::Rename {
                    staged: Path::from("table/_staged-x"),
                },
            },
            StagedFile {
                location: Path::from("table/p=1/b.parquet"),
                size: 5,
                num_rows: 6,
                publish: Publish::Multipart {
                    upload_id: "upload".to_string(),
                    parts: vec!["x".to_string(), String::new(), "z".to_string()],
                },
            },
            StagedFile {
                location: Path::from("table/c.parquet"),
                size: 0,
                num_rows: 0,
                publish: Publish::Multipart {
                    upload_id: String::new(),
                    parts: vec![],
                },
            },
        ];
        let batch = manifest_batch(1, 2, &files)?;
        assert_eq!(batch.schema(), manifest_schema());
        assert_eq!(batch.num_rows(), 3);
        let mut manifests = Manifests::default();
        let mut received = vec![];
        manifests.add(&batch, &mut received)?;
        assert_eq!(received, files);
        assert_eq!(manifests.num_rows, 10);
        // The other partition has not reported.
        assert!(manifests.validate().is_err());

        // A task without files reports a single row without a file.
        let batch = manifest_batch(0, 2, &[])?;
        assert_eq!(batch.num_rows(), 1);
        manifests.add(&batch, &mut received)?;
        assert_eq!(received, files);
        assert_eq!(manifests.validate(), Ok(()));
        Ok(())
    }

    #[tokio::test]
    async fn test_commit_publishes_the_files_of_the_manifests() -> Result<()> {
        for mode in Mode::ALL {
            let write = TestWrite::new(mode);
            write.put("table/_SUCCESS").await?;
            write.put("table/old.parquet").await?;
            let a = write.stage("table/a.parquet").await?;
            let b = write.stage("table/p=1/b.parquet").await?;
            let c = write.stage("table/c.parquet").await?;

            // Nothing of the write is visible before the commit.
            let mut expected = vec!["table/_SUCCESS"];
            expected.extend(write.staged_objects(3));
            expected.push("table/old.parquet");
            assert_eq!(write.store.objects().await?, expected, "{mode:?}");

            let manifests = vec![
                manifest(1, 3, &[&b]),
                manifest(0, 3, &[&a, &c]),
                manifest(2, 3, &[]),
            ];
            assert_eq!(write.commit(manifests).await?, 30);
            // Each file takes one request, and the directory is not listed.
            assert_eq!(
                write.requests(),
                vec![
                    write.publish_request("table/a.parquet"),
                    write.publish_request("table/c.parquet"),
                    write.publish_request("table/p=1/b.parquet"),
                    "PUT table/_SUCCESS 0".to_string(),
                ],
                "{mode:?}"
            );
            assert_eq!(
                write.store.objects().await?,
                vec![
                    "table/_SUCCESS",
                    "table/a.parquet",
                    "table/c.parquet",
                    "table/old.parquet",
                    "table/p=1/b.parquet",
                ],
                "{mode:?}"
            );
            assert_eq!(write.store.open_uploads(), Vec::<String>::new());
            assert_eq!(write.read("table/a.parquet").await?.as_ref(), b"data");
            assert_eq!(write.read("table/_SUCCESS").await?.as_ref(), b"");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_commit_of_write_without_files() -> Result<()> {
        for mode in Mode::ALL {
            let write = TestWrite::new(mode);
            let manifests = vec![manifest(0, 2, &[]), manifest(1, 2, &[])];
            assert_eq!(write.commit(manifests).await?, 0);
            assert_eq!(write.requests(), vec!["PUT table/_SUCCESS 0"]);

            let mut write = TestWrite::new(mode);
            write.success_file = false;
            let a = write.stage("table/a.parquet").await?;
            assert_eq!(write.commit(vec![manifest(0, 1, &[&a])]).await?, 10);
            assert_eq!(
                write.requests(),
                vec![write.publish_request("table/a.parquet")]
            );
            assert_eq!(write.store.objects().await?, vec!["table/a.parquet"]);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_commit_rejects_incomplete_manifests() -> Result<()> {
        for mode in Mode::ALL {
            for (case, message) in [
                "2 of 3 writer tasks",
                "1 of 2 writer tasks",
                "no writer task has reported",
            ]
            .into_iter()
            .enumerate()
            {
                let write = TestWrite::new(mode);
                write.put("table/_SUCCESS").await?;
                write.put("table/old.parquet").await?;
                let a = write.stage("table/a.parquet").await?;
                let b = write.stage("table/b.parquet").await?;
                let c = write.stage("table/c.parquet").await?;
                let manifests = match case {
                    // A task has not reported.
                    0 => vec![manifest(0, 3, &[&a]), manifest(2, 3, &[&b, &c])],
                    // A row of the manifest of a task with two files is missing.
                    1 => vec![
                        manifest(0, 2, &[&a]),
                        Ok(manifest(1, 2, &[&b, &c])?.slice(0, 1)),
                    ],
                    _ => vec![],
                };
                let result = write.overwrite(manifests).await;
                let error = match result {
                    Err(DataFusionError::Execution(e)) => e,
                    result => return internal_err!("unexpected result: {result:?}"),
                };
                assert!(error.contains("the write is incomplete"), "{error}");
                assert!(error.contains(message), "{error}");
                assert!(error.contains("its error is in the logs"), "{error}");
                // The table is as it was, and the staged files that the commit
                // has received are removed.
                assert_eq!(write.store.count(mode.publish_request()), 0);
                assert_eq!(write.store.count("PUT"), 0);
                assert_eq!(write.store.count("LIST"), 0);
                // The staged files that the commit has not received stay.
                let missing = [0, 1, 3][case];
                let mut expected = vec!["table/_SUCCESS"];
                expected.extend(write.staged_objects(missing));
                expected.push("table/old.parquet");
                assert_eq!(write.store.objects().await?, expected, "{mode:?} {message}");
                let uploads = match mode {
                    Mode::Rename => 0,
                    Mode::Multipart => missing,
                };
                assert_eq!(write.store.open_uploads().len(), uploads);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_commit_rejects_duplicate_manifests() -> Result<()> {
        for mode in Mode::ALL {
            // Two attempts of a task use the same locations for their files.
            let cases: Vec<(Vec<Vec<&str>>, _)> = vec![
                (vec![vec!["a"], vec!["a"]], "more than one manifest"),
                (vec![vec!["a", "b"], vec!["a"]], "more than one manifest"),
                (vec![vec!["a"], vec![]], "more than one manifest"),
                (vec![vec![], vec!["a"]], "more than one manifest"),
                (vec![vec![], vec![]], "more than one manifest"),
            ];
            for (attempts, message) in cases {
                let write = TestWrite::new(mode);
                let mut manifests = vec![manifest(1, 2, &[])];
                for attempt in &attempts {
                    let mut files = vec![];
                    for name in attempt {
                        files.push(write.stage(&format!("table/{name}.parquet")).await?);
                    }
                    manifests.push(manifest(0, 2, &files.iter().collect::<Vec<_>>()));
                }
                let result = write.commit(manifests).await;
                assert!(
                    matches!(&result, Err(DataFusionError::Internal(e)) if e.contains(message)),
                    "{mode:?} {attempts:?} {result:?}"
                );
                // The files of both attempts are removed.
                write.store.wait_for(&[]).await?;
                assert_eq!(write.store.count(mode.publish_request()), 0);
            }

            // The same location in the manifests of two tasks.
            let write = TestWrite::new(mode);
            let a = write.stage("table/a.parquet").await?;
            let other = write.stage("table/a.parquet").await?;
            let manifests = vec![manifest(0, 2, &[&a]), manifest(1, 2, &[&other])];
            let result = write.commit(manifests).await;
            assert!(
                matches!(&result, Err(DataFusionError::Internal(e)) if e.contains("more than one manifest")),
                "{result:?}"
            );
            write.store.wait_for(&[]).await?;

            let write = TestWrite::new(mode);
            let a = write.stage("table/a.parquet").await?;
            let b = write.stage("table/b.parquet").await?;
            let manifests = vec![manifest(0, 2, &[&a]), manifest(1, 3, &[&b])];
            let result = write.commit(manifests).await;
            assert!(
                matches!(&result, Err(DataFusionError::Internal(e)) if e.contains("disagree on the number of partitions")),
                "{result:?}"
            );
            write.store.wait_for(&[]).await?;

            // The task is not a task of the write.
            let write = TestWrite::new(mode);
            let a = write.stage("table/a.parquet").await?;
            let result = write.commit(vec![manifest(1, 1, &[&a])]).await;
            assert!(
                matches!(&result, Err(DataFusionError::Internal(e)) if e.contains("is not one of the 1 partitions")),
                "{result:?}"
            );
            write.store.wait_for(&[]).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_commit_removes_received_files_when_its_input_fails() -> Result<()> {
        for mode in Mode::ALL {
            let write = TestWrite::new(mode);
            write.put("table/old.parquet").await?;
            let a = write.stage("table/a.parquet").await?;
            let b = write.stage("table/b.parquet").await?;
            // A manifest that follows the error of another writer task is taken as well.
            let manifests = vec![
                manifest(0, 3, &[&a]),
                exec_err!("writer failure"),
                manifest(1, 3, &[&b]),
            ];
            let result = write.commit(manifests).await;
            assert!(
                matches!(&result, Err(e) if e.to_string().contains("writer failure")),
                "{result:?}"
            );
            assert_eq!(
                write.requests(),
                vec![
                    write.abort_request("table/a.parquet"),
                    write.abort_request("table/b.parquet"),
                ]
            );
            assert_eq!(write.store.objects().await?, vec!["table/old.parquet"]);
            assert_eq!(write.store.open_uploads(), Vec::<String>::new());
        }
        Ok(())
    }

    #[tokio::test]
    #[expect(clippy::panic)]
    async fn test_commit_reports_the_panic_of_a_writer_task() -> Result<()> {
        for mode in Mode::ALL {
            let write = TestWrite::new(mode);
            write.put("table/old.parquet").await?;
            let a = write.stage("table/a.parquet").await?;
            let a = manifest(0, 2, &[&a]);
            let panic = stream::poll_fn(|_| -> std::task::Poll<Option<Result<RecordBatch>>> {
                std::panic::panic_any("writer panic".to_string())
            });
            let manifests = stream::iter(vec![a]).chain(panic).boxed();
            let result = write.commit_stream(manifests, true).await;
            assert!(
                matches!(&result, Err(DataFusionError::Internal(e)) if e.contains("has panicked: writer panic")),
                "{result:?}"
            );
            assert_eq!(
                write.requests(),
                vec![write.abort_request("table/a.parquet")]
            );
            assert_eq!(write.store.objects().await?, vec!["table/old.parquet"]);
            assert_eq!(write.store.open_uploads(), Vec::<String>::new());
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_canceled_commit_removes_received_files() -> Result<()> {
        for mode in Mode::ALL {
            let write = TestWrite::new(mode);
            write.put("table/old.parquet").await?;
            let a = write.stage("table/a.parquet").await?;
            // The commit waits for the other manifest. The input ends when
            // the commit stops the writer task that is running.
            let a = manifest(0, 2, &[&a]);
            let manifests = |cancel: CancellationToken| {
                let running =
                    stream::once(cancel.cancelled_owned()).filter_map(|()| async { None });
                stream::iter(vec![a]).chain(running).boxed()
            };
            let commit = write.commit_input(manifests, true);
            let waiting = tokio::time::timeout(std::time::Duration::from_millis(50), commit);
            assert!(waiting.await.is_err());
            write.store.wait_for(&["table/old.parquet"]).await?;
            assert_eq!(
                write.requests(),
                vec![write.abort_request("table/a.parquet")]
            );
        }
        Ok(())
    }

    /// The manifests are in the input of the commit when the commit is
    /// canceled before it has read them.
    #[tokio::test]
    async fn test_canceled_commit_removes_files_of_manifests_that_it_has_not_read() -> Result<()> {
        for mode in Mode::ALL {
            let write = TestWrite::new(mode);
            write.put("table/old.parquet").await?;
            let a = write.stage("table/a.parquet").await?;
            let b = write.stage("table/b.parquet").await?;
            let manifests = vec![manifest(0, 2, &[&a]), manifest(1, 2, &[&b])];
            drop(write.commit_future(manifests, false));
            write.store.wait_for(&["table/old.parquet"]).await?;
            assert_eq!(write.store.count(mode.publish_request()), 0);
        }
        Ok(())
    }

    /// The manifests are read, and the commit is canceled before it has
    /// received them.
    #[tokio::test]
    async fn test_canceled_commit_removes_files_of_manifests_that_it_has_not_received() -> Result<()>
    {
        for mode in Mode::ALL {
            let write = TestWrite::new(mode);
            write.put("table/old.parquet").await?;
            let a = write.stage("table/a.parquet").await?;
            let b = write.stage("table/b.parquet").await?;
            let manifests = vec![manifest(0, 2, &[&a]), manifest(1, 2, &[&b])];
            let commit = write.commit_future(manifests, false);
            // The input is read to its end while the commit is not polled.
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            assert_eq!(write.requests(), Vec::<String>::new());
            drop(commit);
            write.store.wait_for(&["table/old.parquet"]).await?;
            assert_eq!(write.store.count(mode.publish_request()), 0);
            assert_eq!(write.store.open_uploads(), Vec::<String>::new());
        }
        Ok(())
    }

    /// A writer task can hand over its manifest while the write is canceled.
    #[tokio::test]
    async fn test_canceled_commit_removes_files_of_manifests_that_arrive_later() -> Result<()> {
        for mode in Mode::ALL {
            let write = TestWrite::new(mode);
            write.put("table/old.parquet").await?;
            let a = write.stage("table/a.parquet").await?;
            let b = write.stage("table/b.parquet").await?;
            let (a, b) = (manifest(0, 2, &[&a]), manifest(1, 2, &[&b]));
            let manifests = |cancel: CancellationToken| {
                let late = stream::once(async move {
                    cancel.cancelled().await;
                    b
                });
                stream::iter(vec![a]).chain(late).boxed()
            };
            let commit = write.commit_input(manifests, false);
            let waiting = tokio::time::timeout(std::time::Duration::from_millis(50), commit);
            assert!(waiting.await.is_err());
            write.store.wait_for(&["table/old.parquet"]).await?;
            assert_eq!(write.store.count(mode.publish_request()), 0);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_canceled_commit_publishes_all_files_once_it_has_started() -> Result<()> {
        for mode in Mode::ALL {
            let write = TestWrite::new(mode);
            write.put("table/old.parquet").await?;
            let a = write.stage("table/a.parquet").await?;
            let b = write.stage("table/b.parquet").await?;
            let gate = write.store.hold(mode.publish_request());
            let manifests = vec![manifest(0, 2, &[&a]), manifest(1, 2, &[&b])];
            let mut commit = Box::pin(write.commit_future(manifests, true));
            tokio::select! {
                _ = &mut commit => return internal_err!("the commit must wait"),
                _ = gate.wait_for(2) => {}
            }
            // The commit is canceled while the files are being published.
            drop(commit);
            gate.open();
            write
                .store
                .wait_for(&["table/_SUCCESS", "table/a.parquet", "table/b.parquet"])
                .await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_commit_publishes_again_after_a_failure() -> Result<()> {
        for mode in Mode::ALL {
            let write = TestWrite::new(mode);
            let a = write.stage("table/a.parquet").await?;
            let b = write.stage("table/b.parquet").await?;
            let c = write.stage("table/c.parquet").await?;
            // The second request to publish a file fails once.
            write.store.fail(mode.publish_request(), 1);
            let manifests = vec![manifest(0, 1, &[&a, &b, &c])];
            assert_eq!(write.commit(manifests).await?, 30);
            assert_eq!(write.store.count(mode.publish_request()), 4);
            // The location is checked after the failure.
            assert_eq!(write.store.count("HEAD"), 1);
            assert_eq!(write.store.count("DELETE"), 0);
            assert_eq!(write.store.count("ABORT"), 0);
            assert_eq!(
                write.store.objects().await?,
                vec![
                    "table/_SUCCESS",
                    "table/a.parquet",
                    "table/b.parquet",
                    "table/c.parquet"
                ]
            );
            assert_eq!(write.store.open_uploads(), Vec::<String>::new());
        }
        Ok(())
    }

    /// A request that is sent again after its response was lost fails,
    /// since the file is published already.
    #[tokio::test]
    async fn test_publish_accepts_a_file_that_is_published() -> Result<()> {
        for mode in Mode::ALL {
            let write = TestWrite::new(mode);
            let a = write.stage("table/a.parquet").await?;
            let b = write.stage("table/b.parquet").await?;
            write.store.lose_response(mode.publish_request(), 0);
            let manifests = vec![manifest(0, 1, &[&a, &b])];
            assert_eq!(write.commit(manifests).await?, 20);
            assert_eq!(write.store.count(mode.publish_request()), 2);
            assert_eq!(write.store.count("HEAD"), 1);
            assert_eq!(
                write.store.objects().await?,
                vec!["table/_SUCCESS", "table/a.parquet", "table/b.parquet"]
            );

            // The file is published again.
            write.publisher.publish(&a).await?;
            assert_eq!(write.store.count(mode.publish_request()), 3);
            assert_eq!(write.store.count("HEAD"), 2);
            assert_eq!(write.read("table/a.parquet").await?.as_ref(), b"data");

            // An object of another size at the location is not the file.
            let mut other = a.clone();
            other.size += 1;
            assert!(write.publisher.publish(&other).await.is_err());
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_commit_undoes_a_publication_that_fails() -> Result<()> {
        for mode in Mode::ALL {
            for overwrite in [false, true] {
                let write = TestWrite::new(mode);
                let old = [
                    "table/_SUCCESS",
                    "table/old.parquet",
                    "table/p=1/old.parquet",
                ];
                for path in old {
                    write.put(path).await?;
                }
                let a = write.stage("table/a.parquet").await?;
                let b = write.stage("table/b.parquet").await?;
                let c = write.stage("table/c.parquet").await?;
                write.store.fail_at(mode.publish_request(), "b.parquet");
                let manifests = vec![manifest(0, 1, &[&a, &b, &c])];
                let result = write.commit_future(manifests, overwrite).await;
                let error = match result {
                    Err(e) => e,
                    Ok(_) => return internal_err!("the commit must fail"),
                };
                // The write can be attempted again.
                assert!(!is_state_unknown(&error), "{error}");
                let message = error.to_string();
                assert!(message.contains("as it was before the write"), "{message}");
                assert!(message.contains("failure"), "{message}");
                // The file is tried twice. The files that were published are
                // deleted, and the staged file is removed.
                let requests = write.requests();
                assert_eq!(write.store.count(mode.publish_request()), 4);
                for request in [
                    write.abort_request("table/b.parquet"),
                    "DELETE table/a.parquet".to_string(),
                    "DELETE table/c.parquet".to_string(),
                ] {
                    assert!(requests.contains(&request), "{request} {requests:?}");
                }
                assert_eq!(
                    requests.contains(&"PUT table/_SUCCESS 0".to_string()),
                    overwrite
                );
                assert_eq!(write.store.objects().await?, old, "{mode:?} {overwrite}");
                assert_eq!(write.store.open_uploads(), Vec::<String>::new());
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_commit_reports_unknown_state_when_it_cannot_be_undone() -> Result<()> {
        for mode in Mode::ALL {
            for overwrite in [false, true] {
                let write = TestWrite::new(mode);
                write.put("table/_SUCCESS").await?;
                write.put("table/old.parquet").await?;
                let a = write.stage("table/a.parquet").await?;
                let b = write.stage("table/b.parquet").await?;
                let c = write.stage("table/c.parquet").await?;
                write.store.fail_at(mode.publish_request(), "b.parquet");
                write.store.fail_at("DELETE", "a.parquet");
                let manifests = vec![manifest(0, 1, &[&a, &b, &c])];
                let result = write.commit_future(manifests, overwrite).await;
                assert!(
                    matches!(&result, Err(e) if is_state_unknown(e)),
                    "{result:?}"
                );
                // The directory has a file of the write, and no `_SUCCESS` file.
                assert_eq!(
                    write.store.objects().await?,
                    vec!["table/a.parquet", "table/old.parquet"],
                    "{mode:?} {overwrite}"
                );
                assert_eq!(write.store.open_uploads(), Vec::<String>::new());
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_overwrite_replaces_the_content_when_the_files_are_published() -> Result<()> {
        for mode in Mode::ALL {
            let write = TestWrite::new(mode);
            // The objects that another write has staged are kept, with the names
            // that the object stores give them while they are written.
            let staged = [
                format!("table/_staged-{OTHER_WRITE_ID}-{OTHER_WRITE_ID}"),
                format!("table/._staged-{OTHER_WRITE_ID}-{OTHER_WRITE_ID}.tmp"),
            ];
            let old = [
                "table/.old.parquet.crc",
                "table/_SUCCESS",
                "table/_metadata",
                "table/_started",
                "table/old.parquet",
                "table/p=1/old.parquet",
            ];
            for path in old.iter().copied().chain(staged.iter().map(|x| x.as_str())) {
                write.put(path).await?;
            }
            let a = write.stage("table/a.parquet").await?;
            let b = write.stage("table/p=1/b.parquet").await?;
            let manifests = vec![manifest(0, 1, &[&a, &b])];
            assert_eq!(write.overwrite(manifests).await?, 20);
            // The old content is deleted after the files are published.
            assert_eq!(
                write.requests(),
                vec![
                    "LIST table".to_string(),
                    "DELETE table/_SUCCESS".to_string(),
                    write.publish_request("table/a.parquet"),
                    write.publish_request("table/p=1/b.parquet"),
                    "DELETE table/.old.parquet.crc".to_string(),
                    "DELETE table/_metadata".to_string(),
                    "DELETE table/_started".to_string(),
                    "DELETE table/old.parquet".to_string(),
                    "DELETE table/p=1/old.parquet".to_string(),
                    "PUT table/_SUCCESS 0".to_string(),
                ],
                "{mode:?}"
            );
            assert_eq!(
                write.store.objects().await?,
                vec![
                    "table/._staged-*.tmp",
                    "table/_SUCCESS",
                    "table/_staged-*",
                    "table/a.parquet",
                    "table/p=1/b.parquet",
                ]
            );
            assert_eq!(write.read("table/_SUCCESS").await?.as_ref(), b"");

            // A write without files leaves an empty table.
            let mut write = TestWrite::new(mode);
            write.success_file = false;
            write.put("table/_SUCCESS").await?;
            write.put("table/old.parquet").await?;
            assert_eq!(write.overwrite(vec![manifest(0, 1, &[])]).await?, 0);
            assert_eq!(write.store.objects().await?, Vec::<String>::new());

            // The directory does not exist.
            let write = TestWrite::new(mode);
            assert_eq!(write.overwrite(vec![manifest(0, 1, &[])]).await?, 0);
            assert_eq!(write.requests(), vec!["LIST table", "PUT table/_SUCCESS 0"]);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_overwrite_that_fails_before_it_publishes_keeps_the_table() -> Result<()> {
        for mode in Mode::ALL {
            for failure in ["LIST", "DELETE"] {
                let write = TestWrite::new(mode);
                let old = ["table/_SUCCESS", "table/old.parquet"];
                for path in old {
                    write.put(path).await?;
                }
                let a = write.stage("table/a.parquet").await?;
                write.store.fail(failure, 0);
                let result = write.overwrite(vec![manifest(0, 1, &[&a])]).await;
                assert!(
                    matches!(&result, Err(e) if !is_state_unknown(e) && e.to_string().contains("failure")),
                    "{result:?}"
                );
                assert_eq!(write.store.objects().await?, old, "{mode:?} {failure}");
                assert_eq!(write.store.open_uploads(), Vec::<String>::new());
                assert_eq!(write.store.count(mode.publish_request()), 0);
            }

            // A writer task fails, so the commit does not change the directory.
            let write = TestWrite::new(mode);
            write.put("table/_SUCCESS").await?;
            write.put("table/old.parquet").await?;
            let a = write.stage("table/a.parquet").await?;
            let manifests = vec![manifest(0, 2, &[&a]), exec_err!("writer failure")];
            assert!(write.overwrite(manifests).await.is_err());
            assert_eq!(
                write.requests(),
                vec![write.abort_request("table/a.parquet")]
            );
            assert_eq!(
                write.store.objects().await?,
                vec!["table/_SUCCESS", "table/old.parquet"]
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_commit_reports_unknown_state_when_it_fails_after_it_has_published() -> Result<()>
    {
        for mode in Mode::ALL {
            // The old content cannot be deleted.
            let write = TestWrite::new(mode);
            write.put("table/_SUCCESS").await?;
            write.put("table/old.parquet").await?;
            let a = write.stage("table/a.parquet").await?;
            write.store.fail_at("DELETE", "old.parquet");
            let result = write.overwrite(vec![manifest(0, 1, &[&a])]).await;
            assert!(
                matches!(&result, Err(e) if is_state_unknown(e)),
                "{result:?}"
            );
            assert_eq!(
                write.store.objects().await?,
                vec!["table/a.parquet", "table/old.parquet"]
            );

            // The `_SUCCESS` file cannot be written.
            for overwrite in [false, true] {
                let write = TestWrite::new(mode);
                let a = write.stage("table/a.parquet").await?;
                write.store.fail("PUT", 0);
                let manifests = vec![manifest(0, 1, &[&a])];
                let result = write.commit_future(manifests, overwrite).await;
                assert!(
                    matches!(&result, Err(e) if is_state_unknown(e)),
                    "{result:?}"
                );
                assert_eq!(write.store.objects().await?, vec!["table/a.parquet"]);
            }
        }
        Ok(())
    }

    /// An attempt of a writer task that the driver has given up on can complete
    /// its files after the write is committed.
    #[tokio::test]
    async fn test_attempt_that_completes_after_the_commit_is_not_visible() -> Result<()> {
        for mode in Mode::ALL {
            let write = TestWrite::new(mode);
            let a = write.stage("table/a.parquet").await?;
            // Another attempt of the task is uploading the same file.
            let mut zombie = write.publisher.upload(
                &Path::from("table"),
                &WRITE_ID,
                Path::from("table/a.parquet"),
                2,
                4,
            );
            zombie.put(Bytes::from_static(b"zom")).await?;
            assert_eq!(write.commit(vec![manifest(0, 1, &[&a])]).await?, 10);
            let committed = vec!["table/_SUCCESS", "table/a.parquet"];
            assert_eq!(write.store.objects().await?, committed);

            zombie.put(Bytes::from_static(b"bie")).await?;
            zombie.finish().await?;
            let mut expected = vec!["table/_SUCCESS"];
            expected.extend(write.staged_objects(1));
            expected.push("table/a.parquet");
            assert_eq!(write.store.objects().await?, expected, "{mode:?}");
            assert_eq!(write.read("table/a.parquet").await?.as_ref(), b"data");
            // What the attempt has staged stays until it is removed by other
            // means, since no commit receives its manifest.
            assert_eq!(
                write.store.open_uploads().len(),
                usize::from(mode == Mode::Multipart)
            );
        }
        Ok(())
    }

    /// The task of the commit of a job does not publish the files.
    #[tokio::test]
    async fn test_job_publishes_the_files_when_it_has_succeeded() -> Result<()> {
        for mode in Mode::ALL {
            for overwrite in [false, true] {
                let write = TestWrite::new(mode);
                write.put("table/_SUCCESS").await?;
                write.put("table/old.parquet").await?;
                let a = write.stage("table/a.parquet").await?;
                let b = write.stage("table/b.parquet").await?;
                let job = Arc::new(JobWrites::default());
                assert!(job.is_empty());
                let manifests = vec![manifest(0, 2, &[&a]), manifest(1, 2, &[&b])];
                assert_eq!(
                    write.prepare(&job, 0, manifests, overwrite).await?,
                    Some(20)
                );
                // The task has ended, and the table is as it was.
                assert!(!job.is_empty());
                assert_eq!(write.requests(), Vec::<String>::new());
                let mut expected = vec!["table/_SUCCESS"];
                expected.extend(write.staged_objects(2));
                expected.push("table/old.parquet");
                assert_eq!(write.store.objects().await?, expected);

                job.commit().await?;
                assert!(job.is_empty());
                let mut committed = vec!["table/_SUCCESS", "table/a.parquet", "table/b.parquet"];
                if !overwrite {
                    committed.push("table/old.parquet");
                }
                assert_eq!(write.store.objects().await?, committed);
                assert_eq!(write.store.open_uploads(), Vec::<String>::new());
                // The steps are the steps of a commit that publishes the files itself.
                let requests = write.requests();
                let first = if overwrite {
                    "LIST table"
                } else {
                    mode.publish_request()
                };
                assert!(requests[0].starts_with(first), "{requests:?}");
                assert_eq!(
                    requests.last().map(|x| x.as_str()),
                    Some("PUT table/_SUCCESS 0")
                );
                // The job is committed once.
                job.commit().await?;
                assert_eq!(write.requests(), requests);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_job_removes_the_files_when_it_has_failed() -> Result<()> {
        for mode in Mode::ALL {
            let write = TestWrite::new(mode);
            write.put("table/_SUCCESS").await?;
            write.put("table/old.parquet").await?;
            let a = write.stage("table/a.parquet").await?;
            let job = Arc::new(JobWrites::default());
            let manifests = vec![manifest(0, 1, &[&a])];
            assert_eq!(write.prepare(&job, 0, manifests, true).await?, Some(10));
            job.abort().await;
            assert!(job.is_empty());
            assert_eq!(
                write.requests(),
                vec![write.abort_request("table/a.parquet")]
            );
            let table = vec!["table/_SUCCESS", "table/old.parquet"];
            assert_eq!(write.store.objects().await?, table);
            assert_eq!(write.store.open_uploads(), Vec::<String>::new());

            // The files are removed also when the work is not awaited.
            let a = write.stage("table/a.parquet").await?;
            let job = Arc::new(JobWrites::default());
            let manifests = vec![manifest(0, 1, &[&a])];
            assert_eq!(write.prepare(&job, 0, manifests, true).await?, Some(10));
            drop(job.abort());
            write.store.wait_for(&table).await?;

            // An attempt that ends after the job removes its files.
            let a = write.stage("table/a.parquet").await?;
            let result = write
                .prepare(&job, 1, vec![manifest(0, 1, &[&a])], true)
                .await;
            assert!(
                matches!(&result, Err(DataFusionError::Internal(e)) if e.contains("not the latest attempt")),
                "{result:?}"
            );
            assert_eq!(write.store.objects().await?, table);
            assert_eq!(write.store.open_uploads(), Vec::<String>::new());
            job.commit().await?;
            assert_eq!(write.store.objects().await?, table);
        }
        Ok(())
    }

    /// The tasks of a job are attempted again after the task of the commit
    /// has ended, for example when a task has failed whose output had ended
    /// for the writer tasks like a complete output.
    #[tokio::test]
    async fn test_job_publishes_the_files_of_the_latest_attempt() -> Result<()> {
        for mode in Mode::ALL {
            let write = TestWrite::new(mode);
            let publisher = &write.publisher;
            let job = Arc::new(JobWrites::default());
            // The attempts use the same locations for their files.
            let first = write
                .stage_with("table/a.parquet", "one", publisher)
                .await?;
            let extra = write.stage("table/b.parquet").await?;
            let manifests = vec![manifest(0, 1, &[&first, &extra])];
            assert_eq!(write.prepare(&job, 0, manifests, false).await?, Some(20));
            let second = write
                .stage_with("table/a.parquet", "two", publisher)
                .await?;
            let manifests = vec![manifest(0, 1, &[&second])];
            assert_eq!(write.prepare(&job, 2, manifests, false).await?, Some(10));
            // The files of the first attempt are removed.
            let mut removed = write.requests();
            removed.sort();
            assert_eq!(
                removed,
                vec![
                    write.abort_request("table/a.parquet"),
                    write.abort_request("table/b.parquet"),
                ]
            );
            // An earlier attempt that ends later is not recorded.
            let late = write
                .stage_with("table/a.parquet", "late", publisher)
                .await?;
            let result = write
                .prepare(&job, 1, vec![manifest(0, 1, &[&late])], false)
                .await;
            assert!(
                matches!(&result, Err(DataFusionError::Internal(e)) if e.contains("not the latest attempt")),
                "{result:?}"
            );
            assert_eq!(
                write.requests(),
                vec![write.abort_request("table/a.parquet")]
            );

            job.commit().await?;
            let committed = vec!["table/_SUCCESS", "table/a.parquet"];
            assert_eq!(write.store.objects().await?, committed);
            assert_eq!(write.read("table/a.parquet").await?.as_ref(), b"two");
            assert_eq!(write.store.open_uploads(), Vec::<String>::new());
        }
        Ok(())
    }

    /// A writer task that has failed can end its output like a complete output.
    /// The commit of a job does not add an error to the error of that task.
    #[tokio::test]
    async fn test_incomplete_manifests_are_not_an_error_of_the_commit_of_a_job() -> Result<()> {
        for mode in Mode::ALL {
            let write = TestWrite::new(mode);
            write.put("table/old.parquet").await?;
            let a = write.stage("table/a.parquet").await?;
            let job = Arc::new(JobWrites::default());
            let manifests = vec![manifest(0, 2, &[&a])];
            assert_eq!(write.prepare(&job, 0, manifests, true).await?, None);
            // The staged files that the commit has received are removed.
            assert_eq!(
                write.requests(),
                vec![write.abort_request("table/a.parquet")]
            );
            assert_eq!(write.store.objects().await?, vec!["table/old.parquet"]);
            assert_eq!(write.store.open_uploads(), Vec::<String>::new());

            // The job runner does not let such a job succeed. If it did,
            // the write would fail, and nothing would be published.
            let result = job.commit().await;
            assert!(
                matches!(&result, Err(DataFusionError::Execution(e)) if e.contains("the write is incomplete")),
                "{result:?}"
            );
            assert_eq!(write.store.objects().await?, vec!["table/old.parquet"]);

            // The tasks are attempted again, and the manifests are complete.
            let job = Arc::new(JobWrites::default());
            assert_eq!(write.prepare(&job, 0, vec![], true).await?, None);
            let a = write.stage("table/a.parquet").await?;
            let manifests = vec![manifest(0, 1, &[&a])];
            assert_eq!(write.prepare(&job, 1, manifests, true).await?, Some(10));
            job.commit().await?;
            let committed = vec!["table/_SUCCESS", "table/a.parquet"];
            assert_eq!(write.store.objects().await?, committed);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_job_reports_the_error_of_a_write_that_is_not_published() -> Result<()> {
        for mode in Mode::ALL {
            // The files cannot be published, and the publication is undone.
            let write = TestWrite::new(mode);
            write.put("table/old.parquet").await?;
            let a = write.stage("table/a.parquet").await?;
            let b = write.stage("table/b.parquet").await?;
            let job = Arc::new(JobWrites::default());
            let manifests = vec![manifest(0, 1, &[&a, &b])];
            assert_eq!(write.prepare(&job, 0, manifests, false).await?, Some(20));
            write.store.fail_at(mode.publish_request(), "b.parquet");
            let result = job.commit().await;
            assert!(
                matches!(&result, Err(e) if !is_state_unknown(e) && e.to_string().contains("failure")),
                "{result:?}"
            );
            assert_eq!(write.store.objects().await?, vec!["table/old.parquet"]);
            assert_eq!(write.store.open_uploads(), Vec::<String>::new());

            // The files are published, and the `_SUCCESS` file cannot be written.
            let write = TestWrite::new(mode);
            let a = write.stage("table/a.parquet").await?;
            let job = Arc::new(JobWrites::default());
            let manifests = vec![manifest(0, 1, &[&a])];
            assert_eq!(write.prepare(&job, 0, manifests, false).await?, Some(10));
            write.store.fail("PUT", 0);
            let result = job.commit().await;
            assert!(
                matches!(&result, Err(e) if is_state_unknown(e)),
                "{result:?}"
            );
            assert_eq!(write.store.objects().await?, vec!["table/a.parquet"]);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_job_that_is_canceled_while_it_publishes_is_committed() -> Result<()> {
        for mode in Mode::ALL {
            let write = TestWrite::new(mode);
            let a = write.stage("table/a.parquet").await?;
            let b = write.stage("table/b.parquet").await?;
            let job = Arc::new(JobWrites::default());
            let manifests = vec![manifest(0, 1, &[&a, &b])];
            assert_eq!(write.prepare(&job, 0, manifests, false).await?, Some(20));
            let gate = write.store.hold(mode.publish_request());
            let mut commit = Box::pin(job.commit());
            tokio::select! {
                _ = &mut commit => return internal_err!("the job must wait"),
                _ = gate.wait_for(2) => {}
            }
            drop(commit);
            gate.open();
            write
                .store
                .wait_for(&["table/_SUCCESS", "table/a.parquet", "table/b.parquet"])
                .await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_commit_rejects_files_that_are_staged_in_another_way() -> Result<()> {
        for mode in Mode::ALL {
            let write = TestWrite::new(mode);
            write.put("table/_SUCCESS").await?;
            let other = match mode {
                Mode::Rename => Mode::Multipart,
                Mode::Multipart => Mode::Rename,
            };
            let a = write
                .stage_with("table/a.parquet", "data", &write.store.publisher(other))
                .await?;
            let b = write.stage("table/b.parquet").await?;
            let result = write.overwrite(vec![manifest(0, 1, &[&a, &b])]).await;
            assert!(
                matches!(&result, Err(DataFusionError::Internal(e)) if e.contains("staged in another way")),
                "{result:?}"
            );
            // Nothing is published, and the directory is not changed.
            assert_eq!(write.store.objects().await?, vec!["table/_SUCCESS"]);
            assert_eq!(write.store.count("LIST"), 0);
            // An upload cannot be aborted without the multipart upload interface.
            assert_eq!(
                write.store.open_uploads().len(),
                usize::from(mode == Mode::Rename)
            );
        }
        Ok(())
    }
}
