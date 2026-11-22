// SPDX-License-Identifier: Apache-2.0

//! The files of a write are staged by the writer tasks and published by
//! the commit of the write.
//!
//! A writer task never creates an object that a reader of the table can see.
//! It uploads each file in one of two ways, and reports how the file is
//! published in its manifest.
//!
//!   1. **Rename.** The file is written to a hidden name in the table
//!      directory, and the commit renames it to its location. This is used for
//!      the object stores where a rename does not copy the data, which are
//!      the local file system and HDFS, and for every object store that has
//!      no multipart upload interface.
//!   2. **Multipart upload.** The parts of the file are uploaded to the
//!      location of the file, and the commit completes the upload. The object
//!      does not exist before that. This is used for the object stores with
//!      a multipart upload interface, which are Amazon S3 and the object
//!      stores with the same protocol, Google Cloud Storage, Azure Blob
//!      Storage, and the in-memory object store. A rename copies the data
//!      in these object stores.
//!
//! A file of any size takes three requests with a multipart upload, which are
//! the creation of the upload and one part in the writer task, and
//! the completion in the commit. Two of them could only be saved by writing
//! the object where readers can see it. With a rename, a file takes
//! the requests of a direct write and the rename.
//!
//! Everything that is staged and not published is removed wherever the code
//! that owns it still runs: a writer task that fails or is canceled removes
//! what it has staged and not reported, and the commit removes what it has
//! received and does not publish. The remaining cases leave objects behind
//! that no reader can see: a process that is killed, a writer task that
//! the driver has given up on and that completes later, and a write whose
//! commit never runs after writer tasks have reported their files. These
//! are objects with the name `_staged-<write id>-<random id>` in the table
//! directory, or multipart uploads that are not completed. An object store
//! keeps the parts of such an upload and charges for them until the upload
//! is aborted, and it has no listing of them that is available here. The
//! bucket therefore needs a lifecycle rule that aborts incomplete multipart
//! uploads. Azure Blob Storage removes blocks that are not committed after
//! seven days by itself.
//!
//! The location of a file is the same for every attempt of a writer task.
//! This is safe since the object stores keep the multipart uploads to one
//! location apart, and since nothing puts or deletes an object at the location
//! of a file between the creation of its upload and its completion. Only
//! the uploads that a commit has accepted are ever completed.

use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use bytes::Bytes;
use datafusion::execution::TaskContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::parquet::arrow::async_writer::AsyncFileWriter;
use datafusion::parquet::errors::ParquetError;
use datafusion_common::{DataFusionError, Result, internal_datafusion_err, internal_err};
use futures::FutureExt;
use futures::future::{BoxFuture, Shared};
use object_store::buffered::BufWriter;
use object_store::multipart::{MultipartStore, PartId};
use object_store::path::Path;
use object_store::{
    MultipartId, MultipartUpload, ObjectStore, ObjectStoreExt, PutPayload, PutResult, UploadPart,
    WriteMultipart,
};
use sail_common_datafusion::session::multipart::MultipartStoreProvider;
use tokio::sync::mpsc;
use uuid::Uuid;

/// The start of the name of an object that is renamed when it is published.
/// The name is hidden from the readers of a table, which ignore the names
/// that start with an underscore and have no equals sign. This is the rule
/// of Sail, Spark and Hive, and DuckDB and PyArrow ignore such names as well.
const STAGED_NAME_PREFIX: &str = "_staged-";

/// Returns whether the name is the name of a staged object, or the name that
/// an object store gives to a staged object while the object is written.
/// HDFS writes an object with the name `.<name>.tmp` first.
pub(crate) fn is_staged_name(name: &str) -> bool {
    name.strip_prefix('.')
        .unwrap_or(name)
        .starts_with(STAGED_NAME_PREFIX)
}

/// The number of files that are published or removed at the same time.
///
/// A file is published with one small request, and the commit publishes
/// the files of all writer tasks, so the requests are sent concurrently.
/// The number matters when a request takes tens of milliseconds, as with
/// a remote object store: 64 requests of 20 milliseconds each are 3,200
/// requests per second, which is about the 3,500 write requests per second
/// that Amazon S3 accepts for a prefix, so more requests at the same time
/// would not publish faster there, and the client would be asked to slow
/// down. The number does not matter when a request takes a millisecond:
/// with an S3-compatible object store on the same host (SeaweedFS), 1,000
/// and 10,000 files were published at 3,000 to 5,000 files per second with
/// any number from 8 to 512.
pub(crate) const PUBLISH_CONCURRENCY: usize = 64;

/// How a file that a writer task has uploaded becomes visible.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Publish {
    /// The file is an object at a hidden location, which is renamed.
    Rename { staged: Path },
    /// The parts of the file are uploaded, and the upload is completed.
    Multipart {
        upload_id: String,
        /// The identifiers of the parts in the order of the parts.
        parts: Vec<String>,
    },
}

/// A file that a writer task has uploaded and that no reader can see yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedFile {
    /// The location that the file has when it is published.
    pub location: Path,
    pub size: u64,
    pub num_rows: u64,
    pub publish: Publish,
}

/// Uploads the files of a write and makes them visible.
///
/// The writer tasks and the commit of a write each create this for the same
/// object store, and it must be of the same kind for all of them.
#[derive(Clone)]
pub enum Publisher {
    Rename {
        store: Arc<dyn ObjectStore>,
    },
    Multipart {
        store: Arc<dyn ObjectStore>,
        multipart: Arc<dyn MultipartStore>,
    },
}

impl fmt::Debug for Publisher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rename { store } => write!(f, "Rename({store})"),
            Self::Multipart { store, .. } => write!(f, "Multipart({store})"),
        }
    }
}

impl Publisher {
    /// Creates the publisher for an object store of the session.
    /// The files are published with multipart uploads if the session
    /// gives the multipart upload interface of the object store.
    pub fn try_new(context: &TaskContext, url: &ObjectStoreUrl) -> Result<Self> {
        let store = context.runtime_env().object_store(url)?;
        let multipart = context
            .session_config()
            .get_extension::<MultipartStoreProvider>()
            .map(|provider| provider.multipart_store(url.as_ref()))
            .transpose()?
            .flatten();
        Ok(match multipart {
            Some(multipart) => Self::Multipart { store, multipart },
            None => Self::Rename { store },
        })
    }

    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        match self {
            Self::Rename { store } | Self::Multipart { store, .. } => store,
        }
    }

    /// Starts the upload of a file of the write to the table.
    ///
    /// The file is uploaded in parts of `part_size` bytes,
    /// with at most `max_concurrency` parts at the same time.
    pub fn upload(
        &self,
        table_path: &Path,
        write_id: &Uuid,
        location: Path,
        part_size: usize,
        max_concurrency: usize,
    ) -> StagedUpload {
        let mut buffered = Arc::new(AtomicUsize::new(0));
        let inner = match self {
            Self::Rename { store } => {
                // The name is unique to the file, so that no attempt of
                // a writer task writes to the object of another attempt.
                let name = format!(
                    "{STAGED_NAME_PREFIX}{}-{}",
                    write_id.as_hyphenated(),
                    Uuid::new_v4().as_hyphenated()
                );
                let staged = table_path.clone().join(name);
                let writer = BufWriter::with_capacity(Arc::clone(store), staged.clone(), part_size)
                    .with_max_concurrency(max_concurrency);
                UploadState::Rename {
                    writer: Box::new(writer),
                    staged,
                }
            }
            Self::Multipart { multipart, .. } => {
                let parts = Arc::new(UploadedParts::default());
                let upload = DeferredUpload::new(Arc::clone(multipart), location, &parts);
                buffered = Arc::clone(&parts.buffered);
                UploadState::Multipart {
                    writer: Box::new(WriteMultipart::new_with_chunk_size(
                        Box::new(upload),
                        part_size,
                    )),
                    parts,
                    max_concurrency,
                }
            }
        };
        StagedUpload { inner, buffered }
    }

    /// Whether the files have a largest number of parts, which is the case
    /// for the object stores with a multipart upload interface.
    pub fn limits_parts(&self) -> bool {
        matches!(self, Self::Multipart { .. })
    }

    /// Whether the file is staged in the way in which this publishes files.
    pub fn accepts(&self, file: &StagedFile) -> bool {
        matches!(
            (self, &file.publish),
            (Self::Rename { .. }, Publish::Rename { .. })
                | (Self::Multipart { .. }, Publish::Multipart { .. })
        )
    }

    /// Makes the file visible at its location.
    ///
    /// The result of a request that fails is not known in general. For example,
    /// a request is sent again after a timeout, and an upload that the first
    /// request has completed cannot be completed again. Azure Blob Storage
    /// rejects the second request as invalid, while Amazon S3 does not find
    /// the upload. The file is therefore published after any failure
    /// if an object of its size is at its location. The location is unique
    /// to the write, so the object cannot be another file.
    pub async fn publish(&self, file: &StagedFile) -> Result<()> {
        let result = match (self, &file.publish) {
            (Self::Rename { store }, Publish::Rename { staged }) => {
                store.rename(staged, &file.location).await
            }
            (Self::Multipart { multipart, .. }, Publish::Multipart { upload_id, parts }) => {
                let parts = parts
                    .iter()
                    .map(|part| PartId {
                        content_id: part.clone(),
                    })
                    .collect();
                multipart
                    .complete_multipart(&file.location, upload_id, parts)
                    .await
                    .map(|_| ())
            }
            (Self::Rename { .. }, Publish::Multipart { .. })
            | (Self::Multipart { .. }, Publish::Rename { .. }) => {
                return internal_err!(
                    "the file {} is staged in another way than files are published",
                    file.location
                );
            }
        };
        let Err(error) = result else {
            return Ok(());
        };
        match self.store().head(&file.location).await {
            Ok(meta) if meta.size == file.size => Ok(()),
            _ => Err(error.into()),
        }
    }

    /// Removes what the file occupies before it is published.
    /// A failure is logged. The file is not visible in any case.
    pub async fn abort(&self, file: &StagedFile) {
        let result = match (self, &file.publish) {
            (_, Publish::Rename { staged }) => match self.store().delete(staged).await {
                Err(object_store::Error::NotFound { .. }) => Ok(()),
                result => result,
            },
            (Self::Multipart { multipart, .. }, Publish::Multipart { upload_id, .. }) => {
                multipart.abort_multipart(&file.location, upload_id).await
            }
            (Self::Rename { .. }, Publish::Multipart { upload_id, .. }) => {
                // The upload cannot be aborted without the multipart upload
                // interface of the object store, so it stays open.
                log::warn!(
                    "the multipart upload {upload_id} of {} is not aborted, since the session \
                    does not have the multipart upload interface of the object store",
                    file.location
                );
                Ok(())
            }
        };
        if let Err(e) = result {
            log::warn!(
                "failed to remove the staged file for {}: {e}",
                file.location
            );
        }
    }

    /// Removes what the files occupy before they are published.
    /// The requests are limited as the requests that publish files are.
    pub async fn abort_all(&self, files: &[StagedFile]) {
        for files in files.chunks(PUBLISH_CONCURRENCY) {
            futures::future::join_all(files.iter().map(|file| self.abort(file))).await;
        }
    }
}

/// The upload of one file. Nothing of it is visible to the readers of the table.
///
/// The upload is abandoned when this is dropped before [`Self::finish`] has
/// succeeded, and what it has stored is removed then. After that, the caller
/// owns the staged file and removes it with [`Publisher::abort`] if the file
/// is not to be published.
pub struct StagedUpload {
    inner: UploadState,
    /// The size in bytes of the data that was added and that is not stored.
    buffered: Arc<AtomicUsize>,
}

enum UploadState {
    Rename {
        writer: Box<BufWriter>,
        staged: Path,
    },
    Multipart {
        writer: Box<WriteMultipart>,
        parts: Arc<UploadedParts>,
        max_concurrency: usize,
    },
    Finished(Publish),
    Failed,
}

impl StagedUpload {
    /// Creates an upload that writes the file to its location, for the tests
    /// of the file writer that read the file without a commit.
    #[cfg(test)]
    pub(crate) fn direct(store: Arc<dyn ObjectStore>, location: Path, part_size: usize) -> Self {
        let writer = BufWriter::with_capacity(store, location.clone(), part_size);
        Self {
            inner: UploadState::Rename {
                writer: Box::new(writer),
                staged: location,
            },
            buffered: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// The size in bytes of the data that was added to a multipart upload and
    /// that the object store has not stored yet. This is the data of the part
    /// that is not full and of the parts that are being uploaded, so it is
    /// at most the size of a part for each part that is uploaded at the same
    /// time and for one more. The data of an upload that is renamed is held
    /// by the object store writer, which has the same bound and does not
    /// report it.
    pub fn buffered(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.buffered)
    }

    /// Adds data to the file. This waits when too many parts are being uploaded.
    pub async fn put(&mut self, data: Bytes) -> Result<()> {
        match &mut self.inner {
            UploadState::Rename { writer, .. } => Ok(writer.put(data).await?),
            UploadState::Multipart {
                writer,
                max_concurrency,
                ..
            } => {
                writer.wait_for_capacity(*max_concurrency).await?;
                self.buffered.fetch_add(data.len(), Ordering::Relaxed);
                writer.put(data);
                Ok(())
            }
            UploadState::Finished(_) | UploadState::Failed => {
                internal_err!("data is added to a file upload that has ended")
            }
        }
    }

    /// Uploads the remaining data, and returns how the file is published.
    /// The result is the same when this is called again.
    pub async fn finish(&mut self) -> Result<Publish> {
        let publish = match std::mem::replace(&mut self.inner, UploadState::Failed) {
            UploadState::Rename { mut writer, staged } => {
                use tokio::io::AsyncWriteExt;
                writer.shutdown().await?;
                Publish::Rename { staged }
            }
            UploadState::Multipart { writer, parts, .. } => {
                writer.finish().await?;
                parts.publish()?
            }
            UploadState::Finished(publish) => publish,
            UploadState::Failed => return internal_err!("the file upload has failed"),
        };
        self.inner = UploadState::Finished(publish.clone());
        Ok(publish)
    }
}

/// The parts of a multipart upload that have been stored.
#[derive(Default)]
struct UploadedParts {
    /// The size in bytes of the data that is not stored.
    buffered: Arc<AtomicUsize>,
    upload_id: OnceLock<MultipartId>,
    /// The identifier of each part at the index of the part.
    parts: Mutex<Vec<Option<String>>>,
}

impl UploadedParts {
    fn add(&self, index: usize, part: PartId) {
        let mut parts = self.parts.lock().unwrap_or_else(PoisonError::into_inner);
        if parts.len() <= index {
            parts.resize(index + 1, None);
        }
        if let Some(slot) = parts.get_mut(index) {
            *slot = Some(part.content_id);
        }
    }

    fn publish(&self) -> Result<Publish> {
        let upload_id = self
            .upload_id
            .get()
            .ok_or_else(|| internal_datafusion_err!("a file was uploaded without data"))?;
        let parts = self.parts.lock().unwrap_or_else(PoisonError::into_inner);
        let parts = parts
            .iter()
            .cloned()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| internal_datafusion_err!("a part of a file was not uploaded"))?;
        Ok(Publish::Multipart {
            upload_id: upload_id.clone(),
            parts,
        })
    }
}

/// The creation of a multipart upload, which is awaited by all its parts.
type CreatedUpload = Shared<BoxFuture<'static, Result<MultipartId, Arc<object_store::Error>>>>;

/// A multipart upload that is not completed by the process that uploads
/// the parts. [`WriteMultipart`] divides the data into parts of the same size
/// and limits the parts that are uploaded at the same time.
///
/// The upload is created with the first part. It is aborted when this is
/// dropped before [`MultipartUpload::complete`] is called, which only marks
/// the parts as uploaded here.
struct DeferredUpload {
    store: Arc<dyn MultipartStore>,
    location: Path,
    parts: Arc<UploadedParts>,
    created: Option<CreatedUpload>,
    next_part: usize,
    /// Whether the upload is no longer owned by this.
    released: bool,
    /// Each request for a part holds a sender, so the receiver
    /// shows when all requests have ended.
    in_flight: Option<mpsc::Sender<()>>,
    ended: Option<mpsc::Receiver<()>>,
}

impl fmt::Debug for DeferredUpload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeferredUpload")
            .field("location", &self.location)
            .field("next_part", &self.next_part)
            .finish()
    }
}

impl DeferredUpload {
    fn new(store: Arc<dyn MultipartStore>, location: Path, parts: &Arc<UploadedParts>) -> Self {
        let (in_flight, ended) = mpsc::channel(1);
        Self {
            store,
            location,
            parts: Arc::clone(parts),
            created: None,
            next_part: 0,
            released: false,
            in_flight: Some(in_flight),
            ended: Some(ended),
        }
    }

    fn created(&mut self) -> CreatedUpload {
        let store = Arc::clone(&self.store);
        let location = self.location.clone();
        self.created
            .get_or_insert_with(|| {
                async move { store.create_multipart(&location).await.map_err(Arc::new) }
                    .boxed()
                    .shared()
            })
            .clone()
    }

    /// Returns the task that aborts the upload, if the upload is still owned
    /// by this. The upload is aborted when the requests for its parts have
    /// ended, since an object store keeps a part that it receives after
    /// the upload was aborted. The requests end when [`WriteMultipart`] is
    /// dropped or shut down.
    fn release(&mut self) -> Option<BoxFuture<'static, ()>> {
        if std::mem::replace(&mut self.released, true) {
            return None;
        }
        // Only the requests hold senders from here on.
        self.in_flight = None;
        let created = self.created.take()?;
        let mut ended = self.ended.take()?;
        let store = Arc::clone(&self.store);
        let location = self.location.clone();
        Some(
            async move {
                while ended.recv().await.is_some() {}
                // The creation of the upload continues here if the requests
                // for the parts were canceled before it has ended.
                let Ok(upload_id) = created.await else {
                    return;
                };
                if let Err(e) = store.abort_multipart(&location, &upload_id).await {
                    log::warn!("failed to abort the upload of {location}: {e}");
                }
            }
            .boxed(),
        )
    }
}

/// Subtracts the size of the data of a part from the data that is not stored.
struct Buffered(Arc<AtomicUsize>, usize);

impl Drop for Buffered {
    fn drop(&mut self) {
        self.0.fetch_sub(self.1, Ordering::Relaxed);
    }
}

fn shared_error(error: Arc<object_store::Error>) -> object_store::Error {
    object_store::Error::Generic {
        store: "DeferredUpload",
        source: Box::new(error),
    }
}

#[async_trait::async_trait]
impl MultipartUpload for DeferredUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        // The index is assigned here, in the order in which the parts are requested.
        let index = self.next_part;
        self.next_part += 1;
        let created = self.created();
        let store = Arc::clone(&self.store);
        let location = self.location.clone();
        let parts = Arc::clone(&self.parts);
        let in_flight = self.in_flight.clone();
        Box::pin(async move {
            let _in_flight = in_flight;
            // The data is released when the request ends in any way.
            let _buffered = Buffered(Arc::clone(&parts.buffered), data.content_length());
            let upload_id = created.await.map_err(shared_error)?;
            let part = store.put_part(&location, &upload_id, index, data).await?;
            parts.upload_id.get_or_init(|| upload_id);
            parts.add(index, part);
            Ok(())
        })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        // All parts are stored. The upload is completed when the file is published.
        self.released = true;
        Ok(PutResult {
            e_tag: None,
            version: None,
        })
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        if let Some(abort) = self.release() {
            abort.await;
        }
        Ok(())
    }
}

impl Drop for DeferredUpload {
    fn drop(&mut self) {
        if let Some(abort) = self.release()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            runtime.spawn(abort);
        }
    }
}

/// Converts the error of a file upload to an error of the Parquet writer.
pub(crate) fn upload_error(error: DataFusionError) -> ParquetError {
    ParquetError::External(Box::new(error))
}

impl AsyncFileWriter for StagedUpload {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, datafusion::parquet::errors::Result<()>> {
        Box::pin(async move { self.put(bs).await.map_err(upload_error) })
    }

    fn complete(&mut self) -> BoxFuture<'_, datafusion::parquet::errors::Result<()>> {
        Box::pin(async move { self.finish().await.map(|_| ()).map_err(upload_error) })
    }
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::SessionConfig;
    use datafusion_datasource::ListingTableUrl;

    use super::*;
    use crate::listing::testing::{FaultStore, Mode};
    use crate::listing::utils::has_hidden_path_component;

    const WRITE_ID: Uuid = Uuid::from_u128(7);

    fn upload(store: &Arc<FaultStore>, mode: Mode, part_size: usize) -> StagedUpload {
        store.publisher(mode).upload(
            &Path::from("table"),
            &WRITE_ID,
            Path::from("table/p=1/file.parquet"),
            part_size,
            4,
        )
    }

    fn staged_file(publish: Publish, size: u64) -> StagedFile {
        StagedFile {
            location: Path::from("table/p=1/file.parquet"),
            size,
            num_rows: 1,
            publish,
        }
    }

    #[tokio::test]
    async fn test_staged_object_is_hidden_from_readers() -> Result<()> {
        let store = FaultStore::new();
        let mut upload = upload(&store, Mode::Rename, 2);
        let Publish::Rename { staged } = upload.finish().await? else {
            return internal_err!("unexpected upload");
        };
        let name = staged.filename().unwrap_or_default();
        assert_eq!(staged, Path::from("table").join(name));
        assert!(name.starts_with("_staged-00000000-0000-0000-0000-000000000007-"));
        // The name has no extension and no equals sign.
        assert!(!name.contains(['.', '=']));
        assert!(is_staged_name(name));
        assert!(is_staged_name(&format!(".{name}.tmp")));
        assert!(!is_staged_name("part-00000.parquet"));
        assert!(!is_staged_name("_SUCCESS"));
        // The object is hidden from the readers of Sail, with the rule of Spark.
        let table = ListingTableUrl::parse("memory:///table/")?;
        assert!(has_hidden_path_component(&table, &staged));
        let temporary = Path::from("table").join(format!(".{name}.tmp"));
        assert!(has_hidden_path_component(&table, &temporary));
        Ok(())
    }

    #[tokio::test]
    async fn test_upload_is_visible_when_it_is_published() -> Result<()> {
        for mode in Mode::ALL {
            let store = FaultStore::new();
            let publisher = store.publisher(mode);
            let mut upload = upload(&store, mode, 2);
            upload.put(Bytes::from_static(b"abc")).await?;
            upload.put(Bytes::from_static(b"de")).await?;
            let buffered = upload.buffered();
            let publish = upload.finish().await?;
            // The data is stored, so it is not counted as memory any longer.
            assert_eq!(buffered.load(Ordering::Relaxed), 0);
            // The result is the same when the upload is finished again.
            assert_eq!(upload.finish().await?, publish);
            assert!(upload.put(Bytes::from_static(b"f")).await.is_err());
            drop(upload);

            let mut requests = store.requests();
            requests.sort();
            match mode {
                Mode::Rename => {
                    assert_eq!(requests, vec!["PUT_MULTIPART table/_staged-*"]);
                    assert_eq!(store.objects().await?, vec!["table/_staged-*"]);
                }
                Mode::Multipart => {
                    // The parts have the same size except for the last part.
                    assert_eq!(
                        requests,
                        vec![
                            "CREATE table/p=1/file.parquet",
                            "PART table/p=1/file.parquet 0 2",
                            "PART table/p=1/file.parquet 1 2",
                            "PART table/p=1/file.parquet 2 1",
                        ]
                    );
                    assert_eq!(store.objects().await?, Vec::<String>::new());
                    assert_eq!(store.open_uploads(), vec!["table/p=1/file.parquet"]);
                    assert!(
                        matches!(&publish, Publish::Multipart { parts, .. } if parts.len() == 3)
                    );
                }
            }

            let file = staged_file(publish, 5);
            assert!(publisher.accepts(&file));
            publisher.publish(&file).await?;
            assert_eq!(store.objects().await?, vec!["table/p=1/file.parquet"]);
            assert_eq!(store.open_uploads(), Vec::<String>::new());
            let data = store.inner().get(&file.location).await?.bytes().await?;
            assert_eq!(data.as_ref(), b"abcde");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_upload_without_a_full_part_sends_no_request_until_it_is_finished() -> Result<()> {
        for mode in Mode::ALL {
            let store = FaultStore::new();
            let mut upload = upload(&store, mode, 4);
            upload.put(Bytes::from_static(b"abc")).await?;
            assert_eq!(store.requests(), Vec::<String>::new());
            drop(upload);
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            assert_eq!(store.requests(), Vec::<String>::new());

            // A small file takes one request to stage it with a rename,
            // and two requests with a multipart upload.
            let mut upload = self::upload(&store, mode, 4);
            upload.put(Bytes::from_static(b"abc")).await?;
            upload.finish().await?;
            let expected = match mode {
                Mode::Rename => vec!["PUT table/_staged-* 3"],
                Mode::Multipart => vec![
                    "CREATE table/p=1/file.parquet",
                    "PART table/p=1/file.parquet 0 3",
                ],
            };
            assert_eq!(store.requests(), expected);

            // A file without data cannot be a multipart upload.
            let mut upload = self::upload(&store, mode, 4);
            assert_eq!(upload.finish().await.is_ok(), mode == Mode::Rename);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_dropped_upload_is_aborted() -> Result<()> {
        // The request that is running when the upload is dropped.
        for request in ["CREATE", "PART"] {
            let store = FaultStore::new();
            let gate = store.hold(request);
            let mut upload = upload(&store, Mode::Multipart, 2);
            upload.put(Bytes::from_static(b"abcd")).await?;
            gate.wait_for(1).await;
            // The data of the parts is in memory while it is uploaded.
            assert_eq!(upload.buffered().load(Ordering::Relaxed), 4);
            drop(upload);
            assert_eq!(store.count("ABORT"), 0);
            gate.open();
            // The upload is created by the request that was running,
            // and it is aborted when the requests for the parts have ended.
            for _ in 0..2000 {
                if store.count("ABORT") == 1 {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            assert_eq!(store.count("CREATE"), 1);
            assert_eq!(store.count("ABORT"), 1);
            store.wait_for(&[]).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_upload_that_fails_is_aborted() -> Result<()> {
        for (request, aborts) in [("CREATE", 0), ("PART", 1)] {
            let store = FaultStore::new();
            store.fail(request, 0);
            let mut upload = upload(&store, Mode::Multipart, 2);
            upload.put(Bytes::from_static(b"abcd")).await?;
            let result = upload.finish().await;
            assert!(
                matches!(&result, Err(e) if e.to_string().contains("failure")),
                "{result:?}"
            );
            assert!(upload.finish().await.is_err());
            drop(upload);
            store.wait_for(&[]).await?;
            assert_eq!(store.count("ABORT"), aborts, "{request}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_staged_file_is_removed_when_it_is_aborted() -> Result<()> {
        for mode in Mode::ALL {
            let store = FaultStore::new();
            let publisher = store.publisher(mode);
            let mut upload = upload(&store, mode, 2);
            upload.put(Bytes::from_static(b"abc")).await?;
            let file = staged_file(upload.finish().await?, 3);
            drop(upload);
            publisher.abort(&file).await;
            store.wait_for(&[]).await?;
            // A failure to remove the file is not an error.
            publisher.abort(&file).await;
            assert!(publisher.publish(&file).await.is_err());
            store.wait_for(&[]).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_publisher_uses_multipart_uploads_of_the_session() -> Result<()> {
        let url = ObjectStoreUrl::parse("memory://")?;
        let store = FaultStore::new();
        let context = |multipart: Option<bool>| {
            let mut config = SessionConfig::new();
            if let Some(multipart) = multipart {
                let store = Arc::clone(&store);
                config = config.with_extension(Arc::new(MultipartStoreProvider::new(move |_| {
                    Ok(multipart.then(|| Arc::clone(&store) as Arc<dyn MultipartStore>))
                })));
            }
            let context = TaskContext::default().with_session_config(config);
            context
                .runtime_env()
                .register_object_store(url.as_ref(), Arc::clone(&store) as Arc<dyn ObjectStore>);
            context
        };
        for (multipart, expected) in [(None, false), (Some(false), false), (Some(true), true)] {
            let publisher = Publisher::try_new(&context(multipart), &url)?;
            assert_eq!(
                matches!(publisher, Publisher::Multipart { .. }),
                expected,
                "{multipart:?}"
            );
        }
        Ok(())
    }
}
