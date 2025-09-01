// [Credit]: <https://github.com/delta-io/delta-rs/blob/1f0b4d0965a85400c1effc6e9b4c7ebbb6795978/crates/core/src/kernel/snapshot/mod.rs>

use std::io::{BufRead, BufReader, Cursor};
use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::concat_batches;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::path::{LogPathFileType, ParsedLogPath};
use delta_kernel::scan::scan_row_schema;
use delta_kernel::schema::SchemaRef;
use delta_kernel::snapshot::Snapshot as KernelSnapshot;
use delta_kernel::table_configuration::TableConfiguration;
use delta_kernel::table_properties::TableProperties;
use delta_kernel::{PredicateRef, Version};
use deltalake::kernel::{Action, ActionType, CommitInfo, Metadata, Protocol, Remove, StructType};
use deltalake::logstore::LogStore;
use deltalake::{DeltaResult, DeltaTableConfig, DeltaTableError};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use object_store::path::Path;
use object_store::ObjectStore;
use tokio::task::spawn_blocking;
use url::Url;

use crate::kernel::arrow::engine_ext::{ScanExt, SnapshotExt};
use crate::kernel::models::fields::ActionTypeExt;
use crate::kernel::snapshot::iterators::LogicalFileView;
pub use crate::kernel::snapshot::log_data::LogDataHandler;
use crate::kernel::snapshot::parse::read_removes;
use crate::kernel::snapshot::stream::{RecordBatchReceiverStreamBuilder, SendableRBStream};

pub mod iterators;
pub mod log_data;
mod parse;
pub mod replay;
mod stream;

pub(crate) static SCAN_ROW_ARROW_SCHEMA: LazyLock<arrow_schema::SchemaRef> = LazyLock::new(|| {
    Arc::new(
        scan_row_schema()
            .as_ref()
            .try_into_arrow()
            .unwrap_or_else(|_| {
                // Fallback to an empty schema if conversion fails
                arrow_schema::Schema::empty()
            }),
    )
});

/// A snapshot of a Delta table
#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot {
    /// Log segment containing all log files in the snapshot
    pub(crate) inner: Arc<KernelSnapshot>,
    /// Configuration for the current session
    config: DeltaTableConfig,
    /// Logical table schema
    schema: SchemaRef,
    /// Fully qualified URL of the table
    table_url: Url,
}

impl Snapshot {
    /// Create a new [`Snapshot`] instance
    pub async fn try_new(
        log_store: &dyn LogStore,
        config: DeltaTableConfig,
        version: Option<i64>,
    ) -> DeltaResult<Self> {
        // TODO: bundle operation_id with logstore ...
        let engine = log_store.engine(None);
        let mut table_root = log_store.config().location.clone();
        let version = version.map(|v| v as u64);

        // NB: kernel engine uses Url::join to construct paths,
        // if the path does not end with a slash, the would override the entire path.
        // So we need to be extra sure its ends with a slash.
        if !table_root.path().ends_with('/') {
            table_root.set_path(&format!("{}/", table_root.path()));
        }
        let snapshot = match spawn_blocking(move || {
            KernelSnapshot::try_new(table_root, engine.as_ref(), version)
        })
        .await
        .map_err(|e| DeltaTableError::Generic(e.to_string()))?
        {
            Ok(snapshot) => snapshot,
            Err(e) => {
                // TODO: we should have more handling-friendly errors upstream in kernel.
                if e.to_string().contains("No files in log segment") {
                    return Err(DeltaTableError::NotATable(e.to_string()));
                } else {
                    return Err(e.into());
                }
            }
        };

        let schema = snapshot.table_configuration().schema();

        Ok(Self {
            inner: Arc::new(snapshot),
            config,
            schema,
            table_url: log_store.config().location.clone(),
        })
    }

    /// Update the snapshot to the given version
    pub async fn update(
        &mut self,
        log_store: &dyn LogStore,
        target_version: Option<u64>,
    ) -> DeltaResult<()> {
        if let Some(version) = target_version {
            if version == self.version() as u64 {
                return Ok(());
            }
            if version < self.version() as u64 {
                return Err(DeltaTableError::Generic("Cannot downgrade snapshot".into()));
            }
        }

        // TODO: bundle operation id with log store ...
        let engine = log_store.engine(None);
        let current = self.inner.clone();
        let snapshot = spawn_blocking(move || {
            KernelSnapshot::try_new_from(current, engine.as_ref(), target_version)
        })
        .await
        .map_err(|e| DeltaTableError::Generic(e.to_string()))??;

        self.inner = snapshot;
        self.schema = self.inner.table_configuration().schema();

        Ok(())
    }

    /// Get the table version of the snapshot
    pub fn version(&self) -> i64 {
        self.inner.version() as i64
    }

    /// Get the table schema of the snapshot
    pub fn schema(&self) -> &StructType {
        self.schema.as_ref()
    }

    /// Get the table metadata of the snapshot
    pub fn metadata(&self) -> &Metadata {
        self.inner.metadata()
    }

    /// Get the table protocol of the snapshot
    pub fn protocol(&self) -> &Protocol {
        self.inner.protocol()
    }

    /// Get the table config which is loaded with of the snapshot
    pub fn load_config(&self) -> &DeltaTableConfig {
        &self.config
    }

    /// Get the table root of the snapshot
    #[allow(dead_code)]
    pub(crate) fn table_root_path(&self) -> DeltaResult<Path> {
        Ok(Path::from_url_path(self.table_url.path())?)
    }

    /// Well known properties of the table
    pub fn table_properties(&self) -> &TableProperties {
        self.inner.table_properties()
    }

    pub fn table_configuration(&self) -> &TableConfiguration {
        self.inner.table_configuration()
    }

    /// Get the active files for the current snapshot.
    ///
    /// This method returns a stream of record batches where each row
    /// represents an active file for the current snapshot.
    ///
    /// The files can be filtered using the provided predicate. This is a
    /// best effort to skip files that are excluded by the predicate. Individual
    /// files may still contain data that is not relevant to the predicate.
    ///
    /// ## Arguments
    ///
    /// * `log_store` - The log store to use for reading the snapshot.
    /// * `predicate` - An optional predicate to filter the files.
    ///
    /// ## Returns
    ///
    /// A stream of active files for the current snapshot.
    pub fn files(
        &self,
        log_store: &dyn LogStore,
        predicate: Option<PredicateRef>,
    ) -> SendableRBStream {
        let scan = match self
            .inner
            .clone()
            .scan_builder()
            .with_predicate(predicate)
            .build()
        {
            Ok(scan) => scan,
            Err(err) => {
                return Box::pin(futures::stream::once(async {
                    Err(DeltaTableError::KernelError(err))
                }))
            }
        };

        // TODO: which capacity to choose?
        let mut builder = RecordBatchReceiverStreamBuilder::new(100);
        let tx = builder.tx();
        // TODO: bundle operation id with log store ...
        let engine = log_store.engine(None);
        let inner = self.inner.clone();

        builder.spawn_blocking(move || {
            let scan_iter = scan.scan_metadata_arrow(engine.as_ref())?;
            for res in scan_iter {
                let batch = res?.scan_files;
                let batch = inner.parse_stats_column(&batch)?;
                if tx.blocking_send(Ok(batch)).is_err() {
                    break;
                }
            }
            Ok(())
        });

        builder.build()
    }

    /// Get the commit infos in the snapshot
    ///
    /// ## Parameters
    ///
    /// * `log_store`: The log store to use.
    /// * `limit`: The maximum number of commit infos to return (optional).
    ///
    /// ## Returns
    ///
    /// A stream of commit infos.
    // TODO: move outer error into stream.
    #[allow(dead_code)]
    pub(crate) async fn commit_infos(
        &self,
        log_store: &dyn LogStore,
        limit: Option<usize>,
    ) -> DeltaResult<BoxStream<'_, DeltaResult<Option<CommitInfo>>>> {
        let store = log_store.root_object_store(None);

        let log_root = self.table_root_path()?.child("_delta_log");
        let start_from = log_root.child(
            format!(
                "{:020}",
                limit
                    .map(|l| (self.version() - l as i64 + 1).max(0))
                    .unwrap_or(0)
            )
            .as_str(),
        );

        let dummy_url = url::Url::parse("memory:///")
            .map_err(|e| DeltaTableError::generic(format!("Failed to parse dummy URL: {}", e)))?;
        let mut commit_files = Vec::new();
        for meta in store
            .list_with_offset(Some(&log_root), &start_from)
            .try_collect::<Vec<_>>()
            .await?
        {
            // safety: object store path are always valid urls paths.
            let dummy_path = dummy_url
                .join(meta.location.as_ref())
                .map_err(|e| DeltaTableError::generic(format!("Failed to join URL path: {}", e)))?;
            if let Some(parsed_path) = ParsedLogPath::try_from(dummy_path)? {
                if matches!(parsed_path.file_type, LogPathFileType::Commit) {
                    commit_files.push(meta);
                }
            }
        }
        commit_files.sort_unstable_by(|a, b| b.location.cmp(&a.location));
        Ok(futures::stream::iter(commit_files)
            .map(move |meta| {
                let store = store.clone();
                async move {
                    let commit_log_bytes = store.get(&meta.location).await?.bytes().await?;
                    let reader = BufReader::new(Cursor::new(commit_log_bytes));
                    for line in reader.lines() {
                        let action: Action = serde_json::from_str(line?.as_str())?;
                        if let Action::CommitInfo(commit_info) = action {
                            return Ok::<_, DeltaTableError>(Some(commit_info));
                        }
                    }
                    Ok(None)
                }
            })
            .buffered(self.config.log_buffer_size)
            .boxed())
    }

    pub(crate) fn tombstones(
        &self,
        log_store: &dyn LogStore,
    ) -> BoxStream<'_, DeltaResult<Remove>> {
        static TOMBSTONE_SCHEMA: LazyLock<Arc<StructType>> = LazyLock::new(|| {
            Arc::new(StructType::new(vec![
                ActionType::Remove.schema_field().clone(),
                ActionType::Sidecar.schema_field().clone(),
            ]))
        });

        // TODO: which capacity to choose?
        let mut builder = RecordBatchReceiverStreamBuilder::new(100);
        let tx = builder.tx();

        // TODO: bundle operation id with log store ...
        let engine = log_store.engine(None);

        let remove_data = match self.inner.log_segment().read_actions(
            engine.as_ref(),
            TOMBSTONE_SCHEMA.clone(),
            TOMBSTONE_SCHEMA.clone(),
            None,
        ) {
            Ok(data) => data,
            Err(err) => {
                return Box::pin(futures::stream::once(async {
                    Err(DeltaTableError::KernelError(err))
                }))
            }
        };

        builder.spawn_blocking(move || {
            for res in remove_data {
                let batch: RecordBatch =
                    ArrowEngineData::try_from_engine_data(res?.actions)?.into();
                if tx.blocking_send(Ok(batch)).is_err() {
                    break;
                }
            }
            Ok(())
        });

        builder
            .build()
            .map(|maybe_batch| maybe_batch.and_then(|batch| read_removes(&batch)))
            .map_ok(|removes| {
                futures::stream::iter(removes.into_iter().map(Ok::<_, DeltaTableError>))
            })
            .try_flatten()
            .boxed()
    }

    /// Fetch the latest version of the provided application_id for this snapshot.
    ///
    /// Filters the txn based on the SetTransactionRetentionDuration property and lastUpdated
    async fn application_transaction_version(
        &self,
        log_store: &dyn LogStore,
        app_id: String,
    ) -> DeltaResult<Option<i64>> {
        // TODO: bundle operation id with log store ...
        let engine = log_store.engine(None);
        let inner = self.inner.clone();
        let version = spawn_blocking(move || inner.get_app_id_version(&app_id, engine.as_ref()))
            .await
            .map_err(|e| DeltaTableError::GenericError { source: e.into() })??;
        Ok(version)
    }
}

/// A snapshot of a Delta table that has been eagerly loaded into memory.
#[derive(Debug, Clone, PartialEq)]
pub struct EagerSnapshot {
    snapshot: Snapshot,
    // logical files in the snapshot
    pub(crate) files: RecordBatch,
}

impl EagerSnapshot {
    /// Create a new [`EagerSnapshot`] instance
    pub async fn try_new(
        log_store: &dyn LogStore,
        config: DeltaTableConfig,
        version: Option<i64>,
    ) -> DeltaResult<Self> {
        let snapshot = Snapshot::try_new(log_store, config.clone(), version).await?;

        let files = match config.require_files {
            true => snapshot.files(log_store, None).try_collect().await?,
            false => vec![],
        };

        let scan_row_schema = snapshot.inner.scan_row_parsed_schema_arrow()?;
        let files = concat_batches(&scan_row_schema, &files)?;

        Ok(Self { snapshot, files })
    }

    /// Update the snapshot to the given version
    pub(crate) async fn update(
        &mut self,
        log_store: &dyn LogStore,
        target_version: Option<Version>,
    ) -> DeltaResult<()> {
        let current_version = self.version() as u64;
        if Some(current_version) == target_version {
            return Ok(());
        }

        self.snapshot.update(log_store, target_version).await?;

        let scan = self.snapshot.inner.clone().scan_builder().build()?;
        let engine = log_store.engine(None);
        let current_files = self.files.clone();
        let files: Vec<_> = spawn_blocking(move || {
            scan.scan_metadata_from_arrow(
                engine.as_ref(),
                current_version,
                Box::new(std::iter::once(current_files)),
                None,
            )?
            .map_ok(|s| s.scan_files)
            .try_collect()
        })
        .await
        .map_err(|e| DeltaTableError::Generic(e.to_string()))??;

        let files = concat_batches(&SCAN_ROW_ARROW_SCHEMA, &files)?;
        let files = self.snapshot.inner.parse_stats_column(&files)?;

        self.files = files;

        Ok(())
    }

    /// Get the underlying snapshot
    pub(crate) fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    /// Get the table version of the snapshot
    pub fn version(&self) -> i64 {
        self.snapshot.version()
    }

    /// Get the timestamp of the given version
    pub fn version_timestamp(&self, version: i64) -> Option<i64> {
        for path in &self.snapshot.inner.log_segment().ascending_commit_files {
            if path.version as i64 == version {
                return Some(path.location.last_modified);
            }
        }
        None
    }

    /// Get the table schema of the snapshot
    pub fn schema(&self) -> &StructType {
        self.snapshot.schema()
    }

    /// Get the table metadata of the snapshot
    pub fn metadata(&self) -> &Metadata {
        self.snapshot.metadata()
    }

    /// Get the table protocol of the snapshot
    pub fn protocol(&self) -> &Protocol {
        self.snapshot.protocol()
    }

    /// Get the table config which is loaded with of the snapshot
    pub fn load_config(&self) -> &DeltaTableConfig {
        self.snapshot.load_config()
    }

    /// Well known table configuration
    pub fn table_properties(&self) -> &TableProperties {
        self.snapshot.table_properties()
    }

    /// Well known table configuration (alias for table_properties)
    pub fn config(&self) -> &TableProperties {
        self.table_properties()
    }

    pub fn table_configuration(&self) -> &TableConfiguration {
        self.snapshot.table_configuration()
    }

    /// Get a [`LogDataHandler`] for the snapshot to inspect the currently loaded state of the log.
    pub fn log_data(&self) -> LogDataHandler<'_> {
        LogDataHandler::new(&self.files, self.snapshot.table_configuration())
    }

    /// Stream the active files in the snapshot
    ///
    /// This function returns a stream of [`LogicalFileView`] objects,
    /// which represent the active files in the snapshot.
    ///
    /// # Arguments
    ///
    /// * `log_store` - A reference to a [`LogStore`] implementation.
    /// * `predicate` - An optional predicate to filter the files.
    ///
    /// # Returns
    ///
    /// A stream of [`LogicalFileView`] objects.
    pub fn files(
        &self,
        log_store: &dyn LogStore,
        predicate: Option<PredicateRef>,
    ) -> BoxStream<'_, DeltaResult<LogicalFileView>> {
        // TODO: the logic in this function would be more suitable as an async fn rather than
        // a stream. However as we are moving from an eager to a cached snapshot, this should be
        // a stream just like on the Snapshot. So we swallow the awkward error handling for now
        // knowing that we will be able to clean this up soon (TM).
        let data = if let Some(predicate) = predicate {
            let scan = match self
                .snapshot
                .inner
                .clone()
                .scan_builder()
                .with_predicate(predicate)
                .build()
            {
                Ok(scan) => scan,
                Err(err) => {
                    return Box::pin(futures::stream::once(async {
                        Err(DeltaTableError::KernelError(err))
                    }))
                }
            };
            let engine = log_store.engine(None);
            let current_files = self.files.clone();
            let current_version = self.version() as u64;

            // TODO: while we are always re-processing the cached files, we are confident that no IO
            // is performed when processing, so for now we are not spawning this on a blocking thread.
            // As we continue refactoring, we need to move this onto an actual stream.
            let files_iter = match scan.scan_metadata_from_arrow(
                engine.as_ref(),
                current_version,
                Box::new(std::iter::once(current_files)),
                None,
            ) {
                Ok(files_iter) => files_iter,
                Err(err) => {
                    return Box::pin(futures::stream::once(async {
                        Err(DeltaTableError::KernelError(err))
                    }))
                }
            };

            let files: Vec<_> = match files_iter.map_ok(|s| s.scan_files).try_collect() {
                Ok(files) => files,
                Err(err) => {
                    return Box::pin(futures::stream::once(async {
                        Err(DeltaTableError::KernelError(err))
                    }))
                }
            };

            match concat_batches(&SCAN_ROW_ARROW_SCHEMA, &files)
                .map_err(DeltaTableError::from)
                .and_then(|batch| self.snapshot.inner.parse_stats_column(&batch))
            {
                Ok(files) => files,
                Err(err) => return Box::pin(futures::stream::once(async { Err(err) })),
            }
        } else {
            self.files.clone()
        };
        let iter = (0..data.num_rows()).map(move |i| Ok(LogicalFileView::new(data.clone(), i)));
        futures::stream::iter(iter).boxed()
    }

    /// Iterate over all latest app transactions
    pub async fn transaction_version(
        &self,
        log_store: &dyn LogStore,
        app_id: impl ToString,
    ) -> DeltaResult<Option<i64>> {
        self.snapshot
            .application_transaction_version(log_store, app_id.to_string())
            .await
    }
}
