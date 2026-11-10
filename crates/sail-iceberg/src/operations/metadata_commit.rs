use bytes::Bytes;
use datafusion_common::{DataFusionError, Result, internal_err};
use object_store::ObjectStoreExt;
use object_store::path::Path;
use sail_common_datafusion::error::CommitStateUnknownError;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::io::StoreContext;
use crate::spec::snapshots::MAIN_BRANCH;
use crate::spec::{MetadataLog, SnapshotLog, TableMetadata, TableUpdate};
use crate::table::metadata_loader::{
    MetadataFileCodec, encode_metadata_file, table_metadata_location, write_version_hint,
};
use crate::utils::metadata::metadata_files_for_version;

/// Captured before changing candidate properties; never recomputed from a newer current version.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub(crate) struct BaseMetadataCodec(MetadataFileCodec);

impl BaseMetadataCodec {
    pub(crate) fn capture(base: Option<&TableMetadata>) -> Result<Self> {
        Ok(Self(match base {
            Some(base) => MetadataFileCodec::from_properties(&base.properties)?,
            None => MetadataFileCodec::None,
        }))
    }
}

#[derive(Clone, Copy)]
pub(crate) enum MetadataFileStyle {
    // All participating Sail writers from the same immutable base contend on one key.
    Versioned(BaseMetadataCodec),
    Unique,
}

impl MetadataFileStyle {
    /// A pointer error cannot make already discoverable versioned metadata retryable.
    pub(crate) fn catalog_publication_error(
        self,
        path: &str,
        error: DataFusionError,
    ) -> DataFusionError {
        if matches!(self, Self::Unique)
            || matches!(&error, DataFusionError::External(source)
                if source.is::<CommitStateUnknownError>())
        {
            return error;
        }
        DataFusionError::External(Box::new(CommitStateUnknownError::with_source(
            format!("created {path} but could not update the catalog pointer"),
            error,
        )))
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum MetadataWriteOutcome {
    Written,
    Conflict,
}

/// A prepared metadata object. Catalog pointer publication remains the caller's responsibility.
pub(crate) struct MetadataFileCommit {
    path: Path,
    location: String,
    bytes: Bytes,
    version: i32,
    style: MetadataFileStyle,
    version_hint: String,
}

impl MetadataFileCommit {
    pub(crate) fn prepare(
        table_url: &Url,
        metadata: &TableMetadata,
        version: i32,
        style: MetadataFileStyle,
    ) -> Result<Self> {
        let json = metadata
            .to_json()
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        // Validate the requested codec even when its physical encoding takes effect later.
        let requested_codec = MetadataFileCodec::from_properties(&metadata.properties)?;
        let (file, version_hint) = match style {
            MetadataFileStyle::Versioned(base) => (
                format!("metadata/v{version}{}", base.0.file_extension()),
                version.to_string(),
            ),
            MetadataFileStyle::Unique => {
                let extension = requested_codec.file_extension();
                let name = format!("{version:05}-{}{extension}", uuid::Uuid::new_v4());
                (format!("metadata/{name}"), name)
            }
        };
        let location = table_metadata_location(table_url, &file)?;
        let bytes = encode_metadata_file(&file, &json)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        Ok(Self {
            path: Path::from(file),
            location,
            bytes: Bytes::from(bytes),
            version,
            style,
            version_hint,
        })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn location(&self) -> &str {
        &self.location
    }

    /// Only definite conflicts may be retried; other errors can leave publication uncertain.
    pub(crate) async fn write(&self, store: &StoreContext) -> Result<MetadataWriteOutcome> {
        self.write_replacement(store).await
    }

    /// Failures here are definitely unpublished: no metadata PUT has been attempted.
    pub(crate) async fn replacement_version_exists(&self, store: &StoreContext) -> Result<bool> {
        // Catalog pointer publication, not directory discovery, orders unique metadata files.
        if matches!(self.style, MetadataFileStyle::Unique) {
            return Ok(false);
        }
        Ok(!metadata_files_for_version(store, self.version)
            .await?
            .is_empty())
    }

    /// Call after the separate preflight, marking publication started immediately before this.
    /// Never delete a version after it may have become visible.
    pub(crate) async fn write_replacement(
        &self,
        store: &StoreContext,
    ) -> Result<MetadataWriteOutcome> {
        match store
            .prefixed
            .put_opts(
                &self.path,
                self.bytes.clone().into(),
                object_store::PutOptions {
                    mode: object_store::PutMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => {}
            Err(error) => {
                // An internal retry can return AlreadyExists after the first PUT succeeded.
                // Only exact-object readback can resolve that lost acknowledgement.
                let observed = match store.prefixed.get(&self.path).await {
                    Ok(object) => object.bytes().await.ok(),
                    Err(_) => None,
                };
                match observed {
                    Some(bytes) if bytes == self.bytes => {}
                    Some(_) if matches!(error, object_store::Error::AlreadyExists { .. }) => {
                        return Ok(MetadataWriteOutcome::Conflict);
                    }
                    // A missing object after a failed PUT is not proof of non-publication.
                    _ => {
                        return Err(match self.style {
                            MetadataFileStyle::Versioned(_) => DataFusionError::External(Box::new(
                                CommitStateUnknownError::with_source(
                                    format!(
                                        "could not confirm metadata publication at {}",
                                        self.path
                                    ),
                                    error,
                                ),
                            )),
                            MetadataFileStyle::Unique => DataFusionError::External(Box::new(error)),
                        });
                    }
                }
            }
        }
        if matches!(self.style, MetadataFileStyle::Versioned(_)) {
            let version_files = metadata_files_for_version(store, self.version)
                .await
                .map_err(|error| {
                    DataFusionError::External(Box::new(CommitStateUnknownError::with_source(
                        format!(
                            "created {} but could not check concurrent metadata publication",
                            self.path
                        ),
                        error,
                    )))
                })?;
            if version_files.iter().any(|file| file != self.path.as_ref()) {
                // Successful versioned Create is already discoverable by readers. Neither
                // this metadata nor its referenced files can safely be removed or retried.
                return Err(DataFusionError::External(Box::new(
                    CommitStateUnknownError::from(format!(
                        "created {} but found concurrent metadata for version {}",
                        self.path, self.version
                    )),
                )));
            }
        }
        Ok(MetadataWriteOutcome::Written)
    }

    /// A pointer error cannot make an already discoverable versioned Create retryable.
    pub(crate) fn catalog_publication_error(&self, error: DataFusionError) -> DataFusionError {
        self.style
            .catalog_publication_error(self.path.as_ref(), error)
    }

    pub(crate) async fn write_hint(&self, store: &StoreContext) {
        write_version_hint(&store.prefixed, &self.version_hint).await;
    }
}

pub(crate) enum PreviousMetadataFile<'a> {
    /// An authoritative catalog pointer, which may be a URI or an absolute filesystem path.
    CatalogLocation(&'a str),
    /// An internal object-store key that must be resolved against the table location.
    ObjectPath(&'a str),
}

/// Record a file location, not the object-store key used to load the metadata.
pub(crate) fn previous_metadata_log_entry(
    table_url: &Url,
    previous_metadata_file: PreviousMetadataFile<'_>,
    previous_timestamp_ms: i64,
) -> Result<MetadataLog> {
    Ok(MetadataLog {
        timestamp_ms: previous_timestamp_ms,
        metadata_file: match previous_metadata_file {
            PreviousMetadataFile::CatalogLocation(location) => location.to_string(),
            PreviousMetadataFile::ObjectPath(path) => table_metadata_location(table_url, path)?,
        },
    })
}

/// Apply snapshot actions and record the previous metadata version exactly once.
pub(crate) fn apply_snapshot_updates(
    table_url: &Url,
    metadata: &mut TableMetadata,
    updates: &[TableUpdate],
    previous_metadata_file: PreviousMetadataFile<'_>,
) -> Result<()> {
    let previous_entry =
        previous_metadata_log_entry(table_url, previous_metadata_file, metadata.last_updated_ms)?;
    let timestamp_ms = crate::utils::timestamp::monotonic_timestamp_ms();
    for update in updates {
        match update {
            TableUpdate::AddSnapshot { snapshot } => {
                metadata.last_sequence_number = metadata
                    .last_sequence_number
                    .max(snapshot.sequence_number());
                if let Some(added_rows) = snapshot.added_rows {
                    metadata.advance_next_row_id(added_rows);
                }
                metadata.snapshots.push(snapshot.clone());
            }
            TableUpdate::SetSnapshotRef {
                ref_name,
                reference,
            } => {
                if ref_name == MAIN_BRANCH {
                    metadata.current_snapshot_id = Some(reference.snapshot_id);
                    metadata.snapshot_log.push(SnapshotLog {
                        timestamp_ms,
                        snapshot_id: reference.snapshot_id,
                    });
                }
                metadata.refs.insert(ref_name.clone(), reference.clone());
            }
            TableUpdate::RemoveSnapshotRef { ref_name } => {
                metadata.refs.remove(ref_name);
            }
            _ => return internal_err!("Unsupported Iceberg snapshot metadata update"),
        }
    }
    metadata.last_updated_ms = timestamp_ms;
    metadata.metadata_log.push(previous_entry);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::operations::replacement_test_support::{ReplacementFault, ReplacementStore};

    #[tokio::test]
    #[expect(clippy::expect_used)]
    async fn replacement_storage_requires_exact_readback_and_never_deletes_unknown()
    -> Result<(), Box<dyn std::error::Error>> {
        for fault in [
            ReplacementFault::LostAck,
            ReplacementFault::PutThenAlreadyExists,
            ReplacementFault::DifferentBytes,
            ReplacementFault::Missing,
            ReplacementFault::ConditionalConflict,
            ReplacementFault::CompetingVersion,
            ReplacementFault::ReadbackFailure,
            ReplacementFault::PostflightList,
        ] {
            let store = Arc::new(ReplacementStore::new(fault));
            let context = StoreContext::new(store.clone(), &Url::parse("memory:///table")?)?;
            let publication = MetadataFileCommit {
                path: Path::from("metadata/v2.metadata.json"),
                location: "memory:///table/metadata/v2.metadata.json".into(),
                bytes: Bytes::from_static(b"exact prepared metadata"),
                version: 2,
                style: MetadataFileStyle::Versioned(BaseMetadataCodec::capture(None)?),
                version_hint: "2".into(),
            };
            assert!(!publication.replacement_version_exists(&context).await?);
            let result = publication.write_replacement(&context).await;
            match fault {
                ReplacementFault::LostAck | ReplacementFault::PutThenAlreadyExists => {
                    assert_eq!(result?, MetadataWriteOutcome::Written)
                }
                ReplacementFault::ConditionalConflict => {
                    assert_eq!(result?, MetadataWriteOutcome::Conflict)
                }
                _ => {
                    let error = result.expect_err("unresolved publication");
                    crate::io::test_store::assert_commit_retryability(&error, true);
                }
            }
            let stored = store
                .inner
                .get(&Path::from("table/metadata/v2.metadata.json"))
                .await;
            match fault {
                ReplacementFault::Missing => assert!(stored.is_err()),
                ReplacementFault::LostAck
                | ReplacementFault::PutThenAlreadyExists
                | ReplacementFault::CompetingVersion
                | ReplacementFault::ReadbackFailure
                | ReplacementFault::PostflightList => {
                    assert_eq!(stored?.bytes().await?, publication.bytes)
                }
                _ => assert_eq!(
                    stored?.bytes().await?,
                    Bytes::from_static(b"different metadata")
                ),
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn replacement_storage_existing_version_is_a_non_destructive_conflict()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = StoreContext::new(
            Arc::new(object_store::memory::InMemory::new()),
            &Url::parse("memory:///table")?,
        )?;
        let publication = MetadataFileCommit {
            path: Path::from("metadata/v2.metadata.json"),
            location: "memory:///table/metadata/v2.metadata.json".into(),
            bytes: Bytes::from_static(b"prepared"),
            version: 2,
            style: MetadataFileStyle::Versioned(BaseMetadataCodec::capture(None)?),
            version_hint: "2".into(),
        };
        assert!(!publication.replacement_version_exists(&store).await?);
        assert_eq!(
            publication.write_replacement(&store).await?,
            MetadataWriteOutcome::Written
        );
        assert!(publication.replacement_version_exists(&store).await?);
        assert_eq!(
            store.prefixed.get(&publication.path).await?.bytes().await?,
            publication.bytes
        );
        Ok(())
    }

    #[tokio::test]
    #[expect(clippy::expect_used)]
    async fn create_errors_preserve_style_publication_and_retry_semantics() {
        use std::sync::Arc;
        use std::sync::atomic::Ordering;

        use object_store::ObjectStoreExt;

        use crate::io::test_store::{PutFault, TestStore, assert_commit_retryability};
        use crate::spec::{FormatVersion, PartitionSpec, Schema};

        for style in [
            MetadataFileStyle::Versioned(BaseMetadataCodec::capture(None).expect("base codec")),
            MetadataFileStyle::Unique,
        ] {
            for fault in [
                PutFault::Reject,
                PutFault::LostAckUnreadable,
                PutFault::Collision,
            ] {
                let url = Url::parse("memory://bucket/table/").expect("URL");
                let store =
                    StoreContext::new(Arc::new(object_store::memory::InMemory::new()), &url)
                        .expect("store");
                let metadata: TableMetadata = serde_json::from_value(serde_json::json!({
                    "format-version": FormatVersion::V2,
                    "location": url.as_str(),
                    "last-updated-ms": 0, "last-column-id": 0,
                    "schemas": [Schema::builder().build().expect("schema")],
                    "current-schema-id": 0,
                    "partition-specs": [PartitionSpec::unpartitioned_spec()]
                }))
                .expect("metadata");
                let prepared =
                    MetadataFileCommit::prepare(&url, &metadata, 2, style).expect("prepare");
                let mut injected = TestStore::new(store.base);
                injected.fault = Some((".metadata.json".into(), fault));
                let injected = Arc::new(injected);
                let store = StoreContext::new(injected.clone(), &url).expect("store");
                let result = prepared.write(&store).await;
                {
                    let error = result.expect_err("unresolved Create error");
                    let versioned = matches!(style, MetadataFileStyle::Versioned(_));
                    assert_commit_retryability(&error, versioned);
                    let source = match &error {
                        DataFusionError::External(source) => Some(source),
                        _ => None,
                    }
                    .expect("external error");
                    let original = if versioned {
                        std::error::Error::source(
                            source
                                .downcast_ref::<CommitStateUnknownError>()
                                .expect("typed unknown"),
                        )
                        .expect("source")
                    } else {
                        source.as_ref()
                    };
                    assert!(original.is::<object_store::Error>());
                    assert!(
                        original
                            .source()
                            .expect("I/O source")
                            .is::<std::io::Error>()
                    );
                }
                assert_eq!(injected.puts.load(Ordering::SeqCst), 1);
                assert!(injected.deletes.lock().expect("deletes").is_empty());
                let reader = StoreContext::new(injected.inner.clone(), &url).expect("reader");
                assert_eq!(
                    reader.prefixed.head(prepared.path()).await.is_ok(),
                    matches!(fault, PutFault::LostAckUnreadable)
                );
            }
        }
    }

    #[tokio::test]
    async fn normal_publication_reconciles_exact_bytes_and_rejects_other_bytes()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::io::test_store::{PutFault, TestStore};
        for style in [
            MetadataFileStyle::Versioned(BaseMetadataCodec::capture(None)?),
            MetadataFileStyle::Unique,
        ] {
            let url = Url::parse("memory:///table")?;
            let mut injected = TestStore::new(Arc::new(object_store::memory::InMemory::new()));
            injected.fault = Some((".metadata.json".into(), PutFault::LostAck));
            let injected = Arc::new(injected);
            let store = StoreContext::new(injected.clone(), &url)?;
            let mut publication = MetadataFileCommit {
                path: Path::from("metadata/v2.metadata.json"),
                location: "memory:///table/metadata/v2.metadata.json".into(),
                bytes: Bytes::from_static(b"prepared"),
                version: 2,
                style,
                version_hint: "2".into(),
            };
            assert_eq!(
                publication.write(&store).await?,
                MetadataWriteOutcome::Written
            );
            // An internal retry's AlreadyExists is also resolved by exact readback.
            assert_eq!(
                publication.write(&store).await?,
                MetadataWriteOutcome::Written
            );
            publication.bytes = Bytes::from_static(b"different candidate");
            assert_eq!(
                publication.write(&store).await?,
                MetadataWriteOutcome::Conflict
            );
            assert_eq!(
                store
                    .prefixed
                    .get(publication.path())
                    .await?
                    .bytes()
                    .await?,
                Bytes::from_static(b"prepared")
            );
            assert!(
                injected
                    .deletes
                    .lock()
                    .map_err(|e| e.to_string())?
                    .is_empty()
            );
        }
        Ok(())
    }

    #[test]
    fn previous_metadata_log_resolves_object_keys() -> Result<(), Box<dyn std::error::Error>> {
        for (table, key, expected) in [
            (
                "file:///tmp/table/",
                "tmp/table/metadata/v1.metadata.json",
                "file:///tmp/table/metadata/v1.metadata.json",
            ),
            (
                "file:///tmp/table/",
                "/tmp/table/metadata/v1.metadata.json",
                "file:///tmp/table/metadata/v1.metadata.json",
            ),
            (
                "s3://bucket/warehouse/table/",
                "warehouse/table/metadata/v1.metadata.json",
                "s3://bucket/warehouse/table/metadata/v1.metadata.json",
            ),
            (
                "s3://bucket/warehouse/table/",
                "metadata/v1.metadata.json",
                "s3://bucket/warehouse/table/metadata/v1.metadata.json",
            ),
            (
                "file:///C:/warehouse/table/",
                "C:/warehouse/table/metadata/v1.metadata.json",
                "file:///C:/warehouse/table/metadata/v1.metadata.json",
            ),
            (
                "file:///C:/warehouse/table/",
                "warehouse/table/metadata/v1.metadata.json",
                "file:///C:/warehouse/table/metadata/v1.metadata.json",
            ),
        ] {
            let entry = previous_metadata_log_entry(
                &Url::parse(table)?,
                PreviousMetadataFile::ObjectPath(key),
                123,
            )?;
            assert_eq!(entry.metadata_file, expected);
            assert_eq!(entry.timestamp_ms, 123);
        }
        Ok(())
    }

    #[test]
    fn previous_metadata_log_preserves_catalog_filesystem_locations()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = Url::parse("file:///tmp/table/")?;
        for location in [
            "/tmp/external/old.metadata.json",
            "C:/external/old.metadata.json",
        ] {
            let entry = previous_metadata_log_entry(
                &table,
                PreviousMetadataFile::CatalogLocation(location),
                456,
            )?;
            assert_eq!(entry.metadata_file, location);
            assert_eq!(entry.timestamp_ms, 456);
        }
        Ok(())
    }

    #[test]
    fn previous_metadata_log_preserves_absolute_locations() -> Result<(), Box<dyn std::error::Error>>
    {
        let table = Url::parse("s3://bucket/warehouse/table/")?;
        for location in [
            "s3://bucket/warehouse/table/metadata/v1.metadata.json",
            "s3://bucket/external-root/metadata/v1.metadata.json",
            "s3://OTHER-BUCKET/external%25/%2f/space%20/hash%23/v1.metadata.json",
            "file:///outside/space name/percent%/v1.metadata.json",
            "file:///C:/outside/metadata/v1.metadata.json",
        ] {
            let entry = previous_metadata_log_entry(
                &table,
                PreviousMetadataFile::CatalogLocation(location),
                456,
            )?;
            assert_eq!(entry.metadata_file, location);
            assert_eq!(entry.timestamp_ms, 456);
        }
        Ok(())
    }

    #[test]
    fn previous_metadata_log_preserves_escaped_key_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        for root in ["file:///", "s3://bucket/"] {
            // Object keys preserve URL escapes; decoding them would change the object identity.
            for directory in ["percent%", "percent%25", "slash%2F", "space%20", "hash%23"] {
                let table = Url::parse(&format!("{root}warehouse/{directory}/"))?;
                let key = format!("warehouse/{directory}/metadata/v1.metadata.json");
                let entry = previous_metadata_log_entry(
                    &table,
                    PreviousMetadataFile::ObjectPath(&key),
                    789,
                )?;
                assert_eq!(entry.metadata_file, format!("{root}{key}"));
                assert_eq!(
                    crate::utils::url_to_object_path(&Url::parse(&entry.metadata_file)?)?.as_ref(),
                    key
                );
            }
        }
        Ok(())
    }
}
