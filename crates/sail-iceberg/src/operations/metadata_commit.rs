use bytes::Bytes;
use datafusion_common::{DataFusionError, Result, internal_err};
use object_store::ObjectStoreExt;
use object_store::path::Path;
use url::Url;

use crate::io::StoreContext;
use crate::spec::snapshots::MAIN_BRANCH;
use crate::spec::{MetadataLog, SnapshotLog, TableMetadata, TableUpdate};
use crate::table::metadata_loader::{
    encode_metadata_file, metadata_file_extension_from_properties, table_metadata_location,
    write_version_hint,
};
use crate::utils::metadata::metadata_files_for_version;

#[derive(Clone, Copy)]
pub(crate) enum MetadataFileStyle {
    Versioned,
    Unique,
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
        let extension = metadata_file_extension_from_properties(&metadata.properties)?;
        let (file, version_hint) = match style {
            MetadataFileStyle::Versioned => (
                format!("metadata/v{version}{extension}"),
                version.to_string(),
            ),
            MetadataFileStyle::Unique => {
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

    pub(crate) fn location(&self) -> &str {
        &self.location
    }

    /// Only definite conflicts may be retried; other errors can leave publication uncertain.
    pub(crate) async fn write(&self, store: &StoreContext) -> Result<MetadataWriteOutcome> {
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
            Err(object_store::Error::AlreadyExists { .. }) => {
                return Ok(MetadataWriteOutcome::Conflict);
            }
            Err(error) => return Err(DataFusionError::External(Box::new(error))),
        }
        if matches!(self.style, MetadataFileStyle::Versioned) {
            let version_files = metadata_files_for_version(store, self.version).await?;
            if version_files.iter().any(|file| file != self.path.as_ref()) {
                match store.prefixed.delete(&self.path).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                    Err(error) => {
                        return Err(DataFusionError::Execution(format!(
                            "failed to remove conflicted Iceberg metadata file {}; commit state is uncertain: {error}",
                            self.path
                        )));
                    }
                }
                return Ok(MetadataWriteOutcome::Conflict);
            }
        }
        Ok(MetadataWriteOutcome::Written)
    }

    /// Failures here are definitely unpublished: no metadata PUT has been attempted.
    pub(crate) async fn replacement_version_exists(&self, store: &StoreContext) -> Result<bool> {
        Ok(!metadata_files_for_version(store, self.version)
            .await?
            .is_empty())
    }

    /// Call after the separate preflight, marking publication started immediately before this.
    /// Path replacement never deletes a metadata version after it may have become visible.
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
                    _ => return Err(DataFusionError::External(Box::new(error))),
                }
            }
        }
        let versions = metadata_files_for_version(store, self.version).await?;
        if versions.iter().any(|file| file != self.path.as_ref()) {
            return Err(DataFusionError::Execution(
                "concurrent Iceberg metadata publications; replacement state is unknown".into(),
            ));
        }
        Ok(MetadataWriteOutcome::Written)
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
    async fn replacement_storage_requires_exact_readback_and_never_deletes_unknown() {
        for fault in [
            ReplacementFault::LostAck,
            ReplacementFault::PutThenAlreadyExists,
            ReplacementFault::DifferentBytes,
            ReplacementFault::Missing,
            ReplacementFault::ConditionalConflict,
            ReplacementFault::CompetingVersion,
        ] {
            let store = Arc::new(ReplacementStore::new(fault));
            let context =
                StoreContext::new(store.clone(), &Url::parse("memory:///table").unwrap()).unwrap();
            let publication = MetadataFileCommit {
                path: Path::from("metadata/v2.metadata.json"),
                location: "memory:///table/metadata/v2.metadata.json".into(),
                bytes: Bytes::from_static(b"exact prepared metadata"),
                version: 2,
                style: MetadataFileStyle::Versioned,
                version_hint: "2".into(),
            };
            assert!(
                !publication
                    .replacement_version_exists(&context)
                    .await
                    .unwrap()
            );
            let result = publication.write_replacement(&context).await;
            match fault {
                ReplacementFault::LostAck | ReplacementFault::PutThenAlreadyExists => {
                    assert_eq!(result.unwrap(), MetadataWriteOutcome::Written)
                }
                ReplacementFault::ConditionalConflict => {
                    assert_eq!(result.unwrap(), MetadataWriteOutcome::Conflict)
                }
                _ => assert!(result.is_err(), "{fault:?}: {result:?}"),
            }
            let stored = store
                .inner
                .get(&Path::from("table/metadata/v2.metadata.json"))
                .await;
            match fault {
                ReplacementFault::Missing => assert!(stored.is_err()),
                ReplacementFault::LostAck
                | ReplacementFault::PutThenAlreadyExists
                | ReplacementFault::CompetingVersion => {
                    assert_eq!(stored.unwrap().bytes().await.unwrap(), publication.bytes)
                }
                _ => assert_eq!(
                    stored.unwrap().bytes().await.unwrap(),
                    Bytes::from_static(b"different metadata")
                ),
            }
        }
    }

    #[tokio::test]
    async fn replacement_storage_existing_version_is_a_non_destructive_conflict() {
        let store = StoreContext::new(
            Arc::new(object_store::memory::InMemory::new()),
            &Url::parse("memory:///table").unwrap(),
        )
        .unwrap();
        let publication = MetadataFileCommit {
            path: Path::from("metadata/v2.metadata.json"),
            location: "memory:///table/metadata/v2.metadata.json".into(),
            bytes: Bytes::from_static(b"prepared"),
            version: 2,
            style: MetadataFileStyle::Versioned,
            version_hint: "2".into(),
        };
        assert!(
            !publication
                .replacement_version_exists(&store)
                .await
                .unwrap()
        );
        assert_eq!(
            publication.write_replacement(&store).await.unwrap(),
            MetadataWriteOutcome::Written
        );
        assert!(
            publication
                .replacement_version_exists(&store)
                .await
                .unwrap()
        );
        assert_eq!(
            store
                .prefixed
                .get(&publication.path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            publication.bytes
        );
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
