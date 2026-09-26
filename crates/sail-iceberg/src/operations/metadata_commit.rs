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

    pub(crate) async fn write_hint(&self, store: &StoreContext) {
        write_version_hint(&store.prefixed, &self.version_hint).await;
    }
}

/// Apply snapshot actions and record the previous metadata version exactly once.
pub(crate) fn apply_snapshot_updates(
    metadata: &mut TableMetadata,
    updates: &[TableUpdate],
    previous_metadata_file: &str,
) -> Result<()> {
    let previous_timestamp = metadata.last_updated_ms;
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
            _ => return internal_err!("Unsupported Iceberg snapshot metadata update"),
        }
    }
    metadata.last_updated_ms = timestamp_ms;
    metadata.metadata_log.push(MetadataLog {
        timestamp_ms: previous_timestamp,
        metadata_file: previous_metadata_file.to_string(),
    });
    Ok(())
}
