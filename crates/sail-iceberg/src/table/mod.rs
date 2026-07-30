// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod metadata_loader;

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use datafusion::catalog::Session;
use datafusion::common::{DataFusionError, Result};
pub use metadata_loader::find_latest_metadata_file;
use url::Url;

use crate::datasource::provider::IcebergTableProvider;
use crate::io::StoreContext;
use crate::operations::Transaction;
use crate::options::r#gen::IcebergReadOptions;
use crate::spec::snapshots::MAIN_BRANCH;
use crate::spec::{PartitionSpec, Schema, Snapshot, TableMetadata};

/// High-level representation of an Iceberg table backed by ObjectStore + metadata.
pub struct Table {
    table_url: Url,
    store_ctx: StoreContext,
    metadata: TableMetadata,
}

impl Table {
    /// Load table metadata and IO context using the provided execution session.
    pub async fn load(ctx: &dyn Session, table_url: Url) -> Result<Self> {
        Self::load_with_metadata_location(ctx, table_url, None).await
    }

    /// Load table metadata from an explicit metadata location when one is provided.
    pub async fn load_with_metadata_location(
        ctx: &dyn Session,
        table_url: Url,
        metadata_location: Option<String>,
    ) -> Result<Self> {
        log::trace!(
            "Loading Iceberg table: table_url={}, metadata_location={:?}",
            table_url,
            metadata_location,
        );
        let object_store = ctx
            .runtime_env()
            .object_store_registry
            .get_store(&table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let store_ctx = StoreContext::new(object_store.clone(), &table_url)?;
        let metadata_location = match metadata_location {
            Some(location) => metadata_loader::metadata_location_to_object_path_string(&location)?,
            None => metadata_loader::find_latest_metadata_file(&object_store, &table_url).await?,
        };
        log::trace!("Found Iceberg metadata file at {}", metadata_location);
        let metadata_data =
            metadata_loader::load_metadata_file_bytes(&object_store, &metadata_location).await?;
        let metadata = TableMetadata::from_json(&metadata_data).map_err(|e| {
            log::trace!("Failed to parse table metadata: {:?}", e);
            DataFusionError::External(Box::new(e))
        })?;

        Ok(Self {
            table_url,
            store_ctx,
            metadata,
        })
    }

    /// Return the canonical table URL.
    pub fn table_url(&self) -> &Url {
        &self.table_url
    }

    /// Access the object-store context for this table.
    pub fn store_context(&self) -> &StoreContext {
        &self.store_ctx
    }

    /// Access the loaded table metadata.
    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }

    /// Prepare scan components (schema, snapshot, partition specs) for the given options.
    pub fn scan_state(
        &self,
        options: &IcebergReadOptions,
    ) -> Result<(Schema, Snapshot, Vec<PartitionSpec>)> {
        let (schema, snapshot) = self.select_snapshot(options)?;
        Ok((schema, snapshot, self.metadata.partition_specs.clone()))
    }

    /// Build an Iceberg table provider that reflects the requested snapshot options.
    pub fn to_provider(&self, options: &IcebergReadOptions) -> Result<IcebergTableProvider> {
        validate_snapshot_selection(options)?;
        if self.metadata.current_snapshot().is_none()
            && options.snapshot_id.is_none()
            && options.use_ref.is_none()
            && options.timestamp_as_of.is_none()
        {
            let schema = self.metadata.current_schema().cloned().ok_or_else(|| {
                DataFusionError::Plan("No current schema found in table metadata".to_string())
            })?;
            let provider = IcebergTableProvider::new_empty(
                self.table_url.to_string(),
                schema,
                self.metadata.partition_specs.clone(),
                self.metadata.default_spec_id,
            )?;
            return Ok(provider.with_metadata_as_data_read(options.metadata_as_data_read));
        }
        let (schema, snapshot, partition_specs) = self.scan_state(options)?;
        let provider = IcebergTableProvider::new(
            self.table_url.to_string(),
            schema,
            snapshot,
            partition_specs,
            self.metadata.default_spec_id,
        )?;
        Ok(provider.with_metadata_as_data_read(options.metadata_as_data_read))
    }

    /// Create a Transaction anchored at the current snapshot, if one exists.
    pub fn new_transaction(&self) -> Option<Transaction> {
        self.metadata
            .current_snapshot()
            .cloned()
            .map(|snapshot| Transaction::new(self.table_url.to_string(), snapshot))
    }

    fn select_snapshot(&self, options: &IcebergReadOptions) -> Result<(Schema, Snapshot)> {
        validate_snapshot_selection(options)?;
        let (chosen_snapshot, use_snapshot_schema) = if let Some(id) = options.snapshot_id {
            (
                self.metadata
                    .snapshots
                    .iter()
                    .find(|s| s.snapshot_id() == id)
                    .cloned()
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!("Snapshot with id {} not found", id))
                    })?,
                true,
            )
        } else if let Some(ref_name) = options.use_ref.as_deref() {
            let (sid, use_snapshot_schema) = if ref_name == MAIN_BRANCH {
                (
                    self.metadata.current_snapshot_id.ok_or_else(|| {
                        DataFusionError::Plan(
                            "Iceberg table metadata is missing current snapshot id".to_string(),
                        )
                    })?,
                    false,
                )
            } else {
                let reference = self.metadata.refs.get(ref_name).ok_or_else(|| {
                    DataFusionError::Plan(format!("Unknown Iceberg ref: {}", ref_name))
                })?;
                (reference.snapshot_id, !reference.is_branch())
            };
            (
                self.metadata
                    .snapshots
                    .iter()
                    .find(|s| s.snapshot_id() == sid)
                    .cloned()
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "Snapshot for ref {} (id={}) not found",
                            ref_name, sid
                        ))
                    })?,
                use_snapshot_schema,
            )
        } else if let Some(ts_str) = options.timestamp_as_of.as_deref() {
            let ts_ms =
                parse_timestamp_to_ms(ts_str).map_err(|e| DataFusionError::Plan(e.to_string()))?;
            (
                find_snapshot_by_ts(&self.metadata, ts_ms)
                    .cloned()
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "No Iceberg snapshot exists at or before timestamp {}",
                            ts_str
                        ))
                    })?,
                true,
            )
        } else {
            (
                self.metadata.current_snapshot().cloned().ok_or_else(|| {
                    DataFusionError::Plan("No current snapshot found in table metadata".to_string())
                })?,
                false,
            )
        };

        let schema = if use_snapshot_schema {
            if let Some(schema_id) = chosen_snapshot.schema_id() {
                self.metadata
                    .schemas
                    .iter()
                    .find(|s| s.schema_id() == schema_id)
                    .cloned()
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "Schema with id {} not found for chosen snapshot",
                            schema_id
                        ))
                    })?
            } else {
                self.metadata.current_schema().cloned().ok_or_else(|| {
                    DataFusionError::Plan("No current schema found in table metadata".to_string())
                })?
            }
        } else {
            self.metadata.current_schema().cloned().ok_or_else(|| {
                DataFusionError::Plan("No current schema found in table metadata".to_string())
            })?
        };

        Ok((schema, chosen_snapshot))
    }
}

fn validate_snapshot_selection(options: &IcebergReadOptions) -> Result<()> {
    let mut selectors = Vec::with_capacity(3);
    if options.snapshot_id.is_some() {
        selectors.push("snapshot-id");
    }
    if options.use_ref.is_some() {
        selectors.push("ref");
    }
    if options.timestamp_as_of.is_some() {
        selectors.push("timestamp-as-of");
    }
    if selectors.len() > 1 {
        return Err(DataFusionError::Plan(format!(
            "Iceberg snapshot selection is ambiguous: specify only one of snapshot-id, ref, or timestamp-as-of; received {}",
            selectors.join(", ")
        )));
    }
    Ok(())
}

fn parse_timestamp_to_ms(s: &str) -> std::result::Result<i64, String> {
    let rfc3339_result = DateTime::parse_from_rfc3339(s);
    if let Ok(dt) = rfc3339_result {
        return Ok(dt.with_timezone(&Utc).timestamp_millis());
    }

    let mut last_error = rfc3339_result
        .err()
        .map(|e| format!("RFC3339 parsing error: {e}"));

    for format in [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ] {
        match NaiveDateTime::parse_from_str(s, format) {
            Ok(naive) => return Ok(Utc.from_utc_datetime(&naive).timestamp_millis()),
            Err(e) => {
                last_error = Some(format!("Failed to parse with format '{format}': {e}"));
            }
        }
    }

    let detail = last_error
        .map(|e| format!(" Details: {e}"))
        .unwrap_or_default();
    Err(format!(
        "Invalid timestamp '{s}'. Supported formats are: RFC3339 (e.g. '2024-01-02T03:04:05Z'), '%Y-%m-%d %H:%M:%S%.f', '%Y-%m-%dT%H:%M:%S%.f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S'.{detail}"
    ))
}

fn find_snapshot_by_ts(meta: &TableMetadata, ts_ms: i64) -> Option<&Snapshot> {
    let from_log = meta
        .snapshot_log
        .iter()
        .filter(|e| e.timestamp_ms <= ts_ms)
        .max_by(|a, b| {
            a.timestamp_ms
                .cmp(&b.timestamp_ms)
                .then_with(|| a.snapshot_id.cmp(&b.snapshot_id))
        })
        .and_then(|log_entry| {
            meta.snapshots
                .iter()
                .find(|s| s.snapshot_id() == log_entry.snapshot_id)
                .map(|snapshot| (log_entry.timestamp_ms, snapshot.snapshot_id(), snapshot))
        });

    from_log.map(|(_, _, snapshot)| snapshot).or_else(|| {
        meta.snapshots
            .iter()
            .filter(|s| s.timestamp_ms() <= ts_ms)
            .max_by(|a, b| {
                a.timestamp_ms()
                    .cmp(&b.timestamp_ms())
                    .then_with(|| a.snapshot_id().cmp(&b.snapshot_id()))
            })
    })
}

#[cfg(test)]
mod tests {
    #![expect(clippy::expect_used)]

    use std::collections::HashMap;
    use std::sync::Arc;

    use object_store::memory::InMemory;

    use super::*;
    use crate::spec::metadata::format::FormatVersion;
    use crate::spec::types::{NestedField, PrimitiveType, Type};

    fn table_with_snapshot() -> Table {
        let table_url = Url::parse("memory:///iceberg/table/").expect("table URL");
        let store_ctx =
            StoreContext::new(Arc::new(InMemory::new()), &table_url).expect("store context");
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("schema");
        let snapshot = Snapshot::builder()
            .with_snapshot_id(7)
            .with_sequence_number(1)
            .with_timestamp_ms(1_000)
            .with_manifest_list("metadata/snap-7.avro")
            .with_schema_id(1)
            .build()
            .expect("snapshot");
        let metadata = TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: None,
            location: table_url.to_string(),
            last_sequence_number: 1,
            last_updated_ms: 1_000,
            last_column_id: 1,
            schemas: vec![schema],
            current_schema_id: 1,
            partition_specs: vec![],
            default_spec_id: 0,
            last_partition_id: 0,
            properties: HashMap::new(),
            current_snapshot_id: Some(7),
            next_row_id: None,
            encryption_keys: vec![],
            snapshots: vec![snapshot],
            snapshot_log: vec![],
            metadata_log: vec![],
            sort_orders: vec![],
            default_sort_order_id: None,
            refs: HashMap::new(),
            statistics: vec![],
            partition_statistics: vec![],
        };
        Table {
            table_url,
            store_ctx,
            metadata,
        }
    }

    fn options(
        snapshot_id: Option<i64>,
        use_ref: Option<&str>,
        timestamp_as_of: Option<&str>,
    ) -> IcebergReadOptions {
        IcebergReadOptions {
            use_ref: use_ref.map(ToString::to_string),
            snapshot_id,
            timestamp_as_of: timestamp_as_of.map(ToString::to_string),
            metadata_as_data_read: false,
        }
    }

    #[test]
    fn rejects_snapshot_id_with_ref() {
        let error = table_with_snapshot()
            .select_snapshot(&options(Some(7), Some(MAIN_BRANCH), None))
            .expect_err("snapshot ID and ref must be mutually exclusive");
        assert!(format!("{error}").contains("snapshot"));
    }

    #[test]
    fn rejects_snapshot_id_with_timestamp() {
        let error = table_with_snapshot()
            .select_snapshot(&options(Some(7), None, Some("2024-01-01T00:00:00Z")))
            .expect_err("snapshot ID and timestamp must be mutually exclusive");
        assert!(format!("{error}").contains("snapshot"));
    }

    #[test]
    fn rejects_ref_with_timestamp() {
        let error = table_with_snapshot()
            .select_snapshot(&options(
                None,
                Some(MAIN_BRANCH),
                Some("2024-01-01T00:00:00Z"),
            ))
            .expect_err("ref and timestamp must be mutually exclusive");
        assert!(format!("{error}").contains("snapshot"));
    }
}
