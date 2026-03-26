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
use object_store::path::Path as ObjectPath;
use object_store::ObjectStoreExt;
use url::Url;

use crate::datasource::provider::IcebergTableProvider;
use crate::io::StoreContext;
use crate::operations::Transaction;
use crate::options::TableIcebergOptions;
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
        log::trace!("Loading Iceberg table: {}", table_url);
        let object_store = ctx
            .runtime_env()
            .object_store_registry
            .get_store(&table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let store_ctx = StoreContext::new(object_store.clone(), &table_url)?;
        let metadata_location =
            metadata_loader::find_latest_metadata_file(&object_store, &table_url).await?;
        log::trace!("Found Iceberg metadata file at {}", metadata_location);
        let metadata_path = ObjectPath::from(metadata_location.as_str());
        let metadata_data = object_store
            .get(&metadata_path)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .bytes()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
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
        options: &TableIcebergOptions,
    ) -> Result<(Schema, Snapshot, Vec<PartitionSpec>)> {
        let (schema, snapshot) = self.select_snapshot(options)?;
        Ok((schema, snapshot, self.metadata.partition_specs.clone()))
    }

    /// Build an Iceberg table provider that reflects the requested snapshot options.
    pub fn to_provider(&self, options: &TableIcebergOptions) -> Result<IcebergTableProvider> {
        let (schema, snapshot, partition_specs) = self.scan_state(options)?;
        IcebergTableProvider::new(
            self.table_url.to_string(),
            schema,
            snapshot,
            partition_specs,
            self.metadata.default_spec_id,
        )
    }

    /// Create a Transaction anchored at the current snapshot, if one exists.
    pub fn new_transaction(&self) -> Option<Transaction> {
        self.metadata
            .current_snapshot()
            .cloned()
            .map(|snapshot| Transaction::new(self.table_url.to_string(), snapshot))
    }

    fn select_snapshot(&self, options: &TableIcebergOptions) -> Result<(Schema, Snapshot)> {
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
