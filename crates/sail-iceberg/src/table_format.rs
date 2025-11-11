use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{not_impl_err, plan_err, DataFusionError, Result};
use datafusion::physical_plan::ExecutionPlan;
use sail_common_datafusion::datasource::{SinkInfo, SourceInfo, TableFormat};
use url::Url;

use crate::datasource::provider::IcebergTableProvider;
use crate::options::TableIcebergOptions;
use crate::spec::{PartitionSpec, Schema, Snapshot, TableMetadata};

#[derive(Debug)]
pub struct IcebergTableFormat;

#[async_trait]
impl TableFormat for IcebergTableFormat {
    fn name(&self) -> &str {
        "iceberg"
    }

    async fn create_provider(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableProvider>> {
        let SourceInfo {
            paths,
            schema: _schema,
            constraints: _,
            partition_by: _,
            bucket_by: _,
            sort_order: _,
            options: _options,
        } = info;

        log::trace!("Creating table provider for paths: {:?}", paths);
        let table_url = Self::parse_table_url(ctx, paths).await?;
        log::trace!("Parsed table URL: {}", table_url);

        let (iceberg_schema, snapshot, partition_specs) =
            load_table_metadata(ctx, &table_url).await?;
        log::trace!("Loaded metadata, snapshot_id: {}", snapshot.snapshot_id());

        let provider = IcebergTableProvider::new(
            table_url.to_string(),
            iceberg_schema,
            snapshot,
            partition_specs,
        )?;
        Ok(Arc::new(provider))
    }

    async fn create_writer(
        &self,
        ctx: &dyn Session,
        info: SinkInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion::physical_plan::empty::EmptyExec;
        use sail_common_datafusion::datasource::PhysicalSinkMode;

        let SinkInfo {
            input,
            path,
            mode,
            partition_by,
            bucket_by,
            sort_order,
            options: _,
        } = info;

        if bucket_by.is_some() {
            return not_impl_err!("bucketing for Iceberg format");
        }

        // Parse URL and detect table existence (file-based tables only)
        let table_url = Self::parse_table_url(ctx, vec![path]).await?;
        // Determine existence by presence of a metadata file, not by presence of a snapshot.
        // Tables created via catalogs may have metadata but no current snapshot yet.
        log::trace!("iceberg.create_writer.table_url: {}", &table_url);
        let store = ctx
            .runtime_env()
            .object_store_registry
            .get_store(&table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let exists_res = find_latest_metadata_file(&store, &table_url).await;
        log::trace!(
            "iceberg.create_writer.table_exists_result: {:?}",
            &exists_res
                .as_ref()
                .map(|_| ())
                .map_err(|e| format!("{}", e))
        );
        let table_exists = exists_res.is_ok();

        // Early mode handling (no-op or error)
        match mode {
            PhysicalSinkMode::ErrorIfExists => {
                if table_exists {
                    return plan_err!("Iceberg table already exists at path: {}", table_url);
                }
            }
            PhysicalSinkMode::IgnoreIfExists => {
                if table_exists {
                    return Ok(Arc::new(EmptyExec::new(input.schema())));
                }
            }
            // Allow full-table overwrite here; predicate-based overwrite still not supported
            PhysicalSinkMode::OverwriteIf { .. } | PhysicalSinkMode::OverwritePartitions => {
                return not_impl_err!("predicate or partition overwrite for Iceberg");
            }
            _ => {}
        }

        // Build writer → commit pipeline
        use crate::physical_plan::plan_builder::{IcebergPlanBuilder, IcebergTableConfig};

        let table_config = IcebergTableConfig {
            table_url,
            partition_columns: partition_by,
            table_exists,
        };

        // Convert logical sort requirement to physical sort exprs for SortExec
        let physical_sort = sort_order.map(|req| {
            req.into_iter()
                .map(|r| datafusion::physical_expr::PhysicalSortExpr {
                    expr: r.expr,
                    options: r.options.unwrap_or_default(),
                })
                .collect::<Vec<_>>()
        });

        let builder = IcebergPlanBuilder::new(input, table_config, mode, physical_sort, ctx);
        let exec = builder.build().await?;
        Ok(exec)
    }
}

impl IcebergTableFormat {
    pub async fn create_iceberg_provider(
        ctx: &dyn Session,
        table_url: Url,
        options: TableIcebergOptions,
    ) -> Result<Arc<dyn TableProvider>> {
        let (schema, snapshot, partition_specs) =
            load_table_metadata_with_options(ctx, &table_url, options).await?;

        let provider =
            IcebergTableProvider::new(table_url.to_string(), schema, snapshot, partition_specs)?;
        Ok(Arc::new(provider))
    }

    async fn parse_table_url(ctx: &dyn Session, paths: Vec<String>) -> Result<Url> {
        if paths.len() != 1 {
            return plan_err!(
                "Iceberg table requires exactly one path, got {}",
                paths.len()
            );
        }

        let path = &paths[0];
        let mut table_url = Url::parse(path).map_err(|e| DataFusionError::External(Box::new(e)))?;

        if !table_url.path().ends_with('/') {
            table_url.set_path(&format!("{}/", table_url.path()));
        }

        // Validate that we can access the object store
        let _object_store = ctx
            .runtime_env()
            .object_store_registry
            .get_store(&table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(table_url)
    }
}

/// Load Iceberg table metadata from the table location
pub(crate) async fn load_table_metadata(
    ctx: &dyn Session,
    table_url: &Url,
) -> Result<(Schema, Snapshot, Vec<PartitionSpec>)> {
    log::trace!("Loading table metadata from: {}", table_url);
    let object_store = ctx
        .runtime_env()
        .object_store_registry
        .get_store(table_url)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let metadata_location = find_latest_metadata_file(&object_store, table_url).await?;
    log::trace!("Found metadata file: {}", metadata_location);

    let metadata_path = object_store::path::Path::from(metadata_location.as_str());
    let metadata_data = object_store
        .get(&metadata_path)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .bytes()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    log::trace!("Read {} bytes from metadata file", metadata_data.len());

    let table_metadata = TableMetadata::from_json(&metadata_data).map_err(|e| {
        log::trace!("Failed to parse table metadata: {:?}", e);
        DataFusionError::External(Box::new(e))
    })?;

    log::trace!("Loaded metadata file: {}", &metadata_location);
    log::trace!(
        "  current_snapshot_id: {:?}",
        &table_metadata.current_snapshot_id
    );
    log::trace!("  refs: {:?}", &table_metadata.refs);
    log::trace!("  snapshots count: {}", &table_metadata.snapshots.len());

    // Get the current schema
    let schema = table_metadata
        .current_schema()
        .ok_or_else(|| {
            DataFusionError::Plan("No current schema found in table metadata".to_string())
        })?
        .clone();

    // Get the current snapshot
    let snapshot = table_metadata
        .current_snapshot()
        .ok_or_else(|| {
            DataFusionError::Plan("No current snapshot found in table metadata".to_string())
        })?
        .clone();

    log::trace!(
        "load_table_metadata: loaded snapshot id={} manifest_list={}",
        snapshot.snapshot_id(),
        snapshot.manifest_list()
    );

    let partition_specs = table_metadata.partition_specs.clone();
    Ok((schema, snapshot, partition_specs))
}

/// Load metadata and pick snapshot per options (precedence: snapshot_id > ref > timestamp > current).
pub(crate) async fn load_table_metadata_with_options(
    ctx: &dyn Session,
    table_url: &Url,
    options: TableIcebergOptions,
) -> Result<(Schema, Snapshot, Vec<PartitionSpec>)> {
    log::trace!(
        "Loading table metadata (with options) from: {}, options: {:?}",
        table_url,
        options
    );
    let object_store = ctx
        .runtime_env()
        .object_store_registry
        .get_store(table_url)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let metadata_location = find_latest_metadata_file(&object_store, table_url).await?;
    let metadata_path = object_store::path::Path::from(metadata_location.as_str());
    let metadata_data = object_store
        .get(&metadata_path)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .bytes()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let table_metadata = TableMetadata::from_json(&metadata_data).map_err(|e| {
        log::trace!("Failed to parse table metadata: {:?}", e);
        DataFusionError::External(Box::new(e))
    })?;

    // Choose snapshot according to precedence
    let chosen_snapshot = if let Some(id) = options.snapshot_id {
        table_metadata
            .snapshots
            .iter()
            .find(|s| s.snapshot_id() == id)
            .cloned()
            .ok_or_else(|| DataFusionError::Plan(format!("Snapshot with id {} not found", id)))?
    } else if let Some(ref name) = options.use_ref {
        let sid = table_metadata
            .refs
            .get(name)
            .map(|r| r.snapshot_id)
            .ok_or_else(|| DataFusionError::Plan(format!("Unknown Iceberg ref: {}", name)))?;
        table_metadata
            .snapshots
            .iter()
            .find(|s| s.snapshot_id() == sid)
            .cloned()
            .ok_or_else(|| {
                DataFusionError::Plan(format!("Snapshot for ref {} (id={}) not found", name, sid))
            })?
    } else if let Some(ts_str) = options.timestamp_as_of {
        let ts_ms =
            parse_timestamp_to_ms(&ts_str).map_err(|e| DataFusionError::Plan(e.to_string()))?;
        find_snapshot_by_ts(&table_metadata, ts_ms)
            .cloned()
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "No Iceberg snapshot exists at or before timestamp {}",
                    ts_str
                ))
            })?
    } else {
        table_metadata.current_snapshot().cloned().ok_or_else(|| {
            DataFusionError::Plan("No current snapshot found in table metadata".to_string())
        })?
    };

    // Pick schema associated with snapshot, or current schema as fallback
    let schema = if let Some(schema_id) = chosen_snapshot.schema_id() {
        table_metadata
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
        table_metadata.current_schema().cloned().ok_or_else(|| {
            DataFusionError::Plan("No current schema found in table metadata".to_string())
        })?
    };

    let partition_specs = table_metadata.partition_specs.clone();
    Ok((schema, chosen_snapshot, partition_specs))
}

/// Find the latest metadata file in the table location
pub(crate) async fn find_latest_metadata_file(
    object_store: &Arc<dyn object_store::ObjectStore>,
    table_url: &Url,
) -> Result<String> {
    use futures::TryStreamExt;
    use object_store::path::Path as ObjectPath;

    log::trace!("Finding latest metadata file");
    let version_hint_path =
        ObjectPath::parse(format!("{}metadata/version-hint.text", table_url.path()).as_str())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let mut hinted_version: Option<i32> = None;
    let mut hinted_filename: Option<String> = None;
    if let Ok(version_hint_data) = object_store.get(&version_hint_path).await {
        if let Ok(version_hint_bytes) = version_hint_data.bytes().await {
            if let Ok(version_hint) = String::from_utf8(version_hint_bytes.to_vec()) {
                let content = version_hint.trim();
                if let Ok(version) = content.parse::<i32>() {
                    log::trace!("Using numeric version hint: {}", version);
                    hinted_version = Some(version);
                } else {
                    // If the hint already contains the full metadata filename, use it as-is,
                    // otherwise append .metadata.json
                    let fname = if content.ends_with(".metadata.json") {
                        content.to_string()
                    } else {
                        format!("{}.metadata.json", content)
                    };
                    log::trace!("Using filename version hint: {}", fname);
                    hinted_filename = Some(fname);
                }
            }
        }
    }

    log::trace!("Listing metadata directory");
    let metadata_prefix = ObjectPath::parse(format!("{}metadata/", table_url.path()).as_str())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let objects = object_store.list(Some(&metadata_prefix));

    let metadata_files: Result<Vec<_>, _> = objects
        .try_filter_map(|obj| async move {
            let path_str = obj.location.to_string();
            if path_str.ends_with(".metadata.json") {
                if let Some(filename) = path_str.split('/').next_back() {
                    // Try new format first: 00001-uuid.metadata.json
                    if let Some(version_part) = filename.split('-').next() {
                        if let Ok(version) = version_part.parse::<i32>() {
                            return Ok(Some((version, path_str, obj.last_modified)));
                        }
                    }
                    // Try old format: v123.metadata.json
                    if let Some(version_str) = filename
                        .strip_prefix('v')
                        .and_then(|s| s.strip_suffix(".metadata.json"))
                    {
                        if let Ok(version) = version_str.parse::<i32>() {
                            return Ok(Some((version, path_str, obj.last_modified)));
                        }
                    }
                }
            }
            Ok(None)
        })
        .try_collect()
        .await;

    match metadata_files {
        Ok(mut files) => {
            log::trace!("find_latest_metadata_file: found files: {:?}", &files);
            files.sort_by_key(|(version, _, _)| *version);

            if let Some(fname) = hinted_filename {
                if let Some((version, path, _)) =
                    files.iter().rev().find(|(_, p, _)| p.ends_with(&fname))
                {
                    log::trace!(
                        "find_latest_metadata_file: selected by filename hint version {} path={}",
                        version,
                        &path
                    );
                    return Ok(path.clone());
                }
            } else if let Some(hint) = hinted_version {
                if let Some((version, path, _)) = files.iter().rev().find(|(v, _, _)| *v == hint) {
                    log::trace!(
                        "find_latest_metadata_file: selected by numeric hint version {} path={}",
                        version,
                        &path
                    );
                    return Ok(path.clone());
                }
            }

            if let Some((version, latest_file, _)) = files.last() {
                log::trace!(
                    "find_latest_metadata_file: selected version {} path={}",
                    version,
                    &latest_file
                );
                Ok(latest_file.clone())
            } else {
                plan_err!("No metadata files found in table location: {}", table_url)
            }
        }
        Err(e) => {
            plan_err!("Failed to list metadata directory: {}", e)
        }
    }
}

fn parse_timestamp_to_ms(s: &str) -> std::result::Result<i64, String> {
    use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};

    // Try RFC3339 first
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.with_timezone(&Utc).timestamp_millis());
    }

    // Fallback format "yyyy-MM-dd HH:mm:ss.SSS"
    let fmt = "%Y-%m-%d %H:%M:%S%.3f";
    let naive = NaiveDateTime::parse_from_str(s, fmt)
        .map_err(|e| format!("Invalid timestamp '{}': {}", s, e))?;
    Ok(Utc.from_utc_datetime(&naive).timestamp_millis())
}

fn find_snapshot_by_ts<'a>(meta: &'a TableMetadata, ts_ms: i64) -> Option<&'a Snapshot> {
    // Prefer snapshot_log if present
    if let Some(sid) = meta
        .snapshot_log
        .iter()
        .filter(|e| e.timestamp_ms <= ts_ms)
        .max_by_key(|e| e.timestamp_ms)
        .map(|e| e.snapshot_id)
    {
        return meta.snapshots.iter().find(|s| s.snapshot_id() == sid);
    }
    // Fallback to scanning snapshots by snapshot timestamp
    meta.snapshots
        .iter()
        .filter(|s| s.timestamp_ms() <= ts_ms)
        .max_by_key(|s| s.timestamp_ms())
}
