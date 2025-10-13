use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{not_impl_err, plan_err, DataFusionError, Result};
use datafusion::physical_plan::ExecutionPlan;
use sail_common_datafusion::datasource::{SinkInfo, SourceInfo, TableFormat};
use url::Url;

use crate::datasource::provider::IcebergTableProvider;
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

        log::info!("[ICEBERG] Creating table provider for paths: {:?}", paths);
        let table_url = Self::parse_table_url(ctx, paths).await?;
        log::info!("[ICEBERG] Parsed table URL: {}", table_url);

        let (iceberg_schema, snapshot, partition_specs) =
            load_table_metadata(ctx, &table_url).await?;
        log::info!(
            "[ICEBERG] Loaded metadata, snapshot_id: {}",
            snapshot.snapshot_id()
        );

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
        _ctx: &dyn Session,
        _info: SinkInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("Writing to Iceberg tables is not yet implemented")
    }
}

impl IcebergTableFormat {
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
async fn load_table_metadata(
    ctx: &dyn Session,
    table_url: &Url,
) -> Result<(Schema, Snapshot, Vec<PartitionSpec>)> {
    log::debug!("[ICEBERG] Loading table metadata from: {}", table_url);
    let object_store = ctx
        .runtime_env()
        .object_store_registry
        .get_store(table_url)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let metadata_location = find_latest_metadata_file(&object_store, table_url).await?;
    log::info!("[ICEBERG] Found metadata file: {}", metadata_location);

    let metadata_path = object_store::path::Path::from(metadata_location.as_str());
    let metadata_data = object_store
        .get(&metadata_path)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .bytes()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    log::debug!(
        "[ICEBERG] Read {} bytes from metadata file",
        metadata_data.len()
    );

    let table_metadata = TableMetadata::from_json(&metadata_data).map_err(|e| {
        log::error!("[ICEBERG] Failed to parse table metadata: {:?}", e);
        DataFusionError::External(Box::new(e))
    })?;

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

    let partition_specs = table_metadata.partition_specs.clone();
    Ok((schema, snapshot, partition_specs))
}

/// Find the latest metadata file in the table location
async fn find_latest_metadata_file(
    object_store: &Arc<dyn object_store::ObjectStore>,
    table_url: &Url,
) -> Result<String> {
    use futures::TryStreamExt;
    use object_store::path::Path as ObjectPath;

    log::debug!("[ICEBERG] Finding latest metadata file");
    let version_hint_path =
        ObjectPath::from(format!("{}metadata/version-hint.text", table_url.path()).as_str());

    if let Ok(version_hint_data) = object_store.get(&version_hint_path).await {
        if let Ok(version_hint_bytes) = version_hint_data.bytes().await {
            if let Ok(version_hint) = String::from_utf8(version_hint_bytes.to_vec()) {
                let version = version_hint.trim().parse::<i32>().unwrap_or(0);
                let metadata_file =
                    format!("{}/metadata/v{}.metadata.json", table_url.path(), version);
                log::debug!("[ICEBERG] Using version hint: {}", version);
                return Ok(metadata_file);
            }
        }
    }

    log::debug!("[ICEBERG] No version hint, listing metadata directory");
    let metadata_prefix = ObjectPath::from(format!("{}metadata/", table_url.path()).as_str());
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
            files.sort_by_key(|(version, _, _)| *version);

            if let Some((_, latest_file, _)) = files.last() {
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
