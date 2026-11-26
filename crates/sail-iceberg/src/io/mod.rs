// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::common::DataFusionError;
use object_store::ObjectStoreExt;
use object_store::path::Path as ObjectPath;
use url::Url;

use crate::spec::delete_index::{DeleteFileIndex, DeleteFileRef};
use crate::spec::{
    FormatVersion, Manifest, ManifestContentType, ManifestList, ManifestStatus, PartitionSpec,
};

pub(crate) mod deletion_vector;
pub(crate) mod puffin;
#[cfg(test)]
pub(crate) mod test_store;

#[derive(Clone)]
pub struct StoreContext {
    pub base: Arc<dyn object_store::ObjectStore>,
    pub prefixed: Arc<dyn object_store::ObjectStore>,
    pub prefix_path: ObjectPath,
}

impl StoreContext {
    pub fn new(
        base: Arc<dyn object_store::ObjectStore>,
        table_url: &Url,
    ) -> Result<Self, DataFusionError> {
        let base_path = crate::utils::url_to_object_path(table_url)?;
        let prefixed: Arc<dyn object_store::ObjectStore> = Arc::new(
            object_store::prefix::PrefixStore::new(base.clone(), base_path.clone()),
        );
        Ok(Self {
            base,
            prefixed,
            prefix_path: base_path,
        })
    }

    pub fn resolve<'a>(
        &'a self,
        raw: &str,
    ) -> Result<(&'a Arc<dyn object_store::ObjectStore>, ObjectPath), DataFusionError> {
        if crate::utils::parse_absolute_url(raw).is_some() {
            return Ok((&self.base, self.resolve_to_absolute_path(raw)?));
        }
        if raw.starts_with(object_store::path::DELIMITER) {
            let no_leading = raw.strip_prefix('/').unwrap_or(raw);
            return Ok((&self.base, ObjectPath::from(no_leading)));
        }
        Ok((
            &self.prefixed,
            ObjectPath::parse(raw).map_err(|e| DataFusionError::External(Box::new(e)))?,
        ))
    }

    /// Resolve a statistics file as a URI, including a metadata directory on another store.
    pub(crate) fn statistics_object(
        &self,
        ctx: &datafusion::execution::TaskContext,
        location: &str,
    ) -> Result<(Arc<dyn object_store::ObjectStore>, ObjectPath), DataFusionError> {
        if let Some(url) = crate::utils::parse_absolute_url(location) {
            let store = ctx.runtime_env().object_store_registry.get_store(&url)?;
            let path = self.statistics_uri_path(&url, location)?;
            Ok((store, path))
        } else {
            let (store, path) = self.resolve(location)?;
            Ok((store.clone(), path))
        }
    }

    pub(crate) fn statistics_uri_path(
        &self,
        url: &Url,
        location: &str,
    ) -> Result<ObjectPath, DataFusionError> {
        if url.scheme() == "file" {
            let file = url.to_file_path().map_err(|()| {
                DataFusionError::Plan(format!("Invalid statistics file URI {url}"))
            })?;
            Ok(ObjectPath::from_absolute_path(file)?)
        } else {
            self.resolve_to_absolute_path(location)
        }
    }

    pub fn resolve_to_absolute_path(&self, raw_path: &str) -> Result<ObjectPath, DataFusionError> {
        if let Some(url) = crate::utils::parse_absolute_url(raw_path) {
            // Hadoop Path strings in manifests contain literal object keys, including
            // spaces and Hive percent escapes. URL parsing would re-encode them.
            if let Some((_, rest)) = raw_path.split_once(':') {
                let path = match rest.strip_prefix("//") {
                    Some(authority_path) => authority_path.split_once('/').map(|(_, path)| path),
                    None => rest.strip_prefix('/'),
                };
                if let Some(path) = path {
                    #[cfg(windows)]
                    if url.scheme() == "file" {
                        let path = url::Url::parse(&raw_path.replace('%', "%25"))
                            .map_err(|e| DataFusionError::Plan(e.to_string()))?
                            .to_file_path()
                            .map_err(|_| {
                                DataFusionError::Plan(format!("Invalid file path: {raw_path}"))
                            })?;
                        return Ok(ObjectPath::from_absolute_path(path)?);
                    }
                    return Ok(ObjectPath::parse(path)?);
                }
            }
            return crate::utils::url_to_object_path(&url);
        }

        if raw_path.starts_with(object_store::path::DELIMITER) {
            let no_leading = raw_path.strip_prefix('/').unwrap_or(raw_path);
            return Ok(ObjectPath::from(no_leading));
        }

        let mut full = self.prefix_path.clone();
        for comp in raw_path.split('/').filter(|s| !s.is_empty()) {
            full = full.join(comp);
        }
        Ok(full)
    }
}

pub async fn load_manifest_list(
    store_ctx: &StoreContext,
    manifest_list_str: &str,
) -> Result<ManifestList, DataFusionError> {
    load_manifest_list_with_version(store_ctx, manifest_list_str, FormatVersion::V3).await
}

pub async fn load_manifest_list_with_version(
    store_ctx: &StoreContext,
    manifest_list_str: &str,
    format_version: FormatVersion,
) -> Result<ManifestList, DataFusionError> {
    let (store_ref, path) = store_ctx.resolve(manifest_list_str)?;
    let bytes = store_ref
        .get(&path)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .bytes()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    ManifestList::parse_with_version(&bytes, format_version).map_err(DataFusionError::Execution)
}

pub async fn load_manifest(
    store_ctx: &StoreContext,
    manifest_path_str: &str,
) -> Result<Manifest, DataFusionError> {
    let (store_ref, path) = store_ctx.resolve(manifest_path_str)?;
    let bytes = store_ref
        .get(&path)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .bytes()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    Manifest::parse_avro(&bytes).map_err(DataFusionError::Execution)
}

/// Build a [`DeleteFileIndex`] scoped to the current snapshot.
pub(crate) async fn load_delete_file_index(
    partition_specs: &[PartitionSpec],
    format_version: FormatVersion,
    store_ctx: &StoreContext,
    manifest_list: &ManifestList,
) -> datafusion_common::Result<DeleteFileIndex> {
    let spec_map: std::collections::HashMap<i32, PartitionSpec> = partition_specs
        .iter()
        .map(|s| (s.spec_id(), s.clone()))
        .collect();

    let mut index = DeleteFileIndex::new();
    for manifest_file in manifest_list
        .entries()
        .iter()
        .filter(|mf| mf.content == ManifestContentType::Deletes)
    {
        let manifest_path_str = manifest_file.manifest_path.as_str();
        let manifest = load_manifest(store_ctx, manifest_path_str).await?;
        let partition_spec_id = manifest_file.partition_spec_id;
        let is_unpartitioned = spec_map
            .get(&partition_spec_id)
            .map(|s| s.is_unpartitioned())
            .unwrap_or(false);
        let parent_seq = manifest_file.sequence_number;

        for entry_ref in manifest.entries().iter() {
            let entry = entry_ref.as_ref();
            if !matches!(
                entry.status,
                ManifestStatus::Added | ManifestStatus::Existing
            ) {
                continue;
            }
            let mut df = entry.data_file.clone();
            df.partition_spec_id = partition_spec_id;
            let seq = entry.sequence_number.unwrap_or(parent_seq);
            let file_ref = DeleteFileRef {
                data_file: df,
                data_sequence_number: seq,
                partition_spec_id,
                is_unpartitioned_spec: is_unpartitioned,
            };
            if file_ref.is_deletion_vector() && format_version != FormatVersion::V3 {
                return datafusion_common::plan_err!(
                    "Iceberg deletion vectors require format-version 3"
                );
            }
            index.insert(file_ref).map_err(|e| {
                datafusion::common::DataFusionError::Plan(format!(
                    "failed to index Iceberg delete file: {e}"
                ))
            })?;
        }
    }
    Ok(index)
}
