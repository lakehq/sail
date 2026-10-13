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
        if let Some(url) = crate::utils::parse_absolute_url(raw) {
            return Ok((&self.base, crate::utils::url_to_object_path(&url)?));
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

    pub fn resolve_to_absolute_path(&self, raw_path: &str) -> Result<ObjectPath, DataFusionError> {
        if let Some(url) = crate::utils::parse_absolute_url(raw_path) {
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
    let (store_ref, path) = store_ctx.resolve(manifest_list_str)?;
    let bytes = store_ref
        .get(&path)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .bytes()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    // The v3 reader also accepts older manifest lists and preserves row-ID inheritance.
    ManifestList::parse_with_version(&bytes, FormatVersion::V3).map_err(DataFusionError::Execution)
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
