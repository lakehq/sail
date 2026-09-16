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

use crate::spec::{FormatVersion, Manifest, ManifestList};

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
        if let Some(path) = crate::utils::absolute_location_to_object_path(raw)? {
            return Ok((&self.base, path));
        }
        Ok((
            &self.prefixed,
            ObjectPath::parse(raw).map_err(|e| DataFusionError::External(Box::new(e)))?,
        ))
    }

    pub fn resolve_to_absolute_path(&self, raw_path: &str) -> Result<ObjectPath, DataFusionError> {
        if let Some(path) = crate::utils::absolute_location_to_object_path(raw_path)? {
            return Ok(path);
        }
        ObjectPath::parse(format!("{}/{raw_path}", self.prefix_path))
            .map_err(|error| DataFusionError::External(Box::new(error)))
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
    ManifestList::parse_with_version(&bytes, FormatVersion::V2).map_err(DataFusionError::Execution)
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

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;
    use object_store::{ObjectStore, PutPayload};

    use super::*;

    #[tokio::test]
    async fn resolves_manifest_locations_without_reencoding_path_characters()
    -> Result<(), Box<dyn std::error::Error>> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let relative = "data/part=a+%2B%252F+%E4%B8%AD/file #?中.parquet";
        let payload = bytes::Bytes::from_static(b"file contents");
        for (table_url, table_path) in [
            ("memory:///table%20%2520/", "table %20"),
            ("memory:///C:/table%20%2520/", "C:/table %20"),
        ] {
            let context = StoreContext::new(store.clone(), &Url::parse(table_url)?)?;
            let absolute = format!("{table_path}/{relative}");
            let expected_path = ObjectPath::parse(&absolute)?;
            store
                .put(&expected_path, PutPayload::from(payload.clone()))
                .await?;

            let mut locations = vec![
                relative.to_string(),
                format!("/{absolute}"),
                format!("memory:///{absolute}"),
            ];
            if table_path.starts_with("C:") {
                locations.push(absolute.replace('/', "\\"));
                locations.push(absolute);
            }
            for location in locations {
                let (resolved_store, path) = context.resolve(&location)?;
                assert_eq!(resolved_store.get(&path).await?.bytes().await?, payload);
                let absolute_path = context.resolve_to_absolute_path(&location)?;
                assert_eq!(absolute_path, expected_path);
                assert_eq!(store.get(&absolute_path).await?.bytes().await?, payload);
            }
        }
        Ok(())
    }
}
