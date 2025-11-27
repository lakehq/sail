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

use object_store::path::Path as ObjectPath;
use url::Url;

use crate::error::IcebergResult;
use crate::spec::{FormatVersion, Manifest, ManifestList};

#[derive(Clone)]
pub struct StoreContext {
    pub base: Arc<dyn object_store::ObjectStore>,
    pub prefixed: Arc<dyn object_store::ObjectStore>,
    pub prefix_path: ObjectPath,
}

impl StoreContext {
    pub fn new(base: Arc<dyn object_store::ObjectStore>, table_url: &Url) -> IcebergResult<Self> {
        let base_path = ObjectPath::parse(table_url.path())?;
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
    ) -> IcebergResult<(&'a Arc<dyn object_store::ObjectStore>, ObjectPath)> {
        if let Ok(url) = Url::parse(raw) {
            return Ok((&self.base, ObjectPath::parse(url.path())?));
        }
        if raw.starts_with(object_store::path::DELIMITER) {
            return Ok((&self.base, ObjectPath::parse(raw)?));
        }
        Ok((&self.prefixed, ObjectPath::parse(raw)?))
    }

    pub fn resolve_to_absolute_path(&self, raw_path: &str) -> IcebergResult<ObjectPath> {
        if let Ok(url) = Url::parse(raw_path) {
            return Ok(ObjectPath::parse(url.path())?);
        }

        if raw_path.starts_with(object_store::path::DELIMITER) {
            return Ok(ObjectPath::parse(raw_path)?);
        }

        let mut full = self.prefix_path.clone();
        for comp in raw_path.split('/').filter(|s| !s.is_empty()) {
            full = full.child(comp);
        }
        Ok(full)
    }
}

pub async fn load_manifest_list(
    store_ctx: &StoreContext,
    manifest_list_str: &str,
) -> IcebergResult<ManifestList> {
    let (store_ref, path) = store_ctx.resolve(manifest_list_str)?;
    let bytes = store_ref.get(&path).await?.bytes().await?;
    ManifestList::parse_with_version(&bytes, FormatVersion::V2)
}

pub async fn load_manifest(
    store_ctx: &StoreContext,
    manifest_path_str: &str,
) -> IcebergResult<Manifest> {
    let (store_ref, path) = store_ctx.resolve(manifest_path_str)?;
    let bytes = store_ref.get(&path).await?.bytes().await?;
    Manifest::parse_avro(&bytes)
}
