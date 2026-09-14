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
    table_url: Url,
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
            table_url: table_url.clone(),
        })
    }

    pub(crate) fn validate_location(&self, raw: &str) -> Result<(), DataFusionError> {
        if let Some(url) = crate::utils::parse_absolute_url(raw) {
            let scheme = |url: &Url| match url.scheme() {
                "s3a" | "s3n" => "s3".to_string(),
                scheme => scheme.to_string(),
            };
            if scheme(&url) != scheme(&self.table_url)
                || url.authority() != self.table_url.authority()
            {
                return Err(DataFusionError::NotImplemented(
                    "Iceberg files across object-store origins are unsupported".to_string(),
                ));
            }
        }
        Ok(())
    }

    pub fn resolve<'a>(
        &'a self,
        raw: &str,
    ) -> Result<(&'a Arc<dyn object_store::ObjectStore>, ObjectPath), DataFusionError> {
        self.validate_location(raw)?;
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
        self.validate_location(raw_path)?;
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
    use super::*;

    #[test]
    fn storage_access_preserves_object_store_origin() -> Result<(), Box<dyn std::error::Error>> {
        let context = StoreContext::new(
            Arc::new(object_store::memory::InMemory::new()),
            &Url::parse("s3://bucket/table")?,
        )?;
        assert_eq!(
            context.resolve_to_absolute_path("s3a://bucket/table/data")?,
            ObjectPath::from("table/data")
        );
        assert!(context.resolve("s3://another-bucket/table/data").is_err());
        assert!(
            context
                .resolve_to_absolute_path("s3://another-bucket/table/data")
                .is_err()
        );
        Ok(())
    }
}
