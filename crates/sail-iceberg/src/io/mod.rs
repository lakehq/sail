use std::sync::Arc;

use datafusion::common::DataFusionError;
use object_store::path::Path as ObjectPath;
use url::Url;

use crate::spec::{FormatVersion, Manifest, ManifestList};

#[derive(Clone)]
pub struct StoreContext {
    pub base: Arc<dyn object_store::ObjectStore>,
    pub prefixed: Arc<dyn object_store::ObjectStore>,
    prefix_path: ObjectPath,
}

impl StoreContext {
    pub fn new(base: Arc<dyn object_store::ObjectStore>, table_url: &Url) -> Self {
        let base_path = ObjectPath::from(table_url.path());
        let prefixed: Arc<dyn object_store::ObjectStore> = Arc::new(
            object_store::prefix::PrefixStore::new(base.clone(), base_path.clone()),
        );
        Self {
            base,
            prefixed,
            prefix_path: base_path,
        }
    }

    pub fn resolve<'a>(
        &'a self,
        raw: &str,
    ) -> (&'a Arc<dyn object_store::ObjectStore>, ObjectPath) {
        if let Ok(url) = Url::parse(raw) {
            let p = url.path();
            let no_leading = p.strip_prefix('/').unwrap_or(p);
            return (&self.base, ObjectPath::from(no_leading));
        }
        if raw.starts_with(object_store::path::DELIMITER) {
            let no_leading = raw.strip_prefix('/').unwrap_or(raw);
            return (&self.base, ObjectPath::from(no_leading));
        }
        (&self.prefixed, ObjectPath::from(raw))
    }

    pub fn resolve_to_absolute_path(&self, raw_path: &str) -> ObjectPath {
        if let Ok(url) = Url::parse(raw_path) {
            let encoded_path = url.path();
            let path_no_leading = encoded_path.strip_prefix('/').unwrap_or(encoded_path);
            return ObjectPath::from(path_no_leading);
        }

        if raw_path.starts_with(object_store::path::DELIMITER) {
            let no_leading = raw_path.strip_prefix('/').unwrap_or(raw_path);
            return ObjectPath::from(no_leading);
        }

        let mut full = self.prefix_path.clone();
        for comp in raw_path.split('/').filter(|s| !s.is_empty()) {
            full = full.child(comp);
        }
        full
    }
}

pub async fn load_manifest_list(
    store_ctx: &StoreContext,
    manifest_list_str: &str,
) -> Result<ManifestList, DataFusionError> {
    let (store_ref, path) = store_ctx.resolve(manifest_list_str);
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
    let (store_ref, path) = store_ctx.resolve(manifest_path_str);
    let bytes = store_ref
        .get(&path)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .bytes()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    Manifest::parse_avro(&bytes).map_err(DataFusionError::Execution)
}
