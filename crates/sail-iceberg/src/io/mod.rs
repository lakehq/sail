use std::sync::Arc;

use bytes::Bytes;
use datafusion::common::DataFusionError;
use object_store::path::Path as ObjectPath;
use url::Url;

use crate::spec::{FormatVersion, Manifest, ManifestList};

#[derive(Clone)]
pub struct IcebergObjectStore {
    pub object_store: Arc<dyn object_store::ObjectStore>,
    pub root: ObjectPath,
}

impl IcebergObjectStore {
    pub fn new(object_store: Arc<dyn object_store::ObjectStore>, root: ObjectPath) -> Self {
        Self { object_store, root }
    }

    pub fn child(&self, rel: &str) -> ObjectPath {
        let mut path = self.root.clone();
        for component in rel.split('/').filter(|s| !s.is_empty()) {
            path = path.child(component);
        }
        path
    }

    pub async fn put(&self, path: &ObjectPath, data: Bytes) -> Result<(), String> {
        self.object_store
            .put(path, data.into())
            .await
            .map(|_| ())
            .map_err(|e| format!("object_store put error: {e}"))
    }

    pub async fn put_rel(&self, rel: &str, data: Bytes) -> Result<ObjectPath, String> {
        let full = self.child(rel);
        self.put(&full, data).await?;
        Ok(full)
    }

    pub async fn exists(&self, path: &ObjectPath) -> Result<bool, String> {
        self.object_store
            .head(path)
            .await
            .map(|_| true)
            .or_else(|e| match e {
                object_store::Error::NotFound { .. } => Ok(false),
                other => Err(format!("object_store head error: {other}")),
            })
    }
}

#[derive(Clone)]
pub struct StoreContext {
    pub base: Arc<dyn object_store::ObjectStore>,
    pub prefixed: Arc<dyn object_store::ObjectStore>,
}

impl StoreContext {
    pub fn new(base: Arc<dyn object_store::ObjectStore>, table_url: &Url) -> Self {
        let base_path = ObjectPath::from(table_url.path());
        let prefixed: Arc<dyn object_store::ObjectStore> = Arc::new(
            object_store::prefix::PrefixStore::new(base.clone(), base_path),
        );
        Self { base, prefixed }
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

pub fn resolve_data_object_path(table_base_path: &str, raw_path: &str) -> ObjectPath {
    if let Ok(url) = Url::parse(raw_path) {
        let encoded_path = url.path();
        let path_no_leading = encoded_path.strip_prefix('/').unwrap_or(encoded_path);
        if let Ok(p) = ObjectPath::parse(path_no_leading) {
            return p;
        }
        return ObjectPath::from(path_no_leading);
    }

    if raw_path.starts_with(object_store::path::DELIMITER) {
        let no_leading = raw_path.strip_prefix('/').unwrap_or(raw_path);
        return ObjectPath::from(no_leading);
    }

    let base_no_leading = table_base_path.strip_prefix('/').unwrap_or(table_base_path);
    let mut base = ObjectPath::from(base_no_leading);
    for comp in raw_path.split('/').filter(|s| !s.is_empty()) {
        base = base.child(comp);
    }
    base
}
