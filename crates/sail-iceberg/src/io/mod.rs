use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path as ObjectPath;

pub struct IcebergObjectStore {
    pub object_store: Arc<dyn object_store::ObjectStore>,
    pub root: ObjectPath,
}

impl IcebergObjectStore {
    pub fn new(object_store: Arc<dyn object_store::ObjectStore>, root: ObjectPath) -> Self {
        Self { object_store, root }
    }

    pub fn child(&self, rel: &str) -> ObjectPath {
        self.root.child(rel)
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
