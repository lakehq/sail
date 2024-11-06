use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use datafusion::execution::object_store::ObjectStoreRegistry;
use datafusion_common::{plan_datafusion_err, plan_err, Result};
#[cfg(feature = "hdfs")]
use hdfs_native_object_store::HdfsObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use url::Url;

use crate::object_store::config::OBJECT_STORE_CONFIG;
use crate::object_store::s3::S3CredentialProvider;

#[derive(Debug, Eq, PartialEq, Hash)]
struct ObjectStoreKey {
    scheme: String,
    authority: String,
}

impl ObjectStoreKey {
    fn new(url: &Url) -> Self {
        Self {
            scheme: url.scheme().to_string(),
            authority: url.authority().to_string(),
        }
    }
}

#[derive(Debug)]
pub struct DynamicObjectStoreRegistry {
    stores: RwLock<HashMap<ObjectStoreKey, Arc<dyn ObjectStore>>>,
}

impl Default for DynamicObjectStoreRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl DynamicObjectStoreRegistry {
    pub fn new() -> Self {
        let mut stores: HashMap<_, Arc<dyn ObjectStore>> = HashMap::new();
        stores.insert(
            ObjectStoreKey {
                scheme: "file".to_string(),
                authority: "".to_string(),
            },
            Arc::new(LocalFileSystem::new()),
        );
        Self {
            stores: RwLock::new(stores),
        }
    }

    fn get_dynamic_object_store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>> {
        let key = ObjectStoreKey::new(url);
        match key.scheme.as_str() {
            "s3" => {
                let config = OBJECT_STORE_CONFIG.get().map(|x| &x.aws).ok_or_else(|| {
                    plan_datafusion_err!("AWS configuration is required for S3 object store")
                })?;
                let mut builder = AmazonS3Builder::from_env().with_bucket_name(key.authority);
                if let Some(region) = config.region() {
                    builder = builder.with_region(region.to_string());
                }
                let credentials = config.credentials_provider().ok_or_else(|| {
                    plan_datafusion_err!("AWS credentials are required for S3 object store")
                })?;
                let credentials = S3CredentialProvider::new(credentials);
                builder = builder.with_credentials(Arc::new(credentials));
                Ok(Arc::new(builder.build()?))
            }
            #[cfg(feature = "hdfs")]
            "hdfs" => {
                let store = match OBJECT_STORE_CONFIG.get() {
                    Some(config) => {
                        HdfsObjectStore::with_config(url.as_str(), config.hdfs.clone())?
                    }
                    None => HdfsObjectStore::with_url(url.as_str())?,
                };
                Ok(Arc::new(store))
            }
            _ => {
                plan_err!("unsupported object store URL: {url}")
            }
        }
    }
}

impl ObjectStoreRegistry for DynamicObjectStoreRegistry {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        let key = ObjectStoreKey::new(url);
        if let Ok(mut stores) = self.stores.write() {
            stores.insert(key, store)
        } else {
            None
        }
    }

    fn get_store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>> {
        let key = ObjectStoreKey::new(url);
        let stores = self
            .stores
            .read()
            .map_err(|e| plan_datafusion_err!("failed to get object store: {e}"))?;
        if let Some(store) = stores.get(&key) {
            Ok(Arc::clone(store))
        } else {
            self.get_dynamic_object_store(url)
        }
    }
}
