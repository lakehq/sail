use std::sync::Arc;

use dashmap::DashMap;
use datafusion::execution::object_store::ObjectStoreRegistry;
use datafusion_common::{plan_datafusion_err, Result};
#[cfg(feature = "hdfs")]
use hdfs_native_object_store::HdfsObjectStore;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use sail_common::runtime::RuntimeHandle;
use url::Url;

use crate::object_store::hugging_face::HuggingFaceObjectStore;
use crate::object_store::layers::lazy::LazyObjectStore;
use crate::object_store::layers::runtime::RuntimeAwareObjectStore;
use crate::object_store::s3::get_s3_object_store;

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
    stores: DashMap<ObjectStoreKey, Arc<dyn ObjectStore>>,
    runtime: RuntimeHandle,
}

impl DynamicObjectStoreRegistry {
    pub fn new(runtime: RuntimeHandle) -> Self {
        let stores: DashMap<ObjectStoreKey, Arc<dyn ObjectStore>> = DashMap::new();
        stores.insert(
            ObjectStoreKey {
                scheme: "file".to_string(),
                authority: "".to_string(),
            },
            Arc::new(LocalFileSystem::new()),
        );
        Self { stores, runtime }
    }
}

impl ObjectStoreRegistry for DynamicObjectStoreRegistry {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        let key = ObjectStoreKey::new(url);
        self.stores.insert(key, store)
    }

    fn get_store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>> {
        let key = ObjectStoreKey::new(url);

        // Use entry API for atomic get-or-insert
        let store = self
            .stores
            .entry(key)
            .or_try_insert_with(|| {
                if let Some(handle) = self.runtime.secondary() {
                    Ok(Arc::new(RuntimeAwareObjectStore::try_new(
                        || get_dynamic_object_store(url),
                        handle.clone(),
                    )?))
                } else {
                    get_dynamic_object_store(url)
                }
            })?
            .clone();

        Ok(store)
    }
}

fn get_dynamic_object_store(url: &Url) -> object_store::Result<Arc<dyn ObjectStore>> {
    let key = ObjectStoreKey::new(url);
    match key.scheme.as_str() {
        "s3" => {
            let url = url.clone();
            let store = LazyObjectStore::new(move || {
                let url = url.clone();
                async move { get_s3_object_store(&url).await }
            });
            Ok(Arc::new(store))
        }
        #[cfg(feature = "hdfs")]
        "hdfs" => {
            let store = HdfsObjectStore::with_url(url.as_str())?;
            Ok(Arc::new(store))
        }
        "hf" => {
            if key.authority != "datasets" {
                return Err(object_store::Error::Generic {
                    store: "Hugging Face",
                    source: Box::new(plan_datafusion_err!(
                        "unsupported repository type: {}",
                        key.authority
                    )),
                });
            }
            Ok(Arc::new(HuggingFaceObjectStore::try_new()?))
        }
        _ => Err(object_store::Error::Generic {
            store: "unknown",
            source: Box::new(plan_datafusion_err!("unsupported object store URL: {url}")),
        }),
    }
}
