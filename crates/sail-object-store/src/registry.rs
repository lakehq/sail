use std::sync::Arc;

use dashmap::DashMap;
use datafusion::execution::object_store::ObjectStoreRegistry;
use datafusion_common::{Result, plan_datafusion_err};
#[cfg(feature = "hdfs")]
use hdfs_native_object_store::HdfsObjectStoreBuilder;
use log::debug;
use object_store::azure::{MicrosoftAzure, MicrosoftAzureBuilder};
use object_store::gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder};
use object_store::http::{HttpBuilder, HttpStore};
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::{ObjectStore, ObjectStoreScheme};
use sail_common::runtime::RuntimeHandle;
use url::Url;

use crate::hugging_face::HuggingFaceObjectStore;
use crate::layers::lazy::LazyObjectStore;
use crate::layers::logging::LoggingObjectStore;
use crate::layers::runtime::RuntimeAwareObjectStore;
use crate::s3::get_s3_object_store;

#[derive(Debug, Eq, PartialEq, Hash)]
struct ObjectStoreKey {
    scheme: String,
    authority: String,
    session_fingerprint: Option<String>,
}

impl ObjectStoreKey {
    fn new(url: &Url, session_fingerprint: Option<&str>) -> Self {
        let key = Self {
            scheme: url.scheme().to_string(),
            authority: url.authority().to_string(),
            session_fingerprint: session_fingerprint.map(ToString::to_string),
        };
        debug!("ObjectStoreKey::new({url}, {session_fingerprint:?}) = {key:?}");
        key
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
                session_fingerprint: None,
            },
            Arc::new(LoggingObjectStore::new(Arc::new(LocalFileSystem::new()))),
        );
        Self { stores, runtime }
    }

    pub fn register_session_store(
        &self,
        url: &Url,
        session_fingerprint: &str,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        let key = ObjectStoreKey::new(url, Some(session_fingerprint));
        self.stores.insert(key, store)
    }

    pub fn get_store_with_session(
        &self,
        url: &Url,
        session_fingerprint: Option<&str>,
    ) -> Result<Arc<dyn ObjectStore>> {
        if let Some(session_fingerprint) = session_fingerprint {
            let key = ObjectStoreKey::new(url, Some(session_fingerprint));
            if let Some(store) = self.stores.get(&key) {
                return Ok(store.clone());
            }
        }
        self.get_store(url)
    }
}

impl ObjectStoreRegistry for DynamicObjectStoreRegistry {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        let key = ObjectStoreKey::new(url, None);
        self.stores.insert(key, store)
    }

    fn get_store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>> {
        let key = ObjectStoreKey::new(url, None);

        // Use entry API for atomic get-or-insert
        let store = self
            .stores
            .entry(key)
            .or_try_insert_with(|| -> object_store::Result<Arc<dyn ObjectStore>> {
                Ok(Arc::new(RuntimeAwareObjectStore::try_new(
                    || get_dynamic_object_store(url),
                    self.runtime.io().clone(),
                )?))
            })?
            .clone();

        Ok(store)
    }
}

fn get_dynamic_object_store(url: &Url) -> object_store::Result<Arc<dyn ObjectStore>> {
    let key = ObjectStoreKey::new(url, None);
    let store: Arc<dyn ObjectStore> = match key.scheme.as_str() {
        #[cfg(feature = "hdfs")]
        "hdfs" => Arc::new(
            HdfsObjectStoreBuilder::new()
                .with_url(url.as_str())
                .build()?,
        ),
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
            Arc::new(HuggingFaceObjectStore::try_new()?)
        }
        _ => {
            let (scheme, _path) = ObjectStoreScheme::parse(url)?;
            let store: Arc<dyn ObjectStore> = match scheme {
                ObjectStoreScheme::Local => Arc::new(LocalFileSystem::new()),
                ObjectStoreScheme::Memory => Arc::new(InMemory::new()),
                ObjectStoreScheme::AmazonS3 => {
                    let url = url.clone();
                    let store = LazyObjectStore::new(move || {
                        let url = url.clone();
                        async move { get_s3_object_store(&url).await }
                    });
                    Arc::new(store)
                }
                ObjectStoreScheme::MicrosoftAzure => {
                    let url = url.clone();
                    let store = LazyObjectStore::new(move || {
                        let url = url.clone();
                        async move { get_azure_object_store(&url).await }
                    });
                    Arc::new(store)
                }
                ObjectStoreScheme::GoogleCloudStorage => {
                    let url = url.clone();
                    let store = LazyObjectStore::new(move || {
                        let url = url.clone();
                        async move { get_gcs_object_store(&url).await }
                    });
                    Arc::new(store)
                }
                ObjectStoreScheme::Http => {
                    let url = url[..url::Position::BeforePath].to_string();
                    let store = LazyObjectStore::new(move || {
                        let url = url.to_string();
                        async move { get_http_object_store(url).await }
                    });
                    Arc::new(store)
                }
                other => {
                    return Err(object_store::Error::Generic {
                        store: "unknown",
                        source: Box::new(plan_datafusion_err!(
                            "unsupported object store URL: {url} for {other:?}"
                        )),
                    });
                }
            };
            store
        }
    };
    Ok(Arc::new(LoggingObjectStore::new(store)))
}

// The following implementations are basic for now just to get preliminary functionality.
pub async fn get_azure_object_store(url: &Url) -> object_store::Result<MicrosoftAzure> {
    MicrosoftAzureBuilder::from_env()
        .with_url(url.to_string())
        .build()
}

pub async fn get_gcs_object_store(url: &Url) -> object_store::Result<GoogleCloudStorage> {
    GoogleCloudStorageBuilder::from_env()
        .with_url(url.to_string())
        .build()
}

pub async fn get_http_object_store(url: String) -> object_store::Result<HttpStore> {
    let options: Vec<(String, String)> = std::env::vars().collect();
    let builder = options.into_iter().fold(
        HttpBuilder::new().with_url(url),
        |builder, (key, value)| match key.parse() {
            Ok(k) => builder.with_config(k, value),
            Err(_) => builder,
        },
    );
    builder.build()
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use datafusion::execution::object_store::ObjectStoreRegistry;
    use object_store::ObjectStore;
    use object_store::memory::InMemory;
    use sail_common::runtime::RuntimeHandle;
    use tokio::runtime::Handle;
    use url::Url;

    use super::{DynamicObjectStoreRegistry, ObjectStoreKey};

    #[test]
    fn object_store_key_separates_session_fingerprints() {
        let url = Url::parse("s3://bucket/table/path").unwrap();

        let no_session = ObjectStoreKey::new(&url, None);
        let session_a = ObjectStoreKey::new(&url, Some("session-a"));
        let session_b = ObjectStoreKey::new(&url, Some("session-b"));

        assert_ne!(no_session, session_a);
        assert_ne!(session_a, session_b);
    }

    #[tokio::test]
    async fn registry_prefers_registered_session_store() {
        let handle = Handle::current();
        let runtime = RuntimeHandle::new(handle.clone(), handle);
        let registry = DynamicObjectStoreRegistry::new(runtime);
        let url = Url::parse("file:///tmp/sail-object-store-session-test").unwrap();
        let store_a: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store_b: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        registry.register_session_store(&url, "session-a", store_a.clone());
        registry.register_session_store(&url, "session-b", store_b.clone());

        let resolved_a = registry
            .get_store_with_session(&url, Some("session-a"))
            .unwrap();
        let resolved_b = registry
            .get_store_with_session(&url, Some("session-b"))
            .unwrap();

        assert!(Arc::ptr_eq(&resolved_a, &store_a));
        assert!(Arc::ptr_eq(&resolved_b, &store_b));

        let fallback = registry
            .get_store_with_session(&url, Some("unknown"))
            .unwrap();
        let default_store = registry.get_store(&url).unwrap();
        assert!(Arc::ptr_eq(&fallback, &default_store));
    }
}
