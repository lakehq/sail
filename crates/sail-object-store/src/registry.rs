use std::sync::Arc;

use dashmap::DashMap;
use datafusion::execution::object_store::ObjectStoreRegistry;
use datafusion_common::{plan_datafusion_err, Result};
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
}

impl ObjectStoreKey {
    fn new(url: &Url) -> Self {
        let key = Self {
            scheme: url.scheme().to_string(),
            authority: url.authority().to_string(),
        };
        debug!("ObjectStoreKey::new({url}) = {key:?}");
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
            },
            Arc::new(LoggingObjectStore::new(Arc::new(LocalFileSystem::new()))),
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
                if self.runtime.io_runtime_for_object_store() {
                    Ok(Arc::new(RuntimeAwareObjectStore::try_new(
                        || get_dynamic_object_store(url),
                        self.runtime.io().clone(),
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
                    })
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
