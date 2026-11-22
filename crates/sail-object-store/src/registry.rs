// SPDX-License-Identifier: Apache-2.0

use std::fmt;
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
use object_store::multipart::MultipartStore;
use object_store::{ObjectStore, ObjectStoreScheme};
use sail_common::runtime::RuntimeHandle;
use url::Url;

use crate::hugging_face::HuggingFaceObjectStore;
use crate::layers::lazy::LazyObjectStore;
use crate::layers::logging::LoggingObjectStore;
use crate::layers::multipart::RuntimeAwareMultipartStore;
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

/// An object store of the registry.
#[derive(Clone)]
struct StoreEntry {
    store: Arc<dyn ObjectStore>,
    /// The multipart upload interface of the same object store, if the object
    /// store has one and the registry has created the object store itself.
    multipart: Option<Arc<dyn MultipartStore>>,
}

impl StoreEntry {
    /// An object store without a multipart upload interface.
    fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            multipart: None,
        }
    }
}

impl fmt::Debug for StoreEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoreEntry")
            .field("store", &self.store)
            .field("multipart", &self.multipart.is_some())
            .finish()
    }
}

#[derive(Debug)]
pub struct DynamicObjectStoreRegistry {
    stores: DashMap<ObjectStoreKey, StoreEntry>,
    runtime: RuntimeHandle,
}

impl DynamicObjectStoreRegistry {
    pub fn new(runtime: RuntimeHandle) -> Self {
        let stores: DashMap<ObjectStoreKey, StoreEntry> = DashMap::new();
        stores.insert(
            ObjectStoreKey {
                scheme: "file".to_string(),
                authority: "".to_string(),
                session_fingerprint: None,
            },
            StoreEntry::new(Arc::new(LoggingObjectStore::new(Arc::new(
                LocalFileSystem::new(),
            )))),
        );
        Self { stores, runtime }
    }

    /// Returns the multipart upload interface of the object store for the URL,
    /// or [`None`] if the object store has no such interface.
    ///
    /// The interface is available for the object stores that the registry
    /// creates for Amazon S3 and the object stores with the same protocol,
    /// Google Cloud Storage, Azure Blob Storage, and the in-memory object store.
    /// It is not available for an object store that was registered.
    pub fn multipart_store(&self, url: &Url) -> Result<Option<Arc<dyn MultipartStore>>> {
        Ok(self.entry(url)?.multipart)
    }

    fn entry(&self, url: &Url) -> Result<StoreEntry> {
        let key = ObjectStoreKey::new(url, None);

        // Use entry API for atomic get-or-insert
        let entry = self
            .stores
            .entry(key)
            .or_try_insert_with(|| -> object_store::Result<StoreEntry> {
                let io = self.runtime.io();
                let mut multipart = None;
                let store = RuntimeAwareObjectStore::try_new(
                    || {
                        let (store, store_multipart) = get_dynamic_object_store(url)?;
                        multipart = store_multipart;
                        Ok(store)
                    },
                    io.clone(),
                )?;
                Ok(StoreEntry {
                    store: Arc::new(store),
                    multipart: multipart.map(|x| {
                        Arc::new(RuntimeAwareMultipartStore::new(x, io.clone()))
                            as Arc<dyn MultipartStore>
                    }),
                })
            })?
            .clone();

        Ok(entry)
    }

    pub fn register_session_store(
        &self,
        url: &Url,
        session_fingerprint: &str,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        let key = ObjectStoreKey::new(url, Some(session_fingerprint));
        self.stores
            .insert(key, StoreEntry::new(store))
            .map(|x| x.store)
    }

    pub fn get_store_with_session(
        &self,
        url: &Url,
        session_fingerprint: Option<&str>,
    ) -> Result<Arc<dyn ObjectStore>> {
        if let Some(session_fingerprint) = session_fingerprint {
            let key = ObjectStoreKey::new(url, Some(session_fingerprint));
            if let Some(entry) = self.stores.get(&key) {
                return Ok(entry.store.clone());
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
        self.stores
            .insert(key, StoreEntry::new(store))
            .map(|x| x.store)
    }

    fn get_store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>> {
        Ok(self.entry(url)?.store)
    }
}

/// An object store that is created for a URL, and its multipart upload
/// interface if it has one.
type DynamicObjectStore = (Arc<dyn ObjectStore>, Option<Arc<dyn MultipartStore>>);

fn without_multipart(store: impl ObjectStore) -> DynamicObjectStore {
    (Arc::new(store), None)
}

fn with_multipart(store: impl ObjectStore + MultipartStore) -> DynamicObjectStore {
    let store = Arc::new(store);
    (store.clone(), Some(store))
}

fn get_dynamic_object_store(url: &Url) -> object_store::Result<DynamicObjectStore> {
    let key = ObjectStoreKey::new(url, None);
    let (store, multipart) = match key.scheme.as_str() {
        #[cfg(feature = "hdfs")]
        "hdfs" => without_multipart(
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
            without_multipart(HuggingFaceObjectStore::try_new()?)
        }
        "oss" => {
            let url = url.clone();
            let store = LazyObjectStore::new(move || {
                let url = url.clone();
                async move { get_s3_object_store(&url).await }
            });
            with_multipart(store)
        }
        _ if is_aliyun_oss_url(url) => {
            let url = url.clone();
            let store = LazyObjectStore::new(move || {
                let url = url.clone();
                async move { get_s3_object_store(&url).await }
            });
            with_multipart(store)
        }
        _ => {
            let (scheme, _path) = ObjectStoreScheme::parse(url)?;
            match scheme {
                ObjectStoreScheme::Local => without_multipart(LocalFileSystem::new()),
                ObjectStoreScheme::Memory => with_multipart(InMemory::new()),
                ObjectStoreScheme::AmazonS3 => {
                    let url = url.clone();
                    let store = LazyObjectStore::new(move || {
                        let url = url.clone();
                        async move { get_s3_object_store(&url).await }
                    });
                    with_multipart(store)
                }
                ObjectStoreScheme::MicrosoftAzure => {
                    let url = url.clone();
                    let store = LazyObjectStore::new(move || {
                        let url = url.clone();
                        async move { get_azure_object_store(&url).await }
                    });
                    with_multipart(store)
                }
                ObjectStoreScheme::GoogleCloudStorage => {
                    let url = url.clone();
                    let store = LazyObjectStore::new(move || {
                        let url = url.clone();
                        async move { get_gcs_object_store(&url).await }
                    });
                    with_multipart(store)
                }
                ObjectStoreScheme::Http => {
                    let url = url[..url::Position::BeforePath].to_string();
                    let store = LazyObjectStore::new(move || {
                        let url = url.to_string();
                        async move { get_http_object_store(url).await }
                    });
                    without_multipart(store)
                }
                other => {
                    return Err(object_store::Error::Generic {
                        store: "unknown",
                        source: Box::new(plan_datafusion_err!(
                            "unsupported object store URL: {url} for {other:?}"
                        )),
                    });
                }
            }
        }
    };
    Ok((Arc::new(LoggingObjectStore::new(store)), multipart))
}

fn is_aliyun_oss_url(url: &Url) -> bool {
    if !matches!(url.scheme(), "http" | "https") {
        return false;
    }
    let Some(host) = url.host_str() else {
        return false;
    };
    let labels = host.split('.').collect::<Vec<_>>();
    matches!(
        labels.as_slice(),
        [_, "s3", region, "aliyuncs", "com"] | ["s3", region, "aliyuncs", "com"]
            if region.starts_with("oss-")
    )
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
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStore, ObjectStoreExt};
    use sail_common::runtime::RuntimeHandle;
    use tokio::runtime::Handle;
    use url::Url;

    use super::{DynamicObjectStoreRegistry, ObjectStoreKey, is_aliyun_oss_url};

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

    #[tokio::test]
    async fn registry_has_multipart_interface_of_created_stores() {
        let handle = Handle::current();
        let runtime = RuntimeHandle::new(handle.clone(), handle);
        let registry = DynamicObjectStoreRegistry::new(runtime);
        let multipart = |url: &str| {
            registry
                .multipart_store(&Url::parse(url).unwrap())
                .unwrap()
                .is_some()
        };

        assert!(multipart("memory:///table"));
        assert!(multipart("s3://bucket/table"));
        assert!(multipart("gs://bucket/table"));
        assert!(multipart("az://container/table"));
        assert!(multipart("oss://bucket/table"));
        assert!(!multipart("file:///table"));
        assert!(!multipart("https://example.com/table"));

        // An object store that was registered has no multipart upload interface,
        // also when the registry would create one with such an interface.
        let url = Url::parse("memory://registered/table").unwrap();
        registry.register_store(&url, Arc::new(InMemory::new()));
        assert!(registry.multipart_store(&url).unwrap().is_none());
    }

    #[tokio::test]
    async fn multipart_interface_writes_to_the_object_store_of_the_registry() {
        let handle = Handle::current();
        let runtime = RuntimeHandle::new(handle.clone(), handle);
        let registry = DynamicObjectStoreRegistry::new(runtime);
        let url = Url::parse("memory:///").unwrap();
        let store = registry.get_store(&url).unwrap();
        let multipart = registry.multipart_store(&url).unwrap().unwrap();
        let path = Path::from("table/file");

        let id = multipart.create_multipart(&path).await.unwrap();
        let mut parts = vec![];
        for (i, data) in ["ab", "cd"].into_iter().enumerate() {
            let part = multipart.put_part(&path, &id, i, data.into()).await;
            parts.push(part.unwrap());
        }
        // The object does not exist before the upload is completed.
        assert!(store.head(&path).await.is_err());
        multipart
            .complete_multipart(&path, &id, parts)
            .await
            .unwrap();
        let data = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(data.as_ref(), b"abcd");

        let id = multipart.create_multipart(&path).await.unwrap();
        multipart.abort_multipart(&path, &id).await.unwrap();
        // An upload that was aborted cannot be completed.
        assert!(
            multipart
                .complete_multipart(&path, &id, vec![])
                .await
                .is_err()
        );
    }

    #[test]
    fn aliyun_oss_url_detection_matches_s3_compatible_endpoints() {
        assert!(is_aliyun_oss_url(
            &Url::parse("https://bucket.s3.oss-cn-hangzhou.aliyuncs.com/path").unwrap()
        ));
        assert!(is_aliyun_oss_url(
            &Url::parse("https://s3.oss-cn-hangzhou.aliyuncs.com/bucket/path").unwrap()
        ));
        assert!(!is_aliyun_oss_url(
            &Url::parse("https://bucket.oss-cn-hangzhou.aliyuncs.com/path").unwrap()
        ));
    }
}
