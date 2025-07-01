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

use crate::hugging_face::HuggingFaceObjectStore;
use crate::layers::lazy::LazyObjectStore;
use crate::layers::runtime::RuntimeAwareObjectStore;
use crate::s3::get_s3_object_store;

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
        "delta-rs" => {
            // Parse delta-rs URL back to original location
            // Format: delta-rs://{original_scheme}-{original_host}{original_path_with_delimiters_replaced}
            // Example: delta-rs://file---path-to-some-table
            //   -> file:///path/to/some-table
            let delta_rs_host = url.host_str().unwrap_or("");
            let delta_rs_path = url.path();

            // Combine host and path to get the full encoded string
            let combined = format!("{}{}", delta_rs_host, delta_rs_path);

            // Find the first '-' to separate scheme from the rest
            if let Some(dash_pos) = combined.find('-') {
                let original_scheme = &combined[..dash_pos];
                let encoded_host_and_path = &combined[dash_pos + 1..];

                // The next part depends on whether we have a host or not
                // For file URLs, the host is encoded as "-", so we look for "--" pattern
                let (original_host, original_path) = if original_scheme == "file" {
                    // For file URLs, host is always empty and encoded as "-"
                    // So the pattern is: file---path-with-slashes-as-dashes
                    if encoded_host_and_path.starts_with('-') {
                        // Remove the leading "-" which represents the empty host
                        let encoded_path = &encoded_host_and_path[1..];
                        // Restore the original path by replacing "-" back to "/"
                        let original_path = encoded_path.replace('-', "/");
                        ("", original_path)
                    } else {
                        // This shouldn't happen for file URLs, but handle gracefully
                        ("", encoded_host_and_path.replace('-', "/"))
                    }
                } else {
                    // For other schemes (s3, etc.), we need to find where host ends and path begins
                    // This is more complex as we need to identify the host part
                    // For now, we'll use a simple heuristic: the first part before a longer sequence of dashes
                    // This is a simplification - in practice, we might need more sophisticated parsing
                    if let Some(path_start) = encoded_host_and_path.find("--") {
                        let host_part = &encoded_host_and_path[..path_start];
                        let path_part = &encoded_host_and_path[path_start + 1..]; // +1 to skip one dash, leaving one for the path
                        let original_host = if host_part == "-" { "" } else { host_part };
                        let original_path = path_part.replace('-', "/");
                        (original_host, original_path)
                    } else {
                        // Fallback: treat everything as path
                        ("", encoded_host_and_path.replace('-', "/"))
                    }
                };

                // Build the original URL
                let original_url_str = if original_scheme == "file" {
                    // For file URLs: file:///path
                    format!("{}://{}", original_scheme, original_path)
                } else if original_host.is_empty() {
                    // For schemes without host: scheme://path
                    format!("{}://{}", original_scheme, original_path)
                } else {
                    // For schemes with host: scheme://host/path
                    format!("{}://{}{}", original_scheme, original_host,
                           if original_path.starts_with('/') { original_path } else { format!("/{}", original_path) })
                };

                if let Ok(original_url) = Url::parse(&original_url_str) {
                    // Recursively call with the original URL
                    return get_dynamic_object_store(&original_url);
                }
            }

            // If parsing fails, return an error
            Err(object_store::Error::Generic {
                store: "delta-rs",
                source: Box::new(plan_datafusion_err!("failed to parse delta-rs URL: {url}")),
            })
        }
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
