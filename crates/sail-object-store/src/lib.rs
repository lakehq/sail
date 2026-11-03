mod hugging_face;
mod layers;
mod path;
mod registry;
mod s3;

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::execution::object_store::ObjectStoreRegistry;
use datafusion_common::{Result, plan_err};
pub use path::{
    ResolvedObjectStorePath, delete_object_store_prefix_objects, resolve_object_store_path,
};
pub use registry::DynamicObjectStoreRegistry;
use url::Url;

/// Serve the storage location `prefix` with the credentials a catalog vended for
/// it, for every later request of the session that owns `registry`.
pub fn register_vended_storage_credential(
    registry: &dyn ObjectStoreRegistry,
    prefix: &str,
    config: &BTreeMap<String, String>,
) -> Result<()> {
    let url = Url::parse(prefix).or_else(|e| plan_err!("invalid storage prefix {prefix}: {e}"))?;
    match url.scheme() {
        "s3" | "s3a" => {
            let store = s3::get_vended_s3_object_store(&url, config)?;
            registry.register_store(&url, Arc::new(store));
            Ok(())
        }
        scheme => plan_err!("vended storage credentials for {scheme} are not supported"),
    }
}
