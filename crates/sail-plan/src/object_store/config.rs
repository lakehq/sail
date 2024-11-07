use std::collections::HashMap;

use aws_config::{BehaviorVersion, SdkConfig};
use tokio::sync::OnceCell;

#[derive(Debug, Clone)]
pub struct ObjectStoreConfig {
    pub aws: SdkConfig,
    /// Key-value pairs corresponding to a field in {core-site,hdfs-site}.xml hadoop configuration files.
    pub hdfs: HashMap<String, String>,
}

pub(super) static OBJECT_STORE_CONFIG: OnceCell<ObjectStoreConfig> = OnceCell::const_new();

impl ObjectStoreConfig {
    /// Initialize the global object store configuration asynchronously.
    /// This is a no-op if the configuration has already been loaded.
    pub async fn initialize() {
        OBJECT_STORE_CONFIG
            .get_or_init(|| async {
                Self {
                    aws: aws_config::defaults(BehaviorVersion::latest()).load().await,
                    hdfs: HashMap::new(),
                }
            })
            .await;
    }
}
