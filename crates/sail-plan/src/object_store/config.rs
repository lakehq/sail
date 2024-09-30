use std::collections::HashMap;

use aws_config::{BehaviorVersion, SdkConfig};

#[derive(Debug, Clone)]
pub struct ObjectStoreConfig {
    aws: Option<SdkConfig>,
    /// key,value should correspond to a field in {core-site,hdfs-site}.xml hadoop configuration files
    hdfs: Option<HashMap<String, String>>,
}

impl ObjectStoreConfig {
    pub fn new() -> Self {
        Self {
            aws: None,
            hdfs: None,
        }
    }

    pub fn with_aws(mut self, aws: SdkConfig) -> Self {
        self.aws = Some(aws);
        self
    }

    pub fn with_hdfs(mut self, hdfs: HashMap<String, String>) -> Self {
        self.hdfs = Some(hdfs);
        self
    }

    pub fn aws(&self) -> Option<&SdkConfig> {
        self.aws.as_ref()
    }

    pub fn hdfs(&self) -> Option<&HashMap<String, String>> {
        self.hdfs.as_ref()
    }
}

impl Default for ObjectStoreConfig {
    fn default() -> Self {
        Self::new()
    }
}

pub async fn load_aws_config() -> SdkConfig {
    aws_config::defaults(BehaviorVersion::latest()).load().await
}
