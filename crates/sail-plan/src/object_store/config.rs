use aws_config::{BehaviorVersion, SdkConfig};

#[derive(Debug, Clone)]
pub struct ObjectStoreConfig {
    aws: Option<SdkConfig>,
}

impl ObjectStoreConfig {
    pub fn new() -> Self {
        Self { aws: None }
    }

    pub fn with_aws(mut self, aws: SdkConfig) -> Self {
        self.aws = Some(aws);
        self
    }

    pub fn aws(&self) -> Option<&SdkConfig> {
        self.aws.as_ref()
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
