mod config;
mod registry;
mod s3;

pub use config::{load_aws_config, ObjectStoreConfig};
pub use registry::DynamicObjectStoreRegistry;
