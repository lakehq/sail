pub mod types;

use std::path::Path;

use crate::error::BuildError;
use crate::openapi::types::OpenApi;

pub fn load_spec(path: impl AsRef<Path>) -> Result<OpenApi, BuildError> {
    let content = std::fs::read_to_string(path)?;
    Ok(serde_yaml::from_str(&content)?)
}
