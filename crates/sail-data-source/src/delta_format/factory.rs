use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::file_format::{FileFormat, FileFormatFactory};
use datafusion_common::{GetExt, Result};

/// Factory for creating Delta Lake file format instances
#[derive(Debug, Default)]
pub struct DeltaFormatFactory {
    options: HashMap<String, String>,
}

impl DeltaFormatFactory {
    /// Create a new DeltaFormatFactory with default options
    pub fn new() -> Self {
        Self {
            options: HashMap::new(),
        }
    }

    /// Create a new DeltaFormatFactory with specified default options
    pub fn new_with_options(options: HashMap<String, String>) -> Self {
        Self { options }
    }
}

impl GetExt for DeltaFormatFactory {
    fn get_ext(&self) -> String {
        "delta".to_string()
    }
}

#[async_trait]
impl FileFormatFactory for DeltaFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        format_options: &HashMap<String, String>,
    ) -> Result<Arc<dyn FileFormat>> {
        let mut combined_options = self.options.clone();
        combined_options.extend(format_options.clone());

        Ok(Arc::new(super::format::DeltaFileFormat::new(
            combined_options,
        )))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(super::format::DeltaFileFormat::default())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
