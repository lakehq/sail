use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::file_format::{FileFormat, FileFormatFactory};
use datafusion_common::{GetExt, Result};

#[derive(Debug, Default)]
pub struct DeltaFormatFactory {
    options: HashMap<String, String>,
}

impl DeltaFormatFactory {
    pub fn new() -> Self {
        Self {
            options: HashMap::new(),
        }
    }

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
