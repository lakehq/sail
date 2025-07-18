use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::file_format::{FileFormat, FileFormatFactory};
use datafusion_common::{GetExt, Result};
use sail_common::spec::SaveMode;

#[derive(Debug, Clone)]
pub struct DeltaFormatFactory {
    mode: SaveMode,
    options: HashMap<String, String>,
    partition_columns: Vec<String>,
}

impl DeltaFormatFactory {
    pub fn new() -> Self {
        <Self as Default>::default()
    }

    pub fn with_mode(mut self, mode: SaveMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn with_options(mut self, options: HashMap<String, String>) -> Self {
        self.options = options;
        self
    }

    pub fn with_partition_columns(mut self, columns: Vec<String>) -> Self {
        self.partition_columns = columns;
        self
    }
}

impl Default for DeltaFormatFactory {
    fn default() -> Self {
        Self {
            mode: SaveMode::ErrorIfExists,
            options: HashMap::new(),
            partition_columns: Vec::new(),
        }
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
            self.mode.clone(),
            combined_options,
            self.partition_columns.clone(),
        )))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(super::format::DeltaFileFormat::default())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
