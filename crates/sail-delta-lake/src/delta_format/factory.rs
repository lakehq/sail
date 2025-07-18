use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::file_format::{FileFormat, FileFormatFactory};
use datafusion_common::{GetExt, Result};
use sail_common::spec::SaveMode;

#[derive(Debug, Default)]
pub struct DeltaFormatFactory {
    mode: SaveMode,
    options: HashMap<String, String>,
    partition_columns: Vec<String>,
}

impl DeltaFormatFactory {
    pub fn new() -> Self {
        Self {
            mode: SaveMode::ErrorIfExists, // Default save mode
            options: HashMap::new(),
            partition_columns: Vec::new(),
        }
    }

    pub fn new_with_options(mode: SaveMode, options: HashMap<String, String>) -> Self {
        Self { mode, options, partition_columns: Vec::new() }
    }

    pub fn new_with_partitioning(
        mode: SaveMode,
        options: HashMap<String, String>,
        partition_columns: Vec<String>,
    ) -> Self {
        Self { mode, options, partition_columns }
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
