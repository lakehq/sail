
use std::collections::HashMap;
use std::sync::Arc;

use datafusion_common::{DataFusionError, Result};

use sail_common_datafusion::datasource::{TableFormat, TableWriter};

use crate::formats::csv::CsvTableFormat;
use crate::formats::delta::DeltaTableFormat;
use crate::formats::json::JsonTableFormat;
use crate::formats::parquet::ParquetTableFormat;

#[derive(Default)]
pub struct TableFormatRegistry {
    formats: HashMap<String, Arc<dyn TableFormat>>,
    writers: HashMap<String, Arc<dyn TableWriter>>,
}

impl TableFormatRegistry {
    pub fn new() -> Self {
        let mut registry = Self::default();

        let parquet = Arc::new(ParquetTableFormat::default());
        registry.register_format(parquet.clone());
        registry.register_writer(parquet);

        let csv = Arc::new(CsvTableFormat::default());
        registry.register_format(csv.clone());
        registry.register_writer(csv);

        let json = Arc::new(JsonTableFormat::default());
        registry.register_format(json.clone());
        registry.register_writer(json);

        let delta = Arc::new(DeltaTableFormat::default());
        registry.register_format(delta.clone());
        registry.register_writer(delta);

        registry
    }

    pub fn register_format(&mut self, format: Arc<dyn TableFormat>) {
        self.formats.insert(format.name().to_lowercase(), format);
    }

    pub fn register_writer(&mut self, writer: Arc<dyn TableWriter>) {
        self.writers.insert(writer.name().to_lowercase(), writer);
    }

    pub fn get_format(&self, name: &str) -> Result<Arc<dyn TableFormat>> {
        self.formats.get(&name.to_lowercase()).cloned().ok_or_else(|| {
            DataFusionError::Plan(format!("No table format found for: {}", name))
        })
    }

    pub fn get_writer(&self, name: &str) -> Result<Arc<dyn TableWriter>> {
        self.writers.get(&name.to_lowercase()).cloned().ok_or_else(|| {
            DataFusionError::Plan(format!("No table writer found for: {}", name))
        })
    }
}
