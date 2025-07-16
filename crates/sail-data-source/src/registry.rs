use std::collections::HashMap;
use std::sync::Arc;

use datafusion_common::{DataFusionError, Result};
use sail_common_datafusion::datasource::TableFormat;

use crate::formats::arrow::ArrowTableFormat;
use crate::formats::avro::AvroTableFormat;
use crate::formats::csv::CsvTableFormat;
use crate::formats::delta::DeltaTableFormat;
use crate::formats::json::JsonTableFormat;
use crate::formats::parquet::ParquetTableFormat;

#[derive(Default)]
pub struct TableFormatRegistry {
    formats: HashMap<String, Arc<dyn TableFormat>>,
}

impl TableFormatRegistry {
    pub fn new() -> Self {
        let mut registry = Self::default();

        registry.register_format(Arc::new(ParquetTableFormat::default()));
        registry.register_format(Arc::new(CsvTableFormat::default()));
        registry.register_format(Arc::new(JsonTableFormat::default()));
        registry.register_format(Arc::new(DeltaTableFormat));
        registry.register_format(Arc::new(ArrowTableFormat::default()));
        registry.register_format(Arc::new(AvroTableFormat::default()));

        registry
    }

    pub fn register_format(&mut self, format: Arc<dyn TableFormat>) {
        self.formats.insert(format.name().to_lowercase(), format);
    }

    pub fn get_format(&self, name: &str) -> Result<Arc<dyn TableFormat>> {
        self.formats
            .get(&name.to_lowercase())
            .cloned()
            .ok_or_else(|| DataFusionError::Plan(format!("No table format found for: {name}")))
    }
}
