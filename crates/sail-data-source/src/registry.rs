use std::collections::HashMap;
use std::sync::Arc;

use datafusion_common::{DataFusionError, Result};
use once_cell::sync::Lazy;
use sail_common_datafusion::datasource::TableFormat;

use crate::formats::arrow::ArrowTableFormat;
use crate::formats::avro::AvroTableFormat;
use crate::formats::binary::BinaryTableFormat;
use crate::formats::console::ConsoleTableFormat;
use crate::formats::csv::CsvTableFormat;
use crate::formats::delta::DeltaTableFormat;
use crate::formats::json::JsonTableFormat;
use crate::formats::parquet::ParquetTableFormat;
use crate::formats::rate::RateTableFormat;
use crate::formats::socket::SocketTableFormat;
use crate::formats::text::TextTableFormat;

static DEFAULT_REGISTRY: Lazy<Arc<TableFormatRegistry>> =
    Lazy::new(|| Arc::new(TableFormatRegistry::new()));

/// Returns the default, shared `TableFormatRegistry`.
pub fn default_registry() -> Arc<TableFormatRegistry> {
    DEFAULT_REGISTRY.clone()
}

#[derive(Default)]
pub struct TableFormatRegistry {
    formats: HashMap<String, Arc<dyn TableFormat>>,
}

impl TableFormatRegistry {
    /// Creates a new registry with all default formats.
    ///
    /// Note: In most cases, `default_registry()` should be used to get a shared
    /// instance.
    pub fn new() -> Self {
        let mut registry = Self::default();
        registry.register_format(Arc::new(ArrowTableFormat::default()));
        registry.register_format(Arc::new(AvroTableFormat::default()));
        registry.register_format(Arc::new(BinaryTableFormat::default()));
        registry.register_format(Arc::new(CsvTableFormat::default()));
        registry.register_format(Arc::new(DeltaTableFormat));
        registry.register_format(Arc::new(JsonTableFormat::default()));
        registry.register_format(Arc::new(ParquetTableFormat::default()));
        registry.register_format(Arc::new(TextTableFormat::default()));
        registry.register_format(Arc::new(SocketTableFormat));
        registry.register_format(Arc::new(RateTableFormat));
        registry.register_format(Arc::new(ConsoleTableFormat));

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
