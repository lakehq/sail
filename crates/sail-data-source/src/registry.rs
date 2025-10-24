use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use datafusion_common::{DataFusionError, Result};
use once_cell::sync::Lazy;
use sail_common_datafusion::datasource::TableFormat;

use crate::formats::arrow::ArrowTableFormat;
use crate::formats::avro::AvroTableFormat;
use crate::formats::binary::BinaryTableFormat;
use crate::formats::console::ConsoleTableFormat;
use crate::formats::csv::CsvTableFormat;
use crate::formats::delta::DeltaTableFormat;
use crate::formats::iceberg::IcebergDataSourceFormat;
use crate::formats::json::JsonTableFormat;
use crate::formats::parquet::ParquetTableFormat;
use crate::formats::python::PythonDataSourceFormat;
use crate::formats::rate::RateTableFormat;
use crate::formats::socket::SocketTableFormat;
use crate::formats::text::TextTableFormat;

static DEFAULT_REGISTRY: Lazy<Arc<TableFormatRegistry>> =
    Lazy::new(|| Arc::new(TableFormatRegistry::new()));

/// Returns the default, shared `TableFormatRegistry`.
pub fn default_registry() -> Arc<TableFormatRegistry> {
    DEFAULT_REGISTRY.clone()
}

pub struct TableFormatRegistry {
    formats: RwLock<HashMap<String, Arc<dyn TableFormat>>>,
}

impl Default for TableFormatRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl TableFormatRegistry {
    /// Creates a new registry with all default formats.
    ///
    /// Note: In most cases, `default_registry()` should be used to get a shared
    /// instance.
    pub fn new() -> Self {
        let registry = Self {
            formats: RwLock::new(HashMap::new()),
        };
        registry.register_format(Arc::new(ArrowTableFormat::default()));
        registry.register_format(Arc::new(AvroTableFormat::default()));
        registry.register_format(Arc::new(BinaryTableFormat::default()));
        registry.register_format(Arc::new(CsvTableFormat::default()));
        registry.register_format(Arc::new(DeltaTableFormat));
        registry.register_format(Arc::new(IcebergDataSourceFormat::default()));
        registry.register_format(Arc::new(JsonTableFormat::default()));
        registry.register_format(Arc::new(ParquetTableFormat::default()));
        registry.register_format(Arc::new(PythonDataSourceFormat::default()));
        registry.register_format(Arc::new(TextTableFormat::default()));
        registry.register_format(Arc::new(SocketTableFormat));
        registry.register_format(Arc::new(RateTableFormat));
        registry.register_format(Arc::new(ConsoleTableFormat));

        // Register JDBC as a pre-configured Python data source
        registry.register_format(Arc::new(PythonDataSourceFormat::with_name_and_defaults(
            "jdbc",
            "pysail.jdbc.datasource",
            "JDBCArrowDataSource",
        )));

        registry
    }

    pub fn register_format(&self, format: Arc<dyn TableFormat>) {
        let mut guard = match self.formats.write() {
            Ok(lock) => lock,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.insert(format.name().to_lowercase(), format);
    }

    pub fn get_format(&self, name: &str) -> Result<Arc<dyn TableFormat>> {
        let guard = match self.formats.read() {
            Ok(lock) => lock,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard
            .get(&name.to_lowercase())
            .cloned()
            .ok_or_else(|| DataFusionError::Plan(format!("No table format found for: {name}")))
    }
}
