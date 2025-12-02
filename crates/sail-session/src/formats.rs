use std::sync::Arc;

use datafusion::common::Result;
use sail_common_datafusion::datasource::TableFormatRegistry;
use sail_data_source::formats::arrow::ArrowTableFormat;
use sail_data_source::formats::avro::AvroTableFormat;
use sail_data_source::formats::binary::BinaryTableFormat;
use sail_data_source::formats::console::ConsoleTableFormat;
use sail_data_source::formats::csv::CsvTableFormat;
use sail_data_source::formats::json::JsonTableFormat;
use sail_data_source::formats::parquet::ParquetTableFormat;
use sail_data_source::formats::rate::RateTableFormat;
use sail_data_source::formats::socket::SocketTableFormat;
use sail_data_source::formats::text::TextTableFormat;
use sail_delta_lake::DeltaTableFormat;
use sail_iceberg::IcebergTableFormat;

pub fn create_table_format_registry() -> Result<Arc<TableFormatRegistry>> {
    let registry = Arc::new(TableFormatRegistry::new());
    register_builtin_formats(&registry)?;
    register_external_formats(&registry)?;
    Ok(registry)
}

fn register_builtin_formats(registry: &Arc<TableFormatRegistry>) -> Result<()> {
    registry.register(Arc::new(ArrowTableFormat::default()))?;
    registry.register(Arc::new(AvroTableFormat::default()))?;
    registry.register(Arc::new(BinaryTableFormat::default()))?;
    registry.register(Arc::new(CsvTableFormat::default()))?;
    registry.register(Arc::new(JsonTableFormat::default()))?;
    registry.register(Arc::new(ParquetTableFormat::default()))?;
    registry.register(Arc::new(TextTableFormat::default()))?;
    registry.register(Arc::new(SocketTableFormat))?;
    registry.register(Arc::new(RateTableFormat))?;
    registry.register(Arc::new(ConsoleTableFormat))?;
    Ok(())
}

fn register_external_formats(registry: &Arc<TableFormatRegistry>) -> Result<()> {
    DeltaTableFormat::register(registry)?;
    IcebergTableFormat::register(registry)?;
    Ok(())
}
