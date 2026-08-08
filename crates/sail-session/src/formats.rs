use std::sync::Arc;

use datafusion::common::Result;
use sail_common_datafusion::datasource::TableFormatRegistry;
use sail_data_source::formats::arrow::ArrowTableFormat;
use sail_data_source::formats::avro::AvroTableFormat;
use sail_data_source::formats::binary::BinaryTableFormat;
use sail_data_source::formats::console::ConsoleTableFormat;
use sail_data_source::formats::csv::CsvTableFormat;
use sail_data_source::formats::json::JsonTableFormat;
use sail_data_source::formats::noop::NoopTableFormat;
use sail_data_source::formats::parquet::ParquetTableFormat;
use sail_data_source::formats::python::{PythonTableFormat, discover_data_sources};
use sail_data_source::formats::rate::RateTableFormat;
use sail_data_source::formats::socket::SocketTableFormat;
use sail_data_source::formats::text::TextTableFormat;
use sail_delta_lake::DeltaTableFormat;
use sail_iceberg::IcebergTableFormat;

pub fn create_table_format_registry(
    prewarm_file_statistics_on_source_creation: bool,
) -> Result<Arc<TableFormatRegistry>> {
    let registry = Arc::new(TableFormatRegistry::new());
    register_builtin_formats(&registry, prewarm_file_statistics_on_source_creation)?;
    register_external_formats(&registry)?;
    Ok(registry)
}

fn register_builtin_formats(
    registry: &Arc<TableFormatRegistry>,
    prewarm_file_statistics_on_source_creation: bool,
) -> Result<()> {
    registry.register(Arc::new(ArrowTableFormat::new(
        prewarm_file_statistics_on_source_creation,
    )))?;
    registry.register(Arc::new(AvroTableFormat::new(
        prewarm_file_statistics_on_source_creation,
    )))?;
    registry.register(Arc::new(BinaryTableFormat::new(
        prewarm_file_statistics_on_source_creation,
    )))?;
    registry.register(Arc::new(CsvTableFormat::new(
        prewarm_file_statistics_on_source_creation,
    )))?;
    registry.register(Arc::new(JsonTableFormat::new(
        prewarm_file_statistics_on_source_creation,
    )))?;
    registry.register(Arc::new(ParquetTableFormat::new(
        prewarm_file_statistics_on_source_creation,
    )))?;
    registry.register(Arc::new(TextTableFormat::new(
        prewarm_file_statistics_on_source_creation,
    )))?;
    registry.register(Arc::new(SocketTableFormat))?;
    registry.register(Arc::new(RateTableFormat))?;
    registry.register(Arc::new(ConsoleTableFormat))?;
    registry.register(Arc::new(NoopTableFormat))?;
    Ok(())
}

fn register_external_formats(registry: &Arc<TableFormatRegistry>) -> Result<()> {
    DeltaTableFormat::register(registry)?;
    IcebergTableFormat::register(registry)?;

    // Register Python data sources
    {
        discover_data_sources()?;
        PythonTableFormat::register_all(registry)?;
    }

    Ok(())
}
