use std::sync::Arc;

use datafusion::common::Result;
use sail_common_datafusion::data_source_format::DataSourceFormatRegistry;
use sail_common_datafusion::table_format::TableFormatRegistry;
use sail_data_source::formats::arrow::ArrowDataSourceFormat;
use sail_data_source::formats::avro::AvroDataSourceFormat;
use sail_data_source::formats::binary::BinaryDataSourceFormat;
use sail_data_source::formats::console::ConsoleDataSourceFormat;
use sail_data_source::formats::csv::CsvDataSourceFormat;
use sail_data_source::formats::json::JsonDataSourceFormat;
use sail_data_source::formats::noop::NoopDataSourceFormat;
use sail_data_source::formats::parquet::ParquetDataSourceFormat;
use sail_data_source::formats::python::{PythonDataSourceFormat, discover_data_sources};
use sail_data_source::formats::rate::RateDataSourceFormat;
use sail_data_source::formats::socket::SocketDataSourceFormat;
use sail_data_source::formats::text::TextDataSourceFormat;
use sail_delta_lake::DeltaTableFormat;
use sail_iceberg::IcebergTableFormat;

pub fn create_format_registries()
-> Result<(Arc<DataSourceFormatRegistry>, Arc<TableFormatRegistry>)> {
    let data_source_formats = Arc::new(DataSourceFormatRegistry::new());
    let table_formats = Arc::new(TableFormatRegistry::new());
    register_builtin_formats(&data_source_formats)?;
    register_external_formats(&data_source_formats, &table_formats)?;
    Ok((data_source_formats, table_formats))
}

fn register_builtin_formats(registry: &DataSourceFormatRegistry) -> Result<()> {
    registry.register(Arc::new(ArrowDataSourceFormat::default()))?;
    registry.register(Arc::new(AvroDataSourceFormat::default()))?;
    registry.register(Arc::new(BinaryDataSourceFormat::default()))?;
    registry.register(Arc::new(CsvDataSourceFormat::default()))?;
    registry.register(Arc::new(JsonDataSourceFormat::default()))?;
    registry.register(Arc::new(ParquetDataSourceFormat::default()))?;
    registry.register(Arc::new(TextDataSourceFormat::default()))?;
    registry.register(Arc::new(SocketDataSourceFormat))?;
    registry.register(Arc::new(RateDataSourceFormat))?;
    registry.register(Arc::new(ConsoleDataSourceFormat))?;
    registry.register(Arc::new(NoopDataSourceFormat))?;
    Ok(())
}

fn register_external_formats(
    data_source_formats: &DataSourceFormatRegistry,
    table_formats: &TableFormatRegistry,
) -> Result<()> {
    DeltaTableFormat::register(data_source_formats, table_formats)?;
    IcebergTableFormat::register(data_source_formats, table_formats)?;

    // Register Python data sources
    {
        discover_data_sources()?;
        PythonDataSourceFormat::register_all(data_source_formats)?;
    }

    Ok(())
}
