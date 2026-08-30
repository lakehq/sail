use std::sync::Arc;

use datafusion::common::Result;
use sail_common_datafusion::datasource::DataSourceRegistry;
use sail_common_datafusion::lakeformat::{
    LakeFormatPlugin, LakeFormatRegistry, LakeFormatRegistryBuilder,
};
use sail_data_source::formats::arrow::ArrowDataSource;
use sail_data_source::formats::avro::AvroDataSource;
use sail_data_source::formats::binary::BinaryDataSource;
use sail_data_source::formats::console::ConsoleDataSource;
use sail_data_source::formats::csv::CsvDataSource;
use sail_data_source::formats::json::JsonDataSource;
use sail_data_source::formats::noop::NoopDataSource;
use sail_data_source::formats::parquet::ParquetDataSource;
use sail_data_source::formats::python::{PythonDataSourceAdapter, discover_data_sources};
use sail_data_source::formats::rate::RateDataSource;
use sail_data_source::formats::socket::SocketDataSource;
use sail_data_source::formats::text::TextDataSource;
use sail_delta_lake::DeltaLakeSource;
use sail_iceberg::IcebergLakeSource;

pub(crate) fn create_data_source_registry() -> Result<Arc<DataSourceRegistry>> {
    let registry = Arc::new(DataSourceRegistry::new());
    register_builtin_data_sources(&registry)?;
    register_external_data_sources(&registry)?;
    Ok(registry)
}

pub(crate) fn create_lake_format_registry() -> Result<Arc<LakeFormatRegistry>> {
    let mut builder = LakeFormatRegistryBuilder::new();
    let delta = Arc::new(DeltaLakeSource);
    builder.register(LakeFormatPlugin::try_new(delta)?)?;
    let iceberg = Arc::new(IcebergLakeSource);
    builder.register(
        LakeFormatPlugin::try_new(iceberg.clone())?
            .with_relation_provider(iceberg.clone())
            .with_procedure_provider(iceberg),
    )?;
    Ok(Arc::new(builder.build()))
}

fn register_builtin_data_sources(registry: &DataSourceRegistry) -> Result<()> {
    registry.register_data_source(Arc::new(ArrowDataSource::default()))?;
    registry.register_data_source(Arc::new(AvroDataSource::default()))?;
    registry.register_data_source(Arc::new(BinaryDataSource::default()))?;
    registry.register_data_source(Arc::new(CsvDataSource::default()))?;
    registry.register_data_source(Arc::new(JsonDataSource::default()))?;
    registry.register_data_source(Arc::new(ParquetDataSource::default()))?;
    registry.register_data_source(Arc::new(TextDataSource::default()))?;
    registry.register_data_source(Arc::new(SocketDataSource))?;
    registry.register_data_source(Arc::new(RateDataSource))?;
    registry.register_data_source(Arc::new(ConsoleDataSource))?;
    registry.register_data_source(Arc::new(NoopDataSource))?;
    registry.register_protected_data_source(Arc::new(DeltaLakeSource))?;
    registry.register_protected_data_source(Arc::new(IcebergLakeSource))?;
    Ok(())
}

fn register_external_data_sources(registry: &DataSourceRegistry) -> Result<()> {
    // Register Python data sources
    {
        discover_data_sources()?;
        PythonDataSourceAdapter::register_all(registry)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn separates_data_source_front_doors_from_lake_format_plugins() -> Result<()> {
        let registry = DataSourceRegistry::new();
        register_builtin_data_sources(&registry)?;
        let formats = create_lake_format_registry()?;

        for name in ["parquet", "rate", "console"] {
            assert!(registry.get_data_source(name).is_ok());
            assert!(!formats.contains(name));
        }
        for name in ["delta", "iceberg"] {
            assert!(registry.get_data_source(name).is_ok());
            assert!(registry.is_protected(name)?);
            assert!(formats.contains(name));
        }
        Ok(())
    }
}
