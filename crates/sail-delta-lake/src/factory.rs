use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProviderFactory};
use datafusion::datasource::TableProvider;
use datafusion_common::Result as DataFusionResult;
use datafusion_expr::CreateExternalTable;

use crate::provider::DeltaTableProvider;

/// Factory for creating Delta Lake table providers
#[derive(Debug)]
pub struct DeltaTableFactory;

impl DeltaTableFactory {
    pub fn new() -> Self {
        Self
    }
}

impl Default for DeltaTableFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TableProviderFactory for DeltaTableFactory {
    async fn create(
        &self,
        _state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> DataFusionResult<Arc<dyn TableProvider>> {
        let location = &cmd.location;

        // Check if this is a Delta table creation or reading
        match deltalake::open_table(location).await {
            Ok(delta_table) => {
                // Table exists, create provider for reading
                let provider = DeltaTableProvider::new(delta_table)
                    .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;
                Ok(Arc::new(provider))
            }
            Err(_) => {
                // Table doesn't exist, create a new one
                self.create_delta_table(location, cmd).await
            }
        }
    }
}

impl DeltaTableFactory {
    async fn create_delta_table(
        &self,
        location: &str,
        cmd: &CreateExternalTable,
    ) -> DataFusionResult<Arc<dyn TableProvider>> {
        use deltalake::kernel::{DataType as DeltaDataType, PrimitiveType, StructField};
        use deltalake::protocol::SaveMode;
        use deltalake::DeltaOps;

        // Convert DataFusion schema to Delta Lake schema
        let delta_fields: Result<Vec<StructField>, _> = cmd
            .schema
            .fields()
            .iter()
            .map(|field| {
                let delta_type = match field.data_type() {
                    datafusion::arrow::datatypes::DataType::Boolean => {
                        DeltaDataType::Primitive(PrimitiveType::Boolean)
                    }
                    datafusion::arrow::datatypes::DataType::Int8 => {
                        DeltaDataType::Primitive(PrimitiveType::Byte)
                    }
                    datafusion::arrow::datatypes::DataType::Int16 => {
                        DeltaDataType::Primitive(PrimitiveType::Short)
                    }
                    datafusion::arrow::datatypes::DataType::Int32 => {
                        DeltaDataType::Primitive(PrimitiveType::Integer)
                    }
                    datafusion::arrow::datatypes::DataType::Int64 => {
                        DeltaDataType::Primitive(PrimitiveType::Long)
                    }
                    datafusion::arrow::datatypes::DataType::Float32 => {
                        DeltaDataType::Primitive(PrimitiveType::Float)
                    }
                    datafusion::arrow::datatypes::DataType::Float64 => {
                        DeltaDataType::Primitive(PrimitiveType::Double)
                    }
                    datafusion::arrow::datatypes::DataType::Utf8 => {
                        DeltaDataType::Primitive(PrimitiveType::String)
                    }
                    datafusion::arrow::datatypes::DataType::LargeUtf8 => {
                        DeltaDataType::Primitive(PrimitiveType::String)
                    }
                    datafusion::arrow::datatypes::DataType::Binary => {
                        DeltaDataType::Primitive(PrimitiveType::Binary)
                    }
                    datafusion::arrow::datatypes::DataType::LargeBinary => {
                        DeltaDataType::Primitive(PrimitiveType::Binary)
                    }
                    datafusion::arrow::datatypes::DataType::Date32 => {
                        DeltaDataType::Primitive(PrimitiveType::Date)
                    }
                    datafusion::arrow::datatypes::DataType::Timestamp(_, _) => {
                        DeltaDataType::Primitive(PrimitiveType::Timestamp)
                    }
                    _ => {
                        return Err(format!(
                            "Unsupported data type for Delta Lake: {:?}",
                            field.data_type()
                        ));
                    }
                };

                Ok(StructField::new(
                    field.name().clone(),
                    delta_type,
                    field.is_nullable(),
                ))
            })
            .collect();

        let delta_fields = delta_fields.map_err(|e| {
            datafusion_common::DataFusionError::Plan(format!("Schema conversion error: {}", e))
        })?;

        // Create Delta table
        let delta_ops = DeltaOps::try_from_uri(location)
            .await
            .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;

        let mut create_builder = delta_ops
            .create()
            .with_columns(delta_fields.into_iter())
            .with_save_mode(if cmd.if_not_exists {
                SaveMode::Ignore
            } else {
                SaveMode::ErrorIfExists
            });

        // Add partition columns if specified
        if !cmd.table_partition_cols.is_empty() {
            create_builder =
                create_builder.with_partition_columns(cmd.table_partition_cols.clone());
        }

        // Add table properties from options
        let mut configuration = HashMap::new();
        for (key, value) in &cmd.options {
            if key.starts_with("delta.") {
                configuration.insert(key.clone(), Some(value.clone()));
            }
        }

        if !configuration.is_empty() {
            create_builder = create_builder.with_configuration(configuration);
        }

        // Execute table creation
        let delta_table = create_builder
            .await
            .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;

        // Create provider for the new table
        let provider = DeltaTableProvider::new(delta_table)
            .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;

        Ok(Arc::new(provider))
    }
}
