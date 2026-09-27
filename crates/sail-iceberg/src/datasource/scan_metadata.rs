use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::datasource::listing::PartitionedFile;
use datafusion_common::{DataFusionError, Result, ScalarValue, Statistics, plan_datafusion_err};
use datafusion_proto::generated::datafusion_common as proto;
use prost::Message;

use crate::datasource::file_statistics::file_statistics;
use crate::datasource::partition_defaults::IdentityPartitionDefaults;
use crate::datasource::type_converter::iceberg_type_to_arrow;
use crate::spec::{DataFile, PartitionSpec};
use crate::utils::conversions::to_scalar;

/// File facts carried by the manifest stream, in the unprojected scan schema.
#[derive(Clone, Message)]
pub(crate) struct ScanFileMetadata {
    #[prost(message, optional, tag = "1")]
    statistics: Option<proto::Statistics>,
    #[prost(message, repeated, tag = "2")]
    partition_values: Vec<proto::ScalarValue>,
    #[prost(string, tag = "3")]
    identity_defaults: String,
}

impl ScanFileMetadata {
    pub(crate) fn encode_file(
        file: &DataFile,
        schema: &crate::spec::Schema,
        partition_schema: &crate::spec::Schema,
        file_schema: &Schema,
        specs: &[PartitionSpec],
    ) -> Result<Vec<u8>> {
        let spec = specs
            .iter()
            .find(|spec| spec.spec_id() == file.partition_spec_id)
            .ok_or_else(|| {
                plan_datafusion_err!("Unknown Iceberg partition spec {}", file.partition_spec_id)
            })?;
        let partition_type = spec
            .partition_type(partition_schema)
            .map_err(|error| plan_datafusion_err!("{error}"))?;
        if partition_type.fields().len() != file.partition.len() {
            return Err(plan_datafusion_err!(
                "Iceberg partition value count does not match its spec"
            ));
        }
        let partition_values = partition_type
            .fields()
            .iter()
            .zip(&file.partition)
            .map(|(field, value)| {
                let scalar = match value {
                    Some(value) => to_scalar(value, &field.field_type)?,
                    None => ScalarValue::try_new_null(&iceberg_type_to_arrow(&field.field_type)?)?,
                };
                proto::ScalarValue::try_from(&scalar)
                    .map_err(|error| DataFusionError::External(Box::new(error)))
            })
            .collect::<Result<Vec<_>>>()?;
        // Runtime file pruning needs exact facts. The outer scan still advertises
        // unknown statistics, so optimizer inference cannot turn them into constants.
        let statistics = file_statistics(schema, file_schema, specs, file);
        let defaults = IdentityPartitionDefaults::from_file(file, specs, schema)?;
        Ok(Self {
            statistics: Some((&statistics).into()),
            partition_values,
            identity_defaults: serde_json::to_string(&defaults)
                .map_err(|error| DataFusionError::External(Box::new(error)))?,
        }
        .encode_to_vec())
    }

    pub(crate) fn apply(&self, file: &mut PartitionedFile) -> Result<()> {
        file.statistics = self
            .statistics
            .as_ref()
            .map(Statistics::try_from)
            .transpose()?
            .map(Arc::new);
        file.partition_values = self
            .partition_values
            .iter()
            .map(ScalarValue::try_from)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let defaults: IdentityPartitionDefaults = serde_json::from_str(&self.identity_defaults)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        file.extensions.insert(defaults);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::stats::Precision;

    use super::*;
    use crate::datasource::type_converter::iceberg_schema_to_arrow;
    use crate::spec::{Datum, NestedField, PrimitiveLiteral, PrimitiveType, Type};

    #[test]
    fn metadata_round_trip_preserves_field_order_missing_metrics_and_partition_types() -> Result<()>
    {
        let schema = crate::spec::Schema::builder()
            .with_fields([
                Arc::new(NestedField::optional(
                    1,
                    "value",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "p",
                    Type::Primitive(PrimitiveType::Date),
                )),
                Arc::new(NestedField::optional(
                    3,
                    "score",
                    Type::Primitive(PrimitiveType::Double),
                )),
            ])
            .build()
            .map_err(|error| plan_datafusion_err!("{error}"))?;
        let spec: PartitionSpec = serde_json::from_value(serde_json::json!({"spec-id": 7, "fields": [{"source-id": 2, "field-id": 1000, "name": "p", "transform": "identity"}]})).map_err(|error| plan_datafusion_err!("{error}"))?;
        let mut file: DataFile = serde_json::from_value(serde_json::json!({
            "content": "DATA", "file_path": "missing.parquet", "file_format": "PARQUET",
            "partition": [null], "record_count": 2, "file_size_in_bytes": 100, "partition_spec_id": 7
        })).map_err(|error| plan_datafusion_err!("{error}"))?;
        file.lower_bounds.insert(
            1,
            Datum::new(PrimitiveType::Long, PrimitiveLiteral::Long(10)),
        );
        file.upper_bounds.insert(
            1,
            Datum::new(PrimitiveType::Long, PrimitiveLiteral::Long(20)),
        );
        file.lower_bounds.insert(
            3,
            Datum::new(PrimitiveType::Double, PrimitiveLiteral::Double(1.0.into())),
        );
        file.upper_bounds = file
            .upper_bounds
            .into_iter()
            .chain([(
                3,
                Datum::new(PrimitiveType::Double, PrimitiveLiteral::Double(2.0.into())),
            )])
            .collect();
        let arrow = iceberg_schema_to_arrow(&schema)?.project(&[2, 0, 1])?;
        let encoded = ScanFileMetadata::encode_file(&file, &schema, &schema, &arrow, &[spec])?;
        let decoded = ScanFileMetadata::decode(encoded.as_slice())
            .map_err(|error| plan_datafusion_err!("{error}"))?;
        let mut partitioned = PartitionedFile::new("missing.parquet", 100);
        decoded.apply(&mut partitioned)?;
        assert_eq!(
            partitioned.partition_values,
            vec![ScalarValue::Date32(None)]
        );
        assert!(
            partitioned
                .extensions
                .get::<IdentityPartitionDefaults>()
                .is_some()
        );
        let stats = partitioned
            .statistics
            .ok_or_else(|| plan_datafusion_err!("missing file statistics"))?;
        assert_eq!(stats.num_rows, Precision::Exact(2));
        assert_eq!(stats.column_statistics[0].min_value, Precision::Absent);
        assert_eq!(stats.column_statistics[0].max_value, Precision::Absent);
        assert_eq!(
            stats.column_statistics[1].min_value,
            Precision::Exact(ScalarValue::Int64(Some(10)))
        );
        assert_eq!(
            stats.column_statistics[1].max_value,
            Precision::Exact(ScalarValue::Int64(Some(20)))
        );
        assert_eq!(stats.column_statistics[1].null_count, Precision::Absent);
        assert_eq!(stats.column_statistics[2].null_count, Precision::Exact(2));
        Ok(())
    }
}
