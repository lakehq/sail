use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion_common::{Result, plan_err};
use serde::{Deserialize, Serialize};

use crate::spec::delete_index::{DeleteFileRef, PositionDeleteFile};
use crate::spec::{DataFile, Literal, PrimitiveLiteral};

pub(crate) const MERGE_PARTITION_SPEC_ID_COLUMN: &str = "__sail_iceberg_partition_spec_id";
pub(crate) const MERGE_FILE_METADATA_COLUMN: &str = "__sail_iceberg_file_metadata";

/// Metadata selected from the scan's pinned snapshot, carried only with its rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RowLevelFileMetadata {
    #[serde(with = "partition_serde")]
    pub partition: Vec<Option<Literal>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletion_vector: Option<DeletionVectorTarget>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DeletionVectorTarget {
    pub snapshot_id: i64,
    pub record_count: u64,
    pub positional_deletes: Vec<PositionDeleteFile>,
}

impl RowLevelFileMetadata {
    pub fn encode(
        file: &DataFile,
        deletion_vector_snapshot: Option<i64>,
        positional_deletes: &[DeleteFileRef],
    ) -> Result<String> {
        let deletion_vector = deletion_vector_snapshot
            .map(|snapshot_id| {
                Ok::<_, datafusion_common::DataFusionError>(DeletionVectorTarget {
                    snapshot_id,
                    record_count: file.record_count,
                    positional_deletes: positional_deletes
                        .iter()
                        .map(|delete| PositionDeleteFile::try_from(&delete.data_file))
                        .collect::<Result<_>>()?,
                })
            })
            .transpose()?;
        serde_json::to_string(&Self {
            partition: file.partition.clone(),
            deletion_vector,
        })
        .map_err(|error| datafusion_common::DataFusionError::External(Box::new(error)))
    }
}

mod partition_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::{Literal, PrimitiveLiteral};

    #[derive(Serialize, Deserialize)]
    struct Value(#[serde(with = "crate::utils::literal_serde")] PrimitiveLiteral);

    pub fn serialize<S: Serializer>(
        partition: &[Option<Literal>],
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        partition
            .iter()
            .map(|value| match value {
                None | Some(Literal::Null) => Ok(None),
                Some(Literal::Primitive(value)) => Ok(Some(Value(value.clone()))),
                Some(_) => Err(serde::ser::Error::custom("non-primitive partition value")),
            })
            .collect::<std::result::Result<Vec<_>, S::Error>>()?
            .serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Vec<Option<Literal>>, D::Error> {
        Ok(Vec::<Option<Value>>::deserialize(deserializer)?
            .into_iter()
            .map(|value| value.map(|value| Literal::Primitive(value.0)))
            .collect())
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RowLevelMetadataColumns<'a> {
    file_column_name: Option<&'a str>,
    row_index_column_name: Option<&'a str>,
    include_delete_file_metadata: bool,
}

impl<'a> RowLevelMetadataColumns<'a> {
    pub(crate) fn new(
        file_column_name: Option<&'a str>,
        row_index_column_name: Option<&'a str>,
    ) -> Self {
        Self {
            file_column_name,
            row_index_column_name,
            include_delete_file_metadata: false,
        }
    }

    pub(crate) fn with_delete_file_metadata(mut self) -> Self {
        self.include_delete_file_metadata = true;
        self
    }

    pub(crate) fn append_to_schema(&self, data_schema: &ArrowSchema) -> Result<ArrowSchema> {
        self.validate_no_collisions(data_schema)?;
        let mut fields = data_schema.fields().iter().cloned().collect::<Vec<_>>();
        if let Some(name) = self.file_column_name {
            fields.push(Arc::new(Field::new(name, DataType::Utf8, true)));
        }
        if self.include_delete_file_metadata {
            fields.push(Arc::new(Field::new(
                MERGE_PARTITION_SPEC_ID_COLUMN,
                DataType::Int32,
                false,
            )));
            fields.push(Arc::new(Field::new(
                MERGE_FILE_METADATA_COLUMN,
                DataType::Utf8,
                false,
            )));
        }
        if let Some(name) = self.row_index_column_name {
            fields.push(Arc::new(Field::new(name, DataType::Int64, true)));
        }
        Ok(ArrowSchema::new_with_metadata(
            fields,
            data_schema.metadata().clone(),
        ))
    }

    fn validate_no_collisions(&self, data_schema: &ArrowSchema) -> Result<()> {
        let delete_file_metadata_columns = self
            .include_delete_file_metadata
            .then_some([MERGE_PARTITION_SPEC_ID_COLUMN, MERGE_FILE_METADATA_COLUMN]);
        for name in [self.file_column_name, self.row_index_column_name]
            .into_iter()
            .flatten()
            .chain(delete_file_metadata_columns.into_iter().flatten())
        {
            if data_schema.field_with_name(name).is_ok() {
                return plan_err!(
                    "Iceberg row-level metadata column '{name}' conflicts with an existing table column"
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn partition_transport_preserves_numeric_types_and_bits() -> Result<()> {
        let values = [
            PrimitiveLiteral::Boolean(true),
            PrimitiveLiteral::Int(7),
            PrimitiveLiteral::Long(7),
            PrimitiveLiteral::Float(0.1f32.into()),
            PrimitiveLiteral::Double(0.1f64.into()),
            PrimitiveLiteral::Double((-0.0f64).into()),
            PrimitiveLiteral::Double(f64::NAN.into()),
            PrimitiveLiteral::Float(f32::INFINITY.into()),
            PrimitiveLiteral::Int128(i128::MAX),
            PrimitiveLiteral::UInt128(u128::MAX),
            PrimitiveLiteral::String("7".to_string()),
            PrimitiveLiteral::Binary(vec![0, 255]),
        ];
        let mut partition = values
            .into_iter()
            .map(|value| Some(Literal::Primitive(value)))
            .collect::<Vec<_>>();
        partition.push(None);
        let expected = RowLevelFileMetadata {
            partition,
            deletion_vector: None,
        };
        let encoded = serde_json::to_string(&expected)
            .map_err(|error| datafusion_common::DataFusionError::External(Box::new(error)))?;
        let decoded: RowLevelFileMetadata = serde_json::from_str(&encoded)
            .map_err(|error| datafusion_common::DataFusionError::External(Box::new(error)))?;
        assert_eq!(decoded, expected);
        assert_eq!(
            serde_json::to_string(&decoded)
                .map_err(|error| datafusion_common::DataFusionError::External(Box::new(error)))?,
            encoded
        );
        Ok(())
    }

    #[test]
    fn appends_metadata_columns_and_preserves_schema_metadata() -> Result<()> {
        let schema = ArrowSchema::new_with_metadata(
            vec![Arc::new(Field::new("id", DataType::Int64, false))],
            HashMap::from([("owner".to_string(), "iceberg".to_string())]),
        );

        let actual =
            RowLevelMetadataColumns::new(Some("__sail_file_path"), Some("__sail_file_row_index"))
                .append_to_schema(&schema)?;

        assert_eq!(
            actual.metadata().get("owner").map(String::as_str),
            Some("iceberg")
        );
        assert_eq!(actual.fields().len(), 3);
        assert_eq!(actual.field(1).name(), "__sail_file_path");
        assert_eq!(actual.field(1).data_type(), &DataType::Utf8);
        assert_eq!(actual.field(2).name(), "__sail_file_row_index");
        assert_eq!(actual.field(2).data_type(), &DataType::Int64);
        Ok(())
    }

    #[test]
    fn rejects_existing_metadata_column_names() -> Result<()> {
        let schema = ArrowSchema::new(vec![Arc::new(Field::new(
            "__sail_file_path",
            DataType::Utf8,
            true,
        ))]);

        let err = match RowLevelMetadataColumns::new(Some("__sail_file_path"), None)
            .append_to_schema(&schema)
        {
            Ok(_) => return plan_err!("metadata column conflict should fail"),
            Err(e) => e,
        };

        assert!(
            err.to_string()
                .contains("conflicts with an existing table column")
        );
        Ok(())
    }
}
