use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion_common::{Result, plan_err};

#[derive(Debug, Clone, Copy)]
pub(crate) struct RowLevelMetadataColumns<'a> {
    file_column_name: Option<&'a str>,
    row_index_column_name: Option<&'a str>,
}

impl<'a> RowLevelMetadataColumns<'a> {
    pub(crate) fn new(
        file_column_name: Option<&'a str>,
        row_index_column_name: Option<&'a str>,
    ) -> Self {
        Self {
            file_column_name,
            row_index_column_name,
        }
    }

    pub(crate) fn append_to_schema(&self, data_schema: &ArrowSchema) -> Result<ArrowSchema> {
        self.validate_no_collisions(data_schema)?;
        let mut fields = data_schema.fields().iter().cloned().collect::<Vec<_>>();
        if let Some(name) = self.file_column_name {
            fields.push(Arc::new(Field::new(name, DataType::Utf8, true)));
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
        for name in [self.file_column_name, self.row_index_column_name]
            .into_iter()
            .flatten()
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
