use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::Session;
use datafusion_common::{DataFusionError, Result};
use sail_common_datafusion::datasource::OptionLayer;

use crate::listing::source::{FormatFactory, ListingTableFormat, SchemaInfer};
use crate::options::gen::{CsvReadOptions, CsvWriteOptions};
use crate::options::ResolveOptions;

// Some of the code in the `read` and `write` modules is adapted from the DataFusion `CsvFormat` implementation.
// [CREDIT]: https://github.com/apache/datafusion/blob/53.1.0/datafusion/datasource-csv/src/file_format.rs

mod options;
mod read;
mod write;

pub use read::CsvReadFormat;
pub use write::CsvWriteFormat;

pub type CsvTableFormat = ListingTableFormat<CsvFormatFactory>;

fn convert_string_columns(schema: Schema) -> Schema {
    let string_fields = schema
        .fields()
        .iter()
        .map(|field| Field::new(field.name(), DataType::Utf8, field.is_nullable()))
        .collect::<Vec<_>>();

    Schema::new_with_metadata(string_fields, schema.metadata().clone())
}

fn rename_default_csv_columns(schema: Schema) -> Schema {
    use std::collections::HashSet;

    let mut failed_parsing = false;
    let mut seen_names = HashSet::new();
    let mut new_fields = schema
        .fields()
        .iter()
        .map(|field| {
            // Order may not be guaranteed, so we try to parse the index from the column name
            let new_name = if field.name().starts_with("column_") {
                if let Some(index_str) = field.name().strip_prefix("column_") {
                    if let Ok(index) = index_str.trim().parse::<usize>() {
                        format!("_c{}", index.saturating_sub(1))
                    } else {
                        failed_parsing = true;
                        field.name().to_string()
                    }
                } else {
                    field.name().to_string()
                }
            } else {
                field.name().to_string()
            };
            if !seen_names.insert(new_name.clone()) {
                failed_parsing = true;
            }
            Field::new(new_name, field.data_type().clone(), field.is_nullable())
        })
        .collect::<Vec<_>>();

    if failed_parsing {
        new_fields = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                Field::new(
                    format!("_c{i}"),
                    field.data_type().clone(),
                    field.is_nullable(),
                )
            })
            .collect::<Vec<_>>();
    }

    Schema::new_with_metadata(new_fields, schema.metadata().clone())
}

/// Schema inferrer for CSV format
#[derive(Debug)]
pub struct CsvSchemaInfer {
    infer_schema: bool,
}

#[async_trait::async_trait]
impl SchemaInfer for CsvSchemaInfer {
    async fn get_schema(
        &self,
        ctx: &dyn Session,
        store: &Arc<dyn object_store::ObjectStore>,
        files: &[object_store::ObjectMeta],
        list_options: &datafusion::datasource::listing::ListingOptions,
    ) -> Result<Schema> {
        let mut schema = list_options
            .format
            .infer_schema(ctx, store, files)
            .await?
            .as_ref()
            .clone();
        if !self.infer_schema {
            schema = convert_string_columns(schema);
        }
        // Rename default CSV columns (column_1 -> _c0, etc.)
        schema = rename_default_csv_columns(schema);

        Ok(schema)
    }
}

#[derive(Debug, Default)]
pub struct CsvFormatFactory;

impl FormatFactory for CsvFormatFactory {
    type Read = CsvReadFormat;
    type Write = CsvWriteFormat;

    fn name() -> &'static str {
        "csv"
    }

    fn read(ctx: &dyn Session, options: Vec<OptionLayer>) -> Result<Self::Read> {
        let options = CsvReadOptions::resolve(ctx, options).map_err(DataFusionError::from)?;
        Ok(CsvReadFormat { options })
    }

    fn write(ctx: &dyn Session, options: Vec<OptionLayer>) -> Result<Self::Write> {
        let options = CsvWriteOptions::resolve(ctx, options).map_err(DataFusionError::from)?;
        Ok(CsvWriteFormat { options })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_string_columns() {
        // Create schema with mixed types
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("birthday", DataType::Date32, true),
        ]);
        let converted = convert_string_columns(schema);
        // Verify all fields are now strings
        assert_eq!(converted.fields()[0].data_type(), &DataType::Utf8);
        assert_eq!(converted.fields()[1].data_type(), &DataType::Utf8);
        assert_eq!(converted.fields()[2].data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_rename_default_csv_columns() {
        // Create schema with default column names
        let schema = Schema::new(vec![
            Field::new("column_1", DataType::Utf8, true),
            Field::new("column_2", DataType::Int64, true),
            Field::new("column_3", DataType::Utf8, true),
        ]);
        let renamed = rename_default_csv_columns(schema);
        // Check renamed columns
        assert_eq!(renamed.fields()[0].name(), "_c0");
        assert_eq!(renamed.fields()[1].name(), "_c1");
        assert_eq!(renamed.fields()[2].name(), "_c2");
    }
}
