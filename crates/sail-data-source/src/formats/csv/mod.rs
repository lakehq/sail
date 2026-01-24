use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::Session;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_datasource::file_format::FileFormat;

use crate::formats::csv::options::{resolve_csv_read_options, resolve_csv_write_options};
use crate::formats::listing::{ListingFormat, ListingTableFormat, SchemaInfer};
use crate::options::{load_options, merge_options, CsvReadOptions};

mod options;

pub type CsvTableFormat = ListingTableFormat<CsvListingFormat>;

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
pub struct CsvSchemaInfer;

#[async_trait::async_trait]
impl SchemaInfer for CsvSchemaInfer {
    async fn get_schema(
        &self,
        ctx: &dyn Session,
        store: &Arc<dyn object_store::ObjectStore>,
        files: &[object_store::ObjectMeta],
        list_options: &datafusion::datasource::listing::ListingOptions,
        options: &[HashMap<String, String>],
    ) -> datafusion_common::Result<Schema> {
        let mut schema = list_options
            .format
            .infer_schema(ctx, store, files)
            .await?
            .as_ref()
            .clone();
        let merged_options = merge_options(options.to_vec());
        if let Ok(csv_options) = load_options::<CsvReadOptions>(merged_options) {
            if csv_options.infer_schema == Some(false) {
                schema = convert_string_columns(schema);
            }
        }
        // Rename default CSV columns (column_1 -> _c0, etc.)
        schema = rename_default_csv_columns(schema);

        Ok(schema)
    }
}

#[derive(Debug, Default)]
pub struct CsvListingFormat;

impl ListingFormat for CsvListingFormat {
    fn name(&self) -> &'static str {
        "csv"
    }

    fn create_read_format(
        &self,
        ctx: &dyn Session,
        options: Vec<HashMap<String, String>>,
        compression: Option<CompressionTypeVariant>,
    ) -> datafusion_common::Result<Arc<dyn FileFormat>> {
        let mut options = resolve_csv_read_options(ctx, options)?;
        if let Some(compression) = compression {
            options.compression = compression;
        }
        Ok(Arc::new(CsvFormat::default().with_options(options)))
    }

    fn create_write_format(
        &self,
        ctx: &dyn Session,
        options: Vec<HashMap<String, String>>,
    ) -> datafusion_common::Result<(Arc<dyn FileFormat>, Option<String>)> {
        let options = resolve_csv_write_options(ctx, options)?;
        Ok((Arc::new(CsvFormat::default().with_options(options)), None))
    }

    fn schema_inferrer(&self) -> Arc<dyn SchemaInfer> {
        Arc::new(CsvSchemaInfer)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

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

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_csv_read_options_parsing() {
        use crate::options::{load_options, CsvReadOptions};
        // Test infer_schema option parsing
        let options = HashMap::from([("infer_schema".to_string(), "false".to_string())]);
        let csv_options = load_options::<CsvReadOptions>(options).unwrap();
        assert_eq!(csv_options.infer_schema, Some(false));
        let options = HashMap::from([("inferSchema".to_string(), "true".to_string())]);
        let csv_options = load_options::<CsvReadOptions>(options).unwrap();
        assert_eq!(csv_options.infer_schema, Some(true));
    }
}
