use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::{DataType, Field, Int64Type, Schema};
use datafusion::catalog::Session;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion_common::config::CsvOptions;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_datasource::file_format::FileFormat;
use object_store::ObjectStore;

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

/// Convert Int64 columns to Int32 where all sampled values fit, matching Spark's behavior.
/// Spark tries IntegerType (Int32) first and only promotes to LongType (Int64) if any value
/// doesn't fit, while DataFusion always infers Int64.
async fn convert_spark_integer_types(
    schema: &Schema,
    store: &Arc<dyn ObjectStore>,
    files: &[object_store::ObjectMeta],
    csv_options: &CsvOptions,
) -> datafusion_common::Result<Schema> {
    let int64_cols: Vec<usize> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| f.data_type() == &DataType::Int64)
        .map(|(i, _)| i)
        .collect();

    if int64_cols.is_empty() || files.is_empty() {
        return Ok(schema.clone());
    }

    // Track which Int64 columns can safely be downcast to Int32.
    // Default to true (downcast) — if sampling fails (e.g. compressed files), we fall back
    // to Int32 which matches the common case for small CSV datasets.
    let mut fits_in_int32 = vec![true; schema.fields().len()];

    // Sample the first file to check actual values.
    // If the file is compressed or unreadable as raw CSV, skip the check and use Int32.
    if let Ok(result) = store.get(&files[0].location).await {
        if let Ok(data) = result.bytes().await {
            let cursor = Cursor::new(data);
            let mut reader_builder = arrow::csv::ReaderBuilder::new(Arc::new(schema.clone()))
                .with_delimiter(csv_options.delimiter)
                .with_quote(csv_options.quote);
            if let Some(true) = csv_options.has_header {
                reader_builder = reader_builder.with_header(true);
            }
            if let Some(escape) = csv_options.escape {
                reader_builder = reader_builder.with_escape(escape);
            }
            let batch_size = csv_options.schema_infer_max_rec.unwrap_or(1000);
            reader_builder = reader_builder.with_batch_size(batch_size);
            if let Ok(mut reader) = reader_builder.build(cursor) {
                if let Some(Ok(batch)) = reader.next() {
                    for &col_idx in &int64_cols {
                        let array = batch.column(col_idx).as_primitive::<Int64Type>();
                        for val in array.iter().flatten() {
                            if val < i64::from(i32::MIN) || val > i64::from(i32::MAX) {
                                fits_in_int32[col_idx] = false;
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    let fields = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            if field.data_type() == &DataType::Int64 && fits_in_int32[i] {
                Field::new(field.name(), DataType::Int32, field.is_nullable())
            } else {
                field.as_ref().clone()
            }
        })
        .collect::<Vec<_>>();
    Ok(Schema::new_with_metadata(fields, schema.metadata().clone()))
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
        let csv_options = resolve_csv_read_options(ctx, options.to_vec())?;
        if let Ok(read_options) = load_options::<CsvReadOptions>(merged_options) {
            if read_options.infer_schema == Some(false) {
                schema = convert_string_columns(schema);
            }
        }
        // Convert Int64 → Int32 only where sampled values fit
        schema = convert_spark_integer_types(&schema, store, files, &csv_options).await?;
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

    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn test_convert_spark_integer_types_small_values() {
        use object_store::memory::InMemory;
        use object_store::path::Path;

        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("test.csv");
        let content = b"name,price,weight,count\na,100,1.5,42\nb,200,2.5,99\n";
        store.put(&path, content.as_ref().into()).await.unwrap();
        let meta = store.head(&path).await.unwrap();
        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("price", DataType::Int64, true),
            Field::new("weight", DataType::Float64, true),
            Field::new("count", DataType::Int64, true),
        ]);
        let csv_options = CsvOptions {
            has_header: Some(true),
            ..Default::default()
        };
        let converted = convert_spark_integer_types(&schema, &store, &[meta], &csv_options)
            .await
            .unwrap();
        assert_eq!(converted.fields()[0].data_type(), &DataType::Utf8);
        assert_eq!(converted.fields()[1].data_type(), &DataType::Int32);
        assert_eq!(converted.fields()[2].data_type(), &DataType::Float64);
        assert_eq!(converted.fields()[3].data_type(), &DataType::Int32);
    }

    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn test_convert_spark_integer_types_large_values() {
        use object_store::memory::InMemory;
        use object_store::path::Path;

        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("test.csv");
        let content = b"small,big\n1,3000000000\n2,4000000000\n";
        store.put(&path, content.as_ref().into()).await.unwrap();
        let meta = store.head(&path).await.unwrap();
        let schema = Schema::new(vec![
            Field::new("small", DataType::Int64, true),
            Field::new("big", DataType::Int64, true),
        ]);
        let csv_options = CsvOptions {
            has_header: Some(true),
            ..Default::default()
        };
        let converted = convert_spark_integer_types(&schema, &store, &[meta], &csv_options)
            .await
            .unwrap();
        // small values fit in Int32
        assert_eq!(converted.fields()[0].data_type(), &DataType::Int32);
        // large values don't fit in Int32, stay as Int64
        assert_eq!(converted.fields()[1].data_type(), &DataType::Int64);
    }

    #[test]
    #[expect(clippy::unwrap_used)]
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
