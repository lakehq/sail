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
    // Default to false (keep Int64) — only downcast when sampling successfully verifies
    // that all observed values fit in Int32.  This is conservative: if sampling fails
    // (compressed files, object-store errors, CSV parse errors) we keep the wider type.
    let mut fits_in_int32 = vec![false; schema.fields().len()];

    // Sample across multiple files to avoid mis-inferring Int32 from just the first file.
    const MAX_SAMPLE_BYTES: u64 = 1024 * 1024; // 1 MiB per file
    const MAX_SAMPLE_FILES: usize = 8;
    const MAX_BATCH_SIZE: usize = 8192;
    let max_rows = csv_options.schema_infer_max_rec.unwrap_or(1000);
    if max_rows == 0 {
        return Ok(schema.clone());
    }
    let batch_size = max_rows.min(MAX_BATCH_SIZE);
    let mut total_rows_checked: usize = 0;
    let mut sampled_any = false;

    // Skip sampling for compressed files: we read a raw byte prefix (up to 1 MiB) which
    // cannot be decoded without full decompression.
    if csv_options.compression != CompressionTypeVariant::UNCOMPRESSED {
        return Ok(schema.clone());
    }

    for file in files.iter().take(MAX_SAMPLE_FILES) {
        let sample_end = std::cmp::min(file.size, MAX_SAMPLE_BYTES);
        let Ok(data) = store.get_range(&file.location, 0..sample_end).await else {
            continue;
        };
        let cursor = Cursor::new(data);
        let mut reader_builder = arrow::csv::ReaderBuilder::new(Arc::new(schema.clone()))
            .with_delimiter(csv_options.delimiter)
            .with_quote(csv_options.quote)
            .with_batch_size(batch_size);
        if let Some(true) = csv_options.has_header {
            reader_builder = reader_builder.with_header(true);
        }
        if let Some(escape) = csv_options.escape {
            reader_builder = reader_builder.with_escape(escape);
        }
        if let Some(comment) = csv_options.comment {
            reader_builder = reader_builder.with_comment(comment);
        }
        if let Some(ref null_regex) = csv_options.null_regex {
            if let Ok(re) = regex::Regex::new(null_regex) {
                reader_builder = reader_builder.with_null_regex(re);
            }
        }
        if let Some(terminator) = csv_options.terminator {
            reader_builder = reader_builder.with_terminator(terminator);
        }
        if let Some(true) = csv_options.truncated_rows {
            reader_builder = reader_builder.with_truncated_rows(true);
        }
        let Ok(mut reader) = reader_builder.build(cursor) else {
            continue;
        };
        loop {
            match reader.next() {
                Some(Ok(batch)) => {
                    if !sampled_any {
                        for &col_idx in &int64_cols {
                            fits_in_int32[col_idx] = true;
                        }
                        sampled_any = true;
                    }
                    for &col_idx in &int64_cols {
                        if !fits_in_int32[col_idx] {
                            continue;
                        }
                        let array = batch.column(col_idx).as_primitive::<Int64Type>();
                        let all_fit = array
                            .iter()
                            .flatten()
                            .all(|val| val >= i64::from(i32::MIN) && val <= i64::from(i32::MAX));
                        if !all_fit {
                            fits_in_int32[col_idx] = false;
                        }
                    }
                    total_rows_checked += batch.num_rows();
                    if total_rows_checked >= max_rows {
                        break;
                    }
                    if !int64_cols.iter().any(|&idx| fits_in_int32[idx]) {
                        break;
                    }
                }
                Some(Err(_)) => {
                    // Parse error (e.g. truncated sample) — stop sampling this file.
                    break;
                }
                None => break,
            }
        }
        if total_rows_checked >= max_rows || !int64_cols.iter().any(|&idx| fits_in_int32[idx]) {
            break;
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
    #[expect(clippy::unwrap_used)]
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
    #[expect(clippy::unwrap_used)]
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

    #[tokio::test]
    #[expect(clippy::unwrap_used)]
    async fn test_convert_spark_integer_types_fallback_on_read_failure() {
        use object_store::memory::InMemory;

        // Empty store — no file exists, so sampling will fail.
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let meta = object_store::ObjectMeta {
            location: object_store::path::Path::from("missing.csv"),
            last_modified: chrono::Utc::now(),
            size: 100,
            e_tag: None,
            version: None,
        };
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]);
        let csv_options = CsvOptions {
            has_header: Some(true),
            ..Default::default()
        };
        let converted = convert_spark_integer_types(&schema, &store, &[meta], &csv_options)
            .await
            .unwrap();
        // Conservative fallback: keep Int64 when sampling fails
        assert_eq!(converted.fields()[0].data_type(), &DataType::Int64);
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
