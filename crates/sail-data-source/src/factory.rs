use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::TableProvider;
use datafusion::datasource::file_format::arrow::{ArrowFormat, ArrowFormatFactory};
use datafusion::datasource::file_format::avro::{AvroFormat, AvroFormatFactory};
use datafusion::datasource::file_format::csv::{CsvFormat, CsvFormatFactory};
use datafusion::datasource::file_format::json::{JsonFormat, JsonFormatFactory};
use datafusion::datasource::file_format::parquet::{ParquetFormat, ParquetFormatFactory};
use datafusion::datasource::file_format::FileFormatFactory;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::SessionContext;
use datafusion_common::{internal_err, plan_err, Result};
use futures::{StreamExt, TryStreamExt};
use sail_common::spec::SaveMode;
use sail_delta_lake::create_delta_provider;
use sail_delta_lake::delta_format::DeltaFormatFactory;

use crate::options::DataSourceOptionsResolver;
use crate::url::{rewrite_directory_url, GlobUrl};

pub struct TableProviderFactory<'a> {
    ctx: &'a SessionContext,
}

impl<'a> TableProviderFactory<'a> {
    pub fn new(ctx: &'a SessionContext) -> Self {
        TableProviderFactory { ctx }
    }

    pub async fn read_table(
        &self,
        format: &str,
        paths: Vec<String>,
        schema: Option<Schema>,
        options: Vec<(String, String)>,
    ) -> Result<Arc<dyn TableProvider>> {
        if paths.is_empty() {
            return plan_err!("empty data source paths");
        }

        let options: HashMap<String, String> = options.into_iter().collect();

        // Handle delta format early, paths vec is guaranteed to be non-empty
        if matches!(format.to_lowercase().as_str(), "delta") {
            return create_delta_provider(self.ctx, &paths[0], &options).await;
        }

        let urls = self.resolve_listing_urls(paths).await?;
        // TODO: infer compression type from file extension
        // TODO: support global configuration to ignore file extension (by setting it to empty)
        let resolver = DataSourceOptionsResolver::new(self.ctx);

        let options = match format.to_lowercase().as_str() {
            "json" => {
                let options = resolver.resolve_json_read_options(options)?;
                ListingOptions::new(Arc::new(JsonFormat::default().with_options(options)))
            }
            "csv" => {
                let options = resolver.resolve_csv_read_options(options)?;
                ListingOptions::new(Arc::new(CsvFormat::default().with_options(options)))
            }
            "parquet" => {
                let options = resolver.resolve_parquet_read_options(options)?;
                ListingOptions::new(Arc::new(ParquetFormat::default().with_options(options)))
            }
            "arrow" => {
                if !options.is_empty() {
                    return plan_err!("Arrow data source read options are not yet supported");
                }
                ListingOptions::new(Arc::new(ArrowFormat))
            }
            "avro" => {
                if !options.is_empty() {
                    return plan_err!("Avro data source read options are not yet supported");
                }
                ListingOptions::new(Arc::new(AvroFormat))
            }
            other => return plan_err!("unsupported data source format: {other}"),
        };
        let options = options.with_session_config_options(&self.ctx.copied_config());
        let schema = match schema {
            // ignore empty schema
            Some(x) if !x.fields.is_empty() => x,
            _ => self.resolve_listing_schema(&urls, &options).await?,
        };
        let config = ListingTableConfig::new_with_multi_paths(urls)
            .with_listing_options(options)
            .with_schema(Arc::new(schema))
            .infer_partitions_from_path(&self.ctx.state())
            .await?;
        let config = Self::rewrite_listing_partitions(config)?;
        Ok(Arc::new(ListingTable::try_new(config)?))
    }

    pub async fn write_table(
        &self,
        source: &str,
        mode: SaveMode,
        options: Vec<(String, String)>,
    ) -> Result<Arc<dyn FileFormatFactory>> {
        let options: HashMap<String, String> = options.into_iter().collect();
        let resolver = DataSourceOptionsResolver::new(self.ctx);
        let format_factory: Arc<dyn FileFormatFactory> = match source {
            "json" => {
                let options = resolver.resolve_json_write_options(options)?;
                Arc::new(JsonFormatFactory::new_with_options(options))
            }
            "parquet" => {
                let options = resolver.resolve_parquet_write_options(options)?;
                Arc::new(ParquetFormatFactory::new_with_options(options))
            }
            "csv" => {
                let options = resolver.resolve_csv_write_options(options)?;
                Arc::new(CsvFormatFactory::new_with_options(options))
            }
            "arrow" => {
                if !options.is_empty() {
                    return plan_err!("Arrow data source write options are not yet supported");
                }
                Arc::new(ArrowFormatFactory)
            }
            "avro" => {
                if !options.is_empty() {
                    return plan_err!("Avro data source write options are not yet supported");
                }
                Arc::new(AvroFormatFactory)
            }
            "delta" => {
                let delta_options = resolver.resolve_delta_write_options(options)?;
                Arc::new(DeltaFormatFactory::new_with_options(mode, delta_options))
            }
            _ => return plan_err!("unsupported source: {source}"),
        };
        Ok(format_factory)
    }

    async fn resolve_listing_urls(&self, paths: Vec<String>) -> Result<Vec<ListingTableUrl>> {
        let mut urls = vec![];
        for path in paths {
            for url in GlobUrl::parse(&path)? {
                let url = rewrite_directory_url(url, self.ctx).await?;
                urls.push(url.try_into()?);
            }
        }
        Ok(urls)
    }

    async fn resolve_listing_schema(
        &self,
        urls: &[ListingTableUrl],
        options: &ListingOptions,
    ) -> Result<Schema> {
        // The logic is similar to `ListingOptions::infer_schema()`
        // but here we also check for the existence of files.
        let session_state = self.ctx.state();
        let mut file_groups = vec![];
        for url in urls {
            let store = self.ctx.runtime_env().object_store(url)?;
            let files: Vec<_> = url
                .list_all_files(&session_state, &store, &options.file_extension)
                .await?
                // Here we sample up to 10 files to infer the schema.
                // The value is hard-coded here since DataFusion uses the same hard-coded value
                // for operations such as `infer_partitions_from_path`.
                // We can make it configurable if DataFusion makes those operations configurable
                // as well in the future.
                .take(10)
                .try_collect()
                .await?;
            file_groups.push((store, files));
        }
        let empty = file_groups.iter().all(|(_, files)| files.is_empty());
        if empty {
            let urls = urls
                .iter()
                .map(|url| url.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            return plan_err!("No files found in the specified paths: {urls}")?;
        }

        let mut schemas = vec![];
        for (store, files) in file_groups.iter() {
            let mut schema = options
                .format
                .infer_schema(&session_state, store, files)
                .await?
                .as_ref()
                .clone();
            let ext = options.format.get_ext().to_lowercase();
            let ext = ext.trim();
            if matches!(ext, ".csv") || matches!(ext, "csv") {
                schema = Self::rename_default_csv_columns(schema);
            }
            schemas.push(schema);
        }
        let schema = Schema::try_merge(schemas)?;

        // FIXME: DataFusion 43.0.0 suddenly doesn't support Utf8View
        let new_fields: Vec<Field> = schema
            .fields()
            .iter()
            .map(|field| {
                if matches!(field.data_type(), &DataType::Utf8View) {
                    let mut new_field = field.as_ref().clone();
                    new_field = new_field.with_data_type(DataType::Utf8);
                    new_field
                } else {
                    field.as_ref().clone()
                }
            })
            .collect();
        Ok(Schema::new_with_metadata(
            new_fields,
            schema.metadata().clone(),
        ))
    }

    /// The inferred partition columns are of `Dictionary` types by default, which cannot be
    /// understood by the Spark client. So we rewrite the type to be `Utf8`.
    fn rewrite_listing_partitions(mut config: ListingTableConfig) -> Result<ListingTableConfig> {
        let Some(options) = config.options.as_mut() else {
            return internal_err!("listing options should be present in the config");
        };
        options
            .table_partition_cols
            .iter_mut()
            .for_each(|(_col, data_type)| {
                *data_type = DataType::Utf8;
            });
        Ok(config)
    }

    fn rename_default_csv_columns(schema: Schema) -> Schema {
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
}
