use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::Session;
use datafusion::datasource::listing::{ListingOptions, ListingTableConfig, ListingTableUrl};
use datafusion_common::{internal_err, plan_err, Result};
use futures::{StreamExt, TryStreamExt};

pub async fn resolve_listing_schema(
    ctx: &dyn Session,
    urls: &[ListingTableUrl],
    options: &ListingOptions,
) -> Result<Arc<Schema>> {
    // The logic is similar to `ListingOptions::infer_schema()`
    // but here we also check for the existence of files.
    let mut file_groups = vec![];
    for url in urls {
        let store = ctx.runtime_env().object_store(url)?;
        let files: Vec<_> = url
            .list_all_files(ctx, &store, &options.file_extension)
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
            .infer_schema(ctx, store, files)
            .await?
            .as_ref()
            .clone();
        let ext = options.format.get_ext().to_lowercase();
        let ext = ext.trim();
        if matches!(ext, ".csv") || matches!(ext, "csv") {
            schema = rename_default_csv_columns(schema);
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
    Ok(Arc::new(Schema::new_with_metadata(
        new_fields,
        schema.metadata().clone(),
    )))
}

/// The inferred partition columns are of `Dictionary` types by default, which cannot be
/// understood by the Spark client. So we rewrite the type to be `Utf8`.
pub fn rewrite_listing_partitions(mut config: ListingTableConfig) -> Result<ListingTableConfig> {
    let Some(options) = config.options.as_mut() else {
        return internal_err!("listing options should be present in the config");
    };
    options
        .table_partition_cols
        .iter_mut()
        .for_each(|(_col, data_type)| {
            if matches!(data_type, DataType::Dictionary(_, _)) {
                *data_type = DataType::Utf8;
            }
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
