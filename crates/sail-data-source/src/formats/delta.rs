use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{not_impl_err, plan_err, DataFusionError, Result};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::physical_plan::ExecutionPlan;
use sail_common_datafusion::datasource::{PhysicalSinkMode, SinkInfo, SourceInfo, TableFormat};
use sail_delta_lake::create_delta_provider;
use sail_delta_lake::delta_format::DeltaPlanBuilder;
use sail_delta_lake::options::TableDeltaOptions;
use sail_delta_lake::table::open_table_with_object_store;
use url::Url;

use crate::options::{load_default_options, load_options, DeltaReadOptions, DeltaWriteOptions};

#[derive(Debug, Default)]
pub struct DeltaTableFormat;

#[async_trait]
impl TableFormat for DeltaTableFormat {
    fn name(&self) -> &str {
        "delta"
    }

    async fn create_provider(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableProvider>> {
        let SourceInfo {
            paths,
            schema,
            constraints: _,
            partition_by: _,
            bucket_by: _,
            sort_order: _,
            options,
        } = info;
        let table_url = Self::parse_table_url(ctx, paths).await?;
        let options = resolve_delta_read_options(options)?;
        create_delta_provider(ctx, table_url, schema, options).await
    }

    async fn create_writer(
        &self,
        ctx: &dyn Session,
        info: SinkInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let SinkInfo {
            input,
            path,
            mode,
            partition_by,
            bucket_by,
            sort_order,
            options,
        } = info;

        if bucket_by.is_some() {
            return not_impl_err!("bucketing for Delta format");
        }

        let table_url = Self::parse_table_url(ctx, vec![path]).await?;
        let delta_options = resolve_delta_write_options(options)?;

        // Check for table existence
        let object_store = ctx
            .runtime_env()
            .object_store_registry
            .get_store(&table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let table_exists =
            open_table_with_object_store(table_url.clone(), object_store, Default::default())
                .await
                .is_ok();

        // Handle cases that don't require actual writing
        match mode {
            PhysicalSinkMode::ErrorIfExists => {
                if table_exists {
                    return plan_err!("Delta table already exists at path: {table_url}");
                }
            }
            PhysicalSinkMode::IgnoreIfExists => {
                if table_exists {
                    // If table exists, do nothing. We can return an empty plan.
                    return Ok(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                        input.schema(),
                    )));
                }
            }
            PhysicalSinkMode::OverwritePartitions => {
                return not_impl_err!("unsupported sink mode for Delta: {mode:?}")
            }
            _ => {} // Other modes will be handled in the execution phase
        }

        let plan_builder = DeltaPlanBuilder::new(
            input,
            table_url,
            delta_options,
            partition_by,
            mode,
            table_exists,
            sort_order,
        );
        let sink_exec = plan_builder.build()?;

        Ok(sink_exec)
    }
}

impl DeltaTableFormat {
    async fn parse_table_url(ctx: &dyn Session, paths: Vec<String>) -> Result<Url> {
        let mut urls = crate::url::resolve_listing_urls(ctx, paths.clone()).await?;
        match (urls.pop(), urls.is_empty()) {
            (Some(path), true) => Ok(<ListingTableUrl as AsRef<Url>>::as_ref(&path).clone()),
            _ => plan_err!("expected a single path for Delta table sink: {paths:?}"),
        }
    }
}

fn apply_delta_read_options(from: DeltaReadOptions, to: &mut TableDeltaOptions) -> Result<()> {
    // TODO: implement read options
    let _ = (from, to);
    Ok(())
}

fn apply_delta_write_options(from: DeltaWriteOptions, to: &mut TableDeltaOptions) -> Result<()> {
    if let Some(replace_where) = from.replace_where {
        to.replace_where = Some(replace_where);
    }
    if let Some(merge_schema) = from.merge_schema {
        to.merge_schema = merge_schema;
    }
    if let Some(overwrite_schema) = from.overwrite_schema {
        to.overwrite_schema = overwrite_schema;
    }
    if let Some(target_file_size) = from.target_file_size {
        to.target_file_size = target_file_size;
    }
    if let Some(write_batch_size) = from.write_batch_size {
        to.write_batch_size = write_batch_size;
    }
    Ok(())
}

pub fn resolve_delta_read_options(
    options: Vec<HashMap<String, String>>,
) -> Result<TableDeltaOptions> {
    let mut delta_options = TableDeltaOptions::default();
    apply_delta_read_options(load_default_options()?, &mut delta_options)?;
    for opt in options {
        apply_delta_read_options(load_options(opt)?, &mut delta_options)?;
    }
    Ok(delta_options)
}

pub fn resolve_delta_write_options(
    options: Vec<HashMap<String, String>>,
) -> Result<TableDeltaOptions> {
    let mut delta_options = TableDeltaOptions::default();
    apply_delta_write_options(load_default_options()?, &mut delta_options)?;
    for opt in options {
        apply_delta_write_options(load_options(opt)?, &mut delta_options)?;
    }
    Ok(delta_options)
}
