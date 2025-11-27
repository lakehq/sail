use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Result;
use datafusion::physical_plan::ExecutionPlan;
use sail_common_datafusion::datasource::{SinkInfo, SourceInfo, TableFormat};
use sail_iceberg::table::find_latest_metadata_file;
use sail_iceberg::TableIcebergOptions;
use url::Url;

use crate::options::{load_default_options, load_options, IcebergReadOptions, IcebergWriteOptions};

/// Iceberg table format implementation
#[derive(Debug, Default)]
pub struct IcebergDataSourceFormat;

#[async_trait]
impl TableFormat for IcebergDataSourceFormat {
    fn name(&self) -> &str {
        "iceberg"
    }

    async fn create_provider(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableProvider>> {
        let SourceInfo {
            paths,
            schema: _,
            constraints: _,
            partition_by: _,
            bucket_by: _,
            sort_order: _,
            options,
        } = info;

        let table_url = Self::parse_table_url(paths).await?;
        let iceberg_options = resolve_iceberg_read_options(options)?;

        sail_iceberg::table_format::create_iceberg_provider(ctx, table_url, iceberg_options).await
    }

    async fn create_writer(
        &self,
        ctx: &dyn Session,
        info: SinkInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion::physical_plan::empty::EmptyExec;
        use sail_common_datafusion::datasource::PhysicalSinkMode;
        use sail_iceberg::physical_plan::plan_builder::{IcebergPlanBuilder, IcebergTableConfig};

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
            return datafusion::common::not_impl_err!("bucketing for Iceberg format");
        }

        let table_url = Self::parse_table_url(vec![path]).await?;
        let iceberg_options = resolve_iceberg_write_options(options)?;

        let store = ctx
            .runtime_env()
            .object_store_registry
            .get_store(&table_url)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
        let exists_res = find_latest_metadata_file(&store, &table_url).await;
        let table_exists = exists_res.is_ok();

        match mode {
            PhysicalSinkMode::ErrorIfExists => {
                if table_exists {
                    return datafusion::common::plan_err!(
                        "Iceberg table already exists at path: {}",
                        table_url
                    );
                }
            }
            PhysicalSinkMode::IgnoreIfExists => {
                if table_exists {
                    return Ok(Arc::new(EmptyExec::new(input.schema())));
                }
            }
            PhysicalSinkMode::OverwriteIf { .. } | PhysicalSinkMode::OverwritePartitions => {
                return datafusion::common::not_impl_err!(
                    "predicate or partition overwrite for Iceberg"
                );
            }
            _ => {}
        }

        let table_config = IcebergTableConfig {
            table_url,
            partition_columns: partition_by,
            table_exists,
            options: iceberg_options,
        };

        let physical_sort = sort_order.map(|req| {
            req.into_iter()
                .map(|r| datafusion::physical_expr::PhysicalSortExpr {
                    expr: r.expr,
                    options: r.options.unwrap_or_default(),
                })
                .collect::<Vec<_>>()
        });

        let builder = IcebergPlanBuilder::new(input, table_config, mode, physical_sort, ctx);
        let exec = builder.build().await?;
        Ok(exec)
    }
}

impl IcebergDataSourceFormat {
    async fn parse_table_url(paths: Vec<String>) -> Result<Url> {
        if paths.len() != 1 {
            return datafusion::common::plan_err!(
                "Iceberg table requires exactly one path, got {}",
                paths.len()
            );
        }

        let path = &paths[0];
        let mut table_url = Url::parse(path)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

        if !table_url.path().ends_with('/') {
            table_url.set_path(&format!("{}/", table_url.path()));
        }
        Ok(table_url)
    }
}

fn apply_iceberg_read_options(
    from: IcebergReadOptions,
    to: &mut TableIcebergOptions,
) -> Result<()> {
    if let Some(use_ref) = from.use_ref {
        to.use_ref = Some(use_ref);
    }
    if let Some(snapshot_id) = from.snapshot_id {
        to.snapshot_id = Some(snapshot_id);
    }
    if let Some(ts) = from.timestamp_as_of {
        to.timestamp_as_of = Some(ts);
    }
    Ok(())
}

fn resolve_iceberg_read_options(
    options: Vec<std::collections::HashMap<String, String>>,
) -> Result<TableIcebergOptions> {
    let mut iceberg = TableIcebergOptions::default();
    apply_iceberg_read_options(load_default_options()?, &mut iceberg)?;
    for opt in options {
        apply_iceberg_read_options(load_options(opt)?, &mut iceberg)?;
    }
    Ok(iceberg)
}

fn apply_iceberg_write_options(
    from: IcebergWriteOptions,
    to: &mut TableIcebergOptions,
) -> Result<()> {
    if let Some(merge_schema) = from.merge_schema {
        to.merge_schema = merge_schema;
    }
    if let Some(overwrite_schema) = from.overwrite_schema {
        to.overwrite_schema = overwrite_schema;
    }
    Ok(())
}

fn resolve_iceberg_write_options(
    options: Vec<std::collections::HashMap<String, String>>,
) -> Result<TableIcebergOptions> {
    let mut iceberg = TableIcebergOptions::default();
    apply_iceberg_write_options(load_default_options()?, &mut iceberg)?;
    for opt in options {
        apply_iceberg_write_options(load_options(opt)?, &mut iceberg)?;
    }
    Ok(iceberg)
}
