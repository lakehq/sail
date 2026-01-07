use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{not_impl_err, plan_err, DataFusionError, Result};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::physical_plan::ExecutionPlan;
use sail_common_datafusion::datasource::{
    DeleteInfo, MergeInfo, PhysicalSinkMode, SinkInfo, SourceInfo, TableFormat, TableFormatRegistry,
};
use sail_common_datafusion::streaming::event::schema::is_flow_event_schema;
use sail_data_source::options::{
    load_default_options, load_options, DeltaReadOptions, DeltaWriteOptions,
};
use sail_data_source::resolve_listing_urls;
use url::Url;

use crate::options::{ColumnMappingModeOption, TableDeltaOptions};
use crate::physical_plan::planner::{
    plan_delete, plan_merge, DeltaPhysicalPlanner, DeltaTableConfig, PlannerContext,
};
use crate::table::open_table_with_object_store;
use crate::{create_delta_provider, DeltaTableError, KernelError};

/// Delta Lake implementation of [`TableFormat`].
#[derive(Debug)]
pub struct DeltaTableFormat;

impl DeltaTableFormat {
    pub fn register(registry: &TableFormatRegistry) -> Result<()> {
        registry.register(Arc::new(Self))?;

        crate::init_delta_types();
        Ok(())
    }
}

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

        if is_flow_event_schema(&input.schema()) {
            return not_impl_err!("writing streaming data to Delta table");
        }
        if bucket_by.is_some() {
            return not_impl_err!("bucketing for Delta format");
        }

        let table_url = Self::parse_table_url(ctx, vec![path]).await?;
        let delta_options = resolve_delta_write_options(options)?;

        let object_store = ctx
            .runtime_env()
            .object_store_registry
            .get_store(&table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let table =
            match open_table_with_object_store(table_url.clone(), object_store, Default::default())
                .await
            {
                Ok(table) => Some(table),
                Err(DeltaTableError::Kernel(KernelError::InvalidTableLocation(_)))
                | Err(DeltaTableError::Kernel(KernelError::FileNotFound(_))) => None,
                Err(err) => return Err(DataFusionError::External(Box::new(err))),
            };
        let table_exists = table.is_some();

        match mode {
            PhysicalSinkMode::ErrorIfExists => {
                if table_exists {
                    return plan_err!("Delta table already exists at path: {table_url}");
                }
            }
            PhysicalSinkMode::IgnoreIfExists => {
                if table_exists {
                    return Ok(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                        input.schema(),
                    )));
                }
            }
            PhysicalSinkMode::OverwritePartitions => {
                return not_impl_err!("unsupported sink mode for Delta: {mode:?}")
            }
            _ => {}
        }

        let unified_mode = mode;
        let table_schema_for_cond = None;

        // Get existing partition columns from table metadata if available
        let existing_partition_columns = if let Some(table) = &table {
            Some(
                table
                    .snapshot()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
                    .metadata()
                    .partition_columns()
                    .clone(),
            )
        } else {
            None
        };

        // Validate partition column mismatch for append/overwrite operations
        if let Some(existing_partitions) = &existing_partition_columns {
            if !partition_by.is_empty() && partition_by != *existing_partitions {
                // Allow partition column changes only when overwriting with schema changes
                // For append mode, this is always an error
                match unified_mode {
                    PhysicalSinkMode::Append => {
                        return plan_err!(
                            "Partition column mismatch. Table is partitioned by {:?}, but write specified {:?}. \
                            Cannot change partitioning on append.",
                            existing_partitions,
                            partition_by
                        );
                    }
                    PhysicalSinkMode::Overwrite | PhysicalSinkMode::OverwriteIf { .. } => {
                        // For overwrite mode, check if schema overwrite is allowed
                        if !delta_options.overwrite_schema {
                            return plan_err!(
                                "Partition column mismatch. Table is partitioned by {:?}, but write specified {:?}. \
                                Set overwriteSchema=true to change partitioning.",
                                existing_partitions,
                                partition_by
                            );
                        }
                    }
                    _ => {}
                }
            }
        }

        let partition_columns = if !partition_by.is_empty() {
            partition_by
        } else {
            existing_partition_columns.unwrap_or_default()
        };

        let table_config = DeltaTableConfig::new(
            table_url,
            delta_options,
            partition_columns,
            table_schema_for_cond,
            table_exists,
        );
        let planner_ctx = PlannerContext::new(ctx, table_config);
        let planner = DeltaPhysicalPlanner::new(planner_ctx);
        let sink_exec = planner.create_plan(input, unified_mode, sort_order).await?;

        Ok(sink_exec)
    }

    async fn create_deleter(
        &self,
        ctx: &dyn Session,
        info: DeleteInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let DeleteInfo {
            path,
            condition,
            options,
        } = info;

        let table_url = Self::parse_table_url(ctx, vec![path]).await?;

        let condition = condition.ok_or_else(|| {
            DataFusionError::Plan("DELETE operation requires a WHERE condition".to_string())
        })?;

        let delta_options = resolve_delta_write_options(options)?;

        let delete_config = DeltaTableConfig::new(table_url, delta_options, Vec::new(), None, true);
        let delete_ctx = PlannerContext::new(ctx, delete_config);
        let delete_exec = plan_delete(&delete_ctx, condition).await?;

        Ok(delete_exec)
    }

    async fn create_merger(
        &self,
        ctx: &dyn Session,
        info: MergeInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let table_url = Self::parse_table_url(ctx, vec![info.target.path.clone()]).await?;
        let delta_options = resolve_delta_write_options(info.target.options.clone())?;
        let merge_config = DeltaTableConfig::new(
            table_url,
            delta_options,
            info.target.partition_by.clone(),
            None,
            true,
        );
        let merge_ctx = PlannerContext::new(ctx, merge_config);
        let merge_exec = plan_merge(&merge_ctx, info).await?;
        Ok(merge_exec)
    }
}

impl DeltaTableFormat {
    async fn parse_table_url(ctx: &dyn Session, paths: Vec<String>) -> Result<Url> {
        let mut urls = resolve_listing_urls(ctx, paths.clone()).await?;
        match (urls.pop(), urls.is_empty()) {
            (Some(path), true) => Ok(<ListingTableUrl as AsRef<Url>>::as_ref(&path).clone()),
            _ => plan_err!("expected a single path for Delta table sink: {paths:?}"),
        }
    }
}

fn apply_delta_read_options(from: DeltaReadOptions, to: &mut TableDeltaOptions) -> Result<()> {
    if let Some(timestamp_as_of) = from.timestamp_as_of {
        to.timestamp_as_of = Some(timestamp_as_of)
    }
    if let Some(version_as_of) = from.version_as_of {
        to.version_as_of = Some(version_as_of)
    }
    Ok(())
}

fn apply_delta_write_options(from: DeltaWriteOptions, to: &mut TableDeltaOptions) -> Result<()> {
    if let Some(merge_schema) = from.merge_schema {
        to.merge_schema = merge_schema;
    }
    if let Some(overwrite_schema) = from.overwrite_schema {
        to.overwrite_schema = overwrite_schema;
    }
    if let Some(replace_where) = from.replace_where {
        to.replace_where = Some(replace_where);
    }
    if let Some(target_file_size) = from.target_file_size {
        to.target_file_size = target_file_size;
    }
    if let Some(write_batch_size) = from.write_batch_size {
        to.write_batch_size = write_batch_size;
    }
    if let Some(column_mapping_mode) = from.column_mapping_mode {
        match column_mapping_mode.to_ascii_lowercase().as_str() {
            "name" => to.column_mapping_mode = ColumnMappingModeOption::Name,
            "id" => to.column_mapping_mode = ColumnMappingModeOption::Id,
            _ => to.column_mapping_mode = ColumnMappingModeOption::None,
        }
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
