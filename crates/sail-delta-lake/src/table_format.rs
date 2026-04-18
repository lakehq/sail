use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{not_impl_err, plan_err, DataFusionError, Result};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::logical_expr::TableSource;
use datafusion::physical_plan::ExecutionPlan;
use sail_common_datafusion::datasource::{
    MergeStrategy, OptionLayer, PhysicalSinkMode, RowLevelCommand, RowLevelWriteInfo, SinkInfo,
    SourceInfo, TableFormat, TableFormatRegistry,
};
use sail_common_datafusion::streaming::event::schema::is_flow_event_schema;
use sail_data_source::error::DataSourceResult;
use sail_data_source::options::gen::{
    DeltaReadOptions, DeltaReadPartialOptions, DeltaWriteOptions, DeltaWritePartialOptions,
};
use sail_data_source::options::{BuildPartialOptions, PartialOptions};
use sail_data_source::resolve_listing_urls;
use url::Url;

use crate::kernel::DeltaSnapshotConfig;
use crate::physical_plan::planner::{
    plan_delete, plan_merge, DeltaPhysicalPlanner, DeltaPlannerConfig, PlannerContext,
};
use crate::spec::{canonicalize_and_validate_table_properties, route_table_property_key};
use crate::table::open_table_with_object_store_and_table_config;
use crate::{create_delta_provider, create_delta_source, DeltaTableError};

/// Delta Lake implementation of [`TableFormat`].
#[derive(Debug)]
pub struct DeltaTableFormat;

impl DeltaTableFormat {
    pub fn register(registry: &TableFormatRegistry) -> Result<()> {
        registry.register(Arc::new(Self))?;
        Ok(())
    }
}

#[async_trait]
impl TableFormat for DeltaTableFormat {
    fn name(&self) -> &str {
        "delta"
    }

    async fn create_source(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableSource>> {
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
        let options = resolve_delta_read_options(options)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        create_delta_source(ctx, table_url, schema, options).await
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
        let options = resolve_delta_read_options(options)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        create_delta_provider(ctx, table_url, schema, options).await
    }

    async fn create_writer(
        &self,
        ctx: &dyn Session,
        info: SinkInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let path = info.path();
        let SinkInfo {
            input,
            mode,
            partition_by,
            bucket_by,
            sort_order,
            table_properties,
            options,
        } = info;

        if is_flow_event_schema(&input.schema()) {
            return not_impl_err!("writing streaming data to Delta table");
        }
        if bucket_by.is_some() {
            return not_impl_err!("bucketing for Delta format");
        }
        if partition_by.iter().any(|field| field.transform.is_some()) {
            return not_impl_err!("partition transforms for Delta format");
        }
        let partition_by = partition_by
            .into_iter()
            .map(|field| field.column)
            .collect::<Vec<_>>();

        let table_url = Self::parse_table_url(ctx, vec![path]).await?;
        let (options, routed_table_properties) =
            split_delta_write_options_and_table_properties(options);
        let delta_options = resolve_delta_write_options(options)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let object_store = ctx
            .runtime_env()
            .object_store_registry
            .get_store(&table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let table = match open_table_with_object_store_and_table_config(
            table_url.clone(),
            object_store,
            Default::default(),
            // Only partition columns and table existence are needed at planning time;
            // skip replaying Add/Remove file actions which are not used here.
            DeltaSnapshotConfig {
                require_files: false,
                ..Default::default()
            },
        )
        .await
        {
            Ok(table) => Some(table),
            Err(DeltaTableError::InvalidTableLocation(_))
            | Err(DeltaTableError::FileNotFound(_)) => None,
            Err(err) => return Err(DataFusionError::External(Box::new(err))),
        };
        let table_exists = table.is_some();
        let mut metadata_configuration = resolve_delta_metadata_configuration(&table_properties)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        if table_exists {
            if !routed_table_properties.is_empty() {
                let mut keys: Vec<_> = routed_table_properties.keys().cloned().collect();
                keys.sort();
                log::warn!(
                    "ignoring write-time Delta table properties for existing table at {table_url}: {}",
                    keys.join(", ")
                );
            }
        } else {
            let routed_metadata_configuration =
                resolve_delta_metadata_configuration(&routed_table_properties)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
            metadata_configuration.extend(routed_metadata_configuration);
        }

        match mode {
            PhysicalSinkMode::ErrorIfExists if table_exists => {
                return plan_err!("Delta table already exists at path: {table_url}");
            }
            PhysicalSinkMode::IgnoreIfExists if table_exists => {
                return Ok(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                    input.schema(),
                )));
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
                    PhysicalSinkMode::Overwrite | PhysicalSinkMode::OverwriteIf { .. }
                        // For overwrite mode, check if schema overwrite is allowed
                        if !delta_options.overwrite_schema => {
                            return plan_err!(
                                "Partition column mismatch. Table is partitioned by {:?}, but write specified {:?}. \
                                Set overwriteSchema=true to change partitioning.",
                                existing_partitions,
                                partition_by
                            );
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

        let table_config = DeltaPlannerConfig::new(
            table_url,
            delta_options,
            metadata_configuration,
            partition_columns,
            table_schema_for_cond,
            table_exists,
        );
        let planner_ctx = PlannerContext::new(ctx, table_config);
        let planner = DeltaPhysicalPlanner::new(planner_ctx);
        let sink_exec = planner.create_plan(input, unified_mode, sort_order).await?;

        Ok(sink_exec)
    }

    async fn create_row_level_writer(
        &self,
        ctx: &dyn Session,
        info: RowLevelWriteInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Strategy branching: all row-level operations check the materialization strategy
        // before delegating to the format-specific planner. MoR support will be implemented
        // here in the future; for now only Eager (Copy-on-Write) is supported.
        match info.merge_strategy {
            MergeStrategy::Eager => {}
            MergeStrategy::MergeOnRead => {
                return not_impl_err!(
                    "Merge-on-Read strategy is not yet implemented for Delta Lake"
                );
            }
        }

        match info.command {
            RowLevelCommand::Delete => {
                let table_url = Self::parse_table_url(ctx, vec![info.target.path]).await?;
                let condition = info.condition.ok_or_else(|| {
                    DataFusionError::Plan("DELETE operation requires a WHERE condition".to_string())
                })?;
                let delta_options = resolve_delta_write_options(info.target.options)?;
                let delete_config = DeltaPlannerConfig::new(
                    table_url,
                    delta_options,
                    HashMap::new(),
                    Vec::new(),
                    None,
                    true,
                );
                let delete_ctx = PlannerContext::new(ctx, delete_config);
                plan_delete(&delete_ctx, condition).await
            }
            RowLevelCommand::Merge => {
                let table_url = Self::parse_table_url(ctx, vec![info.target.path.clone()]).await?;
                let delta_options = resolve_delta_write_options(info.target.options.clone())?;
                let merge_config = DeltaPlannerConfig::new(
                    table_url,
                    delta_options,
                    HashMap::new(),
                    info.target.partition_by.clone(),
                    None,
                    true,
                );
                let merge_ctx = PlannerContext::new(ctx, merge_config);
                plan_merge(&merge_ctx, info).await
            }
            RowLevelCommand::Update => {
                not_impl_err!("UPDATE is not yet implemented for Delta Lake")
            }
        }
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

pub fn resolve_delta_read_options(options: Vec<OptionLayer>) -> DataSourceResult<DeltaReadOptions> {
    let mut partial = DeltaReadPartialOptions::initialize();
    for layer in options {
        partial.merge(layer.build_partial_options()?);
    }
    partial.finalize()
}

pub fn resolve_delta_write_options(
    options: Vec<OptionLayer>,
) -> DataSourceResult<DeltaWriteOptions> {
    let mut partial = DeltaWritePartialOptions::initialize();
    for layer in options {
        partial.merge(layer.build_partial_options()?);
    }
    partial.finalize()
}

fn resolve_delta_metadata_configuration(
    table_properties: &HashMap<String, String>,
) -> crate::spec::DeltaResult<HashMap<String, String>> {
    canonicalize_and_validate_table_properties(
        table_properties
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str())),
    )
}

fn split_delta_write_options_and_table_properties(
    options: Vec<OptionLayer>,
) -> (Vec<OptionLayer>, HashMap<String, String>) {
    let mut table_properties = HashMap::new();
    let clean_options = options
        .into_iter()
        .map(|layer| match layer {
            OptionLayer::OptionList { items } => {
                let mut clean_items = Vec::with_capacity(items.len());
                for (key, value) in items {
                    if let Some(property_key) = route_table_property_key(&key) {
                        table_properties.insert(property_key, value);
                    } else {
                        clean_items.push((key, value));
                    }
                }
                OptionLayer::OptionList { items: clean_items }
            }
            OptionLayer::TablePropertyList { items } => {
                let mut clean_items = Vec::with_capacity(items.len());
                for (key, value) in items {
                    if let Some(property_key) = route_table_property_key(&key) {
                        table_properties.insert(property_key, value);
                    } else {
                        clean_items.push((key, value));
                    }
                }
                OptionLayer::TablePropertyList { items: clean_items }
            }
            other => other,
        })
        .collect();
    (clean_options, table_properties)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_delta_write_options_and_table_properties() {
        let options = vec![
            OptionLayer::OptionList {
                items: vec![
                    ("mergeSchema".to_string(), "true".to_string()),
                    ("column_mapping_mode".to_string(), "name".to_string()),
                ],
            },
            OptionLayer::OptionList {
                items: vec![
                    ("delta.appendOnly".to_string(), "true".to_string()),
                    ("targetFileSize".to_string(), "10".to_string()),
                ],
            },
        ];

        let (clean_options, table_properties) =
            split_delta_write_options_and_table_properties(options);

        assert_eq!(clean_options.len(), 2);
        match &clean_options[0] {
            OptionLayer::OptionList { items } => {
                assert_eq!(items, &[("mergeSchema".to_string(), "true".to_string())]);
            }
            _ => unreachable!("expected OptionList"),
        }
        match &clean_options[1] {
            OptionLayer::OptionList { items } => {
                assert_eq!(items, &[("targetFileSize".to_string(), "10".to_string())]);
            }
            _ => unreachable!("expected OptionList"),
        }
        assert_eq!(
            table_properties.get("delta.columnMapping.mode"),
            Some(&"name".to_string())
        );
        assert_eq!(
            table_properties.get("delta.appendOnly"),
            Some(&"true".to_string())
        );
    }
}
