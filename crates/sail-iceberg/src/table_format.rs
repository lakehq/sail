// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{not_impl_err, plan_err, DataFusionError, Result};
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::ExecutionPlan;
use sail_common_datafusion::datasource::{
    PhysicalSinkMode, SinkInfo, SourceInfo, TableFormat, TableFormatRegistry,
};
use sail_data_source::options::{
    load_default_options, load_options, IcebergReadOptions, IcebergWriteOptions,
};
use url::Url;

use crate::options::TableIcebergOptions;
use crate::physical_plan::plan_builder::{IcebergPlanBuilder, IcebergTableConfig};
use crate::spec::{PartitionSpec, Schema, Snapshot};
use crate::table::{find_latest_metadata_file, Table};

/// Iceberg implementation of [`TableFormat`].
#[derive(Debug, Default)]
pub struct IcebergTableFormat;

impl IcebergTableFormat {
    pub fn register(registry: &TableFormatRegistry) -> Result<()> {
        registry.register(Arc::new(Self))
    }
}

#[async_trait]
impl TableFormat for IcebergTableFormat {
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

        create_iceberg_provider(ctx, table_url, iceberg_options).await
    }

    async fn create_writer(
        &self,
        ctx: &dyn Session,
        info: SinkInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion::physical_plan::empty::EmptyExec;

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
            return not_impl_err!("bucketing for Iceberg format");
        }

        let table_url = Self::parse_table_url(vec![path]).await?;
        let iceberg_options = resolve_iceberg_write_options(options)?;

        let store = ctx
            .runtime_env()
            .object_store_registry
            .get_store(&table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let exists_res = find_latest_metadata_file(&store, &table_url).await;
        let table_exists = exists_res.is_ok();

        match mode {
            PhysicalSinkMode::ErrorIfExists => {
                if table_exists {
                    return plan_err!("Iceberg table already exists at path: {table_url}");
                }
            }
            PhysicalSinkMode::IgnoreIfExists => {
                if table_exists {
                    return Ok(Arc::new(EmptyExec::new(input.schema())));
                }
            }
            PhysicalSinkMode::OverwriteIf { .. } | PhysicalSinkMode::OverwritePartitions => {
                return not_impl_err!("predicate or partition overwrite for Iceberg");
            }
            _ => {}
        }

        // Get existing partition columns if table exists
        let existing_partition_columns = if table_exists {
            let table = Table::load(ctx, table_url.clone()).await?;
            Some(Self::partition_columns_from_metadata(&table)?)
        } else {
            None
        };

        // Validate partition column mismatch for append/overwrite operations
        if let Some(existing_partitions) = &existing_partition_columns {
            if !partition_by.is_empty() && partition_by != *existing_partitions {
                // For append mode, partition column changes are not allowed
                match mode {
                    PhysicalSinkMode::Append => {
                        return plan_err!(
                            "Partition column mismatch. Table is partitioned by {:?}, but write specified {:?}. \
                            Cannot change partitioning on append.",
                            existing_partitions,
                            partition_by
                        );
                    }
                    PhysicalSinkMode::Overwrite => {
                        // For overwrite mode, check if schema overwrite is allowed
                        if !iceberg_options.overwrite_schema {
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

        let resolved_partition_columns = if !partition_by.is_empty() {
            partition_by
        } else {
            existing_partition_columns.unwrap_or_default()
        };

        let table_config = IcebergTableConfig {
            table_url,
            partition_columns: resolved_partition_columns,
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

/// Create an Iceberg table provider for reading
pub async fn create_iceberg_provider(
    ctx: &dyn Session,
    table_url: Url,
    options: TableIcebergOptions,
) -> Result<Arc<dyn TableProvider>> {
    let table = Table::load(ctx, table_url).await?;
    let provider = table.to_provider(&options)?;
    Ok(Arc::new(provider))
}

/// Load metadata and pick snapshot per options (precedence: snapshot_id > ref > timestamp > current).
#[allow(dead_code)]
pub(crate) async fn load_table_metadata_with_options(
    ctx: &dyn Session,
    table_url: &Url,
    options: TableIcebergOptions,
) -> Result<(Schema, Snapshot, Vec<PartitionSpec>)> {
    log::trace!(
        "Loading table metadata (with options) from: {}, options: {:?}",
        table_url,
        options
    );
    let table = Table::load(ctx, table_url.clone()).await?;
    table.scan_state(&options)
}

impl IcebergTableFormat {
    async fn parse_table_url(paths: Vec<String>) -> Result<Url> {
        if paths.len() != 1 {
            return plan_err!(
                "Iceberg table requires exactly one path, got {}",
                paths.len()
            );
        }

        let path = &paths[0];
        let mut table_url = Url::parse(path).map_err(|e| DataFusionError::External(Box::new(e)))?;

        if !table_url.path().ends_with('/') {
            table_url.set_path(&format!("{}/", table_url.path()));
        }
        Ok(table_url)
    }

    fn partition_columns_from_metadata(table: &Table) -> Result<Vec<String>> {
        let metadata = table.metadata();
        let spec = match metadata.default_partition_spec() {
            Some(spec) => spec,
            None => return Ok(vec![]),
        };
        if spec.is_unpartitioned() {
            return Ok(vec![]);
        }

        let schema = metadata.current_schema().ok_or_else(|| {
            DataFusionError::Plan("Iceberg table metadata is missing current schema".to_string())
        })?;

        let mut columns = Vec::with_capacity(spec.fields().len());
        for field in spec.fields() {
            let col_name = schema
                .field_by_id(field.source_id)
                .map(|f| f.name.clone())
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "Partition field references unknown source column id {}",
                        field.source_id
                    ))
                })?;
            columns.push(col_name);
        }

        Ok(columns)
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
