// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion_common::TableReference;
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder, TableSource, Union};
use sail_catalog::hive_format::HiveStorageFormat;
use sail_catalog::manager::CatalogManager;
use sail_common_datafusion::datasource::{
    DataSourceRegistry, OptionLayer, RegisteredPartition, SourceInfo,
    get_partition_columns_and_file_schema,
};
use sail_common_datafusion::rename::logical_plan::rename_logical_plan;
use sail_data_source::listing::{RegisteredTableSource, cast_catalog_partition_value};
use sail_data_source::parse_absolute_directory_url;
use url::Position;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) async fn create_catalog_table_sources(
        &self,
        manager: &CatalogManager,
        registry: &DataSourceRegistry,
        reference: &[String],
        format: &str,
        file_read_options: Vec<(String, String)>,
        info: SourceInfo,
    ) -> PlanResult<Vec<Arc<dyn TableSource>>> {
        // A registered non-file provider owns its reads. An unsupported Hive file format
        // can still have readable partitions with explicit storage descriptors.
        let partitions = if !info.partition_by.is_empty()
            && registry.get_data_source_if_registered(format)?.map_or_else(
                || HiveStorageFormat::from_format(format).is_ok(),
                |s| s.supports_registered_partitions(),
            ) {
            manager
                .file_partitions_for_scan(reference, info.partition_by.len())
                .await?
        } else {
            None
        };
        let ctx = self.ctx.state();
        let Some(partitions) = partitions else {
            return Ok(vec![
                registry
                    .get_data_source(format)?
                    .create_source(&ctx, info)
                    .await?,
            ]);
        };

        let schema = info
            .schema
            .as_ref()
            .ok_or_else(|| PlanError::internal("missing catalog schema"))?;
        if partitions.is_empty() {
            return Ok(vec![Arc::new(RegisteredTableSource::try_new(
                format.to_string(),
                info,
                vec![],
            )?)]);
        }
        let (fields, _) = get_partition_columns_and_file_schema(schema, info.partition_by.clone())?;
        // Each file scan uses one reader and one object store. Most tables need only one scan.
        let mut groups = BTreeMap::new();
        for partition in partitions {
            let store = partition
                .location
                .as_deref()
                .and_then(parse_absolute_directory_url)
                .map(|url| url[..Position::BeforePath].to_string());
            if partition.values.len() != fields.len() {
                return Err(PlanError::invalid(
                    "catalog partition value count does not match its columns",
                ));
            }
            let values = partition
                .values
                .into_iter()
                .zip(&fields)
                .map(|(value, field)| {
                    cast_catalog_partition_value(
                        value,
                        field.data_type(),
                        self.config.ansi_mode,
                        &self.config.session_timezone,
                    )
                })
                .collect::<datafusion_common::Result<Vec<_>>>()?;
            groups
                .entry((
                    partition.format.resolve(format),
                    store,
                    partition.read_options,
                ))
                .or_insert_with(Vec::new)
                .push(RegisteredPartition {
                    location: partition.location,
                    values,
                });
        }
        let mut scans = Vec::with_capacity(groups.len());
        for ((partition_format, _, read_options), partitions) in groups {
            let mut info = info.clone();
            // Partition storage overrides table properties. Explicit query options come last.
            let index = info
                .options
                .partition_point(|layer| matches!(layer, OptionLayer::TablePropertyList { .. }));
            info.options.insert(
                index,
                OptionLayer::OptionList {
                    items: read_options,
                },
            );
            if partition_format == format && !file_read_options.is_empty() {
                info.options.insert(
                    0,
                    OptionLayer::OptionList {
                        items: file_read_options.clone(),
                    },
                );
            }
            let source = RegisteredTableSource::try_new(partition_format, info, partitions)?;
            scans.push(Arc::new(source) as Arc<dyn TableSource>);
        }
        Ok(scans)
    }

    pub(super) fn resolve_catalog_table_scans(
        &self,
        sources: Vec<Arc<dyn TableSource>>,
        reference: TableReference,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        if let [source] = sources.as_slice() {
            return self.resolve_table_source_with_rename(
                Arc::clone(source),
                reference,
                None,
                vec![],
                None,
                state,
            );
        }
        let inputs = sources
            .into_iter()
            .map(|source| {
                LogicalPlanBuilder::scan(reference.clone(), source, None)?
                    .build()
                    .map(Arc::new)
            })
            .collect::<datafusion_common::Result<Vec<_>>>()?;
        let plan = LogicalPlanBuilder::from(LogicalPlan::Union(Union::try_new(inputs)?))
            .alias(reference)?
            .build()?;
        let names = state.register_fields(plan.schema().fields());
        let plan = rename_logical_plan(plan, &names)?;
        state.register_missing_input_boundary(&plan);
        Ok(plan)
    }
}
