// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::datasource::empty::EmptyTable;
use datafusion::datasource::provider_as_source;
use datafusion_common::{ScalarValue, TableReference};
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder, TableSource, Union};
use sail_catalog::manager::CatalogManager;
use sail_common_datafusion::datasource::{
    DataSourceRegistry, RegisteredPartition, SourceInfo, get_partition_columns_and_file_schema,
    is_lakehouse_format,
};
use sail_common_datafusion::rename::logical_plan::rename_logical_plan;
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
        info: SourceInfo,
    ) -> PlanResult<Vec<Arc<dyn TableSource>>> {
        let partitions = if !info.partition_by.is_empty() && !is_lakehouse_format(format) {
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
            return Ok(vec![provider_as_source(Arc::new(EmptyTable::new(
                Arc::new(schema.clone()),
            )))]);
        }
        let (fields, _) = get_partition_columns_and_file_schema(schema, info.partition_by.clone())?;
        // Each file scan uses one reader and one object store. Most tables need only one scan.
        let mut groups = BTreeMap::new();
        for partition in partitions {
            let location = partition.location.as_deref().ok_or_else(|| {
                PlanError::invalid("catalog partition is missing its storage location")
            })?;
            let url = parse_absolute_directory_url(location).ok_or_else(|| {
                PlanError::invalid(format!("invalid catalog partition location: {location}"))
            })?;
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
                    if value == "__HIVE_DEFAULT_PARTITION__" {
                        ScalarValue::try_from(field.data_type())
                    } else {
                        ScalarValue::try_from_string(value, field.data_type())
                    }
                })
                .collect::<datafusion_common::Result<Vec<_>>>()?;
            groups
                .entry((
                    partition.format.unwrap_or_else(|| format.to_string()),
                    url[..Position::BeforePath].to_string(),
                ))
                .or_insert_with(Vec::new)
                .push(RegisteredPartition {
                    location: url,
                    values,
                });
        }
        let mut scans = Vec::with_capacity(groups.len());
        for ((format, _), partitions) in groups {
            let source = registry
                .get_data_source(&format)?
                .create_partitioned_source(&ctx, info.clone(), partitions)
                .await?;
            scans.push(source);
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
