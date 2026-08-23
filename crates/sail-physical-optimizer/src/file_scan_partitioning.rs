use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use sail_physical_plan::file_scan_partitioning::FileScanPartitioningFenceExec;

/// Removes the optimizer-only file scan partitioning fence after distribution enforcement.
#[derive(Debug, Default)]
pub struct RemoveFileScanPartitioningFence;

impl RemoveFileScanPartitioningFence {
    pub fn new() -> Self {
        Self
    }
}

impl PhysicalOptimizerRule for RemoveFileScanPartitioningFence {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(plan
            .transform_up(|node| {
                if let Some(fence) = node.downcast_ref::<FileScanPartitioningFenceExec>() {
                    Ok(Transformed::yes(fence.input().clone()))
                } else {
                    Ok(Transformed::no(node))
                }
            })?
            .data)
    }

    fn name(&self) -> &str {
        "RemoveFileScanPartitioningFence"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::{
        FileGroup, FileScanConfig, FileScanConfigBuilder, ParquetSource,
    };
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::physical_optimizer::enforce_distribution::EnforceDistribution;

    use super::*;

    fn file_group_count(plan: &Arc<dyn ExecutionPlan>) -> usize {
        let scan = plan.downcast_ref::<DataSourceExec>().unwrap();
        let config = scan.data_source().downcast_ref::<FileScanConfig>().unwrap();
        config.file_groups.len()
    }

    #[test]
    fn fence_blocks_file_scan_repartitioning_until_removed() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(ParquetSource::new(schema)),
        )
        .with_file_group(FileGroup::new(vec![PartitionedFile::new(
            "file.parquet",
            100,
        )]))
        .build();
        let scan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);

        let mut optimizer_config = ConfigOptions::default();
        optimizer_config.execution.target_partitions = 4;
        optimizer_config.optimizer.repartition_file_scans = true;
        optimizer_config.optimizer.repartition_file_min_size = 0;

        let Some(repartitioned) = scan.repartitioned(4, &optimizer_config)? else {
            return datafusion::common::internal_err!(
                "the unprotected Parquet scan should be repartitionable"
            );
        };
        assert_eq!(file_group_count(&repartitioned), 4);

        let fenced: Arc<dyn ExecutionPlan> = Arc::new(FileScanPartitioningFenceExec::new(scan));

        let optimized = EnforceDistribution::new().optimize(fenced, &optimizer_config)?;
        let fence = optimized
            .downcast_ref::<FileScanPartitioningFenceExec>()
            .unwrap();
        assert_eq!(file_group_count(fence.input()), 1);

        let optimized =
            RemoveFileScanPartitioningFence::new().optimize(optimized, &optimizer_config)?;
        assert_eq!(file_group_count(&optimized), 1);
        Ok(())
    }
}
