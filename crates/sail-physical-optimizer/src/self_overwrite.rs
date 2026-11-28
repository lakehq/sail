// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::config::ConfigOptions;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::Result;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::plan_err;
use sail_data_source::listing::delete::FileDeleteExec;
use sail_physical_plan::barrier::BarrierExec;

/// A physical optimizer rule that rejects an overwrite of a file table or path
/// when the written data reads a file under the output path.
///
/// The overwrite deletes the files under the output path before the plan that produces the
/// data runs, so such a read would fail or lose data. The rule must run on the final plan,
/// so that a read the optimizer has removed (e.g. an aggregate answered by statistics)
/// is not rejected.
#[derive(Debug, Default)]
pub struct RejectSelfOverwrite;

impl PhysicalOptimizerRule for RejectSelfOverwrite {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.apply(|node| {
            if let Some(barrier) = node.downcast_ref::<BarrierExec>() {
                for precondition in barrier.preconditions() {
                    precondition.apply(|node| {
                        if let Some(delete) = node.downcast_ref::<FileDeleteExec>() {
                            check_output_not_read(barrier.plan(), delete)?;
                        }
                        Ok(TreeNodeRecursion::Continue)
                    })?;
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(plan)
    }

    fn name(&self) -> &str {
        "RejectSelfOverwrite"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn check_output_not_read(plan: &Arc<dyn ExecutionPlan>, delete: &FileDeleteExec) -> Result<()> {
    plan.apply(|node| {
        let reads_output = node
            .downcast_ref::<DataSourceExec>()
            .and_then(|scan| scan.data_source().downcast_ref::<FileScanConfig>())
            .is_some_and(|config| {
                config.object_store_url == *delete.object_store_url()
                    && config
                        .file_groups
                        .iter()
                        .flat_map(|group| group.iter())
                        .any(|file| file.object_meta.location.prefix_matches(delete.path()))
            });
        if !reads_output {
            return Ok(TreeNodeRecursion::Continue);
        }
        match delete.table() {
            Some(table) => plan_err!(
                "[UNSUPPORTED_OVERWRITE.TABLE] Can't overwrite the target that is also being read from. The target table is {table}. SQLSTATE: 42902"
            ),
            None => plan_err!(
                "[UNSUPPORTED_OVERWRITE.PATH] Can't overwrite the target that is also being read from. The target path is {}{}. SQLSTATE: 42902",
                delete.object_store_url(),
                delete.path()
            ),
        }
    })?;
    Ok(())
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::physical_plan::empty::EmptyExec;
    use object_store::path::Path;

    use super::*;

    fn scan(url: &str, files: &[&str]) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let config = files
            .iter()
            .fold(
                FileScanConfigBuilder::new(
                    ObjectStoreUrl::parse(url).unwrap(),
                    Arc::new(ParquetSource::new(schema)),
                ),
                |builder, file| builder.with_file(PartitionedFile::new(*file, 1)),
            )
            .build();
        DataSourceExec::from_data_source(config)
    }

    /// The plan shape of an overwrite: the data is written after the delete and the write
    /// has an empty result.
    fn overwrite(table: Option<&str>, input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let delete = Arc::new(FileDeleteExec::new(
            ObjectStoreUrl::local_filesystem(),
            Path::from("data/t"),
            table.map(str::to_string),
        ));
        let write = Arc::new(BarrierExec::new(vec![delete], input));
        Arc::new(BarrierExec::new(
            vec![write],
            Arc::new(EmptyExec::new(Arc::new(Schema::empty()))),
        ))
    }

    fn optimize(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        RejectSelfOverwrite.optimize(plan, &ConfigOptions::default())
    }

    #[test]
    fn rejects_reads_under_the_output_path() {
        for file in ["data/t/part-0.parquet", "data/t/p=1/part-0.parquet"] {
            let error = optimize(overwrite(
                Some("`sail`.`default`.`t`"),
                scan("file://", &[file]),
            ))
            .unwrap_err()
            .to_string();
            assert!(
                error.contains(
                    "[UNSUPPORTED_OVERWRITE.TABLE] Can't overwrite the target that is also being read from. The target table is `sail`.`default`.`t`. SQLSTATE: 42902"
                ),
                "{error}"
            );
        }
        let error = optimize(overwrite(None, scan("file://", &["data/t/part-0.parquet"])))
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("[UNSUPPORTED_OVERWRITE.PATH] Can't overwrite the target that is also being read from. The target path is file:///data/t. SQLSTATE: 42902"),
            "{error}"
        );
    }

    #[test]
    fn allows_reads_outside_the_output_path() -> Result<()> {
        for input in [
            scan(
                "file://",
                &["data/t2/part-0.parquet", "data/part-0.parquet"],
            ),
            scan("s3://bucket", &["data/t/part-0.parquet"]),
            scan("file://", &[]),
        ] {
            let plan = overwrite(None, input);
            assert!(Arc::ptr_eq(&optimize(Arc::clone(&plan))?, &plan));
        }
        Ok(())
    }
}
