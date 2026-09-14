use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result;
use sail_common::storage::StorageAccessSpec;
use sail_physical_plan::storage_access::StorageAccessExec;

use crate::physical_plan::commit::commit_exec::IcebergCommitExec;
use crate::physical_plan::delete_apply_exec::IcebergDeleteApplyExec;
use crate::physical_plan::discovery_exec::IcebergDiscoveryExec;
use crate::physical_plan::manifest_scan_exec::IcebergManifestScanExec;
use crate::physical_plan::merge_metadata_exec::IcebergMergeMetadataExec;
use crate::physical_plan::scan_by_data_files_exec::IcebergScanByDataFilesExec;
use crate::physical_plan::{IcebergEqualityDeleteWriterExec, IcebergWriterExec};

pub(crate) fn bind_scan_storage(
    plan: Arc<dyn ExecutionPlan>,
    spec: &StorageAccessSpec,
    runtime: &Arc<RuntimeEnv>,
) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(plan
        .transform_up(|node| {
            if node.is::<DataSourceExec>()
                || node.is::<IcebergDeleteApplyExec>()
                || node.is::<IcebergDiscoveryExec>()
                || node.is::<IcebergManifestScanExec>()
                || node.is::<IcebergMergeMetadataExec>()
                || node.is::<IcebergScanByDataFilesExec>()
            {
                Ok(Transformed::yes(Arc::new(StorageAccessExec::new(
                    node,
                    spec.clone(),
                    runtime.clone(),
                ))
                    as Arc<dyn ExecutionPlan>))
            } else {
                Ok(Transformed::no(node))
            }
        })?
        .data)
}

pub(crate) fn bind_write_storage(
    plan: Arc<dyn ExecutionPlan>,
    spec: &StorageAccessSpec,
    runtime: &Arc<RuntimeEnv>,
) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(plan
        .transform_up(|node| {
            if node.is::<IcebergWriterExec>()
                || node.is::<IcebergCommitExec>()
                || node.is::<IcebergEqualityDeleteWriterExec>()
            {
                Ok(Transformed::yes(Arc::new(StorageAccessExec::new(
                    node,
                    spec.clone(),
                    runtime.clone(),
                ))
                    as Arc<dyn ExecutionPlan>))
            } else {
                Ok(Transformed::no(node))
            }
        })?
        .data)
}
