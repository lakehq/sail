use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result;
use sail_common::storage::StorageAccessSpec;
use sail_physical_plan::storage_access::StorageAccessExec;

use crate::physical_plan::{
    DeletionVectorRowsWriterExec, DeletionVectorWriterExec, DeltaCommitExec, DeltaDiscoveryExec,
    DeltaLogReplayExec, DeltaMetadataStatsExec, DeltaScanByAddsExec, DeltaWriterExec,
};

pub(crate) fn bind_storage(
    plan: Arc<dyn ExecutionPlan>,
    spec: &StorageAccessSpec,
    runtime: &Arc<RuntimeEnv>,
    inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(plan
        .transform_down(|node| {
            if node.is::<StorageAccessExec>()
                || inputs.iter().any(|input| Arc::ptr_eq(input, &node))
            {
                return Ok(Transformed::new(node, false, TreeNodeRecursion::Jump));
            }
            if node.is::<DataSourceExec>()
                || node.is::<DeltaDiscoveryExec>()
                || node.is::<DeltaLogReplayExec>()
                || node.is::<DeltaScanByAddsExec>()
                || node.is::<DeltaMetadataStatsExec>()
                || node.is::<DeltaWriterExec>()
                || node.is::<DeltaCommitExec>()
                || node.is::<DeletionVectorWriterExec>()
                || node.is::<DeletionVectorRowsWriterExec>()
            {
                // Bind descendants before wrapping so every distributed stage retains its scope.
                let node = node
                    .map_children(|child| {
                        bind_storage(child, spec, runtime, inputs).map(Transformed::yes)
                    })?
                    .data;
                return Ok(Transformed::new(
                    Arc::new(StorageAccessExec::new(node, spec.clone(), runtime.clone()))
                        as Arc<dyn ExecutionPlan>,
                    true,
                    TreeNodeRecursion::Jump,
                ));
            }
            Ok(Transformed::no(node))
        })?
        .data)
}
