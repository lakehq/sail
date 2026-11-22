// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::{Result, exec_err, internal_err};
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use uuid::Uuid;

use crate::listing::part_file::parse_part_file_name;

/// Deletes object-store files under a listing table output path.
/// All files are deleted, except for the data files of the write
/// identified by `keep_write_id` when it is set.
#[derive(Debug, Clone)]
pub struct FileDeleteExec {
    object_store_url: ObjectStoreUrl,
    path: Path,
    keep_write_id: Option<Uuid>,
    properties: Arc<PlanProperties>,
}

impl FileDeleteExec {
    pub fn new(object_store_url: ObjectStoreUrl, path: Path, keep_write_id: Option<Uuid>) -> Self {
        let schema = Arc::new(Schema::empty());
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            object_store_url,
            path,
            keep_write_id,
            properties,
        }
    }

    pub fn object_store_url(&self) -> &ObjectStoreUrl {
        &self.object_store_url
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn keep_write_id(&self) -> Option<&Uuid> {
        self.keep_write_id.as_ref()
    }
}

impl DisplayAs for FileDeleteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}: path={}", self.name(), self.path)
    }
}

impl ExecutionPlan for FileDeleteExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    #[expect(deprecated)]
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: datafusion::physical_plan::ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_new_children(children)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return internal_err!("{} should not have children", self.name());
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return exec_err!(
                "{} expects only partition 0 but got {}",
                self.name(),
                partition
            );
        }
        let object_store_url = self.object_store_url.clone();
        let path = self.path.clone();
        let keep_write_id = self.keep_write_id;
        let stream = futures::stream::once(async move {
            let store = context.runtime_env().object_store(&object_store_url)?;
            let files = store
                .list(Some(&path))
                .map_ok(|meta| meta.location)
                .try_filter(move |location| {
                    // The files of the current write may already exist
                    // since the writers can run before the deletion finishes.
                    let keep = keep_write_id.is_some_and(|keep| {
                        location
                            .filename()
                            .and_then(parse_part_file_name)
                            .is_some_and(|name| name.write_id == keep)
                    });
                    futures::future::ready(!keep)
                })
                .boxed();
            store
                .delete_stream(files)
                .try_for_each(|_| futures::future::ready(Ok(())))
                .await?;
            Ok(RecordBatch::new_empty(Arc::new(Schema::empty())))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use datafusion::prelude::SessionContext;
    use object_store::memory::InMemory;
    use object_store::{ObjectStore, ObjectStoreExt};
    use url::Url;

    use super::*;
    use crate::listing::part_file::part_file_name;

    async fn run_delete(
        files: &[String],
        path: &str,
        keep_write_id: Option<Uuid>,
    ) -> Result<Vec<String>> {
        let ctx = SessionContext::new();
        let store = Arc::new(InMemory::new());
        let url = Url::parse("memory://")
            .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;
        ctx.register_object_store(&url, store.clone());
        for file in files {
            store
                .put(&Path::from(file.as_str()), Bytes::from_static(b"x").into())
                .await?;
        }
        let plan = FileDeleteExec::new(
            ObjectStoreUrl::parse("memory://")?,
            Path::from(path),
            keep_write_id,
        );
        let batches: Vec<_> = plan.execute(0, ctx.task_ctx())?.try_collect().await?;
        assert_eq!(batches.len(), 1);
        let mut remaining: Vec<_> = store
            .list(None)
            .map_ok(|meta| meta.location.to_string())
            .try_collect()
            .await?;
        remaining.sort();
        Ok(remaining)
    }

    fn test_files(write_id: &Uuid, other: &Uuid) -> Vec<String> {
        vec![
            format!("t/{}", part_file_name(0, write_id, 0, 0, ".parquet")),
            format!("t/{}", part_file_name(1, write_id, 1, 2, ".zstd.parquet")),
            format!(
                "t/a=1/b=2/{}",
                part_file_name(3, write_id, 0, 0, ".parquet")
            ),
            format!("t/{}", part_file_name(0, other, 0, 0, ".parquet")),
            format!("t/a=1/b=2/{}", part_file_name(3, other, 0, 0, ".parquet")),
            "t/a1B2c3D4e5F6g7H8_0.zst.parquet".to_string(),
            "t/_SUCCESS".to_string(),
            "t/.hidden".to_string(),
            "t/data.json".to_string(),
            "t/a=1/data.csv".to_string(),
            format!("t/x-{}", part_file_name(0, write_id, 0, 0, ".parquet")),
            format!("other/{}", part_file_name(0, other, 0, 0, ".parquet")),
        ]
    }

    #[tokio::test]
    async fn test_delete_all_files() -> Result<()> {
        let write_id = Uuid::from_u128(1);
        let other = Uuid::from_u128(2);
        let remaining = run_delete(&test_files(&write_id, &other), "t", None).await?;
        assert_eq!(
            remaining,
            vec![format!(
                "other/{}",
                part_file_name(0, &other, 0, 0, ".parquet")
            )]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_delete_keeps_files_of_write() -> Result<()> {
        let write_id = Uuid::from_u128(1);
        let other = Uuid::from_u128(2);
        let remaining = run_delete(&test_files(&write_id, &other), "t", Some(write_id)).await?;
        let mut expected = vec![
            format!("other/{}", part_file_name(0, &other, 0, 0, ".parquet")),
            format!("t/{}", part_file_name(0, &write_id, 0, 0, ".parquet")),
            format!("t/{}", part_file_name(1, &write_id, 1, 2, ".zstd.parquet")),
            format!(
                "t/a=1/b=2/{}",
                part_file_name(3, &write_id, 0, 0, ".parquet")
            ),
        ];
        expected.sort();
        assert_eq!(remaining, expected);
        Ok(())
    }
}
