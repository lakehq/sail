// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_plan::apply_expression_roots;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::projection::{ProjectionOpener, SplitProjection};
use datafusion_datasource::{PartitionedFile, TableSchema};
use futures::StreamExt;
use object_store::path::Path;
use object_store::{GetResult, ObjectStore, ObjectStoreExt};
use percent_encoding::percent_decode_str;

use crate::formats::binary::reader::{BinaryFileMetadata, BinaryFileReader};

#[derive(Debug, Clone)]
pub struct BinarySource {
    table_schema: TableSchema,
    batch_size: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
    projection: SplitProjection,
}

impl BinarySource {
    pub fn new(table_schema: impl Into<TableSchema>) -> Self {
        let table_schema = table_schema.into();
        Self {
            projection: SplitProjection::unprojected(&table_schema),
            table_schema,
            batch_size: None,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl From<BinarySource> for Arc<dyn FileSource> {
    fn from(source: BinarySource) -> Self {
        Arc::new(source)
    }
}

impl FileSource for BinarySource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        let opener = Arc::new(BinaryOpener::new(
            Arc::new(self.clone()),
            object_store,
            base_config.object_store_url.clone(),
        )) as Arc<dyn FileOpener>;

        let opener = ProjectionOpener::try_new(
            self.projection.clone(),
            opener,
            self.table_schema.file_schema(),
        )?;

        Ok(opener)
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.batch_size = Some(batch_size);
        Arc::new(conf)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let mut source = self.clone();
        let new_projection = self.projection.source.try_merge(projection)?;
        source.projection = SplitProjection::new(self.table_schema.file_schema(), &new_projection);
        Ok(Some(Arc::new(source)))
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection.source)
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        apply_expression_roots(self.projection.source.iter(), f)
    }

    fn file_type(&self) -> &str {
        "binary"
    }

    fn supports_repartitioning(&self) -> bool {
        // Each file is read whole into one row, so a byte-range split would duplicate the row.
        false
    }
}

pub struct BinaryOpener {
    config: Arc<BinarySource>,
    object_store: Arc<dyn ObjectStore>,
    object_store_url: ObjectStoreUrl,
}

impl BinaryOpener {
    pub fn new(
        config: Arc<BinarySource>,
        object_store: Arc<dyn ObjectStore>,
        object_store_url: ObjectStoreUrl,
    ) -> Self {
        Self {
            config,
            object_store,
            object_store_url,
        }
    }
}

/// Format a file location as Hadoop's `Path.toString` does, such as `file:/tmp/a b.bin`:
/// the authority only when it is not empty, and decoded path segments.
fn hadoop_path_string(object_store_url: &ObjectStoreUrl, location: &Path) -> String {
    let url: &url::Url = object_store_url.as_ref();
    let mut path = format!("{}:", url.scheme());
    if !url.authority().is_empty() {
        path.push_str("//");
        path.push_str(url.authority());
    }
    for part in location.parts() {
        path.push('/');
        path.push_str(&percent_decode_str(part.as_ref()).decode_utf8_lossy());
    }
    if location.parts().next().is_none() {
        path.push('/');
    }
    path
}

impl FileOpener for BinaryOpener {
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        let store = Arc::clone(&self.object_store);
        let location = file.object_meta.location.clone();
        let path = hadoop_path_string(&self.object_store_url, &location);
        let last_modified = file.object_meta.last_modified;
        let size = file.object_meta.size as i64;
        let schema = Arc::new(
            self.config
                .table_schema
                .file_schema()
                .project(&self.config.projection.file_indices)?,
        );

        Ok(Box::pin(async move {
            let get_result: GetResult = store.get(&location).await?;
            let content = get_result.bytes().await?;
            let modification_time = last_modified.timestamp_micros();
            let metadata = BinaryFileMetadata {
                path,
                modification_time,
                length: size,
            };
            // `content.into()` moves `Bytes` into `Vec<u8>` without copy.
            // `content.to_vec()` would copy the data since this is a method on the slice.
            let reader = BinaryFileReader::new(metadata, content.into(), schema.clone());

            let stream = futures::stream::once(async move {
                let batch = reader.read().map_err(DataFusionError::from)?;
                Ok(batch)
            })
            .boxed();

            Ok(stream)
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hadoop_path_string() -> Result<()> {
        let name = "a b%c#d\u{e9}.bin";
        let location = Path::from_iter(["tmp", "t", name]);
        for (store, expected) in [
            ("file:///", format!("file:/tmp/t/{name}")),
            ("s3://bucket/", format!("s3://bucket/tmp/t/{name}")),
            ("memory:///", format!("memory:/tmp/t/{name}")),
        ] {
            let store = ObjectStoreUrl::parse(store)?;
            assert_eq!(hadoop_path_string(&store, &location), expected);
        }
        let root = hadoop_path_string(&ObjectStoreUrl::parse("file:///")?, &Path::default());
        assert_eq!(root, "file:/");
        Ok(())
    }
}
