// SPDX-License-Identifier: Apache-2.0

use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::common::Result;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::config::ConfigOptions;
use datafusion::datasource::physical_plan::{
    FileGroup, FileOpener, FileScanConfig, FileSource, ParquetSource,
};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalSortExpr};
use datafusion::physical_plan::filter_pushdown::FilterPushdownPropagation;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{DisplayFormatType, PhysicalExpr, SortOrderPushdownResult};
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::morsel::{MorselPlanner, Morselizer};
use object_store::ObjectStore;

use crate::parquet_schema::ParquetStorage;
use crate::schema_evolution::{
    SchemaEvolutionPhysicalExprAdapterFactoryWithMatching, StructFieldMatching,
};

/// Bind the expression adapter to the footer of the file being opened.
#[derive(Clone)]
pub struct ParquetStorageSource {
    inner: Arc<dyn FileSource>,
    matching: StructFieldMatching,
}

impl ParquetStorageSource {
    pub fn wrap_if_needed(
        inner: Arc<dyn FileSource>,
        matching: StructFieldMatching,
        groups: &[FileGroup],
    ) -> Arc<dyn FileSource> {
        if groups
            .iter()
            .flat_map(FileGroup::iter)
            .any(|f| f.extensions.get::<ParquetStorage>().is_some())
        {
            Arc::new(Self { inner, matching })
        } else {
            inner
        }
    }

    pub fn parquet_source(source: &dyn FileSource) -> Option<&ParquetSource> {
        let inner = source
            .downcast_ref::<Self>()
            .map_or(source, |s| s.inner.as_ref());
        inner.downcast_ref::<ParquetSource>()
    }

    fn wrap(&self, inner: Arc<dyn FileSource>) -> Arc<dyn FileSource> {
        Arc::new(Self {
            inner,
            matching: self.matching,
        })
    }
}

impl FileSource for ParquetStorageSource {
    fn create_file_opener(
        &self,
        store: Arc<dyn ObjectStore>,
        config: &FileScanConfig,
        partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        self.inner.create_file_opener(store, config, partition)
    }

    fn create_morselizer(
        &self,
        store: Arc<dyn ObjectStore>,
        config: &FileScanConfig,
        partition: usize,
    ) -> Result<Box<dyn Morselizer>> {
        let default = self
            .inner
            .create_morselizer(Arc::clone(&store), config, partition)?;
        // Parquet uses the scan limit and order when opening a file. File groups
        // can be large and the scheduler already supplies the individual file.
        let config =
            FileScanConfigBuilder::new(config.object_store_url.clone(), Arc::clone(&self.inner))
                .with_limit(config.limit)
                .with_preserve_order(config.preserve_order)
                .build();
        Ok(Box::new(ParquetStorageMorselizer {
            default,
            store,
            config,
            partition,
            matching: self.matching,
        }))
    }

    fn table_schema(&self) -> &TableSchema {
        self.inner.table_schema()
    }

    fn with_batch_size(&self, size: usize) -> Arc<dyn FileSource> {
        self.wrap(self.inner.with_batch_size(size))
    }

    fn filter(&self) -> Option<Arc<dyn PhysicalExpr>> {
        self.inner.filter()
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        self.inner.projection()
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        self.inner.metrics()
    }

    fn file_type(&self) -> &str {
        self.inner.file_type()
    }

    fn fmt_extra(&self, ty: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        self.inner.fmt_extra(ty, f)
    }

    fn reorder_files(&self, files: Vec<PartitionedFile>) -> Vec<PartitionedFile> {
        self.inner.reorder_files(files)
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        Ok(self
            .inner
            .try_pushdown_projection(projection)?
            .map(|s| self.wrap(s)))
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        let mut result = self.inner.try_pushdown_filters(filters, config)?;
        result.updated_node = result.updated_node.map(|s| self.wrap(s));
        Ok(result)
    }

    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
        eq: &EquivalenceProperties,
    ) -> Result<SortOrderPushdownResult<Arc<dyn FileSource>>> {
        Ok(self
            .inner
            .try_pushdown_sort(order, eq)?
            .map(|s| self.wrap(s)))
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.inner.apply_expressions(f)
    }
}

#[derive(Debug)]
struct ParquetStorageMorselizer {
    default: Box<dyn Morselizer>,
    store: Arc<dyn ObjectStore>,
    config: FileScanConfig,
    partition: usize,
    matching: StructFieldMatching,
}

impl Morselizer for ParquetStorageMorselizer {
    fn plan_file(&self, file: PartitionedFile) -> Result<Box<dyn MorselPlanner>> {
        let Some(storage) = file.extensions.get::<ParquetStorage>() else {
            return self.default.plan_file(file);
        };
        let mut config = self.config.clone();
        config.expr_adapter_factory = Some(Arc::new(
            SchemaEvolutionPhysicalExprAdapterFactoryWithMatching::for_parquet_file(
                self.matching,
                Arc::clone(&storage.0),
            ),
        ));
        config
            .file_source
            .create_morselizer(Arc::clone(&self.store), &config, self.partition)?
            .plan_file(file)
    }
}
