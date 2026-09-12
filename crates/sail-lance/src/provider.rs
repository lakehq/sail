// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! The DataFusion table provider backed by a Lance dataset.

use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::logical_expr::{TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result;
use datafusion_expr::Expr;
use lance::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance_table::format::Fragment;

use crate::exec::{LanceScanConfig, LanceScanExec, lance_error};
use crate::filter::is_scan_filter;
use crate::options::{DatasetRef, LanceReadOptions};
use crate::uri;

/// Columns a Lance scan produces from its options rather than from the dataset
/// schema, and which therefore must not be pushed into a column projection.
const METADATA_COLUMNS: &[&str] = &["_rowid", "_rowaddr", "_distance", "_score"];

#[derive(Debug)]
pub struct LanceTableProvider {
    uri: String,
    schema: SchemaRef,
    dataset: Arc<Dataset>,
    read_options: LanceReadOptions,
}

impl LanceTableProvider {
    /// Opens an existing dataset for reading.
    pub async fn try_open(uri: &str, read_options: LanceReadOptions) -> Result<Self> {
        let dataset = open_dataset(uri, &read_options).await?;
        // Asking the scanner for its schema, rather than converting the dataset
        // schema, accounts for the columns the read options add.
        let mut scanner = dataset.scan();
        if read_options.with_row_id {
            scanner.with_row_id();
        }
        if let Some(nearest) = &read_options.nearest {
            let query = arrow::array::Float32Array::from(nearest.query.clone());
            scanner
                .nearest(&nearest.column, &query, nearest.k)
                .map_err(lance_error)?;
        }
        let schema = scanner.schema().await.map_err(lance_error)?;
        Ok(Self {
            uri: uri.to_string(),
            schema,
            dataset: Arc::new(dataset),
            read_options,
        })
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// Returns whether a Lance scan can evaluate `expr` for this dataset.
    ///
    /// Lance binds the expression against the dataset schema, so it has the
    /// final word: an expression it cannot bind stays with DataFusion.
    fn is_pushable(&self, expr: &Expr) -> bool {
        if !is_scan_filter(expr) {
            return false;
        }
        let mut scanner = self.dataset.scan();
        scanner.filter_expr(expr.clone());
        scanner.get_expr_filter().is_ok()
    }
}

#[async_trait]
impl TableProvider for LanceTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let output_schema = match projection {
            Some(projection) => Arc::new(self.schema.project(projection)?),
            None => Arc::clone(&self.schema),
        };
        let columns = scan_columns(&output_schema, &self.schema);
        let config = LanceScanConfig {
            columns,
            filters: filters.to_vec(),
            limit,
            options: self.read_options.clone(),
        };
        let partitions = partition_fragments(
            &self.dataset,
            state.config().target_partitions(),
            self.read_options.nearest.is_some(),
        );
        Ok(Arc::new(LanceScanExec::new(
            Arc::clone(&self.dataset),
            config,
            partitions,
            output_schema,
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| {
                if self.is_pushable(filter) {
                    // Lance evaluates the same DataFusion expression with the
                    // same DataFusion version, so the scan output needs no
                    // second filter above it.
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
}

async fn open_dataset(uri: &str, options: &LanceReadOptions) -> Result<Dataset> {
    let uri = uri::normalize(uri)?;
    let mut builder = DatasetBuilder::from_uri(&uri);
    match &options.version {
        Some(DatasetRef::Version(version)) => builder = builder.with_version(*version),
        Some(DatasetRef::Tag(tag)) => builder = builder.with_tag(tag),
        None => {}
    }
    builder.load().await.map_err(lance_error)
}

/// Returns the dataset columns a scan has to read for `output_schema`.
///
/// An empty projection, which is what `SELECT count(*)` asks for, still has to
/// read something, so the narrowest available column is used.
fn scan_columns(output_schema: &Schema, table_schema: &Schema) -> Vec<String> {
    let columns = output_schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .filter(|name| !METADATA_COLUMNS.contains(&name.as_str()))
        .collect::<Vec<_>>();
    if !columns.is_empty() {
        return columns;
    }
    table_schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .find(|name| !METADATA_COLUMNS.contains(&name.as_str()))
        .into_iter()
        .collect()
}

/// Assigns the dataset fragments to output partitions.
///
/// A vector element is the fragment list of one partition; an empty list means
/// "the whole dataset", which is what a single partition scans.
fn partition_fragments(
    dataset: &Dataset,
    target_partitions: usize,
    is_vector_search: bool,
) -> Vec<Vec<Fragment>> {
    // A vector search returns the globally nearest rows, which cannot be
    // assembled from independent per-fragment scans.
    if is_vector_search || target_partitions <= 1 {
        return vec![vec![]];
    }
    let fragments = dataset
        .get_fragments()
        .iter()
        .map(|fragment| fragment.metadata().clone())
        .collect::<Vec<_>>();
    if fragments.len() <= 1 {
        return vec![vec![]];
    }
    let partitions = target_partitions.min(fragments.len());
    let mut assignment = vec![Vec::new(); partitions];
    for (index, fragment) in fragments.into_iter().enumerate() {
        assignment[index % partitions].push(fragment);
    }
    assignment
}
