// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::catalog::Session;
use datafusion::common::scalar::ScalarValue;
use datafusion::common::stats::{ColumnStatistics, Precision, Statistics};
use datafusion::common::{plan_err, Result, ToDFSchema};
use datafusion::config::TableParquetOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{
    BinaryExpr, Expr, LogicalPlan, Operator, TableProviderFilterPushDown,
};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use object_store::ObjectMeta;
use url::Url;

use crate::datasource::expr_adapter::IcebergPhysicalExprAdapterFactory;
use crate::datasource::expressions::simplify_expr;
use crate::datasource::pruning::{prune_files, prune_manifests_by_partition_summaries};
use crate::datasource::type_converter::iceberg_schema_to_arrow;
use crate::io::{
    load_manifest as io_load_manifest, load_manifest_list as io_load_manifest_list, StoreContext,
};
use crate::physical_plan::delete_apply_exec::IcebergDeleteApplyExec;
use crate::physical_plan::discovery_exec::IcebergDiscoveryExec;
use crate::physical_plan::manifest_scan_exec::IcebergManifestScanExec;
use crate::spec::delete_index::{DeleteFileIndex, DeleteFileRef};
use crate::spec::transform::Transform;
use crate::spec::types::values::Literal;
use crate::spec::{
    DataFile, ManifestContentType, ManifestList, ManifestStatus, PartitionSpec, Schema, Snapshot,
};
use crate::utils::conversions::primitive_to_scalar_default;
use crate::utils::get_object_store_from_session;

/// Iceberg table provider for DataFusion
#[derive(Debug)]
pub struct IcebergTableProvider {
    /// The table location (URI)
    table_uri: String,
    /// The current schema of the table
    schema: Schema,
    /// The current snapshot of the table
    snapshot: Snapshot,
    /// All partition specs referenced by the table
    partition_specs: Vec<PartitionSpec>,
    /// Default partition spec id (for schema ordering / partition metadata)
    #[expect(unused)]
    default_spec_id: i32,
    /// Arrow schema for DataFusion
    arrow_schema: Arc<ArrowSchema>,
    /// Whether to use the metadata-as-data read path (lazy manifest scanning)
    metadata_as_data_read: bool,
}

impl IcebergTableProvider {
    /// Create a new Iceberg table provider
    pub fn new(
        table_uri: impl ToString,
        schema: Schema,
        snapshot: Snapshot,
        partition_specs: Vec<PartitionSpec>,
        default_spec_id: i32,
    ) -> Result<Self> {
        let table_uri_str = table_uri.to_string();
        log::trace!("Creating table provider for: {}", table_uri_str);

        let arrow_schema = iceberg_schema_to_arrow(&schema).map_err(|e| {
            log::trace!("Failed to convert schema to Arrow: {:?}", e);
            e
        })?;
        let arrow_schema = Arc::new(Self::reorder_arrow_schema_for_identity_partitions(
            &schema,
            &partition_specs,
            default_spec_id,
            &arrow_schema,
        ));

        log::trace!(
            "Converted schema to Arrow with {} fields",
            arrow_schema.fields().len()
        );

        Ok(Self {
            table_uri: table_uri_str,
            schema,
            snapshot,
            partition_specs,
            default_spec_id,
            arrow_schema,
            metadata_as_data_read: false,
        })
    }

    /// Set whether to use the metadata-as-data read path.
    pub fn with_metadata_as_data_read(mut self, enabled: bool) -> Self {
        self.metadata_as_data_read = enabled;
        self
    }

    fn reorder_arrow_schema_for_identity_partitions(
        schema: &Schema,
        partition_specs: &[PartitionSpec],
        default_spec_id: i32,
        arrow_schema: &ArrowSchema,
    ) -> ArrowSchema {
        // BDD scenarios expect "data columns" first and identity-partition columns last (in spec order),
        // but only for identity-only partition specs. For mixed transform specs (e.g. `years(x), y`)
        // we keep the original schema order.
        let Some(spec) = partition_specs
            .iter()
            .find(|s| s.spec_id() == default_spec_id)
        else {
            return arrow_schema.clone();
        };
        if spec
            .fields()
            .iter()
            .any(|pf| !matches!(pf.transform, Transform::Identity))
        {
            return arrow_schema.clone();
        }

        let mut identity_cols: Vec<String> = Vec::new();
        for pf in spec.fields().iter() {
            if matches!(pf.transform, Transform::Identity) {
                if let Some(field) = schema.field_by_id(pf.source_id) {
                    identity_cols.push(field.name.clone());
                }
            }
        }
        if identity_cols.is_empty() {
            return arrow_schema.clone();
        }

        let identity_set: std::collections::HashSet<&str> =
            identity_cols.iter().map(|s| s.as_str()).collect();
        let mut out_fields: Vec<datafusion::arrow::datatypes::FieldRef> = Vec::new();

        // Keep non-partition columns in original order.
        for f in arrow_schema.fields().iter() {
            if !identity_set.contains(f.name().as_str()) {
                out_fields.push(Arc::new((**f).clone()));
            }
        }
        // Append identity partition columns in spec order.
        for name in identity_cols {
            if let Ok(idx) = arrow_schema.index_of(&name) {
                out_fields.push(Arc::new(arrow_schema.field(idx).clone()));
            }
        }

        ArrowSchema::new(out_fields)
    }

    /// Get the table URI
    pub fn table_uri(&self) -> &str {
        &self.table_uri
    }

    /// Get the Iceberg schema
    pub fn iceberg_schema(&self) -> &Schema {
        &self.schema
    }

    /// Get the current snapshot
    pub fn current_snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    /// Load manifest list from snapshot
    async fn load_manifest_list(&self, store_ctx: &StoreContext) -> Result<ManifestList> {
        let manifest_list_str = self.snapshot.manifest_list();
        log::trace!("Manifest list path: {}", manifest_list_str);
        let ml = io_load_manifest_list(store_ctx, manifest_list_str).await?;
        Ok(ml)
    }

    /// Load data files from manifests, preserving per-file data sequence numbers.
    async fn load_data_files_with_seq(
        &self,
        session: &dyn Session,
        filters: &[Expr],
        store_ctx: &StoreContext,
        manifest_list: &ManifestList,
    ) -> Result<Vec<(DataFile, i64)>> {
        let spec_map: HashMap<i32, PartitionSpec> = self
            .partition_specs
            .iter()
            .map(|s| (s.spec_id(), s.clone()))
            .collect();
        let manifest_files =
            prune_manifests_by_partition_summaries(manifest_list, &self.schema, &spec_map, filters);

        let mut out: Vec<(DataFile, i64)> = Vec::new();
        for manifest_file in manifest_files {
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }

            let manifest_path_str = manifest_file.manifest_path.as_str();
            log::trace!("Loading manifest: {}", manifest_path_str);
            let manifest = io_load_manifest(store_ctx, manifest_path_str).await?;

            let partition_spec_id = manifest_file.partition_spec_id;
            let parent_seq = manifest_file.sequence_number;

            // Collect (DataFile, seq) pairs preserving inheritance.
            let mut manifest_pairs: Vec<(DataFile, i64)> = Vec::new();
            for entry_ref in manifest.entries().iter() {
                let entry = entry_ref.as_ref();
                if !matches!(
                    entry.status,
                    ManifestStatus::Added | ManifestStatus::Existing
                ) {
                    continue;
                }
                let mut df = entry.data_file.clone();
                df.partition_spec_id = partition_spec_id;
                let seq = entry.sequence_number.unwrap_or(parent_seq);
                manifest_pairs.push((df, seq));
            }

            // Early prune at manifest entry level using DataFusion predicate over metrics.
            if !filters.is_empty() && !manifest_pairs.is_empty() {
                // Preserve pairing by keying on file_path before/after prune.
                let (files_only, seq_only): (Vec<DataFile>, Vec<i64>) =
                    manifest_pairs.iter().cloned().unzip();
                let seq_by_path: HashMap<String, i64> = files_only
                    .iter()
                    .map(|f| f.file_path.clone())
                    .zip(seq_only)
                    .collect();
                let (kept, _mask) = crate::datasource::pruning::prune_files(
                    session,
                    filters,
                    None,
                    self.arrow_schema.clone(),
                    files_only,
                    &self.schema,
                )?;
                for df in kept {
                    let seq = *seq_by_path.get(&df.file_path).unwrap_or(&parent_seq);
                    out.push((df, seq));
                }
            } else {
                out.extend(manifest_pairs);
            }
        }

        Ok(out)
    }

    /// Build a [`DeleteFileIndex`] scoped to the current snapshot.
    async fn build_delete_file_index(
        &self,
        store_ctx: &StoreContext,
        manifest_list: &ManifestList,
    ) -> Result<DeleteFileIndex> {
        let spec_map: HashMap<i32, PartitionSpec> = self
            .partition_specs
            .iter()
            .map(|s| (s.spec_id(), s.clone()))
            .collect();

        let mut index = DeleteFileIndex::new();
        for manifest_file in manifest_list
            .entries()
            .iter()
            .filter(|mf| mf.content == ManifestContentType::Deletes)
        {
            let manifest_path_str = manifest_file.manifest_path.as_str();
            let manifest = io_load_manifest(store_ctx, manifest_path_str).await?;
            let partition_spec_id = manifest_file.partition_spec_id;
            let is_unpartitioned = spec_map
                .get(&partition_spec_id)
                .map(|s| s.is_unpartitioned())
                .unwrap_or(false);
            let parent_seq = manifest_file.sequence_number;

            for entry_ref in manifest.entries().iter() {
                let entry = entry_ref.as_ref();
                if !matches!(
                    entry.status,
                    ManifestStatus::Added | ManifestStatus::Existing
                ) {
                    continue;
                }
                let mut df = entry.data_file.clone();
                df.partition_spec_id = partition_spec_id;
                let seq = entry.sequence_number.unwrap_or(parent_seq);
                let file_ref = DeleteFileRef {
                    data_file: df,
                    data_sequence_number: seq,
                    partition_spec_id,
                    is_unpartitioned_spec: is_unpartitioned,
                };
                // Guard: reject v3 deletion vectors explicitly. Silently skipping would
                // corrupt read results.
                if file_ref.is_deletion_vector() {
                    return plan_err!(
                        "Iceberg v3 deletion vectors are not yet supported \
                         (delete file: {})",
                        file_ref.data_file.file_path
                    );
                }
                index.insert(file_ref).map_err(|e| {
                    datafusion::common::DataFusionError::Plan(format!(
                        "failed to index Iceberg delete file: {e}"
                    ))
                })?;
            }
        }
        Ok(index)
    }

    fn create_partitioned_files(
        &self,
        store_ctx: &StoreContext,
        data_files: Vec<DataFile>,
    ) -> Result<Vec<PartitionedFile>> {
        let mut partitioned_files = Vec::new();

        for data_file in data_files {
            let raw_path = data_file.file_path();
            let file_path = store_ctx.resolve_to_absolute_path(raw_path)?;
            log::trace!("Processing data file: {}", file_path);

            log::trace!("Final ObjectPath: {}", file_path);

            let object_meta = ObjectMeta {
                location: file_path,
                last_modified: chrono::Utc::now(),
                size: data_file.file_size_in_bytes(),
                e_tag: None,
                version: None,
            };

            // Convert partition values to ScalarValues
            let partition_values = data_file
                .partition()
                .iter()
                .map(|literal_opt| match literal_opt {
                    Some(Literal::Primitive(prim)) => primitive_to_scalar_default(prim),
                    Some(other) => {
                        log::warn!(
                            "Unexpected non-primitive partition literal {:?}, treating as NULL",
                            other
                        );
                        ScalarValue::Null
                    }
                    None => ScalarValue::Null,
                })
                .collect();

            let partitioned_file = PartitionedFile {
                object_meta,
                partition_values,
                range: None,
                statistics: Some(Arc::new(self.create_file_statistics(&data_file))),
                ordering: None,
                extensions: None,
                metadata_size_hint: None,
            };

            partitioned_files.push(partitioned_file);
        }

        Ok(partitioned_files)
    }

    /// Create file groups from partitioned files
    fn create_file_groups(&self, partitioned_files: Vec<PartitionedFile>) -> Vec<FileGroup> {
        // Group files by partition values
        let mut file_groups: HashMap<Vec<ScalarValue>, Vec<PartitionedFile>> = HashMap::new();

        for file in partitioned_files {
            file_groups
                .entry(file.partition_values.clone())
                .or_default()
                .push(file);
        }

        file_groups.into_values().map(FileGroup::from).collect()
    }

    /// Compute the object-store URL for this table, to be passed to
    /// `FileScanConfigBuilder`.
    fn object_store_url(&self) -> Result<ObjectStoreUrl> {
        let table_url = Url::parse(&self.table_uri)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
        ObjectStoreUrl::parse(&table_url[..url::Position::BeforePath])
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))
    }

    fn build_parquet_source(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        parquet_pushdown_filters: &[Expr],
        enable_pushdown: bool,
    ) -> Result<Arc<dyn datafusion::datasource::physical_plan::FileSource>> {
        let file_schema = self.arrow_schema.clone();
        let parquet_options = TableParquetOptions {
            global: session.config().options().execution.parquet.clone(),
            ..Default::default()
        };
        let mut parquet_source = ParquetSource::new(Arc::clone(&file_schema))
            .with_table_parquet_options(parquet_options);
        if enable_pushdown && !parquet_pushdown_filters.is_empty() {
            let logical_schema = self.rebuild_logical_schema_for_filters(projection, filters);
            let df_schema = logical_schema.to_dfschema()?;
            if let Some(pushdown_expr) = conjunction(parquet_pushdown_filters.iter().cloned()) {
                let simplified = simplify_expr(session, &df_schema, pushdown_expr)?;
                parquet_source = parquet_source.with_predicate(simplified);
            }
        }
        Ok(Arc::new(parquet_source))
    }

    fn expanded_projection(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
    ) -> Option<Vec<usize>> {
        if let Some(used) = projection {
            let mut cols: Vec<usize> = used.clone();
            if let Some(expr) = conjunction(filters.iter().cloned()) {
                for c in expr.column_refs() {
                    if let Ok(idx) = self.arrow_schema.index_of(c.name.as_str()) {
                        if !cols.contains(&idx) {
                            cols.push(idx);
                        }
                    }
                }
            }
            Some(cols)
        } else {
            None
        }
    }

    /// Aggregate table-level statistics from a list of Iceberg data files
    fn aggregate_statistics(&self, data_files: &[DataFile]) -> Statistics {
        if data_files.is_empty() {
            return Statistics::new_unknown(&self.arrow_schema);
        }

        let mut total_rows: usize = 0;
        let mut total_bytes: usize = 0;

        // Pre-compute field id per column index
        let field_ids: Vec<i32> = self.schema.fields().iter().map(|f| f.id).collect();

        // Initialize accumulators per column
        let mut min_scalars: Vec<Option<ScalarValue>> =
            vec![None; self.arrow_schema.fields().len()];
        let mut max_scalars: Vec<Option<ScalarValue>> =
            vec![None; self.arrow_schema.fields().len()];
        let mut null_counts: Vec<usize> = vec![0; self.arrow_schema.fields().len()];

        for df in data_files {
            total_rows = total_rows.saturating_add(df.record_count() as usize);
            total_bytes = total_bytes.saturating_add(df.file_size_in_bytes() as usize);

            for (col_idx, field_id) in field_ids.iter().enumerate() {
                // null counts
                if let Some(c) = df.null_value_counts().get(field_id) {
                    null_counts[col_idx] = null_counts[col_idx].saturating_add(*c as usize);
                }

                // min
                if let Some(d) = df.lower_bounds().get(field_id) {
                    let sv = primitive_to_scalar_default(&d.literal);
                    min_scalars[col_idx] = match (&min_scalars[col_idx], &sv) {
                        (None, s) => Some(s.clone()),
                        (Some(existing), s) => Some(if s < existing {
                            s.clone()
                        } else {
                            existing.clone()
                        }),
                    };
                }

                // max
                if let Some(d) = df.upper_bounds().get(field_id) {
                    let sv = primitive_to_scalar_default(&d.literal);
                    max_scalars[col_idx] = match (&max_scalars[col_idx], &sv) {
                        (None, s) => Some(s.clone()),
                        (Some(existing), s) => Some(if s > existing {
                            s.clone()
                        } else {
                            existing.clone()
                        }),
                    };
                }
            }
        }

        let column_statistics = (0..self.arrow_schema.fields().len())
            .map(|i| ColumnStatistics {
                null_count: Precision::Exact(null_counts[i]),
                max_value: max_scalars[i]
                    .clone()
                    .map(Precision::Exact)
                    .unwrap_or(Precision::Absent),
                min_value: min_scalars[i]
                    .clone()
                    .map(Precision::Exact)
                    .unwrap_or(Precision::Absent),
                distinct_count: Precision::Absent,
                sum_value: Precision::Absent,
                byte_size: Precision::Absent,
            })
            .collect();

        Statistics {
            num_rows: Precision::Exact(total_rows),
            total_byte_size: Precision::Exact(total_bytes),
            column_statistics,
        }
    }

    /// Create file statistics from Iceberg data file metadata
    fn create_file_statistics(&self, data_file: &DataFile) -> Statistics {
        let num_rows = Precision::Exact(data_file.record_count() as usize);
        let total_byte_size = Precision::Exact(data_file.file_size_in_bytes() as usize);

        // Create column statistics from Iceberg metadata
        let column_statistics = self
            .arrow_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, _field)| {
                let field_id = self
                    .schema
                    .fields()
                    .get(i)
                    .map(|f| f.id)
                    .unwrap_or(i as i32 + 1);

                let null_count = data_file
                    .null_value_counts()
                    .get(&field_id)
                    .map(|&count| Precision::Exact(count as usize))
                    .unwrap_or(Precision::Absent);

                let distinct_count = Precision::Absent;

                let min_value = data_file
                    .lower_bounds()
                    .get(&field_id)
                    .map(|datum| primitive_to_scalar_default(&datum.literal))
                    .map(Precision::Exact)
                    .unwrap_or(Precision::Absent);

                let max_value = data_file
                    .upper_bounds()
                    .get(&field_id)
                    .map(|datum| primitive_to_scalar_default(&datum.literal))
                    .map(Precision::Exact)
                    .unwrap_or(Precision::Absent);

                ColumnStatistics {
                    null_count,
                    max_value,
                    min_value,
                    distinct_count,
                    sum_value: Precision::Absent,
                    byte_size: Precision::Absent,
                }
            })
            .collect();

        Statistics {
            num_rows,
            total_byte_size,
            column_statistics,
        }
    }
}

#[async_trait]
impl TableProvider for IcebergTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<ArrowSchema> {
        self.arrow_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn get_table_definition(&self) -> Option<&str> {
        None
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        None
    }

    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        log::trace!("Starting scan for table: {}", self.table_uri);

        if self.metadata_as_data_read {
            return self
                .scan_metadata_as_data(session, projection, filters, limit)
                .await;
        }

        let table_url = Url::parse(&self.table_uri)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
        let base_store = get_object_store_from_session(session, &table_url)?;
        let store_ctx = StoreContext::new(base_store.clone(), &table_url)?;
        log::trace!("Got object store");

        log::trace!(
            "Loading manifest list from: {}",
            self.snapshot.manifest_list()
        );
        let manifest_list = self.load_manifest_list(&store_ctx).await?;
        log::trace!("Loaded {} manifest files", manifest_list.entries().len());

        // Classify & split filters for pruning vs parquet pushdown
        let (pruning_filters, parquet_pushdown_filters) = self.separate_filters(filters);

        log::trace!("Loading data files from manifests...");
        let mut data_files_with_seq = self
            .load_data_files_with_seq(session, &pruning_filters, &store_ctx, &manifest_list)
            .await?;
        log::trace!("Loaded {} data files", data_files_with_seq.len());

        // Build filter conjunction and run DataFusion-based pruning on Iceberg metrics.
        // Preserve per-file sequence numbers through the prune.
        let filter_expr = conjunction(pruning_filters.iter().cloned());
        if filter_expr.is_some() || limit.is_some() {
            let (files_only, seqs_only): (Vec<DataFile>, Vec<i64>) =
                data_files_with_seq.iter().cloned().unzip();
            let seq_by_path: HashMap<String, i64> = files_only
                .iter()
                .map(|f| f.file_path.clone())
                .zip(seqs_only)
                .collect();
            let (kept, _mask) = prune_files(
                session,
                &pruning_filters,
                limit,
                self.rebuild_logical_schema_for_filters(projection, filters),
                files_only,
                &self.schema,
            )?;
            data_files_with_seq = kept
                .into_iter()
                .map(|df| {
                    let seq = seq_by_path.get(&df.file_path).copied().unwrap_or(0);
                    (df, seq)
                })
                .collect();
            log::trace!(
                "Pruned data files, remaining: {}",
                data_files_with_seq.len()
            );
        }

        // Build the delete-file index for this snapshot. Rejects v3 deletion vectors.
        log::trace!("Building delete file index...");
        let delete_index = self
            .build_delete_file_index(&store_ctx, &manifest_list)
            .await?;

        // Partition each data file into "clean" (no matching deletes) vs "dirty"
        // (one or more matching deletes) buckets. We only pay the cost of
        // `IcebergDeleteApplyExec` for dirty files.
        let mut clean_files: Vec<DataFile> = Vec::new();
        let mut dirty_units: Vec<(DataFile, Vec<DeleteFileRef>, Vec<DeleteFileRef>)> = Vec::new();
        for (df, seq) in data_files_with_seq.iter().cloned() {
            let matched = delete_index.for_data_file(&df, seq);
            if matched.is_empty() {
                clean_files.push(df);
            } else {
                dirty_units.push((df, matched.positional, matched.equality));
            }
        }
        log::trace!(
            "Delete split: {} clean, {} dirty",
            clean_files.len(),
            dirty_units.len()
        );

        // Aggregate stats over ALL files (before split) for the planner.
        let all_data_files: Vec<DataFile> = data_files_with_seq
            .iter()
            .map(|(df, _)| df.clone())
            .collect();
        let table_stats = self.aggregate_statistics(&all_data_files);

        // Object-store URL shared by all branches.
        let object_store_url = self.object_store_url()?;

        if dirty_units.is_empty() {
            // Fast path: no deletes apply. Emit the single-DataSourceExec plan that
            // is identical to the pre-delete-integration behavior.
            let partitioned_files = self.create_partitioned_files(&store_ctx, all_data_files)?;
            let file_groups = self.create_file_groups(partitioned_files);
            let parquet_source = self.build_parquet_source(
                session,
                projection,
                filters,
                &parquet_pushdown_filters,
                true,
            )?;
            let expanded_projection = self.expanded_projection(projection, filters);
            let file_scan_config = FileScanConfigBuilder::new(object_store_url, parquet_source)
                .with_file_groups(if file_groups.is_empty() {
                    vec![FileGroup::from(vec![])]
                } else {
                    file_groups
                })
                .with_statistics(table_stats)
                .with_projection_indices(expanded_projection)?
                .with_limit(limit)
                .with_expr_adapter(Some(Arc::new(IcebergPhysicalExprAdapterFactory {})
                    as Arc<dyn PhysicalExprAdapterFactory>))
                .build();
            return Ok(DataSourceExec::from_data_source(file_scan_config));
        }

        // Delete-aware path: build clean + per-dirty-file branches. We apply
        // predicates, projection, and limit ABOVE the Union so that positional
        // row offsets inside `IcebergDeleteApplyExec` remain aligned with the
        // unfiltered Parquet read of each dirty file.

        let mut branches: Vec<Arc<dyn ExecutionPlan>> = Vec::new();

        // Branch A: clean files scanned as one DataSourceExec. Neither projection nor
        // predicate is pushed down at this level because the upper layers will apply
        // them uniformly across branches.
        if !clean_files.is_empty() {
            let partitioned_files = self.create_partitioned_files(&store_ctx, clean_files)?;
            let file_groups = self.create_file_groups(partitioned_files);
            let parquet_source = self.build_parquet_source(
                session,
                projection,
                filters,
                &[], // no parquet-level predicate
                false,
            )?;
            let file_scan_config =
                FileScanConfigBuilder::new(object_store_url.clone(), parquet_source)
                    .with_file_groups(file_groups)
                    .with_expr_adapter(Some(Arc::new(IcebergPhysicalExprAdapterFactory {})
                        as Arc<dyn PhysicalExprAdapterFactory>))
                    .build();
            branches.push(DataSourceExec::from_data_source(file_scan_config));
        }

        // Branch B: one branch per dirty file.
        for (df, pos_deletes, eq_deletes) in dirty_units {
            let partitioned = self.create_partitioned_files(&store_ctx, vec![df.clone()])?;
            // Single-file, single-partition scan — preserves row order for positional deletes.
            let parquet_source = self.build_parquet_source(session, None, &[], &[], false)?;
            let file_scan_config =
                FileScanConfigBuilder::new(object_store_url.clone(), parquet_source)
                    .with_file_groups(vec![FileGroup::from(partitioned)])
                    .with_expr_adapter(Some(Arc::new(IcebergPhysicalExprAdapterFactory {})
                        as Arc<dyn PhysicalExprAdapterFactory>))
                    .build();
            let data_scan: Arc<dyn ExecutionPlan> =
                DataSourceExec::from_data_source(file_scan_config);
            let data_file_raw_path = df.file_path().to_string();
            // Wrap with DeleteApply.
            let apply: Arc<dyn ExecutionPlan> = Arc::new(IcebergDeleteApplyExec::new(
                data_scan,
                data_file_raw_path,
                pos_deletes,
                eq_deletes,
                self.table_uri.clone(),
                self.schema.clone(),
            ));
            branches.push(apply);
        }

        // Union the branches.
        let unioned: Arc<dyn ExecutionPlan> = if branches.len() == 1 {
            // SAFETY: length was just checked above.
            branches.into_iter().next().ok_or_else(|| {
                datafusion::common::DataFusionError::Internal(
                    "unreachable: branches.len() == 1 but next() returned None".to_string(),
                )
            })?
        } else {
            UnionExec::try_new(branches)?
        };

        // Apply predicate above (covers both clean & dirty branches).
        let after_filter: Arc<dyn ExecutionPlan> = if !parquet_pushdown_filters.is_empty() {
            let df_schema = self.arrow_schema.clone().to_dfschema()?;
            let pushdown_expr = conjunction(parquet_pushdown_filters.clone()).ok_or_else(|| {
                datafusion::common::DataFusionError::Internal(
                    "conjunction over non-empty filters returned None".to_string(),
                )
            })?;
            let simplified = simplify_expr(session, &df_schema, pushdown_expr)?;
            Arc::new(FilterExec::try_new(simplified, unioned)?)
        } else {
            unioned
        };

        // Apply projection above.
        let after_projection: Arc<dyn ExecutionPlan> = if let Some(proj) = projection {
            let projected_schema = self.arrow_schema.clone();
            let proj_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = proj
                .iter()
                .map(|&idx| {
                    let field = projected_schema.field(idx);
                    let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new(field.name(), idx));
                    (col, field.name().to_string())
                })
                .collect();
            Arc::new(ProjectionExec::try_new(proj_exprs, after_filter)?)
        } else {
            after_filter
        };

        // Apply limit above (may over-scan; correctness is preserved because
        // GlobalLimitExec stops streaming once the row count is reached).
        let final_plan: Arc<dyn ExecutionPlan> = if let Some(lim) = limit {
            Arc::new(GlobalLimitExec::new(after_projection, 0, Some(lim)))
        } else {
            after_projection
        };

        Ok(final_plan)
    }

    fn supports_filters_pushdown(
        &self,
        filter: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        if self.metadata_as_data_read {
            return Ok(vec![TableProviderFilterPushDown::Unsupported; filter.len()]);
        }
        Ok(filter
            .iter()
            .map(|e| self.classify_pushdown_for_expr(e))
            .collect())
    }
}

impl IcebergTableProvider {
    fn classify_pushdown_for_expr(&self, expr: &Expr) -> TableProviderFilterPushDown {
        use TableProviderFilterPushDown as FP;
        // Identity partition columns get Exact (Eq/IN) or Inexact (ranges)
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let (l, r) = (Self::strip_expr(left), Self::strip_expr(right));
                match op {
                    Operator::Eq => {
                        if let (Some(col), true) =
                            (self.expr_as_column_name(l), self.expr_is_literal(r))
                        {
                            if self.is_identity_partition_col(&col) {
                                return FP::Exact;
                            }
                        }
                        if let (Some(col), true) =
                            (self.expr_as_column_name(r), self.expr_is_literal(l))
                        {
                            if self.is_identity_partition_col(&col) {
                                return FP::Exact;
                            }
                        }
                        FP::Unsupported
                    }
                    Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq => {
                        if let Some(col) = self.expr_as_column_name(l) {
                            if self.expr_is_literal(r) && self.is_identity_partition_col(&col) {
                                return FP::Inexact;
                            }
                        }
                        FP::Unsupported
                    }
                    _ => FP::Unsupported,
                }
            }
            Expr::InList(in_list) if !in_list.negated => {
                let e = Self::strip_expr(&in_list.expr);
                if let Some(col) = self.expr_as_column_name(e) {
                    let all_literals = in_list.list.iter().all(|it| self.expr_is_literal(it));
                    if all_literals && self.is_identity_partition_col(&col) {
                        TableProviderFilterPushDown::Exact
                    } else {
                        TableProviderFilterPushDown::Unsupported
                    }
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            }
            _ => TableProviderFilterPushDown::Unsupported,
        }
    }

    fn separate_filters(&self, filters: &[Expr]) -> (Vec<Expr>, Vec<Expr>) {
        let mut pruning_filters = Vec::new();
        let mut parquet_pushdown_filters = Vec::new();
        for f in filters.iter() {
            match self.classify_pushdown_for_expr(f) {
                TableProviderFilterPushDown::Exact => {
                    pruning_filters.push(f.clone());
                    // Even if partition pruning is "exact", we still must apply the filter at scan
                    // time. Pruning is an optimization and can be conservative when stats are
                    // missing; correctness requires retaining the predicate.
                    parquet_pushdown_filters.push(f.clone());
                }
                TableProviderFilterPushDown::Inexact => {
                    pruning_filters.push(f.clone());
                    parquet_pushdown_filters.push(f.clone());
                }
                TableProviderFilterPushDown::Unsupported => {}
            }
        }
        (pruning_filters, parquet_pushdown_filters)
    }

    fn strip_expr(expr: &Expr) -> &Expr {
        match expr {
            Expr::Cast(c) => Self::strip_expr(&c.expr),
            Expr::Alias(a) => Self::strip_expr(&a.expr),
            _ => expr,
        }
    }

    fn expr_as_column_name(&self, expr: &Expr) -> Option<String> {
        if let Expr::Column(c) = expr {
            return Some(c.name.clone());
        }
        None
    }

    fn expr_is_literal(&self, expr: &Expr) -> bool {
        matches!(expr, Expr::Literal(_, _))
    }

    fn is_identity_partition_col(&self, col_name: &str) -> bool {
        // Map identity partition source_id to schema field names
        let mut names = std::collections::HashSet::new();
        for spec in &self.partition_specs {
            for pf in spec.fields().iter() {
                if matches!(pf.transform, crate::spec::transform::Transform::Identity) {
                    if let Some(field) = self.schema.field_by_id(pf.source_id) {
                        names.insert(field.name.clone());
                    }
                }
            }
        }
        names.contains(col_name)
    }

    fn rebuild_logical_schema_for_filters(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
    ) -> Arc<ArrowSchema> {
        if let Some(used) = projection {
            let mut fields: Vec<datafusion::arrow::datatypes::FieldRef> = Vec::new();
            for idx in used {
                fields.push(Arc::new(self.arrow_schema.field(*idx).clone()));
            }
            if let Some(expr) = conjunction(filters.iter().cloned()) {
                for c in expr.column_refs() {
                    if let Ok(idx) = self.arrow_schema.index_of(c.name.as_str()) {
                        if !used.contains(&idx)
                            && !fields.iter().any(|f| f.name() == c.name.as_str())
                        {
                            fields.push(Arc::new(self.arrow_schema.field(idx).clone()));
                        }
                    }
                }
            }
            Arc::new(ArrowSchema::new(fields))
        } else {
            self.arrow_schema.clone()
        }
    }

    /// Metadata-as-data scan path: defers manifest scanning to the physical plan.
    /// Instead of eagerly loading file metadata on the driver, an `IcebergManifestScanExec`
    /// node produces file metadata that feeds through `IcebergDiscoveryExec` (tagging) and
    /// into `IcebergScanByDataFilesExec` which dynamically opens Parquet files via a
    /// streaming `try_unfold` loop.
    ///
    /// Physical plan pipeline:
    /// ```text
    /// IcebergManifestScanExec (lazy manifest reading → file metadata stream)
    ///   ↓
    /// IcebergDiscoveryExec (annotate stream with partition_scan flag)
    ///   ↓
    /// IcebergScanByDataFilesExec (consume file paths → dynamic Parquet scan)
    ///   ↓
    /// Data output (actual table rows)
    /// ```
    async fn scan_metadata_as_data(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        log::trace!(
            "Using metadata-as-data scan path for table: {}",
            self.table_uri
        );

        let table_url = Url::parse(&self.table_uri)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
        let base_store = get_object_store_from_session(session, &table_url)?;
        let store_ctx = StoreContext::new(base_store, &table_url)?;
        let manifest_list = self.load_manifest_list(&store_ctx).await?;
        if manifest_list
            .entries()
            .iter()
            .any(|mf| mf.content == ManifestContentType::Deletes)
        {
            return plan_err!(
                "metadata-as-data read path does not yet support tables with delete files; \
                 disable the `metadataAsDataRead` option to use the driver-based read path"
            );
        }

        if projection.is_some() || !filters.is_empty() || limit.is_some() {
            log::debug!(
                "metadata-as-data scan does not push down projection/filters/limit \
                 (projection_provided={}, num_filters={}, limit={:?}); relying on \
                 the planner to apply them above the scan. Note: \
                 `supports_filters_pushdown` returns Unsupported in this mode so \
                 the planner does NOT drop filters from the outer plan.",
                projection.is_some(),
                filters.len(),
                limit,
            );
        }

        let manifest_scan: Arc<dyn ExecutionPlan> = Arc::new(IcebergManifestScanExec::new(
            self.table_uri.clone(),
            self.snapshot.clone(),
        ));

        let discovery: Arc<dyn ExecutionPlan> = Arc::new(IcebergDiscoveryExec::new(
            manifest_scan,
            self.table_uri.clone(),
            self.snapshot.snapshot_id(),
            false, // full data file scan, not partition-only
        )?);

        let scan_exec = Arc::new(
            crate::physical_plan::scan_by_data_files_exec::IcebergScanByDataFilesExec::new(
                discovery,
                self.table_uri.clone(),
                self.arrow_schema.clone(),
            ),
        );

        Ok(scan_exec)
    }
}
