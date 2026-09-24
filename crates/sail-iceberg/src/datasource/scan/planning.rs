//! File selection shared by logical metadata evaluation and physical scans.
use std::collections::HashMap;

use datafusion::catalog::Session;
use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::Expr;
use url::Url;

use super::IcebergScan;
use crate::datasource::predicate::Predicate;
use crate::datasource::pruning::{
    prune_data_files_by_partition_values, prune_manifests_by_partition_summaries,
};
use crate::io::{
    StoreContext, load_manifest as io_load_manifest, load_manifest_list as io_load_manifest_list,
};
use crate::spec::delete_index::{DeleteFileIndex, DeleteFileRef, MatchedDeletes};
use crate::spec::{DataFile, ManifestContentType, ManifestList, ManifestStatus, PartitionSpec};
use crate::utils::get_object_store_from_session;

#[derive(Debug, Clone)]
pub(crate) struct IcebergFileScanTask {
    pub data_file: DataFile,
    pub data_sequence_number: i64,
    pub deletes: MatchedDeletes,
    pub residual: Vec<Expr>,
}

/// A request-scoped selection from the source's fixed snapshot. Projection does
/// not change the selected rows, so residual aggregates can reuse these tasks.
#[derive(Debug)]
pub(crate) struct IcebergScanPlan {
    pub tasks: Vec<IcebergFileScanTask>,
    filters: Vec<Expr>,
    pub limit: Option<usize>,
}

impl IcebergScanPlan {
    pub fn matches(&self, filters: &[Expr], limit: Option<usize>) -> bool {
        self.filters == filters && self.limit == limit
    }

    pub fn residual_filters(&self) -> Vec<Expr> {
        self.filters
            .iter()
            .filter(|filter| self.tasks.iter().any(|task| task.residual.contains(filter)))
            .cloned()
            .collect()
    }
}

impl IcebergScan {
    pub(crate) async fn plan_files(
        &self,
        session: &dyn Session,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<IcebergScanPlan> {
        let mut planned = IcebergScanPlan {
            tasks: vec![],
            filters: filters.to_vec(),
            limit,
        };
        if self.snapshot.is_none() {
            return Ok(planned);
        }
        let table_url = Url::parse(&self.table_uri)
            .map_err(|error| datafusion::common::DataFusionError::External(Box::new(error)))?;
        let store = get_object_store_from_session(session, &table_url)?;
        let store_ctx = StoreContext::new(store, &table_url)?;
        let manifests = self.load_manifest_list(&store_ctx).await?;
        let (pruning_filters, _) = self.separate_filters(filters);
        let mut files = self
            .load_data_files_with_seq(&pruning_filters, &store_ctx, &manifests)
            .await?;
        if let Some(predicate) = &self.copy_on_write_predicate {
            files = crate::datasource::copy_on_write::select_copy_on_write_files(
                predicate,
                &self.schema,
                &self.partition_specs,
                files,
            )
            .candidates;
        }
        let file_limit = limit.filter(|_| {
            filters.is_empty()
                && !manifests
                    .entries()
                    .iter()
                    .any(|manifest| manifest.content == ManifestContentType::Deletes)
        });
        if let Some(limit) = file_limit {
            let mut rows = 0u64;
            files.retain(|(file, _)| {
                let keep = rows < limit as u64;
                rows = rows.saturating_add(file.record_count());
                keep
            });
        }
        if files.is_empty() {
            return Ok(planned);
        }
        let deletes = self.build_delete_file_index(&store_ctx, &manifests).await?;
        planned.tasks = files
            .into_iter()
            .map(|(data_file, data_sequence_number)| {
                let spec = self
                    .partition_specs
                    .iter()
                    .find(|spec| spec.spec_id() == data_file.partition_spec_id);
                let residual = filters
                    .iter()
                    .filter(|filter| {
                        !Predicate::new(&self.schema, filter)
                            .file(&data_file, spec)
                            .all_match()
                    })
                    .cloned()
                    .collect();
                let deletes = deletes.for_data_file(&data_file, data_sequence_number);
                IcebergFileScanTask {
                    data_file,
                    data_sequence_number,
                    deletes,
                    residual,
                }
            })
            .collect();
        Ok(planned)
    }

    /// Load manifest list from snapshot
    pub(super) async fn load_manifest_list(
        &self,
        store_ctx: &StoreContext,
    ) -> Result<ManifestList> {
        let snapshot = self.snapshot.as_ref().ok_or_else(|| {
            datafusion::common::DataFusionError::Plan(
                "Iceberg table has no current snapshot".to_string(),
            )
        })?;
        let manifest_list_str = snapshot.manifest_list();
        log::trace!("Manifest list path: {}", manifest_list_str);
        let ml = io_load_manifest_list(store_ctx, manifest_list_str).await?;
        Ok(ml)
    }

    /// Load data files from manifests, preserving per-file data sequence numbers.
    pub(super) async fn load_data_files_with_seq(
        &self,
        filters: &[Expr],
        store_ctx: &StoreContext,
        manifest_list: &ManifestList,
    ) -> Result<Vec<(DataFile, i64)>> {
        let spec_map: HashMap<i32, PartitionSpec> = self
            .partition_specs
            .iter()
            .map(|s| (s.spec_id(), s.clone()))
            .collect();
        let candidate_filters = self
            .copy_on_write_predicate
            .as_ref()
            .map(|predicate| vec![predicate.clone()]);
        let manifest_files = prune_manifests_by_partition_summaries(
            manifest_list,
            &self.schema,
            &spec_map,
            candidate_filters.as_deref().unwrap_or(filters),
        );

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
            let mut inherited_next_row_id = manifest_file.first_row_id;

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
                if df.first_row_id.is_none() {
                    df.first_row_id = inherited_next_row_id;
                    if let Some(next_row_id) = &mut inherited_next_row_id {
                        let count = i64::try_from(df.record_count).map_err(|error| {
                            datafusion_common::plan_datafusion_err!(
                                "Iceberg row count overflow: {error}"
                            )
                        })?;
                        *next_row_id = next_row_id.checked_add(count).ok_or_else(|| {
                            datafusion_common::plan_datafusion_err!("Iceberg row ID overflow")
                        })?;
                    }
                }
                let seq = entry.sequence_number.unwrap_or(parent_seq);
                manifest_pairs.push((df, seq));
            }

            // Early prune at manifest entry level using DataFusion predicate over metrics.
            if !filters.is_empty() && !manifest_pairs.is_empty() {
                // Preserve pairing by keying on file_path before/after prune.
                let (mut files_only, seq_only): (Vec<DataFile>, Vec<i64>) =
                    manifest_pairs.iter().cloned().unzip();
                let seq_by_path: HashMap<String, i64> = files_only
                    .iter()
                    .map(|f| f.file_path.clone())
                    .zip(seq_only)
                    .collect();
                if let Some(spec) = spec_map.get(&partition_spec_id) {
                    files_only = prune_data_files_by_partition_values(
                        files_only,
                        &self.schema,
                        spec,
                        filters,
                    );
                }
                let (kept, _mask) = crate::datasource::pruning::prune_files(
                    filters,
                    None,
                    files_only,
                    &self.schema,
                );
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
    pub(super) async fn build_delete_file_index(
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
                // TODO: Read and apply v3 Puffin deletion vectors before enabling DV tables.
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
}
