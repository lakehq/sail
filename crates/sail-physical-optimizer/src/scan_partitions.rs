use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;

use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, Partitioning, replace_children_if_necessary,
};
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::{FileRange, PartitionedFile};
use sail_common_datafusion::scan::ParquetScanMetadata;

/// Correct automatic parallelism using Parquet's actual units of scan work.
/// Runs after EnsureRequirements, which otherwise splits files by byte size and
/// treats filtered (inexact) row counts as a reason to add round-robin exchanges.
#[derive(Debug)]
pub struct OptimizeScanPartitions;

impl PhysicalOptimizerRule for OptimizeScanPartitions {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        rewrite(plan, config)
    }

    fn name(&self) -> &str {
        "OptimizeScanPartitions"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn rewrite(plan: Arc<dyn ExecutionPlan>, config: &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>> {
    let children = plan.children();
    // Unknown scan partition counts may shrink through unary operators and
    // union/collect-left joins. Keep partition alignment for other multi-input
    // operators (e.g. InterleaveExec). Explicit exchanges retain their outputs.
    let allow_count_change = children.len() <= 1
        || plan.is::<UnionExec>()
        || plan
            .downcast_ref::<HashJoinExec>()
            .is_some_and(|join| join.mode == PartitionMode::CollectLeft);
    let children = children
        .into_iter()
        .map(|child| {
            let rewritten = rewrite(Arc::clone(child), config)?;
            if !allow_count_change
                && rewritten.output_partitioning().partition_count()
                    != child.output_partitioning().partition_count()
            {
                Ok(Arc::clone(child))
            } else {
                Ok(rewritten)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    let plan = replace_children_if_necessary(plan, children)?;
    if config.optimizer.repartition_file_scans
        && let Some(source) = plan.downcast_ref::<DataSourceExec>()
        && let Some(scan) = source.data_source().downcast_ref::<FileScanConfig>()
        && scan.file_source.is::<ParquetSource>()
        && let Some(groups) =
            repartition_row_groups(scan, config.optimizer.repartition_file_min_size)
    {
        let scan = FileScanConfigBuilder::from(scan.clone())
            .with_file_groups(groups)
            .build();
        return Ok(DataSourceExec::from_data_source(scan));
    }
    if let Some(exchange) = plan.downcast_ref::<RepartitionExec>()
        && matches!(exchange.partitioning(), Partitioning::RoundRobinBatch(_))
        && !exchange.preserve_order()
        && scan_row_upper_bound(exchange.input())
            .is_some_and(|rows| rows <= config.execution.batch_size.get() as u64)
    {
        // ExplicitRepartitionExec is intentionally not matched. A raw source
        // count bounds filters/projections without making filtered stats exact.
        return Ok(Arc::clone(exchange.input()));
    }
    Ok(plan)
}

fn scan_row_upper_bound(plan: &Arc<dyn ExecutionPlan>) -> Option<u64> {
    if plan.is::<FilterExec>() || plan.is::<ProjectionExec>() || plan.is::<CooperativeExec>() {
        let children = plan.children();
        let [child] = children.as_slice() else {
            return None;
        };
        return scan_row_upper_bound(child);
    }
    let source = plan.downcast_ref::<DataSourceExec>()?;
    let scan = source.data_source().downcast_ref::<FileScanConfig>()?;
    if !scan.file_source.is::<ParquetSource>() {
        return None;
    }
    scan.file_groups
        .iter()
        .flat_map(FileGroup::iter)
        .try_fold(0u64, |total, file| {
            let metadata = file.extensions.get::<ParquetScanMetadata>()?;
            metadata
                .row_groups
                .iter()
                .filter(|group| {
                    file.range
                        .as_ref()
                        .is_none_or(|range| range.contains(group.offset))
                })
                .try_fold(total, |total, group| total.checked_add(group.num_rows))
        })
}

fn repartition_row_groups(scan: &FileScanConfig, min_size: usize) -> Option<Vec<FileGroup>> {
    // Retain file-group partitioning and ordering contracts. Limit pushdown can
    // also make the identity/order of a file group observable.
    if scan.file_groups.len() <= 1
        || scan.output_partitioning.is_some()
        || !scan.output_ordering.is_empty()
        || scan.preserve_order
        || scan.limit.is_some()
    {
        return None;
    }

    let mut units: Vec<(u64, PartitionedFile)> = Vec::new();
    for file in scan.file_groups.iter().flat_map(FileGroup::iter) {
        let metadata = file.extensions.get::<ParquetScanMetadata>()?;
        let file_end = i64::try_from(file.object_meta.size).ok()?;
        // Parquet range pruning uses the first column's first page, not the
        // row group's metadata offset or the projected column's offset.
        if metadata
            .row_groups
            .iter()
            .any(|group| group.offset < 0 || group.offset >= file_end)
            || metadata
                .row_groups
                .windows(2)
                .any(|groups| groups[0].offset >= groups[1].offset)
        {
            return None;
        }
        for (index, group) in metadata.row_groups.iter().enumerate() {
            if file
                .range
                .as_ref()
                .is_some_and(|range| !range.contains(group.offset))
            {
                continue;
            }
            let mut unit = file.clone();
            unit.range = Some(FileRange {
                start: group.offset,
                end: metadata
                    .row_groups
                    .get(index + 1)
                    .map_or(file_end, |group| group.offset),
            });
            // Whole-file statistics are not valid for an individual row group.
            // The scan's aggregate statistics remain unchanged.
            unit.statistics = None;
            units.push((group.compressed_size.max(1), unit));
        }
    }
    if units.is_empty() {
        return None;
    }
    let total_size = units
        .iter()
        .try_fold(0u64, |total, (size, _)| total.checked_add(*size))?;
    let by_size = total_size.div_ceil(min_size.max(1) as u64).max(1);
    let count = scan
        .file_groups
        .len()
        .min(units.len())
        .min(usize::try_from(by_size).unwrap_or(usize::MAX));
    // Even when the count does not decrease, align byte ranges to row groups to
    // eliminate empty ranges caused by unevenly sized row groups.
    units.sort_by_key(|(size, _)| Reverse(*size));
    let mut groups = vec![vec![]; count];
    let mut sizes: BinaryHeap<_> = (0..count).map(|index| Reverse((0u64, index))).collect();
    for (size, file) in units {
        let Reverse((used, index)) = sizes.pop()?;
        groups[index].push(file);
        sizes.push(Reverse((used.saturating_add(size), index)));
    }
    Some(groups.into_iter().map(FileGroup::new).collect())
}
