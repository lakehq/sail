// The listing table planner is adapted from the DataFusion `ListingTable` implementation,
// specifically the `TableProvider::scan_with_args` trait method.
// [CREDIT]: https://github.com/apache/datafusion/blob/53.1.0/datafusion/catalog-listing/src/table.rs

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::catalog::Session;
use datafusion::datasource::listing::helpers::pruned_partition_list;
use datafusion::execution::cache::cache_manager::CachedFileMetadata;
use datafusion::execution::cache::TableScopedPath;
use datafusion::execution::SessionState;
use datafusion::logical_expr::expr_rewriter::unnormalize_cols;
use datafusion::logical_expr::{Expr, LogicalPlan, TableScan, UserDefinedLogicalNode};
use datafusion::physical_expr::create_lex_ordering;
use datafusion::physical_expr_common::sort_expr::LexOrdering;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::stats::Precision;
use datafusion_common::{project_schema, Statistics};
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::source::DataSourceExec;
use futures::{future, stream, Stream, StreamExt};
use object_store::ObjectStore;

use crate::listing::source::ListingScanInput;
use crate::listing::table::ListingTableSource;
use crate::listing::utils::can_be_evaluated_for_partition_pruning;

/// Result of a file listing operation for listing table scans.
#[derive(Debug)]
struct ListFilesResult {
    file_groups: Vec<FileGroup>,
    statistics: Statistics,
    grouped_by_partition: bool,
}

/// Physical planner for logical listing table scans.
///
/// Plans `ListingTableSource` table scans directly without an intermediate `TableProvider`.
#[derive(Debug, Default)]
pub struct ListingTablePhysicalPlanner;

#[async_trait]
impl ExtensionPlanner for ListingTablePhysicalPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        _node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> datafusion_common::Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }

    async fn plan_table_scan(
        &self,
        _planner: &dyn PhysicalPlanner,
        scan: &TableScan,
        session_state: &SessionState,
    ) -> datafusion_common::Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(source) = scan.source.downcast_ref::<ListingTableSource>() else {
            return Ok(None);
        };

        let projection = scan.projection.clone();
        let filters = unnormalize_cols(scan.filters.clone());
        let limit = scan.fetch;

        let partition_column_names = source
            .config()
            .schema
            .table_partition_cols()
            .iter()
            .map(|col| col.name().as_str())
            .collect::<Vec<_>>();

        let (partition_filters, filters): (Vec<_>, Vec<_>) =
            filters.iter().cloned().partition(|filter| {
                can_be_evaluated_for_partition_pruning(&partition_column_names, filter)
            });

        let statistic_file_limit = if filters.is_empty() { limit } else { None };

        let ListFilesResult {
            mut file_groups,
            statistics,
            grouped_by_partition: partitioned_by_file_group,
        } = list_files_for_scan(
            source,
            session_state,
            &partition_filters,
            statistic_file_limit,
        )
        .await?;

        let table_schema = source.config().schema.table_schema();
        if file_groups.is_empty() {
            let projected_schema = project_schema(table_schema, projection.as_ref())?;
            return Ok(Some(Arc::new(EmptyExec::new(projected_schema))));
        }

        let output_ordering =
            try_create_output_ordering(source, session_state.execution_props(), &file_groups)?;

        match session_state
            .config_options()
            .execution
            .split_file_groups_by_statistics
            .then(|| {
                output_ordering.first().map(|output_ordering| {
                    FileScanConfig::split_groups_by_statistics_with_target_partitions(
                        table_schema,
                        &file_groups,
                        output_ordering,
                        source.config().target_partitions,
                    )
                })
            })
            .flatten()
        {
            Some(Err(e)) => log::debug!("failed to split file groups by statistics: {e}"),
            Some(Ok(new_groups)) => {
                if new_groups.len() <= source.config().target_partitions {
                    file_groups = new_groups;
                } else {
                    log::debug!(
                        "attempted to split file groups by statistics, but there were more file groups than target_partitions; falling back to unordered"
                    )
                }
            }
            None => {} // no ordering required
        };

        // If user specified ordering, that's already handled in `try_create_output_ordering`.
        // When no ordering is specified and we derived ordering from files, keep it.
        let Some(object_store_url) = source
            .config()
            .table_paths
            .first()
            .map(datafusion_datasource::ListingTableUrl::object_store)
        else {
            return Ok(Some(Arc::new(EmptyExec::new(Arc::new(
                datafusion::arrow::datatypes::Schema::empty(),
            )))));
        };

        let config = source
            .config()
            .read_format
            .scan(
                session_state,
                ListingScanInput {
                    object_store_url,
                    file_groups,
                    constraints: source.config().constraints.clone(),
                    projection,
                    limit,
                    preserve_order: false,
                    output_ordering,
                    statistics,
                    partitioned_by_file_group,
                    schema: source.config().schema.clone(),
                    compression: source.config().compression,
                },
            )
            .await?;

        Ok(Some(DataSourceExec::from_data_source(config)))
    }
}

fn try_create_output_ordering(
    source: &ListingTableSource,
    execution_props: &datafusion::logical_expr::execution_props::ExecutionProps,
    file_groups: &[FileGroup],
) -> datafusion_common::Result<Vec<LexOrdering>> {
    if !source.config().file_sort_order.is_empty() {
        return create_lex_ordering(
            source.config().schema.table_schema(),
            &source.config().file_sort_order,
            execution_props,
        );
    }
    if let Some(ordering) = derive_common_ordering_from_files(file_groups) {
        return Ok(vec![ordering]);
    }
    Ok(vec![])
}

fn derive_common_ordering_from_files(file_groups: &[FileGroup]) -> Option<LexOrdering> {
    enum CurrentOrderingState {
        FirstFile,
        SomeOrdering(LexOrdering),
        NoOrdering,
    }
    let mut state = CurrentOrderingState::FirstFile;

    for group in file_groups {
        for file in group.iter() {
            state = match (&state, &file.ordering) {
                (CurrentOrderingState::FirstFile, Some(ordering)) => {
                    CurrentOrderingState::SomeOrdering(ordering.clone())
                }
                (CurrentOrderingState::FirstFile, None) => CurrentOrderingState::NoOrdering,
                (CurrentOrderingState::SomeOrdering(current), Some(ordering)) => {
                    let prefix_len = current
                        .as_ref()
                        .iter()
                        .zip(ordering.as_ref().iter())
                        .take_while(|(a, b)| a == b)
                        .count();
                    if prefix_len == 0 {
                        log::trace!(
                            "Cannot derive common ordering: no common prefix between orderings {current:?} and {ordering:?}"
                        );
                        return None;
                    } else {
                        let Some(ordering) =
                            LexOrdering::new(current.as_ref()[..prefix_len].to_vec())
                        else {
                            log::trace!(
                                "Cannot derive common ordering: common prefix could not be converted into a LexOrdering"
                            );
                            return None;
                        };
                        CurrentOrderingState::SomeOrdering(ordering)
                    }
                }
                (CurrentOrderingState::SomeOrdering(ordering), None)
                | (CurrentOrderingState::NoOrdering, Some(ordering)) => {
                    log::trace!(
                        "Cannot derive common ordering: some files have ordering {ordering:?}, others don't"
                    );
                    return None;
                }
                (CurrentOrderingState::NoOrdering, None) => CurrentOrderingState::NoOrdering,
            };
        }
    }

    match state {
        CurrentOrderingState::SomeOrdering(ordering) => Some(ordering),
        _ => None,
    }
}

async fn list_files_for_scan<'a>(
    source: &'a ListingTableSource,
    ctx: &'a dyn Session,
    filters: &'a [Expr],
    limit: Option<usize>,
) -> datafusion_common::Result<ListFilesResult> {
    let store = if let Some(url) = source.config().table_paths.first() {
        ctx.runtime_env().object_store(url)?
    } else {
        return Ok(ListFilesResult {
            file_groups: vec![],
            statistics: Statistics::new_unknown(source.config().schema.file_schema()),
            grouped_by_partition: false,
        });
    };

    let partition_cols: Vec<(String, DataType)> = source
        .config()
        .schema
        .table_partition_cols()
        .iter()
        .map(|field| (field.name().clone(), field.data_type().clone()))
        .collect();

    let file_list = future::try_join_all(source.config().table_paths.iter().map(|table_path| {
        pruned_partition_list(
            ctx,
            store.as_ref(),
            table_path,
            filters,
            &source.config().file_extension,
            &partition_cols,
        )
    }))
    .await?;

    let meta_fetch_concurrency = ctx.config_options().execution.meta_fetch_concurrency;
    let file_list = stream::iter(file_list).flatten_unordered(meta_fetch_concurrency);

    let files = file_list
        .map(|part_file| async {
            let part_file = part_file?;
            let (statistics, ordering) = if source.config().collect_stat {
                do_collect_statistics_and_ordering(source, ctx, &store, &part_file).await?
            } else {
                (
                    Arc::new(Statistics::new_unknown(
                        source.config().schema.file_schema(),
                    )),
                    None,
                )
            };
            Ok(part_file
                .with_statistics(statistics)
                .with_ordering(ordering))
        })
        .boxed()
        .buffer_unordered(meta_fetch_concurrency);

    let (file_group, inexact_stats) =
        get_files_with_limit(files, limit, source.config().collect_stat).await?;

    let threshold = ctx.config_options().optimizer.preserve_file_partitions;

    let (file_groups, grouped_by_partition) = if threshold > 0 && !partition_cols.is_empty() {
        let grouped = file_group.group_by_partition_values(source.config().target_partitions);
        if grouped.len() >= threshold {
            (grouped, true)
        } else {
            let all_files: Vec<_> = grouped.into_iter().flat_map(|g| g.into_inner()).collect();
            (
                FileGroup::new(all_files).split_files(source.config().target_partitions),
                false,
            )
        }
    } else {
        (
            file_group.split_files(source.config().target_partitions),
            false,
        )
    };

    let (file_groups, stats) = datafusion_datasource::compute_all_files_statistics(
        file_groups,
        source.config().schema.table_schema().clone(),
        source.config().collect_stat,
        inexact_stats,
    )?;

    Ok(ListFilesResult {
        file_groups,
        statistics: stats,
        grouped_by_partition,
    })
}

async fn do_collect_statistics_and_ordering(
    source: &ListingTableSource,
    ctx: &dyn Session,
    store: &Arc<dyn ObjectStore>,
    part_file: &datafusion_datasource::PartitionedFile,
) -> datafusion_common::Result<(Arc<Statistics>, Option<LexOrdering>)> {
    let path = &part_file.object_meta.location;
    let meta = &part_file.object_meta;
    let collected_statistics = source.collected_statistics();
    // DF54 changed the file-statistics cache key from `Path` to `TableScopedPath`.
    // The listing planner has no table reference at this point, so we scope the
    // entries under `None` — equivalent to the pre-DF54 path-only key.
    let cache_key = TableScopedPath {
        table: None,
        path: path.clone(),
    };

    if let Some(cached) = collected_statistics.get(&cache_key) {
        if cached.is_valid_for(meta) {
            return Ok((Arc::clone(&cached.statistics), cached.ordering.clone()));
        }
    }

    let file_meta = source
        .config()
        .read_format
        .infer_file_meta(
            ctx,
            store,
            source.config().schema.file_schema().clone(),
            meta,
            source
                .config()
                .compression
                .unwrap_or(CompressionTypeVariant::UNCOMPRESSED),
        )
        .await?;
    let statistics = Arc::new(file_meta.statistics);

    collected_statistics.put(
        &cache_key,
        CachedFileMetadata::new(
            meta.clone(),
            Arc::clone(&statistics),
            file_meta.ordering.clone(),
        ),
    );

    Ok((statistics, file_meta.ordering))
}

async fn get_files_with_limit(
    files: impl Stream<Item = datafusion_common::Result<datafusion_datasource::PartitionedFile>>,
    limit: Option<usize>,
    collect_stats: bool,
) -> datafusion_common::Result<(FileGroup, bool)> {
    let mut file_group = FileGroup::default();
    let mut all_files = Box::pin(files.fuse());
    enum ProcessingState {
        ReadingFiles,
        ReachedLimit,
    }

    let mut state = ProcessingState::ReadingFiles;
    let mut num_rows = Precision::Absent;

    while let Some(file_result) = all_files.next().await {
        if matches!(state, ProcessingState::ReachedLimit) {
            break;
        }

        let file = file_result?;

        if collect_stats {
            if let Some(file_stats) = &file.statistics {
                num_rows = if file_group.is_empty() {
                    file_stats.num_rows
                } else {
                    num_rows.add(&file_stats.num_rows)
                };
            }
        }

        file_group.push(file);

        if let Some(limit) = limit {
            if let Precision::Exact(row_count) = num_rows {
                if row_count > limit {
                    state = ProcessingState::ReachedLimit;
                }
            }
        }
    }

    let inexact_stats = all_files.next().await.is_some();
    Ok((file_group, inexact_stats))
}
