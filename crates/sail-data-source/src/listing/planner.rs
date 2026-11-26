// SPDX-License-Identifier: Apache-2.0
// SPDX-FileComment: Credit: https://github.com/apache/datafusion/blob/eae7bf4fa1c037c0a065d1f36d0669f5bb97a9cf/datafusion/catalog-listing/src/table.rs

// The listing table planner is adapted from the DataFusion `ListingTable` implementation,
// specifically the `TableProvider::scan_with_args` trait method.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Metadata};
use datafusion::catalog::Session;
use datafusion::datasource::listing::helpers::{filter_partitioned_file, pruned_partition_list};
use datafusion::datasource::physical_plan::{FileOutputMode, FileSinkConfig};
use datafusion::execution::cache::cache_manager::CachedFileMetadata;
use datafusion::execution::cache::{SchemaFingerprint, TableScopedPath};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::expr_rewriter::unnormalize_cols;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{Expr, LogicalPlan, ScalarUDF, TableScan, UserDefinedLogicalNode};
use datafusion::physical_expr::expressions::{Column, Literal, cast};
use datafusion::physical_expr::{
    Partitioning, PhysicalExpr, ScalarFunctionExpr, create_lex_ordering,
};
use datafusion::physical_expr_common::sort_expr::LexOrdering;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_common::config::HIVE_DEFAULT_PARTITION_NAME;
use datafusion_common::stats::Precision;
use datafusion_common::{DFSchema, ScalarValue, Statistics, internal_err, plan_err};
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::{
    FileScanConfig, output_range_partitioning_from_split_points,
};
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::{ListingTableUrl, PartitionedFile};
use futures::{Stream, StreamExt, TryStreamExt, future, stream};
use object_store::ObjectStore;
use sail_cache::scope::{scoped_listing_url, scoped_path};
use sail_common_datafusion::datasource::create_sort_order;
use sail_common_datafusion::rename::physical_plan::rename_physical_plan;
use sail_common_datafusion::schema_evolution::StructFieldMatching;
use sail_common_datafusion::streaming::event::schema::is_flow_event_schema;
use sail_physical_plan::barrier::BarrierExec;

use crate::listing::delete::FileDeleteExec;
use crate::listing::partition::qualified_partition_path;
use crate::listing::partition_write::{HivePartitionFloat, HivePartitionWriteExec};
use crate::listing::registered::{RegisteredTableSource, prepare_registered_scan};
use crate::listing::source::{ListingScanInput, ListingSinkInput};
use crate::listing::table::ListingTableSource;
use crate::listing::utils::{
    has_hidden_path_component, in_partition_directories, is_direct_input, is_metadata_file,
    list_partition_files, matches_path_glob_filter,
};
use crate::listing::write::{FileWriteNode, FileWriteOptions};

/// Result of a file listing operation for listing table scans.
#[derive(Debug)]
struct ListFilesResult {
    file_groups: Vec<FileGroup>,
    statistics: Statistics,
    partitioning: Option<Partitioning>,
}

/// Physical planner for logical listing table scans.
///
/// Plans `ListingTableSource` table scans directly without an intermediate `TableProvider`.
#[derive(Debug, Default)]
pub struct ListingPhysicalPlanner;

#[async_trait]
impl ExtensionPlanner for ListingPhysicalPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session: &dyn Session,
        _planning_ctx: &PhysicalPlanningContext,
    ) -> datafusion_common::Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(node) = node.as_any().downcast_ref::<FileWriteNode>() {
            let [logical_input] = logical_inputs else {
                return internal_err!("FileWriteNode requires exactly one logical input");
            };
            let [physical_input] = physical_inputs else {
                return internal_err!("FileWriteNode requires exactly one physical input");
            };
            return plan_file_write(session, logical_input, physical_input.clone(), node)
                .await
                .map(Some);
        }
        Ok(None)
    }

    async fn plan_table_scan(
        &self,
        _planner: &dyn PhysicalPlanner,
        scan: &TableScan,
        session: &dyn Session,
        _planning_ctx: &PhysicalPlanningContext,
    ) -> datafusion_common::Result<Option<Arc<dyn ExecutionPlan>>> {
        let filters = unnormalize_cols(scan.filters.clone());
        let resolved;
        let source = if let Some(registered) = scan.source.downcast_ref::<RegisteredTableSource>() {
            let Some(source) = registered.resolve(session, &filters).await? else {
                return Ok(Some(Arc::new(EmptyExec::new(Arc::clone(
                    scan.projected_schema.inner(),
                )))));
            };
            resolved = source;
            &resolved
        } else {
            &scan.source
        };
        let Some(source) = source.downcast_ref::<ListingTableSource>() else {
            return Ok(None);
        };

        let projection = source.scan_projection(scan.projection.as_deref());
        let limit = scan.fetch;

        let (partition_filters, filters) = source.split_partition_filters(filters);

        let statistic_file_limit = if filters.is_empty() { limit } else { None };

        let prepared = match source.registered_read {
            Some(options) => Some(prepare_registered_scan(source, session, options).await?),
            None => None,
        };
        let (source, remaining_filters) = match &prepared {
            Some(prepared) => (prepared, &[][..]),
            None => (source, partition_filters.as_slice()),
        };
        let ListFilesResult {
            mut file_groups,
            statistics,
            mut partitioning,
        } = list_files_for_scan(source, session, remaining_filters, statistic_file_limit).await?;

        let table_schema = source.config().schema.table_schema();
        if file_groups.is_empty() {
            let projected_schema = Arc::clone(scan.projected_schema.inner());
            return Ok(Some(Arc::new(EmptyExec::new(projected_schema))));
        }

        let output_ordering =
            try_create_output_ordering(source, session.execution_props(), &file_groups)?;

        match session
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
                    partitioning = None;
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
            .map(|(url, _)| url.object_store())
        else {
            return Ok(Some(Arc::new(EmptyExec::new(Arc::new(
                datafusion::arrow::datatypes::Schema::empty(),
            )))));
        };

        let config = source
            .config()
            .read_format
            .scan(
                session,
                ListingScanInput {
                    object_store_url,
                    file_groups,
                    constraints: source.config().constraints.clone(),
                    projection,
                    limit,
                    preserve_order: false,
                    output_ordering,
                    statistics,
                    output_partitioning: partitioning,
                    schema: source.config().schema.clone(),
                    compression: source.config().compression,
                },
            )
            .await?;

        let plan = DataSourceExec::from_data_source(config);
        let names = scan.projected_schema.fields();
        if prepared.is_some()
            && !plan
                .schema()
                .fields()
                .iter()
                .map(|f| f.name())
                .eq(names.iter().map(|f| f.name()))
        {
            let names = names
                .iter()
                .map(|field| field.name().clone())
                .collect::<Vec<_>>();
            return Ok(Some(rename_physical_plan(plan, &names)?));
        }
        Ok(Some(plan))
    }
}

async fn plan_file_write(
    session: &dyn Session,
    logical_input: &LogicalPlan,
    physical_input: Arc<dyn ExecutionPlan>,
    node: &FileWriteNode,
) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
    if is_flow_event_schema(logical_input.schema().as_arrow()) {
        return plan_err!("cannot write streaming data to listing table");
    }
    let FileWriteOptions {
        format,
        url,
        overwrite,
        partition_by,
        sort_by,
    } = node.options();
    let table_path = ListingTableUrl::try_new(url.clone(), None)?;
    let object_store_url = table_path.object_store();
    // We do not need to specify the exact data type for partition columns,
    // since the type is inferred from the record batch during writing.
    let table_partition_cols = partition_by
        .iter()
        .map(|field| (field.column.clone(), DataType::Null))
        .collect::<Vec<_>>();
    let physical_input = if table_partition_cols.is_empty() {
        physical_input
    } else {
        let schema = physical_input.schema();
        let exprs = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new(field.name(), index));
                let column = if table_partition_cols
                    .iter()
                    .any(|(name, _)| name == field.name())
                {
                    let value: Arc<dyn PhysicalExpr> = match field.data_type() {
                        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                            Arc::new(ScalarFunctionExpr::try_new(
                                Arc::new(ScalarUDF::from(HivePartitionFloat::default())),
                                vec![column],
                                &schema,
                                Arc::clone(session.config().options()),
                            )?)
                        }
                        DataType::Date64 => cast(
                            cast(column, &schema, DataType::Date32)?,
                            &schema,
                            DataType::Utf8,
                        )?,
                        _ => cast(column, &schema, DataType::Utf8)?,
                    };
                    if field.is_nullable() {
                        let null_marker =
                            Arc::new(Literal::new(ScalarValue::from(HIVE_DEFAULT_PARTITION_NAME)));
                        Arc::new(ScalarFunctionExpr::try_new(
                            datafusion::functions::core::coalesce(),
                            vec![value, null_marker],
                            &schema,
                            Arc::clone(session.config().options()),
                        )?) as Arc<dyn PhysicalExpr>
                    } else {
                        value
                    }
                } else {
                    column
                };
                Ok((column, field.name().clone()))
            })
            .collect::<datafusion_common::Result<Vec<_>>>()?;
        Arc::new(ProjectionExec::try_new(exprs, physical_input)?)
    };
    let conf = FileSinkConfig {
        original_url: url.to_string(),
        object_store_url: object_store_url.clone(),
        file_group: Default::default(),
        table_paths: vec![table_path.clone()],
        output_schema: physical_input.schema(),
        table_partition_cols,
        insert_op: InsertOp::Append,
        keep_partition_by_columns: false,
        file_extension: String::new(),
        file_output_mode: FileOutputMode::Automatic,
    };
    let sort_order = create_sort_order(session, sort_by.clone(), logical_input.schema())?;
    let plan = format
        .sink(
            session,
            ListingSinkInput {
                input: physical_input,
                sink: conf,
                sort_order,
            },
        )
        .await?;
    let plan: Arc<dyn ExecutionPlan> = if partition_by.is_empty() {
        plan
    } else {
        Arc::new(HivePartitionWriteExec::new(plan))
    };
    if *overwrite {
        let delete = Arc::new(FileDeleteExec::new(
            object_store_url,
            table_path.prefix().clone(),
        ));
        Ok(Arc::new(BarrierExec::new(vec![delete], plan)))
    } else {
        Ok(plan)
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
    let (store, object_store_url) = if let Some((url, _)) = source.config().table_paths.first() {
        (ctx.runtime_env().object_store(url)?, url.object_store())
    } else {
        return Ok(ListFilesResult {
            file_groups: vec![],
            partitioning: None,
            statistics: Statistics::new_unknown(source.config().schema.file_schema()),
        });
    };

    let partition_cols: Vec<(String, DataType)> = source
        .config()
        .schema
        .table_partition_cols()
        .iter()
        .map(|field| (field.name().clone(), field.data_type().clone()))
        .collect();

    let store_ref = store.as_ref();
    let partition_cols_ref = partition_cols.as_slice();
    let path_glob_filter = source.config().path_glob_filter.as_ref();
    let table_paths = source
        .config()
        .table_paths
        .iter()
        .map(|(url, values)| (scoped_listing_url(url, ctx.session_id()), values.as_ref()))
        .collect::<Vec<_>>();
    let partition_fields = source.config().schema.table_partition_cols();
    let base_path = source
        .config()
        .base_path
        .as_ref()
        .filter(|_| !partition_fields.is_empty());
    let direct_only = source.config().direct_files_only();
    let partition_directories_only = source.config().partition_directories_only;
    let reads_empty_files = source.config().read_format.reads_empty_files();
    let registered = table_paths.iter().any(|(_, values)| values.is_some());
    let partition_schema = base_path
        .map(|_| DFSchema::from_unqualified_fields(partition_fields.clone(), Metadata::new()))
        .transpose()?;
    // The listing helper inspects the boolean value bit. IS TRUE also tests validity.
    let predicate = datafusion_expr::utils::conjunction(filters.iter().cloned()).map(Expr::is_true);
    let partition_schema = partition_schema.as_ref();
    let predicate = predicate.as_slice();
    let (listing_filters, listing_cols) = if partition_schema.is_some() {
        // List only the selected input paths. Their partition values are relative to basePath.
        (&[][..], &[][..])
    } else {
        (filters, partition_cols_ref)
    };
    let meta_fetch_concurrency: usize =
        ctx.config_options().execution.meta_fetch_concurrency.into();
    let file_list = stream::iter(table_paths.iter())
        .map(|(table_path, registered_values)| async move {
            if let Some(values) = registered_values {
                let files = list_partition_files(table_path, store_ref, path_glob_filter)
                    .await?
                    .try_filter_map(move |object| async move {
                        if (object.size == 0 && !reads_empty_files)
                            || object.location.filename().is_some_and(is_metadata_file)
                        {
                            return Ok(None);
                        }
                        let mut file = PartitionedFile::from(object)
                            .with_table_reference(table_path.get_table_ref().clone());
                        file.partition_values.clone_from(values);
                        Ok(Some(file))
                    })
                    .boxed();
                return Ok(files);
            }
            let files = pruned_partition_list(
                ctx,
                store_ref,
                table_path,
                listing_filters,
                "",
                listing_cols,
            )
            .await?;
            // Apply listing-level file-name filters so scans agree with `inputFiles` and sampling.
            let files = files.try_filter(move |file| {
                let location = &file.object_meta.location;
                futures::future::ready(
                    (!direct_only || is_direct_input(table_path, location))
                        && !location.filename().is_some_and(is_metadata_file)
                        && (!partition_directories_only
                            || in_partition_directories(table_path, location))
                        && !has_hidden_path_component(table_path, location)
                        && matches_path_glob_filter(path_glob_filter, location),
                )
            });
            let files = if let Some(schema) = partition_schema {
                files
                    .try_filter_map(move |mut file| async move {
                        let Some(values) = file.object_meta.location.parent().and_then(|dir| {
                            base_path?.get(&qualified_partition_path(table_path, dir.as_ref()))
                        }) else {
                            return Ok(None);
                        };
                        file.partition_values.clone_from(values);
                        filter_partitioned_file(file, predicate, schema)
                    })
                    .boxed()
            } else {
                files.boxed()
            };
            Ok::<_, datafusion_common::DataFusionError>(files)
        })
        .buffered(meta_fetch_concurrency)
        .try_flatten_unordered(meta_fetch_concurrency);

    // Spark enumerates each input separately for a flat table. Partitioned and recursive
    // scans instead work from one list of distinct leaf files.
    let deduplicate = !registered
        && table_paths.len() > 1
        && (source.config().recursive_file_lookup || !partition_fields.is_empty());
    let mut seen = HashSet::new();
    let file_list = file_list.try_filter(|file| {
        future::ready(!deduplicate || seen.insert(file.object_meta.location.clone()))
    });

    let files = file_list
        .map(|part_file| async {
            let part_file = part_file?;
            let (statistics, ordering) = if source.config().collect_stat {
                do_collect_statistics_and_ordering(
                    source,
                    ctx,
                    &store,
                    &object_store_url,
                    &part_file,
                )
                .await?
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

    let mut partitioning = None;
    let file_groups = if threshold > 0 && !partition_cols.is_empty() {
        let (grouped, split_points) = file_group
            .group_by_partition_values_with_split_points(source.config().target_partitions);
        if grouped.len() >= threshold {
            partitioning = output_range_partitioning_from_split_points(
                source.config().schema.table_schema(),
                partition_fields,
                split_points,
            )?;
            grouped
        } else {
            let all_files: Vec<_> = grouped.into_iter().flat_map(|g| g.into_inner()).collect();
            FileGroup::new(all_files).split_files(source.config().target_partitions)
        }
    } else {
        file_group.split_files(source.config().target_partitions)
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
        partitioning,
    })
}

async fn do_collect_statistics_and_ordering(
    source: &ListingTableSource,
    ctx: &dyn Session,
    store: &Arc<dyn ObjectStore>,
    object_store_url: &ObjectStoreUrl,
    part_file: &datafusion_datasource::PartitionedFile,
) -> datafusion_common::Result<(Arc<Statistics>, Option<LexOrdering>)> {
    let meta = &part_file.object_meta;
    let file_schema = source.config().schema.file_schema();
    let schema_fingerprint = Arc::new(SchemaFingerprint::from_schema(file_schema.as_ref()));
    let file_statistic_cache = ctx.runtime_env().cache_manager.get_file_statistic_cache();
    let cache_key = TableScopedPath {
        table: Some(statistics_cache_table_ref(
            part_file.table_reference.as_ref(),
            file_schema.as_ref(),
            source.config().read_format.statistics_field_matching(),
        )),
        // Scope the path explicitly, since the store in the table reference above depends on
        // DataFusion's `pruned_partition_list` copying it from the scoped listing URL.
        path: scoped_path(object_store_url, ctx.session_id(), &meta.location),
    };

    if let Some(cached) = file_statistic_cache
        .as_ref()
        .and_then(|x| x.get(&cache_key))
    {
        // Skip a cached entry whose column count differs to avoid an out-of-bounds panic
        if cached.is_valid_for(meta, &schema_fingerprint)
            && cached.statistics.column_statistics.len() == file_schema.fields().len()
        {
            return Ok((Arc::clone(&cached.statistics), cached.ordering.clone()));
        }
    }

    let file_meta = source
        .config()
        .read_format
        .infer_file_meta(
            ctx,
            store,
            object_store_url,
            meta,
            source.config().schema.file_schema().clone(),
            source.config().compression,
        )
        .await?;
    let statistics = Arc::new(file_meta.statistics);

    file_statistic_cache.as_ref().map(|x| {
        x.put(
            &cache_key,
            CachedFileMetadata::new(
                meta.clone(),
                schema_fingerprint,
                Arc::clone(&statistics),
                file_meta.ordering.clone(),
            ),
        )
    });

    Ok((statistics, file_meta.ordering))
}

fn statistics_cache_table_ref(
    table: Option<&datafusion_common::TableReference>,
    schema: &arrow::datatypes::Schema,
    matching: StructFieldMatching,
) -> datafusion_common::TableReference {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();

    std::hash::Hash::hash(&matching, &mut hasher);
    for field in schema.fields() {
        std::hash::Hash::hash(field.name(), &mut hasher);
        std::hash::Hash::hash(&field.data_type().to_string(), &mut hasher);
        std::hash::Hash::hash(&field.is_nullable(), &mut hasher);
    }

    let table = table
        .map(ToString::to_string)
        .unwrap_or_else(|| "listing".to_string());

    datafusion_common::TableReference::bare(format!(
        "{table}_{:016x}",
        std::hash::Hasher::finish(&hasher)
    ))
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

        if collect_stats && let Some(file_stats) = &file.statistics {
            num_rows = if file_group.is_empty() {
                file_stats.num_rows
            } else {
                num_rows.add(&file_stats.num_rows)
            };
        }

        file_group.push(file);

        if let Some(limit) = limit
            && let Precision::Exact(row_count) = num_rows
            && row_count > limit
        {
            state = ProcessingState::ReachedLimit;
        }
    }

    let inexact_stats = all_files.next().await.is_some();
    Ok((file_group, inexact_stats))
}
