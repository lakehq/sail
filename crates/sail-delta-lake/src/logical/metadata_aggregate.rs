use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{Array, UInt64Array};
use datafusion::arrow::compute::take_record_batch;
use datafusion::catalog::Session;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::functions_aggregate::count::Count;
use datafusion::functions_aggregate::expr_fn::sum;
use datafusion::logical_expr::expr_rewriter::unnormalize_cols;
use datafusion::logical_expr::logical_plan::{
    Aggregate, EmptyRelation, Extension, FetchType, Limit, Projection, SkipType, TableScan, Union,
};
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{
    Expr, LogicalPlan, LogicalPlanBuilder, TableProviderFilterPushDown, TableScanBuilder,
    TableSource, when,
};
use sail_common_datafusion::logical_rewriter::LogicalRewriter;
use sail_common_datafusion::metadata_aggregate::{AggregateInput, rewrite_aggregate};
use sail_logical_plan::range::RangeNode;

use crate::datasource::get_pushdown_filters;
use crate::datasource::pruning::partition_filter_mask;
use crate::logical::table_source::{
    DeltaFileSelection, DeltaMetadataAggregateSource, DeltaTableSource,
};
use crate::snapshot::{GroupedCountMetadata, GroupedCountMetadataRow, SnapshotPruningStats};

const MAX_METADATA_GROUPS: usize = 100_000;
const LARGE_METADATA_SAVINGS_BYTES: u64 = 64 * 1024 * 1024;
const LARGE_METADATA_SAVINGS_ROWS: u64 = 8192;
const COUNT_WEIGHT_COLUMN: &str = "__sail_delta_count_weight";
const COUNT_SUM_COLUMN: &str = "__sail_delta_count_sum";

#[derive(Debug, Default)]
pub struct DeltaMetadataAggregateRewriter;

#[async_trait::async_trait]
impl LogicalRewriter for DeltaMetadataAggregateRewriter {
    fn name(&self) -> &str {
        "delta_metadata_aggregate"
    }

    async fn rewrite(
        &self,
        plan: LogicalPlan,
        session: &dyn Session,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up_with_subqueries(|plan| {
            let rewritten = match &plan {
                LogicalPlan::Aggregate(aggregate) => {
                    match rewrite_exact_ungrouped_aggregate(aggregate, session)? {
                        Some(rewritten) => Some(rewritten),
                        None => rewrite_metadata_grouping(aggregate, session)?,
                    }
                }
                LogicalPlan::Limit(limit) => rewrite_empty_projection_limit(limit, session)?,
                _ => None,
            };
            match rewritten {
                Some(rewritten) => Ok(Transformed::yes(rewritten)),
                None => Ok(Transformed::no(plan)),
            }
        })
    }
}

fn rewrite_empty_projection_limit(
    limit: &Limit,
    session: &dyn Session,
) -> Result<Option<LogicalPlan>> {
    if !limit.input.schema().fields().is_empty() {
        return Ok(None);
    }
    let (SkipType::Literal(skip), FetchType::Literal(Some(fetch))) =
        (limit.get_skip_type()?, limit.get_fetch_type()?)
    else {
        return Ok(None);
    };
    let Some(required) = skip.checked_add(fetch) else {
        return Ok(None);
    };
    let Some(input) = AggregateInput::try_new(limit.input.as_ref()) else {
        return Ok(None);
    };
    let scan = input.scan();
    let Some(source) = scan.source.downcast_ref::<DeltaTableSource>() else {
        return Ok(None);
    };
    let Some(indices) = metadata_file_indices(scan, source, session)? else {
        return Ok(None);
    };
    let mut known_rows = 0usize;
    let mut complete = true;
    for index in indices {
        match source.snapshot().adds()[index].num_logical_records() {
            Some(rows) => known_rows = known_rows.saturating_add(rows),
            None => complete = false,
        }
        if known_rows >= required {
            break;
        }
    }
    let known_rows = known_rows.min(scan.fetch.unwrap_or(usize::MAX));
    if !complete && known_rows < required {
        return Ok(None);
    }
    let output_rows = known_rows.saturating_sub(skip).min(fetch);
    if output_rows <= 1 {
        return Ok(Some(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: output_rows == 1,
            schema: Arc::clone(limit.input.schema()),
        })));
    }
    let Ok(end) = i64::try_from(output_rows) else {
        return Ok(None);
    };
    // A range carries the cardinality without allocating a row per log record during planning.
    let rows = LogicalPlan::Extension(Extension {
        node: Arc::new(RangeNode::try_new("__sail_delta_row".into(), 0, end, 1, 1)?),
    });
    Ok(Some(LogicalPlan::Projection(
        Projection::try_new_with_schema(vec![], Arc::new(rows), Arc::clone(limit.input.schema()))?,
    )))
}

fn has_partition_filters(scan: &TableScan, source: &DeltaTableSource) -> bool {
    if scan
        .filters
        .iter()
        .flat_map(Expr::column_refs)
        .any(|column| {
            source
                .schema()
                .field_with_name(&column.name)
                .ok()
                .map(|field| field.data_type().clone())
                != source
                    .snapshot()
                    .schema()
                    .field_with_name(&column.name)
                    .ok()
                    .map(|field| field.data_type().clone())
        })
    {
        return false;
    }
    get_pushdown_filters(
        &scan.filters.iter().collect::<Vec<_>>(),
        source.snapshot().metadata().partition_columns(),
    )
    .iter()
    .all(|pushdown| *pushdown == TableProviderFilterPushDown::Exact)
}

fn metadata_file_indices(
    scan: &TableScan,
    source: &DeltaTableSource,
    session: &dyn Session,
) -> Result<Option<Vec<usize>>> {
    let snapshot = source.snapshot();
    if !snapshot.load_config().require_files || !has_partition_filters(scan, source) {
        return Ok(None);
    }
    let indices = match source.file_selection() {
        DeltaFileSelection::Snapshot => (0..snapshot.adds().len()).collect::<Vec<_>>(),
        DeltaFileSelection::Selected(indices) => indices.to_vec(),
    };
    let Some(predicate) = conjunction(unnormalize_cols(scan.filters.clone())) else {
        return Ok(Some(indices));
    };
    if indices.is_empty() {
        return Ok(Some(indices));
    }
    let values = partition_filter_mask(
        session,
        snapshot,
        snapshot.schema(),
        snapshot.adds(),
        predicate,
    )?;
    Ok(Some(
        indices
            .into_iter()
            .filter(|&index| values.is_valid(index) && values.value(index))
            .collect(),
    ))
}

fn rewrite_exact_ungrouped_aggregate(
    aggregate: &Aggregate,
    session: &dyn Session,
) -> Result<Option<LogicalPlan>> {
    if !aggregate.group_expr.is_empty() || aggregate.aggr_expr.is_empty() {
        return Ok(None);
    }
    let Some(input) = AggregateInput::try_new(aggregate.input.as_ref()) else {
        return Ok(None);
    };
    let scan = input.scan();
    if scan.fetch.is_some() {
        return Ok(None);
    }
    let Some(source) = scan.source.downcast_ref::<DeltaTableSource>() else {
        return Ok(None);
    };
    let Some(indices) = metadata_file_indices(scan, source, session)? else {
        return Ok(None);
    };
    let Ok(files) = source.snapshot().files_batch() else {
        return Ok(None);
    };
    let selected = if indices.len() == files.num_rows() {
        files.clone()
    } else {
        let indices = UInt64Array::from_iter_values(indices.into_iter().map(|index| index as u64));
        take_record_batch(files, &indices)?
    };
    let snapshot_stats = SnapshotPruningStats::try_new(&selected, source.snapshot())?;
    rewrite_aggregate(aggregate, &snapshot_stats)
}

fn rewrite_metadata_grouping(
    aggregate: &Aggregate,
    session: &dyn Session,
) -> Result<Option<LogicalPlan>> {
    let distinct = !aggregate.group_expr.is_empty() && aggregate.aggr_expr.is_empty();
    let count = aggregate.aggr_expr.len() == 1 && is_row_count(&aggregate.aggr_expr[0]);
    if !distinct && !count {
        return Ok(None);
    }
    let Some(input) = AggregateInput::try_new(aggregate.input.as_ref()) else {
        return Ok(None);
    };
    let scan = input.scan();
    if scan.fetch.is_some() {
        return Ok(None);
    }
    let Some(source) = scan.source.downcast_ref::<DeltaTableSource>() else {
        return Ok(None);
    };
    if !has_partition_filters(scan, source) {
        return Ok(None);
    }

    let group_columns = aggregate
        .group_expr
        .iter()
        .map(|expression| {
            let Expr::Column(column) = expression else {
                return None;
            };
            input.source_column(column, source.snapshot().schema())
        })
        .collect::<Option<Vec<_>>>();
    let Some(group_columns) = group_columns else {
        return Ok(None);
    };
    if group_columns.iter().collect::<HashSet<_>>().len() != group_columns.len() {
        return Ok(None);
    }
    let partition_columns = source.snapshot().metadata().partition_columns();
    if distinct
        && group_columns
            .iter()
            .any(|name| !partition_columns.contains(name))
    {
        return Ok(None);
    }

    let weighted_projection = weighted_projection_expressions(&aggregate.group_expr);
    let weighted_schema =
        Projection::try_new(weighted_projection.clone(), Arc::clone(&aggregate.input))?.schema;
    if weighted_schema
        .fields()
        .iter()
        .map(|field| field.name())
        .collect::<HashSet<_>>()
        .len()
        != weighted_schema.fields().len()
    {
        return Ok(None);
    }

    let weighted_input = if source.snapshot().load_config().require_files {
        let Some(indices) = metadata_file_indices(scan, source, session)? else {
            return Ok(None);
        };
        let metadata = if indices.len() == source.snapshot().adds().len() {
            source
                .snapshot()
                .grouped_count_metadata(&group_columns, MAX_METADATA_GROUPS)
        } else {
            let adds = indices
                .iter()
                .map(|&index| source.snapshot().adds()[index].clone())
                .collect::<Vec<_>>();
            source
                .snapshot()
                .summarize_metadata_files(&adds, &group_columns, MAX_METADATA_GROUPS)
        };
        let Some(metadata) = metadata else {
            return Ok(None);
        };
        if !worth_rewriting(&metadata) {
            return Ok(None);
        }
        let metadata_branch = build_metadata_branch(metadata.rows, &weighted_schema)?;
        let residual_branch = if metadata.residual_file_indices.is_empty() {
            None
        } else {
            let selected_source = source
                .try_select_files(
                    metadata
                        .residual_file_indices
                        .into_iter()
                        .map(|index| indices[index])
                        .collect(),
                )
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            let residual_input = input.replace_source(Arc::new(selected_source))?;
            Some(LogicalPlan::Projection(Projection::try_new(
                weighted_projection,
                Arc::new(residual_input),
            )?))
        };
        match (metadata_branch, residual_branch) {
            (Some(metadata), Some(residual)) => LogicalPlan::Union(Union::try_new(vec![
                Arc::new(metadata),
                Arc::new(residual),
            ])?),
            (Some(metadata), None) => metadata,
            (None, Some(residual)) => residual,
            (None, None) => LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: Arc::clone(&weighted_schema),
            }),
        }
    } else {
        let metadata_source = DeltaMetadataAggregateSource {
            table: source.clone(),
            filters: unnormalize_cols(scan.filters.clone()),
            group_columns,
            schema: Arc::new(weighted_schema.as_arrow().clone()),
        };
        LogicalPlan::TableScan(
            TableScanBuilder::new(scan.table_name.clone(), Arc::new(metadata_source)).build()?,
        )
    };

    // Metadata rows carry a file's logical count; residual rows carry weight one.
    let weighted_columns = weighted_input.schema().columns();
    let group_count = aggregate.group_expr.len();
    let group_expr = weighted_columns[..group_count]
        .iter()
        .cloned()
        .map(Expr::Column)
        .collect::<Vec<_>>();
    let aggregate_expr = if count {
        let count_weight = Expr::Column(weighted_columns[group_count].clone());
        vec![sum(count_weight).alias(COUNT_SUM_COLUMN)]
    } else {
        vec![]
    };
    let weighted_aggregate = LogicalPlan::Aggregate(Aggregate::try_new(
        Arc::new(weighted_input),
        group_expr,
        aggregate_expr,
    )?);

    let mut output_expr = weighted_aggregate.schema().columns()[..group_count]
        .iter()
        .cloned()
        .map(Expr::Column)
        .collect::<Vec<_>>();
    if count {
        let count_sum = Expr::Column(weighted_aggregate.schema().columns()[group_count].clone());
        let count_sum = if group_count == 0 {
            when(
                count_sum.clone().is_null(),
                Expr::Literal(ScalarValue::Int64(Some(0)), None),
            )
            .otherwise(count_sum)?
        } else {
            count_sum
        };
        output_expr.push(count_sum.alias(aggregate.schema.field(group_count).name()));
    }
    Ok(Some(LogicalPlan::Projection(
        Projection::try_new_with_schema(
            output_expr,
            Arc::new(weighted_aggregate),
            Arc::clone(&aggregate.schema),
        )?,
    )))
}

fn is_row_count(expression: &Expr) -> bool {
    let expression = match expression {
        Expr::Alias(alias) => alias.expr.as_ref(),
        expression => expression,
    };
    let Expr::AggregateFunction(function) = expression else {
        return false;
    };
    function.func.inner().is::<Count>()
        && !function.params.distinct
        && function.params.filter.is_none()
        && function.params.order_by.is_empty()
        && function.params.null_treatment.is_none()
        && matches!(
            function.params.args.as_slice(),
            [Expr::Literal(value, _)] if !value.is_null()
        )
}

fn worth_rewriting(metadata: &GroupedCountMetadata) -> bool {
    if metadata.metadata_file_count == 0 {
        return false;
    }
    if metadata.residual_file_indices.is_empty()
        || metadata.metadata_row_count >= LARGE_METADATA_SAVINGS_ROWS
    {
        return true;
    }
    let residual_files = metadata.residual_file_indices.len();
    let substantial_file_reduction =
        metadata.metadata_file_count > residual_files.saturating_mul(2);
    let substantial_byte_reduction = metadata.metadata_bytes >= LARGE_METADATA_SAVINGS_BYTES;
    (metadata.metadata_file_count >= 2 || substantial_byte_reduction)
        && (substantial_file_reduction || substantial_byte_reduction)
}

fn weighted_projection_expressions(group_expr: &[Expr]) -> Vec<Expr> {
    let mut expressions = group_expr.to_vec();
    expressions.push(Expr::Literal(ScalarValue::Int64(Some(1)), None).alias(COUNT_WEIGHT_COLUMN));
    expressions
}

fn build_metadata_branch(
    rows: Vec<GroupedCountMetadataRow>,
    weighted_schema: &datafusion::common::DFSchemaRef,
) -> Result<Option<LogicalPlan>> {
    if rows.is_empty() {
        return Ok(None);
    }
    let values = rows
        .into_iter()
        .map(|row| {
            let mut values = row
                .group_values
                .into_iter()
                .map(|value| Expr::Literal(value, None))
                .collect::<Vec<_>>();
            values.push(Expr::Literal(ScalarValue::Int64(Some(row.count)), None));
            values
        })
        .collect::<Vec<_>>();
    let values = LogicalPlanBuilder::values(values)?.build()?;
    let projection = values
        .schema()
        .columns()
        .into_iter()
        .zip(weighted_schema.fields())
        .map(|(column, field)| Expr::Column(column).alias(field.name()))
        .collect::<Vec<_>>();
    Ok(Some(LogicalPlan::Projection(Projection::try_new(
        projection,
        Arc::new(values),
    )?)))
}
