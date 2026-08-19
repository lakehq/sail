use std::collections::HashSet;
use std::sync::Arc;

use datafusion::common::tree_node::Transformed;
use datafusion::common::{Column, DataFusionError, Result, ScalarValue};
use datafusion::functions_aggregate::expr_fn::sum;
use datafusion::logical_expr::logical_plan::{
    Aggregate, EmptyRelation, Projection, TableScan, Union,
};
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, TableSource};
use log::debug;
use sail_common_datafusion::logical_rewriter::LogicalRewriter;

use crate::logical::table_source::{DeltaFileSelection, DeltaTableSource};
use crate::snapshot::{GroupedCountMetadata, GroupedCountMetadataRow};

const MAX_METADATA_GROUPS: usize = 100_000;
const LARGE_METADATA_SAVINGS_BYTES: u64 = 64 * 1024 * 1024;
const COUNT_WEIGHT_COLUMN: &str = "__sail_delta_count_weight";
const COUNT_SUM_COLUMN: &str = "__sail_delta_count_sum";

#[derive(Debug, Default)]
pub struct DeltaPartialGroupedAggregateRewriter;

impl LogicalRewriter for DeltaPartialGroupedAggregateRewriter {
    fn name(&self) -> &str {
        "delta_partial_grouped_metadata_aggregate"
    }

    fn rewrite(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up_with_subqueries(|plan| {
            let LogicalPlan::Aggregate(aggregate) = &plan else {
                return Ok(Transformed::no(plan));
            };
            match rewrite_grouped_count(aggregate)? {
                Some(rewritten) => Ok(Transformed::yes(rewritten)),
                None => Ok(Transformed::no(plan)),
            }
        })
    }
}

enum DeltaAggregateInput<'a> {
    Scan(&'a TableScan),
    Projection {
        projection: &'a Projection,
        scan: &'a TableScan,
    },
}

impl<'a> DeltaAggregateInput<'a> {
    fn try_new(plan: &'a LogicalPlan) -> Option<Self> {
        match plan {
            LogicalPlan::TableScan(scan) => Some(Self::Scan(scan)),
            LogicalPlan::Projection(projection) => match projection.input.as_ref() {
                LogicalPlan::TableScan(scan) => Some(Self::Projection { projection, scan }),
                _ => None,
            },
            _ => None,
        }
    }

    fn scan(&self) -> &'a TableScan {
        match self {
            Self::Scan(scan) | Self::Projection { scan, .. } => scan,
        }
    }

    fn source_column(&self, aggregate_column: &Column) -> Option<String> {
        let scan_column = match self {
            Self::Scan(_) => aggregate_column,
            Self::Projection { projection, .. } => {
                let index = projection.schema.index_of_column(aggregate_column).ok()?;
                projected_column(projection.expr.get(index)?)?
            }
        };
        let scan = self.scan();
        let projected_index = scan.projected_schema.index_of_column(scan_column).ok()?;
        let source_index = match &scan.projection {
            Some(projection) => *projection.get(projected_index)?,
            None => projected_index,
        };
        scan.source
            .schema()
            .fields()
            .get(source_index)
            .map(|field| field.name().clone())
    }

    fn replace_source(&self, source: Arc<dyn TableSource>) -> Result<LogicalPlan> {
        let scan = self.scan();
        let scan = LogicalPlan::TableScan(TableScan::try_new(
            scan.table_name.clone(),
            source,
            scan.projection.clone(),
            scan.filters.clone(),
            scan.fetch,
        )?);
        match self {
            Self::Scan(_) => Ok(scan),
            Self::Projection { projection, .. } => {
                Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
                    projection.expr.clone(),
                    Arc::new(scan),
                    Arc::clone(&projection.schema),
                )?))
            }
        }
    }
}

fn rewrite_grouped_count(aggregate: &Aggregate) -> Result<Option<LogicalPlan>> {
    if aggregate.group_expr.is_empty()
        || aggregate.aggr_expr.len() != 1
        || !is_row_count(&aggregate.aggr_expr[0])
    {
        return Ok(None);
    }
    let Some(input) = DeltaAggregateInput::try_new(aggregate.input.as_ref()) else {
        return Ok(None);
    };
    let scan = input.scan();
    if !scan.filters.is_empty() || scan.fetch.is_some() {
        return Ok(None);
    }
    let Some(source) = scan.source.downcast_ref::<DeltaTableSource>() else {
        return Ok(None);
    };
    if !matches!(source.file_selection(), DeltaFileSelection::Snapshot) {
        return Ok(None);
    }

    let group_columns = aggregate
        .group_expr
        .iter()
        .map(|expression| {
            let Expr::Column(column) = expression else {
                return None;
            };
            input.source_column(column)
        })
        .collect::<Option<Vec<_>>>();
    let Some(group_columns) = group_columns else {
        return Ok(None);
    };
    if group_columns.iter().collect::<HashSet<_>>().len() != group_columns.len() {
        return Ok(None);
    }

    let Some(metadata) = source
        .snapshot()
        .grouped_count_metadata(&group_columns, MAX_METADATA_GROUPS)
    else {
        return Ok(None);
    };
    if !worth_rewriting(&metadata) {
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

    let metadata_file_count = metadata.metadata_file_count;
    let metadata_group_count = metadata.rows.len();
    let residual_file_count = metadata.residual_file_indices.len();
    let metadata_bytes = metadata.metadata_bytes;
    let residual_bytes = metadata.residual_bytes;
    let metadata_branch = build_metadata_branch(metadata.rows, &weighted_schema)?;
    let residual_branch = if metadata.residual_file_indices.is_empty() {
        None
    } else {
        let selected_source = source
            .try_select_files(metadata.residual_file_indices)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let residual_input = input.replace_source(Arc::new(selected_source))?;
        Some(LogicalPlan::Projection(Projection::try_new(
            weighted_projection,
            Arc::new(residual_input),
        )?))
    };

    let weighted_input = match (metadata_branch, residual_branch) {
        (Some(metadata), Some(residual)) => LogicalPlan::Union(Union::try_new(vec![
            Arc::new(metadata),
            Arc::new(residual),
        ])?),
        (Some(metadata), None) => metadata,
        (None, Some(residual)) => residual,
        (None, None) => {
            return Ok(Some(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: Arc::clone(&aggregate.schema),
            })));
        }
    };

    // Metadata rows carry a whole file's logical row count while residual rows carry one. A
    // single SUM above the union therefore reuses DataFusion's normal partial/final aggregation.
    let weighted_columns = weighted_input.schema().columns();
    let group_count = aggregate.group_expr.len();
    let group_expr = weighted_columns[..group_count]
        .iter()
        .cloned()
        .map(Expr::Column)
        .collect::<Vec<_>>();
    let count_weight = Expr::Column(weighted_columns[group_count].clone());
    let weighted_aggregate = LogicalPlan::Aggregate(Aggregate::try_new(
        Arc::new(weighted_input),
        group_expr,
        vec![sum(count_weight).alias(COUNT_SUM_COLUMN)],
    )?);

    let mut output_expr = weighted_aggregate.schema().columns()[..group_count]
        .iter()
        .cloned()
        .map(Expr::Column)
        .collect::<Vec<_>>();
    let count_sum = Expr::Column(weighted_aggregate.schema().columns()[group_count].clone());
    output_expr.push(count_sum.alias(aggregate.schema.field(group_count).name()));

    debug!(
        "rewrote Delta grouped count using {metadata_file_count} metadata files ({metadata_bytes} bytes, {metadata_group_count} groups) and {residual_file_count} residual files ({residual_bytes} bytes)"
    );
    Ok(Some(LogicalPlan::Projection(
        Projection::try_new_with_schema(
            output_expr,
            Arc::new(weighted_aggregate),
            Arc::clone(&aggregate.schema),
        )?,
    )))
}

fn projected_column(expression: &Expr) -> Option<&Column> {
    match expression {
        Expr::Column(column) => Some(column),
        Expr::Alias(alias) => projected_column(alias.expr.as_ref()),
        _ => None,
    }
}

fn is_row_count(expression: &Expr) -> bool {
    let expression = match expression {
        Expr::Alias(alias) => alias.expr.as_ref(),
        expression => expression,
    };
    let Expr::AggregateFunction(function) = expression else {
        return false;
    };
    function.func.name().eq_ignore_ascii_case("count")
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
