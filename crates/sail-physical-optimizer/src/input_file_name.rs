use std::sync::Arc;

use datafusion::arrow::datatypes::{Schema, SchemaBuilder};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Constraints, JoinType, Result, Statistics};
use datafusion::config::ConfigOptions;
use datafusion::datasource::physical_plan::{FileScanConfig, FileScanConfigBuilder, FileSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_expr::ScalarFunctionExpr;
use datafusion::physical_expr::expressions::{CaseExpr, Column, IsNotNullExpr, LambdaVariable};
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::filter::{FilterExec, FilterExecBuilder};
use datafusion::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, PartitionMode,
};
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::unnest::UnnestExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning, PhysicalExpr};
use sail_common_datafusion::input_file::{
    InputFileMetadata, InputFileMetadataSource, expression_references_input_file_metadata,
    is_input_file_metadata_function, projection_references_input_file_metadata,
    rewrite_input_file_metadata_projection,
};
use sail_physical_plan::repartition::ExplicitRepartitionExec;

use crate::projection_pushdown::ContextSafeProjectionPushdown;

/// Installs a per-file projection boundary before DataFusion pushes scan-metadata functions down.
#[derive(Debug, Default)]
pub struct WrapInputFileMetadataSource;

impl WrapInputFileMetadataSource {
    pub fn new() -> Self {
        Self
    }
}

impl PhysicalOptimizerRule for WrapInputFileMetadataSource {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !plan_references_input_file_metadata(&plan)? {
            return Ok(plan);
        }

        plan.transform_up(|plan| {
            let Some(scan) = plan.downcast_ref::<DataSourceExec>() else {
                return Ok(Transformed::no(plan));
            };
            let Some(config) = scan.data_source().downcast_ref::<FileScanConfig>() else {
                return Ok(Transformed::no(plan));
            };
            if config.file_source.is::<InputFileMetadataSource>() {
                return Ok(Transformed::no(plan));
            }

            let source = Arc::new(InputFileMetadataSource::try_new(Arc::clone(
                &config.file_source,
            ))?) as Arc<dyn FileSource>;
            let statistics = Statistics::new_unknown(source.table_schema().table_schema());
            let mut config = FileScanConfigBuilder::from(config.clone())
                .with_source(source)
                .with_statistics(statistics)
                .with_output_ordering(Vec::new())
                .with_output_partitioning(None)
                .build();
            config.constraints = Constraints::default();
            let scan = scan.clone().with_data_source(Arc::new(config));
            Ok(Transformed::yes(Arc::new(scan) as Arc<dyn ExecutionPlan>))
        })
        .map(|result| result.data)
    }

    fn name(&self) -> &str {
        "WrapInputFileMetadataSource"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Pushes scan-metadata projections through operators before repartitioning is enforced.
#[derive(Debug, Default)]
pub struct PushDownInputFileMetadata {
    projection_pushdown: ContextSafeProjectionPushdown,
}

impl PushDownInputFileMetadata {
    pub fn new() -> Self {
        Self::default()
    }
}

impl PhysicalOptimizerRule for PushDownInputFileMetadata {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !plan_references_input_file_metadata(&plan)? {
            return Ok(plan);
        }
        let mut plan = plan;
        loop {
            plan = self.projection_pushdown.optimize(plan, config)?;
            let extracted = plan.transform_up(extract_metadata_projection)?;
            let pushed = extracted
                .data
                .transform_down(push_metadata_through_file_context)?;
            plan = pushed.data;
            if !extracted.transformed && !pushed.transformed {
                return Ok(plan);
            }
        }
    }

    fn name(&self) -> &str {
        "PushDownInputFileMetadata"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn is_metadata_expression(expression: &Arc<dyn PhysicalExpr>) -> bool {
    expression
        .downcast_ref::<ScalarFunctionExpr>()
        .is_some_and(|function| is_input_file_metadata_function(function.fun()))
}

/// Keep source-dependent expressions below projections that contain lambda variables.
fn extract_metadata_projection(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let Some(projection) = plan.downcast_ref::<ProjectionExec>() else {
        return Ok(Transformed::no(plan));
    };
    if !projection_references_input_file_metadata(projection.projection_expr()) {
        return Ok(Transformed::no(plan));
    }
    let contains_lambda = projection
        .expr()
        .iter()
        .try_fold(false, |found, expression| {
            Ok::<_, datafusion::common::DataFusionError>(
                found
                    || expression
                        .expr
                        .exists(|expression| Ok(expression.is::<LambdaVariable>()))?,
            )
        })?;
    if !contains_lambda {
        return Ok(Transformed::no(plan));
    }
    let schema = projection.input().schema();
    let (expressions, extracted) = extract_metadata_expressions(projection, &schema)?;
    let identity =
        ProjectionExprs::from_indices(&(0..schema.fields().len()).collect::<Vec<_>>(), &schema);
    let input = Arc::new(ProjectionExec::try_new(
        identity.iter().cloned().chain(extracted),
        Arc::clone(projection.input()),
    )?) as Arc<dyn ExecutionPlan>;
    let rewritten = ProjectionExec::try_new_with_schema_metadata(
        expressions.iter().cloned(),
        input,
        projection.schema().as_ref(),
    )?;
    Ok(Transformed::yes(Arc::new(rewritten)))
}

fn extract_metadata_expressions(
    projection: &ProjectionExec,
    target_schema: &Schema,
) -> Result<(ProjectionExprs, Vec<ProjectionExpr>)> {
    let schema = projection.input().schema();
    let mut extracted: Vec<ProjectionExpr> = Vec::new();
    let expressions = projection
        .projection_expr()
        .clone()
        .try_map_exprs(|expression| {
            expression
                .transform_up(|expression| {
                    if !is_metadata_expression(&expression) {
                        return Ok(Transformed::no(expression));
                    }
                    let index = match extracted
                        .iter()
                        .position(|candidate| candidate.expr.eq(&expression))
                    {
                        Some(index) => index,
                        None => {
                            let mut suffix = extracted.len();
                            let alias = loop {
                                let alias = format!("__sail_input_file_metadata_{suffix}");
                                if schema.field_with_name(&alias).is_err()
                                    && target_schema.field_with_name(&alias).is_err()
                                    && !extracted.iter().any(|expression| expression.alias == alias)
                                {
                                    break alias;
                                }
                                suffix += 1;
                            };
                            extracted.push(ProjectionExpr::new(expression, alias));
                            extracted.len() - 1
                        }
                    };
                    Ok(Transformed::yes(Arc::new(Column::new(
                        &extracted[index].alias,
                        schema.fields().len() + index,
                    ))
                        as Arc<dyn PhysicalExpr>))
                })
                .map(|result| result.data)
        })?;
    // Lambda parameters follow the input columns in the physical evaluation schema.
    let expressions = expressions.try_map_exprs(|expression| {
        expression
            .transform_up(|expression| {
                if let Some(variable) = expression.downcast_ref::<LambdaVariable>() {
                    Ok(Transformed::yes(Arc::new(LambdaVariable::new(
                        variable.index() + extracted.len(),
                        Arc::clone(variable.field()),
                    ))
                        as Arc<dyn PhysicalExpr>))
                } else {
                    Ok(Transformed::no(expression))
                }
            })
            .map(|result| result.data)
    })?;
    Ok((expressions, extracted))
}

fn push_metadata_through_file_context(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let Some(projection) = plan.downcast_ref::<ProjectionExec>() else {
        return Ok(Transformed::no(plan));
    };
    if !projection_references_input_file_metadata(projection.projection_expr()) {
        return Ok(Transformed::no(plan));
    }
    if let Some(filter) = projection.input().downcast_ref::<FilterExec>() {
        let schema = filter.input().schema();
        let (expressions, metadata) = extract_metadata_expressions(projection, &schema)?;
        let column_count = schema.fields().len();
        let identity =
            ProjectionExprs::from_indices(&(0..column_count).collect::<Vec<_>>(), &schema);
        let indices = filter
            .projection()
            .as_ref()
            .map_or_else(
                || (0..column_count).collect::<Vec<_>>(),
                |projection| projection.to_vec(),
            )
            .into_iter()
            .chain(column_count..column_count + metadata.len())
            .collect();
        let input = Arc::new(ProjectionExec::try_new(
            identity.iter().cloned().chain(metadata),
            Arc::clone(filter.input()),
        )?) as Arc<dyn ExecutionPlan>;
        let filter = FilterExecBuilder::from(filter)
            .with_input(input)
            .apply_projection(None)?
            .apply_projection(Some(indices))?
            .build()?;
        let rewritten = ProjectionExec::try_new_with_schema_metadata(
            expressions.iter().cloned(),
            Arc::new(filter),
            projection.schema().as_ref(),
        )?;
        return Ok(Transformed::yes(Arc::new(rewritten)));
    }
    if let Some(unnest) = projection.input().downcast_ref::<UnnestExec>() {
        let schema = unnest.input().schema();
        let (expressions, metadata) = extract_metadata_expressions(projection, &schema)?;
        let identity =
            ProjectionExprs::from_indices(&(0..schema.fields().len()).collect::<Vec<_>>(), &schema);
        let input = Arc::new(ProjectionExec::try_new(
            identity.iter().cloned().chain(metadata),
            Arc::clone(unnest.input()),
        )?) as Arc<dyn ExecutionPlan>;
        // Appended metadata columns are repeated with each generated row.
        let mut output_schema = SchemaBuilder::from(unnest.schema().as_ref());
        output_schema.extend(
            input
                .schema()
                .fields()
                .iter()
                .skip(schema.fields().len())
                .cloned(),
        );
        let unnest = UnnestExec::new(
            input,
            unnest.list_column_indices().to_vec(),
            unnest.struct_column_indices().to_vec(),
            Arc::new(output_schema.finish()),
            unnest.options().clone(),
        )?;
        let rewritten = ProjectionExec::try_new_with_schema_metadata(
            expressions.iter().cloned(),
            Arc::new(unnest),
            projection.schema().as_ref(),
        )?;
        return Ok(Transformed::yes(Arc::new(rewritten)));
    }
    let project = |input: &Arc<dyn ExecutionPlan>| -> Result<Arc<dyn ExecutionPlan>> {
        let schema = input.schema();
        let expressions = projection
            .projection_expr()
            .clone()
            .try_map_exprs(|expression| {
                expression
                    .transform_up(|expression| {
                        if let Some(column) = expression.downcast_ref::<Column>() {
                            Ok(Transformed::yes(Arc::new(Column::new(
                                schema.field(column.index()).name(),
                                column.index(),
                            ))
                                as Arc<dyn PhysicalExpr>))
                        } else {
                            Ok(Transformed::no(expression))
                        }
                    })
                    .map(|result| result.data)
            })?;
        Ok(Arc::new(ProjectionExec::try_new_with_schema_metadata(
            expressions.iter().cloned(),
            Arc::clone(input),
            projection.schema().as_ref(),
        )?))
    };
    if let Some(union) = projection.input().downcast_ref::<UnionExec>() {
        let inputs = union
            .inputs()
            .iter()
            .map(project)
            .collect::<Result<Vec<_>>>()?;
        return Ok(Transformed::yes(UnionExec::try_new(inputs)?));
    }
    if let Some(repartition) = projection.input().downcast_ref::<ExplicitRepartitionExec>()
        && matches!(
            repartition.properties().output_partitioning(),
            Partitioning::UnknownPartitioning(_)
        )
    {
        return Ok(Transformed::yes(Arc::new(ExplicitRepartitionExec::new(
            project(repartition.input())?,
            repartition.properties().output_partitioning().clone(),
        ))));
    }
    Ok(Transformed::no(plan))
}

/// Uses the final join orientation and exchanges to follow only a streamed probe context.
#[derive(Debug, Default)]
pub struct PushDownStreamedJoinInputFileMetadata;

impl PhysicalOptimizerRule for PushDownStreamedJoinInputFileMetadata {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(push_streamed_join_metadata)
            .map(|result| result.data)
    }

    fn name(&self) -> &str {
        "PushDownStreamedJoinInputFileMetadata"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn push_streamed_join_metadata(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let Some(projection) = plan.downcast_ref::<ProjectionExec>() else {
        return Ok(Transformed::no(plan));
    };
    if !projection_references_input_file_metadata(projection.projection_expr()) {
        return Ok(Transformed::no(plan));
    }
    let input = projection.input();
    let hash = input.downcast_ref::<HashJoinExec>().filter(|join| {
        *join.partition_mode() == PartitionMode::CollectLeft
            && matches!(
                join.join_type(),
                JoinType::Inner | JoinType::Right | JoinType::RightSemi | JoinType::RightAnti
            )
    });
    let cross = input.downcast_ref::<CrossJoinExec>();
    let nested = input
        .downcast_ref::<NestedLoopJoinExec>()
        .filter(|join| *join.join_type() == JoinType::Full);
    let (right, original_projection, raw_width) = if let Some(join) = hash {
        (
            join.right(),
            join.projection.as_deref(),
            join.join_schema().fields().len(),
        )
    } else if let Some(join) = cross {
        (join.right(), None, join.schema().fields().len())
    } else if let Some(join) = nested {
        (
            join.right(),
            join.projection().as_deref(),
            join.left().schema().fields().len() + join.right().schema().fields().len(),
        )
    } else {
        return push_late_metadata_through_file_context(plan);
    };

    let right_schema = right.schema();
    let (mut expressions, metadata) = extract_metadata_expressions(projection, &right_schema)?;
    let metadata_count = metadata.len();
    let fallbacks = rewrite_input_file_metadata_projection(
        ProjectionExprs::new(metadata.clone()),
        &InputFileMetadata::fallback(),
    )?;
    let identity = ProjectionExprs::from_indices(
        &(0..right_schema.fields().len()).collect::<Vec<_>>(),
        &right_schema,
    );
    let right = Arc::new(ProjectionExec::try_new(
        identity.iter().cloned().chain(metadata),
        Arc::clone(right),
    )?) as Arc<dyn ExecutionPlan>;
    // These indices address the unprojected join output, not its visible schema.
    let raw_projection = original_projection
        .map_or_else(|| (0..raw_width).collect::<Vec<_>>(), |p| p.to_vec())
        .into_iter()
        .chain(raw_width..raw_width + metadata_count)
        .collect();
    let join: Arc<dyn ExecutionPlan> = if let Some(join) = hash {
        join.builder()
            .with_new_children(vec![Arc::clone(join.left()), right])?
            .with_projection(Some(raw_projection))
            .build_exec()?
    } else if let Some(join) = nested {
        Arc::new(NestedLoopJoinExec::try_new(
            Arc::clone(join.left()),
            right,
            join.filter().cloned(),
            join.join_type(),
            Some(raw_projection),
        )?)
    } else {
        // The cross join has no embedded projection.
        Arc::new(CrossJoinExec::new(Arc::clone(input.children()[0]), right))
    };
    if nested.is_some() {
        let visible_width = input.schema().fields().len();
        expressions = expressions.try_map_exprs(|expression| {
            expression
                .transform_up(|expression| {
                    let Some(column) = expression.downcast_ref::<Column>() else {
                        return Ok(Transformed::no(expression));
                    };
                    if column.index() < visible_width {
                        return Ok(Transformed::no(expression));
                    }
                    // Only fresh metadata uses defaults on unmatched build rows. Ordinary
                    // pre-materialized columns must retain their outer-join null extension.
                    let fallback =
                        Arc::clone(&fallbacks.as_ref()[column.index() - visible_width].expr);
                    // Coalesce's physical UDF is not executable; its simplified CASE
                    // also infers non-nullability from the guarded column and fallback.
                    Ok(Transformed::yes(Arc::new(CaseExpr::try_new(
                        None,
                        vec![(
                            Arc::new(IsNotNullExpr::new(Arc::clone(&expression))),
                            expression,
                        )],
                        Some(fallback),
                    )?)
                        as Arc<dyn PhysicalExpr>))
                })
                .map(|result| result.data)
        })?;
    }
    Ok(Transformed::yes(Arc::new(
        ProjectionExec::try_new_with_schema_metadata(
            expressions.iter().cloned(),
            join,
            projection.schema().as_ref(),
        )?,
    )))
}

fn push_late_metadata_through_file_context(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let Some(projection) = plan.downcast_ref::<ProjectionExec>() else {
        return Ok(Transformed::no(plan));
    };
    if let Some(inner) = projection.input().downcast_ref::<ProjectionExec>() {
        // Append only leaves rather than merging arbitrary expressions or lambda schemas.
        let (expressions, metadata) =
            extract_metadata_expressions(projection, &inner.input().schema())?;
        let mut schema = SchemaBuilder::from(inner.schema().as_ref());
        for expression in &metadata {
            schema.push(
                expression
                    .expr
                    .return_field(&inner.input().schema())?
                    .as_ref()
                    .clone()
                    .with_name(expression.alias.clone()),
            );
        }
        let inner = ProjectionExec::try_new_with_schema_metadata(
            inner.expr().iter().cloned().chain(metadata),
            Arc::clone(inner.input()),
            &schema.finish(),
        )?;
        return Ok(Transformed::yes(Arc::new(
            ProjectionExec::try_new_with_schema_metadata(
                expressions.iter().cloned(),
                Arc::new(inner),
                projection.schema().as_ref(),
            )?,
        )));
    }
    let input = projection.input();
    let file_scan = input
        .downcast_ref::<DataSourceExec>()
        .is_some_and(|scan| scan.data_source().is::<FileScanConfig>());
    // Connector scan nodes absorb metadata through their source-local projection hook.
    // Keep this allowlist source-only: a general projection swap can cross an exchange.
    if (file_scan
        || matches!(
            input.name(),
            "DeltaScanByAddsExec" | "IcebergScanByDataFilesExec"
        ))
        && let Some(source) = input.try_swapping_with_projection(projection)?
    {
        return Ok(Transformed::yes(source));
    }
    // Filters, unnest, union and explicit non-shuffle coalesce preserve the context.
    // Sorts, buffers, exchanges, limits and unknown operators deliberately stop it.
    push_metadata_through_file_context(plan)
}

fn plan_references_input_file_metadata(plan: &Arc<dyn ExecutionPlan>) -> Result<bool> {
    let mut found = false;
    plan.apply(|node| {
        node.apply_expressions(&mut |expression| {
            if expression_references_input_file_metadata(expression) {
                found = true;
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })?;
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    Ok(found)
}

/// Replaces scan-metadata functions that did not reach a file source with Spark defaults.
#[derive(Debug, Default)]
pub struct RewriteInputFileMetadataFallback;

impl RewriteInputFileMetadataFallback {
    pub fn new() -> Self {
        Self
    }
}

impl PhysicalOptimizerRule for RewriteInputFileMetadataFallback {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|plan| {
            let Some(projection) = plan.downcast_ref::<ProjectionExec>() else {
                return Ok(Transformed::no(plan));
            };
            let expressions = rewrite_input_file_metadata_projection(
                projection.projection_expr().clone(),
                &InputFileMetadata::fallback(),
            )?;
            if expressions == *projection.projection_expr() {
                return Ok(Transformed::no(plan));
            }
            let projection = ProjectionExec::try_new_with_schema_metadata(
                expressions.as_ref().iter().cloned(),
                Arc::clone(projection.input()),
                projection.schema().as_ref(),
            )?;
            Ok(Transformed::yes(
                Arc::new(projection) as Arc<dyn ExecutionPlan>
            ))
        })
        .map(|result| result.data)
    }

    fn name(&self) -> &str {
        "RewriteInputFileMetadataFallback"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, deprecated)]
mod tests {
    use datafusion::arrow::array::{Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::{DFSchema, JoinSide, NullEquality, ScalarValue};
    use datafusion::datasource::physical_plan::ParquetSource;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::functions::core::coalesce::CoalesceFunc;
    use datafusion::functions::core::input_file_name::InputFileNameFunc;
    use datafusion::functions::expr_fn;
    use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
    use datafusion::logical_expr::{Operator, ScalarUDF, lit};
    use datafusion::physical_expr::execution_props::ExecutionProps;
    use datafusion::physical_expr::expressions::{BinaryExpr, DynamicFilterPhysicalExpr, Literal};
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr, ScalarFunctionExpr};
    use datafusion::physical_expr_adapter::rewrite::expr_references_scalar_udf;
    use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::joins::SortMergeJoinExec;
    use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
    use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
    use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
    use datafusion::physical_plan::projection::ProjectionExpr;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion_physical_expr::create_physical_exprs;
    use sail_common_datafusion::input_file::{InputFileBlockLengthFunc, InputFileBlockStartFunc};

    use super::*;

    fn columns(prefix: &str) -> Arc<dyn ExecutionPlan> {
        Arc::new(EmptyExec::new(Arc::new(Schema::new(vec![
            Field::new(format!("{prefix}id"), DataType::Int64, false),
            Field::new(format!("{prefix}value"), DataType::Int64, true),
        ]))))
    }

    fn metadata_expressions(schema: &Schema) -> Vec<ProjectionExpr> {
        let expressions = [
            expr_fn::coalesce(vec![expr_fn::input_file_name(), lit("")]),
            ScalarUDF::from(InputFileBlockStartFunc::new()).call(vec![]),
            ScalarUDF::from(InputFileBlockLengthFunc::new()).call(vec![]),
            expr_fn::coalesce(vec![expr_fn::input_file_name(), lit("alias")]),
        ];
        create_physical_exprs(
            &expressions,
            &DFSchema::try_from(schema.clone()).unwrap(),
            &ExecutionProps::default(),
            &PhysicalPlanningContext::default(),
        )
        .unwrap()
        .into_iter()
        .enumerate()
        .map(|(index, expression)| ProjectionExpr::new(expression, format!("metadata{index}")))
        .collect()
    }

    fn metadata_projection(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let schema = input.schema();
        let identity =
            ProjectionExprs::from_indices(&(0..schema.fields().len()).collect::<Vec<_>>(), &schema);
        Arc::new(
            ProjectionExec::try_new(
                identity
                    .iter()
                    .cloned()
                    .chain(metadata_expressions(&schema)),
                input,
            )
            .unwrap(),
        )
    }

    fn join_filter() -> JoinFilter {
        JoinFilter::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 0)),
                Operator::Lt,
                Arc::new(Column::new("b", 1)),
            )),
            vec![
                ColumnIndex {
                    index: 1,
                    side: JoinSide::Left,
                },
                ColumnIndex {
                    index: 1,
                    side: JoinSide::Right,
                },
            ],
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Int64, true),
            ])),
        )
    }

    fn hash_join(
        join_type: JoinType,
        mode: PartitionMode,
        projection: Option<Vec<usize>>,
        right: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            HashJoinExec::try_new(
                columns("l"),
                right,
                vec![(
                    Arc::new(Column::new("lid", 0)),
                    Arc::new(Column::new("rid", 0)),
                )],
                Some(join_filter()),
                &join_type,
                projection,
                mode,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap()
            .builder()
            .with_fetch(Some(7))
            .build()
            .unwrap()
            .with_dynamic_filter_expr(Arc::new(DynamicFilterPhysicalExpr::new(
                vec![Arc::new(Column::new("rid", 0))],
                Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))),
            )))
            .unwrap(),
        )
    }

    fn optimize_streamed(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        PushDownStreamedJoinInputFileMetadata
            .optimize(plan, &ConfigOptions::default())
            .unwrap()
    }

    #[test]
    fn streamed_hash_join_preserves_raw_projection_and_filter_indices() {
        for join_type in [
            JoinType::Inner,
            JoinType::Right,
            JoinType::RightSemi,
            JoinType::RightAnti,
        ] {
            let raw_width = if matches!(join_type, JoinType::RightSemi | JoinType::RightAnti) {
                2
            } else {
                4
            };
            for raw_projection in [
                None,
                Some(vec![raw_width - 1, 0, raw_width - 1]),
                Some(vec![]),
            ] {
                let input = hash_join(
                    join_type,
                    PartitionMode::CollectLeft,
                    raw_projection.clone(),
                    columns("r"),
                );
                let plan = metadata_projection(Arc::clone(&input));
                let schema = plan.schema();
                let visible_width = input.schema().fields().len();
                let optimized = optimize_streamed(plan);
                assert_eq!(optimized.schema(), schema);
                let projection = optimized.downcast_ref::<ProjectionExec>().unwrap();
                let join = projection.input().downcast_ref::<HashJoinExec>().unwrap();
                let expected: Vec<_> = raw_projection
                    .unwrap_or_else(|| (0..raw_width).collect())
                    .into_iter()
                    .chain(raw_width..raw_width + 3)
                    .collect();
                assert_eq!(join.projection.as_deref(), Some(expected.as_slice()));
                assert_eq!(join.schema().fields().len(), visible_width + 3);
                assert_eq!(join.fetch(), Some(7));
                let original = input.downcast_ref::<HashJoinExec>().unwrap();
                assert_eq!(join.on(), original.on());
                assert!(Arc::ptr_eq(
                    &join.dynamic_expressions_produced()[0],
                    &original.dynamic_expressions_produced()[0],
                ));
                let filter = join.filter().unwrap();
                let original_filter = original.filter().unwrap();
                assert_eq!(filter.expression(), original_filter.expression());
                assert_eq!(filter.column_indices(), original_filter.column_indices());
                assert_eq!(filter.schema(), original_filter.schema());
                assert!(Arc::ptr_eq(join.left(), original.left()));
                let right = join.right().downcast_ref::<ProjectionExec>().unwrap();
                assert_eq!(right.expr().len(), 5);
                assert!(
                    right.expr()[2..]
                        .iter()
                        .all(|e| is_metadata_expression(&e.expr))
                );
                let start = projection.expr()[visible_width + 1]
                    .expr
                    .downcast_ref::<Column>()
                    .unwrap();
                assert_eq!(start.index(), visible_width + 1);
                assert!(Arc::ptr_eq(
                    &optimize_streamed(Arc::clone(&optimized)),
                    &optimized
                ));
            }
        }
    }

    #[test]
    fn streamed_join_keeps_outer_expressions_aliases_and_lambda_indices() {
        let join = hash_join(
            JoinType::Inner,
            PartitionMode::CollectLeft,
            Some(vec![2]),
            columns("r"),
        );
        let metadata = metadata_expressions(&join.schema());
        let expression = Arc::new(BinaryExpr::new(
            Arc::new(LambdaVariable::new(
                1,
                Arc::new(Field::new("x", DataType::Int64, true)),
            )),
            Operator::Plus,
            Arc::new(BinaryExpr::new(
                Arc::clone(&metadata[2].expr),
                Operator::Plus,
                Arc::clone(&metadata[2].expr),
            )),
        )) as Arc<dyn PhysicalExpr>;
        let plan = Arc::new(
            ProjectionExec::try_new(vec![ProjectionExpr::new(expression, "nested_alias")], join)
                .unwrap(),
        ) as Arc<dyn ExecutionPlan>;
        let schema = plan.schema();
        let optimized = optimize_streamed(plan);
        assert_eq!(optimized.schema(), schema);
        let projection = optimized.downcast_ref::<ProjectionExec>().unwrap();
        let expression = projection.expr()[0]
            .expr
            .downcast_ref::<BinaryExpr>()
            .unwrap();
        assert_eq!(
            expression
                .left()
                .downcast_ref::<LambdaVariable>()
                .unwrap()
                .index(),
            2
        );
        let nested = expression.right().downcast_ref::<BinaryExpr>().unwrap();
        assert_eq!(nested.left().downcast_ref::<Column>().unwrap().index(), 1);
        assert_eq!(nested.left(), nested.right());
        let join = projection.input().downcast_ref::<HashJoinExec>().unwrap();
        assert_eq!(join.projection.as_deref(), Some([2, 4].as_slice()));
    }

    #[test]
    fn streamed_nested_full_defaults_only_fresh_metadata() {
        for raw_projection in [None, Some(vec![3, 0, 3]), Some(vec![])] {
            let input = Arc::new(
                NestedLoopJoinExec::try_new(
                    columns("l"),
                    columns("r"),
                    Some(join_filter()),
                    &JoinType::Full,
                    raw_projection.clone(),
                )
                .unwrap(),
            ) as Arc<dyn ExecutionPlan>;
            let width = input.schema().fields().len();
            let plan = metadata_projection(input);
            let schema = plan.schema();
            let optimized = optimize_streamed(plan);
            assert_eq!(optimized.schema(), schema);
            let projection = optimized.downcast_ref::<ProjectionExec>().unwrap();
            for expression in &projection.expr()[..width] {
                assert!(expression.expr.is::<Column>());
            }
            for (offset, fallback) in [
                (1, ScalarValue::Int64(Some(-1))),
                (2, ScalarValue::Int64(Some(-1))),
            ] {
                let expression = projection.expr()[width + offset]
                    .expr
                    .downcast_ref::<CaseExpr>()
                    .unwrap();
                let (when, then) = &expression.when_then_expr()[0];
                assert!(when.is::<IsNotNullExpr>());
                assert_eq!(
                    then.downcast_ref::<Column>().unwrap().index(),
                    width + offset
                );
                assert_eq!(
                    expression
                        .else_expr()
                        .unwrap()
                        .downcast_ref::<Literal>()
                        .unwrap()
                        .value(),
                    &fallback
                );
            }
            let name = projection.expr()[width]
                .expr
                .downcast_ref::<ScalarFunctionExpr>()
                .unwrap();
            let fresh_name = name.args()[0].downcast_ref::<CaseExpr>().unwrap();
            assert_eq!(
                fresh_name
                    .else_expr()
                    .unwrap()
                    .downcast_ref::<Literal>()
                    .unwrap()
                    .value(),
                &ScalarValue::Utf8(Some(String::new()))
            );
            let join = projection
                .input()
                .downcast_ref::<NestedLoopJoinExec>()
                .unwrap();
            let filter = join.filter().unwrap();
            let original_filter = join_filter();
            assert_eq!(filter.expression(), original_filter.expression());
            assert_eq!(filter.column_indices(), original_filter.column_indices());
            assert_eq!(filter.schema(), original_filter.schema());
            let expected: Vec<_> = raw_projection
                .unwrap_or_else(|| (0..4).collect())
                .into_iter()
                .chain(4..7)
                .collect();
            assert_eq!(join.projection().as_deref(), Some(expected.as_slice()));
        }
    }

    #[test]
    fn streamed_nested_full_evaluates_fresh_defaults_without_stale_metadata() {
        let join = Arc::new(
            NestedLoopJoinExec::try_new(
                columns("l"),
                columns("r"),
                Some(join_filter()),
                &JoinType::Full,
                None,
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;
        let plan = metadata_projection(join);
        let schema = plan.schema();
        let optimized = optimize_streamed(plan);
        assert_eq!(optimized.schema(), schema);
        let projection = optimized.downcast_ref::<ProjectionExec>().unwrap();
        // Matched probe, unmatched probe from another file, then unmatched build.
        let batch = RecordBatch::try_new(
            projection.input().schema(),
            vec![
                Arc::new(Int64Array::from(vec![Some(101), None, Some(102)])),
                Arc::new(Int64Array::from(vec![Some(1), None, Some(2)])),
                Arc::new(Int64Array::from(vec![Some(101), Some(107), None])),
                Arc::new(Int64Array::from(vec![Some(77), Some(88), None])),
                Arc::new(StringArray::from(vec![
                    Some("file:///a"),
                    Some("file:///b"),
                    None,
                ])),
                Arc::new(Int64Array::from(vec![Some(0), Some(4096), None])),
                Arc::new(Int64Array::from(vec![Some(100), Some(200), None])),
            ],
        )
        .unwrap();
        let name = projection.expr()[4]
            .expr
            .downcast_ref::<ScalarFunctionExpr>()
            .unwrap();
        let fresh = [
            Arc::clone(&name.args()[0]),
            Arc::clone(&projection.expr()[5].expr),
            Arc::clone(&projection.expr()[6].expr),
        ];
        let expected = [
            Arc::new(StringArray::from(vec!["file:///a", "file:///b", ""])) as Arc<dyn Array>,
            Arc::new(Int64Array::from(vec![0, 4096, -1])),
            Arc::new(Int64Array::from(vec![100, 200, -1])),
        ];
        for (expression, expected) in fresh.iter().zip(expected) {
            assert!(expression.is::<CaseExpr>());
            assert!(!expression.nullable(&batch.schema()).unwrap());
            let actual = expression.evaluate(&batch).unwrap().into_array(3).unwrap();
            assert_eq!(actual.as_ref(), expected.as_ref());
        }
        let ordinary = &projection.expr()[3].expr;
        assert!(ordinary.nullable(&batch.schema()).unwrap());
        assert_eq!(
            ordinary
                .evaluate(&batch)
                .unwrap()
                .into_array(3)
                .unwrap()
                .as_ref(),
            batch.column(3).as_ref()
        );
    }

    #[test]
    fn streamed_cross_join_appends_probe_metadata() {
        let input =
            Arc::new(CrossJoinExec::new(columns("l"), columns("r"))) as Arc<dyn ExecutionPlan>;
        let plan = metadata_projection(input);
        let schema = plan.schema();
        let optimized = optimize_streamed(plan);
        assert_eq!(optimized.schema(), schema);
        let projection = optimized.downcast_ref::<ProjectionExec>().unwrap();
        let join = projection.input().downcast_ref::<CrossJoinExec>().unwrap();
        assert_eq!(join.left().schema().fields().len(), 2);
        assert_eq!(join.right().schema().fields().len(), 5);
        assert_eq!(
            projection.expr()[5]
                .expr
                .downcast_ref::<Column>()
                .unwrap()
                .index(),
            5
        );
    }

    #[test]
    fn late_metadata_stops_at_unsupported_joins() {
        let mut joins = Vec::new();
        for mode in [
            PartitionMode::CollectLeft,
            PartitionMode::Partitioned,
            PartitionMode::Auto,
        ] {
            for join_type in [
                JoinType::Inner,
                JoinType::Right,
                JoinType::RightSemi,
                JoinType::RightAnti,
                JoinType::Left,
                JoinType::Full,
                JoinType::LeftSemi,
                JoinType::LeftAnti,
            ] {
                if mode == PartitionMode::CollectLeft
                    && matches!(
                        join_type,
                        JoinType::Inner
                            | JoinType::Right
                            | JoinType::RightSemi
                            | JoinType::RightAnti
                    )
                {
                    continue;
                }
                joins.push(hash_join(join_type, mode, None, columns("r")));
            }
        }
        joins.push(Arc::new(
            NestedLoopJoinExec::try_new(
                columns("l"),
                columns("r"),
                Some(join_filter()),
                &JoinType::Inner,
                None,
            )
            .unwrap(),
        ));
        joins.push(Arc::new(
            SortMergeJoinExec::try_new(
                columns("l"),
                columns("r"),
                vec![(
                    Arc::new(Column::new("lid", 0)),
                    Arc::new(Column::new("rid", 0)),
                )],
                None,
                JoinType::Inner,
                vec![Default::default()],
                NullEquality::NullEqualsNothing,
            )
            .unwrap(),
        ));
        for join in joins {
            let plan = metadata_projection(join);
            assert!(Arc::ptr_eq(&optimize_streamed(Arc::clone(&plan)), &plan));
        }
    }

    #[test]
    fn late_metadata_stops_at_exchange_sort_buffer_limit_and_unknown() {
        let input = columns("r");
        let order = LexOrdering::new(vec![PhysicalSortExpr::new_default(Arc::new(Column::new(
            "rid", 0,
        )))])
        .unwrap();
        let boundaries: Vec<Arc<dyn ExecutionPlan>> = vec![
            Arc::new(
                RepartitionExec::try_new(Arc::clone(&input), Partitioning::RoundRobinBatch(2))
                    .unwrap(),
            ),
            Arc::new(ExplicitRepartitionExec::new(
                Arc::clone(&input),
                Partitioning::RoundRobinBatch(2),
            )),
            Arc::new(SortExec::new(order, Arc::clone(&input))),
            Arc::new(CoalesceBatchesExec::new(Arc::clone(&input), 100)),
            Arc::new(LocalLimitExec::new(Arc::clone(&input), 1)),
            Arc::new(GlobalLimitExec::new(Arc::clone(&input), 0, Some(1))),
            Arc::clone(&input),
        ];
        for boundary in boundaries {
            let plan = metadata_projection(Arc::clone(&boundary));
            assert!(Arc::ptr_eq(&optimize_streamed(Arc::clone(&plan)), &plan));
            let plan = metadata_projection(hash_join(
                JoinType::Inner,
                PartitionMode::CollectLeft,
                None,
                Arc::clone(&boundary),
            ));
            let optimized = optimize_streamed(plan);
            let projection = optimized.downcast_ref::<ProjectionExec>().unwrap();
            let join = projection.input().downcast_ref::<HashJoinExec>().unwrap();
            let probe = join.right().downcast_ref::<ProjectionExec>().unwrap();
            assert!(Arc::ptr_eq(probe.input(), &boundary));
        }
    }

    #[test]
    fn late_metadata_reaches_wrapped_file_through_streaming_operators() {
        let schema = columns("r").schema();
        let scan = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(ParquetSource::new(Arc::clone(&schema))),
        )
        .build();
        let scan = DataSourceExec::from_data_source(scan);
        let inner = Arc::new(
            ProjectionExec::try_new(
                vec![
                    ProjectionExpr::new(Arc::new(Column::new("rvalue", 1)), "renamed"),
                    ProjectionExpr::new(Arc::new(Column::new("rid", 0)), "rid"),
                ],
                scan,
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;
        let filter = Arc::new(
            FilterExec::try_new(
                Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))),
                inner,
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;
        let union = UnionExec::try_new(vec![Arc::clone(&filter), filter]).unwrap();
        let input = Arc::new(ExplicitRepartitionExec::new(
            union,
            Partitioning::UnknownPartitioning(1),
        )) as Arc<dyn ExecutionPlan>;
        let plan = metadata_projection(input);
        let schema = plan.schema();
        let plan = WrapInputFileMetadataSource::new()
            .optimize(plan, &ConfigOptions::default())
            .unwrap();
        let optimized = optimize_streamed(plan);
        assert_eq!(optimized.schema(), schema);
        let mut scans = 0;
        optimized
            .apply(|plan| {
                if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
                    assert!(!projection_references_input_file_metadata(
                        projection.projection_expr()
                    ));
                }
                if let Some(scan) = plan.downcast_ref::<DataSourceExec>() {
                    let config = scan.data_source().downcast_ref::<FileScanConfig>().unwrap();
                    let source = config
                        .file_source
                        .downcast_ref::<InputFileMetadataSource>()
                        .unwrap();
                    assert!(projection_references_input_file_metadata(
                        source.metadata_projection()
                    ));
                    scans += 1;
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .unwrap();
        assert_eq!(scans, 2);
    }

    #[test]
    fn rewrites_unresolved_input_file_metadata_to_spark_defaults() {
        let schema = Arc::new(Schema::empty());
        let df_schema = DFSchema::try_from(Arc::clone(&schema)).unwrap();
        let block_start = datafusion::logical_expr::Expr::ScalarFunction(
            datafusion::logical_expr::expr::ScalarFunction::new_udf(
                Arc::new(ScalarUDF::from(InputFileBlockStartFunc::new())),
                vec![],
            ),
        );
        let block_length = datafusion::logical_expr::Expr::ScalarFunction(
            datafusion::logical_expr::expr::ScalarFunction::new_udf(
                Arc::new(ScalarUDF::from(InputFileBlockLengthFunc::new())),
                vec![],
            ),
        );
        let expressions = [
            expr_fn::coalesce(vec![expr_fn::input_file_name(), lit("")]),
            block_start,
            block_length,
        ];
        let expressions = create_physical_exprs(
            &expressions,
            &df_schema,
            &ExecutionProps::default(),
            &PhysicalPlanningContext::default(),
        )
        .unwrap();
        let input: Arc<dyn ExecutionPlan> = Arc::new(PlaceholderRowExec::new(schema));
        let plan: Arc<dyn ExecutionPlan> = Arc::new(
            ProjectionExec::try_new(
                expressions
                    .into_iter()
                    .enumerate()
                    .map(|(index, expression)| {
                        ProjectionExpr::new(expression, format!("c{index}"))
                    }),
                input,
            )
            .unwrap(),
        );
        let original_schema = plan.schema();

        let optimized = RewriteInputFileMetadataFallback::new()
            .optimize(plan, &ConfigOptions::default())
            .unwrap();

        assert_eq!(optimized.schema(), original_schema);
        let projection = optimized.downcast_ref::<ProjectionExec>().unwrap();
        let file_name = &projection.expr()[0].expr;
        assert!(!expr_references_scalar_udf::<InputFileNameFunc>(file_name));
        let coalesce = file_name.downcast_ref::<ScalarFunctionExpr>().unwrap();
        assert!(coalesce.fun().inner().is::<CoalesceFunc>());
        let fallback = coalesce.args()[0].downcast_ref::<Literal>().unwrap();
        assert_eq!(fallback.value(), &ScalarValue::Utf8(Some(String::new())));
        for expression in [&projection.expr()[1].expr, &projection.expr()[2].expr] {
            let literal = expression.downcast_ref::<Literal>().unwrap();
            assert_eq!(literal.value(), &ScalarValue::Int64(Some(-1)));
        }
    }
}
