use std::collections::HashMap;
use std::sync::Arc;

use datafusion_common::arrow::datatypes::DataType;
use datafusion_common::{Column, DFSchema, DFSchemaRef};
use datafusion_expr::utils::merge_schema;
use datafusion_expr::{Distinct, Expr, LogicalPlan};
use sail_logical_plan::monotonic_id::MonotonicIdNode;
use sail_logical_plan::repartition::ExplicitRepartitionNode;
use sail_logical_plan::sort::{RequiredSortNode, SortWithinPartitionsNode};
use sail_logical_plan::spark_partition_id::SparkPartitionIdNode;

use crate::error::PlanResult;
use crate::function::{ConditionalTypeObservation, conditional_type_observation_with_cache};
use crate::resolver::state::ConditionalTypeContext;

/// Memoization lives only for one observation, including its recursive producers.
/// The complete context keeps aliases, correlated inputs and lambda bindings distinct.
pub(crate) type ConditionalTypeCache =
    HashMap<(Column, bool, ConditionalTypeContext), Option<ConditionalTypeObservation>>;

/// Follows an existing column to its producer only for conditional type observations.
/// Unknown operators and ambiguous mappings retain the column's published Arrow type.
pub(crate) fn conditional_column_type(
    column: &Column,
    context: &ConditionalTypeContext,
    ansi: bool,
    cache: &mut ConditionalTypeCache,
) -> PlanResult<Option<ConditionalTypeObservation>> {
    let key = (column.clone(), ansi, context.clone());
    if let Some(observation) = cache.get(&key) {
        return Ok(observation.clone());
    }
    let observation = conditional_column_type_uncached(column, context, ansi, cache)?;
    cache.insert(key, observation.clone());
    Ok(observation)
}

fn conditional_column_type_uncached(
    column: &Column,
    context: &ConditionalTypeContext,
    ansi: bool,
    cache: &mut ConditionalTypeCache,
) -> PlanResult<Option<ConditionalTypeObservation>> {
    let mut matches = context.inputs.iter().flat_map(|plan| {
        plan.schema()
            .iter()
            .enumerate()
            .filter(|(_, (qualifier, field))| {
                field.name() == &column.name
                    && column.relation.as_ref().is_none_or(|relation| {
                        qualifier.is_some_and(|qualifier| relation.resolved_eq(qualifier))
                    })
            })
            .map(move |(index, _)| (plan.as_ref(), index))
    });
    let Some((plan, index)) = matches.next() else {
        return Ok(None);
    };
    if matches.next().is_some() {
        return Ok(None);
    }
    column_type_from_plan(plan, index, context, ansi, cache)
}

fn column_type_from_plan(
    plan: &LogicalPlan,
    index: usize,
    context: &ConditionalTypeContext,
    ansi: bool,
    cache: &mut ConditionalTypeCache,
) -> PlanResult<Option<ConditionalTypeObservation>> {
    let expression = match plan {
        LogicalPlan::Projection(projection) => projection.expr.get(index),
        LogicalPlan::Aggregate(_) => {
            let expressions = plan.columnized_output_exprs()?;
            return match expressions.get(index) {
                Some((expression, _)) => producer_type(expression, plan, context, ansi, cache),
                None => Ok(None),
            };
        }
        LogicalPlan::Window(window) => {
            if index < window.input.schema().fields().len() {
                return input_column_type(plan, index, &window.input, index, context, ansi, cache);
            }
            window
                .window_expr
                .get(index - window.input.schema().fields().len())
        }
        LogicalPlan::Distinct(Distinct::On(distinct)) => distinct.select_expr.get(index),
        LogicalPlan::Union(union) => {
            let mut observations = Vec::with_capacity(union.inputs.len());
            for input in &union.inputs {
                if input.schema().fields().get(index).is_none() {
                    return Ok(None);
                }
                let mut context = context.clone();
                context.inputs = vec![Arc::clone(input)];
                observations.push(conditional_type_observation_with_cache(
                    &Expr::Column(column_at(input.schema(), index)),
                    input.schema(),
                    ansi,
                    &context,
                    cache,
                )?);
            }
            return Ok(combined_observation(
                observations,
                plan.schema().field(index).data_type(),
            ));
        }
        LogicalPlan::Values(values) => {
            let schema = Arc::new(DFSchema::empty());
            let mut observations = Vec::with_capacity(values.values.len());
            for row in &values.values {
                let Some(expression) = row.get(index) else {
                    return Ok(None);
                };
                observations.push(conditional_type_observation_with_cache(
                    expression, &schema, ansi, context, cache,
                )?);
            }
            return Ok(combined_observation(
                observations,
                plan.schema().field(index).data_type(),
            ));
        }
        LogicalPlan::SubqueryAlias(alias) => {
            return input_column_type(plan, index, &alias.input, index, context, ansi, cache);
        }
        LogicalPlan::Subquery(subquery) => {
            return input_column_type(plan, index, &subquery.subquery, index, context, ansi, cache);
        }
        LogicalPlan::Unnest(unnest) => {
            let Some(&input_index) = unnest.dependency_indices.get(index) else {
                return Ok(None);
            };
            // Only unchanged scalar columns pass through an unnest. Its generated
            // list/struct fields do not have the same producing expression.
            if unnest.struct_type_columns.contains(&input_index)
                || unnest
                    .list_type_columns
                    .iter()
                    .any(|(index, _)| *index == input_index)
            {
                return Ok(None);
            }
            return input_column_type(
                plan,
                index,
                &unnest.input,
                input_index,
                context,
                ansi,
                cache,
            );
        }
        LogicalPlan::Join(join) => {
            let column = column_at(plan.schema(), index);
            if let LogicalPlan::Subquery(subquery) = join.right.as_ref()
                && join.right.schema().is_column_from_schema(&column)
                && !join.left.schema().is_column_from_schema(&column)
                && !subquery.outer_ref_columns.is_empty()
            {
                // Sail wraps a correlated lateral right input in Subquery. Its
                // producers refer to the left relation in that scope.
                if !subquery.outer_ref_columns.iter().all(|expression| {
                    matches!(expression, Expr::OuterReferenceColumn(_, column)
                        if join.left.schema().is_column_from_schema(column))
                }) {
                    return Ok(None);
                }
                let mut outer = context.clone();
                outer.inputs = vec![Arc::clone(&join.left)];
                let mut context = context.clone();
                context.inputs = vec![Arc::clone(&join.right)];
                context.outer = Some(Arc::new(outer));
                return conditional_column_type(&column, &context, ansi, cache);
            }
            return unchanged_column_type(plan, index, context, ansi, cache);
        }
        LogicalPlan::Filter(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::Repartition(_)
        | LogicalPlan::Limit(_)
        | LogicalPlan::Distinct(Distinct::All(_)) => {
            return unchanged_column_type(plan, index, context, ansi, cache);
        }
        LogicalPlan::Extension(extension)
            if extension.node.as_any().is::<ExplicitRepartitionNode>()
                || extension.node.as_any().is::<RequiredSortNode>()
                || extension.node.as_any().is::<SortWithinPartitionsNode>()
                || extension.node.as_any().is::<MonotonicIdNode>()
                || extension.node.as_any().is::<SparkPartitionIdNode>() =>
        {
            return unchanged_column_type(plan, index, context, ansi, cache);
        }
        _ => return Ok(None),
    };
    match expression {
        Some(expression) => producer_type(expression, plan, context, ansi, cache),
        None => Ok(None),
    }
}

fn combined_observation(
    observations: Vec<ConditionalTypeObservation>,
    output_type: &DataType,
) -> Option<ConditionalTypeObservation> {
    if observations.is_empty() {
        return None;
    }
    // Preserve a known observation through rows/branches that agree. Mixed types
    // retain this operator's established output type; this does not add set-operation
    // coercion or assume that conditional coercion rules apply to VALUES/UNION.
    let common = |types: Vec<&DataType>| {
        let mut types = types.into_iter().filter(|data_type| !data_type.is_null());
        let first = types.next().unwrap_or(&DataType::Null);
        if types.all(|data_type| data_type == first) {
            first.clone()
        } else {
            output_type.clone()
        }
    };
    Some(ConditionalTypeObservation {
        arrow: common(
            observations
                .iter()
                .map(|observation| &observation.arrow)
                .collect(),
        ),
        spark: common(
            observations
                .iter()
                .map(|observation| &observation.spark)
                .collect(),
        ),
        spark_without_opaque_casts: common(
            observations
                .iter()
                .map(|observation| &observation.spark_without_opaque_casts)
                .collect(),
        ),
        contains_native: observations
            .iter()
            .any(|observation| observation.contains_native),
    })
}

fn producer_type(
    expression: &Expr,
    plan: &LogicalPlan,
    context: &ConditionalTypeContext,
    ansi: bool,
    cache: &mut ConditionalTypeCache,
) -> PlanResult<Option<ConditionalTypeObservation>> {
    let inputs = plan.inputs();
    let schema = Arc::new(merge_schema(&inputs));
    // Grouping sets contain a synthetic grouping-ID output with no input producer.
    if let Expr::Column(column) = expression
        && schema.maybe_index_of_column(column).is_none()
    {
        return Ok(None);
    }
    let mut context = context.clone();
    context.inputs = inputs
        .into_iter()
        .map(|input| Arc::new(input.clone()))
        .collect();
    conditional_type_observation_with_cache(expression, &schema, ansi, &context, cache).map(Some)
}

fn input_column_type(
    plan: &LogicalPlan,
    index: usize,
    input: &Arc<LogicalPlan>,
    input_index: usize,
    context: &ConditionalTypeContext,
    ansi: bool,
    cache: &mut ConditionalTypeCache,
) -> PlanResult<Option<ConditionalTypeObservation>> {
    let Some(field) = input.schema().fields().get(input_index) else {
        return Ok(None);
    };
    if field.data_type() != plan.schema().field(index).data_type() {
        return Ok(None);
    }
    let column = column_at(input.schema(), input_index);
    let mut context = context.clone();
    context.inputs = vec![Arc::clone(input)];
    conditional_column_type(&column, &context, ansi, cache)
}

fn unchanged_column_type(
    plan: &LogicalPlan,
    index: usize,
    context: &ConditionalTypeContext,
    ansi: bool,
    cache: &mut ConditionalTypeCache,
) -> PlanResult<Option<ConditionalTypeObservation>> {
    let column = column_at(plan.schema(), index);
    let mut context = context.clone();
    context.inputs = plan
        .inputs()
        .into_iter()
        .map(|input| Arc::new(input.clone()))
        .collect();
    conditional_column_type(&column, &context, ansi, cache)
}

fn column_at(schema: &DFSchemaRef, index: usize) -> Column {
    let (qualifier, field) = schema.qualified_field(index);
    Column::new(qualifier.cloned(), field.name())
}
