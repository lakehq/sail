use std::sync::Arc;

use datafusion::logical_expr::LogicalPlan;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DFSchema, DataFusionError};
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::utils::merge_schema;
use sail_common::spec;

use crate::error::PlanResult;
use crate::function::conditional_type_view;
use crate::resolver::PlanResolver;
use crate::resolver::state::{ConditionalTypeContext, PlanResolverState};

#[derive(Debug)]
pub struct NamedPlan {
    pub plan: LogicalPlan,
    /// The user-facing fields for query plan,
    /// or `None` for a non-query plan (e.g. a DDL statement).
    pub fields: Option<Vec<String>>,
}

impl PlanResolver<'_> {
    /// Builds a temporary plan for schema publication after value lowering is complete.
    /// The returned plan must not be used for execution or further expression lowering.
    pub fn conditional_schema_view(&self, plan: LogicalPlan) -> LogicalPlan {
        let view = conditional_plan_view(plan.clone(), self.config.ansi_mode, None);
        // TODO: Resolve deferred observation types without invalidating consumers'
        // existing signatures. Preserve the whole baseline schema if the temporary
        // view cannot be inferred; a partial view could expose inconsistent fields.
        view.unwrap_or(plan)
    }

    /// Resolves a plan into a named plan.
    pub async fn resolve_named_plan(&self, plan: spec::Plan) -> PlanResult<NamedPlan> {
        let mut state = PlanResolverState::new();
        match plan {
            spec::Plan::Query(query) => {
                let plan = self.resolve_query_plan(query, &mut state).await?;
                let plan = Self::preserve_order_sensitive_aggregate_sorts(plan)?;
                let fields = Some(Self::get_field_names(plan.schema(), &state)?);
                Ok(NamedPlan { plan, fields })
            }
            spec::Plan::Command(command) => {
                let plan = self.resolve_command_plan(command, &mut state).await?;
                let plan = Self::preserve_order_sensitive_aggregate_sorts(plan)?;
                Ok(NamedPlan { plan, fields: None })
            }
        }
    }
}

/// Keep original producers available while rebuilding the publication schema.
/// A native parent must be able to recover the Arrow view of a signed child even
/// after that child's published schema has acquired its Spark conditional type.
fn conditional_plan_view(
    plan: LogicalPlan,
    ansi: bool,
    outer: Option<Arc<ConditionalTypeContext>>,
) -> datafusion_common::Result<LogicalPlan> {
    let context = ConditionalTypeContext {
        inputs: plan
            .inputs()
            .into_iter()
            .map(|input| Arc::new(input.clone()))
            .collect(),
        outer: outer.clone(),
        ..Default::default()
    };
    let lateral_outer = if let LogicalPlan::Join(join) = &plan
        && let LogicalPlan::Subquery(subquery) = join.right.as_ref()
        && !subquery.outer_ref_columns.is_empty()
        && subquery.outer_ref_columns.iter().all(|expression| {
            matches!(expression, datafusion_expr::Expr::OuterReferenceColumn(_, column)
                if join.left.schema().is_column_from_schema(column))
        }) {
        let mut left_context = context.clone();
        left_context.inputs = vec![Arc::clone(&join.left)];
        Some(Arc::new(left_context))
    } else {
        None
    };
    let mut input_index = 0;
    let plan = plan
        .map_subqueries(|subquery| {
            Ok(Transformed::yes(conditional_plan_view(
                subquery,
                ansi,
                Some(Arc::new(context.clone())),
            )?))
        })?
        .data
        .map_children(|input| {
            // A lateral right subquery resolves against the original left
            // producers, just as it did during expression lowering. Ordinary
            // relational children retain their inherited outer scope.
            let child_outer = if input_index == 1 {
                lateral_outer.clone().or_else(|| outer.clone())
            } else {
                outer.clone()
            };
            input_index += 1;
            Ok(Transformed::yes(conditional_plan_view(
                input,
                ansi,
                child_outer,
            )?))
        })?
        .data;
    let mut schema = merge_schema(&plan.inputs());
    if let LogicalPlan::TableScan(scan) = &plan {
        schema.merge(&DFSchema::try_from_qualified_schema(
            scan.table_name.clone(),
            &scan.source.schema(),
        )?);
    }
    let schema = Arc::new(schema);
    let names = NamePreserver::new(&plan);
    plan.map_expressions(|expression| {
        let name = names.save(&expression);
        let expression = conditional_type_view(expression, &schema, ansi, &context)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        Ok(Transformed::yes(name.restore(expression)))
    })?
    .data
    .recompute_schema()
}
