use std::sync::Arc;
use std::collections::HashSet;

use datafusion_common::{Column, DFSchema};
use datafusion_expr::{Distinct, DistinctOn, Expr, Filter, LogicalPlan, Projection};
use sail_common::spec;

use crate::error::PlanResult;
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_filter(
        &self,
        input: spec::QueryPlan,
        condition: spec::Expr,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self
            .resolve_query_plan_with_hidden_fields(input, state)
            .await?;
        // A `Filter` is a `UnaryNode`, so a condition of Spark's can name an attribute that the
        // operators under it do not output: the attribute is carried up to the filter and
        // projected away again (`ResolveMissingReferences`). The condition is resolved against the
        // output first, so this only ever accepts what used to fail, and the failure to report is
        // the one from the output, whose schema is the one the user sees.
        // The condition is kept for the second attempt only where there is something under the
        // plan to reach, so a filter that sits on a join, an aggregate or a scan does not pay for
        // a copy of its condition that could never be used.
        let retry = Self::carries_columns_up(&input).then(|| condition.clone());
        let predicate = match self.resolve_expression(condition, input.schema(), state).await {
            Ok(predicate) => predicate,
            Err(error) => {
                // The condition is resolved against the output AND what lies under it together,
                // never against the latter alone: a name the output also carries has to keep
                // reading the output, the way Spark sends down only the attributes that stayed
                // unresolved. Reading it below would bind the name to a column that was replaced
                // and answer the wrong rows.
                let Some(condition) = retry else {
                    return Err(error);
                };
                let schema = Arc::new(Self::schema_with_reachable_fields(&input)?);
                let Ok(predicate) = self.resolve_expression(condition, &schema, state).await else {
                    return Err(error);
                };
                let missing = predicate
                    .column_refs()
                    .into_iter()
                    .filter(|x| !input.schema().has_column(x))
                    .cloned()
                    .collect::<Vec<_>>();
                let output = input
                    .schema()
                    .columns()
                    .into_iter()
                    .map(Expr::Column)
                    .collect::<Vec<_>>();
                let Some(carried) = Self::carry_columns_up(input, &missing)? else {
                    return Err(error);
                };
                let filter = LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(carried))?);
                return Ok(LogicalPlan::Projection(Projection::try_new(
                    output,
                    Arc::new(filter),
                )?));
            }
        };
        let filter = Filter::try_new(predicate, Arc::new(input))?;
        Ok(LogicalPlan::Filter(filter))
    }

    /// The output of the plan followed by the fields that the operators under it read and do not
    /// output. Spark stops at the operators whose own rows would change
    /// (`resolveExprsAndAddMissingAttrs`), so the walk stops where [`Self::carry_columns_up`]
    /// stops and a name below such an operator stays out of reach.
    fn schema_with_reachable_fields(plan: &LogicalPlan) -> PlanResult<DFSchema> {
        let output = plan.schema();
        let mut fields = output
            .iter()
            .map(|(qualifier, field)| (qualifier.cloned(), Arc::clone(field)))
            .collect::<Vec<_>>();
        // A field is named by the identifier the resolver generated for it, which is unique, so
        // the names seen so far are kept in a set: scanning the fields for each of them would be
        // quadratic in the width of the schema and in the depth of the walk. A field an operator
        // only passes on keeps its identifier, so this also keeps it from being offered twice.
        let mut seen = fields
            .iter()
            .map(|(_, x)| x.name().clone())
            .collect::<HashSet<_>>();
        let mut current = Some(plan);
        while let Some(plan) = current.filter(|x| Self::carries_columns_up(x)) {
            let inputs = plan.inputs();
            let [input] = inputs.as_slice() else { break };
            for (qualifier, field) in input.schema().iter() {
                if seen.insert(field.name().clone()) {
                    fields.push((qualifier.cloned(), Arc::clone(field)));
                }
            }
            current = Some(*input);
        }
        Ok(DFSchema::new_with_metadata(
            fields,
            output.metadata().clone(),
        )?)
    }

    /// Whether a column of the input may be carried through the operator. Spark names the ones it
    /// refuses: a `SubqueryAlias`, whose qualifier the column would escape, and a `Distinct`,
    /// which reads every column it outputs, so one more column changes the rows that survive.
    /// `Distinct::On` states the columns it reads, so it is the one Sail can carry through.
    fn carries_columns_up(plan: &LogicalPlan) -> bool {
        matches!(
            plan,
            LogicalPlan::Projection(_)
                | LogicalPlan::Filter(_)
                | LogicalPlan::Limit(_)
                | LogicalPlan::Sort(_)
                | LogicalPlan::Repartition(_)
                | LogicalPlan::Distinct(Distinct::On(_))
        )
    }

    /// Rebuilds the plan so that its output carries `missing` as well, which the caller reads and
    /// then projects away. `None` where an operator the columns would have to cross refuses them.
    fn carry_columns_up(plan: LogicalPlan, missing: &[Column]) -> PlanResult<Option<LogicalPlan>> {
        if missing.iter().all(|x| plan.schema().has_column(x)) {
            return Ok(Some(plan));
        }
        if !Self::carries_columns_up(&plan) {
            return Ok(None);
        }
        let inputs = plan.inputs();
        let [input] = inputs.as_slice() else {
            return Ok(None);
        };
        let input = (*input).clone();
        let Some(input) = Self::carry_columns_up(input, missing)? else {
            return Ok(None);
        };
        match plan {
            // A projection and a `DISTINCT ON` choose what they output, so they are the ones that
            // have to name the columns; every other operator here outputs what it reads.
            LogicalPlan::Projection(projection) => {
                let mut expr = projection.expr;
                for column in missing {
                    if !projection.schema.has_column(column) {
                        expr.push(Expr::Column(column.clone()));
                    }
                }
                Ok(Some(LogicalPlan::Projection(Projection::try_new(
                    expr,
                    Arc::new(input),
                )?)))
            }
            LogicalPlan::Distinct(Distinct::On(distinct)) => {
                let mut select_expr = distinct.select_expr;
                for column in missing {
                    if !distinct.schema.has_column(column) {
                        select_expr.push(Expr::Column(column.clone()));
                    }
                }
                Ok(Some(LogicalPlan::Distinct(Distinct::On(
                    DistinctOn::try_new(
                        distinct.on_expr,
                        select_expr,
                        distinct.sort_expr,
                        Arc::new(input),
                    )?,
                ))))
            }
            plan => Ok(Some(plan.with_new_exprs(plan.expressions(), vec![input])?)),
        }
    }
}
