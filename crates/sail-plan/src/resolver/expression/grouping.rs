use std::collections::HashMap;

use datafusion_common::DFSchemaRef;
use datafusion_expr::expr;
use sail_common::spec;
use sail_common_datafusion::utils::items::ItemTaker;

use crate::error::{PlanError, PlanResult};
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_expression_rollup(
        &self,
        rollup: Vec<spec::Expr>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let (names, expr) = self
            .resolve_expressions_and_names(rollup, schema, state)
            .await?;
        Ok(NamedExpr::new(
            names,
            expr::Expr::GroupingSet(expr::GroupingSet::Rollup(expr)),
        ))
    }

    pub(super) async fn resolve_expression_cube(
        &self,
        cube: Vec<spec::Expr>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let (names, expr) = self
            .resolve_expressions_and_names(cube, schema, state)
            .await?;
        Ok(NamedExpr::new(
            names,
            expr::Expr::GroupingSet(expr::GroupingSet::Cube(expr)),
        ))
    }

    pub(super) async fn resolve_expression_grouping_sets(
        &self,
        grouping_sets: Vec<Vec<spec::Expr>>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let mut name_map: HashMap<expr::Expr, String> = HashMap::new();
        let mut expr_sets: Vec<Vec<expr::Expr>> = Vec::with_capacity(grouping_sets.len());
        for grouping_set in grouping_sets {
            let mut expr_set = vec![];
            let exprs = self
                .resolve_named_expressions(grouping_set, schema, state)
                .await?;
            for NamedExpr { name, expr, .. } in exprs {
                let name = name.one()?;
                expr_set.push(expr.clone());
                name_map.insert(expr, name);
            }
            expr_sets.push(expr_set)
        }
        let grouping_sets = expr::GroupingSet::GroupingSets(expr_sets);
        let names = grouping_sets
            .distinct_expr()
            .into_iter()
            .map(|e| {
                name_map.get(e).cloned().ok_or_else(|| {
                    PlanError::invalid(format!("grouping set expression not found: {e:?}"))
                })
            })
            .collect::<PlanResult<Vec<_>>>()?;
        Ok(NamedExpr::new(
            names,
            expr::Expr::GroupingSet(grouping_sets),
        ))
    }
}
