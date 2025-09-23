use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion_expr::utils::conjunction;
use datafusion_expr::{
    cast, col, is_false, lit, when, Expr, ExprSchemable, Filter, LogicalPlan, Projection, TryCast,
};
use datafusion_functions::expr_fn::isnan;
use sail_common::spec;
use sail_common_datafusion::utils::items::ItemTaker;

use crate::error::{PlanError, PlanResult};
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_fill_na(
        &self,
        input: spec::QueryPlan,
        columns: Vec<spec::Identifier>,
        values: Vec<spec::Expr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        enum Strategy {
            All { value: Expr },
            Columns { columns: Vec<String>, value: Expr },
            EachColumn { columns: Vec<(String, Expr)> },
        }

        let input = self.resolve_query_plan(input, state).await?;
        let schema = input.schema();
        let values = self.resolve_expressions(values, schema, state).await?;
        let columns: Vec<String> = columns.into_iter().map(|x| x.into()).collect();

        if values.is_empty() {
            return Err(PlanError::invalid("missing fill na values"));
        }
        let strategy = if columns.is_empty() {
            let Ok(value) = values.one() else {
                return Err(PlanError::invalid(
                    "expected one value to fill na for all columns",
                ));
            };
            Strategy::All { value }
        } else if values.len() == 1 {
            let value = values.one()?;
            Strategy::Columns { columns, value }
        } else {
            if values.len() != columns.len() {
                return Err(PlanError::invalid(
                    "fill na number of values does not match number of columns",
                ));
            }
            let columns: Vec<(String, Expr)> =
                columns.into_iter().zip(values.into_iter()).collect();
            Strategy::EachColumn { columns }
        };

        let fill_na_exprs = schema
            .iter()
            .map(|(qualifier, field)| {
                let info = state.get_field_info(field.name())?;
                let value = match &strategy {
                    Strategy::All { value } => Some(value.clone()),
                    Strategy::Columns { columns, value } => columns
                        .iter()
                        .any(|col| info.matches(col, None))
                        .then(|| value.clone()),
                    Strategy::EachColumn { columns } => columns
                        .iter()
                        .find_map(|(col, val)| info.matches(col, None).then(|| val.clone())),
                };
                let column_expr = col((qualifier, field));
                let expr = if let Some(value) = value {
                    let value_type = value.get_type(schema)?;
                    if self.can_cast_fill_na_types(&value_type, field.data_type()) {
                        let value = Expr::TryCast(TryCast {
                            expr: Box::new(value),
                            data_type: field.data_type().clone(),
                        });
                        when(column_expr.clone().is_null(), value).otherwise(column_expr)?
                    } else {
                        column_expr
                    }
                } else {
                    column_expr
                };
                Ok(NamedExpr::new(vec![info.name().to_string()], expr))
            })
            .collect::<PlanResult<Vec<_>>>()?;

        Ok(LogicalPlan::Projection(Projection::try_new(
            self.rewrite_named_expressions(fill_na_exprs, state)?,
            Arc::new(input),
        )?))
    }

    fn can_cast_fill_na_types(&self, from_type: &DataType, to_type: &DataType) -> bool {
        // Spark only supports 4 data types for fill na: bool, long, double, string
        if from_type == to_type {
            return true;
        }
        match (from_type, to_type) {
            (DataType::Utf8 | DataType::LargeUtf8, DataType::Utf8 | DataType::LargeUtf8) => true,
            (DataType::Null, _) => true,
            (_, DataType::Null) => true,
            // Only care about checking numeric types because we do TryCast.
            (_, _) => from_type.is_numeric() && to_type.is_numeric(),
        }
    }

    pub(super) async fn resolve_query_drop_na(
        &self,
        input: spec::QueryPlan,
        columns: Vec<spec::Identifier>,
        min_non_nulls: Option<usize>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self.resolve_query_plan(input, state).await?;
        let schema = input.schema();
        let not_null_exprs = schema
            .columns()
            .into_iter()
            .filter_map(|column| {
                (columns.is_empty() || {
                    let columns: Vec<String> = columns.iter().map(|x| x.as_ref().into()).collect();
                    state
                        .get_field_info(column.name())
                        .is_ok_and(|info| columns.iter().any(|c| info.matches(c, None)))
                })
                .then(|| {
                    col(column.clone()).get_type(schema).ok().map(|col_type| {
                        let is_nan = match col_type {
                            DataType::Float16 => {
                                isnan(cast(col(column.clone()), DataType::Float32))
                            }
                            DataType::Float32 | DataType::Float64 => isnan(col(column.clone())),
                            _ => lit(false),
                        };
                        col(column).is_not_null().and(is_false(is_nan))
                    })
                })
                .flatten()
            })
            .collect::<Vec<Expr>>();

        let filter_expr = match min_non_nulls {
            Some(min_non_nulls) if min_non_nulls > 0 => {
                let non_null_count = not_null_exprs
                    .into_iter()
                    .map(|expr| Ok(when(expr, lit(1)).otherwise(lit(0))?))
                    .try_fold(lit(0), |acc: Expr, predicate: PlanResult<Expr>| {
                        Ok(acc + predicate?) as PlanResult<Expr>
                    })?;
                non_null_count.gt_eq(lit(min_non_nulls as u32))
            }
            _ => conjunction(not_null_exprs)
                .ok_or_else(|| PlanError::invalid("No columns specified for drop na."))?,
        };

        Ok(LogicalPlan::Filter(Filter::try_new(
            filter_expr,
            Arc::new(input),
        )?))
    }
}
