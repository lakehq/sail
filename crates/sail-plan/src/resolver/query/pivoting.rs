use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion_expr::{col, expr, lit, ExprSchemable, LogicalPlan, Projection, ScalarUDF};
use datafusion_functions_nested::expr_fn as nested_fn;
use sail_common::spec;
use sail_function::scalar::explode;
use sail_function::scalar::struct_function::StructFunction;

use crate::error::{PlanError, PlanResult};
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;
use crate::resolver::tree::explode::ExplodeRewriter;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_pivot(
        &self,
        _pivot: spec::Pivot,
        _state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        Err(PlanError::todo("pivot"))
    }

    pub(super) async fn resolve_query_unpivot(
        &self,
        unpivot: spec::Unpivot,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self
            .resolve_query_plan(unpivot.input.as_ref().clone(), state)
            .await?;

        let input_names = Self::get_field_names(input.schema(), state)?;
        let columns = Self::resolve_columns(self, input.schema(), &input_names, state)?;
        let col_name_map: HashMap<&str, String> = (columns
            .iter()
            .map(|c| c.name())
            .zip(input_names.iter().cloned()))
        .collect();

        let expr_with_real_name = |expr: expr::Expr| {
            let col_name = expr.qualified_name().1;
            let real_name = col_name_map.get(col_name.as_str()).ok_or_else(|| {
                PlanError::AnalysisError(format!("unpivot: cannot find column name for {col_name}"))
            })?;
            Ok((expr, real_name.clone()))
        };

        let remaining_exprs = |input_names: &Vec<String>,
                               chosen_names_set: &HashSet<String>,
                               state: &mut PlanResolverState|
         -> PlanResult<Vec<(expr::Expr, String)>> {
            let remaining_names = input_names
                .iter()
                .filter(|&name| !chosen_names_set.contains(name))
                .collect::<Vec<_>>();
            let remaining_columns =
                Self::resolve_columns(self, input.schema(), &remaining_names, state)?;
            Ok(remaining_columns
                .iter()
                .zip(remaining_names.iter())
                .map(|(c, &name)| (col(c.flat_name()), name.clone()))
                .collect())
        };

        let all_columns_are_ids_err = || {
            Err(PlanError::AnalysisError(
            "[UNPIVOT_REQUIRES_VALUE_COLUMNS] At least one value column needs to be specified for UNPIVOT, all columns specified as ids.".to_string()
        ))
        };

        let ids_opt = match unpivot.ids {
            Some(ids) => {
                let ids = self.resolve_expressions(ids, input.schema(), state).await?;
                let ids = ids
                    .into_iter()
                    .map(expr_with_real_name)
                    .collect::<Result<Vec<_>, PlanError>>()?;
                Some(ids)
            }
            None => None,
        };

        let values_opt = match unpivot.values {
            Some(values) => {
                let values: Vec<spec::Expr> = values
                    .iter()
                    .flat_map(|value| &value.columns)
                    .cloned()
                    .collect();

                let values = self
                    .resolve_expressions(values, input.schema(), state)
                    .await?;

                let values = values
                    .into_iter()
                    .map(expr_with_real_name)
                    .collect::<Result<Vec<_>, PlanError>>()?;
                Some(values)
            }
            None => None,
        };

        let (ids, values) = match (ids_opt, values_opt) {
            (Some(ids), _) if ids.len() == input_names.len() => all_columns_are_ids_err(),
            (_, Some(values)) if values.is_empty() => all_columns_are_ids_err(),
            (None, None) => all_columns_are_ids_err(),
            (Some(ids), Some(values)) => Ok((ids, values)),
            (None, Some(values)) => {
                let values_names: HashSet<String> =
                    values.iter().map(|expr_name| expr_name.1.clone()).collect();
                Ok((remaining_exprs(&input_names, &values_names, state)?, values))
            }
            (Some(ids), None) => {
                let ids_names: HashSet<String> =
                    ids.iter().map(|expr_name| expr_name.1.clone()).collect();
                Ok((ids, remaining_exprs(&input_names, &ids_names, state)?))
            }
        }?;

        let values_types = values
            .iter()
            .map(|(value, _name)| value.get_type(input.schema()))
            .collect::<Result<Vec<_>, _>>()?;

        if !types_are_coercible(&values_types) {
            let type_names: Vec<String> =
                values_types.iter().map(|dt| format!("{:?}", dt)).collect();
            return Err(PlanError::AnalysisError(format!(
                "[UNPIVOT_VALUE_DATA_TYPE_MISMATCH] Unpivot value columns must share a least common type, some types do not: {}",
                type_names.join(", ")
            )));
        }

        let variable_column_name = unpivot.variable_column_name.as_ref();
        let value_column_name = unpivot.value_column_names[0].as_ref();

        let structs = values
            .into_iter()
            .map(|(value, name)| {
                ScalarUDF::from(StructFunction::new(vec![
                    variable_column_name.to_string(),
                    value_column_name.to_string(),
                ]))
                .call(vec![lit(name), value])
            })
            .collect::<Vec<_>>();

        let structs_arr = nested_fn::make_array(structs);
        let inline_expr = ScalarUDF::from(explode::Explode::new(explode::ExplodeKind::Inline))
            .call(vec![structs_arr]);
        let inline_name = state.register_field_name("");

        let projections = ids
            .into_iter()
            .chain(std::iter::once((inline_expr, inline_name)))
            .map(|(expr, name)| NamedExpr {
                name: vec![name.to_string()],
                expr: expr.clone(),
                metadata: vec![],
            })
            .collect::<Vec<_>>();

        let (input, expr) =
            self.rewrite_projection::<ExplodeRewriter>(input.clone(), projections, state)?;

        let expr = self.rewrite_multi_expr(expr)?;
        let expr = self.rewrite_named_expressions(expr, state)?;

        Ok(LogicalPlan::Projection(Projection::try_new(
            expr,
            Arc::new(input),
        )?))
    }
}

fn types_are_coercible(data_types: &[DataType]) -> bool {
    if data_types.is_empty() {
        return true;
    }
    data_types
        .iter()
        .skip(1)
        .try_fold(data_types[0].clone(), |acc, dt| match (&acc, dt) {
            (DataType::Utf8 | DataType::LargeUtf8, DataType::Utf8 | DataType::LargeUtf8)
            | (DataType::Null, _)
            | (_, DataType::Null)
            | (_, _)
                if (&acc == dt) || (acc.is_numeric() && dt.is_numeric()) =>
            {
                Some(acc)
            }
            _ => None,
        })
        .is_some()
}
