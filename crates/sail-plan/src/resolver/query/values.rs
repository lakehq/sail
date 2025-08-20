use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion_common::{DFSchema, DFSchemaRef};
use datafusion_expr::{cast, Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Projection};
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_values(
        &self,
        values: Vec<Vec<spec::Expr>>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let schema = Arc::new(DFSchema::empty());
        let values = async {
            let mut results: Vec<Vec<Expr>> = Vec::with_capacity(values.len());
            for value in values {
                let value = self.resolve_expressions(value, &schema, state).await?;
                results.push(value);
            }
            let _changed_column_indices = Self::resolve_values_nan_types(&mut results, &schema)?;
            Ok(results) as PlanResult<_>
        }
        .await?;
        let plan = LogicalPlanBuilder::values(values)?.build()?;
        let expr = plan
            .schema()
            .columns()
            .into_iter()
            .enumerate()
            .map(|(i, col)| Expr::Column(col).alias(state.register_field_name(format!("col{i}"))))
            .collect::<Vec<_>>();
        Ok(LogicalPlan::Projection(Projection::try_new(
            expr,
            Arc::new(plan),
        )?))
    }

    fn resolve_values_nan_types(
        values: &mut Vec<Vec<Expr>>,
        schema: &DFSchemaRef,
    ) -> PlanResult<HashSet<usize>> {
        let mut nan_positions = HashSet::new();
        for value in values.iter() {
            value.iter().enumerate().for_each(|(idx, expr)| {
                if let Expr::Cast(cast) = expr {
                    if let Expr::Literal(sv, _) = cast.expr.as_ref() {
                        if let Some(true) = sv
                            .try_as_str()
                            .flatten()
                            .map(|s| s.to_uppercase() == "NAN" && cast.data_type.is_numeric())
                        {
                            nan_positions.insert(idx);
                        }
                    }
                }
            });
        }

        for idx in nan_positions.clone() {
            let override_types = values
                .iter()
                .map(|result| {
                    Ok(match result[idx].get_type(&schema)? {
                        DataType::Utf8 | DataType::LargeUtf8 => DataType::Utf8,
                        DataType::Float64 | DataType::Decimal128(..) | DataType::Decimal256(..) => {
                            DataType::Float64
                        }
                        _ => DataType::Float32,
                    })
                })
                .collect::<Result<Vec<_>, PlanError>>()?;

            let target_type = override_types
                .iter()
                .try_fold(false, |has_float64, t| match t {
                    DataType::Utf8 | DataType::LargeUtf8 => Err(PlanError::invalid(format!(
                        "Found incompatible types in column number {idx:?}"
                    ))),
                    DataType::Float64 | DataType::Decimal128(..) | DataType::Decimal256(..) => {
                        Ok(true)
                    }
                    _ => Ok(has_float64),
                })
                .map(|has_float64| {
                    if has_float64 {
                        DataType::Float64
                    } else {
                        DataType::Float32
                    }
                })?;

            for value in &mut *values {
                value[idx] = cast(value[idx].clone(), target_type.clone());
            }
        }

        Ok(nan_positions)
    }
}
