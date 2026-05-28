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
            let _nan_column_indices = Self::resolve_values_nan_types(&mut results, &schema)?;
            let _map_column_indices = Self::resolve_values_map_types(&mut results, &schema)?;
            Self::resolve_values_null_metadata(&mut results, &schema)?;
            Ok(results) as PlanResult<_>
        }
        .await?;
        let plan = LogicalPlanBuilder::values(values)?.build()?;
        let expr = plan
            .schema()
            .columns()
            .into_iter()
            .enumerate()
            .map(|(i, col)| {
                Expr::Column(col).alias(state.register_field_name(format!("col{}", i + 1)))
            })
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

    fn resolve_values_map_types(
        values: &mut Vec<Vec<Expr>>,
        schema: &DFSchemaRef,
    ) -> PlanResult<HashSet<usize>> {
        let mut map_positions = HashSet::new();
        for value in values.iter() {
            value.iter().enumerate().for_each(|(idx, expr)| {
                if matches!(expr.get_type(schema), Ok(DataType::Map(..))) {
                    map_positions.insert(idx);
                }
            });
        }

        for idx in map_positions.clone() {
            let override_types = values
                .iter()
                .map(|result| {
                    let cur_map_type = result[idx].get_type(&schema)?;
                    Ok(
                        if matches!(cur_map_type.clone(), DataType::Map(inner_type, _)
                        if matches!(inner_type.data_type(), DataType::Struct(fields)
                            if matches!(fields.first().map(|f| f.data_type()), Some(DataType::Null))
                        )) {
                            None
                        } else {
                            Some(cur_map_type)
                        },
                    )
                })
                .collect::<Result<Vec<_>, PlanError>>()?;

            if let Some(target_type) = override_types.into_iter().find_map(|data_type| data_type) {
                for value in &mut *values {
                    value[idx] = cast(value[idx].clone(), target_type.clone());
                }
            }
        }

        Ok(map_positions)
    }

    fn resolve_values_null_metadata(
        values: &mut [Vec<Expr>],
        schema: &DFSchemaRef,
    ) -> PlanResult<()> {
        let Some(first_row) = values.first() else {
            return Ok(());
        };
        for idx in 0..first_row.len() {
            let metadata = values
                .iter()
                .find_map(|row| match row[idx].metadata(schema) {
                    Ok(metadata) if !metadata.is_empty() => Some(Ok(metadata)),
                    Ok(_) => None,
                    Err(e) => Some(Err(e)),
                });
            let Some(metadata) = metadata.transpose()? else {
                continue;
            };
            let mut has_empty_metadata = false;
            for row in values.iter_mut() {
                has_empty_metadata |= row[idx].metadata(schema)?.is_empty();
                if let Expr::Literal(scalar, literal_metadata) = &mut row[idx] {
                    if scalar.is_null() && literal_metadata.as_ref().is_none_or(|m| m.is_empty()) {
                        *literal_metadata = Some(metadata.clone());
                    }
                }
            }
            if has_empty_metadata
                && values
                    .iter()
                    .any(|row| row[idx].metadata(schema).is_ok_and(|m| m.is_empty()))
            {
                for row in values.iter_mut() {
                    Self::clear_values_expr_metadata(&mut row[idx]);
                }
            }
        }
        Ok(())
    }

    fn clear_values_expr_metadata(expr: &mut Expr) {
        match expr {
            Expr::Literal(_, metadata) => *metadata = None,
            Expr::Alias(alias) => {
                alias.metadata = None;
                Self::clear_values_expr_metadata(alias.expr.as_mut());
            }
            _ => {}
        }
    }
}
