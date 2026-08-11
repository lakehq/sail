use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion_common::{DFSchema, DFSchemaRef};
use datafusion_expr::{Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Projection, cast};
use sail_common::spec;

use super::{align_expr_to_ltz_type, contains_ltz, widen_ltz_types};
use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_values(
        &self,
        values: Vec<Vec<spec::Expr>>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let schema = Arc::new(DFSchema::empty());
        let values: Vec<Vec<Expr>> = async {
            let mut results: Vec<Vec<Expr>> = Vec::with_capacity(values.len());
            for value in values {
                let value = self.resolve_expressions(value, &schema, state).await?;
                results.push(value);
            }
            self.resolve_values_ltz_types(&mut results, &schema)?;
            let _nan_column_indices = Self::resolve_values_nan_types(&mut results, &schema)?;
            let _map_column_indices = Self::resolve_values_map_types(&mut results, &schema)?;
            Ok::<_, PlanError>(results)
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

    fn resolve_values_ltz_types(
        &self,
        values: &mut [Vec<Expr>],
        schema: &DFSchemaRef,
    ) -> PlanResult<()> {
        let Some(width) = values.first().map(Vec::len) else {
            return Ok(());
        };
        for index in 0..width {
            let types = values
                .iter()
                .map(|row| {
                    row.get(index)
                        .ok_or_else(|| PlanError::invalid("VALUES rows have different lengths"))?
                        .get_type(schema)
                        .map_err(PlanError::from)
                })
                .collect::<PlanResult<Vec<_>>>()?;
            let Some(first_type) = types.first() else {
                continue;
            };
            let Some((target_type, has_ltz)) = types.iter().skip(1).try_fold(
                (first_type.clone(), contains_ltz(first_type)),
                |(target_type, has_ltz), source_type| {
                    let (target_type, pair_has_ltz) = widen_ltz_types(
                        &target_type,
                        source_type,
                        self.config.ansi_mode,
                        false,
                        self.config.case_sensitive,
                    )?;
                    Some((target_type, has_ltz || pair_has_ltz))
                },
            ) else {
                continue;
            };
            if has_ltz {
                for (row, source_type) in values.iter_mut().zip(types) {
                    row[index] = align_expr_to_ltz_type(
                        row[index].clone(),
                        &source_type,
                        &target_type,
                        &self.config,
                        !self.config.ansi_mode,
                    )?;
                }
            }
        }
        Ok(())
    }

    fn resolve_values_nan_types(
        values: &mut Vec<Vec<Expr>>,
        schema: &DFSchemaRef,
    ) -> PlanResult<HashSet<usize>> {
        let mut nan_positions = HashSet::new();
        for value in values.iter() {
            value.iter().enumerate().for_each(|(idx, expr)| {
                if let Expr::Cast(cast) = expr
                    && let Expr::Literal(sv, _) = cast.expr.as_ref()
                    && let Some(true) = sv
                        .try_as_str()
                        .flatten()
                        .map(|s| s.to_uppercase() == "NAN" && cast.field.data_type().is_numeric())
                {
                    nan_positions.insert(idx);
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

            let non_null_types = override_types.into_iter().flatten().collect::<Vec<_>>();
            let compatible = non_null_types.first().is_none_or(|first| {
                non_null_types
                    .iter()
                    .skip(1)
                    .all(|data_type| map_key_value_types_match(first, data_type))
            });
            if compatible
                && let Some(target_type) = non_null_types
                    .into_iter()
                    .reduce(merge_map_value_nullability)
            {
                for value in &mut *values {
                    value[idx] = cast(value[idx].clone(), target_type.clone());
                }
            }
        }

        Ok(map_positions)
    }
}

fn map_key_value_types_match(left: &DataType, right: &DataType) -> bool {
    let (DataType::Map(left_entries, _), DataType::Map(right_entries, _)) = (left, right) else {
        return false;
    };
    let (DataType::Struct(left_fields), DataType::Struct(right_fields)) =
        (left_entries.data_type(), right_entries.data_type())
    else {
        return false;
    };
    let ([left_key, left_value], [right_key, right_value]) =
        (left_fields.as_ref(), right_fields.as_ref())
    else {
        return false;
    };
    left_key.data_type() == right_key.data_type()
        && left_value.data_type() == right_value.data_type()
}

fn merge_map_value_nullability(left: DataType, right: DataType) -> DataType {
    let (DataType::Map(left_entries, left_sorted), DataType::Map(right_entries, _)) =
        (&left, &right)
    else {
        return left;
    };
    let (DataType::Struct(left_fields), DataType::Struct(right_fields)) =
        (left_entries.data_type(), right_entries.data_type())
    else {
        return left;
    };
    let (Some(left_value), Some(right_value)) = (left_fields.get(1), right_fields.get(1)) else {
        return left;
    };
    let value_nullable = left_value.is_nullable() || right_value.is_nullable();
    if value_nullable == left_value.is_nullable() {
        return left;
    }
    let Some(left_key) = left_fields.first() else {
        return left;
    };
    let fields = vec![
        left_key.clone(),
        Arc::new(left_value.as_ref().clone().with_nullable(value_nullable)),
    ]
    .into();
    DataType::Map(
        Arc::new(
            left_entries
                .as_ref()
                .clone()
                .with_data_type(DataType::Struct(fields)),
        ),
        *left_sorted,
    )
}
