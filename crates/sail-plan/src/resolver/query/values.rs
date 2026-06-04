use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Fields};
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
        let values: Vec<Vec<Expr>> = async {
            let mut results: Vec<Vec<Expr>> = Vec::with_capacity(values.len());
            for value in values {
                let value = self.resolve_expressions(value, &schema, state).await?;
                results.push(value);
            }
            let _nan_column_indices = Self::resolve_values_nan_types(&mut results, &schema)?;
            let _map_column_indices = Self::resolve_values_map_types(&mut results, &schema)?;
            Ok::<_, PlanError>(results)
        }
        .await?;
        let column_nullability = Self::resolve_values_nullability(&values, &schema)?;
        let plan = LogicalPlanBuilder::values(values)?.build()?;
        let columns = plan.schema().columns();
        let names = columns
            .iter()
            .enumerate()
            .map(|(i, _)| state.register_field_name(format!("col{}", i + 1)))
            .collect::<Vec<_>>();
        let expr = columns
            .into_iter()
            .zip(names.iter())
            .map(|(col, name)| Expr::Column(col).alias(name.clone()))
            .collect::<Vec<_>>();
        let fields = plan
            .schema()
            .fields()
            .iter()
            .zip(names)
            .zip(column_nullability)
            .map(|((field, name), nullable)| {
                Arc::new(
                    field
                        .as_ref()
                        .clone()
                        .with_name(name)
                        .with_nullable(nullable),
                )
            })
            .collect::<Vec<_>>();
        let projection_schema = Arc::new(DFSchema::from_unqualified_fields(
            Fields::from(fields),
            plan.schema().metadata().clone(),
        )?);
        Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
            expr,
            Arc::new(plan),
            projection_schema,
        )?))
    }

    fn resolve_values_nullability(
        values: &[Vec<Expr>],
        schema: &DFSchemaRef,
    ) -> PlanResult<Vec<bool>> {
        let Some(first) = values.first() else {
            return Ok(vec![]);
        };
        let mut nullability = vec![false; first.len()];
        for value in values {
            for (idx, expr) in value.iter().enumerate() {
                nullability[idx] |= expr.nullable(schema)?;
            }
        }
        Ok(nullability)
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

            if let Some(target_type) = override_types
                .into_iter()
                .flatten()
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
