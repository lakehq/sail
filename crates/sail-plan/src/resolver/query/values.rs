use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion_common::{DFSchema, DFSchemaRef};
use datafusion_expr::{
    cast, Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Projection, Values,
};
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

/// Returns true if the expression is a literal with a non-null value.
/// This is used to infer non-nullable columns in VALUES clauses, matching
/// Spark's behavior where scalar literal values (e.g. integer `1`, boolean `true`)
/// are non-nullable, while function call expressions (e.g. `TIMESTAMP('...')`)
/// remain nullable.
fn is_non_null_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(sv, _) if !sv.is_null())
}

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
            Ok(results) as PlanResult<_>
        }
        .await?;
        let plan = LogicalPlanBuilder::values(values)?.build()?;

        // Post-process: mark columns as non-nullable when all expressions are non-null
        // literals. This matches Spark's behavior where integer/boolean/string literals
        // in VALUES clauses produce non-nullable columns, while function call expressions
        // (e.g. TIMESTAMP('...')) remain nullable.
        let plan = if let LogicalPlan::Values(values_node) = plan {
            if values_node.values.is_empty() {
                LogicalPlan::Values(values_node)
            } else {
                let num_cols = values_node.values[0].len();
                let qualified_fields = (0..num_cols)
                    .map(|i| {
                        let (qualifier, field) = values_node.schema.qualified_field(i);
                        let all_non_null = values_node
                            .values
                            .iter()
                            .all(|row| row.get(i).is_some_and(is_non_null_literal));
                        let new_field = if all_non_null && field.is_nullable() {
                            Arc::new(Field::new(field.name(), field.data_type().clone(), false))
                        } else {
                            Arc::clone(field)
                        };
                        (qualifier.cloned(), new_field)
                    })
                    .collect::<Vec<_>>();
                let new_schema = Arc::new(DFSchema::new_with_metadata(
                    qualified_fields,
                    values_node.schema.metadata().clone(),
                )?);
                LogicalPlan::Values(Values {
                    schema: new_schema,
                    values: values_node.values,
                })
            }
        } else {
            plan
        };

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
}
