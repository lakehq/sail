use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion_common::metadata::FieldMetadata;
use datafusion_common::{DFSchema, DFSchemaRef, ExprSchema};
use datafusion_expr::{
    cast, Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Projection, Values,
};
use sail_common::spec;
use sail_common_datafusion::logical_expr::alias_preserving_metadata;

use crate::error::{PlanError, PlanResult};
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_values(
        &self,
        values: Vec<Vec<spec::Expr>>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let schema = Arc::new(DFSchema::empty());
        // Preserve per-value extension metadata (e.g. Spark interval qualifier
        // on `INTERVAL '10' YEAR`) alongside the raw exprs. DataFusion's
        // `Cast` carries no metadata field, so without this the qualifier
        // would only live on `NamedExpr` and get dropped before the schema
        // is built — yielding a `Interval(YearMonth)` column whose value
        // formatter falls back to the default `YEAR TO MONTH` rendering.
        let (mut values, value_metadata): (Vec<Vec<Expr>>, Vec<Vec<Option<FieldMetadata>>>) =
            async {
                let mut rows = Vec::with_capacity(values.len());
                let mut metas = Vec::with_capacity(values.len());
                for value in values {
                    let named = self
                        .resolve_named_expressions(value, &schema, state)
                        .await?;
                    let mut row = Vec::with_capacity(named.len());
                    let mut meta_row = Vec::with_capacity(named.len());
                    for NamedExpr { expr, metadata, .. } in named {
                        row.push(expr);
                        meta_row.push(if metadata.is_empty() {
                            None
                        } else {
                            let map: HashMap<String, String> = metadata.into_iter().collect();
                            Some(FieldMetadata::from(map))
                        });
                    }
                    rows.push(row);
                    metas.push(meta_row);
                }
                Ok::<_, PlanError>((rows, metas))
            }
            .await?;
        // Run before the alias-wrap below: these match on raw `Expr::Cast` /
        // `DataType::Map` shapes that an outer alias would hide.
        let _nan_column_indices = Self::resolve_values_nan_types(&mut values, &schema)?;
        let _map_column_indices = Self::resolve_values_map_types(&mut values, &schema)?;
        for (row, meta_row) in values.iter_mut().zip(value_metadata) {
            let mut new_row = Vec::with_capacity(row.len());
            for (i, (expr, meta)) in std::mem::take(row).into_iter().zip(meta_row).enumerate() {
                new_row.push(match meta {
                    // Use a per-column alias so we don't introduce duplicate
                    // field names if a row has multiple metadata-bearing
                    // exprs. `LogicalPlanBuilder::values` renames positionally
                    // anyway, but downstream tools (explain, debug output)
                    // benefit from unique names here.
                    Some(m) => expr.alias_with_metadata(format!("col{}", i + 1), Some(m)),
                    None => expr,
                });
            }
            *row = new_row;
        }
        // DataFusion's `LogicalPlanBuilder::values` hard-codes every inferred
        // field as `nullable=true` (see `infer_data` in
        // `datafusion-expr/src/logical_plan/builder.rs`), regardless of
        // whether the row exprs are actually nullable. Spark reports `c:
        // interval day (nullable = false)` for `VALUES (INTERVAL '1' DAY),
        // (INTERVAL '2' DAY) AS t(c)` because both literals are non-null.
        // Build the plan via the helper to reuse its type-union + cast
        // inference, then rebuild the `Values` node with a schema whose
        // per-column nullability reflects the actual row expressions while
        // preserving the inferred types and extension metadata.
        // (`values_with_schema` drops field metadata in its own
        // `infer_values_from_schema`, so we can't use it directly.)
        let initial_plan = LogicalPlanBuilder::values(values)?.build()?;
        let LogicalPlan::Values(Values {
            schema: initial_schema,
            values: cast_values,
        }) = initial_plan
        else {
            return Err(PlanError::internal(
                "LogicalPlanBuilder::values produced a non-Values plan",
            ));
        };
        let mut corrected_fields: Vec<Field> = Vec::with_capacity(initial_schema.fields().len());
        for (j, field) in initial_schema.fields().iter().enumerate() {
            let mut nullable = false;
            for row in &cast_values {
                if row[j].nullable(initial_schema.as_ref())? {
                    nullable = true;
                    break;
                }
            }
            corrected_fields.push(
                Field::new(field.name(), field.data_type().clone(), nullable)
                    .with_metadata(field.metadata().clone()),
            );
        }
        let corrected_schema = Arc::new(DFSchema::from_unqualified_fields(
            Fields::from(corrected_fields),
            DFSchema::metadata(&initial_schema).clone(),
        )?);
        let plan = LogicalPlan::Values(Values {
            schema: corrected_schema,
            values: cast_values,
        });
        let plan_schema = plan.schema();
        let expr = plan_schema
            .columns()
            .into_iter()
            .enumerate()
            .map(|(i, col)| {
                let metadata = plan_schema
                    .field_from_column(&col)
                    .ok()
                    .map(|f| f.metadata().clone())
                    .unwrap_or_default();
                alias_preserving_metadata(
                    Expr::Column(col),
                    state.register_field_name(format!("col{}", i + 1)),
                    metadata,
                )
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
