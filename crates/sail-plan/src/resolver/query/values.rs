use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion_common::{DFSchema, DFSchemaRef};
use datafusion_expr::expr::FieldMetadata;
use datafusion_expr::{Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Projection, cast};
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::expression::{NamedExpr, spark_interval_metadata_for_expression};
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_values(
        &self,
        values: Vec<Vec<spec::Expr>>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let schema = Arc::new(DFSchema::empty());
        let values: Vec<Vec<Expr>> = async {
            let mut results: Vec<Vec<NamedExpr>> = Vec::with_capacity(values.len());
            for value in values {
                let value = self
                    .resolve_named_expressions(value, &schema, state)
                    .await?;
                results.push(value);
            }
            Self::resolve_values_interval_metadata(&mut results, &schema)?;
            let mut results = results
                .into_iter()
                .map(|row| row.into_iter().map(|value| value.expr).collect())
                .collect::<Vec<Vec<Expr>>>();
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

    fn resolve_values_interval_metadata(
        values: &mut [Vec<NamedExpr>],
        schema: &DFSchemaRef,
    ) -> PlanResult<()> {
        let Some(column_count) = values.first().map(Vec::len) else {
            return Ok(());
        };
        if values.iter().any(|row| row.len() != column_count) {
            return Ok(());
        }

        for column_index in 0..column_count {
            let mut common_interval = None::<spec::SparkIntervalMetadata>;
            for row in values.iter() {
                let value = &row[column_index];
                let metadata = value
                    .metadata
                    .iter()
                    .find(|(key, _)| key == spec::SAIL_SPARK_INTERVAL_METADATA_KEY)
                    .map(|(_, value)| {
                        serde_json::from_str::<spec::SparkIntervalMetadata>(value).map_err(
                            |error| {
                                PlanError::internal(format!(
                                    "invalid Spark interval metadata {value:?}: {error}"
                                ))
                            },
                        )
                    })
                    .transpose()?
                    .or(spark_interval_metadata_for_expression(&value.expr, schema)?);
                let Some(metadata) = metadata else {
                    continue;
                };
                common_interval = Some(match common_interval {
                    None => metadata,
                    Some(current) => current.wider(metadata).ok_or_else(|| {
                        PlanError::invalid("incompatible interval types in VALUES column")
                    })?,
                });
            }

            let Some(common_interval) = common_interval else {
                continue;
            };
            let serialized = serde_json::to_string(&common_interval).map_err(|error| {
                PlanError::internal(format!(
                    "failed to serialize Spark interval metadata: {error}"
                ))
            })?;
            for row in values.iter_mut() {
                let value = &mut row[column_index];
                let mut metadata: HashMap<String, String> =
                    value.expr.metadata(schema)?.to_hashmap();
                metadata.extend(value.metadata.iter().cloned());
                metadata.insert(
                    spec::SAIL_SPARK_INTERVAL_METADATA_KEY.to_string(),
                    serialized.clone(),
                );
                let metadata = Some(FieldMetadata::from(metadata));
                let expression = std::mem::take(&mut value.expr);
                value.expr = match expression {
                    Expr::Literal(value, _) => Expr::Literal(value, metadata),
                    expression => expression.alias_with_metadata(
                        format!("__sail_values_interval_{column_index}"),
                        metadata,
                    ),
                };
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
