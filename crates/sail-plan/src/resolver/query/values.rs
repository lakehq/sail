use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use sail_common::spec;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::analyzer::typing;
use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn resolve_query_values(
        &mut self,
        rows: Vec<Vec<spec::Expr>>,
    ) -> PlanResult<Query> {
        let width = rows
            .first()
            .map(Vec::len)
            .ok_or_else(|| PlanError::analysis("VALUES requires at least one row"))?;
        if rows.iter().any(|row| row.len() != width) {
            return Err(PlanError::analysis("VALUES rows have different widths"));
        }
        let mut resolved = vec![];
        for row in rows {
            let mut values = vec![];
            for value in row {
                values.push(
                    self.resolve_named_expression(value, &QuerySchema::default())
                        .await?
                        .expression,
                );
            }
            resolved.push(values);
        }
        self.typed_values(resolved)
    }
}

impl PlanResolver<'_> {
    pub(in crate::resolver) fn typed_values(
        &mut self,
        resolved: Vec<Vec<Expr>>,
    ) -> PlanResult<Query> {
        let width = resolved
            .first()
            .map(Vec::len)
            .ok_or_else(|| PlanError::analysis("VALUES requires at least one row"))?;
        let mut attributes = vec![];
        for index in 0..width {
            let types = resolved
                .iter()
                .map(|row| Ok(row[index].result_type.resolved("VALUES element type")?))
                .collect::<PlanResult<Vec<_>>>()?;
            let nullable = types.iter().any(|field| field.nullable);
            let mut common = crate::analyzer::typing::coercion::common_type(
                &types
                    .iter()
                    .map(|field| field.data_type.clone())
                    .collect::<Vec<_>>(),
            )?;
            let has_nan = resolved.iter().any(|row| row[index].any(&|node| matches!(node,
                ExprKind::Literal(ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) | ScalarValue::Utf8View(Some(value))) if value.eq_ignore_ascii_case("nan"))
                || matches!(node, ExprKind::Literal(ScalarValue::Float32(Some(value))) if value.is_nan())
                || matches!(node, ExprKind::Literal(ScalarValue::Float64(Some(value))) if value.is_nan())).unwrap_or(false));
            if has_nan
                && types
                    .iter()
                    .all(|t| t.data_type.is_numeric() || t.data_type.is_null())
            {
                common = if types.iter().any(|t| {
                    matches!(
                        t.data_type,
                        DataType::Float64 | DataType::Decimal128(..) | DataType::Decimal256(..)
                    )
                }) {
                    DataType::Float64
                } else {
                    DataType::Float32
                };
            }
            let mut metadata = types
                .iter()
                .find(|t| !t.data_type.is_null())
                .map(|t| t.metadata.clone())
                .unwrap_or_default();
            let mut interval: Option<spec::SparkIntervalMetadata> = None;
            for field in &types {
                if let Some(value) = field.metadata.get(spec::SAIL_SPARK_INTERVAL_METADATA_KEY) {
                    let value = spec::SparkIntervalMetadata::from_json(value)?;
                    interval = Some(match interval {
                        None => value,
                        Some(current) => current.wider(value).ok_or_else(|| {
                            PlanError::invalid("incompatible interval types in VALUES column")
                        })?,
                    });
                }
            }
            if let Some(interval) = interval {
                metadata.insert(
                    spec::SAIL_SPARK_INTERVAL_METADATA_KEY.to_string(),
                    interval.to_json()?,
                );
            }
            attributes.push(
                self.context
                    .function_context(&mut self.state.ids)
                    .attribute(
                        format!("col{}", index + 1),
                        ExprType {
                            data_type: common,
                            nullable,
                            metadata,
                        },
                    ),
            );
        }
        let rows = resolved
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .zip(&attributes)
                    .map(|(value, attribute)| {
                        let target = attribute.result_type.resolved("VALUES target type")?;
                        let value = typing::coerce(value, &target.data_type, &mut self.state.ids)?;
                        let mut result_type =
                            value.result_type.resolved("VALUES coerced value")?.clone();
                        if result_type.metadata == target.metadata {
                            return Ok(value);
                        }
                        result_type.metadata.clone_from(&target.metadata);
                        Ok(Expr {
                            id: self.state.ids.expression(),
                            kind: Deferred::Resolved(ExprKind::WithMetadata(Box::new(value))),
                            result_type: Deferred::Resolved(result_type),
                        })
                    })
                    .collect::<PlanResult<_>>()
            })
            .collect::<PlanResult<_>>()?;
        Ok(Query {
            kind: Deferred::Resolved(QueryKind::Values { rows }),
            schema: Deferred::Resolved(QuerySchema { attributes }),
        })
    }
}
