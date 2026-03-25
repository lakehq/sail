use std::sync::Arc;

use chrono::{SecondsFormat, TimeZone, Utc};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchema, DFSchemaRef, ScalarValue};
use datafusion_expr::{EmptyRelation, Expr, Limit, LogicalPlan, Projection};
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_time_travel_options(
        &self,
        format: &str,
        temporal: Option<spec::TableTemporal>,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<(String, String)>> {
        let Some(temporal) = temporal else {
            return Ok(vec![]);
        };
        match format.to_ascii_lowercase().as_str() {
            "delta" => {
                self.resolve_delta_time_travel_options(temporal, state)
                    .await
            }
            "iceberg" => {
                self.resolve_iceberg_time_travel_options(temporal, state)
                    .await
            }
            other => Err(PlanError::unsupported(format!(
                "SQL time travel is only supported for Delta and Iceberg tables, got {other}",
            ))),
        }
    }

    async fn resolve_delta_time_travel_options(
        &self,
        temporal: spec::TableTemporal,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<(String, String)>> {
        match temporal {
            spec::TableTemporal::Timestamp { value } => Ok(vec![(
                "timestampAsOf".to_string(),
                self.evaluate_time_travel_timestamp(value, state).await?,
            )]),
            spec::TableTemporal::Version { value } => Ok(vec![(
                "versionAsOf".to_string(),
                self.evaluate_time_travel_version_i64(value, "version", state)
                    .await?
                    .to_string(),
            )]),
        }
    }

    async fn resolve_iceberg_time_travel_options(
        &self,
        temporal: spec::TableTemporal,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<(String, String)>> {
        match temporal {
            spec::TableTemporal::Timestamp { value } => Ok(vec![(
                "timestampAsOf".to_string(),
                self.evaluate_time_travel_timestamp(value, state).await?,
            )]),
            spec::TableTemporal::Version { value } => {
                match self
                    .evaluate_time_travel_version_for_iceberg(value, state)
                    .await?
                {
                    IcebergVersionAsOf::SnapshotId(snapshot_id) => {
                        Ok(vec![("snapshotId".to_string(), snapshot_id.to_string())])
                    }
                    IcebergVersionAsOf::Reference(reference) => {
                        Ok(vec![("ref".to_string(), reference)])
                    }
                }
            }
        }
    }

    async fn evaluate_time_travel_timestamp(
        &self,
        expr: spec::Expr,
        state: &mut PlanResolverState,
    ) -> PlanResult<String> {
        let resolved = self
            .resolve_time_travel_expression(expr, "timestamp", state)
            .await?;
        let timestamp = Expr::Cast(datafusion_expr::expr::Cast {
            expr: Box::new(resolved),
            data_type: DataType::Timestamp(
                TimeUnit::Microsecond,
                self.resolve_timezone(&spec::TimestampType::Configured)?,
            ),
        });
        let scalar = self.execute_time_travel_scalar(timestamp).await?;
        Self::normalize_time_travel_timestamp_scalar(scalar)
    }

    async fn evaluate_time_travel_version_i64(
        &self,
        expr: spec::Expr,
        kind: &str,
        state: &mut PlanResolverState,
    ) -> PlanResult<i64> {
        let resolved = self
            .resolve_time_travel_expression(expr, kind, state)
            .await?;
        let scalar = self.execute_time_travel_scalar(resolved).await?;
        Self::scalar_to_time_travel_i64(&scalar, kind)
    }

    async fn evaluate_time_travel_version_for_iceberg(
        &self,
        expr: spec::Expr,
        state: &mut PlanResolverState,
    ) -> PlanResult<IcebergVersionAsOf> {
        let resolved = self
            .resolve_time_travel_expression(expr, "version", state)
            .await?;
        let scalar = self.execute_time_travel_scalar(resolved).await?;
        match scalar {
            ScalarValue::Utf8(Some(value))
            | ScalarValue::LargeUtf8(Some(value))
            | ScalarValue::Utf8View(Some(value)) => {
                let value = value.trim().to_string();
                match value.parse::<i64>() {
                    Ok(snapshot_id) => Ok(IcebergVersionAsOf::SnapshotId(snapshot_id)),
                    Err(_) => Ok(IcebergVersionAsOf::Reference(value)),
                }
            }
            _ => Ok(IcebergVersionAsOf::SnapshotId(
                Self::scalar_to_time_travel_i64(&scalar, "version")?,
            )),
        }
    }

    async fn resolve_time_travel_expression(
        &self,
        expr: spec::Expr,
        kind: &str,
        state: &mut PlanResolverState,
    ) -> PlanResult<Expr> {
        let schema = Arc::new(DFSchema::empty());
        let resolved = self
            .resolve_expression(expr, &schema, state)
            .await
            .map_err(|e| Self::invalid_time_travel_spec(kind, e))?;
        self.validate_time_travel_expression(&resolved, kind)?;
        Ok(resolved)
    }

    // TODO: Extract as general utilities and incorporated into LiteralEvaluator.
    fn validate_time_travel_expression(&self, expr: &Expr, kind: &str) -> PlanResult<()> {
        if expr.any_column_refs() {
            return Err(Self::invalid_time_travel_message(
                kind,
                "expression cannot refer to any columns",
            ));
        }
        if expr.contains_outer() {
            return Err(Self::invalid_time_travel_message(
                kind,
                "expression cannot refer to any outer columns",
            ));
        }
        if expr.is_volatile() {
            return Err(Self::invalid_time_travel_message(
                kind,
                "expression must be deterministic",
            ));
        }
        let mut has_correlated_subquery = false;
        let mut has_volatile_subquery = false;
        expr.apply(|nested| {
            if let Expr::ScalarSubquery(subquery) = nested {
                if !subquery.outer_ref_columns.is_empty()
                    || subquery.subquery.contains_outer_reference()
                {
                    has_correlated_subquery = true;
                    return Ok(TreeNodeRecursion::Stop);
                }
                if Self::logical_plan_has_volatile_expressions(&subquery.subquery)? {
                    has_volatile_subquery = true;
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        if has_correlated_subquery {
            return Err(Self::invalid_time_travel_message(
                kind,
                "scalar subquery cannot be correlated",
            ));
        }
        if has_volatile_subquery {
            return Err(Self::invalid_time_travel_message(
                kind,
                "expression must be deterministic",
            ));
        }
        Ok(())
    }

    fn logical_plan_has_volatile_expressions(
        plan: &LogicalPlan,
    ) -> datafusion_common::Result<bool> {
        let mut contains = false;
        plan.apply(|node| {
            node.apply_expressions(|expr| {
                if expr.is_volatile() {
                    contains = true;
                    Ok(TreeNodeRecursion::Stop)
                } else {
                    Ok(TreeNodeRecursion::Continue)
                }
            })?;
            Ok(if contains {
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })?;
        Ok(contains)
    }

    async fn execute_time_travel_scalar(&self, expr: Expr) -> PlanResult<ScalarValue> {
        let expr = Self::cap_time_travel_scalar_subqueries(expr)?;
        let plan = Self::build_time_travel_scalar_plan(expr)?;
        let batches = self.ctx.execute_logical_plan(plan).await?.collect().await?;
        let mut total_rows = 0usize;
        let mut value = None;
        for batch in batches {
            total_rows += batch.num_rows();
            if value.is_none() && batch.num_rows() > 0 {
                value = Some(ScalarValue::try_from_array(batch.column(0).as_ref(), 0)?);
            }
        }
        match (total_rows, value) {
            (1, Some(value)) => Ok(value),
            (rows, _) => Err(PlanError::invalid(format!(
                "Invalid time travel spec: expression must evaluate to a single value, got {rows} rows",
            ))),
        }
    }

    fn cap_time_travel_scalar_subqueries(expr: Expr) -> datafusion_common::Result<Expr> {
        expr.transform(|nested| match nested {
            Expr::ScalarSubquery(subquery) => Ok(Transformed::yes(Expr::ScalarSubquery(
                datafusion_expr::logical_plan::Subquery {
                    subquery: Arc::new(Self::limit_plan_to_two_rows(Arc::unwrap_or_clone(
                        subquery.subquery,
                    ))),
                    outer_ref_columns: subquery.outer_ref_columns,
                    spans: subquery.spans,
                },
            ))),
            _ => Ok(Transformed::no(nested)),
        })
        .data()
    }

    fn build_time_travel_scalar_plan(expr: Expr) -> PlanResult<LogicalPlan> {
        let projection = LogicalPlan::Projection(Projection::try_new(
            vec![expr],
            Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: true,
                schema: DFSchemaRef::new(DFSchema::empty()),
            })),
        )?);
        Ok(Self::limit_plan_to_two_rows(projection))
    }

    fn limit_plan_to_two_rows(input: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Limit(Limit {
            skip: None,
            fetch: Some(Box::new(Expr::Literal(ScalarValue::Int64(Some(2)), None))),
            input: Arc::new(input),
        })
    }

    fn normalize_time_travel_timestamp_scalar(scalar: ScalarValue) -> PlanResult<String> {
        match scalar {
            ScalarValue::TimestampMicrosecond(Some(value), Some(_)) => {
                let datetime = Utc.timestamp_micros(value).single().ok_or_else(|| {
                    PlanError::invalid("Invalid time travel spec: timestamp is out of range")
                })?;
                Ok(datetime.to_rfc3339_opts(SecondsFormat::Micros, true))
            }
            ScalarValue::TimestampMicrosecond(Some(value), None) => {
                let datetime = Utc.timestamp_micros(value).single().ok_or_else(|| {
                    PlanError::invalid("Invalid time travel spec: timestamp is out of range")
                })?;
                Ok(datetime
                    .naive_utc()
                    .format("%Y-%m-%d %H:%M:%S%.6f")
                    .to_string())
            }
            ScalarValue::TimestampMicrosecond(None, _) => Err(PlanError::invalid(
                "Invalid time travel spec: timestamp expression evaluated to NULL",
            )),
            other => Err(PlanError::invalid(format!(
                "Invalid time travel spec: timestamp expression must evaluate to a timestamp, got {other:?}",
            ))),
        }
    }

    fn scalar_to_time_travel_i64(scalar: &ScalarValue, kind: &str) -> PlanResult<i64> {
        match scalar {
            ScalarValue::Int8(Some(value)) => Ok(i64::from(*value)),
            ScalarValue::Int16(Some(value)) => Ok(i64::from(*value)),
            ScalarValue::Int32(Some(value)) => Ok(i64::from(*value)),
            ScalarValue::Int64(Some(value)) => Ok(*value),
            ScalarValue::UInt8(Some(value)) => Ok(i64::from(*value)),
            ScalarValue::UInt16(Some(value)) => Ok(i64::from(*value)),
            ScalarValue::UInt32(Some(value)) => Ok(i64::from(*value)),
            ScalarValue::UInt64(Some(value)) => i64::try_from(*value).map_err(|_| {
                Self::invalid_time_travel_message(kind, "integer value is out of range")
            }),
            ScalarValue::Utf8(Some(value))
            | ScalarValue::LargeUtf8(Some(value))
            | ScalarValue::Utf8View(Some(value)) => value.trim().parse::<i64>().map_err(|_| {
                Self::invalid_time_travel_message(
                    kind,
                    "expression must evaluate to an integer or numeric string",
                )
            }),
            ScalarValue::Null => Err(Self::invalid_time_travel_message(
                kind,
                "expression evaluated to NULL",
            )),
            other => Err(Self::invalid_time_travel_message(
                kind,
                format!("expression must evaluate to an integer, got {other:?}"),
            )),
        }
    }

    fn invalid_time_travel_spec(kind: &str, error: impl std::fmt::Display) -> PlanError {
        Self::invalid_time_travel_message(kind, format!("failed to resolve expression: {error}"))
    }

    fn invalid_time_travel_message(kind: &str, message: impl Into<String>) -> PlanError {
        PlanError::invalid(format!(
            "Invalid time travel spec: {kind} {}.",
            message.into()
        ))
    }
}

enum IcebergVersionAsOf {
    SnapshotId(i64),
    Reference(String),
}
