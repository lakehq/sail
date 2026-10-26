use arrow_schema::{DataType, TimeUnit};
use chrono::{SecondsFormat, TimeZone, Utc};
use datafusion_common::ScalarValue;
use datafusion_expr::Volatility;
use sail_common::spec;
use sail_common_plan::rewrite::{QueryRewriter, walk_expression, walk_query};

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::analyzer::typing;
use crate::error::{PlanError, PlanResult};
use crate::resolver::{NamedExpression, PlanResolver};

impl PlanResolver<'_> {
    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn time_travel_options(
        &mut self,
        format: &str,
        temporal: Option<spec::TableTemporal>,
    ) -> PlanResult<Vec<(String, String)>> {
        let Some(temporal) = temporal else {
            return Ok(vec![]);
        };
        let format = format.to_ascii_lowercase();
        if !matches!(format.as_str(), "delta" | "iceberg") {
            return Err(PlanError::unsupported(format!(
                "SQL time travel is only supported for Delta and Iceberg tables, got {format}"
            )));
        }
        let (request, timestamp) = match temporal {
            spec::TableTemporal::Timestamp { value } => (value, true),
            spec::TableTemporal::Version { value } => (value, false),
        };
        let kind = if timestamp { "timestamp" } else { "version" };
        let expression = self
            .resolve_named_expression(request, &QuerySchema::default())
            .await
            .map_err(|e| {
                Self::invalid_time_travel_message(
                    kind,
                    format!("failed to resolve expression: {e}"),
                )
            })?
            .expression;
        let mut validator = TimeTravelExpression {
            ids: &mut self.context.ids,
            kind,
            plan_depth: 0,
        };
        let mut expression = validator.rewrite_expression(expression)?;
        if timestamp {
            let target = DataType::Timestamp(
                TimeUnit::Microsecond,
                self.types()
                    .resolve_timezone(&spec::TimestampType::Configured)?,
            );
            expression = typing::coerce(expression, &target, &mut self.context.ids)?;
        }
        let plan = self.project(
            Self::empty(true),
            vec![NamedExpression {
                name: kind.into(),
                expression,
            }],
        )?;
        let plan = self.limit(plan, 0, 2);
        let batches = self.execute(plan).await?;
        let rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();
        if rows != 1 {
            return Err(PlanError::invalid(format!(
                "Invalid time travel spec: expression must evaluate to a single value, got {rows} rows"
            )));
        }
        let batch = batches
            .iter()
            .find(|b| b.num_rows() > 0)
            .ok_or_else(|| PlanError::internal("time travel scalar missing row"))?;
        let value = ScalarValue::try_from_array(batch.column(0), 0)?;
        let option = if timestamp {
            (
                "timestampAsOf",
                Self::normalize_time_travel_timestamp_scalar(value)?,
            )
        } else if format == "delta" {
            (
                "versionAsOf",
                Self::scalar_to_time_travel_i64(&value, kind)?.to_string(),
            )
        } else {
            match value {
                ScalarValue::Utf8(Some(value))
                | ScalarValue::Utf8View(Some(value))
                | ScalarValue::LargeUtf8(Some(value)) => {
                    let value = value.trim();
                    match value.parse::<i64>() {
                        Ok(id) => ("snapshotId", id.to_string()),
                        Err(_) => ("ref", value.to_string()),
                    }
                }
                value => (
                    "snapshotId",
                    Self::scalar_to_time_travel_i64(&value, kind)?.to_string(),
                ),
            }
        };
        Ok(vec![(option.0.into(), option.1)])
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

    fn invalid_time_travel_message(kind: &str, message: impl Into<String>) -> PlanError {
        PlanError::invalid(format!(
            "Invalid time travel spec: {kind} {}.",
            message.into()
        ))
    }
}

struct TimeTravelExpression<'a> {
    ids: &'a mut IdGenerator,
    kind: &'a str,
    plan_depth: usize,
}

impl QueryRewriter for TimeTravelExpression<'_> {
    type Error = PlanError;

    fn rewrite_query(&mut self, plan: Query) -> PlanResult<Query> {
        self.plan_depth += 1;
        let result = walk_query(self, plan);
        self.plan_depth -= 1;
        result
    }

    fn rewrite_expression(&mut self, expression: Expr) -> PlanResult<Expr> {
        let node = expression.kind.resolved("time travel expression")?;
        let message = match node {
            ExprKind::Attribute(_) if self.plan_depth == 0 => {
                Some("expression cannot refer to any columns")
            }
            ExprKind::OuterAttribute { .. } => Some("expression cannot refer to any outer columns"),
            ExprKind::ScalarFunction { function, .. }
                if function.signature().volatility == Volatility::Volatile =>
            {
                Some("expression must be deterministic")
            }
            ExprKind::HigherOrderFunction { function, .. }
                if function.signature().volatility == Volatility::Volatile =>
            {
                Some("expression must be deterministic")
            }
            ExprKind::AggregateFunction { function, .. }
                if function.signature().volatility == Volatility::Volatile =>
            {
                Some("expression must be deterministic")
            }
            ExprKind::WindowFunction { function, .. }
                if function.signature().volatility == Volatility::Volatile =>
            {
                Some("expression must be deterministic")
            }
            _ => None,
        };
        if let Some(message) = message {
            return Err(PlanError::invalid(format!(
                "Invalid time travel spec: {} {message}.",
                self.kind
            )));
        }
        let mut expression = walk_expression(self, expression)?;
        if let Deferred::Resolved(ExprKind::ScalarSubquery(query)) = expression.kind {
            let schema = query.schema.clone();
            expression.kind = Deferred::Resolved(ExprKind::ScalarSubquery(Box::new(Query {
                schema,
                kind: Deferred::Resolved(QueryKind::Limit {
                    input: query,
                    skip: None,
                    fetch: Some(Box::new(Expr::literal(
                        self.ids.expression(),
                        ScalarValue::Int64(Some(2)),
                    ))),
                }),
            })));
        }
        Ok(expression)
    }
}
