use std::str::FromStr;

use arrow::array::timezone::Tz;
use arrow::datatypes::Date32Type;
use datafusion_expr::expr;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::plan::PlanService;
use sail_common_datafusion::utils::datetime::localize_with_fallback;
use sail_sql_analyzer::parser::{parse_date, parse_timestamp};

use crate::config::DefaultTimestampType;
use crate::error::PlanResult;
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) fn resolve_expression_literal(
        &self,
        literal: spec::Literal,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let literal = self.resolve_literal(literal, state)?;
        let service = self.ctx.extension::<PlanService>()?;
        let name = service
            .plan_formatter()
            .literal_to_string(&literal, &self.config.session_timezone)?;
        Ok(NamedExpr::new(
            vec![name],
            expr::Expr::Literal(literal, None),
        ))
    }

    pub(super) fn resolve_expression_date(
        &self,
        value: String,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let date = parse_date(&value)?;
        let literal = spec::Literal::Date32 {
            days: Some(Date32Type::from_naive_date(date.try_into()?)),
        };
        self.resolve_expression_literal(literal, state)
    }

    pub(super) fn resolve_expression_timestamp(
        &self,
        value: String,
        timestamp_type: spec::TimestampType,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let (datetime, timezone) = parse_timestamp(&value).and_then(|x| x.into_naive())?;
        let timezone = if timezone.is_empty() {
            None
        } else {
            Some(timezone.parse::<Tz>()?)
        };
        let (datetime, timestamp_type) = match (timestamp_type, timezone, self.config.default_timestamp_type) {
            (spec::TimestampType::Configured, None, DefaultTimestampType::TimestampLtz)
            | (spec::TimestampType::WithLocalTimeZone, None, _) => {
                let tz = Tz::from_str(&self.config.session_timezone)?;
                let datetime = localize_with_fallback(&tz, &datetime)?;
                (datetime, spec::TimestampType::WithLocalTimeZone)
            }
            (spec::TimestampType::Configured, Some(tz), _)
            | (spec::TimestampType::WithLocalTimeZone, Some(tz), _) => {
                let datetime = localize_with_fallback(&tz, &datetime)?;
                (datetime, spec::TimestampType::WithLocalTimeZone)
            }
            (spec::TimestampType::Configured, None, DefaultTimestampType::TimestampNtz)
            // If the timestamp type is explicitly `TIMESTAMP_NTZ`, the time zone in the literal
            // is simply ignored.
            | (spec::TimestampType::WithoutTimeZone, _, _) => {
                (datetime.and_utc(), spec::TimestampType::WithoutTimeZone)
            }
        };
        let literal = spec::Literal::TimestampMicrosecond {
            microseconds: Some(datetime.timestamp_micros()),
            timestamp_type,
        };
        self.resolve_expression_literal(literal, state)
    }
}
