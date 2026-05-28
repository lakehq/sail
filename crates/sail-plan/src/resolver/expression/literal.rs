use std::str::FromStr;

use arrow::array::timezone::Tz;
use arrow::datatypes::Date32Type;
use chrono::{NaiveTime, Timelike};
use datafusion_expr::expr;
use datafusion_expr::expr::FieldMetadata;
use sail_common::spark::extension::{SparkIntervalMetadata, SparkYearMonthIntervalType};
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::plan::PlanService;
use sail_common_datafusion::utils::datetime::localize_with_fallback;
use sail_sql_analyzer::parser::{parse_date, parse_time, parse_timestamp};

use crate::config::DefaultTimestampType;
use crate::error::{PlanError, PlanResult};
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

fn year_month_interval_field_number(field: spec::IntervalFieldType) -> PlanResult<i32> {
    match field {
        spec::IntervalFieldType::Year => Ok(0),
        spec::IntervalFieldType::Month => Ok(1),
        field => Err(PlanError::invalid(format!(
            "{field:?} is not valid for a year-month interval"
        ))),
    }
}

fn year_month_interval_metadata(
    start_field: Option<spec::IntervalFieldType>,
    end_field: Option<spec::IntervalFieldType>,
) -> PlanResult<SparkIntervalMetadata> {
    Ok(SparkIntervalMetadata::new(
        start_field
            .map(year_month_interval_field_number)
            .transpose()?,
        end_field
            .map(year_month_interval_field_number)
            .transpose()?,
    ))
}

impl PlanResolver<'_> {
    pub(super) fn resolve_expression_literal(
        &self,
        literal: spec::Literal,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let metadata = Self::literal_field_metadata(&literal)?;
        let literal = self.resolve_literal(literal, state)?;
        let service = self.ctx.extension::<PlanService>()?;
        let name = service
            .plan_formatter()
            .literal_to_string(&literal, &self.config.session_timezone)?;
        Ok(NamedExpr::new(
            vec![name],
            expr::Expr::Literal(literal, metadata),
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

    fn literal_field_metadata(literal: &spec::Literal) -> PlanResult<Option<FieldMetadata>> {
        let spec::Literal::IntervalYearMonth {
            start_field,
            end_field,
            ..
        } = literal
        else {
            return Ok(None);
        };
        let metadata = year_month_interval_metadata(*start_field, *end_field)?
            .arrow_metadata(SparkYearMonthIntervalType::NAME)
            .into_iter()
            .collect();
        Ok(Some(FieldMetadata::new(metadata)))
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

    pub(super) fn resolve_expression_time(
        &self,
        value: String,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let time = parse_time(&value)?;

        // Convert to NaiveTime which validates hour (0-23), minute/second (0-59)
        let naive_time: NaiveTime = time.try_into()?;

        // Use chrono methods to get microseconds since midnight
        // Nanoseconds beyond microsecond precision are truncated to match Spark behavior
        let seconds_from_midnight = naive_time.num_seconds_from_midnight() as i64;
        let nanoseconds = naive_time.nanosecond() as i64;
        let microseconds = seconds_from_midnight * 1_000_000 + nanoseconds / 1_000;

        let literal = spec::Literal::Time64Microsecond {
            microseconds: Some(microseconds),
        };
        self.resolve_expression_literal(literal, state)
    }
}
