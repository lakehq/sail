use std::ops::{Div, Mul};
use std::sync::Arc;

use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use datafusion_common::DFSchemaRef;
use datafusion_expr::{cast, expr, lit, try_cast, ExprSchemable, ScalarUDF};
use sail_common::datetime::time_unit_to_multiplier;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::PlanService;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_interval::{
    SparkCalendarInterval, SparkDayTimeInterval, SparkYearMonthInterval,
};
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::spark_to_string::{SparkToLargeUtf8, SparkToUtf8, SparkToUtf8View};

use crate::error::PlanResult;
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_expression_cast(
        &self,
        expr: spec::Expr,
        cast_to_type: spec::DataType,
        _rename: bool,
        is_try: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let cast_to_type = self.resolve_data_type(&cast_to_type, state)?;
        let NamedExpr { expr, name, .. } =
            self.resolve_named_expression(expr, schema, state).await?;
        let expr_type = expr.get_type(schema)?;
        let name = if need_rename_cast(&expr) {
            let service = self.ctx.extension::<PlanService>()?;
            let data_type_string = service
                .plan_formatter()
                .data_type_to_simple_string(&cast_to_type)?;
            vec![format!(
                "{}CAST({} AS {})",
                if is_try { "TRY_" } else { "" },
                name.one()?,
                data_type_string.to_ascii_uppercase()
            )]
        } else {
            name
        };
        let override_string_cast = matches!(
            expr_type,
            DataType::Date32
                | DataType::Date64
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Duration(_)
                | DataType::Interval(_)
                | DataType::Timestamp(_, _)
                | DataType::List(_)
                | DataType::LargeList(_)
                | DataType::FixedSizeList(_, _)
                | DataType::ListView(_)
                | DataType::LargeListView(_)
                | DataType::Struct(_)
                | DataType::Map(_, _)
        );
        let expr = match (expr_type, cast_to_type.clone(), is_try) {
            (from, DataType::Timestamp(time_unit, _) | DataType::Duration(time_unit), _)
                if from.is_numeric() =>
            {
                cast(
                    expr.mul(lit(time_unit_to_multiplier(&time_unit))),
                    cast_to_type,
                )
            }
            (DataType::Timestamp(time_unit, _) | DataType::Duration(time_unit), to, _)
                if to.is_numeric() =>
            {
                cast(
                    lit(1.0)
                        .div(lit(time_unit_to_multiplier(&time_unit)))
                        .mul(cast(expr, DataType::Int64)),
                    to,
                )
            }
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                DataType::Interval(IntervalUnit::YearMonth),
                _,
            ) => ScalarUDF::new_from_impl(SparkYearMonthInterval::new()).call(vec![expr]),
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                DataType::Duration(TimeUnit::Microsecond),
                _,
            ) => ScalarUDF::new_from_impl(SparkDayTimeInterval::new()).call(vec![expr]),
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                DataType::Interval(IntervalUnit::MonthDayNano),
                _,
            ) => ScalarUDF::new_from_impl(SparkCalendarInterval::new()).call(vec![expr]),
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                DataType::Date32,
                is_try,
            ) => ScalarUDF::new_from_impl(SparkDate::new(is_try)).call(vec![expr]),
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                DataType::Timestamp(TimeUnit::Microsecond, tz),
                is_try,
            ) => Arc::new(ScalarUDF::new_from_impl(SparkTimestamp::try_new(
                tz, is_try,
            )?))
            .call(vec![expr]),
            (_, DataType::Utf8, _) if override_string_cast => {
                ScalarUDF::new_from_impl(SparkToUtf8::new()).call(vec![expr])
            }
            (_, DataType::LargeUtf8, _) if override_string_cast => {
                ScalarUDF::new_from_impl(SparkToLargeUtf8::new()).call(vec![expr])
            }
            (_, DataType::Utf8View, _) if override_string_cast => {
                ScalarUDF::new_from_impl(SparkToUtf8View::new()).call(vec![expr])
            }
            (_, to, true) => try_cast(expr, to),
            (_, to, _) => cast(expr, to),
        };
        Ok(NamedExpr::new(name, expr))
    }
}

fn need_rename_cast(expr: &expr::Expr) -> bool {
    match expr {
        expr::Expr::Alias(_) | expr::Expr::Column(_) | expr::Expr::OuterReferenceColumn(..) => {
            false
        }
        expr::Expr::Cast(cast) => need_rename_cast(cast.expr.as_ref()),
        expr::Expr::TryCast(try_cast) => need_rename_cast(try_cast.expr.as_ref()),
        _ => true,
    }
}
