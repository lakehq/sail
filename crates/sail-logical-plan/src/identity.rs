use datafusion::arrow::datatypes::DataType;
use datafusion::functions_window::row_number::row_number_udwf;
use datafusion_common::{DFSchema, Result, ScalarValue};
use datafusion_expr::expr::{NullTreatment, WindowFunction, WindowFunctionParams};
use datafusion_expr::{
    Expr, ExprSchemable, ScalarUDF, WindowFrame, WindowFunctionDefinition, lit, when,
};
use sail_common_datafusion::catalog::CatalogTableColumnIdentity;
use sail_function::scalar::math::spark_try_add::SparkTryAdd;
use sail_function::scalar::math::spark_try_mult::SparkTryMult;
use sail_function::scalar::misc::raise_error::RaiseError;

/// A single allocation domain, enforced by the window's required distribution.
pub fn identity_row_number(partition_by: Vec<Expr>) -> Expr {
    Expr::WindowFunction(Box::new(WindowFunction {
        fun: WindowFunctionDefinition::WindowUDF(row_number_udwf()),
        params: WindowFunctionParams {
            args: vec![],
            partition_by,
            order_by: vec![],
            window_frame: WindowFrame::new(None),
            filter: None,
            null_treatment: Some(NullTreatment::RespectNulls),
            distinct: false,
        },
    }))
}

/// Checked BIGINT arithmetic shared by INSERT and MERGE. A missing base represents
/// an exhausted watermark; the caller must evaluate this only on generating rows.
pub fn identity_value_expr(
    column_name: &str,
    base: Option<i64>,
    step: i64,
    row_number: Expr,
    schema: &DFSchema,
) -> Result<Expr> {
    let row_offset = row_number.cast_to(&DataType::Int64, schema)? - lit(1_i64);
    let product = ScalarUDF::from(SparkTryMult::new()).call(vec![row_offset, lit(step)]);
    let value = ScalarUDF::from(SparkTryAdd::new())
        .call(vec![lit(ScalarValue::Int64(base)), product.clone()]);
    let raise = ScalarUDF::from(RaiseError::new()).call(vec![lit(format!(
        "[DELTA_IDENTITY_COLUMN_VALUE_OVERFLOW] identity column `{column_name}` overflowed BIGINT while generating values"
    ))]);
    when(product.is_null().or(value.clone().is_null()), raise).otherwise(value)
}

pub fn identity_next_value(identity: &CatalogTableColumnIdentity) -> Option<i64> {
    match identity.high_water_mark {
        Some(high_water_mark) => high_water_mark.checked_add(identity.step),
        None => Some(identity.start),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_value_checks_both_bigint_boundaries() {
        for (start, step, high_water_mark, expected) in [
            (10, 3, None, Some(10)),
            (10, 3, Some(43), Some(46)),
            (0, -2, Some(-4), Some(-6)),
            (0, 1, Some(i64::MAX), None),
            (0, -1, Some(i64::MIN), None),
        ] {
            assert_eq!(
                identity_next_value(&CatalogTableColumnIdentity {
                    start,
                    step,
                    high_water_mark,
                    allow_explicit_insert: false,
                }),
                expected
            );
        }
    }
}
