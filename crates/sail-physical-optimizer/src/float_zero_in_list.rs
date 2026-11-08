use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{
    BinaryExpr, CastExpr, Column, InListExpr, Literal, TryCastExpr, in_list,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::{FilterExec, FilterExecBuilder};

/// Evaluates an IN list of floats that is split for pruning as a single IN list.
///
/// The type coercion rule makes `x IN (..., 0.0)` treat `-0.0` and `0.0` as equal by adding the
/// zero of the other sign to the list. When the list would then be too long for pruning, it
/// becomes `x IN (...) OR (x >= -0.0 AND x <= 0.0)` instead, and `x NOT IN (...)` becomes
/// `x NOT IN (...) AND (x < -0.0 OR x > 0.0)` after simplification. Once the predicate is pushed
/// down to the scan for pruning, the filter evaluates the equivalent IN list with both zeros,
/// which costs one lookup per row. The value must be a column (possibly cast), so that merging
/// its comparisons does not change how often it is evaluated.
pub(crate) fn specialize(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    plan.transform_up(|plan| {
        let Some(filter) = plan.downcast_ref::<FilterExec>() else {
            return Ok(Transformed::no(plan));
        };
        let schema = filter.input().schema();
        let predicate = Arc::clone(filter.predicate()).transform_up(|expr| {
            Ok(rewrite_expr(&expr, &schema).map_or(Transformed::no(expr), Transformed::yes))
        })?;
        if !predicate.transformed {
            return Ok(Transformed::no(plan));
        }
        Ok(Transformed::yes(Arc::new(
            FilterExecBuilder::from(filter)
                .with_predicate(predicate.data)
                .build()?,
        ) as Arc<dyn ExecutionPlan>))
    })
    .map(|result| result.data)
}

fn rewrite_expr(expr: &Arc<dyn PhysicalExpr>, schema: &Schema) -> Option<Arc<dyn PhysicalExpr>> {
    let binary = expr.downcast_ref::<BinaryExpr>()?;
    // `x IN (...) OR (x >= -0.0 AND x <= 0.0)`, or
    // `x NOT IN (...) AND (x < -0.0 OR x > 0.0)`
    let (negated, zeros_op, lower_op, upper_op) = match binary.op() {
        Operator::Or => (false, Operator::And, Operator::GtEq, Operator::LtEq),
        Operator::And => (true, Operator::Or, Operator::Lt, Operator::Gt),
        _ => return None,
    };
    for (list, zeros) in [
        (binary.left(), binary.right()),
        (binary.right(), binary.left()),
    ] {
        let Some(list) = list.downcast_ref::<InListExpr>() else {
            continue;
        };
        if list.negated() != negated || !is_column(list.expr()) {
            continue;
        }
        let Some(zeros) = zeros.downcast_ref::<BinaryExpr>() else {
            continue;
        };
        if *zeros.op() != zeros_op {
            continue;
        }
        let bounds = [zeros.left(), zeros.right()].map(|bound| {
            bound
                .downcast_ref::<BinaryExpr>()
                .filter(|bound| bound.left().as_ref() == list.expr().as_ref())
                .and_then(|bound| {
                    let negative = zero_sign(bound.right())?;
                    Some((*bound.op(), negative, Arc::clone(bound.right())))
                })
        });
        let [Some(first), Some(second)] = bounds else {
            continue;
        };
        let (negative_zero, positive_zero) = match (first, second) {
            ((op1, true, z1), (op2, false, z2)) | ((op2, false, z2), (op1, true, z1))
                if op1 == lower_op && op2 == upper_op =>
            {
                (z1, z2)
            }
            _ => continue,
        };
        let mut values = list.list().to_vec();
        values.extend([negative_zero, positive_zero]);
        // The predicate is left as it is if the IN list cannot be built.
        if let Ok(list) = in_list(Arc::clone(list.expr()), values, &negated, schema) {
            return Some(list);
        }
    }
    None
}

fn is_column(expr: &Arc<dyn PhysicalExpr>) -> bool {
    if expr.downcast_ref::<Column>().is_some() {
        true
    } else if let Some(cast) = expr.downcast_ref::<CastExpr>() {
        is_column(cast.expr())
    } else if let Some(cast) = expr.downcast_ref::<TryCastExpr>() {
        is_column(cast.expr())
    } else {
        false
    }
}

/// Whether the expression is a floating-point zero literal, and if so, whether it is `-0.0`.
fn zero_sign(expr: &Arc<dyn PhysicalExpr>) -> Option<bool> {
    let value = expr.downcast_ref::<Literal>()?.value();
    match value {
        ScalarValue::Float16(Some(v)) if v.to_f32() == 0.0 => Some(v.is_sign_negative()),
        ScalarValue::Float32(Some(v)) if *v == 0.0 => Some(v.is_sign_negative()),
        ScalarValue::Float64(Some(v)) if *v == 0.0 => Some(v.is_sign_negative()),
        _ => None,
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::config::ConfigOptions;
    use datafusion::physical_expr::ScalarFunctionExpr;
    use datafusion::physical_expr::expressions::{col, lit};
    use datafusion::physical_plan::empty::EmptyExec;

    use super::*;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("x", DataType::Float64, true),
            Field::new("y", DataType::Float64, true),
        ]))
    }

    fn binary(
        left: Arc<dyn PhysicalExpr>,
        op: Operator,
        right: Arc<dyn PhysicalExpr>,
    ) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(left, op, right))
    }

    fn split(column: &str, negated: bool) -> Arc<dyn PhysicalExpr> {
        split_value(col(column, &schema()).unwrap(), negated)
    }

    fn split_value(x: Arc<dyn PhysicalExpr>, negated: bool) -> Arc<dyn PhysicalExpr> {
        let schema = schema();
        let values = (1..20).map(|i| lit(f64::from(i))).collect::<Vec<_>>();
        let list = in_list(Arc::clone(&x), values, &negated, &schema).unwrap();
        let (op, zeros_op, lower_op, upper_op) = if negated {
            (Operator::And, Operator::Or, Operator::Lt, Operator::Gt)
        } else {
            (Operator::Or, Operator::And, Operator::GtEq, Operator::LtEq)
        };
        let zeros = binary(
            binary(Arc::clone(&x), lower_op, lit(-0.0_f64)),
            zeros_op,
            binary(x, upper_op, lit(0.0_f64)),
        );
        binary(list, op, zeros)
    }

    fn rewrite(predicate: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        let input = Arc::new(EmptyExec::new(schema()));
        let filter = Arc::new(FilterExec::try_new(predicate, input).unwrap());
        let plan = specialize(filter).unwrap();
        Arc::clone(plan.downcast_ref::<FilterExec>().unwrap().predicate())
    }

    #[test]
    fn test_split_in_list_is_evaluated_as_one_in_list() {
        for negated in [false, true] {
            let predicate = rewrite(split("x", negated));
            let list = predicate.downcast_ref::<InListExpr>().unwrap();
            assert_eq!(list.negated(), negated);
            assert_eq!(list.list().len(), 21);
            let zeros = list.list()[19..].iter().map(zero_sign).collect::<Vec<_>>();
            assert_eq!(zeros, vec![Some(true), Some(false)]);
        }
    }

    #[test]
    fn test_split_in_list_of_a_cast_column_is_evaluated_as_one_in_list() {
        let x = Arc::new(CastExpr::new(
            col("x", &schema()).unwrap(),
            DataType::Float64,
            None,
        ));
        let predicate = rewrite(split_value(x, false));
        assert_eq!(
            predicate.downcast_ref::<InListExpr>().unwrap().list().len(),
            21
        );
    }

    #[test]
    fn test_split_in_list_of_other_values_is_unchanged() {
        // A volatile value is evaluated as often as the predicate says.
        let random = Arc::new(
            ScalarFunctionExpr::try_new(
                datafusion::functions::math::random(),
                vec![],
                &schema(),
                Arc::new(ConfigOptions::default()),
            )
            .unwrap(),
        );
        let predicate = split_value(random, false);
        assert_eq!(rewrite(Arc::clone(&predicate)).as_ref(), predicate.as_ref());
    }

    #[test]
    fn test_other_predicates_are_unchanged() {
        let schema = schema();
        // The range is on another column.
        let x = col("x", &schema).unwrap();
        let y = col("y", &schema).unwrap();
        let values = (1..20).map(|i| lit(f64::from(i))).collect::<Vec<_>>();
        let list = in_list(Arc::clone(&x), values, &false, &schema).unwrap();
        let zeros = binary(
            binary(Arc::clone(&y), Operator::GtEq, lit(-0.0_f64)),
            Operator::And,
            binary(y, Operator::LtEq, lit(0.0_f64)),
        );
        let predicate = binary(list, Operator::Or, zeros);
        assert_eq!(rewrite(Arc::clone(&predicate)).as_ref(), predicate.as_ref());
        // The bounds are not both zeros.
        let values = (1..20).map(|i| lit(f64::from(i))).collect::<Vec<_>>();
        let list = in_list(Arc::clone(&x), values, &false, &schema).unwrap();
        let range = binary(
            binary(Arc::clone(&x), Operator::GtEq, lit(-1.0_f64)),
            Operator::And,
            binary(x, Operator::LtEq, lit(0.0_f64)),
        );
        let predicate = binary(list, Operator::Or, range);
        assert_eq!(rewrite(Arc::clone(&predicate)).as_ref(), predicate.as_ref());
        // The IN list is negated but the range is not.
        let mixed = split("x", true);
        let mixed = mixed.downcast_ref::<BinaryExpr>().unwrap();
        let predicate = binary(
            Arc::clone(mixed.left()),
            Operator::Or,
            split("x", false)
                .downcast_ref::<BinaryExpr>()
                .unwrap()
                .right()
                .clone(),
        );
        assert_eq!(rewrite(Arc::clone(&predicate)).as_ref(), predicate.as_ref());
    }
}
