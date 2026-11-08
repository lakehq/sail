use std::cmp::Ordering;

use datafusion::arrow::compute::kernels::cast::single_decimal_to_float_lossy;
use datafusion::arrow::datatypes::{DataType, Decimal128Type};
use datafusion::optimizer::simplify_expressions::SimplifyExpressions as DataFusionSimplifyExpressions;
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchema, Result, ScalarValue};
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::utils::merge_schema;
use datafusion_expr::{BinaryExpr, Expr, ExprSchemable, LogicalPlan, Operator, lit};

/// Extend the existing simplification pass with exact numeric widening comparisons.
#[derive(Debug, Default)]
pub struct SimplifyExpressions(DataFusionSimplifyExpressions);

impl OptimizerRule for SimplifyExpressions {
    fn name(&self) -> &str {
        self.0.name()
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // Folding first exposes constants written as casts or constant expressions.
        self.0.rewrite(plan, config)?.transform_data(|plan| {
            let mut candidate = false;
            plan.apply_expressions(|expr| {
                candidate = expr.exists(|expr| {
                    let Expr::BinaryExpr(binary) = expr else {
                        return Ok(false);
                    };
                    let pair = |value: &Expr, literal: &Expr| {
                        matches!(value, Expr::Cast(_) | Expr::TryCast(_))
                            && matches!(
                                literal,
                                Expr::Literal(ScalarValue::Float32(_) | ScalarValue::Float64(_), _)
                            )
                    };
                    Ok(pair(&binary.left, &binary.right) || pair(&binary.right, &binary.left))
                })?;
                Ok(if candidate {
                    TreeNodeRecursion::Stop
                } else {
                    TreeNodeRecursion::Continue
                })
            })?;
            if !candidate {
                return Ok(Transformed::no(plan));
            }
            let schema = match &plan {
                LogicalPlan::TableScan(scan) => DFSchema::try_from_qualified_schema(
                    scan.table_name.clone(),
                    &scan.source.schema(),
                )?,
                _ => merge_schema(&plan.inputs()),
            };
            let names = NamePreserver::new(&plan);
            let rewrite = |expr: Expr| {
                let name = names.save(&expr);
                expr.transform_up(|expr| unwrap_comparison(expr, &schema))
                    .map(|result| result.update_data(|expr| name.restore(expr)))
            };
            plan.map_expressions(|expr| {
                if matches!(expr, Expr::GroupingSet(_)) {
                    expr.map_children(rewrite)
                } else {
                    rewrite(expr)
                }
            })
        })
    }
}

fn unwrap_comparison(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let Expr::BinaryExpr(binary) = &expr else {
        return Ok(Transformed::no(expr));
    };
    if !matches!(
        binary.op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom
    ) {
        return Ok(Transformed::no(expr));
    }
    let operands = unwrap_pair(&binary.left, &binary.right, schema).or_else(|| {
        unwrap_pair(&binary.right, &binary.left, schema).map(|(value, literal)| (literal, value))
    });
    match operands {
        Some((left, right)) => Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(left),
            binary.op,
            Box::new(right),
        )))),
        None => match unwrap_decimal_comparison(binary, schema) {
            Some(expr) => Ok(Transformed::yes(expr)),
            None => Ok(Transformed::no(expr)),
        },
    }
}

/// Rewrites `CAST(d AS DOUBLE) <op> c`, where `d` is a DECIMAL value and `c` a DOUBLE constant
/// (how Spark compares DECIMAL and DOUBLE values), to comparisons of `d` with DECIMAL constants.
/// This avoids casting every value, and lets the comparison use the statistics of `d`.
///
/// The cast is monotonic, so the values of `d` whose cast compares less than (or not greater
/// than) `c` are the values less than a threshold. The thresholds are found with the cast itself
/// (Arrow's `single_decimal_to_float_lossy`, including its rounding) and with the order the
/// comparison uses (DataFusion's `apply_cmp` turns `-0.0` into `0.0` and then compares with the
/// total order of Arrow's kernels), so the result is the same for every value, including NULL
/// values and NaN, infinite, or negative zero constants. Values out of the range of the DECIMAL
/// precision are compared with its limits.
fn unwrap_decimal_comparison(binary: &BinaryExpr, schema: &DFSchema) -> Option<Expr> {
    let (value, data_type, constant, op) = decimal_cast_pair(&binary.left, &binary.right, schema)
        .map(|(value, data_type, constant)| (value, data_type, constant, binary.op))
        .or_else(|| {
            let (value, data_type, constant) =
                decimal_cast_pair(&binary.right, &binary.left, schema)?;
            Some((value, data_type, constant, binary.op.swap()?))
        })?;
    let (precision, scale) = match data_type {
        DataType::Decimal32(p, s) | DataType::Decimal64(p, s) | DataType::Decimal128(p, s) => {
            (p, s)
        }
        _ => return None,
    };
    let max = 10_i128.checked_pow(u32::from(precision))? - 1;
    let min = -max;
    let normalize = |x: f64| if x == 0.0 { 0.0 } else { x };
    let constant = normalize(constant);
    let to_double = |raw: i128| {
        normalize(single_decimal_to_float_lossy::<Decimal128Type, _>(
            &|x: i128| x as f64,
            raw,
            i32::from(scale),
        ))
    };
    // The first value whose cast is not less than the constant, and the first value whose cast
    // is greater than the constant, or `max + 1` if there is none.
    let first = |predicate: &dyn Fn(Ordering) -> bool| {
        let (mut low, mut high) = (min, max + 1);
        while low < high {
            // The floor of the average, without overflow for the widest precision.
            let middle = (low & high) + ((low ^ high) >> 1);
            if predicate(to_double(middle).total_cmp(&constant)) {
                high = middle;
            } else {
                low = middle + 1;
            }
        }
        low
    };
    let not_less = first(&|ordering| ordering != Ordering::Less);
    let greater = first(&|ordering| ordering == Ordering::Greater);
    let literal = |raw: i128| -> Option<Expr> {
        let value = match data_type {
            DataType::Decimal32(..) => {
                ScalarValue::Decimal32(Some(i32::try_from(raw).ok()?), precision, scale)
            }
            DataType::Decimal64(..) => {
                ScalarValue::Decimal64(Some(i64::try_from(raw).ok()?), precision, scale)
            }
            _ => ScalarValue::Decimal128(Some(raw), precision, scale),
        };
        Some(lit(value))
    };
    // `d < threshold` and `d >= threshold`, for a threshold of at most `max + 1`.
    let less = |threshold: i128| -> Option<Expr> {
        Some(if threshold > max {
            value.clone().lt_eq(literal(max)?)
        } else {
            value.clone().lt(literal(threshold)?)
        })
    };
    let not_less_than = |threshold: i128| -> Option<Expr> {
        Some(if threshold > max {
            value.clone().gt(literal(max)?)
        } else {
            value.clone().gt_eq(literal(threshold)?)
        })
    };
    match op {
        Operator::Lt => less(not_less),
        Operator::LtEq => less(greater),
        Operator::Gt => not_less_than(greater),
        Operator::GtEq => not_less_than(not_less),
        // When one value casts to the constant, an equality keeps equality pruning.
        Operator::Eq if greater == not_less + 1 => Some(value.clone().eq(literal(not_less)?)),
        Operator::NotEq if greater == not_less + 1 => {
            Some(value.clone().not_eq(literal(not_less)?))
        }
        // The two-sided forms evaluate the value twice, so they are only used for a value that
        // is the same each time.
        Operator::Eq if !value.is_volatile() => Some(not_less_than(not_less)?.and(less(greater)?)),
        Operator::NotEq if !value.is_volatile() => {
            Some(less(not_less)?.or(not_less_than(greater)?))
        }
        _ => None,
    }
}

/// Returns the DECIMAL value, its type, and the constant of `CAST(value AS DOUBLE)` and a DOUBLE
/// constant.
fn decimal_cast_pair(
    cast: &Expr,
    literal: &Expr,
    schema: &DFSchema,
) -> Option<(Expr, DataType, f64)> {
    let Expr::Cast(cast) = cast else {
        return None;
    };
    let Expr::Literal(ScalarValue::Float64(Some(constant)), _) = literal else {
        return None;
    };
    if cast.field.data_type() != &DataType::Float64 {
        return None;
    }
    let data_type = cast.expr.get_type(schema).ok()?;
    matches!(
        data_type,
        DataType::Decimal32(..) | DataType::Decimal64(..) | DataType::Decimal128(..)
    )
    .then(|| (cast.expr.as_ref().clone(), data_type, *constant))
}

fn unwrap_pair(cast: &Expr, literal: &Expr, schema: &DFSchema) -> Option<(Expr, Expr)> {
    let (value, target) = match cast {
        Expr::Cast(cast) => (&cast.expr, cast.field.data_type()),
        Expr::TryCast(cast) => {
            // Physical TRY_CAST is nullable even for total casts of nonnullable inputs.
            if !cast.expr.nullable(schema).ok()? {
                return None;
            }
            (&cast.expr, cast.field.data_type())
        }
        _ => return None,
    };
    let Expr::Literal(literal, _) = literal else {
        return None;
    };
    let source = value.get_type(schema).ok()?;
    // Spark's UnwrapCastInBinaryComparison excludes INT->FLOAT and BIGINT->FLOAT/DOUBLE.
    if !matches!(
        (&source, target),
        (DataType::Int8 | DataType::Int16, DataType::Float32)
            | (
                DataType::Int8 | DataType::Int16 | DataType::Int32,
                DataType::Float64
            )
            | (DataType::Float32, DataType::Float64)
    ) || literal.data_type() != *target
    {
        return None;
    }
    // NaN widening can quiet signaling payloads, so a round trip is insufficient
    // to prove that moving a NaN literal preserves its comparison class.
    if !matches!(literal, ScalarValue::Float32(Some(x)) if x.is_finite())
        && !matches!(literal, ScalarValue::Float64(Some(x)) if x.is_finite())
    {
        return None;
    }
    let narrowed = literal.cast_to(&source).ok()?;
    let roundtrip = narrowed.cast_to(target).ok()?;
    let exact = match (literal, roundtrip) {
        (ScalarValue::Float32(Some(a)), ScalarValue::Float32(Some(b))) => {
            a.to_bits() == b.to_bits()
        }
        (ScalarValue::Float64(Some(a)), ScalarValue::Float64(Some(b))) => {
            a.to_bits() == b.to_bits()
        }
        _ => false,
    };
    exact.then(|| (value.as_ref().clone(), lit(narrowed)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{ArrayRef, Float32Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::create_physical_expr;
    use datafusion_expr::execution_props::ExecutionProps;
    use datafusion_expr::physical_planning_context::PhysicalPlanningContext;
    use datafusion_expr::{cast, col, try_cast};

    use super::*;

    fn schema(data_type: DataType) -> Result<DFSchema> {
        DFSchema::try_from(Schema::new(vec![Field::new("x", data_type, true)]))
    }

    #[test]
    fn exact_float_widening_comparisons_preserve_every_operator_and_nulls() -> Result<()> {
        let schema = schema(DataType::Float32)?;
        let raw: ArrayRef = Arc::new(Float32Array::from(vec![
            None,
            Some(f32::NEG_INFINITY),
            Some(f32::from_bits(0xff80_0001)),
            Some(-f32::MAX),
            Some(-1.5),
            Some(-0.0),
            Some(0.0),
            Some(f32::from_bits(1)),
            Some(1.5),
            Some(f32::MAX),
            Some(f32::INFINITY),
            Some(f32::from_bits(0x7f80_0001)),
            Some(f32::from_bits(0x7fc0_0001)),
            Some(f32::from_bits(0x7fc0_0002)),
        ]));
        let batch = RecordBatch::try_new(Arc::new(schema.as_arrow().clone()), vec![raw])?;
        for literal in [
            0.0_f64,
            -0.0,
            1.5,
            -1.5,
            f64::from(f32::from_bits(1)),
            f64::from(f32::MAX),
        ] {
            for op in [
                Operator::Eq,
                Operator::NotEq,
                Operator::Lt,
                Operator::LtEq,
                Operator::Gt,
                Operator::GtEq,
                Operator::IsDistinctFrom,
                Operator::IsNotDistinctFrom,
            ] {
                for reversed in [false, true] {
                    for value in [
                        cast(col("x"), DataType::Float64),
                        try_cast(col("x"), DataType::Float64),
                    ] {
                        let (left, right) = if reversed {
                            (lit(literal), value)
                        } else {
                            (value, lit(literal))
                        };
                        let original =
                            Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)));
                        let optimized = unwrap_comparison(original.clone(), &schema)?;
                        assert!(optimized.transformed, "{original:?}");
                        let evaluate = |expr: &Expr| -> Result<ArrayRef> {
                            create_physical_expr(
                                expr,
                                &schema,
                                &ExecutionProps::new(),
                                &PhysicalPlanningContext::default(),
                            )?
                            .evaluate(&batch)?
                            .into_array(batch.num_rows())
                        };
                        let original_values = evaluate(&original)?;
                        let optimized_values = evaluate(&optimized.data)?;
                        assert_eq!(
                            original_values.as_ref(),
                            optimized_values.as_ref(),
                            "{original:?}"
                        );
                    }
                }
            }
        }
        Ok(())
    }

    /// The DECIMAL comparisons give the same result as casting every DECIMAL value to DOUBLE,
    /// for random and boundary values and constants, including values next to the thresholds.
    #[test]
    fn decimal_double_comparisons_match_the_cast_of_every_value() -> Result<()> {
        use datafusion::arrow::array::{Decimal32Array, Decimal64Array, Decimal128Array};

        // A deterministic pseudo-random sequence.
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let types = [
            DataType::Decimal32(9, 2),
            DataType::Decimal64(18, 4),
            DataType::Decimal128(10, 2),
            DataType::Decimal128(20, 10),
            DataType::Decimal128(31, 15),
            DataType::Decimal128(38, 0),
            DataType::Decimal128(38, 18),
            DataType::Decimal128(38, 38),
            DataType::Decimal128(5, -2),
        ];
        let mut comparisons = 0_usize;
        for data_type in types {
            let (precision, scale) = match data_type {
                DataType::Decimal32(p, s)
                | DataType::Decimal64(p, s)
                | DataType::Decimal128(p, s) => (p, s),
                _ => unreachable!(),
            };
            let max = 10_i128.pow(u32::from(precision)) - 1;
            let to_double = |raw: i128| {
                single_decimal_to_float_lossy::<Decimal128Type, _>(
                    &|x: i128| x as f64,
                    raw,
                    i32::from(scale),
                )
            };
            let random = |next: &mut dyn FnMut() -> u64| -> i128 {
                let wide = (i128::from(next()) << 64) | i128::from(next());
                let digits = (next() % u64::from(precision)) as u32 + 1;
                (wide % 10_i128.pow(digits)).clamp(-max, max)
            };
            let mut constants = vec![
                0.0_f64,
                -0.0,
                f64::NAN,
                -f64::NAN,
                f64::INFINITY,
                f64::NEG_INFINITY,
                f64::MAX,
                f64::MIN,
                f64::MIN_POSITIVE,
                -f64::MIN_POSITIVE,
                0.1,
                1.5,
                -2.25,
                1000.505,
                1e30,
                -1e30,
                to_double(max),
                to_double(-max),
                to_double(max) * 2.0,
            ];
            let mut raws = vec![0, 1, -1, max, -max, max - 1, -max + 1];
            for _ in 0..40 {
                let raw = random(&mut next);
                let double = to_double(raw);
                constants.push(double);
                constants.push(double.next_up());
                constants.push(double.next_down());
                for delta in -3..=3 {
                    raws.push((raw + delta).clamp(-max, max));
                }
            }
            for _ in 0..1000 {
                raws.push(random(&mut next));
            }
            let values = raws
                .iter()
                .copied()
                .map(Some)
                .chain([None])
                .collect::<Vec<_>>();
            let array: ArrayRef = match data_type {
                DataType::Decimal32(..) => Arc::new(
                    Decimal32Array::from(
                        values
                            .iter()
                            .map(|v| v.map(|v| v as i32))
                            .collect::<Vec<_>>(),
                    )
                    .with_data_type(data_type.clone()),
                ),
                DataType::Decimal64(..) => Arc::new(
                    Decimal64Array::from(
                        values
                            .iter()
                            .map(|v| v.map(|v| v as i64))
                            .collect::<Vec<_>>(),
                    )
                    .with_data_type(data_type.clone()),
                ),
                _ => Arc::new(Decimal128Array::from(values).with_data_type(data_type.clone())),
            };
            let schema = schema(data_type.clone())?;
            let batch = RecordBatch::try_new(Arc::new(schema.as_arrow().clone()), vec![array])?;
            let evaluate = |expr: &Expr| -> Result<ArrayRef> {
                create_physical_expr(
                    expr,
                    &schema,
                    &ExecutionProps::new(),
                    &PhysicalPlanningContext::default(),
                )?
                .evaluate(&batch)?
                .into_array(batch.num_rows())
            };
            for constant in constants {
                for op in [
                    Operator::Eq,
                    Operator::NotEq,
                    Operator::Lt,
                    Operator::LtEq,
                    Operator::Gt,
                    Operator::GtEq,
                ] {
                    for reversed in [false, true] {
                        let value = cast(col("x"), DataType::Float64);
                        let (left, right) = if reversed {
                            (lit(constant), value)
                        } else {
                            (value, lit(constant))
                        };
                        let original =
                            Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)));
                        let optimized = unwrap_comparison(original.clone(), &schema)?;
                        assert!(optimized.transformed, "{original:?}");
                        assert!(
                            !format!("{:?}", optimized.data).contains("Cast"),
                            "{:?}",
                            optimized.data
                        );
                        assert_eq!(
                            evaluate(&original)?.as_ref(),
                            evaluate(&optimized.data)?.as_ref(),
                            "{data_type} {original:?} {:?}",
                            optimized.data
                        );
                        comparisons += batch.num_rows();
                    }
                }
            }
        }
        assert!(comparisons > 1_000_000, "{comparisons}");
        Ok(())
    }

    #[test]
    fn decimal_double_comparisons_evaluate_volatile_values_once() -> Result<()> {
        use datafusion_expr::ScalarUDF;
        use sail_function::scalar::misc::spark_partition_id::SparkPartitionId;

        let schema = schema(DataType::Decimal128(38, 20))?;
        // Many DECIMAL(38,20) values cast to 0.5, so `=` and `<>` need two comparisons.
        let volatile = ScalarUDF::from(SparkPartitionId::new()).call(vec![]);
        let volatile = cast(
            cast(volatile, DataType::Decimal128(38, 20)),
            DataType::Float64,
        );
        let stable = cast(col("x"), DataType::Float64);
        for op in [Operator::Eq, Operator::NotEq] {
            let comparison = |value: &Expr| {
                Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(value.clone()),
                    op,
                    Box::new(lit(0.5_f64)),
                ))
            };
            assert!(!unwrap_comparison(comparison(&volatile), &schema)?.transformed);
            let optimized = unwrap_comparison(comparison(&stable), &schema)?;
            assert!(matches!(
                optimized.data,
                Expr::BinaryExpr(BinaryExpr {
                    op: Operator::And | Operator::Or,
                    ..
                })
            ));
        }
        for op in [Operator::Lt, Operator::GtEq] {
            let expr = Expr::BinaryExpr(BinaryExpr::new(
                Box::new(volatile.clone()),
                op,
                Box::new(lit(0.5_f64)),
            ));
            assert!(unwrap_comparison(expr, &schema)?.transformed);
        }
        Ok(())
    }

    #[test]
    fn decimal_double_comparisons_keep_other_casts() -> Result<()> {
        let schema = schema(DataType::Decimal128(10, 2))?;
        for expr in [
            cast(col("x"), DataType::Float32).lt(lit(1.5_f32)),
            try_cast(col("x"), DataType::Float64).lt(lit(1.5_f64)),
            cast(col("x"), DataType::Float64).lt(lit(ScalarValue::Float64(None))),
            Expr::BinaryExpr(BinaryExpr::new(
                Box::new(cast(col("x"), DataType::Float64)),
                Operator::IsDistinctFrom,
                Box::new(lit(1.5_f64)),
            )),
        ] {
            assert!(!unwrap_comparison(expr, &schema)?.transformed);
        }
        let wide = DFSchema::try_from(Schema::new(vec![Field::new(
            "x",
            DataType::Decimal256(40, 2),
            true,
        )]))?;
        let expr = cast(col("x"), DataType::Float64).lt(lit(1.5_f64));
        assert!(!unwrap_comparison(expr, &wide)?.transformed);
        Ok(())
    }

    #[test]
    fn exact_integral_widening_literals_and_ineligible_casts() -> Result<()> {
        for (from, to, good, bad) in [
            (
                DataType::Int8,
                DataType::Float32,
                lit(127.0_f32),
                lit(128.0_f32),
            ),
            (
                DataType::Int16,
                DataType::Float32,
                lit(32767.0_f32),
                lit(32768.0_f32),
            ),
            (
                DataType::Int8,
                DataType::Float64,
                lit(-128.0_f64),
                lit(-129.0_f64),
            ),
            (
                DataType::Int16,
                DataType::Float64,
                lit(-32768.0_f64),
                lit(-32769.0_f64),
            ),
            (
                DataType::Int32,
                DataType::Float64,
                lit(f64::from(i32::MAX)),
                lit(f64::from(i32::MAX) + 1.0),
            ),
        ] {
            let schema = schema(from)?;
            let value = cast(col("x"), to);
            assert!(unwrap_pair(&value, &good, &schema).is_some());
            assert!(unwrap_pair(&value, &bad, &schema).is_none());
            let fractional = match good {
                Expr::Literal(ScalarValue::Float32(_), _) => lit(1.5_f32),
                _ => lit(1.5_f64),
            };
            assert!(unwrap_pair(&value, &fractional, &schema).is_none());
        }
        for (from, to) in [
            (DataType::Int32, DataType::Float32),
            (DataType::Int64, DataType::Float32),
            (DataType::Int64, DataType::Float64),
            (DataType::Float64, DataType::Float32),
        ] {
            let schema = schema(from)?;
            let literal = if to == DataType::Float32 {
                lit(1.0_f32)
            } else {
                lit(1.0_f64)
            };
            assert!(unwrap_pair(&cast(col("x"), to), &literal, &schema).is_none());
        }
        let schema = schema(DataType::Float32)?;
        for literal in [
            1.1_f64,
            f64::from(f32::MAX) * 2.0,
            f64::from(f32::from_bits(1)) / 2.0,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ] {
            assert!(
                unwrap_pair(&cast(col("x"), DataType::Float64), &lit(literal), &schema).is_none()
            );
        }
        assert!(
            unwrap_pair(
                &cast(col("x"), DataType::Float64),
                &lit(ScalarValue::Float64(None)),
                &schema,
            )
            .is_none()
        );
        Ok(())
    }
    #[test]
    fn nonnullable_try_cast_comparisons_keep_their_physical_schema() -> Result<()> {
        let schema =
            DFSchema::try_from(Schema::new(vec![Field::new("x", DataType::Float32, false)]))?;
        let original = try_cast(col("x"), DataType::Float64).gt(lit(1.5_f64));
        let optimized = unwrap_comparison(original.clone(), &schema)?;
        assert!(!optimized.transformed);
        let physical = |expr: &Expr| {
            create_physical_expr(
                expr,
                &schema,
                &ExecutionProps::new(),
                &PhysicalPlanningContext::default(),
            )
        };
        assert!(physical(&original)?.nullable(schema.as_arrow())?);
        assert!(physical(&optimized.data)?.nullable(schema.as_arrow())?);
        let ordinary = cast(col("x"), DataType::Float64).gt(lit(1.5_f64));
        let ordinary = unwrap_comparison(ordinary, &schema)?;
        assert!(ordinary.transformed);
        assert!(!physical(&ordinary.data)?.nullable(schema.as_arrow())?);
        Ok(())
    }
}
