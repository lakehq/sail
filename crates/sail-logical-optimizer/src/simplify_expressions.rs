use datafusion::arrow::datatypes::DataType;
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchema, Result, ScalarValue};
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::utils::merge_schema;
use datafusion_expr::{BinaryExpr, Expr, ExprSchemable, LogicalPlan, Operator, ScalarUDF, lit};
use sail_common_datafusion::simplify_expressions::SimplifyExpressions as BaseSimplifyExpressions;
use sail_function::scalar::normalize_float_keys::NormalizeFloatKeys;

/// Extend simplification with exact numeric widening and signed-zero membership keys.
#[derive(Debug, Default)]
pub struct SimplifyExpressions(BaseSimplifyExpressions);

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
                    if matches!(expr, Expr::InList(_)) {
                        return Ok(true);
                    }
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
                expr.transform_up(|expr| {
                    unwrap_comparison(expr, &schema)?
                        .transform_data(|expr| normalize_float_in_list(expr, &schema))
                })
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

fn normalize_float_in_list(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let Expr::InList(mut in_list) = expr else {
        return Ok(Transformed::no(expr));
    };
    let data_type = in_list.expr.get_type(schema)?;
    let is_zero = |expr: &Expr| {
        matches!(expr, Expr::Literal(ScalarValue::Float32(Some(x)), _) if *x == 0.0)
            || matches!(expr, Expr::Literal(ScalarValue::Float64(Some(x)), _) if *x == 0.0)
    };
    if !matches!(data_type, DataType::Float32 | DataType::Float64)
        || !in_list.list.iter().all(|expr| {
            matches!(expr, Expr::Literal(value, _) if value.data_type() == data_type || value.data_type().is_null())
        })
        || !in_list.list.iter().any(is_zero)
    {
        return Ok(Transformed::no(Expr::InList(in_list)));
    }

    // Static membership compares float bits in both branchless and hash paths.
    // Normalize only private keys, retaining NaN payloads, NULLs and one evaluation
    // of the tested expression (including volatile expressions).
    let mut changed = false;
    if !matches!(in_list.expr.as_ref(), Expr::ScalarFunction(function) if function.func.inner().is::<NormalizeFloatKeys>())
    {
        in_list.expr =
            Box::new(ScalarUDF::from(NormalizeFloatKeys::new()).call(vec![*in_list.expr]));
        changed = true;
    }
    for value in &mut in_list.list {
        match value {
            Expr::Literal(ScalarValue::Float32(Some(x)), _)
                if *x == 0.0 && x.is_sign_negative() =>
            {
                *x = 0.0;
                changed = true;
            }
            Expr::Literal(ScalarValue::Float64(Some(x)), _)
                if *x == 0.0 && x.is_sign_negative() =>
            {
                *x = 0.0;
                changed = true;
            }
            _ => {}
        }
    }
    Ok(Transformed::new(
        Expr::InList(in_list),
        changed,
        TreeNodeRecursion::Continue,
    ))
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
        None => Ok(Transformed::no(expr)),
    }
}

fn unwrap_pair(cast: &Expr, literal: &Expr, schema: &DFSchema) -> Option<(Expr, Expr)> {
    use datafusion::arrow::datatypes::DataType;

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

    #[test]
    fn default_rules_preserve_backslash_regex_and_safe_like_rewrites() -> Result<()> {
        use datafusion::functions::expr_fn::concat;
        use datafusion::optimizer::{Optimizer, OptimizerContext};
        use datafusion_expr::logical_plan::table_scan;
        use datafusion_expr::{LogicalPlanBuilder, binary_expr};

        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let scan = table_scan(Some("t"), &schema, None)?.build()?;
        let value = |prefix| concat(vec![lit(prefix), cast(col("id"), DataType::Utf8)]);
        let plan = LogicalPlanBuilder::from(scan)
            .project(vec![
                binary_expr(value(r"a\b"), Operator::RegexMatch, lit(r"a\\b")).alias("positive"),
                binary_expr(value("ab"), Operator::RegexMatch, lit(r"a\\b")).alias("negative"),
                binary_expr(value("abc"), Operator::RegexMatch, lit("bc")).alias("safe"),
            ])?
            .build()?;
        let optimized = Optimizer::with_rules(crate::default_optimizer_rules()).optimize(
            plan,
            &OptimizerContext::new().with_skip_failing_rules(false),
            |_, _| {},
        )?;
        println!(
            "Backslash regex optimized plan:\n{}",
            optimized.display_indent()
        );
        let LogicalPlan::Projection(projection) = optimized else {
            return datafusion_common::internal_err!("expected projection");
        };
        for expr in &projection.expr[..2] {
            assert!(
                matches!(expr.clone().unalias(), Expr::BinaryExpr(b) if b.op == Operator::RegexMatch)
            );
        }
        assert!(matches!(
            projection.expr[2].clone().unalias(),
            Expr::Like(_)
        ));
        assert_eq!(
            projection.schema.field_names(),
            vec!["positive", "negative", "safe"]
        );
        Ok(())
    }

    fn schema(data_type: DataType) -> Result<DFSchema> {
        DFSchema::try_from(Schema::new(vec![Field::new("x", data_type, true)]))
    }

    #[test]
    fn float_membership_keys_stay_bounded_and_idempotent() -> Result<()> {
        use datafusion::optimizer::{Optimizer, OptimizerContext};
        use datafusion_expr::LogicalPlanBuilder;
        use datafusion_expr::logical_plan::table_scan;

        for data_type in [DataType::Float32, DataType::Float64] {
            let schema = schema(data_type.clone())?;
            for length in [4, 15, 16, 17, 31, 32, 33, 1024] {
                for negated in [false, true] {
                    let list = (0..length)
                        .map(|i| {
                            ScalarValue::Float64(Some(if i == 0 { -0.0 } else { f64::from(i) }))
                                .cast_to(&data_type)
                                .map(lit)
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let scan = table_scan(Some("t"), schema.as_arrow(), None)?.build()?;
                    let plan = LogicalPlanBuilder::from(scan)
                        .project(vec![
                            col("x"),
                            col("x").in_list(list, negated).alias("matched"),
                        ])?
                        .build()?;
                    let optimizer = Optimizer::with_rules(crate::default_optimizer_rules());
                    let config = OptimizerContext::new().with_skip_failing_rules(false);
                    let optimized = optimizer.optimize(plan, &config, |_, _| {})?;
                    assert_eq!(
                        optimizer.optimize(optimized.clone(), &config, |_, _| {})?,
                        optimized
                    );
                    let LogicalPlan::Projection(projection) = optimized else {
                        return datafusion_common::internal_err!("expected projection");
                    };
                    assert!(matches!(projection.expr[0], Expr::Column(_)));
                    let Expr::InList(in_list) = projection.expr[1].clone().unalias() else {
                        return datafusion_common::internal_err!("expected bounded membership");
                    };
                    assert_eq!(in_list.list.len(), length as usize);
                    assert_eq!(in_list.negated, negated);
                    let Expr::ScalarFunction(function) = in_list.expr.as_ref() else {
                        return datafusion_common::internal_err!("expected private key");
                    };
                    assert!(function.func.inner().is::<NormalizeFloatKeys>());
                    assert!(matches!(function.args.as_slice(), [Expr::Column(_)]));
                    assert!(
                        in_list
                            .list
                            .iter()
                            .all(|value| matches!(value, Expr::Literal(_, _)))
                    );
                }
            }
        }
        Ok(())
    }

    #[test]
    fn float_membership_preserves_nan_payloads_and_literal_metadata() -> Result<()> {
        use datafusion::arrow::array::{AsArray, Float64Array};
        use datafusion_common::metadata::FieldMetadata;

        let schema = schema(DataType::Float64)?;
        let bits = [0x7ff0_0000_0000_0001, 0xfff8_0000_0000_0042];
        let metadata = Some(FieldMetadata::default());
        let list = vec![
            Expr::Literal(ScalarValue::Float64(Some(-0.0)), metadata.clone()),
            lit(f64::from_bits(bits[0])),
            lit(ScalarValue::Float64(None)),
            lit(5.0_f64),
        ];
        let original = col("x").in_list(list.clone(), false);
        let normalized = normalize_float_in_list(original, &schema)?;
        assert!(normalized.transformed);
        assert!(!normalize_float_in_list(normalized.data.clone(), &schema)?.transformed);
        let Expr::InList(in_list) = &normalized.data else {
            return datafusion_common::internal_err!("expected membership");
        };
        assert_eq!(
            in_list.list[0],
            Expr::Literal(ScalarValue::Float64(Some(0.0)), metadata)
        );
        assert_eq!(&in_list.list[1..], &list[1..]);
        let raw: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(-0.0),
            Some(0.0),
            Some(f64::from_bits(bits[0])),
            Some(f64::from_bits(bits[1])),
            None,
        ]));
        let batch =
            RecordBatch::try_new(Arc::new(schema.as_arrow().clone()), vec![Arc::clone(&raw)])?;
        let result = create_physical_expr(
            &normalized.data,
            &schema,
            &ExecutionProps::new(),
            &PhysicalPlanningContext::default(),
        )?
        .evaluate(&batch)?
        .into_array(batch.num_rows())?;
        assert_eq!(
            result.as_boolean().iter().collect::<Vec<_>>(),
            vec![Some(true), Some(true), Some(true), None, None]
        );
        let raw = raw.as_primitive::<arrow::datatypes::Float64Type>();
        assert_eq!(raw.value(0).to_bits(), (-0.0_f64).to_bits());
        assert_eq!(raw.value(2).to_bits(), bits[0]);
        assert_eq!(raw.value(3).to_bits(), bits[1]);
        Ok(())
    }

    #[test]
    fn float_membership_evaluates_volatile_operands_once_and_leaves_dynamic_lists() -> Result<()> {
        use std::sync::atomic::{AtomicUsize, Ordering};

        use datafusion::arrow::array::Float64Array;
        use datafusion_expr::{Volatility, create_udf};

        let calls = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&calls);
        let function = create_udf(
            "counted",
            vec![DataType::Float64],
            DataType::Float64,
            Volatility::Volatile,
            Arc::new(move |args| {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(args[0].clone())
            }),
        );
        let schema = schema(DataType::Float64)?;
        let batch = RecordBatch::try_new(
            Arc::new(schema.as_arrow().clone()),
            vec![Arc::new(Float64Array::from(vec![100.0, 101.0]))],
        )?;
        for length in [2, 4, 33] {
            for dynamic in [false, true] {
                let mut list = (0..length).map(|i| lit(f64::from(i))).collect::<Vec<_>>();
                if dynamic {
                    list[1] = function.call(vec![lit(1.0_f64)]);
                }
                let original = function.call(vec![col("x")]).in_list(list, false);
                let normalized = normalize_float_in_list(original.clone(), &schema)?;
                assert_eq!(normalized.transformed, !dynamic);
                if dynamic {
                    assert_eq!(normalized.data, original);
                }
                calls.store(0, Ordering::SeqCst);
                create_physical_expr(
                    &normalized.data,
                    &schema,
                    &ExecutionProps::new(),
                    &PhysicalPlanningContext::default(),
                )?
                .evaluate(&batch)?;
                assert_eq!(calls.load(Ordering::SeqCst), if dynamic { 2 } else { 1 });
            }
        }
        Ok(())
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
