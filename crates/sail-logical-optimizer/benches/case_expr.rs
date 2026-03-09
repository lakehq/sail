#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::sync::Arc;

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{col, BinaryExpr, Case, Expr, Operator};
use datafusion::physical_expr::expressions::{
    BinaryExpr as PhysBinaryExpr, CaseExpr, Column, Literal,
};
use datafusion::physical_expr::PhysicalExpr;
use rand::RngExt;
use sail_logical_optimizer::case_expr::try_reconstruct_simple_case;

/// Build a flattened logical CASE expression (what Sail's SQL analyzer produces):
/// `CASE WHEN col = 'val0' THEN 0 WHEN col = 'val1' THEN 1 ... ELSE -1 END`
fn build_flattened_logical_case(num_branches: usize) -> Case {
    Case {
        expr: None,
        when_then_expr: (0..num_branches)
            .map(|i| {
                (
                    Box::new(Expr::BinaryExpr(BinaryExpr {
                        left: Box::new(col("status")),
                        op: Operator::Eq,
                        right: Box::new(Expr::Literal(
                            ScalarValue::Utf8(Some(format!("val{i}"))),
                            None,
                        )),
                    })),
                    Box::new(Expr::Literal(ScalarValue::Int32(Some(i as i32)), None)),
                )
            })
            .collect(),
        else_expr: Some(Box::new(Expr::Literal(ScalarValue::Int32(Some(-1)), None))),
    }
}

/// Convert a logical Case to a physical CaseExpr.
/// Handles both flattened (expr: None) and simple (expr: Some) forms.
fn logical_to_physical(case: &Case) -> CaseExpr {
    let expr: Option<Arc<dyn PhysicalExpr>> = case.expr.as_ref().map(|e| match e.as_ref() {
        Expr::Column(c) => Arc::new(Column::new(&c.name, 0)) as Arc<dyn PhysicalExpr>,
        _ => unreachable!("expected column in case expr"),
    });

    let when_then: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> = case
        .when_then_expr
        .iter()
        .map(|(when, then)| {
            let when_phys: Arc<dyn PhysicalExpr> = match when.as_ref() {
                // Simple form: WHEN is a literal
                Expr::Literal(sv, _) => Arc::new(Literal::new(sv.clone())),
                // Flattened form: WHEN is col = literal
                Expr::BinaryExpr(BinaryExpr { left, op: _, right }) => {
                    let left_phys: Arc<dyn PhysicalExpr> = match left.as_ref() {
                        Expr::Column(c) => Arc::new(Column::new(&c.name, 0)),
                        Expr::Literal(sv, _) => Arc::new(Literal::new(sv.clone())),
                        _ => unreachable!("unexpected left expr in WHEN"),
                    };
                    let right_phys: Arc<dyn PhysicalExpr> = match right.as_ref() {
                        Expr::Column(c) => Arc::new(Column::new(&c.name, 0)),
                        Expr::Literal(sv, _) => Arc::new(Literal::new(sv.clone())),
                        _ => unreachable!("unexpected right expr in WHEN"),
                    };
                    Arc::new(PhysBinaryExpr::new(left_phys, Operator::Eq, right_phys))
                }
                _ => unreachable!("unexpected when expr"),
            };
            let then_phys: Arc<dyn PhysicalExpr> = match then.as_ref() {
                Expr::Literal(sv, _) => Arc::new(Literal::new(sv.clone())),
                _ => unreachable!("unexpected then expr"),
            };
            (when_phys, then_phys)
        })
        .collect();

    let else_phys: Option<Arc<dyn PhysicalExpr>> =
        case.else_expr.as_ref().map(|e| match e.as_ref() {
            Expr::Literal(sv, _) => Arc::new(Literal::new(sv.clone())) as Arc<dyn PhysicalExpr>,
            _ => unreachable!("unexpected else expr"),
        });

    CaseExpr::try_new(expr, when_then, else_phys).expect("failed to create CaseExpr")
}

/// Generate a RecordBatch with random status values
fn generate_batch(num_rows: usize, num_distinct: usize) -> RecordBatch {
    let mut rng = rand::rng();
    let values: Vec<String> = (0..num_rows)
        .map(|_| format!("val{}", rng.random_range(0..num_distinct)))
        .collect();

    let array = StringArray::from(values);
    let schema = Schema::new(vec![Field::new("status", DataType::Utf8, false)]);
    RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).expect("failed to create batch")
}

fn bench_case_expr(c: &mut Criterion) {
    let num_rows = 100_000;

    for num_branches in [10, 50, 200, 500] {
        let batch = generate_batch(num_rows, num_branches);

        // Build flattened logical CASE (what Sail produces)
        let flattened_logical = build_flattened_logical_case(num_branches);

        // Run our optimizer rule to get the simple form
        let optimized = try_reconstruct_simple_case(flattened_logical.clone());
        assert!(
            optimized.transformed,
            "ReconstructSimpleCaseExpr must transform the flattened CASE"
        );
        let Expr::Case(simple_logical) = optimized.data else {
            unreachable!("expected Case after optimization");
        };

        // Convert both to physical expressions
        let flattened_physical = logical_to_physical(&flattened_logical);
        let simple_physical = logical_to_physical(&simple_logical);

        let mut group = c.benchmark_group(format!("case_{num_branches}_branches"));

        group.bench_function(BenchmarkId::new("flattened_linear", num_rows), |b| {
            b.iter(|| {
                flattened_physical
                    .evaluate(&batch)
                    .expect("evaluation failed");
            });
        });

        group.bench_function(BenchmarkId::new("optimized_hash", num_rows), |b| {
            b.iter(|| {
                simple_physical.evaluate(&batch).expect("evaluation failed");
            });
        });

        group.finish();
    }
}

criterion_group!(benches, bench_case_expr);
criterion_main!(benches);
