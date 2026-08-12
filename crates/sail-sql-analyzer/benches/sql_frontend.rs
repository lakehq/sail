#![expect(
    clippy::expect_used,
    reason = "benchmark setup and operations must fail immediately on an invalid result"
)]

use std::hint::black_box;
use std::time::Instant;

use sail_sql_analyzer::parser::{
    parse_data_type, parse_expression, parse_object_name, parse_one_statement, parse_statements,
};
use sail_sql_analyzer::statement::from_ast_statement;

const SAMPLES: usize = 11;
const SIMPLE_SQL: &str = "SELECT 1 AS value";
const COMPLEX_SQL: &str = "WITH sales AS (\
    SELECT customer_id, amount, event_time FROM catalog.analytics.sales \
    WHERE amount > 10 AND category IN ('books', 'games', 'tools')\
) \
SELECT customer_id, SUM(amount) AS total, \
       ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY event_time DESC) AS rank \
FROM sales \
GROUP BY customer_id, event_time \
HAVING SUM(amount) > 100 \
ORDER BY total DESC LIMIT 100";
const COMPLEX_EXPRESSION: &str = "CASE \
    WHEN amount > 100 AND category IN ('books', 'games', 'tools') \
    THEN COALESCE(discounted_amount, amount * 0.9) \
    ELSE CAST(amount AS DECIMAL(20, 4)) END";
const COMPLEX_DATA_TYPE: &str = "STRUCT<id: BIGINT, events: ARRAY<MAP<STRING, DECIMAL(20, 4)>>>";

fn selected(name: &str) -> bool {
    std::env::var("SAIL_BENCH_FILTER")
        .map(|filter| name.contains(&filter))
        .unwrap_or(true)
}

fn measure(name: &str, iterations: usize, mut run: impl FnMut()) {
    if !selected(name) {
        return;
    }
    for _ in 0..(iterations / 10).max(1) {
        run();
    }

    let mut nanos = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let started = Instant::now();
        for _ in 0..iterations {
            run();
        }
        nanos.push(started.elapsed().as_nanos() as f64 / iterations as f64);
    }
    nanos.sort_by(f64::total_cmp);
    println!(
        "{name}\titerations={iterations}\tmin_ns={:.1}\tmedian_ns={:.1}\tmax_ns={:.1}",
        nanos[0],
        nanos[SAMPLES / 2],
        nanos[SAMPLES - 1]
    );
}

fn main() {
    let batch_sql = (0..64)
        .map(|index| format!("SELECT {index} AS value"))
        .collect::<Vec<_>>()
        .join(";");
    let simple_ast = parse_one_statement(SIMPLE_SQL).expect("parse simple SQL");
    let complex_ast = parse_one_statement(COMPLEX_SQL).expect("parse complex SQL");
    from_ast_statement(simple_ast.clone()).expect("analyze simple SQL");
    from_ast_statement(complex_ast.clone()).expect("analyze complex SQL");

    measure("parse_object_name", 20_000, || {
        black_box(parse_object_name(black_box("catalog.analytics.events")).expect("object name"));
    });
    measure("parse_data_type_complex", 2_000, || {
        black_box(parse_data_type(black_box(COMPLEX_DATA_TYPE)).expect("data type"));
    });
    measure("parse_expression_complex", 1_000, || {
        black_box(parse_expression(black_box(COMPLEX_EXPRESSION)).expect("expression"));
    });
    measure("parse_statement_simple", 2_000, || {
        black_box(parse_one_statement(black_box(SIMPLE_SQL)).expect("simple SQL"));
    });
    measure("parse_statement_complex", 200, || {
        black_box(parse_one_statement(black_box(COMPLEX_SQL)).expect("complex SQL"));
    });
    measure("parse_statements_batch_64", 30, || {
        black_box(parse_statements(black_box(&batch_sql)).expect("SQL batch"));
    });
    measure("analyze_cloned_ast_simple", 5_000, || {
        black_box(from_ast_statement(black_box(simple_ast.clone())).expect("simple analysis"));
    });
    measure("analyze_cloned_ast_complex", 500, || {
        black_box(from_ast_statement(black_box(complex_ast.clone())).expect("complex analysis"));
    });
    measure("frontend_simple", 1_000, || {
        let ast = parse_one_statement(black_box(SIMPLE_SQL)).expect("simple SQL");
        black_box(from_ast_statement(ast).expect("simple analysis"));
    });
    measure("frontend_complex", 100, || {
        let ast = parse_one_statement(black_box(COMPLEX_SQL)).expect("complex SQL");
        black_box(from_ast_statement(ast).expect("complex analysis"));
    });
}
