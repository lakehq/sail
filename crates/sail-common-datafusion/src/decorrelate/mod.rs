// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// [CREDIT]: https://github.com/apache/datafusion/blob/1635ed94742c1c060852bc34dc6ebb2302241c7b/datafusion/optimizer/src/lib.rs

//! Decorrelation rules using Sail's expression simplifier for empty-input compensation.

#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "Retain the upstream implementation and tests"
)]

mod decorrelate_lateral_join;
mod decorrelate_predicate_subquery;
#[path = "decorrelate.rs"]
mod rewrite;
mod scalar_subquery_to_join;
mod utils;

pub use decorrelate_lateral_join::DecorrelateLateralJoin;
pub use decorrelate_predicate_subquery::DecorrelatePredicateSubquery;
pub use rewrite::*;
pub use scalar_subquery_to_join::ScalarSubqueryToJoin;

#[cfg(test)]
mod test {
    // Licensed under the Apache License, Version 2.0 (the "License");
    // you may not use this file except in compliance with the License.
    // You may obtain a copy of the License at
    //
    //     http://www.apache.org/licenses/LICENSE-2.0
    //
    // Unless required by applicable law or agreed to in writing, software
    // distributed under the License is distributed on an "AS IS" BASIS,
    // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    // See the License for the specific language governing permissions and
    // limitations under the License.

    // [CREDIT]: https://github.com/apache/datafusion/blob/1635ed94742c1c060852bc34dc6ebb2302241c7b/datafusion/optimizer/src/test/mod.rs

    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::Result;
    use datafusion_expr::LogicalPlan;
    use datafusion_expr::logical_plan::table_scan;

    pub(super) fn test_table_scan_with_name(name: &str) -> Result<LogicalPlan> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::UInt32, false),
            Field::new("b", DataType::UInt32, false),
            Field::new("c", DataType::UInt32, false),
        ]);
        table_scan(Some(name), &schema, None)?.build()
    }

    pub(super) fn test_table_scan() -> Result<LogicalPlan> {
        test_table_scan_with_name("test")
    }

    pub(super) fn scan_tpch_table(table: &str) -> LogicalPlan {
        let schema = Arc::new(get_tpch_table_schema(table));
        table_scan(Some(table), &schema, None)
            .unwrap()
            .build()
            .unwrap()
    }

    fn get_tpch_table_schema(table: &str) -> Schema {
        match table {
            "customer" => Schema::new(vec![
                Field::new("c_custkey", DataType::Int64, false),
                Field::new("c_name", DataType::Utf8, false),
            ]),
            "orders" => Schema::new(vec![
                Field::new("o_orderkey", DataType::Int64, false),
                Field::new("o_custkey", DataType::Int64, false),
                Field::new("o_orderstatus", DataType::Utf8, false),
                Field::new("o_totalprice", DataType::Float64, true),
            ]),
            "lineitem" => Schema::new(vec![
                Field::new("l_orderkey", DataType::Int64, false),
                Field::new("l_partkey", DataType::Int64, false),
                Field::new("l_suppkey", DataType::Int64, false),
                Field::new("l_linenumber", DataType::Int32, false),
                Field::new("l_quantity", DataType::Float64, false),
                Field::new("l_extendedprice", DataType::Float64, false),
            ]),
            _ => unimplemented!("Table: {}", table),
        }
    }

    macro_rules! assert_optimized_plan_eq_display_indent_snapshot {
        ($rule:expr, $plan:expr, @ $expected:literal $(,)?) => {{
            let optimizer = datafusion::optimizer::Optimizer::with_rules(vec![$rule]);
            let optimized_plan = optimizer
                .optimize($plan, &datafusion::optimizer::OptimizerContext::new(), |_, _| {})
                .expect("failed to optimize plan");
            let formatted_plan = optimized_plan.display_indent_schema();
            insta::assert_snapshot!(formatted_plan, @ $expected);
            Ok::<(), datafusion_common::DataFusionError>(())
        }};
    }

    pub(super) use assert_optimized_plan_eq_display_indent_snapshot;

    pub(super) fn assert_analyzer_check_err(
        rules: Vec<Arc<dyn datafusion::optimizer::AnalyzerRule + Send + Sync>>,
        plan: LogicalPlan,
        expected: &str,
    ) {
        let options = datafusion_common::config::ConfigOptions::default();
        let analyzed_plan = datafusion::optimizer::Analyzer::with_rules(rules).execute_and_check(
            plan,
            &options,
            |_, _| {},
        );
        match analyzed_plan {
            Ok(plan) => assert_eq!(format!("{}", plan.display_indent()), "An error"),
            Err(e) => {
                datafusion_common::assert_contains!(e.to_string(), expected);
            }
        }
    }
}
