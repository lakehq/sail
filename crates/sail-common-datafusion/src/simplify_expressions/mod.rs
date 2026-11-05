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

// [CREDIT]: https://github.com/apache/datafusion/blob/1635ed94742c1c060852bc34dc6ebb2302241c7b/datafusion/optimizer/src/simplify_expressions/mod.rs

//! [`SimplifyExpressions`] simplifies logical plans; [`ExprSimplifier`] simplifies expressions.
//!
//! Sail retains regexes whose literals cannot safely become LIKE patterns.

#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "Retain the upstream implementation and tests"
)]
#![cfg_attr(test, expect(clippy::panic, reason = "Retain the upstream tests"))]

mod cast_preimage;
pub mod expr_simplifier;
mod inlist_simplifier;
mod linear_aggregates;
mod regex;
pub mod simplify_exprs;
mod udf_preimage;
mod utils;

pub use datafusion_expr::simplify::SimplifyContext;
pub use expr_simplifier::*;
pub use simplify_exprs::*;

#[cfg(test)]
mod test {
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::Result;
    use datafusion_expr::LogicalPlan;
    use datafusion_expr::logical_plan::table_scan;

    // Minimal optimizer test helpers.
    // [CREDIT]: https://github.com/apache/datafusion/blob/1635ed94742c1c060852bc34dc6ebb2302241c7b/datafusion/optimizer/src/test/mod.rs
    pub fn test_table_scan_with_name(name: &str) -> Result<LogicalPlan> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::UInt32, false),
            Field::new("b", DataType::UInt32, false),
            Field::new("c", DataType::UInt32, false),
        ]);
        table_scan(Some(name), &schema, None)?.build()
    }

    pub fn assert_fields_eq(plan: &LogicalPlan, expected: Vec<&str>) {
        let actual: Vec<String> = plan
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(actual, expected);
    }
}
