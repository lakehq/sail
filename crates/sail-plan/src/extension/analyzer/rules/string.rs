use datafusion::arrow::datatypes::DataType;
use datafusion::optimizer::AnalyzerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_common::Result;
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::{Cast, Expr, LogicalPlan, ScalarUDF};

use crate::extension::function::spark_to_string::{SparkToLargeUtf8, SparkToUtf8, SparkToUtf8View};

#[derive(Debug, Default)]
pub struct CastToString {}

impl AnalyzerRule for CastToString {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|plan| {
            let mut rewriter = CastToStringRewriter {};
            let name_preserver = NamePreserver::new(&plan);
            plan.map_expressions(|expr| {
                let name = name_preserver.save(&expr);
                expr.rewrite(&mut rewriter)
                    .map(|transformed| transformed.update_data(|e| name.restore(e)))
            })
        })
        .map(|x| x.data)
    }

    fn name(&self) -> &str {
        "cast_to_string"
    }
}

struct CastToStringRewriter {}

impl TreeNodeRewriter for CastToStringRewriter {
    type Node = Expr;

    fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        match node {
            Expr::Cast(Cast {
                expr,
                data_type: DataType::Utf8,
            }) => Ok(Transformed::yes(
                ScalarUDF::new_from_impl(SparkToUtf8::new()).call(vec![*expr]),
            )),
            Expr::Cast(Cast {
                expr,
                data_type: DataType::LargeUtf8,
            }) => Ok(Transformed::yes(
                ScalarUDF::new_from_impl(SparkToLargeUtf8::new()).call(vec![*expr]),
            )),
            Expr::Cast(Cast {
                expr,
                data_type: DataType::Utf8View,
            }) => Ok(Transformed::yes(
                ScalarUDF::new_from_impl(SparkToUtf8View::new()).call(vec![*expr]),
            )),
            _ => Ok(Transformed::no(node)),
        }
    }
}
