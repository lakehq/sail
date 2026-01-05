// https://github.com/datafusion-contrib/datafusion-functions-json/blob/78c5abbf7222510ff221517f5d2e3c344969da98/src/rewrite.rs
// Copyright datafusion-functions-json contributors
// Portions Copyright (2026) LakeSail, Inc.
// Modified in 2026 by LakeSail, Inc.
//
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

use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{Column, DFSchema, Result};
use datafusion::logical_expr::expr::{Alias, Cast, Expr, ScalarFunction};
use datafusion::logical_expr::expr_rewriter::FunctionRewrite;
use datafusion::logical_expr::planner::{ExprPlanner, PlannerResult, RawBinaryExpr};
use datafusion::logical_expr::sqlparser::ast::BinaryOperator;
use datafusion::logical_expr::ScalarUDF;
use datafusion::scalar::ScalarValue;

#[derive(Debug)]
pub(crate) struct JsonFunctionRewriter;

impl FunctionRewrite for JsonFunctionRewriter {
    fn name(&self) -> &'static str {
        "JsonFunctionRewriter"
    }

    fn rewrite(
        &self,
        expr: Expr,
        _schema: &DFSchema,
        _config: &ConfigOptions,
    ) -> Result<Transformed<Expr>> {
        let transform = match &expr {
            Expr::Cast(cast) => optimise_json_get_cast(cast),
            Expr::ScalarFunction(func) => unnest_json_calls(func),
            _ => None,
        };
        Ok(transform.unwrap_or_else(|| Transformed::no(expr)))
    }
}

/// This replaces `get_json(foo, bar)::int` with `json_get_int(foo, bar)` so the JSON function can take care of
/// extracting the right value type from JSON without the need to materialize the JSON union.
fn optimise_json_get_cast(_cast: &Cast) -> Option<Transformed<Expr>> {
    // This functionality is not used in our codebase, so we're removing it
    None
}

// Replace nested JSON functions e.g. `json_get(json_get(col, 'foo'), 'bar')` with `json_get(col, 'foo', 'bar')`
fn unnest_json_calls(func: &ScalarFunction) -> Option<Transformed<Expr>> {
    if !matches!(func.func.name(), "json_as_text") {
        return None;
    }
    let mut outer_args_iter = func.args.iter();
    let first_arg = outer_args_iter.next()?;
    let inner_func = extract_scalar_function(first_arg)?;

    // both json_get and json_as_text would produce new JSON to be processed by the outer
    // function so can be inlined
    if !matches!(inner_func.func.name(), "json_as_text") {
        return None;
    }

    let mut args = inner_func.args.clone();
    args.extend(outer_args_iter.cloned());
    // See #23, unnest only when all lookup arguments are literals
    if args
        .iter()
        .skip(1)
        .all(|arg| matches!(arg, Expr::Literal(_, _)))
    {
        Some(Transformed::yes(Expr::ScalarFunction(ScalarFunction {
            func: func.func.clone(),
            args,
        })))
    } else {
        None
    }
}

fn extract_scalar_function(expr: &Expr) -> Option<&ScalarFunction> {
    match expr {
        Expr::ScalarFunction(func) => Some(func),
        Expr::Alias(alias) => extract_scalar_function(&alias.expr),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy)]
enum JsonOperator {
    LongArrow,
}

impl TryFrom<&BinaryOperator> for JsonOperator {
    type Error = ();

    fn try_from(op: &BinaryOperator) -> Result<Self, Self::Error> {
        match op {
            BinaryOperator::LongArrow => Ok(JsonOperator::LongArrow),
            _ => Err(()),
        }
    }
}

impl From<JsonOperator> for Arc<ScalarUDF> {
    fn from(op: JsonOperator) -> Arc<ScalarUDF> {
        match op {
            JsonOperator::LongArrow => crate::scalar::json::udfs::json_as_text_udf(),
        }
    }
}

impl std::fmt::Display for JsonOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JsonOperator::LongArrow => write!(f, "->>"),
        }
    }
}

/// Convert an Expr to a String representatiion for use in alias names.
fn expr_to_sql_repr(expr: &Expr) -> String {
    match expr {
        Expr::Column(Column {
            name,
            relation,
            spans: _,
        }) => relation
            .as_ref()
            .map_or_else(|| name.clone(), |r| format!("{r}.{name}")),
        Expr::Alias(alias) => alias.name.clone(),
        Expr::Literal(scalar, _) => match scalar {
            ScalarValue::Utf8(Some(v))
            | ScalarValue::Utf8View(Some(v))
            | ScalarValue::LargeUtf8(Some(v)) => {
                format!("'{v}'")
            }
            ScalarValue::UInt8(Some(v)) => v.to_string(),
            ScalarValue::UInt16(Some(v)) => v.to_string(),
            ScalarValue::UInt32(Some(v)) => v.to_string(),
            ScalarValue::UInt64(Some(v)) => v.to_string(),
            ScalarValue::Int8(Some(v)) => v.to_string(),
            ScalarValue::Int16(Some(v)) => v.to_string(),
            ScalarValue::Int32(Some(v)) => v.to_string(),
            ScalarValue::Int64(Some(v)) => v.to_string(),
            _ => scalar.to_string(),
        },
        Expr::Cast(cast) => expr_to_sql_repr(&cast.expr),
        _ => expr.to_string(),
    }
}

/// Implement a custom SQL planner to replace postgres JSON operators with custom UDFs
#[derive(Debug, Default)]
pub struct JsonExprPlanner;

impl ExprPlanner for JsonExprPlanner {
    fn plan_binary_op(
        &self,
        expr: RawBinaryExpr,
        _schema: &DFSchema,
    ) -> Result<PlannerResult<RawBinaryExpr>> {
        let Ok(op) = JsonOperator::try_from(&expr.op) else {
            return Ok(PlannerResult::Original(expr));
        };

        let left_repr = expr_to_sql_repr(&expr.left);
        let right_repr = expr_to_sql_repr(&expr.right);

        let alias_name = format!("{left_repr} {op} {right_repr}");

        // we put the alias in so that default column titles are `foo -> bar` instead of `json_get(foo, bar)`
        Ok(PlannerResult::Planned(Expr::Alias(Alias::new(
            Expr::ScalarFunction(ScalarFunction {
                func: op.into(),
                args: vec![expr.left, expr.right],
            }),
            None::<&str>,
            alias_name,
        ))))
    }
}
