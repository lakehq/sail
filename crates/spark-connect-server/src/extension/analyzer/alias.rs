use datafusion::common::tree_node::{RewriteRecursion, TreeNode, TreeNodeRewriter};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, ScalarUDF, ScalarUDFImpl};

use crate::extension::analyzer::expr_to_udf;
use crate::extension::function::alias::MultiAlias;
use crate::extension::function::explode::Explode;

pub(crate) fn rewrite_multi_alias(expr: Vec<Expr>) -> Result<Vec<Expr>> {
    let mut rewriter = MultiAliasRewriter {};
    let expr = expr
        .into_iter()
        .map(|e| e.rewrite(&mut rewriter))
        .collect::<Result<Vec<_>>>()?;
    Ok(expr)
}

struct MultiAliasRewriter {}

impl MultiAliasRewriter {
    fn with_multi_alias(expr: Expr, names: Vec<String>) -> Result<Expr> {
        let (udf, args) = match expr_to_udf(&expr) {
            Some((udf, args)) => (udf, args),
            None => {
                return Err(DataFusionError::Plan(
                    "cannot set multi-alias on unsupported expression".to_string(),
                ))
            }
        };
        let inner = udf.inner();
        if let Some(f) = inner.as_any().downcast_ref::<MultiAlias>() {
            let f = f.with_names(names)?;
            Ok(ScalarUDF::new_from_impl(f).call(args.clone()))
        } else if let Some(f) = inner.as_any().downcast_ref::<Explode>() {
            let f = f.with_output_names(names)?;
            Ok(ScalarUDF::new_from_impl(f).call(args.clone()))
        } else {
            Err(DataFusionError::Plan(
                "cannot set multi-alias on unsupported function".to_string(),
            ))
        }
    }
}

impl TreeNodeRewriter for MultiAliasRewriter {
    type N = Expr;

    fn pre_visit(&mut self, _: &Expr) -> Result<RewriteRecursion> {
        Ok(RewriteRecursion::Continue)
    }

    fn mutate(&mut self, node: Expr) -> Result<Expr> {
        let (udf, args) = match expr_to_udf(&node) {
            Some((udf, args)) => (udf, args),
            None => return Ok(node),
        };
        let inner = udf.inner();
        let func = match inner.as_any().downcast_ref::<MultiAlias>() {
            Some(f) => f,
            None => return Ok(node),
        };
        if args.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "{} should only have one argument",
                func.name()
            )));
        }
        Self::with_multi_alias(args[0].clone(), func.names().clone())
    }
}
