use datafusion::arrow::datatypes::SchemaRef;
use datafusion_common::plan_datafusion_err;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_expr::Expr;

pub fn expression_before_rename(
    expr: &Expr,
    names: &[String],
    schema_before_rename: &SchemaRef,
    remove_qualifier: bool,
) -> datafusion_common::Result<Expr> {
    let rewrite = |e: Expr| -> datafusion_common::Result<Transformed<Expr>> {
        if let Expr::Column(datafusion_common::Column {
            name,
            relation,
            spans,
        }) = e
        {
            let index = names
                .iter()
                .position(|n| n == &name)
                .ok_or_else(|| plan_datafusion_err!("column {name} not found"))?;
            let name = schema_before_rename.field(index).name().to_string();
            Ok(Transformed::yes(Expr::Column(datafusion_common::Column {
                name,
                relation: if remove_qualifier { None } else { relation },
                spans,
            })))
        } else {
            Ok(Transformed::no(e))
        }
    };
    expr.clone().transform(rewrite).data()
}
