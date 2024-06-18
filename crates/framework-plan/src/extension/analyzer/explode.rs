use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion::common::{Column, DataFusionError, Result, UnnestOptions};
use datafusion::functions::core::expr_ext::FieldAccessor;
use datafusion::logical_expr::builder::unnest_with_options;
use datafusion::logical_expr::{
    col, Expr, ExprSchemable, LogicalPlan, Projection, ScalarUDF, ScalarUDFImpl,
};

use crate::extension::analyzer::expr_to_udf;
use crate::extension::function::array::{ArrayEmptyToNull, ArrayItemWithPosition, MapToArray};
use crate::extension::function::explode::Explode;

pub(crate) fn rewrite_explode(
    input: LogicalPlan,
    expr: Vec<Expr>,
) -> Result<(LogicalPlan, Vec<Expr>)> {
    let mut rewriter = ExplodeRewriter::new(input);
    for e in expr {
        e.rewrite(&mut rewriter)?;
    }
    Ok((rewriter.plan, rewriter.expr))
}

enum ExplodeInput {
    List,
    Map,
}

struct ExplodeRewriter {
    plan: LogicalPlan,
    depth: usize,
    expr: Vec<Expr>,
}

impl ExplodeRewriter {
    fn new(plan: LogicalPlan) -> Self {
        Self {
            plan,
            depth: 0,
            expr: vec![],
        }
    }

    fn mutate_internal(&mut self, node: Expr) -> Result<Vec<Expr>> {
        let (udf, args) = match expr_to_udf(&node) {
            Some((udf, args)) => (udf, args),
            None => return Ok(vec![node]),
        };
        let inner = udf.inner();
        let func = match inner.as_any().downcast_ref::<Explode>() {
            Some(f) => f,
            None => return Ok(vec![node]),
        };
        let (with_position, preserve_nulls) = match func.name() {
            "explode" => (false, false),
            "explode_outer" => (false, true),
            "posexplode" => (true, false),
            "posexplode_outer" => (true, true),
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "unknown function name: {}",
                    func.name()
                )))
            }
        };
        if args.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "{} should only have one argument",
                func.name()
            )));
        }
        let arg = &args[0];
        let name = format!("{}({})", func.name(), arg.display_name()?);
        let column = Column::new_unqualified(name);

        let input = match arg.get_type(self.plan.schema())? {
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
                ExplodeInput::List
            }
            DataType::Map(_, _) => ExplodeInput::Map,
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "{} should only have list or map argument",
                    func.name()
                )))
            }
        };

        let arg = match input {
            ExplodeInput::List => arg.clone(),
            ExplodeInput::Map => {
                let map_to_array = ScalarUDF::new_from_impl(MapToArray::new(preserve_nulls));
                map_to_array.call(vec![arg.clone()])
            }
        };
        let arg = match preserve_nulls {
            true => {
                let array_empty_to_null = ScalarUDF::new_from_impl(ArrayEmptyToNull::new());
                array_empty_to_null.call(vec![arg])
            }
            false => arg,
        };
        let arg = match with_position {
            true => {
                let array_with_pos = ScalarUDF::new_from_impl(ArrayItemWithPosition::new());
                array_with_pos.call(vec![arg])
            }
            false => arg,
        };

        // TODO: handle column name conflict
        let out = match (input, with_position) {
            (ExplodeInput::List, false) => vec![col(column.clone())],
            (ExplodeInput::List, true) => vec![
                col(column.clone()).field("pos"),
                col(column.clone()).field("col"),
            ],
            (ExplodeInput::Map, false) => vec![
                col(column.clone()).field("key"),
                col(column.clone()).field("value"),
            ],
            (ExplodeInput::Map, true) => vec![
                col(column.clone()).field("pos"),
                col(column.clone()).field("col").field("key"),
                col(column.clone()).field("col").field("value"),
            ],
        };

        let out = if let Some(names) = func.output_names() {
            if names.len() != out.len() {
                return Err(DataFusionError::Plan(format!(
                    "expected {} explode output names, found {}",
                    out.len(),
                    names.len()
                )));
            }
            out.into_iter()
                .zip(names.iter())
                .map(|(e, n)| e.alias(n))
                .collect::<Vec<Expr>>()
        } else {
            out
        };

        let mut projections = self
            .plan
            .schema()
            .columns()
            .iter()
            .map(|x| Expr::Column(x.clone()))
            .collect::<Vec<Expr>>();
        projections.push(arg.clone().alias(&column.name));

        self.plan = unnest_with_options(
            LogicalPlan::Projection(Projection::try_new(
                projections,
                Arc::new(self.plan.clone()),
            )?),
            vec![column],
            UnnestOptions { preserve_nulls },
        )?;
        Ok(out)
    }
}

impl TreeNodeRewriter for ExplodeRewriter {
    type Node = Expr;

    fn f_down(&mut self, node: Expr) -> Result<Transformed<Expr>> {
        self.depth += 1;
        Ok(Transformed::no(node))
    }

    fn f_up(&mut self, node: Expr) -> Result<Transformed<Expr>> {
        let nodes = self.mutate_internal(node)?;
        self.depth -= 1;
        if self.depth == 0 {
            self.expr.extend(nodes);
            // returns a dummy expression which is not used
            Ok(Transformed::yes(Expr::Column(Column::new_unqualified(""))))
        } else {
            if nodes.len() != 1 {
                return Err(DataFusionError::Plan(
                    "multi-output explode cannot appear in nested expression".to_string(),
                ));
            }
            Ok(Transformed::yes(nodes[0].clone()))
        }
    }
}
