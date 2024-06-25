use std::mem;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion::common::{Column, Result, UnnestOptions};
use datafusion::functions::core::expr_ext::FieldAccessor;
use datafusion::logical_expr::builder::unnest_with_options;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, Projection, ScalarUDF};
use datafusion_common::{plan_err, ExprSchema};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::ident;
use either::Either;

use crate::extension::function::array::{ArrayEmptyToNull, ArrayItemWithPosition, MapToArray};
use crate::extension::function::explode::{Explode, ExplodeKind};
use crate::extension::function::multi_expr::MultiExpr;
use crate::resolver::tree::{empty_logical_plan, PlanRewriter};
use crate::utils::ItemTaker;

enum ExplodeDataType {
    List,
    Map,
}

impl ExplodeDataType {
    fn try_from_expr(expr: &Expr, schema: &dyn ExprSchema) -> Result<Self> {
        match expr.get_type(schema)? {
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
                Ok(ExplodeDataType::List)
            }
            DataType::Map(_, _) => Ok(ExplodeDataType::Map),
            _ => plan_err!("only list or map can be exploded"),
        }
    }
}

pub(crate) struct ExplodeRewriter {
    plan: LogicalPlan,
}

impl PlanRewriter for ExplodeRewriter {
    fn new_from_plan(plan: LogicalPlan) -> Self {
        Self { plan }
    }

    fn into_plan(self) -> LogicalPlan {
        self.plan
    }
}

impl ExplodeRewriter {
    fn get_named_output(out: Vec<Expr>, names: Option<&[String]>) -> Result<Vec<Expr>> {
        if let Some(names) = names {
            if names.len() != out.len() {
                return plan_err!(
                    "expected {} explode output names, found {}",
                    out.len(),
                    names.len()
                );
            }
            Ok(out
                .into_iter()
                .zip(names.iter())
                .map(|(e, n)| e.alias(n))
                .collect::<Vec<Expr>>())
        } else {
            Ok(out)
        }
    }
}

impl TreeNodeRewriter for ExplodeRewriter {
    type Node = Expr;

    fn f_up(&mut self, node: Expr) -> Result<Transformed<Expr>> {
        let (func, args) = match node {
            Expr::ScalarFunction(ScalarFunction { func, args }) => (func, args),
            _ => return Ok(Transformed::no(node)),
        };
        let inner = func.inner();
        let explode = match inner.as_any().downcast_ref::<Explode>() {
            Some(explode) => explode,
            None => {
                return Ok(Transformed::no(func.call(args)));
            }
        };
        let (with_position, preserve_nulls) = match explode.kind() {
            ExplodeKind::Explode => (false, false),
            ExplodeKind::ExplodeOuter => (false, true),
            ExplodeKind::PosExplode => (true, false),
            ExplodeKind::PosExplodeOuter => (true, true),
        };
        let arg = args.one()?;
        let data_type = ExplodeDataType::try_from_expr(&arg, self.plan.schema())?;

        let arg = match data_type {
            ExplodeDataType::List => arg,
            ExplodeDataType::Map => {
                ScalarUDF::from(MapToArray::new(preserve_nulls)).call(vec![arg])
            }
        };
        let arg = match preserve_nulls {
            true => ScalarUDF::from(ArrayEmptyToNull::new()).call(vec![arg]),
            false => arg,
        };
        let arg = match with_position {
            true => ScalarUDF::from(ArrayItemWithPosition::new()).call(vec![arg]),
            false => arg,
        };

        // TODO: handle column name conflict
        let name = format!("explode({})", arg.display_name()?);

        let out = match (data_type, with_position) {
            (ExplodeDataType::List, false) => vec![ident(&name)],
            (ExplodeDataType::List, true) => {
                vec![ident(&name).field("pos"), ident(&name).field("col")]
            }
            (ExplodeDataType::Map, false) => {
                vec![ident(&name).field("key"), ident(&name).field("value")]
            }
            (ExplodeDataType::Map, true) => vec![
                ident(&name).field("pos"),
                ident(&name).field("col").field("key"),
                ident(&name).field("col").field("value"),
            ],
        };
        let out = Self::get_named_output(out, explode.output_names())?;

        let mut projections = self
            .plan
            .schema()
            .columns()
            .iter()
            .map(|x| Expr::Column(x.clone()))
            .collect::<Vec<Expr>>();
        projections.push(arg.alias(&name));

        let plan = mem::replace(&mut self.plan, empty_logical_plan());
        self.plan = unnest_with_options(
            LogicalPlan::Projection(Projection::try_new(projections, Arc::new(plan))?),
            vec![Column::from_name(&name)],
            UnnestOptions { preserve_nulls },
        )?;

        let out = match out.one_or_more()? {
            Either::Left(node) => node,
            Either::Right(nodes) => ScalarUDF::from(MultiExpr::new()).call(nodes),
        };
        Ok(Transformed::yes(out))
    }
}
