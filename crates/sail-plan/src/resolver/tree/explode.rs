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

use crate::extension::function::array::spark_array_empty_to_null::ArrayEmptyToNull;
use crate::extension::function::array::spark_array_item_with_position::ArrayItemWithPosition;
use crate::extension::function::array::spark_map_to_array::MapToArray;
use crate::extension::function::explode::{Explode, ExplodeKind};
use crate::extension::function::multi_expr::MultiExpr;
use crate::resolver::state::PlanResolverState;
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

pub(crate) struct ExplodeRewriter<'s> {
    plan: LogicalPlan,
    state: &'s mut PlanResolverState,
}

impl<'s> PlanRewriter<'s> for ExplodeRewriter<'s> {
    fn new_from_plan(plan: LogicalPlan, state: &'s mut PlanResolverState) -> Self {
        Self { plan, state }
    }

    fn into_plan(self) -> LogicalPlan {
        self.plan
    }
}

impl TreeNodeRewriter for ExplodeRewriter<'_> {
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

        let name = self.state.register_field_name("");
        let out = match (data_type, with_position) {
            (ExplodeDataType::List, false) => vec![ident(&name).alias("col")],
            (ExplodeDataType::List, true) => {
                vec![
                    ident(&name).field("pos").alias("pos"),
                    ident(&name).field("col").alias("col"),
                ]
            }
            (ExplodeDataType::Map, false) => {
                vec![
                    ident(&name).field("key").alias("key"),
                    ident(&name).field("value").alias("value"),
                ]
            }
            (ExplodeDataType::Map, true) => vec![
                ident(&name).field("pos").alias("pos"),
                ident(&name).field("col").field("key").alias("key"),
                ident(&name).field("col").field("value").alias("value"),
            ],
        };

        let mut projections = self
            .plan
            .schema()
            .columns()
            .into_iter()
            .map(Expr::Column)
            .collect::<Vec<_>>();
        projections.push(arg.alias(&name));

        let plan = mem::replace(&mut self.plan, empty_logical_plan());
        // TODO: If specific columns need to be unnested multiple times (e. g at different depth), declare them here.
        //  Any unnested columns not being mentioned inside this option will be unnested with depth = 1.
        let recursions = vec![];
        self.plan = unnest_with_options(
            LogicalPlan::Projection(Projection::try_new(projections, Arc::new(plan))?),
            vec![Column::from_name(&name)],
            UnnestOptions {
                preserve_nulls,
                recursions,
            },
        )?;

        let out = match out.one_or_more()? {
            Either::Left(node) => node,
            Either::Right(nodes) => ScalarUDF::from(MultiExpr::new()).call(nodes),
        };
        Ok(Transformed::yes(out))
    }
}
