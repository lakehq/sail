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
use datafusion_expr::{ident, when};
use datafusion_functions_nested::expr_fn as nested_fn;
use either::Either;
use sail_common::spec::{SAIL_MAP_KEY_FIELD_NAME, SAIL_MAP_VALUE_FIELD_NAME};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::array::spark_array_item_with_position::ArrayItemWithPosition;
use sail_function::scalar::explode::{Explode, ExplodeKind};
use sail_function::scalar::multi_expr::MultiExpr;
use sail_function::scalar::variant::spark_variant_explode::SparkVariantExplodeUdf;
use sail_function::scalar::variant::utils::helper::try_field_as_variant_array;

use crate::resolver::state::PlanResolverState;
use crate::resolver::tree::{empty_logical_plan, PlanRewriter};

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

fn ensure_variant_expr(expr: &Expr, schema: &dyn ExprSchema, function_name: &str) -> Result<()> {
    let (_, field) = expr.to_field(schema)?;
    if try_field_as_variant_array(field.as_ref()).is_err() {
        return plan_err!(
            "{function_name} expects a VARIANT input, got {:?}",
            field.data_type()
        );
    }
    Ok(())
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
        let (with_position, preserve_nulls, is_inline) = match explode.kind() {
            ExplodeKind::Explode => (false, false, false),
            ExplodeKind::ExplodeOuter => (false, true, false),
            ExplodeKind::PosExplode => (true, false, false),
            ExplodeKind::PosExplodeOuter => (true, true, false),
            ExplodeKind::Inline => (false, false, true),
            ExplodeKind::InlineOuter => (false, true, true),
            ExplodeKind::VariantExplode | ExplodeKind::VariantExplodeOuter => {
                let arg = args.one()?;
                ensure_variant_expr(&arg, self.plan.schema(), func.name())?;

                // Wrap the variant input with SparkVariantExplodeUdf to get
                // List<Struct<pos, key, value>>, then unnest inline-style.
                // Spark documents variant_explode and variant_explode_outer as
                // both ignoring non-array/object inputs, including SQL NULL,
                // variant null, and scalar variants.
                let explode_arr = ScalarUDF::from(SparkVariantExplodeUdf::new()).call(vec![arg]);

                let name = self.state.register_field_name("");
                let out = vec![
                    ident(&name).field("pos").alias("pos"),
                    ident(&name).field("key").alias("key"),
                    ident(&name).field("value").alias("value"),
                ];

                let mut projections = self
                    .plan
                    .schema()
                    .columns()
                    .into_iter()
                    .map(Expr::Column)
                    .collect::<Vec<_>>();
                projections.push(explode_arr.alias(&name));

                let plan = mem::replace(&mut self.plan, empty_logical_plan());
                let recursions = vec![];
                self.plan = unnest_with_options(
                    LogicalPlan::Projection(Projection::try_new(projections, Arc::new(plan))?),
                    vec![Column::from_name(&name)],
                    UnnestOptions {
                        preserve_nulls: false,
                        recursions,
                    },
                )?;

                let out = ScalarUDF::from(MultiExpr::new()).call(out);
                return Ok(Transformed::yes(out));
            }
        };
        let arg = args.one()?;
        let arg_type = arg.get_type(self.plan.schema())?;
        let return_type = func.return_type(&[arg_type])?;
        let data_type = ExplodeDataType::try_from_expr(&arg, self.plan.schema())?;
        let arg = match data_type {
            ExplodeDataType::List => arg,
            ExplodeDataType::Map => nested_fn::map_entries(arg),
        };
        let arg = match preserve_nulls {
            true => when(nested_fn::array_empty(arg.clone()).is_false(), arg).end()?,
            false => arg,
        };
        let arg = match with_position {
            true => ScalarUDF::from(ArrayItemWithPosition::new()).call(vec![arg]),
            false => arg,
        };

        let name = self.state.register_field_name("");
        let out = match (data_type, with_position, is_inline) {
            (ExplodeDataType::List, false, false) => vec![ident(&name).alias("col")],
            (ExplodeDataType::List, true, false) => {
                vec![
                    ident(&name).field("pos").alias("pos"),
                    ident(&name).field("col").alias("col"),
                ]
            }
            (ExplodeDataType::List, _, true) => match return_type {
                DataType::Struct(fields) => Ok(fields
                    .into_iter()
                    .map(|field| {
                        ident(&name)
                            .field(field.name().as_str())
                            .alias(field.name().as_str())
                    })
                    .collect::<Vec<_>>()),
                wrong_type => plan_err!(
                    "inline/inline_outer expects List<Struct> as argument, got {wrong_type:?}"
                ),
            }?,
            (ExplodeDataType::Map, false, _) => {
                vec![
                    ident(&name).field(SAIL_MAP_KEY_FIELD_NAME).alias("key"),
                    ident(&name).field(SAIL_MAP_VALUE_FIELD_NAME).alias("value"),
                ]
            }
            (ExplodeDataType::Map, true, _) => vec![
                ident(&name).field("pos").alias("pos"),
                ident(&name)
                    .field("col")
                    .field(SAIL_MAP_KEY_FIELD_NAME)
                    .alias("key"),
                ident(&name)
                    .field("col")
                    .field(SAIL_MAP_VALUE_FIELD_NAME)
                    .alias("value"),
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

#[cfg(test)]
mod tests {
    use datafusion_common::tree_node::TreeNode;
    use datafusion_common::ScalarValue;
    use datafusion_expr::expr::ScalarFunction;
    use datafusion_expr::{col, lit, LogicalPlanBuilder};
    use sail_function::scalar::multi_expr::MultiExpr;
    use sail_function::scalar::variant::spark_json_to_variant::SparkJsonToVariantUdf;

    use super::*;

    fn build_variant_plan() -> Result<LogicalPlan> {
        LogicalPlanBuilder::values(vec![vec![lit(ScalarValue::Utf8(Some("[1]".into())))]])?
            .project(vec![ScalarUDF::from(SparkJsonToVariantUdf::new())
                .call(vec![col("column1")])
                .alias("variant_col")])?
            .build()
    }

    #[test]
    fn test_ensure_variant_expr_rejects_non_variant_input() -> Result<()> {
        let plan = LogicalPlanBuilder::values(vec![vec![lit(ScalarValue::Int32(Some(1)))]])?
            .project(vec![col("column1").alias("plain_col")])?
            .build()?;

        let error = ensure_variant_expr(&col("plain_col"), plan.schema(), "variant_explode_outer")
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("variant_explode_outer expects a VARIANT input"));
        Ok(())
    }

    #[test]
    fn test_variant_explode_rewriter_builds_unnest_and_multi_expr() -> Result<()> {
        let expr = ScalarUDF::from(Explode::new(ExplodeKind::VariantExplode))
            .call(vec![col("variant_col")]);
        let mut state = PlanResolverState::new();
        let mut rewriter = ExplodeRewriter::new_from_plan(build_variant_plan()?, &mut state);
        let rewritten = expr.rewrite(&mut rewriter)?;

        let Expr::ScalarFunction(ScalarFunction { func, args }) = rewritten.data else {
            panic!("expected a scalar function result");
        };
        assert!(func.inner().as_any().is::<MultiExpr>());
        assert_eq!(args.len(), 3);
        assert_eq!(
            args.iter()
                .map(|arg| match arg {
                    Expr::Alias(alias) => alias.name.clone(),
                    other => panic!("expected aliased field access, got {other:?}"),
                })
                .collect::<Vec<_>>(),
            vec!["pos", "key", "value"]
        );

        match rewriter.into_plan() {
            LogicalPlan::Unnest(unnest) => {
                assert!(matches!(unnest.input.as_ref(), LogicalPlan::Projection(_)));
            }
            other => panic!("expected an Unnest plan, got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_variant_explode_outer_rewriter_uses_same_inline_path() -> Result<()> {
        let expr = ScalarUDF::from(Explode::new(ExplodeKind::VariantExplodeOuter))
            .call(vec![col("variant_col")]);
        let mut state = PlanResolverState::new();
        let mut rewriter = ExplodeRewriter::new_from_plan(build_variant_plan()?, &mut state);
        let rewritten = expr.rewrite(&mut rewriter)?;

        let Expr::ScalarFunction(ScalarFunction { func, args }) = rewritten.data else {
            panic!("expected a scalar function result");
        };
        assert!(func.inner().as_any().is::<MultiExpr>());
        assert_eq!(args.len(), 3);
        assert!(matches!(rewriter.into_plan(), LogicalPlan::Unnest(_)));

        Ok(())
    }
}
