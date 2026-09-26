use std::collections::HashMap;
use std::sync::Arc;

use datafusion::optimizer::OptimizerConfig;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, DFSchema, DFSchemaRef, Result, ScalarValue, TableReference};
use datafusion_expr::{Expr, ExprSchemable, Operator, lit};

use super::{has_uncorrelated_in, has_volatile_expression};

/// Catalyst retains CASE/COALESCE boundaries until IN becomes an existence
/// result. DataFusion's Boolean CASE lowering is safe only after that point:
/// pushing NOT through a lowered CASE would instead create a null-aware NOT IN.
/// Temporary columns let its ordinary simplifier process the surrounding
/// expression without crossing the surviving conditional boundaries. They never
/// enter a plan, and retain each expression's type, metadata, and nullability.
pub(super) fn simplify(
    expr: Expr,
    schema: DFSchemaRef,
    config: &dyn OptimizerConfig,
) -> Result<Expr> {
    let mut fields = vec![];
    let mut replacements = HashMap::new();
    let mut shared = HashMap::<Expr, Column>::new();
    let mut qualifier = None;
    let protected = expr
        .transform_down(|expr| {
            let expr = simplify_complements(push_literal_comparison(expr, &schema, config)?)?;
            if !is_conditional(&expr) || !has_uncorrelated_in(&expr)? {
                return Ok(Transformed::no(expr));
            }
            let expr = simplify_conditional(expr, &schema, config)?;
            if !is_conditional(&expr) || !has_uncorrelated_in(&expr)? {
                return Ok(Transformed::yes(expr));
            }
            let deterministic = !has_volatile_expression(&expr)?;
            let existing = deterministic.then(|| shared.get(&expr)).flatten();
            let column = if let Some(column) = existing {
                column.clone()
            } else {
                let name = fields.len().to_string();
                let qualifier = qualifier.get_or_insert_with(|| {
                    TableReference::bare(config.alias_generator().next("__sail_conditional"))
                });
                let column = Column::new(Some(qualifier.clone()), &name);
                let (_, field) = expr.to_field(schema.as_ref())?;
                fields.push((
                    Some(qualifier.clone()),
                    Arc::new(field.as_ref().clone().with_name(name)),
                ));
                if deterministic {
                    shared.insert(expr.clone(), column.clone());
                }
                replacements.insert(column.clone(), expr);
                column
            };
            Ok(Transformed::new(
                Expr::Column(column),
                true,
                TreeNodeRecursion::Jump,
            ))
        })?
        .data;
    let extended = if fields.is_empty() {
        schema
    } else {
        Arc::new(schema.join(&DFSchema::new_with_metadata(fields, HashMap::new())?)?)
    };
    super::simplifier(extended, config)
        .simplify(protected)?
        .transform_up(|expr| {
            if let Expr::Column(column) = &expr
                && let Some(original) = replacements.get(column)
            {
                return Ok(Transformed::yes(original.clone()));
            }
            Ok(Transformed::no(expr))
        })
        .data()
}

/// Alias substitution in a filter inlines Catalyst's NULLIF common value.
/// Keep projected NULLIF as a scalar call so its comparison and result share
/// the already evaluated existence value; only the copied predicate expands.
pub(super) fn inline_filter_nullif(expr: Expr, schema: &DFSchema) -> Result<Expr> {
    expr.transform_up(|expr| {
        let Expr::ScalarFunction(function) = &expr else {
            return Ok(Transformed::no(expr));
        };
        if function.func.name() != "nullif" || !has_uncorrelated_in(&expr)? {
            return Ok(Transformed::no(expr));
        }
        let [left, right]: [Expr; 2] = function.args.clone().try_into().map_err(|_| {
            datafusion_common::internal_datafusion_err!("NULLIF requires two arguments")
        })?;
        let null = lit(ScalarValue::try_new_null(&expr.get_type(schema)?)?);
        Ok(Transformed::yes(
            datafusion_expr::expr_fn::when(left.clone().eq(right), null).otherwise(left)?,
        ))
    })
    .data()
}

fn is_conditional(expr: &Expr) -> bool {
    matches!(expr, Expr::Case(_))
        || matches!(expr, Expr::ScalarFunction(function)
            if matches!(function.func.name(), "coalesce" | "nvl" | "nvl2" | "nullif"))
        || matches!(expr, Expr::BinaryExpr(binary)
            if matches!(binary.op, Operator::Eq | Operator::NotEq | Operator::Lt | Operator::LtEq
                | Operator::Gt | Operator::GtEq | Operator::IsDistinctFrom | Operator::IsNotDistinctFrom)
                && [binary.left.as_ref(), binary.right.as_ref()].iter().any(|expr|
                matches!(expr, Expr::ScalarFunction(function) if function.func.name() == "nullif")
                    || matches!(expr, Expr::BinaryExpr(_)) && is_conditional(expr)))
}

fn simplify_complements(expr: Expr) -> Result<Expr> {
    let Expr::BinaryExpr(binary) = &expr else {
        return Ok(expr);
    };
    if !matches!(binary.op, Operator::And | Operator::Or) || !has_uncorrelated_in(&expr)? {
        return Ok(expr);
    }
    let positive = match (binary.left.as_ref(), binary.right.as_ref()) {
        (Expr::Not(child), other) | (other, Expr::Not(child)) if child.as_ref() == other => {
            Some(other.clone())
        }
        (Expr::InSubquery(left), Expr::InSubquery(right))
            if left.expr == right.expr
                && left.subquery == right.subquery
                && left.negated != right.negated =>
        {
            Some(Expr::InSubquery(if left.negated {
                right.clone()
            } else {
                left.clone()
            }))
        }
        _ => None,
    };
    let Some(positive) = positive else {
        return Ok(expr);
    };
    if has_volatile_expression(&positive)? {
        return Ok(expr);
    }
    // Catalyst preserves SQL nullability here, then rewrites the surviving IN
    // to an existence value. Folding after separate IN/NOT IN joins is too late.
    datafusion_expr::expr_fn::when(positive.is_null(), lit(ScalarValue::Boolean(None)))
        .otherwise(lit(binary.op == Operator::Or))
}

fn null_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(value, _) if value.is_null())
}

fn bool_literal(expr: &Expr, value: bool) -> bool {
    matches!(expr, Expr::Literal(ScalarValue::Boolean(Some(actual)), _) if *actual == value)
}

fn lower_nvl2(expr: Expr) -> Result<Expr> {
    let Expr::ScalarFunction(function) = &expr else {
        return Ok(expr);
    };
    if function.func.name() != "nvl2" {
        return Ok(expr);
    }
    let [test, then, otherwise]: [Expr; 3] = function.args.clone().try_into().map_err(|_| {
        datafusion_common::internal_datafusion_err!("NVL2 requires three arguments")
    })?;
    Ok(Expr::Case(datafusion_expr::expr::Case::new(
        None,
        vec![(Box::new(test.is_not_null()), Box::new(then))],
        Some(Box::new(otherwise)),
    )))
}

fn push_literal_comparison(
    expr: Expr,
    schema: &DFSchemaRef,
    config: &dyn OptimizerConfig,
) -> Result<Expr> {
    let Expr::BinaryExpr(binary) = &expr else {
        return Ok(expr);
    };
    // NULLIF comparisons must stay intact until alias substitution. Their
    // conditional simplifier already visits the operands; revisiting them here
    // would double the work at every level of a nested comparison chain.
    if is_conditional(&expr) {
        return Ok(expr);
    }
    if !matches!(
        binary.op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom
    ) {
        return Ok(expr);
    }
    let (conditional, literal, conditional_left) =
        match (binary.left.as_ref(), binary.right.as_ref()) {
            (conditional, literal @ Expr::Literal(_, _)) => (conditional, literal, true),
            (literal @ Expr::Literal(_, _), conditional) => (conditional, literal, false),
            _ => return Ok(expr),
        };
    if !has_uncorrelated_in(conditional)? {
        return Ok(expr);
    }
    // Catalyst reaches a fixed point across nested comparisons. Simplify the
    // inner expression first so each enclosing comparison sees its CASE result
    // before DataFusion can turn that comparison into a NOT.
    let conditional = simplify(conditional.clone(), Arc::clone(schema), config)?;
    let conditional = lower_nvl2(conditional)?;
    let Expr::Case(mut case) = conditional else {
        return Ok(if conditional_left {
            datafusion_expr::expr_fn::binary_expr(conditional, binary.op, literal.clone())
        } else {
            datafusion_expr::expr_fn::binary_expr(literal.clone(), binary.op, conditional)
        });
    };
    // Spark pushes a foldable comparison into CASE/IF when at most one result
    // branch is not foldable. This happens before comparison-to-NOT folding.
    let mut non_foldable = 0;
    for result in case
        .when_then_expr
        .iter()
        .map(|(_, then)| then.as_ref())
        .chain(case.else_expr.as_deref())
    {
        if !result.column_refs().is_empty()
            || !super::locally_evaluable(result)?
            || has_volatile_expression(result)?
        {
            non_foldable += 1;
        }
    }
    if non_foldable > 1 {
        return Ok(expr);
    }
    let compare = |branch: Expr| {
        if conditional_left {
            datafusion_expr::expr_fn::binary_expr(branch, binary.op, literal.clone())
        } else {
            datafusion_expr::expr_fn::binary_expr(literal.clone(), binary.op, branch)
        }
    };
    for (_, then) in &mut case.when_then_expr {
        **then = compare(*then.clone());
    }
    case.else_expr = Some(Box::new(compare(
        case.else_expr.map_or(lit(ScalarValue::Null), |expr| *expr),
    )));
    Ok(Expr::Case(case))
}

fn simplify_conditional(
    expr: Expr,
    schema: &DFSchemaRef,
    config: &dyn OptimizerConfig,
) -> Result<Expr> {
    // TODO: Match Spark's deferred errors in data-dependent conditional branches.
    // DataFusion's later literal-cast simplifier also folds such branches eagerly;
    // preserving their errors requires support beyond this projected-IN helper.
    let data_type = expr.get_type(schema.as_ref())?;
    let null = || ScalarValue::try_new_null(&data_type).map(lit);
    match expr {
        Expr::BinaryExpr(mut binary) => {
            binary.left = Box::new(simplify(*binary.left, Arc::clone(schema), config)?);
            binary.right = Box::new(simplify(*binary.right, Arc::clone(schema), config)?);
            Ok(Expr::BinaryExpr(binary))
        }
        Expr::ScalarFunction(mut function) if function.func.name() == "nullif" => {
            function.args = function
                .args
                .into_iter()
                .map(|arg| simplify(arg, Arc::clone(schema), config))
                .collect::<Result<Vec<_>>>()?;
            if function.args.get(1).is_some_and(null_literal) {
                return Ok(function.args.remove(0));
            }
            Ok(Expr::ScalarFunction(function))
        }
        Expr::ScalarFunction(function) if function.func.name() == "nvl2" => {
            // Spark replaces NVL2 with IF(IS NOT NULL(test), then, else).
            // Lower through the same protected CASE path before Boolean folding.
            simplify_conditional(lower_nvl2(Expr::ScalarFunction(function))?, schema, config)
        }
        Expr::ScalarFunction(mut function)
            if matches!(function.func.name(), "coalesce" | "nvl") =>
        {
            // NVL and IFNULL both resolve to DataFusion's `nvl` function, whose
            // two arguments have the same conditional boundary as COALESCE.
            // Spark NullPropagation removes NULL arguments and everything after
            // the first non-nullable argument, retaining any surviving COALESCE.
            let mut args = vec![];
            for arg in function.args {
                let arg = simplify(arg, Arc::clone(schema), config)?;
                if null_literal(&arg) {
                    continue;
                }
                let nullable = arg.nullable(schema.as_ref())?;
                args.push(arg);
                if !nullable {
                    break;
                }
            }
            match args.len() {
                0 => null(),
                1 => Ok(args.remove(0)),
                _ => {
                    function.args = args;
                    Ok(Expr::ScalarFunction(function))
                }
            }
        }
        Expr::Case(mut case) => {
            // Fold conditions before their result branches so a statically
            // unreachable branch never exposes a constant error to DataFusion.
            let base = case
                .expr
                .take()
                .map(|expr| simplify(*expr, Arc::clone(schema), config))
                .transpose()?;
            let mut branches = vec![];
            let mut terminated = None;
            for (when, then) in case.when_then_expr {
                // Spark represents simple CASE as searched CASE with equalities.
                let when = if let Some(base) = &base {
                    base.clone().eq(*when)
                } else {
                    *when
                };
                let when = simplify(when, Arc::clone(schema), config)?;
                if bool_literal(&when, false) || null_literal(&when) {
                    continue;
                }
                let then = simplify(*then, Arc::clone(schema), config)?;
                if bool_literal(&when, true) {
                    if branches.is_empty() {
                        return Ok(then);
                    }
                    // The SQL resolver encodes ELSE as a final WHEN TRUE.
                    // Restore the fallback before comparing result branches.
                    terminated = Some(Box::new(then));
                    break;
                }
                branches.push((Box::new(when), Box::new(then)));
            }
            case.when_then_expr = branches;
            case.else_expr = if let Some(otherwise) = terminated {
                Some(otherwise)
            } else {
                case.else_expr
                    .map(|expr| simplify(*expr, Arc::clone(schema), config).map(Box::new))
                    .transpose()?
            };
            if case.when_then_expr.is_empty() {
                return case.else_expr.map_or_else(null, |expr| Ok(*expr));
            }
            // Catalyst simplifies only these Boolean CASE identities, rather
            // than expanding arbitrary Boolean results into AND/OR clauses.
            if let [(when, then)] = case.when_then_expr.as_slice()
                && let Some(otherwise) = &case.else_expr
                && ((bool_literal(then, true) && bool_literal(otherwise, false))
                    || (bool_literal(then, false) && bool_literal(otherwise, true)))
            {
                let condition = if when.nullable(schema.as_ref())? {
                    datafusion_expr::expr_fn::binary_expr(
                        *when.clone(),
                        Operator::IsNotDistinctFrom,
                        lit(true),
                    )
                } else {
                    *when.clone()
                };
                return Ok(if bool_literal(then, true) {
                    condition
                } else {
                    !condition
                });
            }
            let otherwise = case.else_expr.as_deref().cloned().map_or_else(null, Ok)?;
            if case
                .when_then_expr
                .iter()
                .all(|(_, then)| then.as_ref() == &otherwise)
            {
                // Only remove a deterministic suffix: Spark preserves volatile
                // conditions and their evaluation order even for equal results.
                while let Some((when, _)) = case.when_then_expr.last() {
                    if has_volatile_expression(when)? {
                        break;
                    }
                    case.when_then_expr.pop();
                }
                if case.when_then_expr.is_empty() {
                    return Ok(otherwise);
                }
            }
            if case.else_expr.as_deref().is_some_and(null_literal) {
                case.else_expr = None;
            }
            Ok(Expr::Case(case))
        }
        expr => Ok(expr),
    }
}
