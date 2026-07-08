use std::sync::{Arc, LazyLock};

/// [CREDIT]: https://github.com/apache/datafusion/blob/32e27acb9f647c40c93e8f208b8805cd7c779ff6/datafusion/optimizer/src/analyzer/type_coercion.rs
///
/// Spark-compatible type coercion for Sail's logical analyzer.
///
/// Spark implements type coercion as Catalyst analyzer rules in the `Resolution` batch.
/// `Analyzer.scala` is `AnsiTypeCoercion.typeCoercionRules` when ANSI mode is enabled, otherwise
/// `TypeCoercion.typeCoercionRules`, and inserts into `Batch("Resolution", fixedPoint, ...)`.
///
/// Spark reference points:
/// - `opt/spark/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/Analyzer.scala:411`
/// - `opt/spark/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/Analyzer.scala:502`
/// - `opt/spark/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/TypeCoercion.scala:47`
/// - `opt/spark/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/AnsiTypeCoercion.scala:76`
/// - `opt/spark/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/TypeCoercionBase.scala:56`
/// - `opt/spark/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/TypeCoercion.scala:364`
/// - `opt/spark/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/resolver/CoercesExpressionTypes.scala:65`
///
/// This rule starts from DataFusion's analyzer coercion and keeps DataFusion behavior where it is
/// already more permissive than Spark. Sail-specific changes should only add cases Spark accepts
/// but DataFusion rejects, or resolve conflicts in favor of Spark semantics.
///
use datafusion::arrow::compute::can_cast_types;
use datafusion::arrow::datatypes::{DataType, Field, IntervalUnit, Schema, TimeUnit};
use datafusion::arrow::temporal_conversions::SECONDS_IN_DAY;
use datafusion::optimizer::AnalyzerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_common::{
    Column, DFSchema, DFSchemaRef, DataFusionError, Result, ScalarValue, TableReference, exec_err,
    internal_datafusion_err, internal_err, not_impl_err, plan_datafusion_err, plan_err,
};
use datafusion_expr::binary::BinaryTypeCoercer;
use datafusion_expr::expr::{
    self, AggregateFunctionParams, Alias, Between, BinaryExpr, Case, Exists, HigherOrderFunction,
    InList, InSubquery, Like, ScalarFunction, SetComparison, Sort, WindowFunction,
};
use datafusion_expr::expr_rewriter::{NamePreserver, coerce_plan_expr_for_schema};
use datafusion_expr::expr_schema::cast_subquery;
use datafusion_expr::logical_plan::Subquery;
use datafusion_expr::type_coercion::binary::{
    comparison_coercion, like_coercion, type_union_coercion,
};
use datafusion_expr::type_coercion::functions::{
    UDFCoercionExt, fields_with_udf, value_fields_with_higher_order_udf_and_lambdas,
};
use datafusion_expr::type_coercion::other::{
    get_coerce_type_for_case_expression, get_coerce_type_for_case_when, get_coerce_type_for_list,
};
use datafusion_expr::type_coercion::{is_datetime, is_interval, is_signed_numeric, is_timestamp};
use datafusion_expr::utils::merge_schema;
use datafusion_expr::{
    Cast, Expr, ExprSchemable, Join, Limit, LogicalPlan, Operator, Projection, Union,
    ValueOrLambda, WindowFrame, WindowFrameBound, WindowFrameUnits, is_false, is_not_false,
    is_not_true, is_not_unknown, is_true, is_unknown, lit, not,
};
use itertools::{Itertools as _, izip};

/// Performs type coercion by determining the schema
/// and performing the expression rewrites.
#[derive(Default, Debug)]
pub struct TypeCoercion {}

impl TypeCoercion {
    pub fn new() -> Self {
        Self {}
    }
}

/// Coerce output schema based upon optimizer config.
fn coerce_output(plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
    if !config.optimizer.expand_views_at_output {
        return Ok(plan);
    }

    let outer_refs = plan.expressions();
    if outer_refs.is_empty() {
        return Ok(plan);
    }

    if let Some(dfschema) = transform_schema_to_nonview(plan.schema()) {
        coerce_plan_expr_for_schema(plan, &dfschema?)
    } else {
        Ok(plan)
    }
}

impl AnalyzerRule for TypeCoercion {
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        static EMPTY_SCHEMA: LazyLock<DFSchema> = LazyLock::new(DFSchema::empty);

        // recurse
        let transformed_plan = plan
            .transform_up_with_subqueries(|plan| analyze_internal(&EMPTY_SCHEMA, plan))?
            .data;

        // finish
        coerce_output(transformed_plan, config)
    }

    fn name(&self) -> &str {
        "type_coercion"
    }
}

/// use the external schema to handle the correlated subqueries case
///
/// Assumes that children have already been optimized
fn analyze_internal(
    external_schema: &DFSchema,
    plan: LogicalPlan,
) -> Result<Transformed<LogicalPlan>> {
    // get schema representing all available input fields. This is used for data type
    // resolution only, so order does not matter here
    let mut schema = merge_schema(&plan.inputs());

    if let LogicalPlan::TableScan(ts) = &plan {
        let source_schema =
            DFSchema::try_from_qualified_schema(ts.table_name.clone(), &ts.source.schema())?;
        schema.merge(&source_schema);
    }

    // merge the outer schema for correlated subqueries
    // like case:
    // select t2.c2 from t1 where t1.c1 in (select t2.c1 from t2 where t2.c2=t1.c3)
    schema.merge(external_schema);

    // Coerce filter predicates to boolean (handles `WHERE NULL`)
    let plan = if let LogicalPlan::Filter(mut filter) = plan {
        filter.predicate = filter.predicate.cast_to(&DataType::Boolean, &schema)?;
        LogicalPlan::Filter(filter)
    } else {
        plan
    };

    let mut expr_rewrite = TypeCoercionRewriter::new(&schema);

    let name_preserver = NamePreserver::new(&plan);
    // apply coercion rewrite all expressions in the plan individually
    plan.map_expressions(|expr| {
        let original_name = name_preserver.save(&expr);
        expr.rewrite(&mut expr_rewrite)
            .map(|transformed| transformed.update_data(|e| original_name.restore(e)))
    })?
    // some plans need extra coercion after their expressions are coerced
    .map_data(|plan| expr_rewrite.coerce_plan(plan))?
    // recompute the schema after the expressions have been rewritten as the types may have changed
    .map_data(|plan| plan.recompute_schema())
}

/// Rewrite expressions to apply type coercion.
pub struct TypeCoercionRewriter<'a> {
    pub(crate) schema: &'a DFSchema,
}

impl<'a> TypeCoercionRewriter<'a> {
    /// Create a new [`TypeCoercionRewriter`] with a provided schema
    /// representing both the inputs and output of the [`LogicalPlan`] node.
    pub fn new(schema: &'a DFSchema) -> Self {
        Self { schema }
    }

    /// Coerce the [`LogicalPlan`].
    ///
    /// Refer to [`TypeCoercionRewriter::coerce_join`] and [`TypeCoercionRewriter::coerce_union`]
    /// for type-coercion approach.
    pub fn coerce_plan(&mut self, plan: LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Join(join) => self.coerce_join(join),
            LogicalPlan::Union(union) => Self::coerce_union(union),
            LogicalPlan::Limit(limit) => Self::coerce_limit(limit),
            _ => Ok(plan),
        }
    }

    /// Coerce join equality expressions and join filter
    ///
    /// Joins must be treated specially as their equality expressions are stored
    /// as a parallel list of left and right expressions, rather than a single
    /// equality expression
    ///
    /// For example, on_exprs like `t1.a = t2.b AND t1.x = t2.y` will be stored
    /// as a list of `(t1.a, t2.b), (t1.x, t2.y)`
    pub fn coerce_join(&mut self, mut join: Join) -> Result<LogicalPlan> {
        join.on = join
            .on
            .into_iter()
            .map(|(lhs, rhs)| {
                // coerce the arguments as though they were a single binary equality
                // expression
                let left_schema = join.left.schema();
                let right_schema = join.right.schema();
                let (lhs, rhs) =
                    self.coerce_binary_op(lhs, left_schema, Operator::Eq, rhs, right_schema)?;
                Ok((lhs, rhs))
            })
            .collect::<Result<Vec<_>>>()?;

        // Join filter must be boolean
        join.filter = join
            .filter
            .map(|expr| self.coerce_join_filter(expr))
            .transpose()?;

        Ok(LogicalPlan::Join(join))
    }

    /// Coerce the union’s inputs to a common schema compatible with all inputs.
    /// This occurs after wildcard expansion and the coercion of the input expressions.
    pub fn coerce_union(union_plan: Union) -> Result<LogicalPlan> {
        let union_schema = Arc::new(coerce_union_schema_with_schema(
            &union_plan.inputs,
            &union_plan.schema,
        )?);
        let new_inputs = union_plan
            .inputs
            .into_iter()
            .map(|p| {
                let plan = coerce_plan_expr_for_schema(Arc::unwrap_or_clone(p), &union_schema)?;
                match plan {
                    LogicalPlan::Projection(Projection { expr, input, .. }) => Ok(Arc::new(
                        project_with_column_index(expr, input, Arc::clone(&union_schema))?,
                    )),
                    other_plan => Ok(Arc::new(other_plan)),
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(LogicalPlan::Union(Union {
            inputs: new_inputs,
            schema: union_schema,
        }))
    }

    /// Coerce the fetch and skip expression to Int64 type.
    fn coerce_limit(limit: Limit) -> Result<LogicalPlan> {
        fn coerce_limit_expr(expr: Expr, schema: &DFSchema, expr_name: &str) -> Result<Expr> {
            let dt = expr.get_type(schema)?;
            if dt.is_integer() || dt.is_null() {
                expr.cast_to(&DataType::Int64, schema)
            } else {
                plan_err!("Expected {expr_name} to be an integer or null, but got {dt}")
            }
        }

        let empty_schema = DFSchema::empty();
        let new_fetch = limit
            .fetch
            .map(|expr| coerce_limit_expr(*expr, &empty_schema, "LIMIT"))
            .transpose()?;
        let new_skip = limit
            .skip
            .map(|expr| coerce_limit_expr(*expr, &empty_schema, "OFFSET"))
            .transpose()?;
        Ok(LogicalPlan::Limit(Limit {
            input: limit.input,
            fetch: new_fetch.map(Box::new),
            skip: new_skip.map(Box::new),
        }))
    }

    fn coerce_join_filter(&self, expr: Expr) -> Result<Expr> {
        let expr_type = expr.get_type(self.schema)?;
        match expr_type {
            DataType::Boolean => Ok(expr),
            DataType::Null => expr.cast_to(&DataType::Boolean, self.schema),
            other => plan_err!("Join condition must be boolean type, but got {other:?}"),
        }
    }

    fn coerce_binary_op(
        &self,
        left: Expr,
        left_schema: &DFSchema,
        op: Operator,
        right: Expr,
        right_schema: &DFSchema,
    ) -> Result<(Expr, Expr)> {
        let left_data_type = left.get_type(left_schema)?;
        let right_data_type = right.get_type(right_schema)?;
        let (left_type, right_type) =
            BinaryTypeCoercer::new(&left_data_type, &op, &right_data_type).get_input_types()?;
        let left_cast_ok = can_cast_types(&left_data_type, &left_type);
        let right_cast_ok = can_cast_types(&right_data_type, &right_type);

        // handle special cases for
        // * Date +/- int => Date
        // * Date + time => Timestamp
        let left_expr = if !left_cast_ok {
            Self::coerce_date_time_math_op(left, &op, &left_data_type, &left_type, &right_type)?
        } else {
            left.cast_to(&left_type, left_schema)?
        };

        let right_expr = if !right_cast_ok {
            Self::coerce_date_time_math_op(right, &op, &right_data_type, &right_type, &left_type)?
        } else {
            right.cast_to(&right_type, right_schema)?
        };

        Ok((left_expr, right_expr))
    }

    fn coerce_date_time_math_op(
        expr: Expr,
        op: &Operator,
        left_current_type: &DataType,
        left_target_type: &DataType,
        right_target_type: &DataType,
    ) -> Result<Expr, DataFusionError> {
        use DataType::*;

        fn cast(expr: Expr, target_type: DataType) -> Expr {
            Expr::Cast(Cast::new(Box::new(expr), target_type))
        }

        fn time_to_nanos(expr: Expr, expr_type: &DataType) -> Result<Expr, DataFusionError> {
            let expr = match expr_type {
                Time32(TimeUnit::Second) => {
                    cast(cast(expr, Int32), Int64) * lit(ScalarValue::Int64(Some(1_000_000_000)))
                }
                Time32(TimeUnit::Millisecond) => {
                    cast(cast(expr, Int32), Int64) * lit(ScalarValue::Int64(Some(1_000_000)))
                }
                Time64(TimeUnit::Microsecond) => {
                    cast(expr, Int64) * lit(ScalarValue::Int64(Some(1_000)))
                }
                Time64(TimeUnit::Nanosecond) => cast(expr, Int64),
                t => return internal_err!("Unexpected time data type {t}"),
            };

            Ok(expr)
        }

        let e = match (
            &op,
            &left_current_type,
            &left_target_type,
            &right_target_type,
        ) {
            // int +/- date => date
            (
                Operator::Plus | Operator::Minus,
                Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64,
                Interval(IntervalUnit::MonthDayNano),
                Date32 | Date64,
            ) => {
                // cast to i64 first
                let expr = match *left_current_type {
                    Int64 => expr,
                    _ => cast(expr, Int64),
                };
                // next, multiply by 86400 to get seconds
                let expr = expr * lit(ScalarValue::from(SECONDS_IN_DAY));
                // cast to duration
                let expr = cast(expr, Duration(TimeUnit::Second));
                // finally cast to interval
                cast(expr, Interval(IntervalUnit::MonthDayNano))
            }
            // These might seem to be a bit convoluted, however for arrow to do date + time arithmetic
            // date must be cast to Timestamp(Nanosecond) and time cast to Duration(Nanosecond)
            // (they must be the same timeunit).
            //
            // For Time32/64 we first need to cast to an Int64, convert that to nanoseconds based
            // on the time unit, then cast that to duration.
            //
            // Time + date -> timestamp or
            (
                Operator::Plus | Operator::Minus,
                Time32(_) | Time64(_),
                Duration(TimeUnit::Nanosecond),
                Timestamp(TimeUnit::Nanosecond, None),
            ) => {
                // cast to int64, convert to nanoseconds
                let expr = time_to_nanos(expr, left_current_type)?;
                // cast to duration
                cast(expr, Duration(TimeUnit::Nanosecond))
            }
            // Similar to above, for arrow to do time - time we need to convert to an interval.
            // To do that we first need to cast to an Int64, convert that to nanoseconds based
            // on the time unit, then cast that to duration, then finally cast to an interval.
            //
            // Time - time -> timestamp
            (
                Operator::Plus | Operator::Minus,
                Time32(_) | Time64(_),
                Interval(IntervalUnit::MonthDayNano),
                Interval(IntervalUnit::MonthDayNano),
            ) => {
                // cast to int64, convert to nanoseconds
                let expr = time_to_nanos(expr, left_current_type)?;
                // cast to duration
                let expr = cast(expr, Duration(TimeUnit::Nanosecond));
                // finally cast to interval
                cast(expr, Interval(IntervalUnit::MonthDayNano))
            }
            _ => {
                return plan_err!(
                    "Cannot automatically convert {left_current_type} to {left_target_type}"
                );
            }
        };

        Ok(e)
    }
}

impl TreeNodeRewriter for TypeCoercionRewriter<'_> {
    type Node = Expr;

    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        match expr {
            Expr::Unnest(_) => not_impl_err!(
                "Unnest should be rewritten to LogicalPlan::Unnest before type coercion"
            ),
            Expr::ScalarSubquery(Subquery {
                subquery,
                outer_ref_columns,
                spans,
            }) => {
                let new_plan = analyze_internal(self.schema, Arc::unwrap_or_clone(subquery))?.data;
                Ok(Transformed::yes(Expr::ScalarSubquery(Subquery {
                    subquery: Arc::new(new_plan),
                    outer_ref_columns,
                    spans,
                })))
            }
            Expr::Exists(Exists { subquery, negated }) => {
                let new_plan =
                    analyze_internal(self.schema, Arc::unwrap_or_clone(subquery.subquery))?.data;
                Ok(Transformed::yes(Expr::Exists(Exists {
                    subquery: Subquery {
                        subquery: Arc::new(new_plan),
                        outer_ref_columns: subquery.outer_ref_columns,
                        spans: subquery.spans,
                    },
                    negated,
                })))
            }
            Expr::InSubquery(InSubquery {
                expr,
                subquery,
                negated,
            }) => {
                let new_plan =
                    analyze_internal(self.schema, Arc::unwrap_or_clone(subquery.subquery))?.data;
                let expr_type = expr.get_type(self.schema)?;
                let subquery_type = new_plan.schema().field(0).data_type();
                let common_type =
                    comparison_coercion(&expr_type, subquery_type).ok_or(plan_datafusion_err!(
                        "expr type {expr_type} can't cast to {subquery_type} in InSubquery"
                    ))?;
                let new_subquery = Subquery {
                    subquery: Arc::new(new_plan),
                    outer_ref_columns: subquery.outer_ref_columns,
                    spans: subquery.spans,
                };
                Ok(Transformed::yes(Expr::InSubquery(InSubquery::new(
                    Box::new(expr.cast_to(&common_type, self.schema)?),
                    cast_subquery(new_subquery, &common_type)?,
                    negated,
                ))))
            }
            Expr::SetComparison(SetComparison {
                expr,
                subquery,
                op,
                quantifier,
            }) => {
                let new_plan =
                    analyze_internal(self.schema, Arc::unwrap_or_clone(subquery.subquery))?.data;
                let expr_type = expr.get_type(self.schema)?;
                let subquery_type = new_plan.schema().field(0).data_type();
                if (expr_type.is_numeric() && subquery_type.is_string())
                    || (subquery_type.is_numeric() && expr_type.is_string())
                {
                    return plan_err!(
                        "expr type {expr_type} can't cast to {subquery_type} in SetComparison"
                    );
                }
                let common_type =
                    comparison_coercion(&expr_type, subquery_type).ok_or(plan_datafusion_err!(
                        "expr type {expr_type} can't cast to {subquery_type} in SetComparison"
                    ))?;
                let new_subquery = Subquery {
                    subquery: Arc::new(new_plan),
                    outer_ref_columns: subquery.outer_ref_columns,
                    spans: subquery.spans,
                };
                Ok(Transformed::yes(Expr::SetComparison(SetComparison::new(
                    Box::new(expr.cast_to(&common_type, self.schema)?),
                    cast_subquery(new_subquery, &common_type)?,
                    op,
                    quantifier,
                ))))
            }
            Expr::Not(expr) => Ok(Transformed::yes(not(get_casted_expr_for_bool_op(
                *expr,
                self.schema,
            )?))),
            Expr::IsTrue(expr) => Ok(Transformed::yes(is_true(get_casted_expr_for_bool_op(
                *expr,
                self.schema,
            )?))),
            Expr::IsNotTrue(expr) => Ok(Transformed::yes(is_not_true(
                get_casted_expr_for_bool_op(*expr, self.schema)?,
            ))),
            Expr::IsFalse(expr) => Ok(Transformed::yes(is_false(get_casted_expr_for_bool_op(
                *expr,
                self.schema,
            )?))),
            Expr::IsNotFalse(expr) => Ok(Transformed::yes(is_not_false(
                get_casted_expr_for_bool_op(*expr, self.schema)?,
            ))),
            Expr::IsUnknown(expr) => Ok(Transformed::yes(is_unknown(get_casted_expr_for_bool_op(
                *expr,
                self.schema,
            )?))),
            Expr::IsNotUnknown(expr) => Ok(Transformed::yes(is_not_unknown(
                get_casted_expr_for_bool_op(*expr, self.schema)?,
            ))),
            Expr::Negative(expr) => {
                let data_type = expr.get_type(self.schema)?;
                if data_type.is_null()
                    || is_signed_numeric(&data_type)
                    || is_interval(&data_type)
                    || is_timestamp(&data_type)
                {
                    Ok(Transformed::no(Expr::Negative(expr)))
                } else {
                    plan_err!("Negation only supports numeric, interval and timestamp types")
                }
            }
            Expr::Like(Like {
                negated,
                expr,
                pattern,
                escape_char,
                case_insensitive,
            }) => {
                let left_type = expr.get_type(self.schema)?;
                let right_type = pattern.get_type(self.schema)?;
                let coerced_type = like_coercion(&left_type,  &right_type).ok_or_else(|| {
                    let op_name = if case_insensitive {
                        "ILIKE"
                    } else {
                        "LIKE"
                    };
                    plan_datafusion_err!(
                        "There isn't a common type to coerce {left_type} and {right_type} in {op_name} expression"
                    )
                })?;
                let expr = match left_type {
                    DataType::Dictionary(_, inner) if *inner == DataType::Utf8 => expr,
                    _ => Box::new(expr.cast_to(&coerced_type, self.schema)?),
                };
                let pattern = Box::new(pattern.cast_to(&coerced_type, self.schema)?);
                Ok(Transformed::yes(Expr::Like(Like::new(
                    negated,
                    expr,
                    pattern,
                    escape_char,
                    case_insensitive,
                ))))
            }
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let (left, right) =
                    self.coerce_binary_op(*left, self.schema, op, *right, self.schema)?;
                Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(left),
                    op,
                    Box::new(right),
                ))))
            }
            Expr::Between(Between {
                expr,
                negated,
                low,
                high,
            }) => {
                let expr_type = expr.get_type(self.schema)?;
                let low_type = low.get_type(self.schema)?;
                let low_coerced_type = comparison_coercion(&expr_type, &low_type)
                    .ok_or_else(|| {
                        internal_datafusion_err!(
                            "Failed to coerce types {expr_type} and {low_type} in BETWEEN expression"
                        )
                    })?;
                let high_type = high.get_type(self.schema)?;
                let high_coerced_type = comparison_coercion(&expr_type, &high_type)
                    .ok_or_else(|| {
                        internal_datafusion_err!(
                            "Failed to coerce types {expr_type} and {high_type} in BETWEEN expression"
                        )
                    })?;
                let coercion_type =
                    comparison_coercion(&low_coerced_type, &high_coerced_type)
                        .ok_or_else(|| {
                            internal_datafusion_err!(
                                "Failed to coerce types {expr_type} and {high_type} in BETWEEN expression"
                            )
                        })?;
                Ok(Transformed::yes(Expr::Between(Between::new(
                    Box::new(expr.cast_to(&coercion_type, self.schema)?),
                    negated,
                    Box::new(low.cast_to(&coercion_type, self.schema)?),
                    Box::new(high.cast_to(&coercion_type, self.schema)?),
                ))))
            }
            Expr::InList(InList {
                expr,
                list,
                negated,
            }) => {
                let expr_data_type = expr.get_type(self.schema)?;
                let list_data_types = list
                    .iter()
                    .map(|list_expr| list_expr.get_type(self.schema))
                    .collect::<Result<Vec<_>>>()?;
                let result_type = get_coerce_type_for_list(&expr_data_type, &list_data_types);
                match result_type {
                    None => plan_err!(
                        "Can not find compatible types to compare {expr_data_type} with [{}]",
                        list_data_types.iter().join(", ")
                    ),
                    Some(coerced_type) => {
                        // find the coerced type
                        let cast_expr = expr.cast_to(&coerced_type, self.schema)?;
                        let cast_list_expr = list
                            .into_iter()
                            .map(|list_expr| list_expr.cast_to(&coerced_type, self.schema))
                            .collect::<Result<Vec<_>>>()?;
                        Ok(Transformed::yes(Expr::InList(InList::new(
                            Box::new(cast_expr),
                            cast_list_expr,
                            negated,
                        ))))
                    }
                }
            }
            Expr::Case(case) => {
                let case = coerce_case_expression(case, self.schema)?;
                Ok(Transformed::yes(Expr::Case(case)))
            }
            Expr::ScalarFunction(ScalarFunction { func, args }) => {
                let new_expr = coerce_arguments_for_signature(args, self.schema, func.as_ref())?;
                Ok(Transformed::yes(Expr::ScalarFunction(
                    ScalarFunction::new_udf(func, new_expr),
                )))
            }
            Expr::AggregateFunction(expr::AggregateFunction {
                func,
                params:
                    AggregateFunctionParams {
                        args,
                        distinct,
                        filter,
                        order_by,
                        null_treatment,
                    },
            }) => {
                let new_expr = coerce_arguments_for_signature(args, self.schema, func.as_ref())?;
                Ok(Transformed::yes(Expr::AggregateFunction(
                    expr::AggregateFunction::new_udf(
                        func,
                        new_expr,
                        distinct,
                        filter,
                        order_by,
                        null_treatment,
                    ),
                )))
            }
            Expr::WindowFunction(window_fun) => {
                let WindowFunction {
                    fun,
                    params:
                        expr::WindowFunctionParams {
                            args,
                            partition_by,
                            order_by,
                            window_frame,
                            filter,
                            null_treatment,
                            distinct,
                        },
                } = *window_fun;
                let window_frame = coerce_window_frame(window_frame, self.schema, &order_by)?;

                let args = match &fun {
                    expr::WindowFunctionDefinition::AggregateUDF(udf) => {
                        coerce_arguments_for_signature(args, self.schema, udf.as_ref())?
                    }
                    expr::WindowFunctionDefinition::WindowUDF(udf) => {
                        coerce_arguments_for_signature(args, self.schema, udf.as_ref())?
                    }
                };

                let new_expr = Expr::from(WindowFunction {
                    fun,
                    params: expr::WindowFunctionParams {
                        args,
                        partition_by,
                        order_by,
                        window_frame,
                        filter,
                        null_treatment,
                        distinct,
                    },
                });
                Ok(Transformed::yes(new_expr))
            }
            Expr::HigherOrderFunction(HigherOrderFunction { func, args }) => {
                let current_fields = args
                    .iter()
                    .map(|arg| match arg {
                        Expr::Lambda(lambda) => {
                            Ok(ValueOrLambda::Lambda(lambda.body.to_field(self.schema)?.1))
                        }
                        _ => Ok(ValueOrLambda::Value(arg.to_field(self.schema)?.1)),
                    })
                    .collect::<Result<Vec<_>>>()?;

                let new_fields =
                    value_fields_with_higher_order_udf_and_lambdas(&current_fields, func.as_ref())?;

                let new_args = std::iter::zip(args, new_fields)
                    .map(|(arg, new_field)| match (&arg, new_field) {
                        (Expr::Lambda(_lambda), ValueOrLambda::Lambda(_)) => Ok(arg),
                        (Expr::Lambda(_lambda), ValueOrLambda::Value(_)) => internal_err!("value_fields_with_higher_order_udf returned a value for a lambda argument"),
                        (_, ValueOrLambda::Value(new_field)) => arg.cast_to(new_field.data_type(), self.schema),
                        (_, ValueOrLambda::Lambda(_)) => internal_err!("value_fields_with_higher_order_udf returned a lambda for a value argument"),
                    })
                    .collect::<Result<_>>()?;

                Ok(Transformed::yes(Expr::HigherOrderFunction(
                    HigherOrderFunction::new(func, new_args),
                )))
            }
            // TODO: remove the next line after `Expr::Wildcard` is removed
            #[expect(deprecated)]
            Expr::Alias(_)
            | Expr::Column(_)
            | Expr::ScalarVariable(_, _)
            | Expr::Literal(_, _)
            | Expr::SimilarTo(_)
            | Expr::IsNotNull(_)
            | Expr::IsNull(_)
            | Expr::Cast(_)
            | Expr::TryCast(_)
            | Expr::Wildcard { .. }
            | Expr::GroupingSet(_)
            | Expr::Placeholder(_)
            | Expr::OuterReferenceColumn(_, _)
            | Expr::Lambda(_)
            | Expr::LambdaVariable(_) => Ok(Transformed::no(expr)),
        }
    }
}

/// Transform a schema to use non-view types for Utf8View and BinaryView
fn transform_schema_to_nonview(dfschema: &DFSchemaRef) -> Option<Result<DFSchema>> {
    let metadata = dfschema.as_arrow().metadata.clone();
    let mut transformed = false;

    let (qualifiers, transformed_fields): (Vec<Option<TableReference>>, Vec<Arc<Field>>) = dfschema
        .iter()
        .map(|(qualifier, field)| match field.data_type() {
            DataType::Utf8View => {
                transformed = true;
                (
                    qualifier.cloned(),
                    Arc::new(Field::new(
                        field.name(),
                        DataType::LargeUtf8,
                        field.is_nullable(),
                    )),
                )
            }
            DataType::BinaryView => {
                transformed = true;
                (
                    qualifier.cloned(),
                    Arc::new(Field::new(
                        field.name(),
                        DataType::LargeBinary,
                        field.is_nullable(),
                    )),
                )
            }
            _ => (qualifier.cloned(), Arc::clone(field)),
        })
        .unzip();

    if !transformed {
        return None;
    }

    let schema = Schema::new_with_metadata(transformed_fields, metadata);
    Some(DFSchema::from_field_specific_qualified_schema(
        qualifiers,
        &Arc::new(schema),
    ))
}

/// Casts the given `value` to `target_type`. Note that this function
/// only considers `Null` or `Utf8` values.
fn coerce_scalar(target_type: &DataType, value: &ScalarValue) -> Result<ScalarValue> {
    match value {
        // Coerce Utf8 values:
        ScalarValue::Utf8(Some(val)) => ScalarValue::try_from_string(val.clone(), target_type),
        s => {
            if s.is_null() {
                // Coerce `Null` values:
                ScalarValue::try_from(target_type)
            } else {
                // Values except `Utf8`/`Null` variants already have the right type
                // (casted before) since we convert `sqlparser` outputs to `Utf8`
                // for all possible cases. Therefore, we return a clone here.
                Ok(s.clone())
            }
        }
    }
}

/// This function coerces `value` to `target_type` in a range-aware fashion.
/// If the coercion is successful, we return an `Ok` value with the result.
/// If the coercion fails because `target_type` is not wide enough (i.e. we
/// can not coerce to `target_type`, but we can to a wider type in the same
/// family), we return a `Null` value of this type to signal this situation.
/// Downstream code uses this signal to treat these values as *unbounded*.
fn coerce_scalar_range_aware(target_type: &DataType, value: &ScalarValue) -> Result<ScalarValue> {
    coerce_scalar(target_type, value).or_else(|err| {
        // If type coercion fails, check if the largest type in family works:
        if let Some(largest_type) = get_widest_type_in_family(target_type) {
            coerce_scalar(largest_type, value).map_or_else(
                |_| exec_err!("Cannot cast {value} to {target_type}"),
                |_| ScalarValue::try_from(target_type),
            )
        } else {
            Err(err)
        }
    })
}

/// This function returns the widest type in the family of `given_type`.
/// If the given type is already the widest type, it returns `None`.
/// For example, if `given_type` is `Int8`, it returns `Int64`.
fn get_widest_type_in_family(given_type: &DataType) -> Option<&DataType> {
    match given_type {
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => Some(&DataType::UInt64),
        DataType::Int8 | DataType::Int16 | DataType::Int32 => Some(&DataType::Int64),
        DataType::Float16 | DataType::Float32 => Some(&DataType::Float64),
        _ => None,
    }
}

/// Coerces the given (window frame) `bound` to `target_type`.
fn coerce_frame_bound(target_type: &DataType, bound: WindowFrameBound) -> Result<WindowFrameBound> {
    match bound {
        WindowFrameBound::Preceding(v) => {
            coerce_scalar_range_aware(target_type, &v).map(WindowFrameBound::Preceding)
        }
        WindowFrameBound::CurrentRow => Ok(WindowFrameBound::CurrentRow),
        WindowFrameBound::Following(v) => {
            coerce_scalar_range_aware(target_type, &v).map(WindowFrameBound::Following)
        }
    }
}

fn extract_window_frame_target_type(col_type: &DataType) -> Result<DataType> {
    if col_type.is_numeric()
        || col_type.is_string()
        || col_type.is_null()
        || matches!(
            col_type,
            DataType::List(_)
                | DataType::LargeList(_)
                | DataType::FixedSizeList(_, _)
                | DataType::Boolean
        )
    {
        Ok(col_type.clone())
    } else if is_datetime(col_type) {
        Ok(DataType::Interval(IntervalUnit::MonthDayNano))
    } else if let DataType::Dictionary(_, value_type) = col_type {
        extract_window_frame_target_type(value_type)
    } else {
        internal_err!("Cannot run range queries on datatype: {col_type}")
    }
}

// Coerces the given `window_frame` to use appropriate natural types.
// For example, ROWS and GROUPS frames use `UInt64` during calculations.
fn coerce_window_frame(
    window_frame: WindowFrame,
    schema: &DFSchema,
    expressions: &[Sort],
) -> Result<WindowFrame> {
    let mut window_frame = window_frame;
    let target_type = match window_frame.units {
        WindowFrameUnits::Range => {
            let current_types = expressions
                .first()
                .map(|s| s.expr.get_type(schema))
                .transpose()?;
            if let Some(col_type) = current_types {
                extract_window_frame_target_type(&col_type)?
            } else {
                return internal_err!("ORDER BY column cannot be empty");
            }
        }
        WindowFrameUnits::Rows | WindowFrameUnits::Groups => DataType::UInt64,
    };
    window_frame.start_bound = coerce_frame_bound(&target_type, window_frame.start_bound)?;
    window_frame.end_bound = coerce_frame_bound(&target_type, window_frame.end_bound)?;
    Ok(window_frame)
}

// Support the `IsTrue` `IsNotTrue` `IsFalse` `IsNotFalse` type coercion.
// The above op will be rewrite to the binary op when creating the physical op.
fn get_casted_expr_for_bool_op(expr: Expr, schema: &DFSchema) -> Result<Expr> {
    let left_type = expr.get_type(schema)?;
    BinaryTypeCoercer::new(&left_type, &Operator::IsDistinctFrom, &DataType::Boolean)
        .get_input_types()?;
    expr.cast_to(&DataType::Boolean, schema)
}

/// Returns `expressions` coerced to types compatible with
/// `signature`, if possible.
///
/// See the module level documentation for more detail on coercion.
fn coerce_arguments_for_signature<F: UDFCoercionExt>(
    expressions: Vec<Expr>,
    schema: &DFSchema,
    func: &F,
) -> Result<Vec<Expr>> {
    let current_fields = expressions
        .iter()
        .map(|e| e.to_field(schema).map(|(_, f)| f))
        .collect::<Result<Vec<_>>>()?;

    let coerced_types = fields_with_udf(&current_fields, func)?
        .into_iter()
        .map(|f| f.data_type().clone())
        .collect::<Vec<_>>();

    expressions
        .into_iter()
        .enumerate()
        .map(|(i, expr)| expr.cast_to(&coerced_types[i], schema))
        .collect()
}

fn coerce_case_expression(case: Case, schema: &DFSchema) -> Result<Case> {
    // Given expressions like:
    //
    // CASE a1
    //   WHEN a2 THEN b1
    //   WHEN a3 THEN b2
    //   ELSE b3
    // END
    //
    // or:
    //
    // CASE
    //   WHEN x1 THEN b1
    //   WHEN x2 THEN b2
    //   ELSE b3
    // END
    //
    // Then all aN (a1, a2, a3) must be converted to a common data type in the first example
    // (case-when expression coercion)
    //
    // All xN (x1, x2) must be converted to a boolean data type in the second example
    // (when-boolean expression coercion)
    //
    // And all bN (b1, b2, b3) must be converted to a common data type in both examples
    // (then-else expression coercion)
    //
    // If any fail to find and cast to a common/specific data type, will return error
    //
    // Note that case-when and when-boolean expression coercions are mutually exclusive
    // Only one or the other can occur for a case expression, whilst then-else expression coercion will always occur

    // prepare types
    let case_type = case
        .expr
        .as_ref()
        .map(|expr| expr.get_type(schema))
        .transpose()?;
    let then_types = case
        .when_then_expr
        .iter()
        .map(|(_when, then)| then.get_type(schema))
        .collect::<Result<Vec<_>>>()?;
    let else_type = case
        .else_expr
        .as_ref()
        .map(|expr| expr.get_type(schema))
        .transpose()?;

    // find common coercible types
    let case_when_coerce_type = case_type
        .as_ref()
        .map(|case_type| {
            let when_types = case
                .when_then_expr
                .iter()
                .map(|(when, _then)| when.get_type(schema))
                .collect::<Result<Vec<_>>>()?;
            let coerced_type = get_coerce_type_for_case_when(&when_types, case_type);
            coerced_type.ok_or_else(|| {
                plan_datafusion_err!(
                    "Failed to coerce case ({case_type}) and when ({}) \
                     to common types in CASE WHEN expression",
                    when_types.iter().join(", ")
                )
            })
        })
        .transpose()?;
    let then_else_coerce_type =
        get_coerce_type_for_case_expression(&then_types, else_type.as_ref()).ok_or_else(|| {
            if let Some(else_type) = else_type {
                plan_datafusion_err!(
                    "Failed to coerce then ({}) and else ({else_type}) \
                         to common types in CASE WHEN expression",
                    then_types.iter().join(", ")
                )
            } else {
                plan_datafusion_err!(
                    "Failed to coerce then ({}) and else (None) \
                         to common types in CASE WHEN expression",
                    then_types.iter().join(", ")
                )
            }
        })?;

    // do cast if found common coercible types
    let case_expr = case
        .expr
        .zip(case_when_coerce_type.as_ref())
        .map(|(case_expr, coercible_type)| case_expr.cast_to(coercible_type, schema))
        .transpose()?
        .map(Box::new);
    let when_then = case
        .when_then_expr
        .into_iter()
        .map(|(when, then)| {
            let when_type = case_when_coerce_type.as_ref().unwrap_or(&DataType::Boolean);
            let when = when.cast_to(when_type, schema).map_err(|e| {
                DataFusionError::Context(
                    format!(
                        "WHEN expressions in CASE couldn't be \
                         converted to common type ({when_type})"
                    ),
                    Box::new(e),
                )
            })?;
            let then = then.cast_to(&then_else_coerce_type, schema)?;
            Ok((Box::new(when), Box::new(then)))
        })
        .collect::<Result<Vec<_>>>()?;
    let else_expr = case
        .else_expr
        .map(|expr| expr.cast_to(&then_else_coerce_type, schema))
        .transpose()?
        .map(Box::new);

    Ok(Case::new(case_expr, when_then, else_expr))
}

/// Get a common schema that is compatible with all inputs of UNION.
///
/// This method presumes that the wildcard expansion is unneeded, or has already
/// been applied.
///
/// ## Schema and Field Handling in Union Coercion
///
/// **Processing order**: The function starts with the base schema (first input) and then
/// processes remaining inputs sequentially, with later inputs taking precedence in merging.
///
/// **Schema-level metadata merging**: Later schemas take precedence for duplicate keys.
///
/// **Field-level metadata merging**: Later fields take precedence for duplicate metadata keys.
///
/// **Type coercion precedence**: The coerced type is determined by iteratively applying
/// `type_union_coercion()` between the accumulated type and each new input's type. The
/// result depends on type coercion rules, not input order.
///
/// **Nullability merging**: Nullability is accumulated using logical OR (`||`).
/// Once any input field is nullable, the result field becomes nullable permanently.
/// Later inputs can make a field nullable but cannot make it non-nullable.
///
/// **Field precedence**: Field names come from the first (base) schema, but the field properties
/// (nullability and field-level metadata) have later schemas taking precedence.
///
/// **Example**:
/// ```sql
/// SELECT a, b FROM table1  -- a: Int32, metadata {"source": "t1"}, nullable=false
/// UNION
/// SELECT a, b FROM table2  -- a: Int64, metadata {"source": "t2"}, nullable=true
/// UNION
/// SELECT a, b FROM table3  -- a: Int32, metadata {"encoding": "utf8"}, nullable=false
/// -- Result:
/// -- a: Int64 (from type coercion), nullable=true (from table2),
/// -- metadata: {"source": "t2", "encoding": "utf8"} (later inputs take precedence)
/// ```
///
/// **Precedence Summary**:
/// - **Datatypes**: Determined by `type_union_coercion()` rules, not input order
/// - **Nullability**: Later inputs can add nullability but cannot remove it (logical OR)
/// - **Metadata**: Later inputs take precedence for same keys (HashMap::extend semantics)
fn coerce_union_schema_with_schema(
    inputs: &[Arc<LogicalPlan>],
    base_schema: &DFSchemaRef,
) -> Result<DFSchema> {
    let mut union_datatypes = base_schema
        .fields()
        .iter()
        .map(|f| f.data_type().clone())
        .collect::<Vec<_>>();
    let mut union_nullabilities = base_schema
        .fields()
        .iter()
        .map(|f| f.is_nullable())
        .collect::<Vec<_>>();
    let mut union_field_meta = base_schema
        .fields()
        .iter()
        .map(|f| f.metadata().clone())
        .collect::<Vec<_>>();

    let mut metadata = base_schema.metadata().clone();

    for (i, plan) in inputs.iter().enumerate() {
        let plan_schema = plan.schema();
        metadata.extend(plan_schema.metadata().clone());

        if plan_schema.fields().len() != base_schema.fields().len() {
            return plan_err!(
                "Union schemas have different number of fields: \
                query 1 has {} fields whereas query {} has {} fields",
                base_schema.fields().len(),
                i + 1,
                plan_schema.fields().len()
            );
        }

        // coerce data type and nullability for each field
        for (union_datatype, union_nullable, union_field_map, plan_field) in izip!(
            union_datatypes.iter_mut(),
            union_nullabilities.iter_mut(),
            union_field_meta.iter_mut(),
            plan_schema.fields().iter()
        ) {
            let coerced_type = type_union_coercion(union_datatype, plan_field.data_type())
                .ok_or_else(|| {
                    plan_datafusion_err!(
                        "Incompatible inputs for Union: Previous inputs were \
                            of type {}, but got incompatible type {} on column '{}'",
                        union_datatype,
                        plan_field.data_type(),
                        plan_field.name()
                    )
                })?;

            *union_datatype = coerced_type;
            *union_nullable = *union_nullable || plan_field.is_nullable();
            union_field_map.extend(plan_field.metadata().clone());
        }
    }
    let union_qualified_fields = izip!(
        base_schema.fields(),
        union_datatypes.into_iter(),
        union_nullabilities,
        union_field_meta.into_iter()
    )
    .map(|(field, datatype, nullable, metadata)| {
        let mut field = Field::new(field.name().clone(), datatype, nullable);
        field.set_metadata(metadata);
        (None, field.into())
    })
    .collect::<Vec<_>>();

    DFSchema::new_with_metadata(union_qualified_fields, metadata)
}

/// See `<https://github.com/apache/datafusion/pull/2108>`
fn project_with_column_index(
    expr: Vec<Expr>,
    input: Arc<LogicalPlan>,
    schema: DFSchemaRef,
) -> Result<LogicalPlan> {
    let alias_expr = expr
        .into_iter()
        .enumerate()
        .map(|(i, e)| match e {
            Expr::Alias(Alias { ref name, .. }) if name != schema.field(i).name() => {
                Ok(e.unalias().alias(schema.field(i).name()))
            }
            Expr::Column(Column {
                relation: _,
                ref name,
                spans: _,
            }) if name != schema.field(i).name() => Ok(e.alias(schema.field(i).name())),
            Expr::Alias { .. } | Expr::Column { .. } => Ok(e),
            #[expect(deprecated)]
            Expr::Wildcard { .. } => {
                plan_err!("Wildcard should be expanded before type coercion")
            }
            _ => Ok(e.alias(schema.field(i).name())),
        })
        .collect::<Result<Vec<_>>>()?;

    Projection::try_new_with_schema(alias_expr, input, schema).map(LogicalPlan::Projection)
}
