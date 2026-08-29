use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use datafusion::logical_expr::expr::NullTreatment;
use datafusion::prelude::SessionContext;
use datafusion_common::tree_node::TreeNode;
use datafusion_common::{DFSchemaRef, ScalarValue};
use datafusion_expr::expr::{AggregateFunction, AggregateFunctionParams, WindowFunctionParams};
use datafusion_expr::{
    AggregateUDF, BinaryExpr, ExprSchemable, Operator, ScalarUDF, ScalarUDFImpl, WindowFrame,
    WindowFunctionDefinition, WindowUDF, cast, expr, lit,
};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_common_datafusion::variant::is_variant_storage_type;
use sail_function::scalar::variant::spark_cast_to_variant::SparkCastToVariant;
use sail_function::sketch::{DEFAULT_HLL_LG_CONFIG_K, DEFAULT_THETA_LG_NOM_ENTRIES};
use sail_python_udf::udf::pyspark_batch_collector::PySparkBatchCollectorUDF;
use sail_python_udf::udf::pyspark_cogroup_map_udf::PySparkCoGroupMapUDF;
use sail_python_udf::udf::pyspark_group_map_udf::PySparkGroupMapUDF;
use sail_python_udf::udf::pyspark_udaf::PySparkGroupAggregateUDF;
use sail_python_udf::udf::pyspark_udf::PySparkUDF;
use sail_python_udf::udf::pyspark_unresolved_udf::PySparkUnresolvedUDF;

use crate::config::PlanConfig;
use crate::error::{IntoPlanResult, PlanError, PlanResult};

pub struct FunctionContextInput<'a> {
    /// The names of function arguments.
    /// Most functions do not need this information, so it is
    /// passed as `&[String]` rather than `Vec<String>` to avoid unnecessary clone.
    /// These are the display names from the [`crate::resolver::expression::NamedExpr`]s,
    /// not to be confused with named function arguments
    /// (e.g., not like encode(charset => "utf-8", expr => "abc")).
    pub argument_display_names: &'a [String],
    pub plan_config: &'a Arc<PlanConfig>,
    pub session_context: &'a SessionContext,
    pub schema: &'a DFSchemaRef,
}

pub struct ScalarFunctionInput<'a> {
    pub arguments: Vec<expr::Expr>,
    pub function_context: FunctionContextInput<'a>,
}

pub(crate) type ScalarFunction =
    Arc<dyn Fn(ScalarFunctionInput) -> PlanResult<expr::Expr> + Send + Sync>;

pub(crate) struct ScalarFunctionBuilder;

impl ScalarFunctionBuilder {
    pub fn nullary<F, R>(f: F) -> ScalarFunction
    where
        F: Fn() -> R + Send + Sync + 'static,
        R: IntoPlanResult<expr::Expr>,
    {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| {
                arguments.zero()?;
                f().into_plan_result()
            },
        )
    }

    pub fn unary<F, R>(f: F) -> ScalarFunction
    where
        F: Fn(expr::Expr) -> R + Send + Sync + 'static,
        R: IntoPlanResult<expr::Expr>,
    {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| f(arguments.one()?).into_plan_result(),
        )
    }

    pub fn binary<F, R>(f: F) -> ScalarFunction
    where
        F: Fn(expr::Expr, expr::Expr) -> R + Send + Sync + 'static,
        R: IntoPlanResult<expr::Expr>,
    {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| {
                let (left, right) = arguments.two()?;
                f(left, right).into_plan_result()
            },
        )
    }

    pub fn ternary<F, R>(f: F) -> ScalarFunction
    where
        F: Fn(expr::Expr, expr::Expr, expr::Expr) -> R + Send + Sync + 'static,
        R: IntoPlanResult<expr::Expr>,
    {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| {
                let (first, second, third) = arguments.three()?;
                f(first, second, third).into_plan_result()
            },
        )
    }

    pub fn quaternary<F, R>(f: F) -> ScalarFunction
    where
        F: Fn(expr::Expr, expr::Expr, expr::Expr, expr::Expr) -> R + Send + Sync + 'static,
        R: IntoPlanResult<expr::Expr>,
    {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| {
                let (first, second, third, fourth) = arguments.four()?;
                f(first, second, third, fourth).into_plan_result()
            },
        )
    }

    pub fn var_arg<F, R>(f: F) -> ScalarFunction
    where
        F: Fn(Vec<expr::Expr>) -> R + Send + Sync + 'static,
        R: IntoPlanResult<expr::Expr>,
    {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| f(arguments).into_plan_result(),
        )
    }

    pub fn binary_op(op: Operator) -> ScalarFunction {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| {
                let (left, right) = arguments.two()?;
                Ok(expr::Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                }))
            },
        )
    }

    pub fn cast(data_type: DataType) -> ScalarFunction {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| { Ok(cast(arguments.one()?, data_type.clone())) },
        )
    }

    pub fn udf<F>(f: F) -> ScalarFunction
    where
        F: ScalarUDFImpl + Send + Sync + 'static,
    {
        let func = ScalarUDF::from(f);
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| { Ok(func.call(arguments)) },
        )
    }

    #[expect(dead_code)]
    pub fn scalar_udf<F>(f: F) -> ScalarFunction
    where
        F: Fn() -> Arc<ScalarUDF> + Send + Sync + 'static,
    {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| { Ok(f().call(arguments)) },
        )
    }

    pub fn custom<F>(f: F) -> ScalarFunction
    where
        F: Fn(ScalarFunctionInput) -> PlanResult<expr::Expr> + Send + Sync + 'static,
    {
        Arc::new(f)
    }

    pub fn unknown(name: &str) -> ScalarFunction {
        let name = name.to_string();
        Arc::new(move |_| Err(PlanError::todo(format!("function: {name}"))))
    }
}

/// Aggregate function input components, excluding the function name.
///
/// Populated by the resolver from `spec::UnresolvedFunction` after resolving spec expressions
/// to DataFusion expressions. Used by aggregate function builders to produce the final
/// DataFusion aggregate expression.
pub struct AggFunctionInput<'a> {
    pub arguments: Vec<expr::Expr>,
    pub distinct: bool,
    pub ignore_nulls: Option<bool>,
    pub filter: Option<Box<expr::Expr>>,
    pub order_by: Vec<expr::Sort>,
    pub preserve_count_argument_columns: bool,
    pub function_context: FunctionContextInput<'a>,
}

/// Builds a DataFusion aggregate expression from resolved function components.
///
/// Takes the resolved arguments, modifiers (DISTINCT, FILTER, ORDER BY), and context,
/// and produces a `datafusion_expr::Expr::AggregateFunction` ready for inclusion in the logical plan.
pub(crate) type AggFunction = Arc<dyn Fn(AggFunctionInput) -> PlanResult<expr::Expr> + Send + Sync>;

/// Factory methods for creating `AggFunction`s.
///
/// Provides different ways to build aggregate function handlers:
/// - `default`: Use an existing DataFusion UDAF directly
/// - `custom`: Provide custom logic for building the expression
/// - `unknown`: Placeholder for unimplemented functions
pub(crate) struct AggFunctionBuilder;

impl AggFunctionBuilder {
    /// Converts a DataFusion UDAF factory into an AggFunction.
    ///
    /// Passes all resolved components through directly with no transformation.
    pub fn default<F>(f: F) -> AggFunction
    where
        F: Fn() -> Arc<AggregateUDF> + Send + Sync + 'static,
    {
        Arc::new(move |input| {
            let AggFunctionInput {
                arguments,
                distinct,
                ignore_nulls,
                filter,
                order_by,
                preserve_count_argument_columns: _,
                function_context: _function_context,
            } = input;
            let null_treatment = get_null_treatment(ignore_nulls);
            Ok(expr::Expr::AggregateFunction(AggregateFunction {
                func: f(),
                params: AggregateFunctionParams {
                    args: arguments,
                    distinct,
                    filter,
                    order_by,
                    null_treatment,
                },
            }))
        })
    }

    /// Wraps a custom function that builds the aggregate expression with full control.
    ///
    /// Use when the function needs special argument handling that `default` cannot provide.
    pub fn custom<F>(f: F) -> AggFunction
    where
        F: Fn(AggFunctionInput) -> PlanResult<expr::Expr> + Send + Sync + 'static,
    {
        Arc::new(f)
    }

    pub fn unknown(name: &str) -> AggFunction {
        let name = name.to_string();
        Arc::new(move |_| Err(PlanError::todo(format!("function: {name}"))))
    }
}

pub struct WinFunctionInput<'a> {
    pub arguments: Vec<expr::Expr>,
    pub partition_by: Vec<expr::Expr>,
    pub order_by: Vec<expr::Sort>,
    pub window_frame: WindowFrame,
    pub ignore_nulls: Option<bool>,
    pub distinct: bool,
    pub function_context: FunctionContextInput<'a>,
}

pub(crate) type WinFunction = Arc<dyn Fn(WinFunctionInput) -> PlanResult<expr::Expr> + Send + Sync>;

pub(crate) struct WinFunctionBuilder;

impl WinFunctionBuilder {
    pub fn aggregate<F>(f: F) -> WinFunction
    where
        F: Fn() -> Arc<AggregateUDF> + Send + Sync + 'static,
    {
        Arc::new(move |input| {
            let WinFunctionInput {
                arguments,
                partition_by,
                order_by,
                window_frame,
                ignore_nulls,
                distinct,
                function_context: _function_context,
            } = input;
            let null_treatment = get_null_treatment(ignore_nulls);
            Ok(expr::Expr::WindowFunction(Box::new(expr::WindowFunction {
                fun: WindowFunctionDefinition::AggregateUDF(f()),
                params: WindowFunctionParams {
                    args: arguments,
                    partition_by,
                    order_by,
                    window_frame,
                    filter: None,
                    null_treatment,
                    distinct,
                },
            })))
        })
    }

    pub fn window<F>(f: F) -> WinFunction
    where
        F: Fn() -> Arc<WindowUDF> + Send + Sync + 'static,
    {
        Arc::new(move |input| {
            let WinFunctionInput {
                arguments,
                partition_by,
                order_by,
                window_frame,
                ignore_nulls,
                distinct,
                function_context,
            } = input;
            let null_treatment = get_null_treatment(ignore_nulls);
            let win_func_expr = expr::Expr::WindowFunction(Box::new(expr::WindowFunction {
                fun: WindowFunctionDefinition::WindowUDF(f()),
                params: WindowFunctionParams {
                    args: arguments,
                    partition_by,
                    order_by,
                    window_frame,
                    filter: None,
                    null_treatment,
                    distinct,
                },
            }));
            Ok(match win_func_expr.get_type(function_context.schema)? {
                DataType::UInt64 => cast(win_func_expr.clone(), DataType::Int32),
                _ => win_func_expr,
            })
        })
    }

    pub fn custom<F>(f: F) -> WinFunction
    where
        F: Fn(WinFunctionInput) -> PlanResult<expr::Expr> + Send + Sync + 'static,
    {
        Arc::new(f)
    }

    pub fn unknown(name: &str) -> WinFunction {
        let name = name.to_string();
        Arc::new(move |_| Err(PlanError::todo(format!("unknown window function: {name}"))))
    }
}

pub(crate) fn get_null_treatment(ignore_nulls: Option<bool>) -> Option<NullTreatment> {
    match ignore_nulls {
        Some(true) => Some(NullTreatment::IgnoreNulls),
        Some(false) => Some(NullTreatment::RespectNulls),
        None => None,
    }
}

pub(crate) fn get_arguments_and_null_treatment(
    args: Vec<expr::Expr>,
    ignore_nulls: Option<bool>,
) -> PlanResult<(Vec<expr::Expr>, Option<NullTreatment>)> {
    if args.len() == 1 {
        let expr = args.one()?;
        Ok((vec![expr], get_null_treatment(ignore_nulls)))
    } else if args.len() == 2 {
        if ignore_nulls.is_some() {
            return Err(PlanError::invalid(
                "arguments conflict with IGNORE NULLS clause",
            ));
        }
        let (expr, ignore_nulls) = args.two()?;
        let null_treatment = match ignore_nulls {
            expr::Expr::Literal(ScalarValue::Boolean(Some(ignore_nulls)), _metadata) => {
                if ignore_nulls {
                    Some(NullTreatment::IgnoreNulls)
                } else {
                    Some(NullTreatment::RespectNulls)
                }
            }
            _ => {
                return Err(PlanError::invalid(
                    "requires a boolean literal as the second argument",
                ));
            }
        };
        Ok((vec![expr], null_treatment))
    } else {
        Err(PlanError::invalid("requires 1 or 2 arguments"))
    }
}

pub(crate) fn hll_args_with_default_lg(
    arguments: Vec<expr::Expr>,
    function_name: &str,
) -> PlanResult<Vec<expr::Expr>> {
    match arguments.len() {
        1 => {
            let value = arguments.one()?;
            Ok(vec![value, lit(DEFAULT_HLL_LG_CONFIG_K)])
        }
        2 => {
            let (value, lg_config_k) = arguments.two()?;
            Ok(vec![value, cast(lg_config_k, DataType::Int32)])
        }
        count => Err(PlanError::invalid(format!(
            "{function_name} requires 1 or 2 arguments, got {count}"
        ))),
    }
}

pub(crate) fn hll_union_args_with_default_allow_different_lg(
    arguments: Vec<expr::Expr>,
) -> PlanResult<Vec<expr::Expr>> {
    match arguments.len() {
        1 => {
            let value = arguments.one()?;
            Ok(vec![value, lit(false)])
        }
        2 => {
            let (value, allow_different_lg_config_k) = arguments.two()?;
            Ok(vec![
                value,
                cast(allow_different_lg_config_k, DataType::Boolean),
            ])
        }
        count => Err(PlanError::invalid(format!(
            "hll_union_agg requires 1 or 2 arguments, got {count}"
        ))),
    }
}

pub(crate) fn count_min_sketch_args(arguments: Vec<expr::Expr>) -> PlanResult<Vec<expr::Expr>> {
    match arguments.len() {
        4 => {
            let (value, eps, confidence, seed) = arguments.four()?;
            // Spark requires `eps` and `confidence` to be DOUBLE literals and rejects
            // other types (including DECIMAL/FLOAT). Pass them through unchanged so the
            // aggregate's type validation enforces the same rule, instead of casting and
            // silently accepting types Spark rejects.
            Ok(vec![value, eps, confidence, seed])
        }
        count => Err(PlanError::invalid(format!(
            "count_min_sketch requires 4 arguments, got {count}"
        ))),
    }
}

pub(crate) fn theta_args_with_default_lg(
    arguments: Vec<expr::Expr>,
    function_name: &str,
) -> PlanResult<Vec<expr::Expr>> {
    match arguments.len() {
        1 => {
            let value = arguments.one()?;
            Ok(vec![value, lit(DEFAULT_THETA_LG_NOM_ENTRIES)])
        }
        2 => {
            let (value, lg_nom_entries) = arguments.two()?;
            Ok(vec![value, cast(lg_nom_entries, DataType::Int32)])
        }
        count => Err(PlanError::invalid(format!(
            "{function_name} requires 1 or 2 arguments, got {count}"
        ))),
    }
}

pub fn expr_contains_python_udf(body: &expr::Expr) -> PlanResult<bool> {
    Ok(body.exists(|expression| {
        Ok(match expression {
            expr::Expr::ScalarFunction(function) => {
                let f = function.func.inner();
                f.is::<PySparkUDF>()
                    || f.is::<PySparkUnresolvedUDF>()
                    || f.is::<PySparkCoGroupMapUDF>()
            }
            expr::Expr::AggregateFunction(function) => {
                let f = function.func.inner();
                f.is::<PySparkGroupAggregateUDF>()
                    || f.is::<PySparkGroupMapUDF>()
                    || f.is::<PySparkBatchCollectorUDF>()
            }
            expr::Expr::WindowFunction(window) => matches!(
                &window.fun,
                WindowFunctionDefinition::AggregateUDF(udf)
                    if udf.inner().is::<PySparkGroupAggregateUDF>()
            ),
            _ => false,
        })
    })?)
}

// TODO: Match Catalyst constant folding and NullPropagation before extracting opaque
//  Python or Variant scalar calls, so a foldable NULL can eliminate them.
pub fn expr_contains_spark_cast_to_variant(body: &expr::Expr) -> PlanResult<bool> {
    Ok(body.exists(|expression| {
        Ok(matches!(
            expression,
            expr::Expr::ScalarFunction(function)
                if function.func.inner().is::<SparkCastToVariant>()
        ))
    })?)
}

/// The Spark type name (`INT`, `STRING`, `INTERVAL DAY TO SECOND`, ...), for error messages that
/// quote operand types rather than leaking Arrow's `Debug` (`Int32`, `Utf8`, `Interval(...)`).
pub(crate) fn spark_type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => {
            format!("DECIMAL({precision},{scale})")
        }
        DataType::Int8 => "TINYINT".to_string(),
        DataType::Int16 => "SMALLINT".to_string(),
        DataType::Int32 => "INT".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        DataType::Float32 => "FLOAT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        // Spark has no unsigned or half-float type, but Sail can surface them (e.g. from
        // Parquet) and the caller's gate is `is_numeric()`, which admits them. Name them
        // the way the plan formatter does rather than leaking Arrow's `Debug`.
        DataType::UInt8 => "UNSIGNED TINYINT".to_string(),
        DataType::UInt16 => "UNSIGNED SMALLINT".to_string(),
        DataType::UInt32 => "UNSIGNED INT".to_string(),
        DataType::UInt64 => "UNSIGNED BIGINT".to_string(),
        DataType::Float16 => "HALF FLOAT".to_string(),
        // The non-numeric types the arithmetic operand-reject error surfaces, named the Spark
        // way rather than leaking Arrow's `Debug` (`Utf8`, `Boolean`, `Interval(...)`).
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "STRING".to_string(),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "BINARY".to_string(),
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Timestamp(_, Some(_)) => "TIMESTAMP".to_string(),
        DataType::Timestamp(_, None) => "TIMESTAMP_NTZ".to_string(),
        // Spark's `TimeType.typeName` is `time($precision)` (`TimeType.scala:45`), so the
        // precision is part of the name: Arrow's time unit maps to 0/3/6/9 digits.
        DataType::Time32(unit) | DataType::Time64(unit) => {
            let precision = match unit {
                TimeUnit::Second => 0,
                TimeUnit::Millisecond => 3,
                TimeUnit::Microsecond => 6,
                TimeUnit::Nanosecond => 9,
            };
            format!("TIME({precision})")
        }
        DataType::Interval(IntervalUnit::YearMonth) => "INTERVAL YEAR TO MONTH".to_string(),
        // Spark's legacy CalendarIntervalType, which `operand_role` also keeps apart from the
        // ANSI day-time interval. `CalendarIntervalType.typeName` is plain `interval`
        // (`CalendarIntervalType.scala:40`), so naming it `INTERVAL DAY TO SECOND` would make the
        // two tables this change adds disagree about the one type it exists to distinguish.
        DataType::Interval(_) => "INTERVAL".to_string(),
        DataType::Duration(_) => "INTERVAL DAY TO SECOND".to_string(),
        DataType::Null => "VOID".to_string(),
        // The container types reach this function through the `/` and `%` operand rejects,
        // which decide on the peer's type and so can surface any type at all. Without these
        // arms the fallback below leaks Arrow's `Debug` for the whole nested type
        // (`Struct([Field { name: "a", data_type: Int32, nullable: true, .. }])`). Spark spells
        // them `STRUCT<a: INT NOT NULL>`, `ARRAY<INT>` and `MAP<STRING, INT>`.
        //
        // This deliberately duplicates the shape of `SparkPlanFormatter::data_type_to_simple_string`
        // (formatter.rs), which is NOT reused here because it answers a different question: it
        // renders Spark's lowercase `simpleString` for plan/catalog output, while an operand
        // error needs Spark's uppercase `DATATYPE_MISMATCH` spelling, the ` NOT NULL` field
        // suffix, a VARIANT arm, and `Decimal32`/`Decimal64` (which the formatter rejects with
        // `not_impl_err!`). It is also infallible, which the reject path needs. Keep the two
        // tables in sync when adding a type to either.
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) => {
            format!("ARRAY<{}>", spark_type_name(field.data_type()))
        }
        // A VARIANT is stored as a struct of binary `metadata`/`value` columns, so it has to be
        // named before the generic struct arm below — otherwise the message reports Sail's
        // physical shredding layout (`STRUCT<value: BINARY NOT NULL, ...>`) for a value Spark
        // simply calls "VARIANT".
        DataType::Struct(_) if is_variant_storage_type(data_type) => "VARIANT".to_string(),
        DataType::Struct(fields) => {
            let fields = fields
                .iter()
                .map(|field| {
                    let nullability = if field.is_nullable() { "" } else { " NOT NULL" };
                    format!(
                        "{}: {}{nullability}",
                        field.name(),
                        spark_type_name(field.data_type())
                    )
                })
                .collect::<Vec<_>>();
            format!("STRUCT<{}>", fields.join(", "))
        }
        DataType::Map(field, _) => match field.data_type() {
            DataType::Struct(entries) if entries.len() == 2 => format!(
                "MAP<{}, {}>",
                spark_type_name(entries[0].data_type()),
                spark_type_name(entries[1].data_type())
            ),
            other => format!("MAP<{}>", spark_type_name(other)),
        },
        // A dictionary-encoded column carries its logical type in the VALUE type, and
        // `rejects_as_divide_dividend` deliberately does not reject `Dictionary` (it may wrap a
        // numeric), so one can reach this function as the PEER of a rejected operand. Naming the
        // value type keeps the message in Spark's vocabulary; the encoding is an Arrow storage
        // detail Spark has no name for.
        DataType::Dictionary(_, value_type) => spark_type_name(value_type),
        DataType::RunEndEncoded(_, values) => spark_type_name(values.data_type()),
        // Spark has no union type, so there is no Spark spelling to borrow; `UNION` at least keeps
        // the message in SQL vocabulary instead of leaking Arrow's `Debug` (`Union(UnionFields([..
        // ]), Sparse)`), which is the leak this function exists to remove. A union operand is never
        // rejected on its own account (`operand_role` defers it), but it reaches this function as
        // the PEER of a rejected operand.
        DataType::Union(_, _) => "UNION".to_string(),
    }
}
