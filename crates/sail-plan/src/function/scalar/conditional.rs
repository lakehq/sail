//! Spark citations in this file refer to Spark 4.2.0 (`opt/spark`).

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use datafusion::functions::expr_fn;
use datafusion_common::{DFSchemaRef, ScalarValue};
use datafusion_expr::type_coercion::binary::type_union_coercion;
use datafusion_expr::{ExprSchemable, ScalarUDF, cast, expr, lit, try_cast, when};
use sail_common::spec::ARROW_DECIMAL128_MAX_PRECISION;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::spark_ansi_string_cast::SparkAnsiStringCast;
use sail_function::scalar::spark_cast_string_to_int32::SparkCastStringToInt32;
use sail_function::scalar::spark_struct_rename::SparkStructRename;
use sail_function::scalar::spark_to_string::SparkToUtf8;

use crate::config::PlanConfig;
use crate::error::{PlanError, PlanResult};
use crate::function::common::{FunctionContextInput, ScalarFunction, ScalarFunctionInput};
use crate::function::is_spark_compatible_arrow_fixed_offset;
use crate::resolver::{
    build_rename_target_type, needs_struct_field_rename, ntz_to_zoned_timestamp,
    spark_string_cast_arguments,
};

fn case(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let labels = Labels::new(
        Conditional::Case,
        function_context.argument_display_names,
        arguments.len(),
    );
    let mut conditions = Vec::new();
    let mut branch_values = Vec::new();
    let mut has_else = false;
    let mut iter = arguments.into_iter();
    while let Some(condition) = iter.next() {
        match iter.next() {
            Some(result) => {
                conditions.push(condition);
                branch_values.push(result);
            }
            _ => {
                branch_values.push(condition);
                has_else = true;
                break;
            }
        }
    }
    let mut branch_values =
        coerce_conditional_branches(branch_values, &function_context, Conditional::Case, &labels)?;
    // A call without WHEN pairs, such as `when(1)`, is just its ELSE value.
    if conditions.is_empty()
        && let Some(value) = branch_values.pop()
    {
        return Ok(value);
    }
    let mut else_expr = if has_else {
        branch_values.pop().map(Box::new)
    } else {
        None
    };
    let mut conditions =
        check_conditions(conditions, &function_context, Conditional::Case, &labels)?;
    // `CaseWhen.nullable` stops at the first literal TRUE: nothing after it can run, so it is the
    // ELSE (`conditionalExpressions.scala:191-198`). The same rewrite leaves the plan equivalent.
    if let Some(first_true) = conditions.iter().position(is_true_literal) {
        conditions.truncate(first_true);
        let value = branch_values.swap_remove(first_true);
        branch_values.truncate(first_true);
        if conditions.is_empty() {
            return Ok(value);
        }
        else_expr = Some(Box::new(value));
    }
    let when_then_expr = conditions
        .into_iter()
        .zip(branch_values)
        .map(|(condition, value)| (Box::new(condition), Box::new(value)))
        .collect();
    with_spark_nullability(
        expr::Expr::Case(expr::Case {
            expr: None, // Expr::Case in from_ast_expression incorporates into when_then_expr
            when_then_expr,
            else_expr,
        }),
        function_context.schema,
    )
}

fn is_true_literal(condition: &expr::Expr) -> bool {
    matches!(
        condition,
        expr::Expr::Literal(ScalarValue::Boolean(Some(true)), _)
    )
}

fn if_expr(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let labels = Labels::new(
        Conditional::If,
        function_context.argument_display_names,
        arguments.len(),
    );
    let (when_expr, then_expr, else_expr) = arguments.three()?;
    let predicate_type = when_expr
        .get_type(function_context.schema)
        .unwrap_or(DataType::Boolean);
    let branches = coerce_conditional_branches(
        vec![then_expr, else_expr],
        &function_context,
        Conditional::If,
        &labels,
    );
    // `IfCoercion` only casts an untyped NULL predicate when the branches have a common type;
    // otherwise `If.checkInputDataTypes` reports the predicate first (`TypeCoercionHelper.scala:521`,
    // `conditionalExpressions.scala:70`).
    if branches.is_err() && predicate_type != DataType::Boolean {
        return Err(unexpected_condition_error(
            Conditional::If,
            0,
            &predicate_type,
            &labels,
        ));
    }
    let when_expr =
        check_conditions(vec![when_expr], &function_context, Conditional::If, &labels)?.one()?;
    let (then_expr, else_expr) = branches?.two()?;
    with_spark_nullability(
        expr::Expr::Case(expr::Case {
            expr: None,
            when_then_expr: vec![(Box::new(when_expr), Box::new(then_expr))],
            else_expr: Some(Box::new(else_expr)),
        }),
        function_context.schema,
    )
}

/// How Spark names the conditional and its conditions in an error: `toSQLExpr` of the expression
/// (`CASE WHEN 1 THEN 1 ELSE 2 END`, `(IF(a, true, 1))`) and of the offending argument. They are
/// rebuilt from the display names of the arguments, and left out when those do not line up.
struct Labels<'a> {
    sql: Option<String>,
    names: Option<&'a [String]>,
}

impl<'a> Labels<'a> {
    fn new(kind: Conditional, names: &'a [String], argument_count: usize) -> Self {
        if names.len() != argument_count {
            return Self {
                sql: None,
                names: None,
            };
        }
        let sql = match kind {
            Conditional::Case => {
                let mut sql = String::from("CASE");
                for pair in names.chunks(2) {
                    match pair {
                        [condition, value] => {
                            sql.push_str(&format!(" WHEN {condition} THEN {value}"));
                        }
                        [other] => sql.push_str(&format!(" ELSE {other}")),
                        _ => {}
                    }
                }
                sql.push_str(" END");
                Some(sql)
            }
            Conditional::If => match names {
                [predicate, then, otherwise] => {
                    Some(format!("(IF({predicate}, {then}, {otherwise}))"))
                }
                _ => None,
            },
        };
        Self {
            sql,
            names: Some(names),
        }
    }

    /// The text of the expression, quoted as in `Cannot resolve "<sql>" due to ...`.
    fn header(&self, kind: Conditional) -> String {
        match &self.sql {
            Some(sql) => format!("Cannot resolve \"{sql}\""),
            None => format!("Cannot resolve the {} expression", kind.spark_name()),
        }
    }

    /// The name of the condition at `index` (the argument at `2 * index` of a CASE, the first of an IF).
    fn condition(&self, kind: Conditional, index: usize) -> Option<&str> {
        let at = match kind {
            Conditional::Case => index.checked_mul(2)?,
            Conditional::If => 0,
        };
        self.names?.get(at).map(String::as_str)
    }
}

/// `CaseWhen.nullable` is `branches.exists(_._2.nullable) || elseValue.forall(_.nullable)` and
/// `If.nullable` is `trueValue.nullable || falseValue.nullable`; neither looks at a condition
/// (`conditionalExpressions.scala:55`, `:191-203`). DataFusion also drops a nullable branch it can
/// show is unreachable (`CASE WHEN a IS NOT NULL THEN a ELSE 0 END`), so the Spark answer is
/// restored with the idiom that forces nullability.
fn with_spark_nullability(case: expr::Expr, schema: &DFSchemaRef) -> PlanResult<expr::Expr> {
    let expr::Expr::Case(inner) = &case else {
        return Ok(case);
    };
    let schema_ref = schema.as_ref();
    // Computing the nullability can force the type check of a nested higher-order function whose
    // lambda is resolved later; the plan is left as built when that is not possible yet.
    let spark_nullable = (|| -> datafusion_common::Result<bool> {
        let mut nullable = match &inner.else_expr {
            Some(else_expr) => else_expr.nullable(schema_ref)?,
            None => true,
        };
        for (_, value) in &inner.when_then_expr {
            nullable = nullable || value.nullable(schema_ref)?;
        }
        Ok(nullable)
    })();
    let (Ok(true), Ok(false)) = (spark_nullable, case.nullable(schema_ref)) else {
        return Ok(case);
    };
    Ok(when(lit(true), case).end()?)
}

#[derive(Clone, Copy, PartialEq)]
enum Conditional {
    Case,
    If,
}

impl Conditional {
    /// `toSQLId(prettyName)` of the Spark expression, as `DATA_DIFF_TYPES` reports it.
    fn spark_name(self) -> &'static str {
        match self {
            Conditional::Case => "casewhen",
            Conditional::If => "if",
        }
    }
}

/// Every condition must be a BOOLEAN (`CaseWhen.checkInputDataTypes`,
/// `conditionalExpressions.scala:205-215`; `If.checkInputDataTypes`, `:70-80`). Only `IF` casts an
/// untyped NULL predicate to BOOLEAN (`IfCoercion`, `TypeCoercionHelper.scala:532-535`).
fn check_conditions(
    conditions: Vec<expr::Expr>,
    function_context: &FunctionContextInput<'_>,
    kind: Conditional,
    labels: &Labels<'_>,
) -> PlanResult<Vec<expr::Expr>> {
    conditions
        .into_iter()
        .enumerate()
        .map(|(index, condition)| {
            // A condition whose type cannot be computed yet (a higher-order function whose lambda
            // is checked later) is left to the checks that run when it is resolved.
            let Ok(data_type) = condition.get_type(function_context.schema) else {
                return Ok(condition);
            };
            match data_type {
                DataType::Boolean => Ok(condition),
                DataType::Null if kind == Conditional::If => Ok(cast(condition, DataType::Boolean)),
                data_type => Err(unexpected_condition_error(kind, index, &data_type, labels)),
            }
        })
        .collect()
}

fn unexpected_condition_error(
    kind: Conditional,
    index: usize,
    data_type: &DataType,
    labels: &Labels<'_>,
) -> PlanError {
    let input = match labels.condition(kind, index) {
        Some(name) => format!("\"{name}\""),
        None => "the condition".to_string(),
    };
    PlanError::analysis(format!(
        "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] {} due to data type mismatch: The {} parameter \
         requires the \"BOOLEAN\" type, however {input} has the type \"{}\".",
        labels.header(kind),
        ordinal_number(index),
        spark_sql_type_name(data_type),
    ))
}

/// `QueryErrorsBase.ordinalNumber` (`:70-75`): only the first three indexes are words.
fn ordinal_number(index: usize) -> String {
    match index {
        0 => "first".to_string(),
        1 => "second".to_string(),
        2 => "third".to_string(),
        n => format!("{}th", n.saturating_add(1)),
    }
}

/// `toSQLType`: the upper-cased SQL name Spark prints in error messages.
fn spark_sql_type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Null => "VOID".to_string(),
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 | DataType::UInt8 => "TINYINT".to_string(),
        DataType::Int16 | DataType::UInt16 => "SMALLINT".to_string(),
        DataType::Int32 | DataType::UInt32 => "INT".to_string(),
        DataType::Int64 | DataType::UInt64 => "BIGINT".to_string(),
        DataType::Float16 | DataType::Float32 => "FLOAT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => format!("DECIMAL({precision},{scale})"),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "STRING".to_string(),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "BINARY".to_string(),
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Timestamp(_, None) => "TIMESTAMP_NTZ".to_string(),
        DataType::Timestamp(_, Some(_)) => "TIMESTAMP".to_string(),
        DataType::Duration(_) => "INTERVAL DAY TO SECOND".to_string(),
        DataType::Interval(arrow::datatypes::IntervalUnit::YearMonth) => {
            "INTERVAL YEAR TO MONTH".to_string()
        }
        DataType::Interval(_) => "INTERVAL".to_string(),
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            format!("ARRAY<{}>", spark_sql_type_name(field.data_type()))
        }
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(fields) if fields.len() == 2 => format!(
                "MAP<{}, {}>",
                spark_sql_type_name(fields[0].data_type()),
                spark_sql_type_name(fields[1].data_type())
            ),
            _ => "MAP".to_string(),
        },
        DataType::Struct(fields) => format!(
            "STRUCT<{}>",
            fields
                .iter()
                .map(|field| format!(
                    "{}: {}{}",
                    field.name(),
                    spark_sql_type_name(field.data_type()),
                    if field.is_nullable() { "" } else { " NOT NULL" }
                ))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        other => other.to_string().to_uppercase(),
    }
}

fn coalesce(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arguments = coerce_string_temporal_values(arguments, &function_context)?;
    Ok(expr_fn::coalesce(arguments))
}

/// Unifies the result branches of `CASE`/`IF`. The order matters: `type_union_coercion` answers
/// `Date32` for `(Utf8, Date32)`, the opposite of Spark's non-ANSI `stringPromotion`, so the
/// string/temporal pass has to run first.
fn coerce_conditional_branches(
    values: Vec<expr::Expr>,
    function_context: &FunctionContextInput<'_>,
    kind: Conditional,
    labels: &Labels<'_>,
) -> PlanResult<Vec<expr::Expr>> {
    let original_types = values
        .iter()
        .map(|value| value.get_type(function_context.schema))
        .collect::<Result<Vec<_>, _>>()?;
    let values = coerce_string_temporal_values(values, function_context)?;
    coerce_branch_values(values, function_context, kind, &original_types, labels)
}

/// Casts the result branches to their common type, the way `CaseWhenCoercion` / `IfCoercion` do
/// (`TypeCoercionHelper.scala:498-538`). Without this the plan takes `Expr::Case::get_type`, which
/// reports the first non-null `THEN` branch and ignores the `ELSE`.
fn coerce_branch_values(
    values: Vec<expr::Expr>,
    function_context: &FunctionContextInput<'_>,
    kind: Conditional,
    original_types: &[DataType],
    labels: &Labels<'_>,
) -> PlanResult<Vec<expr::Expr>> {
    let data_types = values
        .iter()
        .map(|value| value.get_type(function_context.schema))
        .collect::<Result<Vec<_>, _>>()?;
    let common_type = common_branch_type(&data_types, function_context.plan_config, kind)
        .ok_or_else(|| {
            PlanError::analysis(format!(
                "[DATATYPE_MISMATCH.DATA_DIFF_TYPES] {header} due to data type mismatch: Input to \
                 `{name}` should all be the same type, but it's [{types}].",
                header = labels.header(kind),
                name = kind.spark_name(),
                types = original_types
                    .iter()
                    .map(|data_type| format!("\"{}\"", spark_sql_type_name(data_type)))
                    .collect::<Vec<_>>()
                    .join(", "),
            ))
        })?;
    values
        .into_iter()
        .zip(data_types)
        .map(|(value, data_type)| {
            if data_type == common_type {
                return Ok(value);
            }
            // DataFusion casts struct fields by name and case-sensitively, so a branch whose field
            // names only differ in case (or in order) needs Sail's positional rename adapter first.
            let renamed = if needs_struct_field_rename(&data_type, &common_type) {
                let target = build_rename_target_type(&data_type, &common_type);
                ScalarUDF::from(SparkStructRename::new(target)).call(vec![value])
            } else if is_string_type(&common_type) && needs_spark_string_cast(&data_type) {
                // Sail's own CAST renders temporal, interval and nested values the way Spark does.
                ScalarUDF::from(SparkToUtf8::new())
                    .call(spark_string_cast_arguments(value, function_context.schema)?)
            } else {
                value
            };
            let cast_value =
                cast_branch_value(renamed, &data_type, &common_type, function_context.schema)?;
            // `Cast.nullable` is `child.nullable || forceNullable(from, to)` (`Cast.scala:630`),
            // which a DataFusion cast does not report.
            //
            // Known gap: the simplifier removes this marker, so `df.schema` and `toArrow()` say
            // nullable but a file written from real, non-null columns keeps the field required (it
            // never did better). A `TryCast` as the cast itself survives to the physical plan, but it
            // turns a failed conversion into NULL, which Decimal256 narrowing can hit; left for a
            // follow-up that handles that case.
            Ok(if cast_forces_null(&data_type, &common_type) {
                when(lit(true), cast_value).end()?
            } else {
                cast_value
            })
        })
        .collect()
}

/// Casts a branch to the common type.
///
/// A string that Spark parses as a number or a boolean goes through `SparkAnsiStringCast`: it
/// follows Spark's grammar and, being a function and not a `Cast`, raises its error only when a row
/// takes the branch. DataFusion's simplifier raises the error of a failing `CAST(<literal>)` while
/// planning, even inside a branch that is never taken, whereas Spark's `ConstantFolding` skips the
/// error inside a conditional branch.
fn cast_branch_value(
    value: expr::Expr,
    source_type: &DataType,
    common_type: &DataType,
    schema: &DFSchemaRef,
) -> PlanResult<expr::Expr> {
    if SparkAnsiStringCast::applies_to(source_type, common_type) {
        return Ok(
            ScalarUDF::from(SparkAnsiStringCast::new(common_type.clone())).call(vec![value]),
        );
    }
    if let DataType::Timestamp(TimeUnit::Microsecond, Some(timezone)) = common_type
        && matches!(source_type, DataType::Timestamp(_, None))
        && !is_spark_compatible_arrow_fixed_offset(timezone.as_ref())
    {
        return Ok(ntz_to_zoned_timestamp(
            value,
            timezone.as_ref(),
            common_type.clone(),
        ));
    }
    Ok(value.cast_to(common_type, schema)?)
}

/// Source types whose string form Spark renders itself and Arrow renders differently.
fn needs_spark_string_cast(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Duration(_)
            | DataType::Interval(_)
            | DataType::Timestamp(_, _)
    )
}

/// The type Spark gives a conditional, folding the branches as `findWiderCommonType` does.
/// `None` means the branches have no common type, which the caller turns into the analysis error
/// Spark raises from `checkInputDataTypes` (`conditionalExpressions.scala:220-227`).
///
/// A `Decimal256` anywhere in any branch switches the rules below off for the WHOLE conditional.
/// Spark has no such type, so they would only narrow it, and deciding per pair instead would let a
/// fold alternate between two rule sets and depend on branch order.
fn common_branch_type(
    data_types: &[DataType],
    config: &PlanConfig,
    kind: Conditional,
) -> Option<DataType> {
    if data_types.iter().any(contains_decimal256_type) {
        let (first, rest) = data_types.split_first()?;
        return rest.iter().try_fold(first.clone(), |left, right| {
            type_union_coercion(&left, right)
        });
    }
    // Outside ANSI, `findWiderTypeForTwo` is not associative for strings, so `CaseWhen` folds the
    // string-typed branches first (`TypeCoercion.scala:175-186`). `If` folds its two branches in
    // the order they are written (`TypeCoercionHelper.scala:521-531`).
    let ordered: Vec<&DataType> = if config.ansi_mode || kind == Conditional::If {
        data_types.iter().collect()
    } else {
        let (strings, others): (Vec<_>, Vec<_>) =
            data_types.iter().partition(|t| has_string_type(t));
        strings.into_iter().chain(others).collect()
    };
    let (first, rest) = ordered.split_first()?;
    rest.iter().try_fold((*first).clone(), |left, right| {
        branch_type_coercion(&left, right, config)
    })
}

/// Spark's `findWiderTypeForTwo` (`TypeCoercion.scala:168-173`, and `AnsiTypeCoercion.scala` when
/// ANSI is on), which differs from `type_union_coercion` in both directions: it widens pairs
/// DataFusion narrows, and it rejects pairs DataFusion happily unifies.
fn branch_type_coercion(
    left: &DataType,
    right: &DataType,
    config: &PlanConfig,
) -> Option<DataType> {
    if left == right {
        return Some(left.clone());
    }
    match (left, right) {
        (DataType::Null, other) | (other, DataType::Null) => return Some(other.clone()),
        (DataType::List(left), DataType::List(right)) => {
            return Some(DataType::List(Arc::new(coerce_element_field(
                left, right, config,
            )?)));
        }
        (DataType::LargeList(left), DataType::LargeList(right)) => {
            return Some(DataType::LargeList(Arc::new(coerce_element_field(
                left, right, config,
            )?)));
        }
        (DataType::FixedSizeList(left, size), DataType::FixedSizeList(right, other_size))
            if size == other_size =>
        {
            return Some(DataType::FixedSizeList(
                Arc::new(coerce_element_field(left, right, config)?),
                *size,
            ));
        }
        // Spark has one array type, so the Arrow list flavors only differ in how they are stored.
        (left, right) if is_list_type(left) && is_list_type(right) => {
            let (left, right) = (list_element_field(left)?, list_element_field(right)?);
            return Some(DataType::List(Arc::new(coerce_element_field(
                left, right, config,
            )?)));
        }
        (DataType::Map(left, sorted), DataType::Map(right, _)) => {
            return coerce_map_type(left, right, *sorted, config);
        }
        (DataType::Struct(left), DataType::Struct(right)) => {
            return coerce_struct_fields(left, right, config).map(DataType::Struct);
        }
        // Spark has no rule pairing a complex type with anything else.
        _ if is_complex_type(left) || is_complex_type(right) => return None,
        _ => {}
    }
    if is_string_type(left) || is_string_type(right) {
        return string_branch_type(left, right, config.ansi_mode);
    }
    // `findWiderDateTimeType` rejects every pairing that involves TIME, and Spark has no arm
    // pairing a datetime with a non-datetime.
    if is_time_type(left) || is_time_type(right) {
        return None;
    }
    if is_temporal_type(left) || is_temporal_type(right) {
        return (is_temporal_type(left) && is_temporal_type(right))
            .then(|| common_temporal_type(&[left.clone(), right.clone()], &config.session_timezone))
            .flatten();
    }
    if left.is_numeric() && right.is_numeric() {
        // ANSI returns DOUBLE rather than FLOAT for integral-meets-float, to avoid the precision
        // loss of routing an integral through float32 (`AnsiTypeCoercion.scala:113-124`).
        if config.ansi_mode
            && ((left.is_integer() && matches!(right, DataType::Float32))
                || (matches!(left, DataType::Float32) && right.is_integer()))
        {
            return Some(DataType::Float64);
        }
        if (left.is_floating() && right.is_decimal()) || (left.is_decimal() && right.is_floating())
        {
            return Some(DataType::Float64);
        }
        return wider_decimal_type(left, right).or_else(|| type_union_coercion(left, right));
    }
    // Spark has one BINARY type, so the Arrow flavors (fixed size included) only differ in storage.
    (is_binary_type(left) && is_binary_type(right))
        .then(|| type_union_coercion(left, right).unwrap_or(DataType::Binary))
}

/// `stringPromotion` outside ANSI promotes the other side to the string; ANSI promotes the string
/// to the other side instead (`AnsiStringPromotionTypeCoercion.scala:92-108`).
fn string_branch_type(left: &DataType, right: &DataType, ansi_mode: bool) -> Option<DataType> {
    if is_string_type(left) && is_string_type(right) {
        return type_union_coercion(left, right);
    }
    let (string_type, other) = if is_string_type(left) {
        (left, right)
    } else {
        (right, left)
    };
    if !ansi_mode {
        return (is_spark_atomic_type(other)
            && !matches!(other, DataType::Boolean)
            && !is_binary_type(other))
        .then(|| string_type.clone());
    }
    // `findWiderTypeForString` has no arm for an interval (`AnsiStringPromotionTypeCoercion.scala:100`).
    if matches!(other, DataType::Duration(_) | DataType::Interval(_)) {
        return None;
    }
    if other.is_integer() {
        return Some(DataType::Int64);
    }
    if other.is_floating() || other.is_decimal() {
        return Some(DataType::Float64);
    }
    is_spark_atomic_type(other).then(|| other.clone())
}

/// Spark rejects a map merge whose KEY cast could produce nulls, because a null map key is illegal
/// (`TypeCoercionHelper.scala:149-153`).
fn coerce_map_type(
    left: &Field,
    right: &Field,
    sorted: bool,
    config: &PlanConfig,
) -> Option<DataType> {
    let (DataType::Struct(left_fields), DataType::Struct(right_fields)) =
        (left.data_type(), right.data_type())
    else {
        return None;
    };
    if left_fields.len() != 2 || right_fields.len() != 2 {
        return None;
    }
    let (left_key, right_key) = (left_fields[0].as_ref(), right_fields[0].as_ref());
    let key_type = branch_type_coercion(left_key.data_type(), right_key.data_type(), config)?;
    if cast_forces_null(left_key.data_type(), &key_type)
        || cast_forces_null(right_key.data_type(), &key_type)
    {
        return None;
    }
    let key = left_key.clone().with_data_type(key_type);
    let value = coerce_element_field(left_fields[1].as_ref(), right_fields[1].as_ref(), config)?;
    let entries = left
        .clone()
        .with_data_type(DataType::Struct(Fields::from(vec![key, value])));
    Some(DataType::Map(Arc::new(entries), sorted))
}

/// Spark ORs the two source nullabilities with whether either inserted cast can produce a null
/// (`findTypeForComplex`, `TypeCoercionHelper.scala:141-176`).
fn coerce_element_field(left: &Field, right: &Field, config: &PlanConfig) -> Option<Field> {
    let data_type = branch_type_coercion(left.data_type(), right.data_type(), config)?;
    let nullable = left.is_nullable()
        || right.is_nullable()
        || cast_forces_null(left.data_type(), &data_type)
        || cast_forces_null(right.data_type(), &data_type);
    Some(
        left.clone()
            .with_data_type(data_type)
            .with_nullable(nullable),
    )
}

/// `None` when the fields do not line up by name at every position, which is what Spark requires
/// (`resolver(field1.name, field2.name)`); the resolver is case-insensitive unless the session says
/// otherwise.
fn coerce_struct_fields(left: &Fields, right: &Fields, config: &PlanConfig) -> Option<Fields> {
    if left.len() != right.len() {
        return None;
    }
    left.iter()
        .zip(right.iter())
        .map(|(left, right)| {
            field_names_match(left.name(), right.name(), config.case_sensitive)
                .then(|| coerce_element_field(left, right, config))
                .flatten()
        })
        .collect::<Option<Vec<_>>>()
        .map(Fields::from)
}

/// Spark compares names with `String.equalsIgnoreCase`, which folds non-ASCII letters too
/// (`analysis/package.scala:40`).
fn field_names_match(left: &str, right: &str, case_sensitive: bool) -> bool {
    if case_sensitive {
        return left == right;
    }
    left.chars().count() == right.chars().count()
        && left
            .chars()
            .zip(right.chars())
            .all(|(left, right)| chars_equal_ignore_case(left, right))
}

/// `Character` comparison of `String.regionMatches(ignoreCase = true)`: equal, or equal after the
/// simple upper-case mapping, or equal after the simple lower-case mapping. Rust's mappings can
/// produce several chars (`ß` upper-cases to `SS`), where Java's simple mapping keeps the char.
fn chars_equal_ignore_case(left: char, right: char) -> bool {
    fn simple_upper(c: char) -> char {
        let mut upper = c.to_uppercase();
        match (upper.next(), upper.next()) {
            (Some(single), None) => single,
            _ => c,
        }
    }
    fn simple_lower(c: char) -> char {
        c.to_lowercase().next().unwrap_or(c)
    }
    left == right || {
        let (left, right) = (simple_upper(left), simple_upper(right));
        left == right || simple_lower(left) == simple_lower(right)
    }
}

/// `Cast.forceNullable` (`Cast.scala:427-448`), in the order Spark tests its arms, restricted to
/// the conversions a conditional can produce.
fn cast_forces_null(from: &DataType, to: &DataType) -> bool {
    use arrow::datatypes::IntervalUnit;

    match (from, to) {
        _ if from == to => false,
        (DataType::Null, _) => false,
        (from, to) if is_string_type(from) => !is_string_type(to) && !is_binary_type(to),
        (_, to) if is_string_type(to) => false,
        (DataType::Timestamp(_, Some(_)), DataType::Int8 | DataType::Int16 | DataType::Int32) => {
            true
        }
        (
            DataType::Float16 | DataType::Float32 | DataType::Float64,
            DataType::Timestamp(_, Some(_)),
        ) => true,
        (DataType::Timestamp(_, Some(_)), DataType::Date32 | DataType::Date64) => false,
        (_, DataType::Date32 | DataType::Date64) => true,
        (DataType::Date32 | DataType::Date64, DataType::Timestamp(_, Some(_))) => false,
        (DataType::Date32 | DataType::Date64, _) => true,
        (_, DataType::Interval(IntervalUnit::MonthDayNano)) => true,
        (from, to) if to.is_decimal() => !decimal_cast_is_null_safe(from, to),
        (from, to) => (from.is_floating() || from.is_decimal()) && to.is_integer(),
    }
}

/// `canNullSafeCastToDecimal` (`Cast.scala:413-420`): the target must have strictly more integral
/// digits, or at least as many along with at least as much scale.
fn decimal_cast_is_null_safe(from: &DataType, to: &DataType) -> bool {
    let (Some((to_precision, to_scale)), Some((from_precision, from_scale))) =
        (decimal_type_for(to), decimal_type_for(from))
    else {
        return false;
    };
    let (to_range, from_range) = (to_precision - to_scale, from_precision - from_scale);
    to_range > from_range || (to_range >= from_range && to_scale >= from_scale)
}

/// `TypeCoercion.hasStringType` (`TypeCoercion.scala:326-331`): a string, or an array of them.
/// Spark leaves structs and maps out, so they are not moved ahead of the other branches.
fn has_string_type(data_type: &DataType) -> bool {
    match list_element_field(data_type) {
        Some(field) => has_string_type(field.data_type()),
        None => is_string_type(data_type),
    }
}

fn is_list_type(data_type: &DataType) -> bool {
    list_element_field(data_type).is_some()
}

fn list_element_field(data_type: &DataType) -> Option<&Field> {
    match data_type {
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            Some(field.as_ref())
        }
        _ => None,
    }
}

fn is_complex_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Map(_, _)
            | DataType::Struct(_)
    )
}

fn is_binary_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
    )
}

fn is_time_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Time32(_) | DataType::Time64(_))
}

fn is_spark_atomic_type(data_type: &DataType) -> bool {
    data_type.is_numeric()
        || is_string_type(data_type)
        || is_binary_type(data_type)
        || is_temporal_type(data_type)
        || is_time_type(data_type)
        || matches!(
            data_type,
            DataType::Boolean
                | DataType::Duration(_)
                | DataType::Interval(
                    arrow::datatypes::IntervalUnit::YearMonth
                        | arrow::datatypes::IntervalUnit::DayTime
                )
        )
}

/// Spark's `widerDecimalType` (`DecimalPrecisionTypeCoercion.scala:195-199`), applied only past 38
/// digits — below that DataFusion already agrees. Assumes the non-legacy
/// `boundedPreferIntegralDigits`, which is the default.
fn wider_decimal_type(left: &DataType, right: &DataType) -> Option<DataType> {
    let (left_precision, left_scale) = decimal_type_for(left)?;
    let (right_precision, right_scale) = decimal_type_for(right)?;
    let scale = left_scale.max(right_scale);
    let range = (left_precision - left_scale).max(right_precision - right_scale);
    let max_precision = i32::from(ARROW_DECIMAL128_MAX_PRECISION);
    if range + scale <= max_precision {
        return None;
    }
    // Spark keeps the integral digits and gives what is left to the scale, so the result is
    // `38 - range`. `range` is at least 1 here, so this is in `[0, 37]`.
    let scale = i8::try_from((max_precision - range).max(0)).unwrap_or(0);
    Some(DataType::Decimal128(ARROW_DECIMAL128_MAX_PRECISION, scale))
}

/// The decimal Spark uses for a type: decimals as they are, integrals via `DecimalType.forType`
/// (`DecimalType.scala:133-140`). `Decimal256` answers `None`, holding `common_branch_type`'s line.
fn decimal_type_for(data_type: &DataType) -> Option<(i32, i32)> {
    match data_type {
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale) => {
            Some((i32::from(*precision), i32::from(*scale)))
        }
        DataType::Int8 | DataType::UInt8 => Some((3, 0)),
        DataType::Int16 | DataType::UInt16 => Some((5, 0)),
        DataType::Int32 | DataType::UInt32 => Some((10, 0)),
        DataType::Int64 | DataType::UInt64 => Some((20, 0)),
        _ => None,
    }
}

fn contains_decimal256_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::Decimal256(_, _) => true,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => contains_decimal256_type(field.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| contains_decimal256_type(field.data_type())),
        _ => false,
    }
}

fn coerce_string_temporal_values(
    arguments: Vec<expr::Expr>,
    function_context: &FunctionContextInput<'_>,
) -> PlanResult<Vec<expr::Expr>> {
    let data_types = arguments
        .iter()
        .map(|arg| arg.get_type(function_context.schema))
        .collect::<Result<Vec<_>, _>>()?;
    let has_string = data_types.iter().any(is_string_type);
    let temporal_type =
        common_temporal_type(&data_types, &function_context.plan_config.session_timezone);
    let arguments = if has_string {
        if let Some(temporal_type) = temporal_type {
            if function_context.plan_config.ansi_mode {
                arguments
                    .into_iter()
                    .zip(data_types.iter())
                    .map(|(arg, data_type)| coerce_to_temporal(arg, data_type, &temporal_type))
                    .collect::<PlanResult<Vec<_>>>()?
            } else {
                arguments
                    .into_iter()
                    .zip(data_types)
                    .map(|(arg, data_type)| {
                        if is_temporal_type(&data_type) {
                            ScalarUDF::from(SparkToUtf8::new()).call(vec![arg])
                        } else {
                            arg
                        }
                    })
                    .collect()
            }
        } else {
            arguments
        }
    } else {
        arguments
    };
    Ok(arguments)
}

fn coerce_to_temporal(
    arg: expr::Expr,
    data_type: &DataType,
    target_type: &DataType,
) -> PlanResult<expr::Expr> {
    if data_type == target_type {
        return Ok(arg);
    }
    if is_string_type(data_type) {
        match target_type {
            DataType::Date32 => Ok(ScalarUDF::from(SparkDate::new(false)).call(vec![arg])),
            // This is only reached when ANSI mode requires a temporal common type.
            DataType::Timestamp(_, timezone) => {
                Ok(
                    ScalarUDF::from(SparkTimestamp::try_new(timezone.clone(), true, false)?)
                        .call(vec![arg]),
                )
            }
            _ => Ok(cast(arg, target_type.clone())),
        }
    } else if is_temporal_type(data_type) {
        Ok(cast(arg, target_type.clone()))
    } else {
        Ok(arg)
    }
}

fn common_temporal_type(data_types: &[DataType], session_timezone: &Arc<str>) -> Option<DataType> {
    if data_types
        .iter()
        .any(|data_type| matches!(data_type, DataType::Timestamp(_, Some(_))))
    {
        Some(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(Arc::clone(session_timezone)),
        ))
    } else if data_types
        .iter()
        .any(|data_type| matches!(data_type, DataType::Timestamp(_, None)))
    {
        Some(DataType::Timestamp(TimeUnit::Microsecond, None))
    } else {
        data_types
            .iter()
            .any(is_date_type)
            .then_some(DataType::Date32)
    }
}

fn is_string_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn is_temporal_type(data_type: &DataType) -> bool {
    is_date_type(data_type) || matches!(data_type, DataType::Timestamp(_, _))
}

fn is_date_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Date32 | DataType::Date64)
}

pub(super) fn list_built_in_conditional_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("coalesce", F::custom(coalesce)),
        ("if", F::custom(if_expr)),
        ("ifnull", F::binary(expr_fn::nvl)),
        ("nanvl", F::binary(expr_fn::nanvl)),
        ("nullif", F::binary(expr_fn::nullif)),
        ("nullifzero", F::custom(nullifzero)),
        ("nvl", F::binary(expr_fn::nvl)),
        ("nvl2", F::ternary(expr_fn::nvl2)),
        ("zeroifnull", F::custom(zeroifnull)),
        ("when", F::custom(case)),
        ("case", F::custom(case)),
    ]
}

fn nullifzero(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arg = arguments.one()?;
    let ansi_mode = function_context.plan_config.ansi_mode;
    let condition = match arg.get_type(function_context.schema)? {
        data_type if is_string_type(&data_type) => {
            let value = if ansi_mode {
                cast(arg.clone(), DataType::Int64)
            } else {
                ScalarUDF::from(SparkCastStringToInt32::new()).call(vec![arg.clone()])
            };
            value.eq(lit(0_i32))
        }
        DataType::Boolean if !ansi_mode => arg.clone().eq(lit(false)),
        data_type if data_type.is_numeric() || data_type == DataType::Null => {
            arg.clone().eq(lit(0_i32))
        }
        data_type => {
            return Err(PlanError::invalid(format!(
                "nullifzero cannot compare data type {data_type} with an integer zero"
            )));
        }
    };
    Ok(when(condition, lit(ScalarValue::Null)).otherwise(arg)?)
}

fn zeroifnull(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arg = arguments.one()?;
    let ansi_mode = function_context.plan_config.ansi_mode;
    let (arg, zero) = match arg.get_type(function_context.schema)? {
        DataType::Float32 if ansi_mode => (cast(arg, DataType::Float64), lit(0_i32)),
        data_type if is_string_type(&data_type) && ansi_mode => {
            (cast(arg, DataType::Int64), lit(0_i32))
        }
        data_type if is_string_type(&data_type) => (arg, lit("0")),
        DataType::Decimal128(precision, scale) => {
            // The INT zero contributes ten integer digits. Keep integer digits when
            // the common decimal type exceeds Spark's 38-digit precision limit.
            let integer_digits = (i16::from(precision) - i16::from(scale)).max(10);
            let scale = i16::from(scale).max(0);
            let precision = (integer_digits + scale).min(38) as u8;
            let scale = scale.min((38 - integer_digits).max(0)) as i8;
            let data_type = DataType::Decimal128(precision, scale);
            let arg = if ansi_mode {
                cast(arg, data_type)
            } else {
                try_cast(arg, data_type)
            };
            (arg, lit(0_i32))
        }
        data_type if data_type.is_numeric() || data_type == DataType::Null => (arg, lit(0_i32)),
        data_type => {
            return Err(PlanError::invalid(format!(
                "zeroifnull cannot combine data type {data_type} with an integer zero"
            )));
        }
    };
    Ok(expr_fn::coalesce(vec![arg, zero]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn struct_names_mark_the_fields_that_cannot_be_null() {
        let data_type = DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]));
        assert_eq!(
            spark_sql_type_name(&data_type),
            "STRUCT<a: INT NOT NULL, b: STRING>"
        );
    }

    #[test]
    fn ordinals_are_words_only_up_to_the_third() {
        let ordinals: Vec<String> = (0..5).map(ordinal_number).collect();
        assert_eq!(ordinals, ["first", "second", "third", "4th", "5th"]);
    }

    #[test]
    fn field_names_fold_like_java() {
        assert!(field_names_match("É", "é", false));
        assert!(field_names_match("İ", "i", false));
        assert!(!field_names_match("ß", "ss", false));
        assert!(!field_names_match("A", "a", true));
        assert!(!field_names_match("ab", "abc", false));
    }
}
