use std::sync::Arc;

use arrow::datatypes::{DataType, Field, IntervalUnit, TimeUnit};
use datafusion::functions::expr_fn;
use datafusion_common::ScalarValue;
use datafusion_expr::{ExprSchemable, ScalarUDF, cast, expr, lit};
use sail_common::spec::{SAIL_SPARK_INTERVAL_METADATA_KEY, SparkIntervalMetadata};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::spark_struct_rename::SparkStructRename;
use sail_function::scalar::spark_to_string::SparkToUtf8;

use crate::coercion::{
    build_rename_target_type, needs_struct_field_rename, spark_map_key_cast_can_be_null,
    spark_map_pair_refuses, spark_nested_interval_metadata_for_expression,
    spark_wider_numeric_type_of, spark_wider_type, spark_wider_type_of, struct_pair_spark_refuses,
};
use crate::error::{PlanError, PlanResult};
use crate::function::common::{
    FunctionContextInput, ScalarFunction, ScalarFunctionInput, spark_type_name,
};

fn data_diff_types_error(
    name: &str,
    data_types: &[DataType],
    argument_display_names: &[String],
) -> PlanError {
    let function_name = match name {
        "case" => "casewhen",
        "nvl" | "ifnull" | "coalesce" => "coalesce",
        name => name,
    };
    let sql_expr = match function_name {
        "casewhen" if !argument_display_names.is_empty() => {
            let mut sql = String::from("CASE");
            let mut arguments = argument_display_names.iter();
            while let Some(condition) = arguments.next() {
                if let Some(value) = arguments.next() {
                    sql.push_str(&format!(" WHEN {condition} THEN {value}"));
                } else {
                    sql.push_str(&format!(" ELSE {condition}"));
                }
            }
            sql.push_str(" END");
            sql
        }
        "coalesce" if !argument_display_names.is_empty() => {
            format!("coalesce({})", argument_display_names.join(", "))
        }
        "if" if argument_display_names.len() == 3 => {
            format!("(IF({}))", argument_display_names.join(", "))
        }
        _ => function_name.to_string(),
    };
    let data_types = data_types
        .iter()
        .map(|data_type| format!("\"{}\"", spark_type_name(data_type)))
        .collect::<Vec<_>>()
        .join(", ");
    PlanError::analysis(format!(
        "[DATATYPE_MISMATCH.DATA_DIFF_TYPES] Cannot resolve \"{sql_expr}\" due to data type mismatch: Input to `{function_name}` should all be the same type, but it's [{data_types}]. SQLSTATE: 42K09"
    ))
}

fn data_diff_types_pair_error(name: &str, left: &DataType, right: &DataType) -> PlanError {
    data_diff_types_error(name, &[left.clone(), right.clone()], &[])
}

/// Refuses a branch set Spark refuses to type. `CaseWhenCoercion` and `IfCoercion` take the branches
/// to `findWiderCommonType`, which pairs struct fields through the resolver and gives up when a name
/// does not match or the counts differ (`TypeCoercionHelper.scala:164-176`); Spark then raises
/// `DATATYPE_MISMATCH.DATA_DIFF_TYPES` instead of keeping the first branch's struct.
fn rejects_incompatible_branches(
    name: &str,
    branch_values: &[expr::Expr],
    function_context: &FunctionContextInput<'_>,
) -> Option<PlanError> {
    let data_types = branch_values
        .iter()
        .filter_map(|value| value.get_type(function_context.schema).ok())
        .collect::<Vec<_>>();
    data_types
        .iter()
        .enumerate()
        .flat_map(|(index, left)| {
            data_types[index + 1..]
                .iter()
                .map(move |right| (left, right))
        })
        .find(|(left, right)| {
            if left.is_null() || right.is_null() {
                return false;
            }
            // A scalar cannot share a common type with a container. NULL is the exception.
            if left.is_nested() != right.is_nested() {
                return true;
            }
            // Neither common-type rule unifies numeric and datetime types. Legacy CASE
            // can nevertheless promote both through a third STRING branch, because
            // TypeCoercion.findWiderCommonType processes strings first (TypeCoercion.scala:180).
            let numeric_datetime = (left.is_numeric() && is_temporal_type(right))
                || (right.is_numeric() && is_temporal_type(left));
            let legacy_string_promotion =
                !function_context.plan_config.ansi_mode && data_types.iter().any(is_string_type);
            struct_pair_spark_refuses(left, right, function_context.plan_config.case_sensitive)
                || spark_map_pair_refuses(
                    left,
                    right,
                    function_context.plan_config.ansi_mode,
                    function_context.plan_config.case_sensitive,
                )
                || (numeric_datetime && !legacy_string_promotion)
        })
        .map(|_| data_diff_types_error(name, &data_types, function_context.argument_display_names))
}

fn case(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let mut conditions = Vec::new();
    let mut branch_values = Vec::new();
    let mut iter = arguments.into_iter();
    while let Some(condition) = iter.next() {
        match iter.next() {
            Some(result) => {
                conditions.push(condition);
                branch_values.push(result);
            }
            _ => {
                conditions.push(lit(true));
                branch_values.push(condition);
                break;
            }
        }
    }
    if let Some(error) = rejects_incompatible_branches("case", &branch_values, &function_context) {
        return Err(error);
    }
    let branch_values = coerce_string_temporal_values(branch_values, &function_context)?;
    let branch_values = widen_container_values(
        widen_numeric_values(branch_values, &function_context)?,
        &function_context,
    )?;
    let when_then_expr = conditions
        .into_iter()
        .zip(branch_values)
        .map(|(condition, value)| (Box::new(condition), Box::new(value)))
        .collect();
    Ok(expr::Expr::Case(expr::Case {
        expr: None, // Expr::Case in from_ast_expression incorporates into when_then_expr
        when_then_expr,
        else_expr: None,
    }))
}

fn if_expr(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let (when_expr, then_expr, else_expr) = arguments.three()?;
    if let Some(error) = rejects_incompatible_branches(
        "if",
        &[then_expr.clone(), else_expr.clone()],
        &function_context,
    ) {
        return Err(error);
    }
    let (then_expr, else_expr) = widen_container_values(
        widen_numeric_values(
            coerce_string_temporal_values(vec![then_expr, else_expr], &function_context)?,
            &function_context,
        )?,
        &function_context,
    )?
    .two()?;
    Ok(expr::Expr::Case(expr::Case {
        expr: None,
        when_then_expr: vec![(Box::new(when_expr), Box::new(then_expr))],
        else_expr: Some(Box::new(else_expr)),
    }))
}

fn map_value_interval_metadata(
    data_type: DataType,
    metadata: SparkIntervalMetadata,
) -> PlanResult<DataType> {
    let DataType::Map(entries, sorted) = data_type else {
        return Ok(data_type);
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return Ok(DataType::Map(entries, sorted));
    };
    let metadata = metadata
        .to_json()
        .map_err(|error| PlanError::analysis(error.to_string()))?;
    let fields = fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            if index != 1 {
                return Arc::clone(field);
            }
            if !matches!(
                field.data_type(),
                DataType::Duration(TimeUnit::Microsecond)
                    | DataType::Interval(IntervalUnit::YearMonth)
            ) {
                return Arc::clone(field);
            }
            let mut field_metadata = field.metadata().clone();
            field_metadata.insert(
                SAIL_SPARK_INTERVAL_METADATA_KEY.to_string(),
                metadata.clone(),
            );
            Arc::new(field.as_ref().clone().with_metadata(field_metadata))
        })
        .collect();
    let entries = Arc::new(
        Field::new(
            entries.name(),
            DataType::Struct(fields),
            entries.is_nullable(),
        )
        .with_metadata(entries.metadata().clone()),
    );
    Ok(DataType::Map(entries, sorted))
}

/// `nvl`/`ifnull` are `Coalesce(Seq(left, right))` in Spark (`nullExpressions.scala:246`).
/// DataFusion's `nvl` coerces every container to `Utf8` -- `nvl(array, array)` is a STRING, and so
/// is `nvl(NULL, array('2'))` -- and a STRING is an arithmetic operand, so
/// `2 / nvl(NULL, array('2'))` resolved where Spark refuses an ARRAY. A container therefore goes
/// through `coalesce`, which keeps its type.
///
/// Scalars stay on `nvl`: `coalesce` refuses `nvl('a', 1)`, which Sail answers like Spark today.
fn nvl(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let (left, right) = arguments.two()?;
    let schema = function_context.schema;
    let data_type = |expr: &expr::Expr| expr.get_type(schema).ok();
    let (left_type, right_type) = (data_type(&left), data_type(&right));
    // `nvl`/`ifnull` are Spark `RuntimeReplaceable` expressions over `Coalesce`; reject before
    // DataFusion's separate nvl coercion leaks its internal type-resolution error.
    if let Some(error) = rejects_incompatible_branches(
        "coalesce",
        &[left.clone(), right.clone()],
        &function_context,
    ) {
        return Err(error);
    }
    let is_container = |expr: &expr::Expr| {
        matches!(
            expr.get_type(schema),
            Ok(DataType::List(_)
                | DataType::LargeList(_)
                | DataType::FixedSizeList(_, _)
                | DataType::ListView(_)
                | DataType::LargeListView(_)
                | DataType::Map(_, _)
                | DataType::Struct(_))
        )
    };
    if is_container(&left) || is_container(&right) {
        // A struct pair Spark cannot type is refused, not renamed: `findTypeForComplex` pairs the
        // fields through its resolver and gives up when a name does not match or the counts differ
        // (`TypeCoercionHelper.scala:164-176`), and `Coalesce` then raises
        // `DATATYPE_MISMATCH.DATA_DIFF_TYPES`.
        if let (Some(left_type), Some(right_type)) = (&left_type, &right_type)
            && struct_pair_spark_refuses(
                left_type,
                right_type,
                function_context.plan_config.case_sensitive,
            )
        {
            return Err(data_diff_types_pair_error("nvl", left_type, right_type));
        }
        // `findTypeForComplex` rejects MAP pairs when the common key needs a cast that can return
        // NULL. Do not let DataFusion subsequently coerce that rejected pair to STRING.
        if let (
            Some(left_type @ DataType::Map(left, _)),
            Some(right_type @ DataType::Map(right, _)),
        ) = (&left_type, &right_type)
            && spark_map_key_cast_can_be_null(
                left,
                right,
                function_context.plan_config.ansi_mode,
                function_context.plan_config.case_sensitive,
            )
        {
            return Err(data_diff_types_pair_error("nvl", left_type, right_type));
        }
        // `coalesce` alone cannot type two containers whose leaves differ -- an array of structs
        // whose leaves widen, a map whose values need a promotion, two structs whose field names
        // differ only by case -- so the common type is computed the way `findWiderTypeForTwo` does
        // and both sides are cast to it first (`TypeCoercionHelper.scala:141`).
        if let (Some(left_type), Some(right_type)) = (&left_type, &right_type)
            && left_type != right_type
            && let Some(common) = spark_wider_type(
                left_type,
                right_type,
                function_context.plan_config.ansi_mode,
                function_context.plan_config.case_sensitive,
            )
        {
            let mut interval_metadata = None::<SparkIntervalMetadata>;
            for expression in [&left, &right] {
                if let Some(candidate) =
                    spark_nested_interval_metadata_for_expression(expression, schema)
                        .map_err(|error| PlanError::analysis(error.to_string()))?
                {
                    interval_metadata = Some(match interval_metadata {
                        Some(current) => current.wider(candidate).ok_or_else(|| {
                            PlanError::analysis(
                                "incompatible Spark interval metadata in nvl".to_string(),
                            )
                        })?,
                        None => candidate,
                    });
                }
            }
            let common = match interval_metadata {
                Some(metadata) => map_value_interval_metadata(common, metadata)?,
                None => common,
            };
            let to_common = |expr: expr::Expr, from: &DataType| {
                let expr = if needs_struct_field_rename(from, &common) {
                    ScalarUDF::new_from_impl(SparkStructRename::new(build_rename_target_type(
                        from, &common,
                    )))
                    .call(vec![expr])
                } else {
                    expr
                };
                cast(expr, common.clone())
            };
            let left_is_non_nullable = left
                .to_field(schema)
                .is_ok_and(|(_, field)| !field.is_nullable());
            let left = to_common(left, left_type);
            // A non-nullable first argument means Coalesce never reaches the second one. This is
            // observable in ANSI mode, where eagerly casting `array('a')` to `ARRAY<BIGINT>` would
            // raise even though Spark returns the first array, and in the result nullability.
            if left_is_non_nullable {
                return Ok(left);
            }
            return Ok(expr_fn::coalesce(vec![left, to_common(right, right_type)]));
        }
        let widens = match (&left_type, &right_type) {
            (Some(left_type), Some(right_type)) => coalesce_widens_leaves(left_type, right_type),
            _ => true,
        };
        if !widens {
            return Ok(expr_fn::nvl(left, right));
        }
        return Ok(expr_fn::coalesce(vec![left, right]));
    }
    // DataFusion's `nvl` coerces an INTERVAL or a TIME to `Utf8` as well, so
    // `DATE + nvl(NULL, INTERVAL '1' DAY)` was refused and `nvl(NULL, INTERVAL '1' DAY) * 2` answered
    // NULL with ANSI off, where Spark's `Coalesce` keeps the interval or the TIME. A pair with a
    // string stays on `nvl`: `coalesce` cannot type a TIME beside a string.
    let is_interval_or_time = |expr: &expr::Expr| {
        matches!(
            expr.get_type(schema),
            Ok(DataType::Duration(_)
                | DataType::Interval(_)
                | DataType::Time32(_)
                | DataType::Time64(_))
        )
    };
    let is_string = |expr: &expr::Expr| expr.get_type(schema).is_ok_and(|t| is_string_type(&t));
    if (is_interval_or_time(&left) || is_interval_or_time(&right))
        && !is_string(&left)
        && !is_string(&right)
    {
        return Ok(expr_fn::coalesce(vec![left, right]));
    }
    // DataFusion's `nvl` coerces a DATE or TIMESTAMP to `Utf8`, so a datetime pair goes through
    // `coalesce`, widened first the way Sail's `coalesce` widens it: `nvl(date, '...')` is a DATE
    // with ANSI on and a STRING with it off, as in Spark.
    let is_temporal = |t: &Option<DataType>| t.as_ref().is_some_and(is_temporal_type);
    let is_date = |t: &Option<DataType>| t.as_ref().is_some_and(is_date_type);
    // `Coalesce` widens a TIMESTAMP beside a DATE to that TIMESTAMP
    // (`TypeCoercion.findWiderTypeForTwo`), and Sail reads the DATE as midnight in the session zone.
    // DataFusion's `coalesce` widens the pair to a NANOSECOND timestamp instead, which has no Spark
    // type at all, so the DATE is cast to the timestamp's own type first.
    let timestamp_beside_date = match (&left_type, &right_type) {
        (Some(timestamp @ DataType::Timestamp(_, _)), date) if is_date(date) => Some(timestamp),
        (date, Some(timestamp @ DataType::Timestamp(_, _))) if is_date(date) => Some(timestamp),
        _ => None,
    };
    if let Some(DataType::Timestamp(_, zone)) = timestamp_beside_date {
        // A zoned timestamp is read in the SESSION zone, the way `spark_minus` reads a DATE beside
        // one; a zone the column carries of its own is not the session's.
        let target = DataType::Timestamp(
            TimeUnit::Microsecond,
            zone.as_ref()
                .map(|_| Arc::clone(&function_context.plan_config.session_timezone)),
        );
        return Ok(expr_fn::coalesce(vec![
            cast(left, target.clone()),
            cast(right, target),
        ]));
    }
    if is_temporal(&left_type) || is_temporal(&right_type) {
        let arguments = coerce_string_temporal_values(vec![left, right], &function_context)?;
        return Ok(expr_fn::coalesce(arguments));
    }
    Ok(expr_fn::nvl(left, right))
}

/// Whether `coalesce` types two containers: every pair of leaves is equal, numeric, string, timestamp
/// or NULL. A leaf pair that needs a string or datetime promotion is not widened by it yet.
fn coalesce_widens_leaves(left: &DataType, right: &DataType) -> bool {
    match (left, right) {
        (
            DataType::List(left)
            | DataType::LargeList(left)
            | DataType::FixedSizeList(left, _)
            | DataType::ListView(left)
            | DataType::LargeListView(left),
            DataType::List(right)
            | DataType::LargeList(right)
            | DataType::FixedSizeList(right, _)
            | DataType::ListView(right)
            | DataType::LargeListView(right),
        ) => coalesce_widens_leaves(left.data_type(), right.data_type()),
        (DataType::Map(left, _), DataType::Map(right, _)) => {
            coalesce_widens_leaves(left.data_type(), right.data_type())
        }
        (DataType::Struct(left), DataType::Struct(right)) => {
            left.len() == right.len()
                && left.iter().zip(right.iter()).all(|(left, right)| {
                    coalesce_widens_leaves(left.data_type(), right.data_type())
                })
        }
        // A container beside a scalar is refused by `coalesce`, as Spark refuses it.
        (left, right) if left.is_nested() || right.is_nested() => true,
        (left, right) => {
            left == right
                || left.is_null()
                || right.is_null()
                || (left.is_numeric() && right.is_numeric())
                || (is_string_type(left) && is_string_type(right))
                || matches!(
                    (left, right),
                    (DataType::Timestamp(_, _), DataType::Timestamp(_, _))
                )
        }
    }
}

/// `CaseWhenCoercion` and `IfTypeCoercion` use `findWiderCommonType`, which recurses through
/// containers after the scalar type rules (`TypeCoercionHelper.scala:137-178,521-531`). DataFusion
/// preserves the first branch's nested leaf instead, so cast every container branch to the common
/// recursive type before building the CASE expression.
fn widen_container_values(
    arguments: Vec<expr::Expr>,
    function_context: &FunctionContextInput<'_>,
) -> PlanResult<Vec<expr::Expr>> {
    let data_types = arguments
        .iter()
        .map(|argument| argument.get_type(function_context.schema))
        .collect::<Result<Vec<_>, _>>()?;
    if !data_types.iter().any(DataType::is_nested) {
        return Ok(arguments);
    }
    let Some(common) =
        data_types
            .iter()
            .skip(1)
            .fold(data_types.first().cloned(), |current, data_type| {
                current.and_then(|current| {
                    spark_wider_type(
                        &current,
                        data_type,
                        function_context.plan_config.ansi_mode,
                        function_context.plan_config.case_sensitive,
                    )
                })
            })
    else {
        return Ok(arguments);
    };
    Ok(arguments
        .into_iter()
        .zip(data_types)
        .map(|(argument, data_type)| {
            if data_type == common {
                argument
            } else {
                let argument = if needs_struct_field_rename(&data_type, &common) {
                    ScalarUDF::new_from_impl(SparkStructRename::new(build_rename_target_type(
                        &data_type, &common,
                    )))
                    .call(vec![argument])
                } else {
                    argument
                };
                cast(argument, common.clone())
            }
        })
        .collect())
}

fn coalesce(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    if let Some(error) = rejects_incompatible_branches("coalesce", &arguments, &function_context) {
        return Err(error);
    }
    let arguments = coerce_string_temporal_values(arguments, &function_context)?;
    Ok(expr_fn::coalesce(arguments))
}

/// Widens the branches of a `CASE`/`IF` to their common numeric type, the way
/// `CaseWhenCoercion` does (`TypeCoercion.scala`, `findWiderCommonType`; the pairwise rule lives in
/// [`spark_wider_numeric_type_of`]). DataFusion types the
/// expression by its FIRST branch, so `CASE WHEN false THEN -2147483648 ELSE 3000000000L END`
/// declared an INT while carrying a BIGINT value: the rows were right, and the schema lied, which
/// broke `toArrow` and `CREATE TABLE AS SELECT`. Only an all-numeric set is widened here; a string
/// or a datetime beside it is the business of `coerce_string_temporal_values`, and a container has
/// no numeric common type to find.
fn widen_numeric_values(
    arguments: Vec<expr::Expr>,
    function_context: &FunctionContextInput<'_>,
) -> PlanResult<Vec<expr::Expr>> {
    let data_types = arguments
        .iter()
        .map(|arg| arg.get_type(function_context.schema))
        .collect::<Result<Vec<_>, _>>()?;
    if data_types.len() < 2
        || !data_types.iter().all(DataType::is_numeric)
        || data_types.windows(2).all(|pair| pair[0] == pair[1])
    {
        return Ok(arguments);
    }
    let Some(common) =
        spark_wider_numeric_type_of(&data_types, function_context.plan_config.ansi_mode)
    else {
        return Ok(arguments);
    };
    Ok(arguments
        .into_iter()
        .zip(data_types)
        .map(|(argument, data_type)| {
            if data_type == common {
                argument
            } else {
                cast(argument, common.clone())
            }
        })
        .collect())
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
        } else if let Some(common) = spark_wider_type_of(
            &data_types,
            function_context.plan_config.ansi_mode,
            function_context.plan_config.case_sensitive,
        ) {
            arguments
                .into_iter()
                .zip(data_types)
                .map(|(arg, data_type)| {
                    if data_type == common {
                        arg
                    } else {
                        cast(arg, common.clone())
                    }
                })
                .collect()
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
        ("ifnull", F::custom(nvl)),
        ("nanvl", F::binary(expr_fn::nanvl)),
        ("nullif", F::binary(expr_fn::nullif)),
        ("nullifzero", F::custom(nullifzero)),
        ("nvl", F::custom(nvl)),
        ("nvl2", F::ternary(expr_fn::nvl2)),
        ("zeroifnull", F::custom(zeroifnull)),
        ("when", F::custom(case)),
        ("case", F::custom(case)),
    ]
}

/// Create a zero literal with the same type as the input expression
fn create_zero_literal(data_type: &DataType) -> ScalarValue {
    match data_type {
        DataType::Int8 => ScalarValue::Int8(Some(0)),
        DataType::Int16 => ScalarValue::Int16(Some(0)),
        DataType::Int32 => ScalarValue::Int32(Some(0)),
        DataType::Int64 => ScalarValue::Int64(Some(0)),
        DataType::UInt8 => ScalarValue::UInt8(Some(0)),
        DataType::UInt16 => ScalarValue::UInt16(Some(0)),
        DataType::UInt32 => ScalarValue::UInt32(Some(0)),
        DataType::UInt64 => ScalarValue::UInt64(Some(0)),
        DataType::Float32 => ScalarValue::Float32(Some(0.0)),
        DataType::Float64 => ScalarValue::Float64(Some(0.0)),
        DataType::Decimal128(precision, scale) => {
            ScalarValue::Decimal128(Some(0), *precision, *scale)
        }
        DataType::Decimal256(precision, scale) => {
            ScalarValue::Decimal256(Some(0.into()), *precision, *scale)
        }
        // For non-numeric types, default to Int32
        _ => ScalarValue::Int32(Some(0)),
    }
}

/// Implementation of nullifzero function with type-aware casting
fn nullifzero(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arg = arguments.one()?;

    // Get the data type of the input argument
    let data_type = arg.to_field(function_context.schema)?.1.data_type().clone();

    // Create a zero literal with the same type as the input
    let zero_literal = lit(create_zero_literal(&data_type));

    // Return nullif(arg, zero_literal)
    Ok(expr_fn::nullif(arg, zero_literal))
}

/// Implementation of zeroifnull function with type-aware casting
fn zeroifnull(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arg = arguments.one()?;

    // Get the data type of the input argument
    let data_type = arg.to_field(function_context.schema)?.1.data_type().clone();

    // Create a zero literal with the same type as the input
    let zero_literal = lit(create_zero_literal(&data_type));

    // Return nvl(arg, zero_literal)
    Ok(expr_fn::nvl(arg, zero_literal))
}
