//! Higher-order typing separates value arguments, lambda parameter scopes, and
//! lambda result fields. A lambda is never represented by an Arrow Null field.

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, plan_err};
use datafusion_expr::type_coercion::functions::{
    value_fields_with_higher_order_udf, value_fields_with_higher_order_udf_and_lambdas,
};
use datafusion_expr::{
    HigherOrderReturnFieldArgs, HigherOrderUDF, LambdaParametersProgress, ValueOrLambda,
};

pub fn value_lambda_pair<'a, V: std::fmt::Debug, L: std::fmt::Debug>(
    name: &str,
    args: &'a [ValueOrLambda<V, L>],
) -> Result<(&'a V, &'a L)> {
    let [value, lambda] = take_function_args(name, args)?;
    let (ValueOrLambda::Value(value), ValueOrLambda::Lambda(lambda)) = (value, lambda) else {
        return plan_err!(
            "{name} expects a value followed by a lambda, got {value:?} and {lambda:?}"
        );
    };
    Ok((value, lambda))
}

pub fn coerce_single_list_arg(name: &str, arg_types: &[DataType]) -> Result<Vec<DataType>> {
    let [list] = arg_types else {
        return plan_err!(
            "{name} function requires 1 value argument, got {}",
            arg_types.len()
        );
    };
    Ok(vec![match list {
        DataType::List(_) | DataType::LargeList(_) => list.clone(),
        DataType::ListView(field) | DataType::FixedSizeList(field, _) => {
            DataType::List(Arc::clone(field))
        }
        DataType::LargeListView(field) => DataType::LargeList(Arc::clone(field)),
        _ => return plan_err!("{name} expected a list as first argument, got {list}"),
    }])
}

pub fn list_parameters(
    name: &str,
    fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    indexed: bool,
    index_first: bool,
) -> Result<LambdaParametersProgress> {
    let (list, _) = value_lambda_pair(name, fields)?;
    let element = match list.data_type() {
        DataType::List(field) | DataType::LargeList(field) => Arc::clone(field),
        other => return plan_err!("{name} expected a list, got {other}"),
    };
    let parameters = if indexed {
        let index = Arc::new(Field::new("", DataType::Int32, false));
        if index_first {
            vec![index, element]
        } else {
            vec![element, index]
        }
    } else {
        vec![element]
    };
    Ok(LambdaParametersProgress::Complete(vec![parameters]))
}

pub enum ListResult {
    Transform,
    Filter,
    Predicate,
}

pub fn list_result(
    name: &str,
    args: HigherOrderReturnFieldArgs,
    policy: ListResult,
) -> Result<FieldRef> {
    let (list, lambda) = value_lambda_pair(name, args.arg_fields)?;
    let (data_type, nullable) = match policy {
        ListResult::Transform => {
            let field = Arc::new(Field::new(
                Field::LIST_FIELD_DEFAULT_NAME,
                lambda.data_type().clone(),
                lambda.is_nullable(),
            ));
            let data_type = match list.data_type() {
                DataType::List(_) => DataType::List(field),
                DataType::LargeList(_) => DataType::LargeList(field),
                other => return plan_err!("{name} expected a list, got {other}"),
            };
            (data_type, list.is_nullable())
        }
        ListResult::Filter => (list.data_type().clone(), list.is_nullable()),
        ListResult::Predicate => (DataType::Boolean, true),
    };
    Ok(Arc::new(Field::new("", data_type, nullable)))
}

pub fn value_fields<L: Clone>(
    function: &HigherOrderUDF,
    fields: &[ValueOrLambda<FieldRef, L>],
) -> Result<Vec<ValueOrLambda<FieldRef, L>>> {
    let targets = value_fields_with_higher_order_udf(fields, function)?;
    normalize(fields, targets)
}

fn normalize<L>(
    sources: &[ValueOrLambda<FieldRef, L>],
    targets: Vec<ValueOrLambda<FieldRef, L>>,
) -> Result<Vec<ValueOrLambda<FieldRef, L>>> {
    crate::coercion::validate_arity("higher-order coercion", targets.len(), sources.len())?;
    targets
        .into_iter()
        .zip(sources)
        .map(|(target, source)| {
            Ok(match (target, source) {
                (ValueOrLambda::Value(target), ValueOrLambda::Value(source)) => {
                    ValueOrLambda::Value(crate::coercion::cast_field(source, target.data_type()))
                }
                (ValueOrLambda::Lambda(target), ValueOrLambda::Lambda(_)) => {
                    ValueOrLambda::Lambda(target)
                }
                _ => return plan_err!("higher-order coercion changed argument kind"),
            })
        })
        .collect()
}

pub struct HigherOrderTyping {
    pub arguments: Vec<ValueOrLambda<FieldRef, FieldRef>>,
    pub result: FieldRef,
}

pub fn resolve(
    function: &HigherOrderUDF,
    fields: &[ValueOrLambda<FieldRef, FieldRef>],
    literals: &[Option<&ScalarValue>],
) -> Result<HigherOrderTyping> {
    crate::coercion::validate_arity(function.name(), literals.len(), fields.len())?;
    let arguments = normalize(
        fields,
        value_fields_with_higher_order_udf_and_lambdas(fields, function)?,
    )?;
    let literals = literals
        .iter()
        .zip(fields)
        .zip(&arguments)
        .map(|((literal, source), target)| match (source, target) {
            (ValueOrLambda::Value(source), ValueOrLambda::Value(target))
                if source.data_type() == target.data_type() =>
            {
                *literal
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let result = function.return_field_from_args(HigherOrderReturnFieldArgs {
        arg_fields: &arguments,
        scalar_arguments: &literals,
    })?;
    Ok(HigherOrderTyping { arguments, result })
}

pub fn map_entry_fields(name: &str, entries: &FieldRef) -> Result<(FieldRef, FieldRef)> {
    let DataType::Struct(fields) = entries.data_type() else {
        return plan_err!("{name} expected key and value fields");
    };
    let [key, value] = fields.as_ref() else {
        return plan_err!("{name} expected key and value fields");
    };
    Ok((Arc::clone(key), Arc::clone(value)))
}

pub fn map_parameters(
    name: &str,
    fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
) -> Result<LambdaParametersProgress> {
    let (map, _) = value_lambda_pair(name, fields)?;
    let DataType::Map(entries, _) = map.data_type() else {
        return plan_err!("{name} expected a map, got {}", map.data_type());
    };
    let (key, value) = map_entry_fields(name, entries)?;
    Ok(LambdaParametersProgress::Complete(vec![vec![key, value]]))
}

pub enum MapResult {
    Filter,
    Keys,
    Values,
}

pub fn map_result(
    name: &str,
    args: HigherOrderReturnFieldArgs,
    policy: MapResult,
) -> Result<FieldRef> {
    let (map, lambda) = value_lambda_pair(name, args.arg_fields)?;
    if matches!(policy, MapResult::Filter) {
        if !matches!(lambda.data_type(), DataType::Boolean | DataType::Null) {
            return plan_err!(
                "{name} lambda must return boolean, got {}",
                lambda.data_type()
            );
        }
        return Ok(Arc::new(Field::new(
            "",
            map.data_type().clone(),
            map.is_nullable(),
        )));
    }
    let DataType::Map(entries, sorted) = map.data_type() else {
        return plan_err!("{name} expected a map, got {}", map.data_type());
    };
    let (key, value) = map_entry_fields(name, entries)?;
    let (key, value, sorted) = match policy {
        MapResult::Keys => (
            Arc::new(Field::new(key.name(), lambda.data_type().clone(), false)),
            value,
            false,
        ),
        MapResult::Values => (
            key,
            Arc::new(Field::new(
                value.name(),
                lambda.data_type().clone(),
                lambda.is_nullable(),
            )),
            *sorted,
        ),
        MapResult::Filter => unreachable!(),
    };
    let entries = Arc::new(Field::new(
        entries.name(),
        DataType::Struct(vec![key, value].into()),
        entries.is_nullable(),
    ));
    Ok(Arc::new(Field::new(
        "",
        DataType::Map(entries, sorted),
        map.is_nullable(),
    )))
}

pub fn aggregate_parameters(
    element_first: bool,
    fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
) -> Result<LambdaParametersProgress> {
    let (list, zero, has_finish) = match fields {
        [
            ValueOrLambda::Value(list),
            ValueOrLambda::Value(zero),
            ValueOrLambda::Lambda(_merge),
        ] => (list, zero, false),
        [
            ValueOrLambda::Value(list),
            ValueOrLambda::Value(zero),
            ValueOrLambda::Lambda(_merge),
            ValueOrLambda::Lambda(_finish),
        ] => (list, zero, true),
        _ => {
            return plan_err!(
                "{} expects value, value, lambda[, lambda] arguments",
                "aggregate"
            );
        }
    };

    let element = list_element_field("aggregate", list.data_type())?;
    let acc = Arc::new(Field::new("", zero.data_type().clone(), true));
    let merge_params = if element_first {
        vec![element, Arc::clone(&acc)]
    } else {
        vec![Arc::clone(&acc), element]
    };
    let mut params = vec![merge_params];
    if has_finish {
        params.push(vec![acc]);
    }
    Ok(LambdaParametersProgress::Complete(params))
}

pub fn aggregate_result(args: HigherOrderReturnFieldArgs) -> Result<FieldRef> {
    let (list, zero, merge, finish) = aggregate_args("aggregate", args.arg_fields)?;
    list_element_field("aggregate", list.data_type())?;

    if !equals_structurally_ignore_nullability(zero.data_type(), merge.data_type()) {
        return plan_err!(
            "{} merge lambda result type must match zero value type, got {} and {}",
            "aggregate",
            merge.data_type(),
            zero.data_type()
        );
    }

    Ok(Arc::new(Field::new(
        "",
        finish.data_type().clone(),
        list.is_nullable() || finish.is_nullable(),
    )))
}

pub fn aggregate_value_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    let [list, zero] = take_function_args("aggregate", arg_types)?;
    Ok(vec![coerce_list_type("aggregate", list)?, zero.clone()])
}

pub fn aggregate_args<'a, V: std::fmt::Debug, L: std::fmt::Debug>(
    name: &str,
    args: &'a [ValueOrLambda<V, L>],
) -> Result<(&'a V, &'a V, &'a L, &'a L)> {
    let [list, zero, merge, finish] = take_function_args(name, args)?;
    let (
        ValueOrLambda::Value(list),
        ValueOrLambda::Value(zero),
        ValueOrLambda::Lambda(merge),
        ValueOrLambda::Lambda(finish),
    ) = (list, zero, merge, finish)
    else {
        return plan_err!("{name} expects value, value, lambda, lambda arguments");
    };
    Ok((list, zero, merge, finish))
}

pub fn coerce_list_type(name: &str, data_type: &DataType) -> Result<DataType> {
    match data_type {
        DataType::List(_) | DataType::LargeList(_) => Ok(data_type.clone()),
        DataType::ListView(field) | DataType::FixedSizeList(field, _) => {
            Ok(DataType::List(Arc::clone(field)))
        }
        DataType::LargeListView(field) => Ok(DataType::LargeList(Arc::clone(field))),
        _ => plan_err!("{name} expected a list as first argument, got {data_type}"),
    }
}

pub fn list_element_field(name: &str, data_type: &DataType) -> Result<FieldRef> {
    match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) => Ok(Arc::clone(field)),
        other => plan_err!("{name} expected a list as first argument, got {other}"),
    }
}

pub fn equals_structurally_ignore_nullability(left: &DataType, right: &DataType) -> bool {
    match (left, right) {
        (DataType::List(left), DataType::List(right))
        | (DataType::LargeList(left), DataType::LargeList(right))
        | (DataType::ListView(left), DataType::ListView(right))
        | (DataType::LargeListView(left), DataType::LargeListView(right)) => {
            equals_structurally_ignore_nullability(left.data_type(), right.data_type())
        }
        (DataType::FixedSizeList(left, left_size), DataType::FixedSizeList(right, right_size)) => {
            left_size == right_size
                && equals_structurally_ignore_nullability(left.data_type(), right.data_type())
        }
        (DataType::Struct(left), DataType::Struct(right)) => {
            left.len() == right.len()
                && left.iter().zip(right).all(|(left, right)| {
                    equals_structurally_ignore_nullability(left.data_type(), right.data_type())
                })
        }
        (DataType::Map(left, _), DataType::Map(right, _)) => {
            equals_structurally_ignore_nullability(left.data_type(), right.data_type())
        }
        _ => left == right,
    }
}

pub fn sort_parameters(
    fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
) -> Result<LambdaParametersProgress> {
    let (list, _lambda) = value_lambda_pair("array_sort", fields)?;

    let element = match list.data_type() {
        DataType::List(field) | DataType::LargeList(field) => Arc::clone(field),
        other => return plan_err!("{} expected a list, got {other}", "array_sort"),
    };

    // The comparator takes two elements of the array. Both parameters share
    // the element type, so the `[left, right]` / `[right, left]` ordering is
    // irrelevant for typing (it only matters for evaluation in `swapped`).
    let params = vec![Arc::clone(&element), element];
    Ok(LambdaParametersProgress::Complete(vec![params]))
}

pub fn sort_result(args: HigherOrderReturnFieldArgs) -> Result<FieldRef> {
    let (list, lambda) = value_lambda_pair("array_sort", args.arg_fields)?;

    // Spark requires the comparator to return exactly `IntegerType` (Int32);
    // bigint/double/etc. raise `UNEXPECTED_RETURN_TYPE` at analysis.
    if lambda.data_type() != &DataType::Int32 {
        return plan_err!(
            "{} comparator must return INT, got {}",
            "array_sort",
            lambda.data_type()
        );
    }

    // `array_sort` preserves the input array type (element type and
    // nullability are unchanged by sorting).
    match list.data_type() {
        DataType::List(_) | DataType::LargeList(_) => Ok(Arc::new(Field::new(
            "",
            list.data_type().clone(),
            list.is_nullable(),
        ))),
        other => plan_err!("{} expected a list, got {other}", "array_sort"),
    }
}
