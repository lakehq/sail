use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion_common::{Column, DFSchema, DFSchemaRef};
use datafusion_expr::expr::{Cast, HigherOrderFunction, Lambda};
use datafusion_expr::utils::grouping_set_to_exprlist;
use datafusion_expr::{Expr, ExprSchemable, LogicalPlan, Operator};
use sail_common::spec;
use sail_function::scalar::array::spark_array_aggregate::SparkArrayAggregate;
use sail_function::scalar::array::spark_array_transform::SparkArrayTransform;
use sail_function::scalar::spark_struct_cast::SparkStructCast;

use crate::error::PlanResult;
use crate::resolver::state::PlanResolverState;

/// Spark interval qualifiers are logical types. Keep them separate from Arrow
/// nested fields, whose metadata is not preserved by every scalar/array kernel.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum IntervalShape {
    NonInterval,
    Null,
    Unknown,
    Interval(spec::SparkIntervalMetadata),
    List(Box<Self>),
    Map(Box<Self>, Box<Self>),
    Struct(Vec<Self>),
}

impl IntervalShape {
    pub fn from_field(field: &Field) -> Self {
        if matches!(field.data_type(), DataType::Duration(TimeUnit::Microsecond)) {
            return field
                .metadata()
                .get(spec::SAIL_SPARK_INTERVAL_METADATA_KEY)
                .and_then(|value| spec::SparkIntervalMetadata::from_json(value).ok())
                .map(Self::Interval)
                .unwrap_or(Self::Unknown);
        }
        Self::from_type(field.data_type())
    }

    fn from_type(data_type: &DataType) -> Self {
        match data_type {
            DataType::Null => Self::Null,
            DataType::Duration(TimeUnit::Microsecond) => Self::Unknown,
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::ListView(field)
            | DataType::LargeListView(field)
            | DataType::FixedSizeList(field, _) => Self::List(Box::new(Self::from_field(field))),
            DataType::Map(field, _) => match field.data_type() {
                DataType::Struct(fields) if fields.len() == 2 => Self::Map(
                    Box::new(Self::from_field(&fields[0])),
                    Box::new(Self::from_field(&fields[1])),
                ),
                _ => Self::NonInterval,
            },
            DataType::Struct(fields) => {
                Self::Struct(fields.iter().map(|f| Self::from_field(f)).collect())
            }
            _ => Self::NonInterval,
        }
    }

    pub fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::Null, value) | (value, Self::Null) => value,
            (Self::Interval(left), Self::Interval(right)) => left
                .wider(right)
                .map(Self::Interval)
                .unwrap_or(Self::Unknown),
            (Self::List(left), Self::List(right)) => Self::List(Box::new(left.merge(*right))),
            (Self::Map(lk, lv), Self::Map(rk, rv)) => {
                Self::Map(Box::new(lk.merge(*rk)), Box::new(lv.merge(*rv)))
            }
            (Self::Struct(left), Self::Struct(right)) if left.len() == right.len() => Self::Struct(
                left.into_iter()
                    .zip(right)
                    .map(|(l, r)| l.merge(r))
                    .collect(),
            ),
            (Self::NonInterval, Self::NonInterval) => Self::NonInterval,
            _ => Self::Unknown,
        }
    }

    pub fn contains_interval(&self) -> bool {
        match self {
            Self::Unknown | Self::Interval(_) => true,
            Self::List(element) => element.contains_interval(),
            Self::Map(key, value) => key.contains_interval() || value.contains_interval(),
            Self::Struct(fields) => fields.iter().any(Self::contains_interval),
            Self::Null | Self::NonInterval => false,
        }
    }

    fn element(&self) -> Self {
        match self {
            Self::List(element) => *element.clone(),
            Self::Null => Self::Null,
            _ => Self::Unknown,
        }
    }

    fn day_time_default() -> Self {
        Self::Interval(spec::SparkIntervalMetadata::DayTime {
            start_field: spec::DayTimeIntervalField::Day,
            end_field: spec::DayTimeIntervalField::Second,
        })
    }
}

type Columns = Vec<(Column, IntervalShape)>;

fn lookup(columns: &Columns, column: &Column) -> Option<IntervalShape> {
    if let Some((_, shape)) = columns.iter().find(|(c, _)| c == column) {
        return Some(shape.clone());
    }
    let mut matches = columns.iter().filter(|(c, _)| c.name == column.name);
    let (_, shape) = matches.next()?;
    matches.next().is_none().then(|| shape.clone())
}

/// Only the completed query's registered output IDs are persisted by the
/// caller. Native aggregate/window/unnest column names stay in this local walk.
pub(super) fn plan_interval_shapes(
    plan: &LogicalPlan,
    state: &PlanResolverState,
) -> Vec<IntervalShape> {
    let inputs = plan.inputs();
    let children = inputs
        .iter()
        .map(|input| plan_interval_shapes(input, state))
        .collect::<Vec<_>>();
    let columns = inputs
        .iter()
        .zip(&children)
        .flat_map(|(input, shapes)| {
            input
                .schema()
                .columns()
                .into_iter()
                .zip(shapes.iter().cloned())
        })
        .collect::<Columns>();
    let schema = inputs
        .first()
        .map(|input| input.schema())
        .unwrap_or_else(|| plan.schema());
    let infer = |expr: &Expr| expression_shape(expr, schema, state, &columns, &[]);
    let result: Vec<_> = match plan {
        LogicalPlan::Projection(project) => project.expr.iter().map(infer).collect(),
        LogicalPlan::Aggregate(aggregate) => {
            let mut shapes = grouping_set_to_exprlist(&aggregate.group_expr)
                .unwrap_or_default()
                .into_iter()
                .map(infer)
                .collect::<Vec<_>>();
            if matches!(aggregate.group_expr.as_slice(), [Expr::GroupingSet(_)]) {
                shapes.push(IntervalShape::NonInterval);
            }
            shapes.extend(aggregate.aggr_expr.iter().map(infer));
            shapes
        }
        LogicalPlan::Window(window) => children[0]
            .iter()
            .cloned()
            .chain(window.window_expr.iter().map(infer))
            .collect(),
        LogicalPlan::Values(values) => (0..plan.schema().fields().len())
            .map(|index| {
                values
                    .values
                    .iter()
                    .filter_map(|row| row.get(index))
                    .map(infer)
                    .fold(IntervalShape::Null, IntervalShape::merge)
            })
            .collect(),
        LogicalPlan::Union(_) => (0..plan.schema().fields().len())
            .map(|index| {
                children
                    .iter()
                    .filter_map(|child| child.get(index))
                    .cloned()
                    .fold(IntervalShape::Null, IntervalShape::merge)
            })
            .collect(),
        LogicalPlan::Unnest(unnest) => {
            let mut result = Vec::new();
            for (index, shape) in children[0].iter().enumerate() {
                if unnest.struct_type_columns.contains(&index) {
                    if let IntervalShape::Struct(fields) = shape {
                        result.extend(fields.iter().cloned());
                    }
                } else {
                    let recursions = unnest
                        .list_type_columns
                        .iter()
                        .filter(|(i, _)| *i == index)
                        .collect::<Vec<_>>();
                    if recursions.is_empty() {
                        result.push(shape.clone());
                    } else {
                        for (_, recursion) in recursions {
                            let mut shape = shape.clone();
                            for _ in 0..recursion.depth {
                                shape = shape.element();
                            }
                            result.push(shape);
                        }
                    }
                }
            }
            result
        }
        _ => plan
            .schema()
            .iter()
            .map(|(qualifier, field)| {
                lookup(&columns, &Column::new(qualifier.cloned(), field.name()))
                    .or_else(|| state.interval_shape(field.name()).cloned())
                    .unwrap_or_else(|| IntervalShape::from_field(field))
            })
            .collect(),
    };
    // A resolved output's fact also carries type widening performed before
    // set operations lower to joins. Raw Union still reuses its left input IDs
    // and must merge by position before the resolver gives widened outputs IDs.
    if matches!(plan, LogicalPlan::Union(_)) {
        result
    } else {
        plan.schema()
            .fields()
            .iter()
            .zip(result)
            .map(|(field, shape)| state.interval_shape(field.name()).cloned().unwrap_or(shape))
            .collect()
    }
}

pub(super) fn interval_shape(
    expr: &Expr,
    schema: &DFSchema,
    state: &PlanResolverState,
) -> IntervalShape {
    expression_shape(expr, schema, state, &vec![], &[])
}

fn expression_shape(
    expr: &Expr,
    schema: &DFSchema,
    state: &PlanResolverState,
    columns: &Columns,
    lambda: &[(String, IntervalShape)],
) -> IntervalShape {
    use IntervalShape as S;
    let infer = |expr: &Expr| expression_shape(expr, schema, state, columns, lambda);
    let fallback = || {
        expr.to_field(schema)
            .map(|(_, field)| S::from_field(&field))
            .unwrap_or(S::NonInterval)
    };
    let common = |args: &[Expr]| args.iter().map(infer).fold(S::Null, S::merge);
    match expr {
        Expr::Alias(alias) => infer(&alias.expr),
        Expr::Column(column) | Expr::OuterReferenceColumn(_, column) => lookup(columns, column)
            .or_else(|| state.interval_shape(&column.name).cloned())
            .unwrap_or_else(fallback),
        Expr::LambdaVariable(variable) => lambda
            .iter()
            .rev()
            .find(|(name, _)| name.eq_ignore_ascii_case(&variable.name))
            .map(|(_, shape)| shape.clone())
            .or_else(|| state.lambda_interval_shape(&variable.name).cloned())
            .unwrap_or_else(fallback),
        Expr::Negative(value) => infer(value),
        Expr::Cast(cast) => cast_shape(&cast.expr, &cast.field, schema, infer),
        Expr::TryCast(cast) => cast_shape(&cast.expr, &cast.field, schema, infer),
        Expr::Case(case) => case
            .when_then_expr
            .iter()
            .map(|(_, value)| infer(value))
            .chain(case.else_expr.iter().map(|value| infer(value)))
            .fold(S::Null, S::merge),
        Expr::BinaryExpr(binary) => match binary.op {
            Operator::Plus | Operator::Minus
                if matches!(
                    expr.get_type(schema),
                    Ok(DataType::Duration(TimeUnit::Microsecond))
                ) =>
            {
                infer(&binary.left).merge(infer(&binary.right))
            }
            Operator::Multiply | Operator::Divide
                if matches!(
                    expr.get_type(schema),
                    Ok(DataType::Duration(TimeUnit::Microsecond))
                ) =>
            {
                S::day_time_default()
            }
            _ => fallback(),
        },
        Expr::ScalarSubquery(subquery) => plan_interval_shapes(&subquery.subquery, state)
            .into_iter()
            .next()
            .unwrap_or_else(fallback),
        Expr::ScalarFunction(function) => {
            let args = &function.args;
            let first = || args.first().map(infer).unwrap_or(S::NonInterval);
            match function.func.name() {
                "spark_abs" | "abs" | "negate_duration" | "negative" => first(),
                "make_dt_interval" => S::day_time_default(),
                "coalesce" | "greatest" | "least" => common(args),
                "struct" => S::Struct(args.iter().map(infer).collect()),
                "named_struct" => S::Struct(args.iter().skip(1).step_by(2).map(infer).collect()),
                "get_field" => match first() {
                    S::Struct(fields) => struct_field_index(args, schema, false)
                        .and_then(|i| fields.get(i).cloned())
                        .unwrap_or_else(fallback),
                    S::Map(_, value) => *value,
                    _ => fallback(),
                },
                "spark_array" | "make_array" => S::List(Box::new(common(args))),
                "arrays_zip" => S::List(Box::new(S::Struct(
                    args.iter().map(|arg| infer(arg).element()).collect(),
                ))),
                "array_element" | "array_min" | "array_max" => first().element(),
                "array_append" => S::List(Box::new(
                    first()
                        .element()
                        .merge(args.get(1).map(infer).unwrap_or(S::Null)),
                )),
                "array_prepend" => S::List(Box::new(
                    args.get(1)
                        .map(infer)
                        .unwrap_or(S::Null)
                        .element()
                        .merge(first()),
                )),
                "array_concat" | "spark_concat" | "array_union" | "array_intersect"
                | "array_except" => common(args),
                "array_slice"
                | "array_sort"
                | "array_distinct"
                | "spark_array_compact"
                | "array_remove_all"
                | "spark_reverse"
                | "shuffle" => first(),
                "flatten" => S::List(Box::new(first().element().element())),
                "array_repeat" => S::List(Box::new(first())),
                "map" | "map_from_arrays" => S::Map(
                    Box::new(first().element()),
                    Box::new(args.get(1).map(infer).unwrap_or(S::Null).element()),
                ),
                "map_from_entries" => match first().element() {
                    S::Struct(fields) if fields.len() == 2 => {
                        S::Map(Box::new(fields[0].clone()), Box::new(fields[1].clone()))
                    }
                    _ => fallback(),
                },
                "map_extract" | "map_values" => match first() {
                    S::Map(_, value) => S::List(value),
                    _ => fallback(),
                },
                "map_keys" => match first() {
                    S::Map(key, _) => S::List(key),
                    _ => fallback(),
                },
                "map_entries" => match first() {
                    S::Map(key, value) => S::List(Box::new(S::Struct(vec![*key, *value]))),
                    _ => fallback(),
                },
                "array_item_with_position" => {
                    S::List(Box::new(S::Struct(vec![S::NonInterval, first().element()])))
                }
                "array_struct_field" => match first().element() {
                    S::Struct(fields) => S::List(Box::new(
                        struct_field_index(args, schema, true)
                            .and_then(|i| fields.get(i).cloned())
                            .unwrap_or(S::Unknown),
                    )),
                    _ => fallback(),
                },
                "spark_struct_rename" => target_shape(fallback(), first()),
                _ => fallback(),
            }
        }
        Expr::AggregateFunction(function) => {
            aggregate_shape(function.func.name(), &function.params.args, infer, fallback)
        }
        Expr::WindowFunction(function) => {
            aggregate_shape(function.fun.name(), &function.params.args, infer, fallback)
        }
        Expr::HigherOrderFunction(function) => {
            higher_order_shape(function, schema, state, columns, lambda)
        }
        _ => fallback(),
    }
}

fn aggregate_shape(
    name: &str,
    args: &[Expr],
    infer: impl Fn(&Expr) -> IntervalShape,
    fallback: impl Fn() -> IntervalShape,
) -> IntervalShape {
    use IntervalShape as S;
    let first = || args.first().map(&infer).unwrap_or(S::NonInterval);
    match name {
        "min" | "max" | "sum" | "try_sum" | "first_value" | "last_value" | "nth_value"
        | "min_by" | "max_by" | "lag" | "lead" => first(),
        "array_agg" => S::List(Box::new(first())),
        "avg" | "try_avg" if first().contains_interval() => S::day_time_default(),
        _ => fallback(),
    }
}

fn cast_shape(
    expr: &Expr,
    field: &Field,
    schema: &DFSchema,
    infer: impl Fn(&Expr) -> IntervalShape,
) -> IntervalShape {
    let target = IntervalShape::from_field(field);
    if target == IntervalShape::Unknown
        && expr
            .get_type(schema)
            .is_ok_and(|data_type| data_type.is_numeric())
    {
        // Spark's unqualified day-time cast, also used when lowering interval
        // multiplication/division through a numeric intermediate.
        return IntervalShape::day_time_default();
    }
    target_shape(target, infer(expr))
}

fn target_shape(target: IntervalShape, source: IntervalShape) -> IntervalShape {
    use IntervalShape as S;
    match (target, source) {
        // Internal NULL/error arms are cast to an Arrow type without a Spark
        // qualifier. They do not widen the known value arms. Explicit typed
        // NULLs retain their target Interval qualifier below.
        (S::Unknown, S::Null) => S::Null,
        (S::List(target), S::Null) => S::List(Box::new(target_shape(*target, S::Null))),
        (S::Map(key, value), S::Null) => S::Map(
            Box::new(target_shape(*key, S::Null)),
            Box::new(target_shape(*value, S::Null)),
        ),
        (S::Struct(target), S::Null) => S::Struct(
            target
                .into_iter()
                .map(|field| target_shape(field, S::Null))
                .collect(),
        ),
        (S::Unknown, source @ (S::Unknown | S::Interval(_))) => source,
        (S::List(target), S::List(source)) => S::List(Box::new(target_shape(*target, *source))),
        (S::Map(tk, tv), S::Map(sk, sv)) => S::Map(
            Box::new(target_shape(*tk, *sk)),
            Box::new(target_shape(*tv, *sv)),
        ),
        (S::Struct(target), S::Struct(source)) if target.len() == source.len() => S::Struct(
            target
                .into_iter()
                .zip(source)
                .map(|(t, s)| target_shape(t, s))
                .collect(),
        ),
        (target, _) => target,
    }
}

fn struct_field_index(args: &[Expr], schema: &DFSchema, array: bool) -> Option<usize> {
    let mut data_type = args.first()?.get_type(schema).ok()?;
    if array {
        data_type = match data_type {
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::ListView(field)
            | DataType::LargeListView(field)
            | DataType::FixedSizeList(field, _) => field.data_type().clone(),
            _ => return None,
        };
    }
    let DataType::Struct(fields) = data_type else {
        return None;
    };
    let Expr::Literal(value, _) = args.get(1)? else {
        return None;
    };
    let name = value.try_as_str()??;
    fields.iter().position(|field| field.name() == name)
}

/// Parameter order here is Spark's order, before Sail's index-only lowering.
pub(super) fn lambda_interval_parameters(
    name: &str,
    values: &[IntervalShape],
) -> Vec<Vec<IntervalShape>> {
    use IntervalShape as S;
    let first = values.first().cloned().unwrap_or(S::NonInterval);
    match name {
        "aggregate" | "reduce" => {
            let zero = values.get(1).cloned().unwrap_or(S::NonInterval);
            vec![vec![zero.clone(), first.element()], vec![zero]]
        }
        "map_filter" | "transform_keys" | "transform_values" => match first {
            S::Map(key, value) => vec![vec![*key, *value]],
            _ => vec![vec![]],
        },
        "array_sort" => vec![vec![first.element(), first.element()]],
        _ => vec![vec![first.element(), S::NonInterval]],
    }
}

fn higher_order_shape(
    function: &HigherOrderFunction,
    schema: &DFSchema,
    state: &PlanResolverState,
    columns: &Columns,
    outer: &[(String, IntervalShape)],
) -> IntervalShape {
    use IntervalShape as S;
    let values = function
        .args
        .iter()
        .filter(|arg| !matches!(arg, Expr::Lambda(_)))
        .map(|arg| expression_shape(arg, schema, state, columns, outer))
        .collect::<Vec<_>>();
    let first = values.first().cloned().unwrap_or(S::NonInterval);
    let mut parameters = lambda_interval_parameters(function.func.name(), &values);
    let inner: &dyn Any = function.func.inner().as_ref();
    if inner
        .downcast_ref::<SparkArrayTransform>()
        .is_some_and(SparkArrayTransform::is_index_first)
    {
        parameters[0].swap(0, 1);
    }
    if inner
        .downcast_ref::<SparkArrayAggregate>()
        .is_some_and(SparkArrayAggregate::is_element_first)
    {
        parameters[0].swap(0, 1);
    }
    let outputs = function
        .args
        .iter()
        .filter_map(|arg| match arg {
            Expr::Lambda(lambda) => Some(lambda),
            _ => None,
        })
        .zip(parameters)
        .map(|(Lambda { params, body }, shapes)| {
            let mut frame = outer.to_vec();
            frame.extend(params.iter().cloned().zip(shapes));
            expression_shape(body, schema, state, columns, &frame)
        })
        .collect::<Vec<_>>();
    match function.func.name() {
        "transform" => S::List(Box::new(outputs.first().cloned().unwrap_or(S::Unknown))),
        "filter" | "array_sort" | "map_filter" => first,
        "transform_keys" | "spark_cast_map_keys" => match first {
            S::Map(_, value) => S::Map(
                Box::new(outputs.first().cloned().unwrap_or(S::Unknown)),
                value,
            ),
            _ => S::Unknown,
        },
        "transform_values" => match first {
            S::Map(key, _) => S::Map(
                key,
                Box::new(outputs.first().cloned().unwrap_or(S::Unknown)),
            ),
            _ => S::Unknown,
        },
        "aggregate" => outputs.last().cloned().unwrap_or(S::Unknown),
        "spark_struct_cast" => inner
            .downcast_ref::<SparkStructCast>()
            .map(|cast| {
                target_shape(
                    S::Struct(
                        cast.target_fields()
                            .iter()
                            .map(|field| S::from_field(field))
                            .collect(),
                    ),
                    first,
                )
            })
            .unwrap_or(S::Unknown),
        _ => S::NonInterval,
    }
}

/// The existing math builder consumes one scalar qualifier immediately. No
/// nested qualifier shape is written into an Arrow field or runtime value.
pub(super) fn qualify_date_interval_arguments(
    name: &str,
    args: &mut [Expr],
    schema: &DFSchemaRef,
    state: &PlanResolverState,
) -> PlanResult<()> {
    if !matches!(name, "+" | "-") || args.len() != 2 {
        return Ok(());
    }
    let index = match (args[0].get_type(schema), args[1].get_type(schema)) {
        (Ok(DataType::Date32), Ok(DataType::Duration(TimeUnit::Microsecond))) => 1,
        (Ok(DataType::Duration(TimeUnit::Microsecond)), Ok(DataType::Date32)) if name == "+" => 0,
        _ => return Ok(()),
    };
    let shape = interval_shape(&args[index], schema, state);
    let field = args[index].to_field(schema)?.1;
    if let Some(value) = field.metadata().get(spec::SAIL_SPARK_INTERVAL_METADATA_KEY) {
        // Preserve the existing math builder's validation of this reserved key.
        spec::SparkIntervalMetadata::from_json(value)?;
    }
    let mut metadata = field.metadata().clone();
    metadata.remove(spec::SAIL_SPARK_INTERVAL_METADATA_KEY);
    if let IntervalShape::Interval(qualifier) = shape {
        metadata.insert(
            spec::SAIL_SPARK_INTERVAL_METADATA_KEY.to_owned(),
            qualifier.to_json()?,
        );
    }
    if metadata != *field.metadata() {
        let field = Arc::new(field.as_ref().clone().with_metadata(metadata));
        args[index] = Expr::Cast(Cast::new_from_field(Box::new(args[index].clone()), field));
    }
    Ok(())
}
