use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef, IntervalUnit};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DFSchemaRef, plan_datafusion_err, plan_err};
use datafusion_expr::{ExprSchemable, ScalarUDF, expr};
use sail_common::spec::{self, SAIL_SPARK_INTERVAL_METADATA_KEY, SparkIntervalMetadata};
use sail_function::scalar::spark_struct_rename::SparkStructRename;

use crate::error::PlanResult;

pub(super) fn preserve_aggregate_window_fields(
    expression: expr::Expr,
    schema: &DFSchemaRef,
) -> PlanResult<expr::Expr> {
    Ok(expression
        .transform_up(|mut expression| {
            let (args, order_by) = match &expression {
                expr::Expr::AggregateFunction(function) => {
                    // Grouping markers must retain the exact GROUP BY expression for resolution.
                    if matches!(function.func.name(), "grouping" | "grouping_id") {
                        return Ok(Transformed::no(expression));
                    }
                    (&function.params.args, &function.params.order_by)
                }
                expr::Expr::WindowFunction(function) => {
                    (&function.params.args, &function.params.order_by)
                }
                _ => return Ok(Transformed::no(expression)),
            };
            let types = args
                .iter()
                .chain(order_by.iter().map(|sort| &sort.expr))
                .map(|arg| arg.get_type(schema))
                .collect::<datafusion_common::Result<Vec<_>>>()?;
            let normalized: Vec<_> = types.iter().map(normalize_list_child_metadata).collect();
            if types == normalized {
                return Ok(Transformed::no(expression));
            }
            if let expr::Expr::WindowFunction(function) = &expression
                && matches!(function.fun.name(), "lead" | "lag")
                && let [input, _, default, ..] = args.as_slice()
            {
                check_window_default_intervals(
                    &input.get_type(schema)?,
                    &default.get_type(schema)?,
                )?;
            }
            let output_type = expression.get_type(schema)?;
            let (args, order_by) = match &mut expression {
                expr::Expr::AggregateFunction(function) => {
                    (&mut function.params.args, &mut function.params.order_by)
                }
                expr::Expr::WindowFunction(function) => {
                    (&mut function.params.args, &mut function.params.order_by)
                }
                _ => return Ok(Transformed::no(expression)),
            };
            // DataFusion drops list child metadata when storing scalar aggregate/window state.
            for ((arg, source), target) in args
                .iter_mut()
                .chain(order_by.iter_mut().map(|sort| &mut sort.expr))
                .zip(types)
                .zip(normalized)
            {
                if source != target {
                    *arg = ScalarUDF::from(SparkStructRename::new(target)).call(vec![arg.clone()]);
                }
            }
            if expression.get_type(schema)? != output_type {
                expression =
                    ScalarUDF::from(SparkStructRename::new(output_type)).call(vec![expression]);
            }
            Ok(Transformed::yes(expression))
        })?
        .data)
}

fn normalize_list_child_metadata(data_type: &DataType) -> DataType {
    let field = |field: &FieldRef, clear: bool| {
        let mut field = field
            .as_ref()
            .clone()
            .with_data_type(normalize_list_child_metadata(field.data_type()));
        if clear {
            field.metadata_mut().clear();
        }
        Arc::new(field)
    };
    match data_type {
        DataType::List(child) => DataType::List(field(child, true)),
        DataType::LargeList(child) => DataType::LargeList(field(child, true)),
        DataType::ListView(child) => DataType::ListView(field(child, true)),
        DataType::LargeListView(child) => DataType::LargeListView(field(child, true)),
        DataType::FixedSizeList(child, size) => DataType::FixedSizeList(field(child, true), *size),
        DataType::Struct(fields) => {
            DataType::Struct(fields.iter().map(|child| field(child, false)).collect())
        }
        DataType::Map(child, sorted) => DataType::Map(field(child, false), *sorted),
        _ => data_type.clone(),
    }
}

fn check_window_default_intervals(
    input: &DataType,
    default: &DataType,
) -> datafusion_common::Result<()> {
    let qualifier = |field: &Field| {
        let unit = match field.data_type() {
            DataType::Interval(IntervalUnit::YearMonth) => spec::IntervalUnit::YearMonth,
            DataType::Interval(IntervalUnit::DayTime) => spec::IntervalUnit::DayTime,
            _ => return Ok(None),
        };
        match field.metadata().get(SAIL_SPARK_INTERVAL_METADATA_KEY) {
            Some(value) => SparkIntervalMetadata::from_json(value).map(Some),
            None => SparkIntervalMetadata::try_new(unit, None, None),
        }
        .map_err(|error| plan_datafusion_err!("{error}"))
    };
    let field = |input: &FieldRef, default: &FieldRef| {
        if matches!(input.data_type(), DataType::Interval(_))
            && matches!(default.data_type(), DataType::Interval(_))
            && qualifier(input)? != qualifier(default)?
        {
            return plan_err!("lead/lag default interval type must match the input interval type");
        }
        check_window_default_intervals(input.data_type(), default.data_type())
    };
    match (input, default) {
        (
            DataType::List(input)
            | DataType::LargeList(input)
            | DataType::ListView(input)
            | DataType::LargeListView(input)
            | DataType::FixedSizeList(input, _),
            DataType::List(default)
            | DataType::LargeList(default)
            | DataType::ListView(default)
            | DataType::LargeListView(default)
            | DataType::FixedSizeList(default, _),
        )
        | (DataType::Map(input, _), DataType::Map(default, _)) => field(input, default),
        (DataType::Struct(input), DataType::Struct(default)) => {
            input.iter().zip(default).try_for_each(|(i, d)| field(i, d))
        }
        _ => Ok(()),
    }
}
