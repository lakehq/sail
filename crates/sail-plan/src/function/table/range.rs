use arrow_schema::DataType;
use sail_common::spec;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::error::{PlanError, PlanResult};
use crate::function::common::FunctionContextInput;

pub(crate) fn resolve_range(
    context: &mut FunctionContextInput<'_>,
    range: spec::Range,
) -> PlanResult<Query> {
    Ok({
        let partitions = range.num_partitions.unwrap_or(1);
        if partitions == 0 || range.step == 0 {
            return Err(PlanError::analysis(
                "range step must not be 0 and partition count must be positive",
            ));
        }
        let attribute = context.attribute("id".into(), ExprType::new(DataType::Int64, false));
        Query {
            kind: Deferred::Resolved(QueryKind::Range {
                start: range.start.unwrap_or(0),
                end: range.end,
                step: range.step,
                partitions,
            }),
            schema: Deferred::Resolved(QuerySchema {
                attributes: vec![attribute],
            }),
        }
    })
}

pub(super) fn range(mut input: super::TableFunctionInput<'_>) -> PlanResult<Query> {
    use sail_common_datafusion::literal::LiteralValue;
    let args = input
        .arguments
        .iter()
        .map(crate::analyzer::lowering::evaluate_constant)
        .collect::<PlanResult<Vec<_>>>()?;
    let (start, end, step, partitions) = match args.as_slice() {
        [end] => (0, LiteralValue(end).try_to_i64()?, 1, 1),
        [start, end] => (
            LiteralValue(start).try_to_i64()?,
            LiteralValue(end).try_to_i64()?,
            1,
            1,
        ),
        [start, end, step] => (
            LiteralValue(start).try_to_i64()?,
            LiteralValue(end).try_to_i64()?,
            LiteralValue(step).try_to_i64()?,
            1,
        ),
        [start, end, step, partitions] => (
            LiteralValue(start).try_to_i64()?,
            LiteralValue(end).try_to_i64()?,
            LiteralValue(step).try_to_i64()?,
            LiteralValue(partitions).try_to_usize()?,
        ),
        _ => return Err(PlanError::invalid("range requires 1 to 4 arguments")),
    };
    resolve_range(
        &mut input.context,
        spec::Range {
            start: Some(start),
            end,
            step,
            num_partitions: Some(partitions),
        },
    )
}
