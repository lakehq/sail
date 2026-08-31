use datafusion::common::{DataFusionError, Result, plan_err};
use sail_common_datafusion::lakeprocedure::{LakeProcedureInvocation, LakeProcedureValue};

pub(super) fn required_i64(invocation: &LakeProcedureInvocation, name: &str) -> Result<i64> {
    optional_i64(invocation, name)?.ok_or_else(|| {
        DataFusionError::Plan(format!("Missing required procedure argument '{name}'"))
    })
}

pub(super) fn optional_i64(
    invocation: &LakeProcedureInvocation,
    name: &str,
) -> Result<Option<i64>> {
    match invocation.argument(name) {
        Some(LakeProcedureValue::Int64(value)) => Ok(Some(*value)),
        Some(LakeProcedureValue::Null) | None => Ok(None),
        value => plan_err!("Procedure argument '{name}' is not an int64: {value:?}"),
    }
}

pub(super) fn required_timestamp_micros(
    invocation: &LakeProcedureInvocation,
    name: &str,
) -> Result<i64> {
    match invocation.argument(name) {
        Some(LakeProcedureValue::TimestampMicros(value)) => Ok(*value),
        value => plan_err!("Procedure argument '{name}' is not a timestamp: {value:?}"),
    }
}

pub(super) fn required_string(invocation: &LakeProcedureInvocation, name: &str) -> Result<String> {
    optional_string(invocation, name)?.ok_or_else(|| {
        DataFusionError::Plan(format!("Missing required procedure argument '{name}'"))
    })
}

pub(super) fn optional_string(
    invocation: &LakeProcedureInvocation,
    name: &str,
) -> Result<Option<String>> {
    match invocation.argument(name) {
        Some(LakeProcedureValue::Utf8(value)) => Ok(Some(value.clone())),
        Some(LakeProcedureValue::Null) | None => Ok(None),
        value => plan_err!("Procedure argument '{name}' is not a string: {value:?}"),
    }
}
