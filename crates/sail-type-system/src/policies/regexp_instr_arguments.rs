use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{Result, exec_err};
use datafusion_expr::{HigherOrderReturnFieldArgs, LambdaParametersProgress, ValueOrLambda};
pub fn regexp_instr_arguments<V, L>(args: &[ValueOrLambda<V, L>]) -> Result<(&V, &V, &L)> {
    match args {
        [
            ValueOrLambda::Value(string),
            ValueOrLambda::Value(pattern),
            ValueOrLambda::Lambda(index),
        ] => Ok((string, pattern, index)),
        _ => exec_err!("regexp_instr requires two search arguments and an index lambda"),
    }
}
pub fn lambda_parameters(
    _step: usize,
    fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
) -> Result<LambdaParametersProgress> {
    regexp_instr_arguments(fields)?;
    Ok(LambdaParametersProgress::Complete(vec![vec![Arc::new(
        Field::new("", DataType::Null, true),
    )]]))
}
pub fn return_field_from_args(args: HigherOrderReturnFieldArgs) -> Result<FieldRef> {
    let (string, pattern, index) = regexp_instr_arguments(args.arg_fields)?;
    let nullable = string.is_nullable()
        || pattern.is_nullable()
        || index.is_nullable()
        || index.data_type().is_string();
    Ok(Arc::new(Field::new(
        "regexp_instr",
        DataType::Int32,
        nullable,
    )))
}
