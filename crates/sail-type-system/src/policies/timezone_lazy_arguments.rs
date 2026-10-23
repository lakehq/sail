use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{Result, exec_err};
use datafusion_expr::{
    HigherOrderReturnFieldArgs, LambdaParametersProgress, ReturnFieldArgs, ScalarUDFImpl,
    ValueOrLambda,
};
pub fn check_lazy_convert_tz_args<V, L>(args: &[ValueOrLambda<V, L>]) -> Result<()> {
    if args.len() != 3 {
        return exec_err!(
            "convert_timezone takes 3 internal arguments, got {}",
            args.len()
        );
    }
    if args
        .iter()
        .any(|arg| matches!(arg, ValueOrLambda::Value(_)))
    {
        return exec_err!("convert_timezone expected lambda arguments");
    }
    Ok(())
}
pub fn lambda_parameters(
    _step: usize,
    fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
) -> Result<LambdaParametersProgress> {
    check_lazy_convert_tz_args(fields)?;
    let dummy = Arc::new(Field::new("", DataType::Null, true));
    Ok(LambdaParametersProgress::Complete(
        fields.iter().map(|_| vec![Arc::clone(&dummy)]).collect(),
    ))
}
pub fn return_field_from_args(
    inner: &dyn ScalarUDFImpl,
    args: HigherOrderReturnFieldArgs,
) -> Result<FieldRef> {
    check_lazy_convert_tz_args(args.arg_fields)?;
    let fields = args
        .arg_fields
        .iter()
        .map(|arg| match arg {
            ValueOrLambda::Lambda(field) => Ok(Arc::clone(field)),
            ValueOrLambda::Value(_) => {
                exec_err!("convert_timezone expected lambda arguments")
            }
        })
        .collect::<Result<Vec<_>>>()?;

    ScalarUDFImpl::return_field_from_args(
        inner,
        ReturnFieldArgs {
            arg_fields: &fields,
            scalar_arguments: args.scalar_arguments,
        },
    )
}
