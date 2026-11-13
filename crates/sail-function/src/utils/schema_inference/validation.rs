use datafusion_common::{Result, exec_err, plan_err};
use datafusion_expr::ColumnarValue;

pub(crate) fn validate_args_len<T>(name: &str, args: &[T]) -> Result<()> {
    if args.is_empty() || args.len() > 2 {
        return plan_err!(
            "function `{name}` expected 1 to 2 args but got {}",
            args.len()
        );
    }
    Ok(())
}

pub(crate) fn validate_literal_args(name: &str, args: &[ColumnarValue]) -> Result<()> {
    validate_args_len(name, args)?;
    for (index, arg) in args.iter().enumerate() {
        if matches!(arg, ColumnarValue::Array(_)) {
            let position = if index == 0 { "first" } else { "second" };
            return exec_err!(
                "Expected a literal value for the {position} arg of `{name}`, instead got a column"
            );
        }
    }
    Ok(())
}
