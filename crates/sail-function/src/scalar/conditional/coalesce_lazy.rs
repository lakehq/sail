use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow::compute::{cast_with_options, interleave};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::format::DEFAULT_CAST_OPTIONS;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{Result, exec_datafusion_err, exec_err};
use datafusion_expr::expr::{HigherOrderFunction, Lambda};
use datafusion_expr::{
    ColumnarValue, Expr, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDF, HigherOrderUDFImpl, LambdaParametersProgress, ReturnFieldArgs, ScalarUDFImpl,
    ValueOrLambda, Volatility,
};

use crate::functions_nested_utils::{evaluate_lambda_all_rows, evaluate_lambda_rows};
use crate::scalar::conditional::coalesce::CoalesceFunc;

/// `coalesce` that evaluates its arguments lazily, from left to right, and
/// evaluates each argument at most once per row, like Spark's `Coalesce`.
///
/// The first argument is an ordinary value, evaluated on every row. Every
/// later argument is a lambda whose declared parameter is a dummy that the body
/// never uses, and it is only evaluated on the rows that are still NULL.
///
/// [`CoalesceFunc::simplify`] uses this function for the arguments from the
/// first volatile non-final one onward. A `CASE` rewrite would name the
/// volatile argument twice (evaluating it twice), and the eager kernel would
/// evaluate every later argument on every row.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CoalesceLazy {
    signature: HigherOrderSignature,
}

impl Default for CoalesceLazy {
    fn default() -> Self {
        Self::new()
    }
}

impl CoalesceLazy {
    pub fn new() -> Self {
        Self {
            signature: HigherOrderSignature::variadic_any(Volatility::Immutable),
        }
    }

    /// Builds `coalesce_lazy(arg1, () -> arg2, () -> arg3, ...)`.
    pub fn call(args: Vec<Expr>) -> Result<Expr> {
        let mut args = args.into_iter();
        let Some(first) = args.next() else {
            return exec_err!("coalesce must have at least one argument");
        };
        let args = std::iter::once(Ok(first))
            .chain(args.map(lambda_with_fresh_parameter))
            .collect::<Result<Vec<_>>>()?;
        Ok(Expr::HigherOrderFunction(HigherOrderFunction::new(
            Arc::new(HigherOrderUDF::new_from_impl(Self::new())),
            args,
        )))
    }
}

impl HigherOrderUDFImpl for CoalesceLazy {
    fn name(&self) -> &str {
        "coalesce_lazy"
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        check_args(fields)?;
        let dummy = Arc::new(Field::new("", DataType::Null, true));
        Ok(LambdaParametersProgress::Complete(
            fields[1..]
                .iter()
                .map(|_| vec![Arc::clone(&dummy)])
                .collect(),
        ))
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<FieldRef> {
        check_args(args.arg_fields)?;
        let fields = args
            .arg_fields
            .iter()
            .map(|arg| match arg {
                ValueOrLambda::Value(field) | ValueOrLambda::Lambda(field) => Arc::clone(field),
            })
            .collect::<Vec<_>>();
        ScalarUDFImpl::return_field_from_args(
            &CoalesceFunc::new(),
            ReturnFieldArgs {
                arg_fields: &fields,
                scalar_arguments: args.scalar_arguments,
            },
        )
    }

    fn clear_null_values(&self) -> bool {
        false
    }

    fn short_circuits(&self) -> bool {
        true
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        check_args(&args.args)?;
        let return_type = args.return_type();
        let number_rows = args.number_rows;
        let mut args = args.args.iter();
        let first = match args.next() {
            Some(ValueOrLambda::Value(value)) => value.to_array(number_rows)?,
            _ => return exec_err!("coalesce expected a value as the first argument"),
        };
        let first = cast_to(first, return_type)?;

        // The rows that are still NULL, which the next argument is evaluated on.
        let mut pending = match first.logical_nulls() {
            None => vec![],
            Some(nulls) => (0..number_rows)
                .filter(|&row| nulls.is_null(row))
                .map(|row| {
                    u64::try_from(row)
                        .map_err(|_| exec_datafusion_err!("coalesce row index does not fit in u64"))
                })
                .collect::<Result<Vec<_>>>()?,
        };
        if pending.is_empty() {
            return Ok(ColumnarValue::Array(first));
        }

        // For each output row, the (argument, position) to take its value from.
        // Rows that stay NULL keep pointing at the (NULL) first argument.
        let mut picks = (0..number_rows).map(|row| (0, row)).collect::<Vec<_>>();
        let mut sources = vec![first];
        for arg in args {
            if pending.is_empty() {
                break;
            }
            let ValueOrLambda::Lambda(lambda) = arg else {
                return exec_err!("coalesce expected lambda arguments");
            };
            let value = if pending.len() == number_rows {
                evaluate_lambda_all_rows(lambda, number_rows)?
            } else {
                evaluate_lambda_rows(lambda, &pending)?
            };
            let value = cast_to(value, return_type)?;
            let source = sources.len();
            let nulls = value.logical_nulls();
            let mut next_pending =
                Vec::with_capacity(nulls.as_ref().map_or(0, |nulls| nulls.null_count()));
            for (position, &row) in pending.iter().enumerate() {
                if nulls.as_ref().is_some_and(|nulls| nulls.is_null(position)) {
                    next_pending.push(row);
                } else {
                    let row = usize::try_from(row).map_err(|_| {
                        exec_datafusion_err!("coalesce row index does not fit in usize")
                    })?;
                    let pick = picks.get_mut(row).ok_or_else(|| {
                        exec_datafusion_err!("coalesce row index is out of bounds")
                    })?;
                    *pick = (source, position);
                }
            }
            sources.push(value);
            pending = next_pending;
        }
        let sources = sources
            .iter()
            .map(|array| array.as_ref())
            .collect::<Vec<_>>();
        Ok(ColumnarValue::Array(interleave(&sources, &picks)?))
    }
}

/// The arguments are coerced to a common type, but nested field nullability
/// or metadata can still differ from the return type, which `interleave` rejects.
/// The cast is not "safe", so a value that cannot be converted is an error.
fn cast_to(array: ArrayRef, data_type: &DataType) -> Result<ArrayRef> {
    if array.data_type() == data_type {
        Ok(array)
    } else {
        Ok(cast_with_options(
            array.as_ref(),
            data_type,
            &DEFAULT_CAST_OPTIONS,
        )?)
    }
}

fn check_args<V, L>(args: &[ValueOrLambda<V, L>]) -> Result<()> {
    match args.split_first() {
        Some((ValueOrLambda::Value(_), rest))
            if rest
                .iter()
                .all(|arg| matches!(arg, ValueOrLambda::Lambda(_))) =>
        {
            Ok(())
        }
        Some(_) => exec_err!("coalesce expected a value followed by lambda arguments"),
        None => exec_err!("coalesce must have at least one argument"),
    }
}

/// Wraps `body` in a lambda whose single (unused) parameter does not shadow a
/// lambda variable that `body` references.
fn lambda_with_fresh_parameter(body: Expr) -> Result<Expr> {
    let mut parameter = "_coalesce".to_string();
    while body_uses_lambda_variable(&body, &parameter)? {
        parameter.push('_');
    }
    Ok(Expr::Lambda(Lambda::new(vec![parameter], body)))
}

fn body_uses_lambda_variable(body: &Expr, name: &str) -> Result<bool> {
    let mut found = false;
    body.apply(|e| {
        Ok(match e {
            Expr::Lambda(inner) if inner.params.iter().any(|p| p == name) => {
                TreeNodeRecursion::Jump
            }
            Expr::LambdaVariable(variable) if variable.name == name => {
                found = true;
                TreeNodeRecursion::Stop
            }
            _ => TreeNodeRecursion::Continue,
        })
    })?;
    Ok(found)
}
