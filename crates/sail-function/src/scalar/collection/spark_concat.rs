use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::compute::cast;
use datafusion::arrow::compute::kernels::boolean::{is_null, or};
use datafusion::arrow::compute::kernels::concat_elements::concat_elements_dyn;
use datafusion::arrow::compute::kernels::nullif::nullif;
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::functions::string::concat::ConcatFunc;
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions_nested::concat::ArrayConcat;
use sail_common_datafusion::utils::items::ItemTaker;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkConcat {
    signature: Signature,
}

impl Default for SparkConcat {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkConcat {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkConcat {
    fn name(&self) -> &str {
        "spark_concat"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arguments: &[DataType]) -> Result<Vec<DataType>> {
        sail_type_system::policies::scalar_collection_spark_concat_typing::coerce_types(arguments)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        sail_type_system::policies::scalar_collection_spark_concat_typing::spark_concat_return_type(
            self.name(),
            _arg_types,
        )
    }

    /// `concat` propagates NULL: the result is null iff any argument is null
    /// (mirrors Spark `Concat.nullable`). Set here rather than in the deprecated
    /// `is_nullable`, which DataFusion no longer consults to derive the field.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        sail_type_system::policies::scalar_collection_spark_concat_typing::spark_concat_return_field_from_args(self.name(), args)
    }

    /// Plan-time simplifications that mirror Spark's `concat` semantics.
    ///
    /// concat — simplify summary:
    /// ```text
    /// concat(concat(a, b), c)  -> concat(a, b, c)  (R1: CombineConcats — flatten nested)
    /// concat(a, NULL, b)       -> NULL             (R2: null propagation — any null arg)
    /// concat(x)                -> x                (R3: single-arg identity: string/array/binary)
    /// ```
    ///
    /// * **R1** — flatten nested `concat` calls first, so deeply nested calls
    ///   collapse in one bottom-up pass and a NULL bubbled up from an inner call
    ///   is caught by R2. Only flattens when the return type is preserved.
    /// * **R2** — `concat` propagates NULL: a literal NULL argument folds the whole
    ///   call to a typed NULL at planning time, eliminating the other argument
    ///   subtrees (DCE) and enabling column pruning. Falls through on a type-invalid
    ///   combo (e.g. array + untyped NULL) so the normal path still raises the error.
    /// * **R3** — single-argument identity for string/array/binary inputs; other
    ///   types retain the explicit casts inserted during analysis.
    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        // R1: flatten nested concat (Spark's CombineConcats).
        let args = flatten_nested_concats(args, info)?;

        // R2: null propagation. Only fold when the arg types form a valid concat —
        // otherwise fall through so the normal path still raises the type error
        // (e.g. array + untyped NULL).
        if args
            .iter()
            .any(|arg| matches!(arg, Expr::Literal(scalar, _) if scalar.is_null()))
        {
            let arg_types = args
                .iter()
                .map(|arg| info.get_data_type(arg))
                .collect::<Result<Vec<_>>>()?;
            if let Ok(null) = concat_return_type(&arg_types)
                .and_then(|data_type| ScalarValue::try_from(&data_type))
            {
                return Ok(ExprSimplifyResult::Simplified(Expr::Literal(null, None)));
            }
        }

        if args.len() != 1 {
            return Ok(ExprSimplifyResult::Original(args));
        }
        let arg = args.one()?;
        // Input casts are explicit; only remove the call for an identity type.
        if matches!(
            info.get_data_type(&arg)?,
            DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::BinaryView
                | DataType::List(_)
        ) {
            Ok(ExprSimplifyResult::Simplified(arg))
        } else {
            Ok(ExprSimplifyResult::Original(vec![arg]))
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() {
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                Some(String::new()),
            )));
        }
        // `return_type` is cloned because `args.return_field` is moved into the
        // delegate call below, but the null-typed early-return still needs it.
        let return_type = args.return_field.data_type().clone();
        let mut null_mask = None;
        for arg in &args.args {
            match arg {
                ColumnarValue::Scalar(s) if s.is_null() => {
                    return Ok(ColumnarValue::Scalar(ScalarValue::try_from(&return_type)?));
                }
                ColumnarValue::Array(a) => {
                    let mask = is_null(a)?;
                    null_mask = match null_mask {
                        Some(existing) => Some(or(&existing, &mask)?),
                        None => Some(mask),
                    };
                }
                _ => (),
            }
        }
        let null_mask = null_mask.unwrap_or_else(|| BooleanArray::from(vec![false; 1]));

        let return_field = args.return_field.clone();
        let return_type = return_field.data_type();

        let concatenated = if args
            .args
            .iter()
            .any(|arg| matches!(arg.data_type(), DataType::List(_)))
        {
            ArrayConcat::new().invoke_with_args(args)
        } else if matches!(return_type, DataType::Binary | DataType::LargeBinary) {
            concat_binary_values(&args.args, return_type)
        } else {
            ConcatFunc::new().invoke_with_args(args)
        }?;

        let concatenated_array = match concatenated {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(s) => s.to_array()?,
        };

        Ok(ColumnarValue::Array(nullif(
            cast(&concatenated_array, return_type)?.as_ref(),
            &null_mask,
        )?))
    }
}

fn concat_binary_values(values: &[ColumnarValue], return_type: &DataType) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(values)?;
    let mut arrays = arrays
        .into_iter()
        .map(|array| {
            if array.data_type() != return_type {
                return internal_err!("spark_concat requires analyzed binary arguments");
            }
            Ok(array)
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter();
    let Some(first) = arrays.next() else {
        return internal_err!("spark_concat: binary concatenation requires an argument");
    };
    let concatenated = arrays.try_fold(first, |left, right| -> Result<_> {
        Ok(concat_elements_dyn(left.as_ref(), right.as_ref())?)
    })?;
    Ok(ColumnarValue::Array(concatenated))
}

/// If `expr` is a nested `spark_concat` call, return its arguments.
fn nested_concat_args(expr: &Expr) -> Option<&[Expr]> {
    match expr {
        Expr::ScalarFunction(inner)
            if inner.func.inner().downcast_ref::<SparkConcat>().is_some() =>
        {
            Some(&inner.args)
        }
        _ => None,
    }
}

/// Spark's `CombineConcats`: flatten nested `spark_concat` children,
/// `concat(concat(a, b), c)` -> `concat(a, b, c)`, reducing nesting to a single
/// kernel pass. Only flatten when it keeps the same return type — this guards the
/// array element-type / dimension merge in `return_type`; otherwise the arguments
/// are returned unchanged.
fn flatten_nested_concats(args: Vec<Expr>, info: &SimplifyContext) -> Result<Vec<Expr>> {
    if !args.iter().any(|arg| nested_concat_args(arg).is_some()) {
        return Ok(args);
    }
    let mut flattened = Vec::with_capacity(args.len());
    for arg in &args {
        match nested_concat_args(arg) {
            Some(inner) => flattened.extend(inner.iter().cloned()),
            None => flattened.push(arg.clone()),
        }
    }
    let before = args
        .iter()
        .map(|arg| info.get_data_type(arg))
        .collect::<Result<Vec<_>>>()?;
    let after = flattened
        .iter()
        .map(|arg| info.get_data_type(arg))
        .collect::<Result<Vec<_>>>()?;
    match (concat_return_type(&before), concat_return_type(&after)) {
        (Ok(before_type), Ok(after_type)) if before_type == after_type => Ok(flattened),
        _ => Ok(args),
    }
}

/// Spark `concat` output type: array concat (list dim/element merge), all-binary,
/// or string (widest of Utf8View / LargeUtf8 / Utf8).
///
/// [Credit]: <https://github.com/apache/datafusion/blob/7ccc6d7c55ae9dbcb7dee031f394bf11a03000ba/datafusion/functions-nested/src/concat.rs#L276-L310>
use sail_type_system::policies::scalar_collection_spark_concat_typing::concat_return_type;
