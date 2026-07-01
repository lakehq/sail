//! Spark-compatible `concat_ws` function that handles arrays.
//!
//! In Spark, `concat_ws(sep, a, b, ...)` joins strings with a separator. If any
//! argument is an array, its elements are joined too. Null values (scalar and
//! array elements) are skipped; a null separator yields a null row.
//!
//! The kernel downcasts each argument once into a typed accessor before the row
//! loop and writes parts directly into the `StringBuilder` via `fmt::Write` (no
//! intermediate `String`). List arguments have their element type forced to Utf8
//! by `coerce_types`, so the list child is downcast once up front too — the row
//! loop walks `value_offsets()` instead of re-slicing/re-downcasting per row.

use std::fmt::Write as _;
use std::sync::Arc;

use datafusion::arrow::array::{
    new_null_array, Array, ArrayRef, AsArray, GenericListArray, LargeStringArray, OffsetSizeTrait,
    StringArray, StringBuilder, StringViewArray,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{internal_err, Result, ScalarValue};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};
use crate::functions_utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkConcatWs {
    signature: Signature,
}

impl Default for SparkConcatWs {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkConcatWs {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkConcatWs {
    fn name(&self) -> &str {
        "spark_concat_ws"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "spark_concat_ws: `return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    /// `concat_ws` is null only when the separator is null; null value arguments
    /// are skipped, so a non-null separator always yields a (possibly empty)
    /// string. Mirrors Spark's `ConcatWs.nullable = children.head.nullable`.
    ///
    /// Set here (not in the deprecated `is_nullable`, which DataFusion no longer
    /// consults to derive the output field).
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.first().is_none_or(|sep| sep.is_nullable());
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, nullable)))
    }

    /// Plan-time simplifications that mirror Spark's `concat_ws` semantics, so the
    /// per-row kernel runs on a smaller (or no) argument set.
    ///
    /// concat_ws — simplify summary:
    /// ```text
    /// concat_ws(NULL, …)             -> NULL                  (R1: null literal separator)
    /// concat_ws(sep, a, NULL, b)     -> concat_ws(sep, a, b)  (R2: drop null value literals)
    /// concat_ws(<non-null literal>)  -> ''                    (R3: separator only)
    /// ```
    ///
    /// * **R1** — a null separator literal makes every row null regardless of the
    ///   value arguments (even non-literal columns), so the whole call folds to a
    ///   typed null. Runs first: `concat_ws(NULL)` is NULL, not `''`.
    /// * **R2** — Spark silently skips null value arguments, so a null literal among
    ///   the values (index >= 1) is dropped. This also reaches the mixed case
    ///   `concat_ws(col, a, NULL, b)` that constant folding cannot, since the call
    ///   is not all-literal.
    /// * **R3** — `concat_ws(sep)` with a non-null literal separator and no value
    ///   arguments is always the empty string. A *column* separator is left
    ///   untouched: the kernel still has to emit null for its null rows.
    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        if matches!(args.first(), Some(Expr::Literal(scalar, _)) if scalar.is_null()) {
            return Ok(ExprSimplifyResult::Simplified(Expr::Literal(
                ScalarValue::Utf8(None),
                None,
            )));
        }

        // Keep the separator (index 0); drop null literals among the values.
        let args: Vec<Expr> = args
            .into_iter()
            .enumerate()
            .filter(|(i, arg)| {
                *i == 0 || !matches!(arg, Expr::Literal(scalar, _) if scalar.is_null())
            })
            .map(|(_, arg)| arg)
            .collect();

        if matches!(args.as_slice(), [Expr::Literal(_, _)]) {
            return Ok(ExprSimplifyResult::Simplified(Expr::Literal(
                ScalarValue::Utf8(Some(String::new())),
                None,
            )));
        }

        Ok(ExprSimplifyResult::Original(args))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // Arity is enforced by `coerce_types` at planning time.
        make_scalar_function(concat_ws_inner, vec![])(&args.args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        // Spark errors on `concat_ws()` with no separator (WRONG_NUM_ARGS).
        if arg_types.is_empty() {
            return Err(invalid_arg_count_exec_err("concat_ws", (1, i32::MAX), 0));
        }
        arg_types
            .iter()
            .map(|arg_type| match arg_type {
                DataType::Null => Ok(DataType::Null),
                DataType::Utf8 | DataType::Utf8View => Ok(DataType::Utf8),
                DataType::LargeUtf8 => Ok(DataType::LargeUtf8),
                // Expand list args at runtime; force the element type to Utf8 so
                // the planner casts non-string children to STRING (Spark coerces
                // array elements the same way it does scalars, e.g.
                // `concat_ws(',', array(1,2,3))` -> "1,2,3"). The forced Utf8 also
                // lets the kernel downcast the list child once (see Rule 23).
                DataType::List(field)
                | DataType::ListView(field)
                | DataType::FixedSizeList(field, _) => Ok(DataType::List(Arc::new(Field::new(
                    field.name(),
                    DataType::Utf8,
                    field.is_nullable(),
                )))),
                DataType::LargeList(field) | DataType::LargeListView(field) => {
                    Ok(DataType::LargeList(Arc::new(Field::new(
                        field.name(),
                        DataType::Utf8,
                        field.is_nullable(),
                    ))))
                }
                // Spark casts any other scalar (numbers, booleans, dates, binary, …) to STRING.
                other => {
                    if other.is_nested() {
                        Ok(other.clone())
                    } else {
                        Ok(DataType::Utf8)
                    }
                }
            })
            .collect()
    }
}

/// Join each row's parts with the separator. `args[0]` is the separator; the rest
/// are value arguments (strings, or arrays whose elements are expanded).
fn concat_ws_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    // Arity is enforced by `coerce_types` at planning time; guard the invoke path
    // too so a direct call that bypasses planning errors instead of panicking.
    let [separator_arg, value_args @ ..] = args else {
        return Err(invalid_arg_count_exec_err(
            "concat_ws",
            (1, i32::MAX),
            args.len(),
        ));
    };
    let num_rows = separator_arg.len();

    // A null-typed separator makes every row null. The `simplify` hook folds a
    // *literal* null separator to a constant NULL at planning time, so in a normal
    // query this branch is not reached; it stays as a safeguard for a non-literal
    // Null-typed separator (e.g. a Null-typed column) or a direct invoke that
    // bypasses simplification. Return an N-null column (not a single scalar) so the
    // shape lines up with the other arguments.
    if *separator_arg.data_type() == DataType::Null {
        return Ok(new_null_array(&DataType::Utf8, num_rows));
    }

    // Downcast every argument once, up front — the per-row loop then does no type
    // dispatch and no per-element allocation.
    let separator = StrCol::try_new(separator_arg)?;
    let values = value_args
        .iter()
        .map(ConcatArg::try_new)
        .collect::<Result<Vec<_>>>()?;

    let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);

    for row in 0..num_rows {
        if separator.is_null(row) {
            builder.append_null();
            continue;
        }
        let sep = separator.value(row);
        let mut first = true;
        for arg in &values {
            arg.write_row(row, sep, &mut builder, &mut first);
        }
        // Finalise the row: records the offset + validity, no extra byte copy.
        builder.append_value("");
    }

    Ok(Arc::new(builder.finish()))
}

/// A string column downcast once. `value`/`is_null` are then branch-only per row.
enum StrCol<'a> {
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Utf8View(&'a StringViewArray),
}

impl<'a> StrCol<'a> {
    fn try_new(arr: &'a ArrayRef) -> Result<Self> {
        match arr.data_type() {
            DataType::Utf8 => Ok(Self::Utf8(arr.as_string::<i32>())),
            DataType::LargeUtf8 => Ok(Self::LargeUtf8(arr.as_string::<i64>())),
            DataType::Utf8View => Ok(Self::Utf8View(arr.as_string_view())),
            other => Err(unsupported_data_type_exec_err("concat_ws", "STRING", other)),
        }
    }

    fn value(&self, row: usize) -> &str {
        match self {
            Self::Utf8(a) => a.value(row),
            Self::LargeUtf8(a) => a.value(row),
            Self::Utf8View(a) => a.value(row),
        }
    }

    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Utf8(a) => a.is_null(row),
            Self::LargeUtf8(a) => a.is_null(row),
            Self::Utf8View(a) => a.is_null(row),
        }
    }
}

/// A value argument, downcast once: a string column, a list of strings, or all-null.
enum ConcatArg<'a> {
    Null,
    Str(StrCol<'a>),
    List(ListCol<'a, i32>),
    LargeList(ListCol<'a, i64>),
}

impl<'a> ConcatArg<'a> {
    fn try_new(arr: &'a ArrayRef) -> Result<Self> {
        match arr.data_type() {
            DataType::Null => Ok(Self::Null),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                Ok(Self::Str(StrCol::try_new(arr)?))
            }
            DataType::List(_) => Ok(Self::List(ListCol::try_new(arr.as_list::<i32>())?)),
            DataType::LargeList(_) => Ok(Self::LargeList(ListCol::try_new(arr.as_list::<i64>())?)),
            other => Err(unsupported_data_type_exec_err(
                "concat_ws",
                "STRING or ARRAY<STRING>",
                other,
            )),
        }
    }

    /// Append this argument's contribution for `row` directly into `builder`.
    fn write_row(&self, row: usize, sep: &str, builder: &mut StringBuilder, first: &mut bool) {
        match self {
            Self::Null => {}
            Self::Str(col) => {
                if !col.is_null(row) {
                    push_part(builder, col.value(row), sep, first);
                }
            }
            Self::List(list) => list.write_row(row, sep, builder, first),
            Self::LargeList(list) => list.write_row(row, sep, builder, first),
        }
    }
}

/// A list-of-strings argument. The child is always a string column — `coerce_types`
/// forces the element type to Utf8 — so it is downcast ONCE here; per-row access
/// walks `value_offsets()` against that child instead of re-slicing (`value(row)`)
/// and re-downcasting per row.
struct ListCol<'a, O: OffsetSizeTrait> {
    list: &'a GenericListArray<O>,
    child: StrCol<'a>,
}

impl<'a, O: OffsetSizeTrait> ListCol<'a, O> {
    fn try_new(list: &'a GenericListArray<O>) -> Result<Self> {
        let child = StrCol::try_new(list.values())?;
        Ok(Self { list, child })
    }

    /// Append the non-null elements of `row`'s list cell. A null or empty cell
    /// contributes nothing — Spark renders it as "", not an error.
    fn write_row(&self, row: usize, sep: &str, builder: &mut StringBuilder, first: &mut bool) {
        if self.list.is_null(row) {
            return;
        }
        let offsets = self.list.value_offsets();
        let (start, end) = (offsets[row].as_usize(), offsets[row + 1].as_usize());
        for i in start..end {
            if !self.child.is_null(i) {
                push_part(builder, self.child.value(i), sep, first);
            }
        }
    }
}

/// Write `part` into `builder`, prefixing the separator for everything after the
/// first. `StringBuilder::write_str` is infallible (it only extends the byte
/// buffer), so the returned `fmt::Result` is discarded.
fn push_part(builder: &mut StringBuilder, part: &str, sep: &str, first: &mut bool) {
    if !*first {
        let _ = builder.write_str(sep);
    }
    *first = false;
    let _ = builder.write_str(part);
}
