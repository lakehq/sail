//! Spark-compatible `concat_ws` function that handles arrays.
//!
//! In Spark, `concat_ws(sep, a, b, ...)` joins strings with a separator. If any
//! argument is an array, its elements are joined too. Null values (scalar and
//! array elements) are skipped; a null separator yields a null row.
//!
//! The kernel uses one reused `String` buffer + `StringBuilder`, downcasting each
//! argument once into a typed accessor before the row loop to avoid per-element
//! allocation.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, GenericListArray, LargeStringArray, OffsetSizeTrait, StringArray,
    StringBuilder, StringViewArray, new_null_array,
};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

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
        Ok(DataType::Utf8)
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
                DataType::List(field)
                | DataType::ListView(field)
                | DataType::FixedSizeList(field, _) => Ok(DataType::List(Arc::clone(field))),
                DataType::LargeList(field) | DataType::LargeListView(field) => {
                    Ok(DataType::LargeList(Arc::clone(field)))
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

    // A null-typed separator makes every row null. Return an N-null column (not a
    // single scalar) so the shape lines up with the other arguments.
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

    let mut builder = StringBuilder::with_capacity(num_rows, 0);
    let mut row_buf = String::new(); // reused across rows; capacity is kept after clear()

    for row in 0..num_rows {
        if separator.is_null(row) {
            builder.append_null();
            continue;
        }
        let sep = separator.value(row);
        row_buf.clear();
        let mut first = true;
        for arg in &values {
            arg.write_row(row, sep, &mut row_buf, &mut first)?;
        }
        builder.append_value(&row_buf);
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

/// A value argument, downcast once: a string column, an array of strings, or all-null.
enum ConcatArg<'a> {
    Null,
    Str(StrCol<'a>),
    List(&'a GenericListArray<i32>),
    LargeList(&'a GenericListArray<i64>),
}

impl<'a> ConcatArg<'a> {
    fn try_new(arr: &'a ArrayRef) -> Result<Self> {
        match arr.data_type() {
            DataType::Null => Ok(Self::Null),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                Ok(Self::Str(StrCol::try_new(arr)?))
            }
            DataType::List(_) => Ok(Self::List(arr.as_list::<i32>())),
            DataType::LargeList(_) => Ok(Self::LargeList(arr.as_list::<i64>())),
            other => Err(unsupported_data_type_exec_err(
                "concat_ws",
                "STRING or ARRAY<STRING>",
                other,
            )),
        }
    }

    /// Append this argument's contribution for `row` directly into `buf`.
    fn write_row(&self, row: usize, sep: &str, buf: &mut String, first: &mut bool) -> Result<()> {
        match self {
            Self::Null => {}
            Self::Str(col) => {
                if !col.is_null(row) {
                    push_part(buf, col.value(row), sep, first);
                }
            }
            Self::List(list) => write_list_elements(*list, row, sep, buf, first)?,
            Self::LargeList(list) => write_list_elements(*list, row, sep, buf, first)?,
        }
        Ok(())
    }
}

/// Append the non-null elements of a list cell into `buf`.
fn write_list_elements<O: OffsetSizeTrait>(
    list: &GenericListArray<O>,
    row: usize,
    sep: &str,
    buf: &mut String,
    first: &mut bool,
) -> Result<()> {
    if list.is_null(row) {
        return Ok(());
    }
    let elements = list.value(row);
    // An empty array (e.g. `array()`) or an all-null-typed array contributes
    // nothing — Spark renders it as "", not an error.
    if elements.is_empty() || *elements.data_type() == DataType::Null {
        return Ok(());
    }
    let len = elements.len();
    let elements = StrCol::try_new(&elements)?;
    for i in 0..len {
        if !elements.is_null(i) {
            push_part(buf, elements.value(i), sep, first);
        }
    }
    Ok(())
}

/// Write `part` into `buf`, prefixing the separator for everything after the first.
fn push_part(buf: &mut String, part: &str, sep: &str, first: &mut bool) {
    if !*first {
        buf.push_str(sep);
    }
    *first = false;
    buf.push_str(part);
}
