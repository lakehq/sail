use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, UInt64Array};
use datafusion::arrow::compute::{cast, take};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Int64Type};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_common::{exec_err, internal_err};
use datafusion_expr::{ReturnFieldArgs, ScalarFunctionArgs};

/// Spark's `GetArrayItem` (`array[index]`): the element at a zero-based index.
///
/// The index may be any integral expression. Both arguments are evaluated once per row,
/// so a volatile index sees the same value in its bounds check, its error message, and
/// the extraction. An out-of-range index raises `INVALID_ARRAY_INDEX` in ANSI mode and
/// returns NULL otherwise. A NULL array or index returns NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkArrayItem {
    signature: Signature,
    ansi_mode: bool,
}

impl SparkArrayItem {
    pub fn new(ansi_mode: bool) -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            ansi_mode,
        }
    }

    pub fn ansi_mode(&self) -> bool {
        self.ansi_mode
    }

    fn element_field(array_type: &DataType) -> Result<FieldRef> {
        match array_type {
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _) => Ok(Arc::clone(field)),
            other => exec_err!("spark_array_item expects an array, got {other}"),
        }
    }

    fn out_of_bounds(index: i64, length: i64) -> DataFusionError {
        DataFusionError::Execution(format!(
            "[INVALID_ARRAY_INDEX] The index {index} is out of bounds. The array has {length} elements. Use the SQL function `get()` to tolerate accessing element at invalid index and return NULL instead."
        ))
    }
}

impl ScalarUDFImpl for SparkArrayItem {
    fn name(&self) -> &str {
        "spark_array_item"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    /// Spark: `GetArrayItem` is nullable, since an out-of-range index yields NULL.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [array, _index] = args.arg_fields else {
            return internal_err!("spark_array_item expects two arguments");
        };
        let element = Self::element_field(array.data_type())?;
        Ok(Arc::new(Field::new(
            self.name(),
            element.data_type().clone(),
            true,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;
        let [array, index] = args.as_slice() else {
            return internal_err!("spark_array_item expects two arguments");
        };
        let array = array.to_array(number_rows)?;
        let element = Self::element_field(array.data_type())?;
        let lists = cast(&array, &DataType::LargeList(element))?;
        let lists = lists.as_list::<i64>();
        let index = cast(&index.to_array(number_rows)?, &DataType::Int64)?;
        let index = index.as_primitive::<Int64Type>();
        let offsets = lists.value_offsets();

        let mut positions: Vec<Option<u64>> = Vec::with_capacity(number_rows);
        for row in 0..number_rows {
            if lists.is_null(row) || index.is_null(row) {
                positions.push(None);
                continue;
            }
            let start = offsets[row];
            let length = offsets[row + 1] - start;
            let position = index.value(row);
            let in_range = (0..length).contains(&position);
            if in_range {
                positions.push(Some((start + position) as u64));
            } else if self.ansi_mode {
                return Err(Self::out_of_bounds(position, length));
            } else {
                positions.push(None);
            }
        }
        let items = take(lists.values().as_ref(), &UInt64Array::from(positions), None)?;
        Ok(ColumnarValue::Array(items))
    }
}
