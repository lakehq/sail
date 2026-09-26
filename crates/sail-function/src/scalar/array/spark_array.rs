/// [Credit]: <https://github.com/apache/datafusion/blob/c21d025df463ce623f9193c4b24d86141fce81ca/datafusion/functions-nested/src/make_array.rs>
/// Spark defaults to DataType::Int32 while DataFusion defaults to DataType::Int64.
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayData, ArrayRef, Capacities, GenericListArray, MutableArrayData, NullArray,
    OffsetSizeTrait, make_array, new_empty_array, new_null_array,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::utils::SingleRowListArrayBuilder;
use datafusion_common::{Result, plan_datafusion_err, plan_err};
use datafusion_expr::type_coercion::binary::comparison_coercion;
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

use crate::functions_nested_utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkArray {
    signature: Signature,
    aliases: Vec<String>,
    force_element_nullable: bool,
}

impl Default for SparkArray {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkArray {
    pub fn new() -> Self {
        Self::new_with_force_element_nullable(false)
    }

    /// Builds an ARRAY whose element cast can produce NULL even from a non-null source.
    /// Spark exposes that through `containsNull` (`Cast.forceNullable`).
    pub fn new_with_force_element_nullable(force_element_nullable: bool) -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::UserDefined, TypeSignature::Nullary],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("spark_make_array")],
            force_element_nullable,
        }
    }
}

impl ScalarUDFImpl for SparkArray {
    fn name(&self) -> &str {
        if self.force_element_nullable {
            "spark_array_force_nullable"
        } else {
            "spark_array"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types.len() {
            0 => Ok(empty_array_type()),
            _ => {
                let expr_type = arg_types
                    .iter()
                    .find(|f| !f.is_null())
                    .cloned()
                    .unwrap_or(DataType::Null);

                Ok(DataType::List(Arc::new(Field::new_list_field(
                    expr_type, true,
                ))))
            }
        }
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let data_types = args
            .arg_fields
            .iter()
            .map(|f| f.data_type())
            .cloned()
            .collect::<Vec<_>>();
        let contains_null =
            self.force_element_nullable || args.arg_fields.iter().any(|field| field.is_nullable());
        let return_type = match self.return_type(&data_types)? {
            DataType::List(field) => DataType::List(Arc::new(
                field.as_ref().clone().with_nullable(contains_null),
            )),
            data_type => data_type,
        };
        Ok(Arc::new(Field::new(self.name(), return_type, false)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, return_field, ..
        } = args;
        let value_nullable = match return_field.data_type() {
            DataType::List(field) | DataType::LargeList(field) => field.is_nullable(),
            _ => true,
        };
        let func = make_scalar_function(move |arrays| {
            make_array_inner_with_nullable(arrays, value_nullable)
        });
        func(args.as_slice())
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let first_type = arg_types.first().ok_or_else(|| {
            plan_datafusion_err!("Spark array function requires at least one argument")
        })?;
        // Spark non-ANSI semantics: when mixing strings with other (non-null) types,
        // coerce everything to string. DataFusion's `comparison_coercion` prefers
        // numeric types, which would break Spark's string-wins behavior and cause
        // runtime cast failures for values like `array('a', 1)`.
        let is_string_like = |dt: &DataType| {
            matches!(
                dt,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            )
        };
        let has_string = arg_types.iter().any(is_string_like);
        let has_non_string_non_null = arg_types
            .iter()
            .any(|dt| !is_string_like(dt) && !dt.is_null());
        if has_string && has_non_string_non_null {
            let string_type = if arg_types.iter().any(|dt| matches!(dt, DataType::LargeUtf8)) {
                DataType::LargeUtf8
            } else if arg_types.iter().any(|dt| matches!(dt, DataType::Utf8View)) {
                DataType::Utf8View
            } else {
                DataType::Utf8
            };
            return Ok(vec![string_type; arg_types.len()]);
        }
        // Spark's `CreateArray` uses `widerDecimalType` for DECIMAL and integral arguments.
        // At precision 38 it calls `boundedPreferIntegralDigits`, reducing scale before the
        // integral range. DataFusion's comparison coercion instead preserves the largest scale.
        let decimal_parts = |data_type: &DataType| match data_type {
            DataType::Decimal32(precision, scale)
            | DataType::Decimal64(precision, scale)
            | DataType::Decimal128(precision, scale)
            | DataType::Decimal256(precision, scale) => {
                Some((i32::from(*precision), i32::from(*scale)))
            }
            _ => None,
        };
        let integral_decimal_parts = |data_type: &DataType| match data_type {
            DataType::Int8 => Some((3, 0)),
            DataType::Int16 => Some((5, 0)),
            DataType::Int32 => Some((10, 0)),
            DataType::Int64 => Some((20, 0)),
            _ => None,
        };
        let decimal_or_integral = arg_types
            .iter()
            .map(|data_type| decimal_parts(data_type).or_else(|| integral_decimal_parts(data_type)))
            .collect::<Option<Vec<_>>>();
        if let Some(parts) = decimal_or_integral
            && arg_types
                .iter()
                .any(|data_type| decimal_parts(data_type).is_some())
        {
            let scale = parts
                .iter()
                .map(|(_, scale)| *scale)
                .max()
                .unwrap_or_default();
            let integral_digits = parts
                .iter()
                .map(|(precision, scale)| precision - scale)
                .max()
                .unwrap_or_default();
            let precision = scale + integral_digits;
            let (precision, scale) = if precision > 38 {
                (38, (scale - (precision - 38)).max(0))
            } else {
                (precision, scale)
            };
            let common = DataType::Decimal128(
                u8::try_from(precision)
                    .map_err(|error| plan_datafusion_err!("invalid decimal precision: {error}"))?,
                i8::try_from(scale)
                    .map_err(|error| plan_datafusion_err!("invalid decimal scale: {error}"))?,
            );
            return Ok(vec![common; arg_types.len()]);
        }
        let new_type = arg_types
            .iter()
            .skip(1)
            .try_fold(first_type.clone(), |acc, x| {
                // The coerced types found by `comparison_coercion` are not guaranteed to be
                // coercible for the arguments. `comparison_coercion` returns more loose
                // types that can be coerced to both `acc` and `x` for comparison purpose.
                // See `maybe_data_types` for the actual coercion.
                let coerced_type = comparison_coercion(&acc, x);
                if let Some(coerced_type) = coerced_type {
                    Ok(coerced_type)
                } else {
                    plan_err!("Coercion from {acc:?} to {x:?} failed.")
                }
            })?;
        // When any input is a floating-point type (Double/Float), keep it as Double
        // instead of promoting to Decimal128. Floats support NaN/Infinity which
        // Decimal128 cannot represent, causing runtime overflow errors.
        let new_type = if matches!(new_type, DataType::Decimal128(_, _))
            && arg_types.iter().any(|dt| dt.is_floating())
        {
            DataType::Float64
        } else {
            new_type
        };
        Ok(vec![new_type; arg_types.len()])
    }
}

// Empty array is a special case that is useful for many other array functions
pub(crate) fn empty_array_type() -> DataType {
    DataType::List(Arc::new(Field::new_list_field(DataType::Null, false)))
}

/// `make_array_inner` is the implementation of the `make_array` function.
/// Constructs an array using the input `data` as `ArrayRef`.
/// Returns a reference-counted `Array` instance result.
pub fn make_array_inner(arrays: &[ArrayRef]) -> Result<ArrayRef> {
    make_array_inner_with_nullable(arrays, true)
}

fn make_array_inner_with_nullable(arrays: &[ArrayRef], value_nullable: bool) -> Result<ArrayRef> {
    if arrays.is_empty() {
        let array = new_empty_array(&DataType::Null);
        return Ok(Arc::new(
            SingleRowListArrayBuilder::new(array)
                .with_nullable(false)
                .build_list_array(),
        ));
    }

    let data_type = arrays
        .iter()
        .map(|arr| arr.data_type())
        .find(|arr_type| !arr_type.is_null())
        .unwrap_or(&DataType::Null)
        .clone();

    match data_type {
        // Array or all nulls:
        DataType::Null => {
            let length = arrays.iter().map(|a| a.len()).sum();
            let array = new_null_array(&DataType::Null, length);
            let offsets =
                OffsetBuffer::from_lengths(std::iter::repeat_n(arrays.len(), arrays[0].len()));
            Ok(Arc::new(GenericListArray::<i32>::try_new(
                Arc::new(Field::new_list_field(DataType::Null, value_nullable)),
                offsets,
                array,
                None,
            )?))
        }
        DataType::LargeList(..) => array_array::<i64>(arrays, data_type, value_nullable),
        _ => array_array::<i32>(arrays, data_type, value_nullable),
    }
}

/// Convert one or more [`ArrayRef`] of the same type into a
/// `ListArray` or 'LargeListArray' depending on the offset size.
///
/// # Example (non nested)
///
/// Calling `array(col1, col2)` where col1 and col2 are non nested
/// would return a single new `ListArray`, where each row was a list
/// of 2 elements:
///
/// ```text
/// ┌─────────┐   ┌─────────┐           ┌──────────────┐
/// │ ┌─────┐ │   │ ┌─────┐ │           │ ┌──────────┐ │
/// │ │  A  │ │   │ │  X  │ │           │ │  [A, X]  │ │
/// │ ├─────┤ │   │ ├─────┤ │           │ ├──────────┤ │
/// │ │NULL │ │   │ │  Y  │ │──────────▶│ │[NULL, Y] │ │
/// │ ├─────┤ │   │ ├─────┤ │           │ ├──────────┤ │
/// │ │  C  │ │   │ │  Z  │ │           │ │  [C, Z]  │ │
/// │ └─────┘ │   │ └─────┘ │           │ └──────────┘ │
/// └─────────┘   └─────────┘           └──────────────┘
///   col1           col2                    output
/// ```
///
/// # Example (nested)
///
/// Calling `array(col1, col2)` where col1 and col2 are lists
/// would return a single new `ListArray`, where each row was a list
/// of the corresponding elements of col1 and col2.
///
/// ``` text
/// ┌──────────────┐   ┌──────────────┐        ┌─────────────────────────────┐
/// │ ┌──────────┐ │   │ ┌──────────┐ │        │ ┌────────────────────────┐  │
/// │ │  [A, X]  │ │   │ │    []    │ │        │ │    [[A, X], []]        │  │
/// │ ├──────────┤ │   │ ├──────────┤ │        │ ├────────────────────────┤  │
/// │ │[NULL, Y] │ │   │ │[Q, R, S] │ │───────▶│ │ [[NULL, Y], [Q, R, S]] │  │
/// │ ├──────────┤ │   │ ├──────────┤ │        │ ├────────────────────────│  │
/// │ │  [C, Z]  │ │   │ │   NULL   │ │        │ │    [[C, Z], NULL]      │  │
/// │ └──────────┘ │   │ └──────────┘ │        │ └────────────────────────┘  │
/// └──────────────┘   └──────────────┘        └─────────────────────────────┘
///      col1               col2                         output
/// ```
fn array_array<O: OffsetSizeTrait>(
    args: &[ArrayRef],
    data_type: DataType,
    value_nullable: bool,
) -> Result<ArrayRef> {
    // do not accept 0 arguments.
    if args.is_empty() {
        return plan_err!("Array requires at least one argument");
    }

    let mut data = vec![];
    let mut total_len = 0;
    for arg in args {
        let arg_data = if arg.as_any().is::<NullArray>() {
            ArrayData::new_empty(&data_type)
        } else {
            arg.to_data()
        };
        total_len += arg_data.len();
        data.push(arg_data);
    }

    let mut offsets: Vec<O> = Vec::with_capacity(total_len);
    offsets.push(O::usize_as(0));

    let capacity = Capacities::Array(total_len);
    let data_ref = data.iter().collect::<Vec<_>>();
    let mut mutable = MutableArrayData::with_capacities(data_ref, true, capacity);

    let num_rows = args[0].len();
    for row_idx in 0..num_rows {
        for (arr_idx, arg) in args.iter().enumerate() {
            if !arg.as_any().is::<NullArray>() && !arg.is_null(row_idx) && arg.is_valid(row_idx) {
                mutable.try_extend(arr_idx, row_idx, row_idx + 1)?;
            } else {
                mutable.try_extend_nulls(1)?;
            }
        }
        offsets.push(O::usize_as(mutable.len()));
    }
    let data = mutable.freeze();

    Ok(Arc::new(GenericListArray::<O>::try_new(
        Arc::new(Field::new_list_field(data_type, value_nullable)),
        OffsetBuffer::new(offsets.into()),
        make_array(data),
        None,
    )?))
}
