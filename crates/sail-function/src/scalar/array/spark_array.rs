/// [Credit]: <https://github.com/apache/datafusion/blob/c21d025df463ce623f9193c4b24d86141fce81ca/datafusion/functions-nested/src/make_array.rs>
/// Spark defaults to DataType::Int32 while DataFusion defaults to DataType::Int64.
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayData, ArrayRef, Capacities, GenericListArray, MutableArrayData, NullArray,
    OffsetSizeTrait, make_array, new_empty_array, new_null_array,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::utils::SingleRowListArrayBuilder;
use datafusion_common::{Result, plan_datafusion_err, plan_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use sail_common_datafusion::metadata::coerced_type_field;
use sail_common_datafusion::type_coercion::binary_type_coercion::comparison_coercion;
use sail_common_datafusion::utils::scalar::scalar_from_array;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkArray {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SparkArray {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkArray {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::UserDefined, TypeSignature::Nullary],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("spark_make_array")],
        }
    }
}

impl ScalarUDFImpl for SparkArray {
    fn name(&self) -> &str {
        "spark_array"
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
        let contains_null = args.arg_fields.iter().any(|f| f.is_nullable());
        let return_type = if data_types.is_empty() {
            empty_array_type()
        } else {
            let types = self.coerce_types(&data_types)?;
            let data_type = types
                .into_iter()
                .find(|t| !t.is_null())
                .unwrap_or(DataType::Null);
            let field = Field::new_list_field(data_type, contains_null);
            let sources = args
                .arg_fields
                .iter()
                .map(AsRef::as_ref)
                .collect::<Vec<_>>();
            DataType::List(Arc::new(coerced_type_field(&field, &sources)?))
        };
        Ok(Arc::new(Field::new(self.name(), return_type, false)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, return_field, ..
        } = args;
        let DataType::List(field) = return_field.data_type() else {
            return plan_err!("Spark array requires a list return field");
        };
        let scalar = args
            .iter()
            .all(|value| matches!(value, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args)?;
        let result = make_array_inner_with_field(&arrays, field.clone())?;
        if scalar {
            Ok(ColumnarValue::Scalar(scalar_from_array(&result, 0)?))
        } else {
            Ok(ColumnarValue::Array(result))
        }
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
        // Resolve nested qualifiers before analyzer casts replace the source fields.
        let sources = arg_types
            .iter()
            .map(|data_type| Field::new_list_field(data_type.clone(), true))
            .collect::<Vec<_>>();
        let new_type = coerced_type_field(
            &Field::new_list_field(new_type, true),
            &sources.iter().collect::<Vec<_>>(),
        )?
        .data_type()
        .clone();
        // Keep untyped NULL neutral for logical type consensus. The array builder
        // already accepts NullArray.
        Ok(arg_types
            .iter()
            .map(|source| {
                if source.is_null()
                    && matches!(
                        new_type,
                        DataType::FixedSizeBinary(16)
                            | DataType::Interval(arrow::datatypes::IntervalUnit::YearMonth)
                            | DataType::Duration(arrow::datatypes::TimeUnit::Microsecond)
                    )
                {
                    DataType::Null
                } else {
                    new_type.clone()
                }
            })
            .collect())
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
    let data_type = arrays
        .iter()
        .map(|array| array.data_type())
        .find(|data_type| !data_type.is_null())
        .cloned()
        .unwrap_or(DataType::Null);
    make_array_inner_with_field(arrays, Arc::new(Field::new_list_field(data_type, true)))
}

fn make_array_inner_with_field(arrays: &[ArrayRef], field: FieldRef) -> Result<ArrayRef> {
    if arrays.is_empty() {
        let array = new_empty_array(&DataType::Null);
        return Ok(Arc::new(
            SingleRowListArrayBuilder::new(array)
                .with_nullable(false)
                .build_list_array(),
        ));
    }

    match field.data_type() {
        // Array or all nulls:
        DataType::Null => {
            let length = arrays.iter().map(|a| a.len()).sum();
            let array = new_null_array(&DataType::Null, length);
            let offsets =
                OffsetBuffer::from_lengths(std::iter::repeat_n(arrays.len(), arrays[0].len()));
            Ok(Arc::new(GenericListArray::<i32>::try_new(
                field, offsets, array, None,
            )?))
        }
        _ => array_array::<i32>(arrays, field),
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
fn array_array<O: OffsetSizeTrait>(args: &[ArrayRef], field: FieldRef) -> Result<ArrayRef> {
    // do not accept 0 arguments.
    if args.is_empty() {
        return plan_err!("Array requires at least one argument");
    }

    let mut data = vec![];
    let mut total_len = 0;
    for arg in args {
        let arg_data = if arg.as_any().is::<NullArray>() {
            ArrayData::new_empty(field.data_type())
        } else if arg.data_type() == field.data_type() {
            arg.to_data()
        } else {
            cast(arg, field.data_type())?.to_data()
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
        field,
        OffsetBuffer::new(offsets.into()),
        make_array(data),
        None,
    )?))
}

#[cfg(test)]
mod tests {
    use sail_common_datafusion::uuid::{UUID_METADATA_KEY, contains_uuid};

    use super::*;

    #[test]
    fn array_null_coercion_and_runtime_keep_resolved_identity() -> Result<()> {
        let function = SparkArray::new();
        let uuid = Arc::new(
            Field::new("source", DataType::FixedSizeBinary(16), true).with_metadata(
                std::collections::HashMap::from([
                    (UUID_METADATA_KEY.to_string(), "uuid".to_string()),
                    ("PARQUET:field_id".to_string(), "17".to_string()),
                ]),
            ),
        );
        for (other_type, expected_uuid) in [
            (DataType::Null, true),
            (DataType::FixedSizeBinary(16), false),
        ] {
            let fields = [
                uuid.clone(),
                Arc::new(Field::new("other", other_type.clone(), true)),
            ];
            let types = [DataType::FixedSizeBinary(16), other_type.clone()];
            assert_eq!(function.coerce_types(&types)?, types);
            let output = function.return_field_from_args(ReturnFieldArgs {
                arg_fields: &fields,
                scalar_arguments: &[None, None],
            })?;
            assert_eq!(contains_uuid(&output), expected_uuid);
            let DataType::List(child) = output.data_type() else {
                return plan_err!("expected a list field");
            };
            assert!(!child.metadata().contains_key("PARQUET:field_id"));
            let arrays = [new_null_array(&types[0], 2), new_null_array(&other_type, 2)];
            let result = make_array_inner_with_field(&arrays, child.clone())?;
            assert_eq!(result.data_type(), output.data_type());
            assert_eq!(result.len(), 2);
            let result = function.invoke_with_args(ScalarFunctionArgs {
                args: types
                    .iter()
                    .map(|data_type| {
                        datafusion_common::ScalarValue::try_from(data_type)
                            .map(ColumnarValue::Scalar)
                    })
                    .collect::<Result<Vec<_>>>()?,
                arg_fields: fields.to_vec(),
                number_rows: 1,
                return_field: output.clone(),
                config_options: Arc::new(Default::default()),
            })?;
            assert_eq!(&result.data_type(), output.data_type());
        }
        Ok(())
    }
}
