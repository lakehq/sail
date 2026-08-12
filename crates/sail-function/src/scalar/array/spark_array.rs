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
use datafusion_common::{Result, ScalarValue, plan_datafusion_err, plan_err};
use datafusion_expr::type_coercion::binary::comparison_coercion;
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use sail_common_datafusion::utils::data_type::merge_spark_time_metadata;

use crate::functions_nested_utils::make_scalar_function;

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
        let return_type = match self.return_type(&data_types)? {
            DataType::List(field) => {
                let field = field.as_ref().clone().with_nullable(contains_null);
                let field = args.arg_fields.iter().try_fold(field, |target, source| {
                    merge_spark_time_metadata(source, &target)
                })?;
                DataType::List(Arc::new(field))
            }
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
        let return_type = return_field.data_type().clone();
        let scalar_return_type = return_type.clone();
        let func = make_scalar_function(move |arrays| {
            make_array_inner_with_nullable(arrays, &return_type, value_nullable)
        });
        match (func(args.as_slice())?, scalar_return_type) {
            // `ScalarValue::try_from_array` keeps the child type but rebuilds its
            // field without metadata. Restore the exact field promised at planning.
            (ColumnarValue::Scalar(ScalarValue::List(array)), DataType::List(field)) => Ok(
                ColumnarValue::Scalar(ScalarValue::List(Arc::new(GenericListArray::try_new(
                    field,
                    array.offsets().clone(),
                    array.values().clone(),
                    array.nulls().cloned(),
                )?))),
            ),
            (value, _) => Ok(value),
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
        let target = Field::new_list_field(new_type, true);
        let target = arg_types.iter().try_fold(target, |target, source| {
            merge_spark_time_metadata(&Field::new("source", source.clone(), true), &target)
        })?;
        let new_type = target.data_type().clone();
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
    let data_type = arrays
        .iter()
        .map(|array| array.data_type())
        .find(|data_type| !data_type.is_null())
        .unwrap_or(&DataType::Null)
        .clone();
    make_array_inner_with_nullable(
        arrays,
        &DataType::List(Arc::new(Field::new_list_field(data_type, true))),
        true,
    )
}

fn make_array_inner_with_nullable(
    arrays: &[ArrayRef],
    return_type: &DataType,
    value_nullable: bool,
) -> Result<ArrayRef> {
    if arrays.is_empty() {
        let array = new_empty_array(&DataType::Null);
        return Ok(Arc::new(
            SingleRowListArrayBuilder::new(array)
                .with_nullable(false)
                .build_list_array(),
        ));
    }

    let DataType::List(field) = return_type else {
        return plan_err!("Spark array expected List return type, got {return_type}");
    };
    let data_type = field.data_type().clone();

    match data_type {
        // Array or all nulls:
        DataType::Null => {
            let length = arrays.iter().map(|a| a.len()).sum();
            let array = new_null_array(&DataType::Null, length);
            Ok(Arc::new(
                SingleRowListArrayBuilder::new(array)
                    .with_nullable(value_nullable)
                    .build_list_array(),
            ))
        }
        DataType::LargeList(..) => array_array::<i64>(arrays, field.clone(), value_nullable),
        _ => array_array::<i32>(arrays, field.clone(), value_nullable),
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
    field: FieldRef,
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
            ArrayData::new_empty(field.data_type())
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
                mutable.extend(arr_idx, row_idx, row_idx + 1);
            } else {
                mutable.extend_nulls(1);
            }
        }
        offsets.push(O::usize_as(mutable.len()));
    }
    let data = mutable.freeze();

    Ok(Arc::new(GenericListArray::<O>::try_new(
        Arc::new(field.as_ref().clone().with_nullable(value_nullable)),
        OffsetBuffer::new(offsets.into()),
        make_array(data),
        None,
    )?))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::datatypes::TimeUnit;
    use datafusion_common::ScalarValue;
    use datafusion_common::config::ConfigOptions;
    use sail_common::spec::SAIL_SPARK_TIME_PRECISION_METADATA_KEY;

    use super::*;

    #[test]
    fn scalar_time_array_matches_planned_child_metadata() -> Result<()> {
        let metadata = HashMap::from([(
            SAIL_SPARK_TIME_PRECISION_METADATA_KEY.to_string(),
            "1".to_string(),
        )]);
        let arg_field = Arc::new(
            Field::new(
                "current_time",
                DataType::Time32(TimeUnit::Millisecond),
                false,
            )
            .with_metadata(metadata),
        );
        let udf = SparkArray::new();
        let arg_fields = vec![arg_field];
        let scalar_arguments = [None];
        let return_field = udf.return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &scalar_arguments,
        })?;
        let output = udf.invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Time32Millisecond(Some(
                12_345,
            )))],
            arg_fields,
            number_rows: 2,
            return_field: Arc::clone(&return_field),
            config_options: Arc::new(ConfigOptions::default()),
        })?;
        assert_eq!(&output.data_type(), return_field.data_type());
        let output = output.into_array(2)?;

        assert_eq!(output.data_type(), return_field.data_type());
        Ok(())
    }
}
