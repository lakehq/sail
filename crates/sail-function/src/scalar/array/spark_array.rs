/// [Credit]: <https://github.com/apache/datafusion/blob/c21d025df463ce623f9193c4b24d86141fce81ca/datafusion/functions-nested/src/make_array.rs>
/// Spark defaults to DataType::Int32 while DataFusion defaults to DataType::Int64.
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayData, ArrayRef, Capacities, GenericListArray, MutableArrayData, NullArray,
    OffsetSizeTrait, make_array, new_empty_array, new_null_array,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::utils::SingleRowListArrayBuilder;
use datafusion_common::{Result, plan_datafusion_err, plan_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use sail_common_datafusion::type_coercion::binary_type_coercion::comparison_coercion;
use sail_common_datafusion::type_coercion::nested::with_nested_field_metadata;

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
                let expr_type = element_type(arg_types);

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
        Ok(vec![new_type; arg_types.len()])
    }
}

// Empty array is a special case that is useful for many other array functions
pub(crate) fn empty_array_type() -> DataType {
    DataType::List(Arc::new(Field::new_list_field(DataType::Null, false)))
}

/// The type of the array elements, which is the type of the first non-null argument.
///
/// As in Spark's `CreateArray`, the arguments can differ in the nullability of nested fields,
/// and a nested field of the element type is nullable if it is nullable in any argument.
/// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/complexTypeCreator.scala#L90-L95>
fn element_type<'a>(data_types: impl IntoIterator<Item = &'a DataType>) -> DataType {
    let mut data_types = data_types
        .into_iter()
        .filter(|data_type| !data_type.is_null());
    match data_types.next() {
        Some(first) => data_types.fold(first.clone(), with_nested_field_metadata),
        None => DataType::Null,
    }
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

    let data_type = element_type(arrays.iter().map(|arr| arr.data_type()));

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
        // The result is a list whatever the element type, as the return type says.
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
        } else if arg.data_type() != &data_type {
            // The arguments can differ in the nullability of nested fields.
            let options = CastOptions {
                safe: false,
                ..Default::default()
            };
            cast_with_options(arg.as_ref(), &data_type, &options)?.to_data()
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

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{
        AsArray, FixedSizeListArray, Int32Array, LargeListArray, ListArray, MapArray, StructArray,
    };
    use datafusion::arrow::buffer::NullBuffer;
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::Int32Type;

    use super::*;

    fn int_field(name: &str, nullable: bool) -> FieldRef {
        Arc::new(Field::new(name, DataType::Int32, nullable))
    }

    fn list_type(nullable: bool) -> DataType {
        DataType::List(int_field("item", nullable))
    }

    fn map_type(nullable: bool) -> DataType {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![int_field("key", false), int_field("value", nullable)].into(),
                ),
                false,
            )),
            false,
        )
    }

    #[test]
    fn test_element_type_widens_nested_nullability() {
        let struct_type = |nullable: bool| {
            DataType::Struct(vec![int_field("a", nullable), int_field("b", false)].into())
        };
        let nested = |nullable: bool| {
            DataType::LargeList(Arc::new(Field::new(
                "element",
                DataType::Struct(
                    vec![Arc::new(Field::new(
                        "a",
                        DataType::FixedSizeList(int_field("item", nullable), 2),
                        false,
                    ))]
                    .into(),
                ),
                true,
            )))
        };
        let cases = [
            (list_type(false), list_type(true)),
            (
                DataType::LargeList(int_field("item", false)),
                DataType::LargeList(int_field("item", true)),
            ),
            (
                DataType::ListView(int_field("item", false)),
                DataType::ListView(int_field("item", true)),
            ),
            (
                DataType::LargeListView(int_field("item", false)),
                DataType::LargeListView(int_field("item", true)),
            ),
            (
                DataType::FixedSizeList(int_field("item", false), 3),
                DataType::FixedSizeList(int_field("item", true), 3),
            ),
            (map_type(false), map_type(true)),
            (struct_type(false), struct_type(true)),
            (nested(false), nested(true)),
        ];
        for (non_nullable, nullable) in cases {
            for data_types in [
                [non_nullable.clone(), nullable.clone()],
                [nullable.clone(), non_nullable.clone()],
                [nullable.clone(), nullable.clone()],
            ] {
                assert_eq!(element_type(&data_types), nullable);
            }
            assert_eq!(
                element_type(&[non_nullable.clone(), non_nullable.clone()]),
                non_nullable
            );
        }
    }

    #[test]
    fn test_element_type_merges_nested_nullability() {
        assert_eq!(
            element_type(&[DataType::Null, list_type(false), list_type(true)]),
            list_type(true)
        );
        assert_eq!(
            element_type(&[list_type(true), list_type(false)]),
            list_type(true)
        );
        assert_eq!(element_type(&[DataType::Null]), DataType::Null);
        let spark_array = SparkArray::new();
        assert_eq!(
            spark_array
                .return_type(&[list_type(false), DataType::Null, list_type(true)])
                .ok(),
            Some(DataType::List(Arc::new(Field::new_list_field(
                list_type(true),
                true
            ))))
        );
    }

    /// `array(array(id % 2), array(id, NULL))` used to panic in `MutableArrayData` because the
    /// arguments differ in the nullability of the list elements.
    #[test]
    fn test_make_array_of_lists_with_different_nullability() -> Result<()> {
        let non_nullable = ListArray::new(
            int_field("item", false),
            OffsetBuffer::from_lengths([1, 1]),
            Arc::new(Int32Array::from(vec![0, 1])),
            None,
        );
        let nullable = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(0), None]),
            None,
        ]);
        for arrays in [
            vec![
                Arc::new(non_nullable.clone()) as ArrayRef,
                Arc::new(nullable.clone()),
            ],
            vec![
                Arc::new(nullable.clone()) as ArrayRef,
                Arc::new(non_nullable.clone()),
            ],
        ] {
            let result = make_array_inner(&arrays)?;
            assert_eq!(
                result.data_type(),
                &DataType::List(Arc::new(Field::new_list_field(list_type(true), true)))
            );
            let result = result.as_list::<i32>();
            let values = result.values().as_list::<i32>();
            assert_eq!(values.len(), 4);
            for (index, arg) in arrays.iter().enumerate() {
                assert_eq!(
                    values.value(index).as_ref(),
                    arg.as_list::<i32>().value(0).as_ref()
                );
                assert_eq!(values.is_null(2 + index), arg.is_null(1));
            }
        }
        Ok(())
    }

    #[test]
    fn test_make_array_of_nested_types_with_different_nullability() -> Result<()> {
        let values = |nulls: Option<Vec<bool>>| -> ArrayRef {
            Arc::new(Int32Array::new(
                vec![1, 2].into(),
                nulls.map(NullBuffer::from),
            ))
        };
        let large_list = |nullable: bool, nulls| -> ArrayRef {
            Arc::new(LargeListArray::new(
                int_field("item", nullable),
                OffsetBuffer::from_lengths([2]),
                values(nulls),
                None,
            ))
        };
        let fixed_size_list = |nullable: bool, nulls| -> ArrayRef {
            Arc::new(FixedSizeListArray::new(
                int_field("item", nullable),
                2,
                values(nulls),
                None,
            ))
        };
        let r#struct = |nullable: bool, nulls| -> ArrayRef {
            Arc::new(StructArray::new(
                vec![int_field("a", nullable)].into(),
                vec![values(nulls)],
                None,
            ))
        };
        let map = |nullable: bool, nulls| -> ArrayRef {
            let entries = StructArray::new(
                vec![int_field("key", false), int_field("value", nullable)].into(),
                vec![values(None), values(nulls)],
                None,
            );
            let DataType::Map(field, _) = map_type(nullable) else {
                unreachable!()
            };
            Arc::new(MapArray::new(
                field,
                OffsetBuffer::from_lengths([2]),
                entries,
                None,
                false,
            ))
        };
        let constructors: [&dyn Fn(bool, Option<Vec<bool>>) -> ArrayRef; 4] =
            [&large_list, &fixed_size_list, &r#struct, &map];
        for constructor in constructors {
            let non_nullable = constructor(false, None);
            let nullable = constructor(true, Some(vec![true, false]));
            let result = make_array_inner(&[Arc::clone(&non_nullable), Arc::clone(&nullable)])?;
            let DataType::List(field) = result.data_type() else {
                unreachable!("{}", result.data_type())
            };
            let elements = result.as_list::<i32>().value(0);
            assert_eq!(field.data_type(), nullable.data_type());
            assert_eq!(elements.len(), 2);
            let expected = cast(&non_nullable, nullable.data_type())?;
            assert_eq!(
                elements.slice(0, 1).to_data(),
                expected.slice(0, 1).to_data()
            );
            assert_eq!(
                elements.slice(1, 1).to_data(),
                nullable.slice(0, 1).to_data()
            );
        }
        Ok(())
    }
}
