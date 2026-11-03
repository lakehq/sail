use std::collections::HashMap;

use datafusion::arrow::array::{AsArray, Int32Array, ListArray};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{Field, Fields};
use datafusion_common::ScalarValue;

use super::*;

fn invoke(udf: &dyn ScalarUDFImpl, args: Vec<ColumnarValue>) -> Result<ColumnarValue> {
    let arg_fields = args
        .iter()
        .map(|arg| Arc::new(Field::new("input", arg.data_type(), true)))
        .collect::<Vec<_>>();
    let return_field = udf.return_field_from_args(ReturnFieldArgs {
        arg_fields: &arg_fields,
        scalar_arguments: &vec![None; args.len()],
    })?;
    let expected = return_field.data_type().clone();
    let number_rows = args
        .iter()
        .find_map(|arg| match arg {
            ColumnarValue::Array(array) => Some(array.len()),
            _ => None,
        })
        .unwrap_or(1);
    let result = udf.invoke_with_args(ScalarFunctionArgs {
        args,
        arg_fields,
        number_rows,
        return_field,
        config_options: Arc::new(Default::default()),
    })?;
    assert_eq!(result.data_type(), expected);
    Ok(result)
}

fn list(values: ArrayRef, offsets: &[i32], nullable: bool, nulls: Option<NullBuffer>) -> ArrayRef {
    Arc::new(ListArray::new(
        Arc::new(Field::new("item", values.data_type().clone(), nullable)),
        OffsetBuffer::new(offsets.to_vec().into()),
        values,
        nulls,
    ))
}

fn map_fields(data_type: &DataType) -> &Fields {
    let DataType::Map(entries, _) = data_type else {
        panic!("expected map")
    };
    let DataType::Struct(fields) = entries.data_type() else {
        panic!("expected entries")
    };
    fields
}

#[test]
fn infer_value_and_outer_nullability() -> Result<()> {
    let arrays = SparkMapFromArrays::new(false);
    assert_eq!(arrays.value_contains_null(), None);
    let nested = DataType::Struct(
        vec![
            Field::new("b", DataType::Int32, false)
                .with_metadata(HashMap::from([("tag".to_owned(), "kept".to_owned())])),
        ]
        .into(),
    );
    for nullable in [false, true] {
        let element = Arc::new(Field::new("item", nested.clone(), nullable));
        for values in [
            DataType::List(element.clone()),
            DataType::LargeList(element.clone()),
            DataType::FixedSizeList(element.clone(), 1),
            DataType::Null,
        ] {
            for override_ in [None, Some(false), Some(true)] {
                let args = [
                    Arc::new(Field::new(
                        "keys",
                        DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
                        false,
                    )),
                    Arc::new(Field::new("values", values.clone(), nullable)),
                ];
                let result = SparkMapFromArrays::new(false)
                    .with_value_contains_null(override_)
                    .return_field_from_args(ReturnFieldArgs {
                        arg_fields: &args,
                        scalar_arguments: &[None, None],
                    })?;
                assert_eq!(result.is_nullable(), nullable);
                let fields = map_fields(result.data_type());
                assert_eq!(
                    fields[1].is_nullable(),
                    override_.unwrap_or(nullable || values == DataType::Null)
                );
                assert_eq!(fields[1].data_type(), get_element_type(&values)?);
                assert!(!fields[0].is_nullable());
            }
        }
    }
    for entry_nullable in [false, true] {
        let entry = Arc::new(Field::new(
            "entry",
            DataType::Struct(
                vec![
                    Field::new("k", DataType::Int32, false),
                    Field::new("v", nested.clone(), false),
                ]
                .into(),
            ),
            entry_nullable,
        ));
        for data_type in [
            DataType::List(entry.clone()),
            DataType::LargeList(entry.clone()),
            DataType::FixedSizeList(entry, 1),
        ] {
            let result =
                SparkMapFromEntries::new(false).return_field_from_args(ReturnFieldArgs {
                    arg_fields: &[Arc::new(Field::new("input", data_type, false))],
                    scalar_arguments: &[None],
                })?;
            assert_eq!(result.is_nullable(), entry_nullable);
            assert!(!map_fields(result.data_type())[1].is_nullable());
            assert_eq!(map_fields(result.data_type())[1].data_type(), &nested);
        }
    }
    assert!(
        arrays
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[],
                scalar_arguments: &[]
            })
            .is_err()
    );
    assert!(
        SparkMapFromEntries::new(false)
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::new(Field::new("bad", DataType::Int32, false))],
                scalar_arguments: &[None],
            })
            .is_err()
    );
    Ok(())
}

#[test]
fn constructors_preserve_nested_scalar_batch_empty_and_null() -> Result<()> {
    let keys: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
    let values: ArrayRef = Arc::new(StructArray::new(
        vec![Field::new("b", DataType::Int32, false)].into(),
        vec![Arc::new(Int32Array::from(vec![10, 20]))],
        None,
    ));
    let offsets = &[0, 1, 1, 2, 2];
    let nulls = Some(NullBuffer::from(vec![true, true, true, false]));
    let arrays = vec![
        list(keys.clone(), offsets, false, nulls.clone()),
        list(values.clone(), offsets, false, nulls.clone()),
    ];
    let entries: ArrayRef = Arc::new(StructArray::new(
        vec![
            Field::new("k", DataType::Int32, false),
            Field::new("v", values.data_type().clone(), false),
        ]
        .into(),
        vec![keys, values],
        None,
    ));
    let entries = vec![list(entries, offsets, false, nulls)];
    for (udf, args) in [
        (
            &SparkMapFromArrays::new(false) as &dyn ScalarUDFImpl,
            arrays,
        ),
        (
            &SparkMapFromEntries::new(false) as &dyn ScalarUDFImpl,
            entries,
        ),
    ] {
        let batch = invoke(
            udf,
            args.iter().cloned().map(ColumnarValue::Array).collect(),
        )?
        .into_array(4)?;
        let map = batch.as_map();
        assert_eq!(map.offsets().as_ref(), &[0, 1, 1, 2, 2]);
        assert!(map.is_null(3));
        assert!(!map_fields(map.data_type())[1].is_nullable());
        assert_eq!(
            map.values()
                .as_struct()
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>(),
            Some(&Int32Array::from(vec![10, 20]))
        );
        for row in 0..4 {
            let scalar = invoke(
                udf,
                args.iter()
                    .map(|arg| ScalarValue::try_from_array(arg, row).map(ColumnarValue::Scalar))
                    .collect::<Result<_>>()?,
            )?;
            assert!(matches!(scalar, ColumnarValue::Scalar(_)));
            assert_eq!(
                scalar.into_array(1)?.to_data(),
                batch.slice(row, 1).to_data()
            );
        }
        let empty = invoke(
            udf,
            args.iter()
                .map(|arg| ColumnarValue::Array(arg.slice(0, 0)))
                .collect(),
        )?
        .into_array(0)?;
        assert_eq!(empty.len(), 0);
    }
    let null = invoke(
        &SparkMapFromArrays::new(false),
        vec![
            ColumnarValue::Scalar(ScalarValue::Null),
            ColumnarValue::Scalar(ScalarValue::Null),
        ],
    )?;
    assert!(matches!(null, ColumnarValue::Scalar(_)));
    assert!(null.into_array(1)?.is_null(0));
    Ok(())
}

#[test]
fn constructors_validate_duplicates_and_nullable_entries() -> Result<()> {
    let keys: ArrayRef = Arc::new(Int32Array::from(vec![1, 1]));
    let values: ArrayRef = Arc::new(Int32Array::from(vec![10, 20]));
    let entry_fields: Fields = vec![
        Field::new("k", DataType::Int32, false),
        Field::new("v", DataType::Int32, false),
    ]
    .into();
    let entries: ArrayRef = Arc::new(StructArray::new(
        entry_fields.clone(),
        vec![keys.clone(), values.clone()],
        None,
    ));
    let arrays = vec![
        list(keys.clone(), &[0, 2], false, None),
        list(values.clone(), &[0, 2], false, None),
    ];
    let entry_args = vec![list(entries, &[0, 2], false, None)];
    for (reject, accept, args) in [
        (
            &SparkMapFromArrays::new(false) as &dyn ScalarUDFImpl,
            &SparkMapFromArrays::new(true) as &dyn ScalarUDFImpl,
            arrays,
        ),
        (
            &SparkMapFromEntries::new(false) as &dyn ScalarUDFImpl,
            &SparkMapFromEntries::new(true) as &dyn ScalarUDFImpl,
            entry_args,
        ),
    ] {
        let args = args
            .into_iter()
            .map(ColumnarValue::Array)
            .collect::<Vec<_>>();
        assert!(invoke(reject, args.clone()).is_err());
        let result = invoke(accept, args)?.into_array(1)?;
        assert_eq!(
            result
                .as_map()
                .values()
                .as_any()
                .downcast_ref::<Int32Array>(),
            Some(&Int32Array::from(vec![20]))
        );
    }
    let entries: ArrayRef = Arc::new(StructArray::new(
        entry_fields,
        vec![keys, values],
        Some(NullBuffer::from(vec![true, false])),
    ));
    let result = invoke(
        &SparkMapFromEntries::new(false),
        vec![ColumnarValue::Array(list(entries, &[0, 1, 2], true, None))],
    )?
    .into_array(2)?;
    assert!(!result.is_null(0));
    assert!(result.is_null(1));
    assert!(!map_fields(result.data_type())[1].is_nullable());
    Ok(())
}

#[test]
fn retype_rejects_null_values_and_any_other_schema_change() -> Result<()> {
    let keys: ArrayRef = Arc::new(Int32Array::from(vec![1]));
    for values in [
        Arc::new(Int32Array::from(vec![Some(2)])) as ArrayRef,
        Arc::new(Int32Array::from(vec![None])),
    ] {
        let source = map_from_arrays_inner(
            &[
                list(keys.clone(), &[0, 1], false, None),
                list(values.clone(), &[0, 1], true, None),
            ],
            false,
        )?;
        let target = with_map_value_nullability(source.data_type(), false)?;
        let result = retype_map_value_nullability(source.clone(), &target);
        assert_eq!(result.is_err(), values.null_count() != 0);
        if let Ok(result) = result {
            assert_eq!(result.data_type(), &target);
            assert_eq!(
                retype_map_value_nullability(result, source.data_type())?.to_data(),
                source.to_data()
            );
        }
    }
    let nested: ArrayRef = Arc::new(StructArray::new(
        vec![Field::new("b", DataType::Int32, false)].into(),
        vec![keys.clone()],
        None,
    ));
    let source = map_from_arrays_inner(
        &[
            list(keys, &[0, 1], false, None),
            list(nested, &[0, 1], false, None),
        ],
        false,
    )?;
    let DataType::Map(entry, sorted) = source.data_type() else {
        panic!("expected map")
    };
    let fields = map_fields(source.data_type());
    let mut targets = vec![
        DataType::Null,
        DataType::Map(entry.clone(), !sorted),
        DataType::Map(
            Arc::new(entry.as_ref().clone().with_name("renamed")),
            *sorted,
        ),
        DataType::Map(
            Arc::new(entry.as_ref().clone().with_nullable(true)),
            *sorted,
        ),
    ];
    for index in [0, 1] {
        for field in [
            fields[index].as_ref().clone().with_name("renamed"),
            fields[index]
                .as_ref()
                .clone()
                .with_metadata(HashMap::from([("new".to_owned(), "metadata".to_owned())])),
            fields[index]
                .as_ref()
                .clone()
                .with_data_type(DataType::Int64),
        ] {
            let mut changed = fields.to_vec();
            changed[index] = Arc::new(field);
            targets.push(DataType::Map(
                Arc::new(
                    entry
                        .as_ref()
                        .clone()
                        .with_data_type(DataType::Struct(changed.into())),
                ),
                *sorted,
            ));
        }
    }
    for target in targets {
        assert!(
            retype_map_value_nullability(source.clone(), &target).is_err(),
            "{target:?}"
        );
    }
    assert!(retype_map_value_nullability(Arc::new(NullArray::new(1)), &DataType::Null).is_err());
    Ok(())
}
