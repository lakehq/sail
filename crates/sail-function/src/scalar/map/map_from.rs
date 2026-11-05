use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, MapArray, NullArray, NullBufferBuilder, StructArray,
};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, exec_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_functions::utils::make_scalar_function;
use datafusion_spark::function::map::map_from_arrays::MapFromArrays;
use datafusion_spark::function::map::map_from_entries::MapFromEntries;

#[cfg(test)]
mod tests {
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

    fn list(
        values: ArrayRef,
        offsets: &[i32],
        nullable: bool,
        nulls: Option<NullBuffer>,
    ) -> ArrayRef {
        Arc::new(ListArray::new(
            Arc::new(Field::new("item", values.data_type().clone(), nullable)),
            OffsetBuffer::new(offsets.to_vec().into()),
            values,
            nulls,
        ))
    }

    #[expect(clippy::panic, reason = "Assert map entry field types in test helper")]
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
        assert_eq!(arrays.nullable(), None);
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
    fn outer_nullability_override_validates_scalar_and_batch_outputs() -> Result<()> {
        let keys: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let values: ArrayRef = Arc::new(Int32Array::from(vec![Some(10), None]));
        for nullable in [None, Some(false), Some(true)] {
            let udf = SparkMapFromArrays::new(false).with_nullable(nullable);
            for nulls in [None, Some(NullBuffer::from(vec![true, false]))] {
                let args = [
                    list(keys.clone(), &[0, 1, 2], false, nulls.clone()),
                    list(values.clone(), &[0, 1, 2], true, None),
                ];
                let fields = args
                    .iter()
                    .map(|arg| Arc::new(Field::new("input", arg.data_type().clone(), true)))
                    .collect::<Vec<_>>();
                let field = udf.return_field_from_args(ReturnFieldArgs {
                    arg_fields: &fields,
                    scalar_arguments: &[None, None],
                })?;
                assert_eq!(field.is_nullable(), nullable.unwrap_or(true));
                assert!(map_fields(field.data_type())[1].is_nullable());
                let result = invoke(
                    &udf,
                    args.iter().cloned().map(ColumnarValue::Array).collect(),
                );
                let rejects = nullable == Some(false) && nulls.is_some();
                assert_eq!(result.is_err(), rejects);
                if let Ok(result) = result {
                    let result = result.into_array(2)?;
                    assert_eq!(result.null_count(), usize::from(nulls.is_some()));
                }
                for row in 0..2 {
                    let scalar = invoke(
                        &udf,
                        args.iter()
                            .map(|arg| {
                                ScalarValue::try_from_array(arg, row).map(ColumnarValue::Scalar)
                            })
                            .collect::<Result<_>>()?,
                    );
                    assert_eq!(scalar.is_err(), rejects && row == 1);
                    if let Ok(scalar) = scalar {
                        assert!(matches!(scalar, ColumnarValue::Scalar(_)));
                    }
                }
                let empty = invoke(
                    &udf,
                    args.iter()
                        .map(|arg| ColumnarValue::Array(arg.slice(0, 0)))
                        .collect(),
                )?
                .into_array(0)?;
                assert_eq!(empty.len(), 0);
            }
        }
        assert!(
            invoke(
                &SparkMapFromArrays::new(false).with_nullable(Some(false)),
                vec![ColumnarValue::Scalar(ScalarValue::Null); 2],
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic,
        reason = "Assert map representation before testing schema changes"
    )]
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
        assert!(
            retype_map_value_nullability(Arc::new(NullArray::new(1)), &DataType::Null).is_err()
        );
        Ok(())
    }
}

use crate::scalar::map::utils::{
    get_element_type, get_list_offsets, get_list_values, map_from_keys_values_offsets_nulls,
    map_type_from_key_value_types,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMapFromArrays {
    delegate: MapFromArrays,
    last_value_wins: bool,
    value_contains_null: Option<bool>,
    nullable: Option<bool>,
}

impl SparkMapFromArrays {
    pub fn new(last_value_wins: bool) -> Self {
        Self {
            delegate: MapFromArrays::new(),
            last_value_wins,
            value_contains_null: None,
            nullable: None,
        }
    }

    pub fn last_value_wins(&self) -> bool {
        self.last_value_wins
    }

    pub fn with_value_contains_null(mut self, value_contains_null: Option<bool>) -> Self {
        self.value_contains_null = value_contains_null;
        self
    }

    pub fn value_contains_null(&self) -> Option<bool> {
        self.value_contains_null
    }

    /// Override conservative intermediate-array nullability when the planner has a proof.
    pub fn with_nullable(mut self, nullable: Option<bool>) -> Self {
        self.nullable = nullable;
        self
    }

    pub fn nullable(&self) -> Option<bool> {
        self.nullable
    }
}

impl ScalarUDFImpl for SparkMapFromArrays {
    fn name(&self) -> &str {
        self.delegate.name()
    }

    fn signature(&self) -> &Signature {
        self.delegate.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.delegate.return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let arg_fields = args.arg_fields;
        let field = self.delegate.return_field_from_args(args)?;
        let derived = match arg_fields[1].data_type() {
            DataType::List(element)
            | DataType::LargeList(element)
            | DataType::FixedSizeList(element, _) => element.is_nullable(),
            DataType::Null => true,
            other => return exec_err!("map_from_arrays: expected list, got {other:?}"),
        };
        let data_type = with_map_value_nullability(
            field.data_type(),
            self.value_contains_null.unwrap_or(derived),
        )?;
        Ok(Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(data_type)
                .with_nullable(self.nullable.unwrap_or(field.is_nullable())),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let last_value_wins = self.last_value_wins;
        let return_field = args.return_field;
        make_scalar_function(
            move |args| {
                let map = map_from_arrays_inner(args, last_value_wins)?;
                if !return_field.is_nullable() && map.null_count() != 0 {
                    return exec_err!("map_from_arrays: NULL output for non-nullable return field");
                }
                retype_map_value_nullability(map, return_field.data_type())
            },
            vec![],
        )(&args.args)
    }
}

fn map_from_arrays_inner(args: &[ArrayRef], last_value_wins: bool) -> Result<ArrayRef> {
    let [keys, values] = take_function_args("map_from_arrays", args)?;

    if *keys.data_type() == DataType::Null || *values.data_type() == DataType::Null {
        return Ok(cast(
            &NullArray::new(keys.len()),
            &map_type_from_key_value_types(
                get_element_type(keys.data_type())?,
                get_element_type(values.data_type())?,
            ),
        )?);
    }

    map_from_keys_values_offsets_nulls(
        get_list_values(keys)?,
        get_list_values(values)?,
        &get_list_offsets(keys)?,
        &get_list_offsets(values)?,
        keys.nulls(),
        values.nulls(),
        last_value_wins,
    )
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMapFromEntries {
    delegate: MapFromEntries,
    last_value_wins: bool,
}

impl SparkMapFromEntries {
    pub fn new(last_value_wins: bool) -> Self {
        Self {
            delegate: MapFromEntries::new(),
            last_value_wins,
        }
    }

    pub fn last_value_wins(&self) -> bool {
        self.last_value_wins
    }
}

impl ScalarUDFImpl for SparkMapFromEntries {
    fn name(&self) -> &str {
        self.delegate.name()
    }

    fn signature(&self) -> &Signature {
        self.delegate.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.delegate.return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let arg_fields = args.arg_fields;
        let field = self.delegate.return_field_from_args(args)?;
        let element = get_element_type(arg_fields[0].data_type())?;
        let DataType::Struct(fields) = element else {
            return exec_err!("map_from_entries: expected struct, got {element:?}");
        };
        let data_type = with_map_value_nullability(field.data_type(), fields[1].is_nullable())?;
        Ok(Arc::new(field.as_ref().clone().with_data_type(data_type)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let last_value_wins = self.last_value_wins;
        let return_field = args.return_field;
        make_scalar_function(
            move |args| {
                let map = map_from_entries_inner(args, last_value_wins)?;
                retype_map_value_nullability(map, return_field.data_type())
            },
            vec![],
        )(&args.args)
    }
}

fn with_map_value_nullability(data_type: &DataType, nullable: bool) -> Result<DataType> {
    let DataType::Map(entries, sorted) = data_type else {
        return exec_err!("expected map type, got {data_type:?}");
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return exec_err!("expected map entries struct, got {entries:?}");
    };
    let [key, value] = fields.as_ref() else {
        return exec_err!("expected two map entry fields, got {fields:?}");
    };
    let fields = vec![
        key.clone(),
        Arc::new(value.as_ref().clone().with_nullable(nullable)),
    ];
    Ok(DataType::Map(
        Arc::new(
            entries
                .as_ref()
                .clone()
                .with_data_type(DataType::Struct(fields.into())),
        ),
        *sorted,
    ))
}

/// Retype only the map value field's nullability, without casting nested values.
fn retype_map_value_nullability(array: ArrayRef, target: &DataType) -> Result<ArrayRef> {
    let Some(map) = array.as_any().downcast_ref::<MapArray>() else {
        return exec_err!("expected map array, got {:?}", array.data_type());
    };
    let DataType::Map(entries, sorted) = target else {
        return exec_err!("expected map return type, got {target:?}");
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return exec_err!("expected map entries struct, got {entries:?}");
    };
    let [_, value] = fields.as_ref() else {
        return exec_err!("expected two map entry fields, got {fields:?}");
    };
    if with_map_value_nullability(map.data_type(), value.is_nullable())? != *target {
        return exec_err!(
            "map retype may only change value nullability: {:?} to {target:?}",
            map.data_type()
        );
    }
    if map.data_type() == target {
        return Ok(array);
    }
    // No entry validity mask may hide a NULL value when narrowing the value field.
    let values = StructArray::try_new(
        fields.clone(),
        vec![map.keys().clone(), map.values().clone()],
        None,
    )?;
    Ok(Arc::new(MapArray::try_new(
        entries.clone(),
        map.offsets().clone(),
        values,
        map.nulls().cloned(),
        *sorted,
    )?))
}

fn map_from_entries_inner(args: &[ArrayRef], last_value_wins: bool) -> Result<ArrayRef> {
    let [entries] = take_function_args("map_from_entries", args)?;
    let entries_offsets = get_list_offsets(entries)?;
    let entries_values = get_list_values(entries)?;

    let (flat_keys, flat_values) = match entries_values.as_any().downcast_ref::<StructArray>() {
        Some(entries) => Ok((entries.column(0), entries.column(1))),
        None => exec_err!(
            "map_from_entries: expected array<struct<key, value>>, got {:?}",
            entries_values.data_type()
        ),
    }?;

    let entries_with_nulls = entries_values.nulls().and_then(|entries_inner_nulls| {
        let mut builder = NullBufferBuilder::new_with_len(0);
        let mut current_offset = entries_offsets
            .first()
            .map(|offset| *offset as usize)
            .unwrap_or(0);

        for next_offset in entries_offsets.iter().skip(1) {
            let entry_count = *next_offset as usize - current_offset;
            builder.append(
                entries_inner_nulls
                    .slice(current_offset, entry_count)
                    .null_count()
                    == 0,
            );
            current_offset = *next_offset as usize;
        }
        builder.finish()
    });

    let result_nulls = NullBuffer::union(entries.nulls(), entries_with_nulls.as_ref());

    map_from_keys_values_offsets_nulls(
        flat_keys,
        flat_values,
        &entries_offsets,
        &entries_offsets,
        None,
        result_nulls.as_ref(),
        last_value_wins,
    )
}
