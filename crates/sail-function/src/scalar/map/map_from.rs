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
#[path = "map_from_tests.rs"]
mod tests;

use crate::scalar::map::utils::{
    get_element_type, get_list_offsets, get_list_values, map_from_keys_values_offsets_nulls,
    map_type_from_key_value_types,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMapFromArrays {
    delegate: MapFromArrays,
    last_value_wins: bool,
    value_contains_null: Option<bool>,
}

impl SparkMapFromArrays {
    pub fn new(last_value_wins: bool) -> Self {
        Self {
            delegate: MapFromArrays::new(),
            last_value_wins,
            value_contains_null: None,
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
        Ok(Arc::new(field.as_ref().clone().with_data_type(data_type)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let last_value_wins = self.last_value_wins;
        let return_field = args.return_field;
        make_scalar_function(
            move |args| {
                let map = map_from_arrays_inner(args, last_value_wins)?;
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
