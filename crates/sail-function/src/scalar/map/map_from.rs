use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, NullArray, NullBufferBuilder, StructArray};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_functions::utils::make_scalar_function;
use datafusion_spark::function::map::map_from_arrays::MapFromArrays;
use datafusion_spark::function::map::map_from_entries::MapFromEntries;

use crate::scalar::map::utils::{
    get_element_field, get_list_offsets, get_list_values, map_from_keys_values_offsets_nulls,
    map_type_from_key_value_fields,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMapFromArrays {
    delegate: MapFromArrays,
    last_value_wins: bool,
}

impl SparkMapFromArrays {
    pub fn new(last_value_wins: bool) -> Self {
        Self {
            delegate: MapFromArrays::new(),
            last_value_wins,
        }
    }

    pub fn last_value_wins(&self) -> bool {
        self.last_value_wins
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
        let [keys, values] = args.arg_fields else {
            return internal_err!("map_from_arrays expects exactly 2 arguments");
        };
        let data_type = map_type_from_key_value_fields(
            get_element_field(keys.data_type())?.as_ref(),
            get_element_field(values.data_type())?.as_ref(),
        );
        Ok(Arc::new(Field::new(
            self.name(),
            data_type,
            keys.is_nullable() || values.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let last_value_wins = self.last_value_wins;
        let return_type = args.return_field.data_type();
        make_scalar_function(
            move |args| map_from_arrays_inner(args, last_value_wins, return_type),
            vec![],
        )(&args.args)
    }
}

fn map_from_arrays_inner(
    args: &[ArrayRef],
    last_value_wins: bool,
    return_type: &DataType,
) -> Result<ArrayRef> {
    let [keys, values] = take_function_args("map_from_arrays", args)?;

    if *keys.data_type() == DataType::Null || *values.data_type() == DataType::Null {
        return Ok(cast(&NullArray::new(keys.len()), return_type)?);
    }

    map_from_keys_values_offsets_nulls(
        get_list_values(keys)?,
        get_list_values(values)?,
        &get_list_offsets(keys)?,
        &get_list_offsets(values)?,
        keys.nulls(),
        values.nulls(),
        last_value_wins,
        return_type,
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
        let [entries] = args.arg_fields else {
            return exec_err!("map_from_entries: expected one argument");
        };
        let element = match entries.data_type() {
            DataType::List(element)
            | DataType::LargeList(element)
            | DataType::FixedSizeList(element, _) => element,
            other => {
                return exec_err!(
                    "map_from_entries: expected array<struct<key, value>>, got {other:?}"
                );
            }
        };
        let DataType::Struct(fields) = element.data_type() else {
            return exec_err!(
                "map_from_entries: expected array<struct<key, value>>, got {:?}",
                element.data_type()
            );
        };
        let [key, value] = fields.as_ref() else {
            return exec_err!(
                "map_from_entries: expected array<struct<key, value>>, got {:?}",
                element.data_type()
            );
        };
        Ok(Arc::new(Field::new(
            self.name(),
            map_type_from_key_value_fields(key, value),
            entries.is_nullable() || element.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let last_value_wins = self.last_value_wins;
        let return_type = args.return_field.data_type();
        make_scalar_function(
            move |args| map_from_entries_inner(args, last_value_wins, return_type),
            vec![],
        )(&args.args)
    }
}

fn map_from_entries_inner(
    args: &[ArrayRef],
    last_value_wins: bool,
    return_type: &DataType,
) -> Result<ArrayRef> {
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
        return_type,
    )
}
