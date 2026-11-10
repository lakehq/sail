use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, NullArray, NullBufferBuilder, StructArray};
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
        let [keys, values] = take_function_args("map_from_arrays", arg_types)?;
        map_type_from_key_value_fields(
            get_element_field(keys)?.as_ref(),
            get_element_field(values)?.as_ref(),
        )
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let types = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        let data_type = self.return_type(&types)?;
        let field = self.delegate.return_field_from_args(args)?;
        Ok(Arc::new(field.as_ref().clone().with_data_type(data_type)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let last_value_wins = self.last_value_wins;
        let return_field = args.return_field;
        make_scalar_function(
            move |args| map_from_arrays_inner(args, last_value_wins, return_field.data_type()),
            vec![],
        )(&args.args)
    }
}

fn map_from_arrays_inner(
    args: &[ArrayRef],
    last_value_wins: bool,
    map_type: &DataType,
) -> Result<ArrayRef> {
    let [keys, values] = take_function_args("map_from_arrays", args)?;

    if *keys.data_type() == DataType::Null || *values.data_type() == DataType::Null {
        return Ok(cast(&NullArray::new(keys.len()), map_type)?);
    }

    map_from_keys_values_offsets_nulls(
        get_list_values(keys)?,
        get_list_values(values)?,
        &get_list_offsets(keys)?,
        &get_list_offsets(values)?,
        keys.nulls(),
        values.nulls(),
        last_value_wins,
        map_type,
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
        let [entries] = take_function_args("map_from_entries", arg_types)?;
        let element = get_element_field(entries)?;
        if let DataType::Struct(fields) = element.data_type() {
            let [key, value] = take_function_args("map_from_entries", fields.as_ref())?;
            map_type_from_key_value_fields(key, value)
        } else {
            exec_err!("map_from_entries: expected array<struct<key, value>>")
        }
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let types = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        let data_type = self.return_type(&types)?;
        let field = self.delegate.return_field_from_args(args)?;
        Ok(Arc::new(field.as_ref().clone().with_data_type(data_type)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let last_value_wins = self.last_value_wins;
        let return_field = args.return_field;
        make_scalar_function(
            move |args| map_from_entries_inner(args, last_value_wins, return_field.data_type()),
            vec![],
        )(&args.args)
    }
}

fn map_from_entries_inner(
    args: &[ArrayRef],
    last_value_wins: bool,
    map_type: &DataType,
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
        map_type,
    )
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::new_null_array;
    use datafusion::arrow::datatypes::Field;
    use sail_common_datafusion::uuid::UUID_METADATA_KEY;

    use super::*;

    #[test]
    fn typed_arrays_keep_key_and_value_identity_at_runtime() -> Result<()> {
        let function = SparkMapFromArrays::new(false);
        let types = ["17", "29"].map(|id| {
            DataType::List(Arc::new(
                Field::new("source", DataType::FixedSizeBinary(16), true).with_metadata(
                    std::collections::HashMap::from([
                        (UUID_METADATA_KEY.to_string(), "uuid".to_string()),
                        ("PARQUET:field_id".to_string(), id.to_string()),
                    ]),
                ),
            ))
        });
        let output = function.return_type(&types)?;
        let DataType::Map(entries, _) = &output else {
            return exec_err!("expected map");
        };
        let DataType::Struct(fields) = entries.data_type() else {
            return exec_err!("expected entries");
        };
        for field in fields {
            assert!(sail_common_datafusion::uuid::is_uuid_field(field));
            assert!(!field.metadata().contains_key("PARQUET:field_id"));
        }
        let arrays = types.map(|data_type| new_null_array(&data_type, 1));
        let result = map_from_arrays_inner(&arrays, false, &output)?;
        assert_eq!(result.data_type(), &output);
        assert!(result.is_null(0));
        Ok(())
    }
}
