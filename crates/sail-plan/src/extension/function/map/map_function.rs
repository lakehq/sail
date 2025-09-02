use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BooleanBuilder, MapArray, StructArray};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute::filter;
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion_common::cast::as_list_array;
use datafusion_common::{exec_err, plan_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_functions::utils::make_scalar_function;

#[derive(Debug)]
pub struct MapFunction {
    signature: Signature,
}

impl Default for MapFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl MapFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for MapFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "map"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let (key_type, value_type) = match arg_types.len() {
            1 => get_list_struct_key_value_types(&arg_types[0]),
            2 => Ok((get_element_type(&arg_types[0])?, get_element_type(&arg_types[1])?)),
            _ => plan_err!("create_map: expected array<struct<key, value>> or (array<key>, array<value>), got {:?}", arg_types)
        }?;

        Ok(DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    // the key must not be nullable
                    Field::new("key", key_type.clone(), false),
                    Field::new("value", value_type.clone(), true),
                ])),
                false, // the entry is not nullable
            )),
            false, // the keys are not sorted
        ))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        // MapArray stores entries in List type with i32 offset only
        // So cast LargeList or FixedSizeList args to List to simplify logic
        arg_types
            .iter()
            .map(|data_type| Ok(DataType::List(get_list_field(data_type)?.clone())))
            .collect()
    }

    fn invoke_with_args(&self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(map_from_arrays_inner, vec![])(&args.args)
    }
}

pub fn map_from_arrays_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let lists = args
        .iter()
        .map(|list_arc| as_list_array(list_arc.as_ref()))
        .collect::<Result<Vec<_>>>()?;

    let (keys, values, offsets) = match lists.len() {
        1 => {
           match lists[0]
                    .values()
                    .as_any()
                    .downcast_ref::<StructArray>() {
                Some(a) => Ok((a.column(0), a.column(1), lists[0].offsets())),
                None => exec_err!(
                    "create_map: expected array<struct<key, value>> or (array<key>, array<value>), got {:?}", 
                    lists[0].data_type()
                )
            }
        }
        2 => Ok((lists[0].values(), lists[1].values(), lists[0].offsets())),
        wrong_cnt => exec_err!("create_map: expected array<struct<key, value>> or (array<key>, array<value>), got {wrong_cnt} args")
    }?;

    let (keys, values, offsets) = map_deduplicate_keys(keys, values, offsets)?;

    let fields = Fields::from(vec![
        Field::new("key", keys.data_type().clone(), false),
        Field::new("value", values.data_type().clone(), true),
    ]);
    let entries = StructArray::try_new(fields.clone(), vec![keys, values], None)?;
    let field = Arc::new(Field::new("entries", DataType::Struct(fields), false));
    Ok(Arc::new(MapArray::try_new(
        field, offsets, entries, None, false,
    )?))
}

pub fn map_deduplicate_keys(
    keys: &ArrayRef,
    values: &ArrayRef,
    offsets: &[i32],
) -> Result<(ArrayRef, ArrayRef, OffsetBuffer<i32>)> {
    let num_rows = offsets.len() - 1;
    let mut new_offsets = Vec::with_capacity(num_rows + 1);
    new_offsets.push(0);
    let mut last_offset = 0;

    let mut needed_rows_builder = BooleanBuilder::new();
    for row_num in 0..num_rows {
        let cur_offset = offsets[row_num] as usize;
        let next_offset = offsets[row_num + 1] as usize;
        let num_entries = next_offset - cur_offset;
        let keys_slice = keys.slice(cur_offset, num_entries);
        let mut seen_keys = HashSet::new();
        let mut needed_rows_one = [false].repeat(num_entries);
        for index in (0..num_entries).rev() {
            let key = ScalarValue::try_from_array(&keys_slice, index)?.compacted();
            if seen_keys.contains(&key) {
                // TODO: implement configuration and logic for spark.sql.mapKeyDedupPolicy=EXCEPTION (this is default spark-config)
                // exec_err!("invalid argument: duplicate keys in map")
                // https://github.com/apache/spark/blob/cf3a34e19dfcf70e2d679217ff1ba21302212472/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4961
            } else {
                // This code implements deduplication logic for spark.sql.mapKeyDedupPolicy=LAST_WIN (this is NOT default spark-config)
                needed_rows_one[index] = true;
                seen_keys.insert(key);
                last_offset += 1;
            }
        }
        needed_rows_builder.append_array(&needed_rows_one.into());
        new_offsets.push(last_offset);
    }
    let needed_rows = needed_rows_builder.finish();
    let needed_keys = filter(&keys, &needed_rows)?;
    let needed_values = filter(&values, &needed_rows)?;
    let offsets = OffsetBuffer::new(new_offsets.into());
    Ok((needed_keys, needed_values, offsets))
}

fn get_list_field(data_type: &DataType) -> Result<&Arc<Field>> {
    match data_type {
        DataType::List(element)
        | DataType::LargeList(element)
        | DataType::FixedSizeList(element, _) => Ok(element),
        _ => exec_err!(
            "create_map: expected array<struct<key, value>> or (array<key>, array<value>), got {:?}",
            data_type
        ),
    }
}

fn get_element_type(data_type: &DataType) -> Result<&DataType> {
    get_list_field(data_type).map(|field| field.data_type())
}

fn get_list_struct_key_value_types(data_type: &DataType) -> Result<(&DataType, &DataType)> {
    let err = |wrong_type| {
        exec_err!(
            "create_map: expected array<struct<key, value>> or (array<key>, array<value>), got {:?}",
            wrong_type
        )
    };

    match data_type {
        DataType::List(element) => match element.data_type() {
            DataType::Struct(fields) if fields.len() == 2 => {
                Ok((fields[0].data_type(), fields[1].data_type()))
            }
            _ => err(data_type),
        },
        _ => err(data_type),
    }
}
