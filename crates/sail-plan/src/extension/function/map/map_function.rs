use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BooleanBuilder, MapArray, NullArray, StructArray};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute::{cast, filter, interleave};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

trait KeyValue<T>
where
    T: 'static,
{
    fn keys(&self) -> impl Iterator<Item = &T>;
    fn values(&self) -> impl Iterator<Item = &T>;
}

impl<T> KeyValue<T> for &[T]
where
    T: 'static,
{
    fn keys(&self) -> impl Iterator<Item = &T> {
        self.iter().step_by(2)
    }

    fn values(&self) -> impl Iterator<Item = &T> {
        self.iter().skip(1).step_by(2)
    }
}

fn to_map_array(args: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
    if !args.len().is_multiple_of(2) {
        return exec_err!("map requires an even number of arguments");
    }
    let num_entries = args.len() / 2;
    if args.iter().any(|a| a.len() != num_rows) {
        return exec_err!("map requires all arrays to have the same length");
    }
    let key_type = args
        .first()
        .map(|a| a.data_type())
        .unwrap_or(&DataType::Null);
    let value_type = args
        .values()
        .map(|a| a.data_type())
        .find(|dt| *dt != &DataType::Null)
        .unwrap_or(&DataType::Null);
    let keys = args.keys().map(|a| a.as_ref()).collect::<Vec<_>>();
    let arc_values = args
        .values()
        .map(|a| match a.data_type() {
            &DataType::Null => Ok(cast(a, value_type)?),
            _ => Ok(a.clone()),
        })
        .collect::<Result<Vec<_>>>()?;
    let values = arc_values.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
    if keys.iter().any(|a| a.data_type() != key_type) {
        return exec_err!("map requires all key types to be the same");
    }
    if values.iter().any(|a| a.data_type() != value_type) {
        return exec_err!("map requires all value types to be the same");
    }

    let (keys, values, offsets) = match (key_type, value_type) {
        (&DataType::Null, &DataType::Null) => {
            let keys = Arc::new(NullArray::new(0)) as ArrayRef;
            let values = Arc::new(NullArray::new(0)) as ArrayRef;
            (keys, values, vec![0, 0])
        }
        _ => {
            // TODO: avoid materializing the indices
            let indices = (0..num_rows)
                .flat_map(|i| (0..num_entries).map(move |j| (j, i)))
                .collect::<Vec<_>>();
            let keys = interleave(keys.as_slice(), indices.as_slice())?;
            let values = interleave(values.as_slice(), indices.as_slice())?;

            let mut offsets = Vec::with_capacity(num_rows + 1);
            offsets.push(0);
            let mut last_offset = 0;

            let mut needed_rows_builder = BooleanBuilder::new();
            for row_num in 0..num_rows {
                let keys_slice = keys.slice(row_num * num_entries, num_entries);
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
                offsets.push(last_offset);
            }
            let needed_rows = needed_rows_builder.finish();
            let needed_keys = filter(&keys, &needed_rows)?;
            let needed_values = filter(&values, &needed_rows)?;
            (needed_keys, needed_values, offsets)
        }
    };
    let offsets = OffsetBuffer::new(offsets.into());
    let fields = Fields::from(vec![
        Field::new("key", key_type.clone(), false),
        Field::new("value", value_type.clone(), true),
    ]);
    let entries = StructArray::try_new(fields.clone(), vec![keys, values], None)?;
    let field = Arc::new(Field::new("entries", DataType::Struct(fields), false));
    Ok(Arc::new(MapArray::try_new(
        field, offsets, entries, None, false,
    )?))
}

#[derive(Debug, Clone)]
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
            signature: Signature::one_of(
                vec![TypeSignature::VariadicAny, TypeSignature::Nullary],
                Volatility::Immutable,
            ),
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
        if !arg_types.len().is_multiple_of(2) {
            return exec_err!("map requires an even number of arguments");
        }
        if arg_types.keys().any(|dt| dt == &DataType::Null) {
            return exec_err!("map cannot contain null key type");
        }
        let key_type = arg_types.first().unwrap_or(&DataType::Null);
        let value_type = arg_types
            .values()
            .find(|dt| *dt != &DataType::Null)
            .unwrap_or(&DataType::Null);
        // TODO: support type coercion
        if arg_types.keys().any(|dt| dt != key_type) {
            return exec_err!("map requires all key types to be the same");
        }
        if arg_types
            .values()
            .any(|dt| (dt != value_type) && (dt != &DataType::Null))
        {
            return exec_err!("map requires all value types to be the same");
        }
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;
        let arrays = ColumnarValue::values_to_arrays(&args)?;
        Ok(ColumnarValue::Array(to_map_array(
            arrays.as_slice(),
            number_rows,
        )?))
    }
}
