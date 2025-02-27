use std::any::Any;

use datafusion::arrow::array::{
    make_array, Array, ArrayRef, AsArray, Capacities, MapArray, MutableArrayData,
};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{exec_err, internal_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_functions_nested::extract::ArrayElement;

use crate::extension::function::functions_nested_utils::make_scalar_function;

#[derive(Debug)]
pub struct SparkElementAt {
    signature: Signature,
}

impl Default for SparkElementAt {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkElementAt {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkElementAt {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_element_at"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::List(field)
            | DataType::ListView(field)
            | DataType::FixedSizeList(field, _)
            | DataType::LargeList(field)
            | DataType::LargeListView(field) => Ok(field.data_type().clone()),
            DataType::Map(field, _) => {
                let field_data_type = field.data_type();
                match field_data_type {
                    DataType::Struct(fields) => Ok(fields[1].data_type().clone()),
                    _ => {
                        internal_err!(
                            "Spark `element_at`: expected a Struct type, got {field_data_type:?}"
                        )
                    }
                }
            }
            other => {
                internal_err!("Spark `element_at`: expected a List or Map type, got {other:?}")
            }
        }
    }

    fn invoke_batch(&self, args: &[ColumnarValue], number_rows: usize) -> Result<ColumnarValue> {
        if args.len() != 2 {
            return exec_err!(
                "Spark `element_at` requires 2 arguments, got {}",
                args.len()
            );
        }
        if !matches!(args[0].data_type(), DataType::Map(_, _)) {
            let args = match args[1].data_type() {
                DataType::Int64 => args,
                DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64 => &[args[0].clone(), args[1].cast_to(&DataType::Int64, None)?],
                _ => {
                    return exec_err!(
                        "Spark `element_at` for array requires the second argument to be INT, got {}",
                        args[1].data_type()
                    );
                }
            };
            ArrayElement::new().invoke_batch(args, number_rows)
        } else {
            make_scalar_function(map_element_at)(args)
        }
    }
}

fn map_element_at(args: &[ArrayRef]) -> Result<ArrayRef> {
    let map_array: &MapArray = args[0].as_map();
    let query_keys = &args[1];

    if map_array.key_type() != query_keys.data_type() {
        return exec_err!(
            "Spark `element_at` key type mismatch: query key type is {}, but map key type is {}",
            query_keys.data_type(),
            map_array.key_type()
        );
    }

    let map_keys = map_array.keys();
    let map_data = map_array.values().to_data();
    let mut mutable_array_data =
        MutableArrayData::with_capacities(vec![&map_data], true, Capacities::Array(map_data.len()));

    for (row_index, offset_window) in map_array.value_offsets().windows(2).enumerate() {
        let query_key = query_keys.slice(row_index, 1);
        let start = offset_window[0] as usize;
        let end = offset_window[1] as usize;
        let value_index =
            (0..end - start).find(|&i| map_keys.slice(start + i, 1).as_ref() == query_key.as_ref());

        match value_index {
            Some(index) => {
                mutable_array_data.extend(0, start + index, start + index + 1);
            }
            None => {
                mutable_array_data.extend_nulls(1);
            }
        }
    }

    Ok(make_array(mutable_array_data.freeze()))
}

#[derive(Debug)]
pub struct SparkTryElementAt {
    signature: Signature,
}

impl Default for SparkTryElementAt {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTryElementAt {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkTryElementAt {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_try_element_at"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::List(field)
            | DataType::ListView(field)
            | DataType::FixedSizeList(field, _)
            | DataType::LargeList(field)
            | DataType::LargeListView(field) => Ok(field.data_type().clone()),
            DataType::Map(field, _) => {
                let field_data_type = field.data_type();
                match field_data_type {
                    DataType::Struct(fields) => Ok(fields[1].data_type().clone()),
                    _ => {
                        internal_err!(
                            "Spark `element_at`: expected a Struct type, got {field_data_type:?}"
                        )
                    }
                }
            }
            other => {
                internal_err!("Spark `element_at`: expected a List or Map type, got {other:?}")
            }
        }
    }

    fn invoke_batch(&self, args: &[ColumnarValue], number_rows: usize) -> Result<ColumnarValue> {
        let result = SparkElementAt::new().invoke_batch(args, number_rows);
        match result {
            Ok(result) => Ok(result),
            Err(_) => Ok(ColumnarValue::Scalar(ScalarValue::Null)),
        }
    }
}
