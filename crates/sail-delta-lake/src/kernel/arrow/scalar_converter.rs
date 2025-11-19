// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, StringArray, TimestampMicrosecondArray,
};
use datafusion::arrow::compute::{cast_with_options, CastOptions};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
use datafusion::common::scalar::ScalarValue;
use datafusion::common::Result as DataFusionResult;
use delta_kernel::expressions::Scalar;
use delta_kernel::schema::{DataType, PrimitiveType, StructField};

use crate::kernel::{DeltaResult as DeltaResultLocal, DeltaTableError};

pub(crate) struct ScalarConverter;

impl ScalarConverter {
    pub(crate) fn json_to_arrow_scalar_value(
        stat_val: &serde_json::Value,
        field_dt: &ArrowDataType,
    ) -> DataFusionResult<Option<ScalarValue>> {
        match stat_val {
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => Ok(None),
            serde_json::Value::Null => Ok(Some(ScalarValue::try_new_null(field_dt)?)),
            _ => {
                let string_val = match stat_val {
                    serde_json::Value::String(s) => s.to_owned(),
                    other => other.to_string(),
                };

                match field_dt {
                    ArrowDataType::Timestamp(_, _) => Ok(Some(Self::parse_timestamp(
                        &serde_json::Value::String(string_val),
                        field_dt,
                    )?)),
                    ArrowDataType::Date32 => Ok(Some(Self::parse_date(
                        &serde_json::Value::String(string_val),
                        field_dt,
                    )?)),
                    _ => Ok(Some(ScalarValue::try_from_string(string_val, field_dt)?)),
                }
            }
        }
    }

    pub(crate) fn scalars_to_arrow_array(
        field: &StructField,
        values: &[Scalar],
    ) -> DeltaResultLocal<Arc<dyn Array>> {
        let array: Arc<dyn Array> = match field.data_type() {
            DataType::Primitive(PrimitiveType::String) => {
                Arc::new(StringArray::from_iter(values.iter().map(|v| match v {
                    Scalar::String(s) => Some(s.clone()),
                    Scalar::Null(_) => None,
                    _ => None,
                })))
            }
            DataType::Primitive(PrimitiveType::Long) => {
                Arc::new(Int64Array::from_iter(values.iter().map(|v| match v {
                    Scalar::Long(i) => Some(*i),
                    Scalar::Null(_) => None,
                    _ => None,
                })))
            }
            DataType::Primitive(PrimitiveType::Integer) => {
                Arc::new(Int32Array::from_iter(values.iter().map(|v| match v {
                    Scalar::Integer(i) => Some(*i),
                    Scalar::Null(_) => None,
                    _ => None,
                })))
            }
            DataType::Primitive(PrimitiveType::Short) => {
                Arc::new(Int16Array::from_iter(values.iter().map(|v| match v {
                    Scalar::Short(i) => Some(*i),
                    Scalar::Null(_) => None,
                    _ => None,
                })))
            }
            DataType::Primitive(PrimitiveType::Byte) => {
                Arc::new(Int8Array::from_iter(values.iter().map(|v| match v {
                    Scalar::Byte(i) => Some(*i),
                    Scalar::Null(_) => None,
                    _ => None,
                })))
            }
            DataType::Primitive(PrimitiveType::Float) => {
                Arc::new(Float32Array::from_iter(values.iter().map(|v| match v {
                    Scalar::Float(f) => Some(*f),
                    Scalar::Null(_) => None,
                    _ => None,
                })))
            }
            DataType::Primitive(PrimitiveType::Double) => {
                Arc::new(Float64Array::from_iter(values.iter().map(|v| match v {
                    Scalar::Double(f) => Some(*f),
                    Scalar::Null(_) => None,
                    _ => None,
                })))
            }
            DataType::Primitive(PrimitiveType::Boolean) => {
                Arc::new(BooleanArray::from_iter(values.iter().map(|v| match v {
                    Scalar::Boolean(b) => Some(*b),
                    Scalar::Null(_) => None,
                    _ => None,
                })))
            }
            DataType::Primitive(PrimitiveType::Binary) => {
                Arc::new(BinaryArray::from_iter(values.iter().map(|v| match v {
                    Scalar::Binary(b) => Some(b.clone()),
                    Scalar::Null(_) => None,
                    _ => None,
                })))
            }
            DataType::Primitive(PrimitiveType::Date) => {
                Arc::new(Date32Array::from_iter(values.iter().map(|v| match v {
                    Scalar::Date(d) => Some(*d),
                    Scalar::Null(_) => None,
                    _ => None,
                })))
            }
            DataType::Primitive(PrimitiveType::Timestamp) => Arc::new(
                TimestampMicrosecondArray::from_iter(values.iter().map(|v| match v {
                    Scalar::Timestamp(ts) => Some(*ts),
                    Scalar::Null(_) => None,
                    _ => None,
                }))
                .with_timezone("UTC"),
            ),
            DataType::Primitive(PrimitiveType::TimestampNtz) => Arc::new(
                TimestampMicrosecondArray::from_iter(values.iter().map(|v| match v {
                    Scalar::TimestampNtz(ts) => Some(*ts),
                    Scalar::Null(_) => None,
                    _ => None,
                })),
            ),
            DataType::Primitive(PrimitiveType::Decimal(decimal)) => {
                let array = Decimal128Array::from_iter(values.iter().map(|v| match v {
                    Scalar::Decimal(d) => Some(d.bits()),
                    Scalar::Null(_) => None,
                    _ => None,
                }));
                let array = array
                    .with_precision_and_scale(decimal.precision(), decimal.scale() as i8)
                    .map_err(|e| {
                        DeltaTableError::generic(format!("Decimal precision error: {e}"))
                    })?;
                Arc::new(array)
            }
            _ => {
                return Err(DeltaTableError::generic(
                    "complex partition values are not supported",
                ))
            }
        };

        Ok(array)
    }

    fn parse_date(
        stat_val: &serde_json::Value,
        field_dt: &ArrowDataType,
    ) -> DataFusionResult<ScalarValue> {
        let string = match stat_val {
            serde_json::Value::String(s) => s.to_owned(),
            _ => stat_val.to_string(),
        };

        let time_micro = ScalarValue::try_from_string(string, &ArrowDataType::Date32)?;
        let cast_arr = cast_with_options(
            &time_micro.to_array()?,
            field_dt,
            &CastOptions {
                safe: false,
                ..Default::default()
            },
        )?;
        ScalarValue::try_from_array(&cast_arr, 0)
    }

    fn parse_timestamp(
        stat_val: &serde_json::Value,
        field_dt: &ArrowDataType,
    ) -> DataFusionResult<ScalarValue> {
        let string = match stat_val {
            serde_json::Value::String(s) => s.to_owned(),
            _ => stat_val.to_string(),
        };

        let time_micro = ScalarValue::try_from_string(
            string,
            &ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
        )?;
        let cast_arr = cast_with_options(
            &time_micro.to_array()?,
            field_dt,
            &CastOptions {
                safe: false,
                ..Default::default()
            },
        )?;
        ScalarValue::try_from_array(&cast_arr, 0)
    }
}
