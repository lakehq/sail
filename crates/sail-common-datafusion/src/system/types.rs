//! Helper functions to define Arrow data types used in system tables.
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use sail_common::spec::SAIL_LIST_FIELD_NAME;
use serde::{Deserialize, Serialize};

pub(crate) fn string() -> DataType {
    DataType::Utf8
}

pub(crate) fn timestamp() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
}

#[expect(unused)]
pub(crate) fn boolean() -> DataType {
    DataType::Boolean
}

#[expect(private_bounds)]
pub(crate) fn numeric<T: NumericType>() -> DataType {
    T::data_type()
}

trait NumericType {
    fn data_type() -> DataType;
}

macro_rules! impl_numeric_type {
    ($t:ty, $dt:expr) => {
        impl NumericType for $t {
            fn data_type() -> DataType {
                $dt
            }
        }
    };
}

impl_numeric_type!(i8, DataType::Int8);
impl_numeric_type!(i16, DataType::Int16);
impl_numeric_type!(i32, DataType::Int32);
impl_numeric_type!(i64, DataType::Int64);
impl_numeric_type!(u8, DataType::UInt8);
impl_numeric_type!(u16, DataType::UInt16);
impl_numeric_type!(u32, DataType::UInt32);
impl_numeric_type!(u64, DataType::UInt64);
impl_numeric_type!(f32, DataType::Float32);
impl_numeric_type!(f64, DataType::Float64);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageInput {
    pub stage: u64,
    pub mode: String,
}

pub(crate) fn stage_input_list() -> DataType {
    DataType::List(Arc::new(Field::new(
        SAIL_LIST_FIELD_NAME,
        DataType::Struct(
            vec![
                Field::new("stage", DataType::UInt64, false),
                Field::new("mode", DataType::Utf8, false),
            ]
            .into(),
        ),
        // FIXME: The elements should be non-nullable,
        //   but the SQL syntax does not yet support specifying nullability
        //   for `ARRAY` element types or `MAP` value types.
        true,
    )))
}
