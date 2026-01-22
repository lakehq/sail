//! Helper functions to define Arrow data types used in system tables.
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};

pub fn string() -> DataType {
    DataType::Utf8
}

pub fn timestamp() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
}

#[expect(unused)]
pub fn boolean() -> DataType {
    DataType::Boolean
}

#[expect(private_bounds)]
pub fn numeric<T: NumericType>() -> DataType {
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
