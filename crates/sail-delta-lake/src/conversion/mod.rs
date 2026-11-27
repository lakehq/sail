// SPDX-License-Identifier: Apache-2.0

pub mod scalar;
pub mod type_promotion;

pub(crate) use scalar::partition_parse_type;
pub use scalar::{
    ScalarConverter, ScalarExt, parse_optional_partition_value, parse_partition_value,
    scalar_from_array_opt, scalar_value_to_array,
};
pub use type_promotion::DeltaTypeConverter;
