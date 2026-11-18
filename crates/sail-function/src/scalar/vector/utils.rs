// SPDX-License-Identifier: Apache-2.0

use datafusion::arrow::datatypes::DataType;

pub(crate) fn is_float_vector(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::List(field) | DataType::LargeList(field)
            if field.data_type() == &DataType::Float32
    )
}
