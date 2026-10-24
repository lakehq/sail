//! Type policies for relational expansion operators.
use arrow_schema::DataType;
use datafusion_common::{Result, plan_err};

pub fn pivot_first_supported(data_type: &DataType) -> bool {
    data_type.is_numeric() || matches!(data_type, DataType::Boolean)
}

pub fn unpivot_common_type(types: &[DataType]) -> Result<DataType> {
    let non_null = types.iter().filter(|t| !t.is_null()).collect::<Vec<_>>();
    if let Some(first) = non_null.first()
        && non_null.iter().any(|t| {
            *t != *first
                && !(t.is_numeric() && first.is_numeric())
                && !(t.is_string() && first.is_string())
        })
    {
        return plan_err!(
            "[UNPIVOT_VALUE_DATA_TYPE_MISMATCH] Unpivot value columns must share a least common type: {types:?}"
        );
    }
    super::coercion::common_type(types)
}
