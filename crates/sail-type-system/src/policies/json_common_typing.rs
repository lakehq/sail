//! Shared type utilities.
// https://github.com/datafusion-contrib/datafusion-functions-json/blob/cb1ba7a80a84e10a4d658f3100eae8f6bca2ced9/LICENSE
//
// [Credit]: https://github.com/datafusion-contrib/datafusion-functions-json/blob/78c5abbf7222510ff221517f5d2e3c344969da98/src/common.rs

use arrow_schema::DataType;
use datafusion_common::{Result, plan_err};

use super::json_union_typing::is_json_union;

pub fn return_type_check(
    args: &[DataType],
    fn_name: &str,
    value_type: DataType,
) -> Result<DataType> {
    let Some(first) = args.first() else {
        return plan_err!("The '{fn_name}' function requires one or more arguments.");
    };
    let first_dict_key_type = dict_key_type(first);
    if !(is_str(first) || is_json_union(first) || first_dict_key_type.is_some()) {
        // if !matches!(first, DataType::Utf8 | DataType::LargeUtf8) {
        return plan_err!(
            "Unexpected argument type to '{fn_name}' at position 1, expected a string, got {first:?}."
        );
    }
    args.iter().skip(1).enumerate().try_for_each(|(index, arg)| {
        if is_str(arg) || is_int(arg) || dict_key_type(arg).is_some() {
            Ok(())
        } else {
            plan_err!(
                "Unexpected argument type to '{fn_name}' at position {}, expected string or int, got {arg:?}.",
                index + 2
            )
        }
    })?;
    if first_dict_key_type.is_some() && !value_type.is_primitive() {
        Ok(DataType::Dictionary(
            Box::new(DataType::Int64),
            Box::new(value_type),
        ))
    } else {
        Ok(value_type)
    }
}

pub fn is_str(d: &DataType) -> bool {
    matches!(d, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
}

pub fn is_int(d: &DataType) -> bool {
    // TODO we should support more types of int, but that's a longer task
    matches!(d, DataType::UInt64 | DataType::Int64)
}

pub fn dict_key_type(d: &DataType) -> Option<DataType> {
    if let DataType::Dictionary(key, value) = d
        && (is_str(value) || is_json_union(value))
    {
        return Some(*key.clone());
    }
    None
}
