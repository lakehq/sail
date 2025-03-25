use datafusion::arrow::datatypes::DataType;
use datafusion_common::{exec_err, Result};
use datafusion_expr::ColumnarValue;

/// [Credit]: <https://github.com/apache/datafusion/blob/d8e4e92daf7f20eef9af6919a8061192f7505043/datafusion/functions/src/datetime/common.rs#L45-L67>
pub(crate) fn validate_data_types(args: &[ColumnarValue], name: &str, skip: usize) -> Result<()> {
    for (idx, a) in args.iter().skip(skip).enumerate() {
        match a.data_type() {
            DataType::Utf8View | DataType::LargeUtf8 | DataType::Utf8 => {
                // all good
            }
            _ => {
                return exec_err!(
                    "{name} function unsupported data type at index {}: {}",
                    idx + 1,
                    a.data_type()
                );
            }
        }
    }

    Ok(())
}
