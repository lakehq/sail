/// [Credit]: <https://github.com/datafusion-contrib/datafusion-variant/blob/51e0d4be62d7675e9b7b56ed1c0b0a10ae4a28d7/src/shared.rs>
use arrow_schema::{DataType, Field};
use datafusion_common::{exec_err, ScalarValue};

pub fn try_field_as_string(field: &Field) -> datafusion_common::Result<()> {
    match field.data_type() {
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 | DataType::Null => {}
        unsupported => return exec_err!("expected string field, got {unsupported} field"),
    }

    Ok(())
}

pub fn try_parse_string_scalar(scalar: &ScalarValue) -> datafusion_common::Result<Option<&String>> {
    let b = match scalar {
        ScalarValue::Null => return Ok(None),
        ScalarValue::Utf8(s) | ScalarValue::Utf8View(s) | ScalarValue::LargeUtf8(s) => s,
        unsupported => {
            return exec_err!(
                "expected binary scalar value, got data type: {}",
                unsupported.data_type()
            );
        }
    };

    Ok(b.as_ref())
}
