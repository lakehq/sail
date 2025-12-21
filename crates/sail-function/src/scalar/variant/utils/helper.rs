use arrow_schema::extension::ExtensionType;
/// [Credit]: <https://github.com/datafusion-contrib/datafusion-variant/blob/51e0d4be62d7675e9b7b56ed1c0b0a10ae4a28d7/src/shared.rs>
use arrow_schema::{DataType, Field};
use datafusion_common::{exec_err, ScalarValue};
use parquet_variant_compute::{VariantArray, VariantType};

pub fn try_field_as_variant_array(field: &Field) -> datafusion_common::Result<()> {
    // Accept Null type (for parse_json(null) case)
    if matches!(field.data_type(), DataType::Null) {
        return Ok(());
    }

    ensure(
        matches!(field.extension_type(), VariantType),
        "field does not have extension type VariantType",
    )?;

    let variant_type = VariantType;
    variant_type.supports_data_type(field.data_type())?;

    Ok(())
}
pub fn ensure(pred: bool, err_msg: &str) -> datafusion_common::Result<()> {
    if !pred {
        return exec_err!("{}", err_msg);
    }

    Ok(())
}
pub fn try_parse_variant_scalar(scalar: &ScalarValue) -> datafusion_common::Result<VariantArray> {
    let v = match scalar {
        ScalarValue::Struct(v) => v,
        unsupported => {
            return exec_err!(
                "expected variant scalar value, got data type: {}",
                unsupported.data_type()
            );
        }
    };

    VariantArray::try_new(v.as_ref()).map_err(Into::into)
}

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
