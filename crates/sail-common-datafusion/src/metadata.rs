use std::collections::BTreeMap;

use arrow::datatypes::Field;
use arrow_schema::extension::{EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY};
use datafusion_common::DataFusionError;
use datafusion_common::error::_plan_err;
use datafusion_common::metadata::FieldMetadata;

// [CREDIT]: https://github.com/apache/datafusion/blob/90b55e38b5de8e74a17e24bf7ea7259cbcdab022/datafusion/common/src/metadata.rs#L108-L175
/// Compute the field metadata to propagate for an expression whose result
/// may come from any one of several inputs coerced to a common type, such as
/// `COALESCE`, `NVL2`, or `CASE WHEN`.
///
/// Metadata is merged across the fields whose data type is not `Null` (an
/// untyped NULL literal carries no metadata and cannot contribute a typed
/// value), with earlier fields taking precedence for repeated keys. Unrelated
/// keys are not required to agree: metadata that arrived with the data (e.g.
/// from an Arrow file or an embedded Arrow schema in a Parquet file) must not
/// cause an extension type to be silently dropped from the result.
///
/// The Arrow extension type keys are handled as a unit, like in
/// [`Expr::Cast`] output field derivation:
/// - Differing values of `ARROW:extension:name` produce a planning error;
///   the inputs are of different logical types even though they were coerced
///   to a common storage type.
/// - `ARROW:extension:metadata` is taken from the field that supplied the
///   extension name, never combined from another field. Byte-wise differences
///   in the extension metadata of fields that agree on the extension name are
///   tolerated, since extension parameters are often JSON-encoded, where byte
///   equality is too strict.
///
/// [`Expr::Cast`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html#variant.Cast
pub fn coerced_fields_metadata<'a>(
    fields: impl IntoIterator<Item = &'a Field>,
) -> Result<FieldMetadata, DataFusionError> {
    let mut merged: BTreeMap<String, String> = BTreeMap::new();
    let mut extension: Option<(String, Option<String>)> = None;

    for field in fields {
        if field.data_type().is_null() {
            continue;
        }
        let metadata = FieldMetadata::new_from_field(field);
        if let Some(name) = metadata.inner().get(EXTENSION_TYPE_NAME_KEY) {
            match &extension {
                Some((first_name, _)) if first_name != name => {
                    return _plan_err!(
                        "Cannot merge field metadata with differing values of \
                         '{EXTENSION_TYPE_NAME_KEY}': '{first_name}' and '{name}'"
                    );
                }
                Some(_) => {}
                None => {
                    extension = Some((
                        name.clone(),
                        metadata.inner().get(EXTENSION_TYPE_METADATA_KEY).cloned(),
                    ));
                }
            }
        }
        for (key, value) in metadata.inner() {
            if key == EXTENSION_TYPE_NAME_KEY || key == EXTENSION_TYPE_METADATA_KEY {
                continue;
            }
            merged.entry(key.clone()).or_insert_with(|| value.clone());
        }
    }

    if let Some((name, extension_metadata)) = extension {
        merged.insert(EXTENSION_TYPE_NAME_KEY.to_string(), name);
        if let Some(extension_metadata) = extension_metadata {
            merged.insert(EXTENSION_TYPE_METADATA_KEY.to_string(), extension_metadata);
        }
    }

    Ok(FieldMetadata::from(merged))
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use arrow_schema::DataType;

    use super::*;

    #[test]
    fn coerced_fields_metadata_merges_with_earlier_precedence() {
        let a = Field::new("a", DataType::Binary, true).with_metadata(
            std::collections::HashMap::from([
                ("k".to_string(), "v1".to_string()),
                ("only_a".to_string(), "a".to_string()),
            ]),
        );
        let b = Field::new("b", DataType::Binary, true).with_metadata(
            std::collections::HashMap::from([
                ("k".to_string(), "v2".to_string()),
                ("only_b".to_string(), "b".to_string()),
            ]),
        );
        let result = coerced_fields_metadata([&a, &b]).unwrap();
        assert_eq!(result.inner().get("k").map(String::as_str), Some("v1"));
        assert_eq!(result.inner().get("only_a").map(String::as_str), Some("a"));
        assert_eq!(result.inner().get("only_b").map(String::as_str), Some("b"));
    }

    #[test]
    fn coerced_fields_metadata_skips_null_typed_fields() {
        let null = Field::new("null", DataType::Null, true);
        let b = Field::new("b", DataType::Binary, true).with_metadata(
            std::collections::HashMap::from([("k".to_string(), "v".to_string())]),
        );
        let result = coerced_fields_metadata([&null, &b]).unwrap();
        assert_eq!(result.inner().get("k").map(String::as_str), Some("v"));
    }

    #[test]
    fn coerced_fields_metadata_empty_when_no_field_has_any() {
        let a = Field::new("a", DataType::Binary, true);
        let null = Field::new("null", DataType::Null, true);
        assert!(coerced_fields_metadata([&a, &null]).unwrap().is_empty());
    }

    #[test]
    fn coerced_fields_metadata_errors_on_differing_extension_names() {
        let a = Field::new("a", DataType::Binary, true).with_metadata(
            std::collections::HashMap::from([(
                EXTENSION_TYPE_NAME_KEY.to_string(),
                "arrow.json".to_string(),
            )]),
        );
        let b = Field::new("b", DataType::Binary, true).with_metadata(
            std::collections::HashMap::from([(
                EXTENSION_TYPE_NAME_KEY.to_string(),
                "geoarrow.wkb".to_string(),
            )]),
        );
        let err = coerced_fields_metadata([&a, &b]).unwrap_err();
        assert!(err.to_string().contains("arrow.json"), "{err}");
        assert!(err.to_string().contains("geoarrow.wkb"), "{err}");
    }

    #[test]
    fn coerced_fields_metadata_keeps_extension_keys_paired() {
        // The extension metadata travels with the field that supplied the
        // extension name: byte-wise differences for the same name are
        // tolerated, and a bare extension name never picks up another
        // field's parameters.
        let a =
            Field::new("a", DataType::Binary, true).with_metadata(std::collections::HashMap::from(
                [(EXTENSION_TYPE_NAME_KEY.to_string(), "ext.type".to_string())],
            ));
        let b = Field::new("b", DataType::Binary, true).with_metadata(
            std::collections::HashMap::from([
                (EXTENSION_TYPE_NAME_KEY.to_string(), "ext.type".to_string()),
                (
                    EXTENSION_TYPE_METADATA_KEY.to_string(),
                    "{\"param\": 1}".to_string(),
                ),
            ]),
        );
        let result = coerced_fields_metadata([&a, &b]).unwrap();
        assert_eq!(
            result
                .inner()
                .get(EXTENSION_TYPE_NAME_KEY)
                .map(String::as_str),
            Some("ext.type")
        );
        // "a" supplied the extension name first and carried no parameters;
        // "b"'s parameters are not grafted onto it.
        assert_eq!(result.inner().get(EXTENSION_TYPE_METADATA_KEY), None);
    }
}
