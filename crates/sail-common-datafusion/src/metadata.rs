use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, IntervalUnit, TimeUnit};
use arrow_schema::extension::{EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY};
use datafusion_common::error::_plan_err;
use datafusion_common::metadata::FieldMetadata;
use datafusion_common::{DataFusionError, ScalarValue};
use sail_common::spec::{
    SAIL_SPARK_INTERVAL_METADATA_KEY, SAIL_SPARK_UDT_METADATA_KEY, SparkIntervalMetadata,
};

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

/// Keeps only the field metadata that describes the type of a value, such as an Arrow
/// extension type, a Spark UDT or a Spark interval type.
///
/// Use it for expressions whose result takes its type from an argument (e.g. `min`, `max` and
/// `coalesce`). The rest of the metadata describes the argument column itself, such as Spark
/// column metadata or storage field ids. Like Spark, which only keeps column metadata for
/// expressions that refer to a column or a struct field directly, the result must not inherit it.
pub fn type_metadata(metadata: &FieldMetadata) -> FieldMetadata {
    FieldMetadata::from(
        metadata
            .inner()
            .iter()
            .filter(|(key, _)| {
                matches!(
                    key.as_str(),
                    EXTENSION_TYPE_NAME_KEY
                        | EXTENSION_TYPE_METADATA_KEY
                        | SAIL_SPARK_UDT_METADATA_KEY
                        | SAIL_SPARK_INTERVAL_METADATA_KEY
                        | crate::uuid::UUID_METADATA_KEY
                )
            })
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>(),
    )
}

/// Attach type identity from the original inputs to a resolved physical field, recursively.
/// Untyped NULL is neutral, but a typed fixed[16] without the UUID marker is not UUID.
/// Names and nullability come from the resolved target, not from a source column.
pub fn coerced_type_field(target: &Field, sources: &[&Field]) -> Result<Field, DataFusionError> {
    use crate::uuid::{UUID_METADATA_KEY, is_uuid_field};

    let sources = sources
        .iter()
        .copied()
        .filter(|field| !field.data_type().is_null())
        .collect::<Vec<_>>();
    let mut metadata = type_metadata(&coerced_fields_metadata(sources.iter().copied())?)
        .inner()
        .clone();
    metadata.remove(SAIL_SPARK_INTERVAL_METADATA_KEY);
    if matches!(
        target.data_type(),
        DataType::Duration(TimeUnit::Microsecond) | DataType::Interval(IntervalUnit::YearMonth)
    ) {
        // Typed intervals, including typed NULLs, contribute their declared range.
        if let Some(interval) =
            widest_interval_metadata(sources.iter().map(|field| (*field, None)))?
        {
            metadata.insert(SAIL_SPARK_INTERVAL_METADATA_KEY.to_string(), interval);
        }
    }
    if target.data_type() != &DataType::FixedSizeBinary(16)
        || sources.is_empty()
        || !sources.iter().all(|field| is_uuid_field(field))
    {
        metadata.remove(UUID_METADATA_KEY);
    }
    let child = |field: &Field| {
        let children = sources
            .iter()
            .filter_map(|source| match source.data_type() {
                DataType::List(child)
                | DataType::LargeList(child)
                | DataType::FixedSizeList(child, _)
                | DataType::ListView(child)
                | DataType::LargeListView(child)
                | DataType::Map(child, _) => Some(child.as_ref()),
                _ => None,
            })
            .collect::<Vec<_>>();
        coerced_type_field(field, &children).map(Arc::new)
    };
    let data_type = match target.data_type() {
        DataType::List(field) => DataType::List(child(field)?),
        DataType::LargeList(field) => DataType::LargeList(child(field)?),
        DataType::FixedSizeList(field, size) => DataType::FixedSizeList(child(field)?, *size),
        DataType::ListView(field) => DataType::ListView(child(field)?),
        DataType::LargeListView(field) => DataType::LargeListView(child(field)?),
        DataType::Map(field, sorted) => DataType::Map(child(field)?, *sorted),
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    let children = sources
                        .iter()
                        .filter_map(|source| {
                            let DataType::Struct(source) = source.data_type() else {
                                return None;
                            };
                            // Duplicate names cannot identify a field uniquely; keep positions.
                            let same_names = source.len() == fields.len()
                                && fields
                                    .iter()
                                    .map(|field| field.name())
                                    .collect::<std::collections::BTreeSet<_>>()
                                    .len()
                                    == fields.len()
                                && fields.iter().all(|field| {
                                    source
                                        .iter()
                                        .filter(|candidate| candidate.name() == field.name())
                                        .count()
                                        == 1
                                });
                            if same_names {
                                source.iter().find(|s| s.name() == field.name())
                            } else {
                                source.get(index)
                            }
                            .map(AsRef::as_ref)
                        })
                        .collect::<Vec<_>>();
                    coerced_type_field(field, &children).map(Arc::new)
                })
                .collect::<Result<Vec<_>, _>>()?
                .into(),
        ),
        data_type => data_type.clone(),
    };
    Ok(FieldMetadata::from(metadata).add_to_field(Field::new(
        target.name(),
        data_type,
        target.is_nullable(),
    )))
}

/// The Spark interval metadata of a value that may come from any one of the fields, such as
/// the result of `coalesce`. Like Spark's type coercion for intervals, it is the widest interval
/// range of the fields. Each field comes with its value if it is a literal.
///
/// An interval without the metadata has the default range (`DAY TO SECOND` or
/// `YEAR TO MONTH`), which is the widest range, so the result has it too. A NULL literal
/// supplies no value, so it does not contribute unless it has the metadata.
pub fn widest_interval_metadata<'a>(
    fields: impl IntoIterator<Item = (&'a Field, Option<&'a ScalarValue>)>,
) -> Result<Option<String>, DataFusionError> {
    let mut widest = None::<SparkIntervalMetadata>;
    for (field, value) in fields {
        let Some(value) = field.metadata().get(SAIL_SPARK_INTERVAL_METADATA_KEY) else {
            let is_interval = matches!(
                field.data_type(),
                DataType::Duration(TimeUnit::Microsecond)
                    | DataType::Interval(IntervalUnit::YearMonth)
            );
            if is_interval && !value.is_some_and(ScalarValue::is_null) {
                return Ok(None);
            }
            continue;
        };
        let candidate = SparkIntervalMetadata::from_json(value)
            .map_err(|error| DataFusionError::Plan(error.to_string()))?;
        widest = Some(match widest {
            None => candidate,
            Some(current) => current.wider(candidate).ok_or_else(|| {
                DataFusionError::Plan("incompatible Spark interval metadata".to_string())
            })?,
        });
    }
    widest
        .map(|metadata| {
            metadata
                .to_json()
                .map_err(|error| DataFusionError::Internal(error.to_string()))
        })
        .transpose()
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn uuid_field(id: &str) -> Field {
        Field::new("source", DataType::FixedSizeBinary(16), true).with_metadata(
            std::collections::HashMap::from([
                (
                    crate::uuid::UUID_METADATA_KEY.to_string(),
                    "uuid".to_string(),
                ),
                ("PARQUET:field_id".to_string(), id.to_string()),
                ("doc".to_string(), "source documentation".to_string()),
            ]),
        )
    }

    #[test]
    fn constructor_interval_metadata_uses_declared_ranges_recursively() {
        use sail_common::spec::{IntervalFieldType, IntervalUnit as SparkIntervalUnit};

        let interval = |start, end| {
            SparkIntervalMetadata::try_new(SparkIntervalUnit::YearMonth, Some(start), Some(end))
                .unwrap()
                .unwrap()
                .to_json()
                .unwrap()
        };
        let year = interval(IntervalFieldType::Year, IntervalFieldType::Year);
        let month = interval(IntervalFieldType::Month, IntervalFieldType::Month);
        let wide = interval(IntervalFieldType::Year, IntervalFieldType::Month);
        let target = Field::new("item", DataType::Interval(IntervalUnit::YearMonth), true);
        let field = |metadata: String| {
            target
                .clone()
                .with_metadata(std::collections::HashMap::from([
                    (SAIL_SPARK_INTERVAL_METADATA_KEY.to_string(), metadata),
                    ("PARQUET:field_id".to_string(), "1".to_string()),
                ]))
        };
        let year_field = field(year.clone());
        let month_field = field(month);
        let null = Field::new("null", DataType::Null, true);
        for (right, expected) in [
            (&month_field, Some(&wide)),
            (&null, Some(&year)),
            // A typed interval without a qualifier has the default, widest range.
            (&target, None),
        ] {
            for sources in [[&year_field, right], [right, &year_field]] {
                let result = coerced_type_field(&target, &sources).unwrap();
                assert_eq!(
                    result.metadata().get(SAIL_SPARK_INTERVAL_METADATA_KEY),
                    expected
                );
                assert_eq!(result.metadata().len(), usize::from(expected.is_some()));
                let nested_sources = sources
                    .map(|f| Field::new("source", DataType::List(Arc::new(f.clone())), false));
                let nested_target =
                    Field::new("result", DataType::List(Arc::new(target.clone())), false);
                let nested =
                    coerced_type_field(&nested_target, &nested_sources.iter().collect::<Vec<_>>())
                        .unwrap();
                assert_eq!(nested.data_type(), &DataType::List(Arc::new(result)));
                assert!(nested.metadata().is_empty());
            }
        }
    }

    #[test]
    fn constructor_uuid_identity_requires_typed_consensus() {
        let uuid = uuid_field("1");
        let other = uuid_field("2");
        let null = Field::new("null", DataType::Null, true);
        let fixed = Field::new("typed_null", DataType::FixedSizeBinary(16), true);
        let binary = Field::new("binary", DataType::Binary, true);
        for (right, target_type, expected_uuid) in [
            (&other, DataType::FixedSizeBinary(16), true),
            (&null, DataType::FixedSizeBinary(16), true),
            (&fixed, DataType::FixedSizeBinary(16), false),
            (&binary, DataType::Binary, false),
        ] {
            let target = Field::new_list_field(target_type, true);
            for sources in [[&uuid, right], [right, &uuid]] {
                let field = coerced_type_field(&target, &sources).unwrap();
                assert_eq!(crate::uuid::is_uuid_field(&field), expected_uuid);
                assert_eq!(field.name(), target.name());
                assert_eq!(field.is_nullable(), target.is_nullable());
                assert_eq!(field.metadata().len(), usize::from(expected_uuid));
            }
        }
    }

    #[test]
    fn nested_uuid_coercion_is_order_independent() {
        use crate::type_coercion::binary_type_coercion::{
            comparison_coercion, type_union_coercion,
        };
        let uuid = uuid_field("1");
        let other = uuid_field("2");
        let fixed = Field::new("source", DataType::FixedSizeBinary(16), true);
        let null = Field::new("source", DataType::Null, true);
        let binary = Field::new("source", DataType::Binary, true);
        for (right, expected_uuid) in [
            (&other, true),
            (&fixed, false),
            (&null, true),
            (&binary, false),
        ] {
            for wrap in [
                |field: Field| DataType::List(Arc::new(field)),
                |field: Field| DataType::Struct(vec![field].into()),
                |field: Field| {
                    DataType::Map(
                        Arc::new(Field::new(
                            "entries",
                            DataType::Struct(
                                vec![Field::new("key", DataType::Utf8, false), field].into(),
                            ),
                            false,
                        )),
                        false,
                    )
                },
            ] {
                let left =
                    DataType::List(Arc::new(Field::new_list_field(wrap(uuid.clone()), true)));
                let right =
                    DataType::List(Arc::new(Field::new_list_field(wrap(right.clone()), true)));
                for coerce in [comparison_coercion, type_union_coercion] {
                    let forward = coerce(&left, &right).unwrap();
                    assert_eq!(forward, coerce(&right, &left).unwrap());
                    let result = Field::new("result", forward, true);
                    assert_eq!(crate::uuid::contains_uuid(&result), expected_uuid);
                }
            }
        }
    }

    #[test]
    fn constructor_struct_identity_handles_duplicate_and_reordered_names()
    -> Result<(), DataFusionError> {
        let uuid = uuid_field("1").with_name("x");
        let fixed = Field::new("x", DataType::FixedSizeBinary(16), true);
        for fields in [
            vec![uuid.clone(), fixed.clone()],
            vec![fixed.clone(), uuid.clone()],
        ] {
            let source = Field::new("source", DataType::Struct(fields.clone().into()), true);
            let target = Field::new("result", source.data_type().clone(), true);
            let result = coerced_type_field(&target, &[&source]).unwrap();
            let DataType::Struct(actual) = result.data_type() else {
                return datafusion_common::internal_err!("expected struct");
            };
            for (actual, expected) in actual.iter().zip(&fields) {
                assert_eq!(
                    crate::uuid::is_uuid_field(actual),
                    crate::uuid::is_uuid_field(expected)
                );
            }
        }
        let source = Field::new(
            "source",
            DataType::Struct(vec![uuid.with_name("u"), fixed.with_name("f")].into()),
            true,
        );
        let target = Field::new(
            "result",
            DataType::Struct(
                vec![
                    Field::new("f", DataType::FixedSizeBinary(16), true),
                    Field::new("u", DataType::FixedSizeBinary(16), true),
                ]
                .into(),
            ),
            true,
        );
        let result = coerced_type_field(&target, &[&source]).unwrap();
        let DataType::Struct(fields) = result.data_type() else {
            return datafusion_common::internal_err!("expected struct");
        };
        assert!(!crate::uuid::is_uuid_field(&fields[0]));
        assert!(crate::uuid::is_uuid_field(&fields[1]));
        Ok(())
    }

    #[test]
    fn constructor_keeps_extension_type_metadata_without_column_metadata() {
        let metadata = std::collections::HashMap::from([
            (
                EXTENSION_TYPE_NAME_KEY.to_string(),
                "geoarrow.wkb".to_string(),
            ),
            (
                EXTENSION_TYPE_METADATA_KEY.to_string(),
                r#"{"edges":"spherical"}"#.to_string(),
            ),
            ("PARQUET:field_id".to_string(), "1".to_string()),
        ]);
        let source = Field::new("geography", DataType::Binary, true).with_metadata(metadata);
        let target = Field::new_list_field(DataType::Binary, true);
        let result = coerced_type_field(&target, &[&source]).unwrap();
        assert_eq!(
            result.metadata().get(EXTENSION_TYPE_NAME_KEY),
            source.metadata().get(EXTENSION_TYPE_NAME_KEY)
        );
        assert_eq!(
            result.metadata().get(EXTENSION_TYPE_METADATA_KEY),
            source.metadata().get(EXTENSION_TYPE_METADATA_KEY)
        );
        assert!(!result.metadata().contains_key("PARQUET:field_id"));
    }

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

    #[test]
    fn type_metadata_keeps_only_type_keys() {
        use sail_common::spec::SPARK_METADATA_JSON_KEY;

        let field = Field::new("a", DataType::Binary, true).with_metadata(
            std::collections::HashMap::from([
                (EXTENSION_TYPE_NAME_KEY.to_string(), "ext.type".to_string()),
                (EXTENSION_TYPE_METADATA_KEY.to_string(), "{}".to_string()),
                (SAIL_SPARK_UDT_METADATA_KEY.to_string(), "udt".to_string()),
                (
                    SAIL_SPARK_INTERVAL_METADATA_KEY.to_string(),
                    "interval".to_string(),
                ),
                (
                    SPARK_METADATA_JSON_KEY.to_string(),
                    "{\"comment\":\"c\"}".to_string(),
                ),
                ("PARQUET:field_id".to_string(), "1".to_string()),
                ("delta.columnMapping.id".to_string(), "1".to_string()),
            ]),
        );
        let result = type_metadata(&FieldMetadata::from(&field));
        let mut keys = result
            .inner()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![
                EXTENSION_TYPE_METADATA_KEY,
                EXTENSION_TYPE_NAME_KEY,
                SAIL_SPARK_INTERVAL_METADATA_KEY,
                SAIL_SPARK_UDT_METADATA_KEY,
            ]
        );
    }

    #[test]
    fn widest_interval_metadata_widens_the_range() {
        use sail_common::spec::{IntervalFieldType, IntervalUnit as SparkIntervalUnit};

        let interval = |start: IntervalFieldType, end: IntervalFieldType| {
            SparkIntervalMetadata::try_new(SparkIntervalUnit::YearMonth, Some(start), Some(end))
                .unwrap()
                .unwrap()
        };
        let field = |metadata: Option<SparkIntervalMetadata>| {
            let field = Field::new(
                "a",
                DataType::Interval(arrow_schema::IntervalUnit::YearMonth),
                true,
            );
            match metadata {
                Some(metadata) => field.with_metadata(std::collections::HashMap::from([(
                    SAIL_SPARK_INTERVAL_METADATA_KEY.to_string(),
                    metadata.to_json().unwrap(),
                )])),
                None => field,
            }
        };
        let year = interval(IntervalFieldType::Year, IntervalFieldType::Year);
        let month = interval(IntervalFieldType::Month, IntervalFieldType::Month);
        let null = ScalarValue::IntervalYearMonth(None);
        let untyped_null = Field::new("a", DataType::Null, true);
        let fields = [
            (field(Some(year)), None),
            (field(None), Some(&null)),
            (untyped_null, Some(&ScalarValue::Null)),
            (field(Some(month)), None),
        ];
        let widest = widest_interval_metadata(fields.iter().map(|(f, v)| (f, *v)))
            .unwrap()
            .unwrap();
        assert_eq!(
            SparkIntervalMetadata::from_json(&widest).unwrap(),
            interval(IntervalFieldType::Year, IntervalFieldType::Month)
        );
        // An interval value without the metadata has the default range.
        let fields = [(field(Some(year)), None), (field(None), None)];
        assert_eq!(
            widest_interval_metadata(fields.iter().map(|(f, v)| (f, *v))).unwrap(),
            None
        );
    }
}
