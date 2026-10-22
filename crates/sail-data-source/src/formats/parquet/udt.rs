use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
use sail_common::spec::{SAIL_SPARK_UDT_METADATA_KEY, SparkUdtMetadata};
use serde_json::Value;

/// The key of the Parquet file metadata where Spark writes its schema as JSON.
pub(super) const SPARK_ROW_METADATA_KEY: &str = "org.apache.spark.sql.parquet.row.metadata";

/// Restores the Spark user-defined types of the fields from the Spark schema in the Parquet
/// file metadata, since the physical type of such a field is the SQL type of the user-defined type.
/// The user-defined type is kept in the metadata of the field, as Sail does for all the fields.
/// The schema is returned unchanged if there is no valid Spark schema, which is the value of
/// the file metadata with the key [`SPARK_ROW_METADATA_KEY`].
pub(super) fn restore_spark_udts(schema: Schema, spark_schema: Option<&str>) -> Schema {
    let Some(spark_schema) =
        spark_schema.and_then(|value| serde_json::from_str::<Value>(value).ok())
    else {
        return schema;
    };
    let fields = restore_fields(schema.fields(), &spark_schema);
    Schema::new_with_metadata(fields, schema.metadata().clone())
}

/// Restores the user-defined types of the fields from the `fields` of a Spark struct type.
fn restore_fields(fields: &Fields, spark_struct: &Value) -> Fields {
    let Some(spark_fields) = spark_struct.get("fields").and_then(Value::as_array) else {
        return fields.clone();
    };
    fields
        .iter()
        .map(|field| {
            let spark_type = spark_fields
                .iter()
                .find(|spark_field| {
                    spark_field.get("name").and_then(Value::as_str) == Some(field.name())
                })
                .and_then(|spark_field| spark_field.get("type"));
            match spark_type {
                Some(spark_type) => Arc::new(restore_field(field, spark_type)),
                None => Arc::clone(field),
            }
        })
        .collect()
}

fn restore_field(field: &Field, spark_type: &Value) -> Field {
    let mut field = field.clone();
    if let Some(udt) = udt_metadata(spark_type) {
        let mut metadata = field.metadata().clone();
        metadata.insert(SAIL_SPARK_UDT_METADATA_KEY.to_string(), udt);
        field.set_metadata(metadata);
    }
    // The physical type of a user-defined type is its SQL type.
    let spark_type = spark_type
        .get("sqlType")
        .filter(|_| is_udt(spark_type))
        .unwrap_or(spark_type);
    let data_type = restore_data_type(field.data_type(), spark_type);
    field.with_data_type(data_type)
}

fn restore_data_type(data_type: &DataType, spark_type: &Value) -> DataType {
    let kind = spark_type.get("type").and_then(Value::as_str);
    match (data_type, kind) {
        (DataType::List(item), Some("array")) => spark_type
            .get("elementType")
            .map(|element| DataType::List(Arc::new(restore_field(item, element))))
            .unwrap_or_else(|| data_type.clone()),
        (DataType::LargeList(item), Some("array")) => spark_type
            .get("elementType")
            .map(|element| DataType::LargeList(Arc::new(restore_field(item, element))))
            .unwrap_or_else(|| data_type.clone()),
        (DataType::Struct(fields), Some("struct")) => {
            DataType::Struct(restore_fields(fields, spark_type))
        }
        (DataType::Map(entries, sorted), Some("map")) => {
            let DataType::Struct(entry_fields) = entries.data_type() else {
                return data_type.clone();
            };
            let (Some(key), Some(value), 2) = (
                spark_type.get("keyType"),
                spark_type.get("valueType"),
                entry_fields.len(),
            ) else {
                return data_type.clone();
            };
            let entry_fields = Fields::from(vec![
                restore_field(&entry_fields[0], key),
                restore_field(&entry_fields[1], value),
            ]);
            let entries = entries
                .as_ref()
                .clone()
                .with_data_type(DataType::Struct(entry_fields));
            DataType::Map(Arc::new(entries), *sorted)
        }
        _ => data_type.clone(),
    }
}

fn is_udt(spark_type: &Value) -> bool {
    spark_type.get("type").and_then(Value::as_str) == Some("udt")
}

/// Returns the metadata of the user-defined type if the Spark type is one.
fn udt_metadata(spark_type: &Value) -> Option<String> {
    if !is_udt(spark_type) {
        return None;
    }
    let string = |key: &str| {
        spark_type
            .get(key)
            .and_then(Value::as_str)
            .map(ToString::to_string)
    };
    let metadata = SparkUdtMetadata {
        jvm_class: string("class"),
        python_class: string("pyClass"),
        serialized_python_class: string("serializedClass"),
    };
    serde_json::to_string(&metadata).ok()
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, clippy::panic)]

    use super::*;

    fn udt(field: &Field) -> Option<SparkUdtMetadata> {
        field
            .metadata()
            .get(SAIL_SPARK_UDT_METADATA_KEY)
            .map(|udt| serde_json::from_str(udt).unwrap())
    }

    const POINT: &str = r#"{"type":"udt","class":"org.example.PointUDT","pyClass":"example.PointUDT",
        "sqlType":{"type":"array","elementType":"double","containsNull":false}}"#;

    #[test]
    fn test_restore_udt_field() {
        let item = Arc::new(Field::new("item", DataType::Float64, false));
        let schema = Schema::new(vec![
            Field::new("key", DataType::Int64, true),
            Field::new("point", DataType::List(item), true),
        ]);
        let json = format!(
            r#"{{"type":"struct","fields":[
                {{"name":"key","type":"long","nullable":true,"metadata":{{}}}},
                {{"name":"point","type":{POINT},"nullable":true,"metadata":{{}}}}]}}"#
        );
        let schema = restore_spark_udts(schema, Some(&json));
        assert_eq!(udt(schema.field(0)), None);
        assert_eq!(
            udt(schema.field(1)),
            Some(SparkUdtMetadata {
                jvm_class: Some("org.example.PointUDT".to_string()),
                python_class: Some("example.PointUDT".to_string()),
                serialized_python_class: None,
            })
        );
    }

    #[test]
    fn test_restore_nested_udt_fields() {
        let point = DataType::List(Arc::new(Field::new("item", DataType::Float64, false)));
        let array = DataType::List(Arc::new(Field::new("item", point.clone(), true)));
        let entries = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Int64, false),
                Field::new("value", point.clone(), true),
            ])),
            false,
        );
        let schema = Schema::new(vec![
            Field::new("array", array, true),
            Field::new("map", DataType::Map(Arc::new(entries), false), true),
            Field::new(
                "struct",
                DataType::Struct(Fields::from(vec![Field::new("p", point, true)])),
                true,
            ),
        ]);
        let json = format!(
            r#"{{"type":"struct","fields":[
                {{"name":"array","nullable":true,"metadata":{{}},"type":
                    {{"type":"array","elementType":{POINT},"containsNull":true}}}},
                {{"name":"map","nullable":true,"metadata":{{}},"type":
                    {{"type":"map","keyType":"long","valueType":{POINT},"valueContainsNull":true}}}},
                {{"name":"struct","nullable":true,"metadata":{{}},"type":
                    {{"type":"struct","fields":[{{"name":"p","type":{POINT},"nullable":true,"metadata":{{}}}}]}}}}]}}"#
        );
        let schema = restore_spark_udts(schema, Some(&json));
        let DataType::List(item) = schema.field(0).data_type() else {
            panic!("expected a list");
        };
        assert!(udt(item).is_some());
        let DataType::Map(entries, _) = schema.field(1).data_type() else {
            panic!("expected a map");
        };
        let DataType::Struct(entry_fields) = entries.data_type() else {
            panic!("expected a struct");
        };
        assert!(udt(&entry_fields[0]).is_none());
        assert!(udt(&entry_fields[1]).is_some());
        let DataType::Struct(fields) = schema.field(2).data_type() else {
            panic!("expected a struct");
        };
        assert!(udt(&fields[0]).is_some());
    }

    #[test]
    fn test_schema_is_unchanged_without_valid_metadata() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        assert_eq!(restore_spark_udts(schema.clone(), None), schema);
        assert_eq!(restore_spark_udts(schema.clone(), Some("not json")), schema);
        assert_eq!(restore_spark_udts(schema.clone(), Some("{}")), schema);
    }
}
