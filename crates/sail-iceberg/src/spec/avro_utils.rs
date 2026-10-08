// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use apache_avro::Schema as AvroSchema;
use apache_avro::schema::{RecordField as AvroRecordField, RecordFieldOrder, UnionSchema};
use serde_json::{Number, Value as JsonValue};

/// Avro custom attribute used to annotate Iceberg field ids.
pub const FIELD_ID_ATTR: &str = "field-id";
/// Avro custom attribute used to retain an Iceberg field's original name.
pub const ICEBERG_FIELD_NAME_ATTR: &str = "iceberg-field-name";

/// Convert an Iceberg field name to a valid Avro field name.
pub(crate) fn avro_compatible_name(name: &str) -> String {
    let mut output = String::with_capacity(name.len());
    for (index, character) in name.chars().enumerate() {
        let valid = if index == 0 {
            character.is_ascii_alphabetic() || character == '_'
        } else {
            character.is_ascii_alphanumeric() || character == '_'
        };
        if valid {
            output.push(character);
        } else if index == 0 && character.is_ascii_digit() {
            output.push('_');
            output.push(character);
        } else {
            let mut encoded = [0; 2];
            for code_unit in character.encode_utf16(&mut encoded) {
                output.push_str(&format!("_x{code_unit:X}"));
            }
        }
    }
    output
}

/// Wrap a schema in an optional (null-union) Avro schema.
pub fn optional(schema: AvroSchema) -> AvroSchema {
    #[expect(clippy::unwrap_used)]
    AvroSchema::Union(UnionSchema::new(vec![AvroSchema::Null, schema]).unwrap())
}

/// Build an Avro record field annotated with Iceberg's field id attribute.
pub fn record_field(
    name: &str,
    schema: AvroSchema,
    field_id: i32,
    required: bool,
) -> AvroRecordField {
    let avro_name = avro_compatible_name(name);
    let mut schema = schema;
    let default = if required {
        None
    } else {
        Some(JsonValue::Null)
    };
    if !required {
        schema = optional(schema);
    }
    let mut field = AvroRecordField {
        name: avro_name.clone(),
        doc: None,
        default,
        aliases: None,
        order: RecordFieldOrder::Ignore,
        position: 0,
        schema,
        custom_attributes: Default::default(),
    };
    field.custom_attributes.insert(
        FIELD_ID_ATTR.to_string(),
        JsonValue::Number(Number::from(field_id)),
    );
    if avro_name != name {
        field.custom_attributes.insert(
            ICEBERG_FIELD_NAME_ATTR.to_string(),
            JsonValue::String(name.to_string()),
        );
    }
    field
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn special_iceberg_names_use_avro_compatible_names() {
        let cases = [
            ("9x", "_9x"),
            ("x_", "x_"),
            ("a.b", "a_x2Eb"),
            ("☃", "_x2603"),
            ("a#b", "a_x23b"),
            ("part/name", "part_x2Fname"),
        ];
        for (name, expected) in cases {
            assert_eq!(avro_compatible_name(name), expected);
        }

        let field = record_field("part/name", AvroSchema::String, 1000, true);
        assert_eq!(field.name, "part_x2Fname");
        assert_eq!(
            field.custom_attributes.get(ICEBERG_FIELD_NAME_ATTR),
            Some(&JsonValue::String("part/name".to_string()))
        );

        let field = record_field("value", AvroSchema::String, 1001, true);
        assert!(
            !field
                .custom_attributes
                .contains_key(ICEBERG_FIELD_NAME_ATTR)
        );
    }
}
