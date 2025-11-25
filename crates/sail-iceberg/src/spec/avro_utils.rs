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

use apache_avro::schema::{RecordField as AvroRecordField, RecordFieldOrder, UnionSchema};
use apache_avro::Schema as AvroSchema;
use serde_json::{Number, Value as JsonValue};

/// Avro custom attribute used to annotate Iceberg field ids.
pub const FIELD_ID_ATTR: &str = "field-id";

/// Wrap a schema in an optional (null-union) Avro schema.
pub fn optional(schema: AvroSchema) -> AvroSchema {
    #[allow(clippy::unwrap_used)]
    AvroSchema::Union(UnionSchema::new(vec![AvroSchema::Null, schema]).unwrap())
}

/// Build an Avro record field annotated with Iceberg's field id attribute.
pub fn record_field(
    name: &str,
    schema: AvroSchema,
    field_id: i32,
    required: bool,
) -> AvroRecordField {
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
        name: name.to_string(),
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
    field
}
