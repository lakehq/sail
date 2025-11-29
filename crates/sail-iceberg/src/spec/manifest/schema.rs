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

use std::collections::BTreeMap;

use apache_avro::schema::{
    ArraySchema, DecimalSchema, FixedSchema, Name, RecordField as AvroRecordField,
    RecordFieldOrder, RecordSchema,
};
use apache_avro::Schema as AvroSchema;
use serde_json::{Number, Value as JsonValue};

use crate::spec::avro_utils::{optional, record_field, FIELD_ID_ATTR};
use crate::spec::types::{PrimitiveType, StructType, Type};

const ELEMENT_ID: &str = "element-id";
const LOGICAL_TYPE: &str = "logicalType";
const MAP_LOGICAL_TYPE: &str = "map";

fn avro_primitive(prim: &PrimitiveType) -> AvroSchema {
    match prim {
        PrimitiveType::Boolean => AvroSchema::Boolean,
        PrimitiveType::Int => AvroSchema::Int,
        PrimitiveType::Long => AvroSchema::Long,
        PrimitiveType::Float => AvroSchema::Float,
        PrimitiveType::Double => AvroSchema::Double,
        PrimitiveType::Date => AvroSchema::Date,
        PrimitiveType::Time => AvroSchema::TimeMicros,
        PrimitiveType::Timestamp => AvroSchema::TimestampMicros,
        PrimitiveType::Timestamptz => AvroSchema::TimestampMicros,
        PrimitiveType::TimestampNs => AvroSchema::TimestampNanos,
        PrimitiveType::TimestamptzNs => AvroSchema::TimestampNanos,
        PrimitiveType::String => AvroSchema::String,
        PrimitiveType::Uuid => AvroSchema::Uuid,
        PrimitiveType::Fixed(len) => AvroSchema::Fixed(FixedSchema {
            #[allow(clippy::unwrap_used)]
            name: Name::new(format!("fixed_{len}").as_str())
                .unwrap_or_else(|_| Name::new("fixed").unwrap()),
            aliases: None,
            doc: None,
            size: *len as usize,
            attributes: Default::default(),
            default: None,
        }),
        PrimitiveType::Binary => AvroSchema::Bytes,
        PrimitiveType::Decimal { precision, scale } => AvroSchema::Decimal(DecimalSchema {
            precision: *precision as usize,
            scale: *scale as usize,
            inner: Box::new(AvroSchema::Fixed(FixedSchema {
                #[allow(clippy::unwrap_used)]
                name: Name::new(format!("decimal_{precision}_{scale}").as_str())
                    .unwrap_or_else(|_| Name::new("decimal").unwrap()),
                aliases: None,
                doc: None,
                size: crate::spec::Type::decimal_required_bytes(*precision).unwrap_or(16) as usize,
                attributes: Default::default(),
                default: None,
            })),
        }),
    }
}

fn struct_to_avro_record(name: &str, s: &StructType) -> AvroSchema {
    let fields = s
        .fields()
        .iter()
        .map(|f| match &*f.field_type {
            Type::Primitive(p) => record_field(&f.name, avro_primitive(p), f.id, f.required),
            Type::Struct(inner) => record_field(
                &f.name,
                struct_to_avro_record(&format!("r{}", f.id), inner),
                f.id,
                f.required,
            ),
            Type::List(list) => {
                let mut attrs = BTreeMap::new();
                attrs.insert(
                    ELEMENT_ID.to_string(),
                    JsonValue::Number(Number::from(list.element_field.id)),
                );
                let elem_schema = match &*list.element_field.field_type {
                    Type::Primitive(p) => avro_primitive(p),
                    Type::Struct(inner) => {
                        struct_to_avro_record(&format!("r{}", list.element_field.id), inner)
                    }
                    _ => AvroSchema::String,
                };
                let array = AvroSchema::Array(ArraySchema {
                    items: Box::new(if list.element_field.required {
                        elem_schema
                    } else {
                        optional(elem_schema)
                    }),
                    attributes: attrs,
                });
                record_field(&f.name, array, f.id, f.required)
            }
            Type::Map(map) => {
                // Represent non-string-key maps as array of records with logicalType: map
                #[allow(clippy::unwrap_used)]
                let key_field = AvroRecordField {
                    name: map.key_field.name.clone(),
                    doc: None,
                    default: None,
                    aliases: None,
                    order: RecordFieldOrder::Ascending,
                    position: 0,
                    schema: match &*map.key_field.field_type {
                        Type::Primitive(p) => avro_primitive(p),
                        _ => AvroSchema::String,
                    },
                    custom_attributes: BTreeMap::from([(
                        FIELD_ID_ATTR.to_string(),
                        JsonValue::Number(Number::from(map.key_field.id)),
                    )]),
                };
                let value_schema = match &*map.value_field.field_type {
                    Type::Primitive(p) => avro_primitive(p),
                    _ => AvroSchema::String,
                };
                let value_field = AvroRecordField {
                    name: map.value_field.name.clone(),
                    doc: None,
                    default: if map.value_field.required {
                        None
                    } else {
                        Some(JsonValue::Null)
                    },
                    aliases: None,
                    order: RecordFieldOrder::Ignore,
                    position: 0,
                    schema: if map.value_field.required {
                        value_schema
                    } else {
                        optional(value_schema)
                    },
                    custom_attributes: BTreeMap::from([(
                        FIELD_ID_ATTR.to_string(),
                        JsonValue::Number(Number::from(map.value_field.id)),
                    )]),
                };
                let item_fields = vec![key_field, value_field];
                let mut item_lookup = BTreeMap::new();
                for (idx, f) in item_fields.iter().enumerate() {
                    item_lookup.insert(f.name.clone(), idx);
                }
                let item_record = AvroSchema::Record(RecordSchema {
                    #[allow(clippy::unwrap_used)]
                    name: Name::new(
                        format!("k{}_v{}", map.key_field.id, map.value_field.id).as_str(),
                    )
                    .unwrap_or_else(|_| Name::new("map_item").unwrap()),
                    aliases: None,
                    doc: None,
                    fields: item_fields,
                    lookup: item_lookup,
                    attributes: Default::default(),
                });
                let mut attrs = BTreeMap::new();
                attrs.insert(
                    LOGICAL_TYPE.to_string(),
                    JsonValue::String(MAP_LOGICAL_TYPE.to_string()),
                );
                let array = AvroSchema::Array(ArraySchema {
                    items: Box::new(item_record),
                    attributes: attrs,
                });
                record_field(&f.name, array, f.id, f.required)
            }
        })
        .collect::<Vec<_>>();

    // Populate lookup map so apache_avro can resolve fields by name
    let mut lookup = BTreeMap::new();
    for (idx, f) in fields.iter().enumerate() {
        lookup.insert(f.name.clone(), idx);
    }
    AvroSchema::Record(RecordSchema {
        #[allow(clippy::unwrap_used)]
        name: Name::new(name).unwrap_or_else(|_| Name::new("record").unwrap()),
        aliases: None,
        doc: None,
        fields,
        lookup,
        attributes: Default::default(),
    })
}

fn partition_record_schema(partition_type: &StructType) -> AvroSchema {
    struct_to_avro_record("r102", partition_type)
}

fn array_of_longs(element_id: i32, required: bool) -> AvroSchema {
    let mut attrs = BTreeMap::new();
    attrs.insert(
        ELEMENT_ID.to_string(),
        JsonValue::Number(Number::from(element_id)),
    );
    let array = AvroSchema::Array(ArraySchema {
        items: Box::new(AvroSchema::Long),
        attributes: attrs,
    });
    if required {
        array
    } else {
        optional(array)
    }
}

fn array_of_ints(element_id: i32, required: bool) -> AvroSchema {
    let mut attrs = BTreeMap::new();
    attrs.insert(
        ELEMENT_ID.to_string(),
        JsonValue::Number(Number::from(element_id)),
    );
    let array = AvroSchema::Array(ArraySchema {
        items: Box::new(AvroSchema::Int),
        attributes: attrs,
    });
    if required {
        array
    } else {
        optional(array)
    }
}

pub fn data_file_schema_v2(partition_type: &StructType) -> AvroSchema {
    let fields = vec![
        record_field("content", AvroSchema::Int, 134, true),
        record_field("file_path", AvroSchema::String, 100, true),
        record_field("file_format", AvroSchema::String, 101, true),
        record_field(
            "partition",
            partition_record_schema(partition_type),
            102,
            false,
        ),
        record_field("record_count", AvroSchema::Long, 103, true),
        record_field("file_size_in_bytes", AvroSchema::Long, 104, true),
        record_field("key_metadata", AvroSchema::Bytes, 131, false),
        // split_offsets: array<long> element-id 133
        AvroRecordField {
            name: "split_offsets".to_string(),
            doc: None,
            default: Some(JsonValue::Null),
            aliases: None,
            order: RecordFieldOrder::Ignore,
            position: 0,
            schema: array_of_longs(133, false),
            custom_attributes: BTreeMap::from([(
                FIELD_ID_ATTR.to_string(),
                JsonValue::Number(Number::from(132)),
            )]),
        },
        // equality_ids: array<int> element-id 136
        AvroRecordField {
            name: "equality_ids".to_string(),
            doc: None,
            default: Some(JsonValue::Null),
            aliases: None,
            order: RecordFieldOrder::Ignore,
            position: 0,
            schema: array_of_ints(136, false),
            custom_attributes: BTreeMap::from([(
                FIELD_ID_ATTR.to_string(),
                JsonValue::Number(Number::from(135)),
            )]),
        },
        record_field("sort_order_id", AvroSchema::Int, 140, false),
        record_field("first_row_id", AvroSchema::Long, 142, false),
        record_field("referenced_data_file", AvroSchema::String, 143, false),
        record_field("content_offset", AvroSchema::Long, 144, false),
        record_field("content_size_in_bytes", AvroSchema::Long, 145, false),
    ];

    let mut lookup = BTreeMap::new();
    for (idx, f) in fields.iter().enumerate() {
        lookup.insert(f.name.clone(), idx);
    }
    AvroSchema::Record(RecordSchema {
        #[allow(clippy::unwrap_used)]
        name: Name::new("data_file").unwrap_or_else(|_| Name::new("data_file_fallback").unwrap()),
        aliases: None,
        doc: None,
        fields,
        lookup,
        attributes: Default::default(),
    })
}

pub fn manifest_entry_schema_v2(partition_type: &StructType) -> AvroSchema {
    let df_schema = data_file_schema_v2(partition_type);
    let fields = vec![
        record_field("status", AvroSchema::Int, 0, true),
        record_field("snapshot_id", AvroSchema::Long, 1, false),
        record_field("sequence_number", AvroSchema::Long, 3, false),
        record_field("file_sequence_number", AvroSchema::Long, 4, false),
        AvroRecordField {
            name: "data_file".to_string(),
            doc: None,
            default: None,
            aliases: None,
            order: RecordFieldOrder::Ignore,
            position: 0,
            schema: df_schema,
            custom_attributes: BTreeMap::from([(
                FIELD_ID_ATTR.to_string(),
                JsonValue::Number(Number::from(2)),
            )]),
        },
    ];

    let mut lookup = BTreeMap::new();
    for (idx, f) in fields.iter().enumerate() {
        lookup.insert(f.name.clone(), idx);
    }
    AvroSchema::Record(RecordSchema {
        #[allow(clippy::unwrap_used)]
        name: Name::new("manifest_entry")
            .unwrap_or_else(|_| Name::new("manifest_entry_fallback").unwrap()),
        aliases: None,
        doc: None,
        fields,
        lookup,
        attributes: Default::default(),
    })
}
