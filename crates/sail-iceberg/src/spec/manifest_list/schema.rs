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

use apache_avro::schema::{ArraySchema, Name, RecordField as AvroRecordField, RecordSchema};
use apache_avro::Schema as AvroSchema;
use once_cell::sync::Lazy;
use serde_json::{Number, Value as JsonValue};

use crate::spec::avro_utils::record_field;

fn partitions_field() -> AvroRecordField {
    // element record for partitions: field_summary
    let fields = vec![
        record_field("contains_null", AvroSchema::Boolean, 509, true),
        record_field("contains_nan", AvroSchema::Boolean, 518, false),
        record_field("lower_bound", AvroSchema::Bytes, 510, false),
        record_field("upper_bound", AvroSchema::Bytes, 511, false),
    ];

    // Provide lookup for element record
    let mut lookup = BTreeMap::new();
    for (idx, f) in fields.iter().enumerate() {
        lookup.insert(f.name.clone(), idx);
    }

    let element_record = AvroSchema::Record(RecordSchema {
        #[allow(clippy::unwrap_used)]
        name: Name::new("field_summary").unwrap_or_else(|_| Name::new("field_summary").unwrap()),
        aliases: None,
        doc: None,
        fields,
        lookup,
        attributes: Default::default(),
    });

    // Wrap element record into array with element-id 508
    let mut attrs = BTreeMap::new();
    attrs.insert(
        "element-id".to_string(),
        JsonValue::Number(Number::from(508)),
    );
    let array = AvroSchema::Array(ArraySchema {
        items: Box::new(element_record),
        attributes: attrs,
    });

    // record_field already marked field-id; nothing else to tweak
    record_field("partitions", array, 507, false)
}

/// Typed Avro schema for manifest list V2 entries (record name: manifest_file)
pub static MANIFEST_LIST_AVRO_SCHEMA_V2: Lazy<AvroSchema> = Lazy::new(|| {
    let fields = vec![
        record_field("manifest_path", AvroSchema::String, 500, true),
        record_field("manifest_length", AvroSchema::Long, 501, true),
        record_field("partition_spec_id", AvroSchema::Int, 502, true),
        record_field("content", AvroSchema::Int, 517, true),
        record_field("sequence_number", AvroSchema::Long, 515, true),
        record_field("min_sequence_number", AvroSchema::Long, 516, true),
        record_field("added_snapshot_id", AvroSchema::Long, 503, true),
        // In V2 these are required. We set required=true to avoid unions here.
        record_field("added_files_count", AvroSchema::Int, 504, true),
        record_field("existing_files_count", AvroSchema::Int, 505, true),
        record_field("deleted_files_count", AvroSchema::Int, 506, true),
        record_field("added_rows_count", AvroSchema::Long, 512, true),
        record_field("existing_rows_count", AvroSchema::Long, 513, true),
        record_field("deleted_rows_count", AvroSchema::Long, 514, true),
        partitions_field(),
        record_field("key_metadata", AvroSchema::Bytes, 519, false),
    ];

    let mut lookup = BTreeMap::new();
    for (idx, f) in fields.iter().enumerate() {
        lookup.insert(f.name.clone(), idx);
    }

    AvroSchema::Record(RecordSchema {
        #[allow(clippy::unwrap_used)]
        name: Name::new("manifest_file").unwrap_or_else(|_| Name::new("manifest_file").unwrap()),
        aliases: None,
        doc: None,
        fields,
        lookup,
        attributes: Default::default(),
    })
});

// TODO(V1): Add MANIFEST_LIST_AVRO_SCHEMA_V1 when implementing typed V1 writer
