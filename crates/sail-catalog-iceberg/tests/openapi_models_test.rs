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

use sail_catalog_iceberg::r#gen::TableUpdate;

#[test]
fn add_snapshot_update_preserves_summary_additional_properties() -> Result<(), serde_json::Error> {
    let input = serde_json::json!({
        "action": "add-snapshot",
        "snapshot": {
            "snapshot-id": 123,
            "sequence-number": 2,
            "timestamp-ms": 456,
            "manifest-list": "s3://bucket/table/metadata/snap.avro",
            "summary": {
                "operation": "delete",
                "added-delete-files": "1",
                "added-equality-delete-files": "1",
                "total-delete-files": "1"
            },
            "schema-id": 0
        }
    });

    let update: TableUpdate = serde_json::from_value(input.clone())?;
    let output = serde_json::to_value(update)?;

    assert_eq!(output, input);
    Ok(())
}
