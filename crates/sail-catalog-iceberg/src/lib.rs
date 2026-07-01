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

#![expect(unused_imports)]
#![expect(dead_code)]
#![expect(clippy::all)]

pub mod apis;
pub mod gen {
    #![expect(clippy::enum_variant_names)]
    include!(concat!(env!("OUT_DIR"), "/iceberg_rest_catalog_gen.rs"));
}
mod models;
mod provider;

pub use provider::{
    IcebergRestCatalogOptions, IcebergRestCatalogProvider, REST_CATALOG_PROP_PREFIX,
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE,
};

#[cfg(test)]
mod table_update_tests {
    //! Regression tests for the discriminated `TableUpdate` / `ViewUpdate`
    //! models. Before the spec was given a discriminator, these were generated
    //! as flat structs with every field required, so any real update (e.g.
    //! `set-properties`) failed to deserialize with `missing field uuid`.
    #![expect(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use crate::models;

    #[test]
    fn commit_table_request_accepts_set_properties_update() {
        let request: models::CommitTableRequest = serde_json::from_value(serde_json::json!({
            "requirements": [{"type": "assert-table-uuid", "uuid": "11111111-1111-1111-1111-111111111111"}],
            "updates": [{"action": "set-properties", "updates": {"k": "v"}}]
        }))
        .expect("set-properties commit must deserialize");
        assert_eq!(request.requirements.len(), 1);
        assert!(matches!(
            &request.requirements[0],
            models::TableRequirement::AssertTableUuid { uuid }
                if uuid == "11111111-1111-1111-1111-111111111111"
        ));
        assert_eq!(request.updates.len(), 1);
        assert!(matches!(
            &request.updates[0],
            models::TableUpdate::SetPropertiesUpdate { updates }
                if updates.get("k").map(String::as_str) == Some("v")
        ));
    }

    #[test]
    fn commit_table_request_preserves_ref_snapshot_requirement() {
        let request = models::CommitTableRequest::new(
            vec![models::TableRequirement::AssertRefSnapshotId {
                r#ref: "main".to_string(),
                snapshot_id: None,
            }],
            vec![models::TableUpdate::SetSnapshotRefUpdate {
                ref_name: "main".to_string(),
                r#type: models::set_snapshot_ref_update::Type::Branch,
                snapshot_id: 123,
                max_ref_age_ms: None,
                max_snapshot_age_ms: None,
                min_snapshots_to_keep: None,
            }],
        );
        let value = serde_json::to_value(request).expect("commit request must serialize");
        assert_eq!(
            value["requirements"][0],
            serde_json::json!({
                "type": "assert-ref-snapshot-id",
                "ref": "main",
                "snapshot-id": null
            })
        );
        assert_eq!(
            value["updates"][0],
            serde_json::json!({
                "action": "set-snapshot-ref",
                "ref-name": "main",
                "type": "branch",
                "snapshot-id": 123
            })
        );
    }

    #[test]
    fn table_update_discriminates_on_action() {
        let cases = [
            serde_json::json!({"action": "assign-uuid", "uuid": "table-uuid"}),
            serde_json::json!({"action": "set-location", "location": "s3://bucket/table"}),
            serde_json::json!({"action": "remove-properties", "removals": ["k"]}),
            serde_json::json!({"action": "set-properties", "updates": {"k": "v"}}),
            serde_json::json!({"action": "upgrade-format-version", "format-version": 2}),
        ];
        for input in cases {
            let update: models::TableUpdate =
                serde_json::from_value(input.clone()).unwrap_or_else(|err| {
                    panic!("update {input} must deserialize and preserve fields: {err}")
                });
            let value = serde_json::to_value(&update).unwrap();
            assert_eq!(value, input);
        }
    }

    #[test]
    fn commit_view_request_preserves_requirements_and_updates() {
        let request: models::CommitViewRequest = serde_json::from_value(serde_json::json!({
            "requirements": [{"type": "assert-view-uuid", "uuid": "22222222-2222-2222-2222-222222222222"}],
            "updates": [{"action": "set-properties", "updates": {"k": "v"}}]
        }))
        .expect("view set-properties commit must deserialize");
        let requirement = request
            .requirements
            .as_ref()
            .expect("view requirements must deserialize");
        assert!(matches!(
            &requirement[0],
            models::ViewRequirement::AssertViewUuid { uuid }
                if uuid == "22222222-2222-2222-2222-222222222222"
        ));
        assert!(matches!(
            &request.updates[0],
            models::ViewUpdate::SetPropertiesUpdate { updates }
                if updates.get("k").map(String::as_str) == Some("v")
        ));
    }

    #[test]
    fn view_update_discriminates_on_action() {
        let cases = [
            serde_json::json!({"action": "assign-uuid", "uuid": "view-uuid"}),
            serde_json::json!({"action": "set-current-view-version", "view-version-id": 7}),
            serde_json::json!({"action": "set-location", "location": "s3://bucket/view"}),
            serde_json::json!({"action": "remove-properties", "removals": ["k"]}),
            serde_json::json!({"action": "set-properties", "updates": {"k": "v"}}),
            serde_json::json!({"action": "upgrade-format-version", "format-version": 1}),
        ];
        for input in cases {
            let update: models::ViewUpdate =
                serde_json::from_value(input.clone()).unwrap_or_else(|err| {
                    panic!("view update {input} must deserialize and preserve fields: {err}")
                });
            let value = serde_json::to_value(&update).unwrap();
            assert_eq!(value, input);
        }
    }

    #[test]
    fn unknown_action_is_rejected() {
        let result: Result<models::TableUpdate, _> =
            serde_json::from_value(serde_json::json!({"action": "not-a-real-update"}));
        assert!(result.is_err(), "unknown action must not deserialize");

        let result: Result<models::ViewUpdate, _> =
            serde_json::from_value(serde_json::json!({"action": "not-a-real-update"}));
        assert!(result.is_err(), "unknown view action must not deserialize");
    }
}
