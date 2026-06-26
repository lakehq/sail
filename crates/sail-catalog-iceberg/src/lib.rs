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
#![expect(clippy::all)]

pub mod apis;
pub mod models;
mod planning;
mod provider;

pub use planning::{
    completed_planning_result_from_values, completed_planning_with_id_result_from_values,
    fetch_scan_tasks_result_from_values,
};
pub use provider::{
    load_table_result_to_status, IcebergRestCatalogProvider, REST_CATALOG_PROP_PREFIX,
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE,
};

pub use crate::models::{LoadTableResult, TableMetadata};

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
        assert_eq!(request.updates.len(), 1);
        assert!(matches!(
            request.updates[0],
            models::TableUpdate::SetPropertiesUpdate {}
        ));
    }

    #[test]
    fn table_update_discriminates_on_action() {
        let cases = [
            ("assign-uuid", "AssignUuidUpdate"),
            ("add-snapshot", "AddSnapshotUpdate"),
            ("set-location", "SetLocationUpdate"),
            ("remove-properties", "RemovePropertiesUpdate"),
            ("upgrade-format-version", "UpgradeFormatVersionUpdate"),
        ];
        for (action, _name) in cases {
            let update: models::TableUpdate =
                serde_json::from_value(serde_json::json!({"action": action}))
                    .unwrap_or_else(|err| panic!("action {action} must deserialize: {err}"));
            // Round-trips back to the same tagged action.
            let value = serde_json::to_value(&update).unwrap();
            assert_eq!(value["action"], serde_json::json!(action));
        }
    }

    #[test]
    fn view_update_accepts_set_properties() {
        let request: models::CommitViewRequest = serde_json::from_value(serde_json::json!({
            "updates": [{"action": "set-properties", "updates": {"k": "v"}}]
        }))
        .expect("view set-properties commit must deserialize");
        assert!(matches!(
            request.updates[0],
            models::ViewUpdate::SetPropertiesUpdate {}
        ));
    }

    #[test]
    fn unknown_action_is_rejected() {
        let result: Result<models::TableUpdate, _> =
            serde_json::from_value(serde_json::json!({"action": "not-a-real-update"}));
        assert!(result.is_err(), "unknown action must not deserialize");
    }
}
