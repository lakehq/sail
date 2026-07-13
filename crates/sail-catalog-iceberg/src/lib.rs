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

mod adapter;
mod provider;

#[expect(unused_imports)]
#[expect(clippy::enum_variant_names)]
#[expect(clippy::match_overlapping_arm)]
#[expect(clippy::doc_lazy_continuation)]
pub mod r#gen {
    include!(concat!(env!("OUT_DIR"), "/iceberg_rest_catalog_gen.rs"));

    // The discriminator is missing for `ReportMetricsRequest` in the OpenAPI specification,
    // so this schema is excluded from generation and defined manually.
    // See also: https://github.com/apache/iceberg/issues/12696
    #[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
    #[serde(tag = "report-type")]
    pub enum ReportMetricsRequest {
        #[serde(rename = "scan-report")]
        ScanReport(Box<ScanReport>),
        #[serde(rename = "commit-report")]
        CommitReport(Box<CommitReport>),
    }

    // TODO: Generate this schema once the OpenAPI generator supports objects with both fixed
    // properties and additional properties.
    pub mod snapshot {
        #[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
        pub enum Operation {
            #[serde(rename = "append")]
            Append,
            #[serde(rename = "replace")]
            Replace,
            #[serde(rename = "overwrite")]
            Overwrite,
            #[serde(rename = "delete")]
            Delete,
        }

        impl std::fmt::Display for Operation {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    Self::Append => f.write_str("append"),
                    Self::Replace => f.write_str("replace"),
                    Self::Overwrite => f.write_str("overwrite"),
                    Self::Delete => f.write_str("delete"),
                }
            }
        }

        #[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
        pub struct Summary {
            pub operation: Operation,
            #[serde(flatten)]
            pub additional_properties: std::collections::HashMap<String, String>,
        }
    }

    #[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
    pub struct Snapshot {
        #[serde(rename = "added-rows")]
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub added_rows: Option<i64>,
        #[serde(rename = "first-row-id")]
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub first_row_id: Option<i64>,
        #[serde(rename = "manifest-list")]
        pub manifest_list: String,
        #[serde(rename = "parent-snapshot-id")]
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub parent_snapshot_id: Option<i64>,
        #[serde(rename = "schema-id")]
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub schema_id: Option<i32>,
        #[serde(rename = "sequence-number")]
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub sequence_number: Option<i64>,
        #[serde(rename = "snapshot-id")]
        pub snapshot_id: i64,
        pub summary: snapshot::Summary,
        #[serde(rename = "timestamp-ms")]
        pub timestamp_ms: i64,
    }
}

pub use provider::{
    IcebergRestCatalogOptions, IcebergRestCatalogProvider, REST_CATALOG_PROP_PREFIX,
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE,
};
