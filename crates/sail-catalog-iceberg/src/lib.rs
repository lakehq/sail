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
}

pub use provider::{
    IcebergRestCatalogOptions, IcebergRestCatalogProvider, REST_CATALOG_PROP_PREFIX,
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE,
};
