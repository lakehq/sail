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

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::spec::catalog::NamespaceIdent;
use crate::spec::SchemaId;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ViewVersion {
    #[serde(rename = "version-id")]
    pub version_id: i32,
    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: i64,
    #[serde(rename = "schema-id")]
    pub schema_id: SchemaId,
    #[serde(default, rename = "default-catalog")]
    pub default_catalog: Option<String>,
    #[serde(rename = "default-namespace")]
    pub default_namespace: NamespaceIdent,
    #[serde(default)]
    pub summary: HashMap<String, String>,
    pub representations: ViewRepresentations,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ViewRepresentations(pub Vec<ViewRepresentation>);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum ViewRepresentation {
    #[serde(rename_all = "kebab-case")]
    Sql {
        sql: String,
        #[serde(default)]
        dialect: String,
    },
}
