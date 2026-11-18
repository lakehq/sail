// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::spec::SchemaId;
use crate::spec::catalog::NamespaceIdent;

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
