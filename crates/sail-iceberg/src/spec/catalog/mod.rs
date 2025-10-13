// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// [CREDIT]: https://raw.githubusercontent.com/apache/iceberg-rust/dc349284a4204c1a56af47fb3177ace6f9e899a0/crates/iceberg/src/catalog/mod.rs

use std::collections::HashMap;
use std::fmt::Display;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::spec::partition::UnboundPartitionSpec;
use crate::spec::views::ViewVersion;
use crate::spec::{
    FormatVersion, PartitionStatisticsFile, Schema, SchemaId, Snapshot, SnapshotReference,
    SortOrder, StatisticsFile,
};

pub mod metadata_location;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NamespaceIdent(Vec<String>);

impl NamespaceIdent {
    pub fn new(name: String) -> Self {
        Self(vec![name])
    }
    pub fn from_vec(names: Vec<String>) -> Self {
        Self(names)
    }
    pub fn inner(self) -> Vec<String> {
        self.0
    }
}

impl AsRef<Vec<String>> for NamespaceIdent {
    fn as_ref(&self) -> &Vec<String> {
        &self.0
    }
}

impl Display for NamespaceIdent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.join("."))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Namespace {
    name: NamespaceIdent,
    properties: HashMap<String, String>,
}

impl Namespace {
    pub fn new(name: NamespaceIdent) -> Self {
        Self {
            name,
            properties: HashMap::new(),
        }
    }
    pub fn with_properties(name: NamespaceIdent, properties: HashMap<String, String>) -> Self {
        Self { name, properties }
    }
    pub fn name(&self) -> &NamespaceIdent {
        &self.name
    }
    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableIdent {
    pub namespace: NamespaceIdent,
    pub name: String,
}

impl TableIdent {
    pub fn new(namespace: NamespaceIdent, name: String) -> Self {
        Self { namespace, name }
    }
    pub fn namespace(&self) -> &NamespaceIdent {
        &self.namespace
    }
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl Display for TableIdent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.namespace, self.name)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct TableCreation {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    pub schema: Schema,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_spec: Option<UnboundPartitionSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sort_order: Option<SortOrder>,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum TableRequirement {
    #[serde(rename = "assert-create")]
    NotExist,
    #[serde(rename = "assert-table-uuid")]
    UuidMatch { uuid: Uuid },
    #[serde(rename = "assert-ref-snapshot-id")]
    RefSnapshotIdMatch {
        r#ref: String,
        #[serde(rename = "snapshot-id")]
        snapshot_id: Option<i64>,
    },
    #[serde(rename = "assert-last-assigned-field-id")]
    LastAssignedFieldIdMatch {
        #[serde(rename = "last-assigned-field-id")]
        last_assigned_field_id: i32,
    },
    #[serde(rename = "assert-current-schema-id")]
    CurrentSchemaIdMatch {
        #[serde(rename = "current-schema-id")]
        current_schema_id: SchemaId,
    },
    #[serde(rename = "assert-last-assigned-partition-id")]
    LastAssignedPartitionIdMatch {
        #[serde(rename = "last-assigned-partition-id")]
        last_assigned_partition_id: i32,
    },
    #[serde(rename = "assert-default-spec-id")]
    DefaultSpecIdMatch {
        #[serde(rename = "default-spec-id")]
        default_spec_id: i32,
    },
    #[serde(rename = "assert-default-sort-order-id")]
    DefaultSortOrderIdMatch {
        #[serde(rename = "default-sort-order-id")]
        default_sort_order_id: i64,
    },
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum TableUpdate {
    UpgradeFormatVersion {
        format_version: FormatVersion,
    },
    AssignUuid {
        uuid: Uuid,
    },
    AddSchema {
        schema: Box<Schema>,
    },
    SetCurrentSchema {
        #[serde(rename = "schema-id")]
        schema_id: i32,
    },
    AddSpec {
        spec: UnboundPartitionSpec,
    },
    SetDefaultSpec {
        #[serde(rename = "spec-id")]
        spec_id: i32,
    },
    AddSortOrder {
        sort_order: SortOrder,
    },
    SetDefaultSortOrder {
        #[serde(rename = "sort-order-id")]
        sort_order_id: i64,
    },
    AddSnapshot {
        #[serde(deserialize_with = "crate::spec::catalog::_serde::deserialize_snapshot")]
        snapshot: Snapshot,
    },
    SetSnapshotRef {
        #[serde(rename = "ref-name")]
        ref_name: String,
        #[serde(flatten)]
        reference: SnapshotReference,
    },
    RemoveSnapshots {
        #[serde(rename = "snapshot-ids")]
        snapshot_ids: Vec<i64>,
    },
    RemoveSnapshotRef {
        #[serde(rename = "ref-name")]
        ref_name: String,
    },
    SetLocation {
        location: String,
    },
    SetProperties {
        updates: HashMap<String, String>,
    },
    RemoveProperties {
        removals: Vec<String>,
    },
    RemovePartitionSpecs {
        #[serde(rename = "spec-ids")]
        spec_ids: Vec<i32>,
    },
    #[serde(with = "_serde_set_statistics")]
    SetStatistics {
        statistics: StatisticsFile,
    },
    RemoveStatistics {
        #[serde(rename = "snapshot-id")]
        snapshot_id: i64,
    },
    SetPartitionStatistics {
        partition_statistics: PartitionStatisticsFile,
    },
    RemovePartitionStatistics {
        #[serde(rename = "snapshot-id")]
        snapshot_id: i64,
    },
    RemoveSchemas {
        #[serde(rename = "schema-ids")]
        schema_ids: Vec<i32>,
    },
}

pub(super) mod _serde {
    use serde::{Deserialize as _, Deserializer};

    use super::*;
    pub(super) fn deserialize_snapshot<'de, D>(
        deserializer: D,
    ) -> std::result::Result<Snapshot, D::Error>
    where
        D: Deserializer<'de>,
    {
        let buf = CatalogSnapshot::deserialize(deserializer)?;
        Snapshot::try_from(buf).map_err(serde::de::Error::custom)
    }
    #[derive(Debug, Deserialize, PartialEq, Eq)]
    #[serde(rename_all = "kebab-case")]
    struct CatalogSnapshot {
        snapshot_id: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        parent_snapshot_id: Option<i64>,
        #[serde(default)]
        sequence_number: i64,
        timestamp_ms: i64,
        manifest_list: String,
        summary: super::super::snapshot::Summary,
        #[serde(skip_serializing_if = "Option::is_none")]
        schema_id: Option<SchemaId>,
    }
    impl TryFrom<CatalogSnapshot> for Snapshot {
        type Error = String;
        fn try_from(snapshot: CatalogSnapshot) -> Result<Self, Self::Error> {
            let mut builder = Snapshot::builder()
                .with_snapshot_id(snapshot.snapshot_id)
                .with_sequence_number(snapshot.sequence_number)
                .with_timestamp_ms(snapshot.timestamp_ms)
                .with_manifest_list(snapshot.manifest_list)
                .with_summary(snapshot.summary);
            if let Some(parent) = snapshot.parent_snapshot_id {
                builder = builder.with_parent_snapshot_id(parent);
            }
            if let Some(schema_id) = snapshot.schema_id {
                builder = builder.with_schema_id(schema_id);
            }
            builder.build()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ViewFormatVersion {
    #[serde(rename = "1")]
    V1 = 1,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum ViewUpdate {
    AssignUuid {
        uuid: Uuid,
    },
    UpgradeFormatVersion {
        format_version: ViewFormatVersion,
    },
    AddSchema {
        schema: Box<Schema>,
        #[serde(rename = "last-column-id", skip_serializing_if = "Option::is_none")]
        last_column_id: Option<i32>,
    },
    SetLocation {
        location: String,
    },
    SetProperties {
        updates: HashMap<String, String>,
    },
    RemoveProperties {
        removals: Vec<String>,
    },
    AddViewVersion {
        #[serde(rename = "view-version")]
        view_version: ViewVersion,
    },
    SetCurrentViewVersion {
        #[serde(rename = "view-version-id")]
        view_version_id: i32,
    },
}

mod _serde_set_statistics {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::*;
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "kebab-case")]
    struct SetStatistics {
        snapshot_id: Option<i64>,
        statistics: StatisticsFile,
    }
    pub fn serialize<S>(
        value: &StatisticsFile,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        SetStatistics {
            snapshot_id: Some(value.snapshot_id),
            statistics: value.clone(),
        }
        .serialize(serializer)
    }
    pub fn deserialize<'de, D>(deserializer: D) -> std::result::Result<StatisticsFile, D::Error>
    where
        D: Deserializer<'de>,
    {
        let SetStatistics {
            snapshot_id,
            statistics,
        } = SetStatistics::deserialize(deserializer)?;
        if let Some(snapshot_id) = snapshot_id {
            if snapshot_id != statistics.snapshot_id {
                return Err(serde::de::Error::custom(format!("Snapshot id to set {snapshot_id} does not match the statistics file snapshot id {}", statistics.snapshot_id)));
            }
        }
        Ok(statistics)
    }
}
