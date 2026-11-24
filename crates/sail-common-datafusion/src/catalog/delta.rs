// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::managed;

pub const DELTA_TABLE_TYPE_VALUE: &str = "delta";
pub const DELTA_UNITY_TABLE_ID_KEY: &str = "io.unitycatalog.tableId";
pub const DELTA_UNITY_TABLE_ID_LEGACY_KEY: &str = "table_id";

/// A Delta protocol, with the fields of the `protocol` action of the Delta log.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeltaProtocolInfo {
    pub min_reader_version: i32,
    pub min_writer_version: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reader_features: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub writer_features: Option<Vec<String>>,
}

/// The Delta table metadata that a catalog keeps for a table, with the fields of the
/// `metaData` action of the Delta log.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeltaMetadataInfo {
    pub schema_string: String,
    pub partition_columns: Vec<String>,
    pub configuration: BTreeMap<String, String>,
    pub description: Option<String>,
}

/// The protocol, the metadata, and the in-commit timestamp of the first version of a
/// Delta table, which a catalog registers the table with.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeltaCreatedVersion {
    pub protocol: DeltaProtocolInfo,
    pub metadata: DeltaMetadataInfo,
    pub in_commit_timestamp: i64,
    /// The row ID high-water mark of the row tracking domain, for a first version that
    /// assigns row IDs to the data that it adds.
    #[serde(default)]
    pub row_id_high_water_mark: Option<i64>,
}

/// A domain metadata action of the Delta log.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeltaDomainMetadataInfo {
    pub domain: String,
    #[serde(default)]
    pub configuration: String,
    #[serde(default)]
    pub removed: bool,
}

/// A staged commit of a catalog-managed Delta table that a catalog ratifies.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct DeltaCatalogCommit {
    pub table_id: String,
    pub table_uri: String,
    pub commit_info: DeltaCatalogCommitInfo,
    /// The latest version of the table that is published.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_backfilled_version: Option<i64>,
    /// The actions of the commit that the catalog derives the table metadata from.
    #[serde(default)]
    pub delta_log: DeltaCommitActions,
}

/// The staged commit file of a Delta commit.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct DeltaCatalogCommitInfo {
    pub version: i64,
    /// The in-commit timestamp of the commit.
    pub timestamp: i64,
    pub file_name: String,
    pub file_size: i64,
    pub file_modification_timestamp: i64,
}

/// The metadata action, the protocol action, and the domain metadata actions of a Delta
/// commit, in the form of the Delta log. The metadata action and the protocol action come
/// with the metadata and the protocol of the snapshot that the commit is based on, so that
/// the catalog can tell what the commit changes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeltaCommitActions {
    #[serde(rename = "metaData", default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<DeltaMetadataInfo>,
    /// The metadata of the base snapshot, for a commit with a metadata action.
    #[serde(
        rename = "baseMetaData",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub base_metadata: Option<DeltaMetadataInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol: Option<DeltaProtocolInfo>,
    /// The protocol of the base snapshot, for a commit with a protocol action.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_protocol: Option<DeltaProtocolInfo>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub domain_metadata: Vec<DeltaDomainMetadataInfo>,
}

/// A commit of a catalog-managed Delta table that a catalog ratified.
#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct DeltaRatifiedCommit {
    pub version: i64,
    pub timestamp: i64,
    pub file_name: String,
    pub file_size: i64,
    pub file_modification_timestamp: i64,
}

/// The latest version of a catalog-managed Delta table that a catalog ratified, and the
/// ratified commits that the catalog returns, which are the ones that are not reported to
/// it as published.
#[derive(Debug, Clone, Default, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct DeltaRatifiedCommits {
    pub latest_table_version: i64,
    pub commits: Vec<DeltaRatifiedCommit>,
}

/// Whether a Delta table is catalog-managed, as the catalog told when it resolved the table.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub enum DeltaCatalogManaged {
    /// The catalog ratifies the commits of the table.
    Yes,
    /// The table is committed to the file system.
    No,
    /// The catalog did not tell, so the protocol of the published Delta log decides.
    #[default]
    Undetermined,
}

/// What a catalog told about a Delta table whose commits it may ratify, when it resolved
/// the table. A statement reads and commits the table from this state, so that the catalog
/// is asked once.
#[derive(Debug, Clone, Default, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct DeltaCatalogState {
    pub catalog_managed: DeltaCatalogManaged,
    /// All the ratified commits of the table when it was resolved, for a catalog that
    /// returned them with the table. They are requested from the catalog otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commits: Option<DeltaRatifiedCommits>,
    /// Whether the catalog registers a table before version 0 of the table is written, in
    /// which case the first reader or writer of a table without a Delta log writes it.
    #[serde(default)]
    pub registered_before_log: bool,
}

/// Properties that bind a Delta log to a catalog or commit coordinator.
pub fn is_delta_coordinated_property(key: &str) -> bool {
    key.eq_ignore_ascii_case(DELTA_UNITY_TABLE_ID_KEY)
        || key.eq_ignore_ascii_case(DELTA_UNITY_TABLE_ID_LEGACY_KEY)
        || key
            .to_ascii_lowercase()
            .starts_with("delta.coordinatedcommits.")
        || key.eq_ignore_ascii_case("delta.feature.catalogManaged")
        || key.eq_ignore_ascii_case("delta.feature.catalogOwned")
}

pub fn is_delta_table_properties<'a, I>(properties: I) -> bool
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    managed::is_lake_source_properties(properties, DELTA_TABLE_TYPE_VALUE)
}

pub fn unity_table_id_value<'a, I>(properties: I) -> Option<&'a str>
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
    I::IntoIter: Clone,
{
    let properties = properties.into_iter();
    managed::property_value(properties.clone(), DELTA_UNITY_TABLE_ID_KEY)
        .or_else(|| managed::property_value(properties, DELTA_UNITY_TABLE_ID_LEGACY_KEY))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coordinated_properties_are_not_arbitrary_substrings() {
        for key in [
            DELTA_UNITY_TABLE_ID_KEY,
            DELTA_UNITY_TABLE_ID_LEGACY_KEY,
            "delta.coordinatedCommits.commitCoordinator-preview",
            "delta.coordinatedCommits.tableConf-preview",
            "delta.feature.catalogManaged",
            "delta.feature.catalogOwned",
        ] {
            assert!(is_delta_coordinated_property(key));
            assert!(is_delta_coordinated_property(&key.to_ascii_uppercase()));
        }
        for key in [
            "community.owner",
            "owner.unity",
            "catalogManaged.notes",
            "delta.coordinatedCommitsNotes",
        ] {
            assert!(!is_delta_coordinated_property(key));
        }
    }
}
