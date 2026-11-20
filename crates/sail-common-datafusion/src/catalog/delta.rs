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
