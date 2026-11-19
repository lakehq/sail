// SPDX-License-Identifier: Apache-2.0

use super::managed;

pub const DELTA_TABLE_TYPE_VALUE: &str = "delta";
pub const DELTA_UNITY_TABLE_ID_KEY: &str = "io.unitycatalog.tableId";
pub const DELTA_UNITY_TABLE_ID_LEGACY_KEY: &str = "table_id";
/// The create-table property with the Delta protocol that Unity Catalog requires for
/// version 0 of a staged managed table, as a Delta `protocol` action in JSON.
/// The property is consumed when version 0 is written and is never stored.
pub const DELTA_UNITY_REQUIRED_PROTOCOL_KEY: &str = "sail.unity.requiredProtocol";
/// The create-table result property with version 0 of a staged managed table, which
/// Unity Catalog needs to register the table. The value is a JSON object with the
/// `protocol` and `metaData` actions and the `inCommitTimestamp` of the commit.
/// The property is never stored.
pub const DELTA_UNITY_CREATE_COMMIT_KEY: &str = "sail.unity.createCommit";

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
