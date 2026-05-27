use std::collections::HashMap;

use datafusion::common::{plan_err, DataFusionError, Result};

use crate::spec::{FormatVersion, TableMetadata};

pub(crate) fn metadata_properties_from_table_properties(
    table_properties: &[(String, String)],
) -> Result<(FormatVersion, HashMap<String, String>)> {
    let mut properties = HashMap::new();
    let mut format_version = FormatVersion::default();

    for (key, value) in table_properties {
        if key == "format-version" {
            format_version = parse_format_version(value)?;
        } else if !is_reserved_iceberg_table_property(key) {
            properties.insert(key.clone(), value.clone());
        }
    }

    Ok((format_version, properties))
}

pub(crate) fn apply_table_property_changes(
    table_meta: &mut TableMetadata,
    changes: &[(String, Option<String>)],
    if_exists: bool,
) -> Result<()> {
    let mut properties = table_meta.properties.clone();
    let mut format_version = table_meta.format_version;

    for (key, value) in changes {
        match value {
            Some(value) => {
                if key == "format-version" {
                    format_version = upgrade_format_version(format_version, value)?;
                } else if !is_reserved_iceberg_table_property(key) {
                    properties.insert(key.clone(), value.clone());
                }
            }
            None => {
                if !if_exists && !properties.contains_key(key) {
                    return plan_err!(
                        "cannot remove property '{key}' because it is not set on the table"
                    );
                }
                if !is_reserved_iceberg_table_property(key) {
                    properties.remove(key);
                }
            }
        }
    }

    table_meta.properties = properties;
    table_meta.format_version = format_version;
    Ok(())
}

pub(crate) fn is_reserved_iceberg_table_property(key: &str) -> bool {
    matches!(
        key,
        "format-version"
            | "uuid"
            | "snapshot-count"
            | "current-snapshot-summary"
            | "current-snapshot-id"
            | "current-snapshot-timestamp-ms"
            | "current-schema"
            | "default-partition-spec"
            | "default-sort-order"
    )
}

fn upgrade_format_version(current: FormatVersion, requested: &str) -> Result<FormatVersion> {
    let requested = parse_format_version(requested)?;
    let current_i32 = current as i32;
    let requested_i32 = requested as i32;
    if requested_i32 < current_i32 {
        return plan_err!(
            "cannot downgrade Iceberg table format version from v{current_i32} to v{requested_i32}"
        );
    }
    Ok(requested)
}

fn parse_format_version(requested: &str) -> Result<FormatVersion> {
    let version = requested.parse::<i32>().map_err(|_| {
        DataFusionError::Plan(format!("invalid Iceberg format-version value: {requested}"))
    })?;
    match version {
        1 => Ok(FormatVersion::V1),
        2 => Ok(FormatVersion::V2),
        3 => Ok(FormatVersion::V3),
        _ => plan_err!("cannot use unsupported Iceberg table format version v{version}"),
    }
}
