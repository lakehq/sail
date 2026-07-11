use std::collections::HashMap;

use datafusion_common::{DataFusionError, Result};
use url::Url;

const DEFAULT_DATA_DIR: &str = "data";

pub(crate) fn resolve_data_dir_from_options_and_properties(
    write_data_path: Option<&str>,
    write_folder_storage_path: Option<&str>,
    properties: &HashMap<String, String>,
    table_url: &Url,
) -> Result<String> {
    if let Some(data_dir) = resolve_data_dir_from_property_value(
        write_data_path.or(write_folder_storage_path),
        table_url,
    )? {
        return Ok(data_dir);
    }
    resolve_data_dir_from_properties(properties, table_url)
}

pub(crate) fn resolve_data_dir_from_property_value(
    value: Option<&str>,
    table_url: &Url,
) -> Result<Option<String>> {
    let Some(raw) = value.map(str::trim) else {
        return Ok(None);
    };
    if raw.is_empty() {
        return Ok(None);
    }

    let base_path = crate::utils::url_to_object_path(table_url)?;
    // TODO: Support write paths outside the table root with operation-scoped object stores.
    if let Some(prop_url) = crate::utils::parse_absolute_url(raw) {
        if prop_url.scheme() == table_url.scheme()
            && prop_url.host_str() == table_url.host_str()
            && prop_url.port_or_known_default() == table_url.port_or_known_default()
        {
            let prop_path = crate::utils::url_to_object_path(&prop_url)?;
            let prop_str = prop_path.as_ref();
            let base_str = base_path.as_ref();
            if let Ok(relative) = path_relative_to_table(prop_str, base_str) {
                return Ok(relative);
            }
        }
    } else {
        let prop_path = raw.replace('\\', "/");
        if prop_path.starts_with('/') {
            let prop_no_leading = prop_path.trim_start_matches('/');
            if let Ok(relative) = path_relative_to_table(prop_no_leading, base_path.as_ref()) {
                return Ok(relative);
            }
        } else {
            let rel = prop_path.trim_matches('/');
            if !rel.is_empty() && !rel.split('/').any(|component| component == "..") {
                return Ok(Some(rel.to_string()));
            }
        }
    }

    Err(DataFusionError::Plan(format!(
        "external Iceberg write paths are not supported: `{raw}` is outside table location `{table_url}`"
    )))
}

fn path_relative_to_table(path: &str, table_path: &str) -> std::result::Result<Option<String>, ()> {
    let path = path.trim_matches('/');
    let table_path = table_path.trim_matches('/');
    if path == table_path {
        return Ok(None);
    }
    if table_path.is_empty() {
        return Ok((!path.is_empty()).then(|| path.to_string()));
    }
    path.strip_prefix(table_path)
        .and_then(|suffix| suffix.strip_prefix('/'))
        .map(|relative| {
            let relative = relative.trim_matches('/');
            (!relative.is_empty()).then(|| relative.to_string())
        })
        .ok_or(())
}

pub(crate) fn resolve_data_dir_from_properties(
    properties: &HashMap<String, String>,
    table_url: &Url,
) -> Result<String> {
    Ok(resolve_data_dir_from_property_value(
        properties
            .get("write.data.path")
            .or_else(|| properties.get("write.folder-storage.path"))
            .map(String::as_str),
        table_url,
    )?
    .unwrap_or_else(|| DEFAULT_DATA_DIR.to_string()))
}

pub(crate) fn parquet_file_path(data_dir: &str, file_prefix: &str) -> String {
    format!(
        "{}/{}-{}.parquet",
        data_dir.trim_matches('/'),
        file_prefix,
        uuid::Uuid::new_v4()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table_url() -> std::result::Result<Url, url::ParseError> {
        Url::parse("file:///tmp/iceberg/table/")
    }

    #[test]
    fn data_dir_prefers_write_data_path_option()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let properties =
            HashMap::from([("write.data.path".to_string(), "property-data".to_string())]);
        let table_url = table_url()?;

        let actual = resolve_data_dir_from_options_and_properties(
            Some("option-data"),
            Some("folder-data"),
            &properties,
            &table_url,
        )?;

        assert_eq!(actual, "option-data");
        Ok(())
    }

    #[test]
    fn data_dir_uses_folder_storage_path_option_before_table_properties()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let properties =
            HashMap::from([("write.data.path".to_string(), "property-data".to_string())]);
        let table_url = table_url()?;

        let actual = resolve_data_dir_from_options_and_properties(
            None,
            Some("folder-data"),
            &properties,
            &table_url,
        )?;

        assert_eq!(actual, "folder-data");
        Ok(())
    }

    #[test]
    fn data_dir_falls_back_to_table_properties_then_default()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let table_url = table_url()?;
        let properties = HashMap::from([(
            "write.folder-storage.path".to_string(),
            "property-folder".to_string(),
        )]);

        let actual =
            resolve_data_dir_from_options_and_properties(None, None, &properties, &table_url)?;
        assert_eq!(actual, "property-folder");

        let actual =
            resolve_data_dir_from_options_and_properties(None, None, &HashMap::new(), &table_url)?;
        assert_eq!(actual, DEFAULT_DATA_DIR);
        Ok(())
    }

    #[test]
    fn data_dir_normalizes_absolute_paths_under_table_root()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let table_url = table_url()?;
        let actual = resolve_data_dir_from_property_value(
            Some("file:///tmp/iceberg/table/custom/data/"),
            &table_url,
        )?;

        assert_eq!(actual.as_deref(), Some("custom/data"));
        Ok(())
    }

    #[test]
    fn data_dir_rejects_absolute_paths_outside_table_root()
    -> std::result::Result<(), url::ParseError> {
        let table_url = table_url()?;
        let result = resolve_data_dir_from_property_value(
            Some("file:///tmp/iceberg/table-other/data/"),
            &table_url,
        );

        assert!(matches!(
            result,
            Err(DataFusionError::Plan(message))
                if message.contains("external Iceberg write paths are not supported")
        ));
        Ok(())
    }

    #[test]
    fn parquet_file_path_uses_data_dir_and_prefix() {
        let path = parquet_file_path("custom/data/", "equality-delete");

        assert!(path.starts_with("custom/data/equality-delete-"));
        assert!(path.ends_with(".parquet"));
    }
}
