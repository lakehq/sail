use std::collections::HashMap;

use url::Url;

const DEFAULT_DATA_DIR: &str = "data";

pub(crate) fn resolve_data_dir_from_options_and_properties(
    write_data_path: Option<&str>,
    write_folder_storage_path: Option<&str>,
    properties: &HashMap<String, String>,
    table_url: &Url,
) -> String {
    resolve_data_dir_from_property_value(write_data_path.or(write_folder_storage_path), table_url)
        .unwrap_or_else(|| resolve_data_dir_from_properties(properties, table_url))
}

pub(crate) fn resolve_data_dir_from_property_value(
    value: Option<&str>,
    table_url: &Url,
) -> Option<String> {
    let raw = value?.trim();
    if raw.is_empty() {
        return None;
    }

    let base_path = crate::utils::url_to_object_path(table_url).ok();
    if let Some(prop_url) = crate::utils::parse_absolute_url(raw) {
        if prop_url.scheme() == table_url.scheme() && prop_url.host_str() == table_url.host_str() {
            if let (Ok(prop_path), Some(base_path)) = (
                crate::utils::url_to_object_path(&prop_url),
                base_path.as_ref(),
            ) {
                let prop_str = prop_path.as_ref();
                let base_str = base_path.as_ref();
                if let Some(stripped) = prop_str.strip_prefix(base_str) {
                    let rel = stripped.trim_start_matches('/').trim_matches('/');
                    if !rel.is_empty() {
                        return Some(rel.to_string());
                    }
                }
            }
        }
    } else {
        let prop_path = raw.replace('\\', "/");
        if prop_path.starts_with('/') {
            if let Some(base_path) = base_path.as_ref() {
                let base_str = base_path.as_ref();
                let prop_no_leading = prop_path.trim_start_matches('/');
                if let Some(stripped) = prop_no_leading.strip_prefix(base_str) {
                    let rel = stripped.trim_start_matches('/').trim_matches('/');
                    if !rel.is_empty() {
                        return Some(rel.to_string());
                    }
                }
            }
        } else {
            let rel = prop_path.trim_matches('/');
            if !rel.is_empty() {
                return Some(rel.to_string());
            }
        }
    }

    None
}

pub(crate) fn resolve_data_dir_from_properties(
    properties: &HashMap<String, String>,
    table_url: &Url,
) -> String {
    resolve_data_dir_from_property_value(
        properties
            .get("write.data.path")
            .or_else(|| properties.get("write.folder-storage.path"))
            .map(String::as_str),
        table_url,
    )
    .unwrap_or_else(|| DEFAULT_DATA_DIR.to_string())
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

    fn table_url() -> Url {
        Url::parse("file:///tmp/iceberg/table/").unwrap()
    }

    #[test]
    fn data_dir_prefers_write_data_path_option() {
        let properties =
            HashMap::from([("write.data.path".to_string(), "property-data".to_string())]);

        let actual = resolve_data_dir_from_options_and_properties(
            Some("option-data"),
            Some("folder-data"),
            &properties,
            &table_url(),
        );

        assert_eq!(actual, "option-data");
    }

    #[test]
    fn data_dir_uses_folder_storage_path_option_before_table_properties() {
        let properties =
            HashMap::from([("write.data.path".to_string(), "property-data".to_string())]);

        let actual = resolve_data_dir_from_options_and_properties(
            None,
            Some("folder-data"),
            &properties,
            &table_url(),
        );

        assert_eq!(actual, "folder-data");
    }

    #[test]
    fn data_dir_falls_back_to_table_properties_then_default() {
        let table_url = table_url();
        let properties = HashMap::from([(
            "write.folder-storage.path".to_string(),
            "property-folder".to_string(),
        )]);

        let actual =
            resolve_data_dir_from_options_and_properties(None, None, &properties, &table_url);
        assert_eq!(actual, "property-folder");

        let actual =
            resolve_data_dir_from_options_and_properties(None, None, &HashMap::new(), &table_url);
        assert_eq!(actual, DEFAULT_DATA_DIR);
    }

    #[test]
    fn data_dir_normalizes_absolute_paths_under_table_root() {
        let table_url = table_url();
        let actual = resolve_data_dir_from_property_value(
            Some("file:///tmp/iceberg/table/custom/data/"),
            &table_url,
        );

        assert_eq!(actual.as_deref(), Some("custom/data"));
    }

    #[test]
    fn parquet_file_path_uses_data_dir_and_prefix() {
        let path = parquet_file_path("custom/data/", "equality-delete");

        assert!(path.starts_with("custom/data/equality-delete-"));
        assert!(path.ends_with(".parquet"));
    }
}
