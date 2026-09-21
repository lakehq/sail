use std::collections::HashMap;

use datafusion_common::{DataFusionError, Result};
use url::Url;

pub(crate) fn resolve_data_location_from_options_and_properties(
    write_data_path: Option<&str>,
    write_folder_storage_path: Option<&str>,
    properties: &HashMap<String, String>,
    table_url: &Url,
) -> Result<Url> {
    if let Some(data_location) = resolve_data_location_from_property_value(
        write_data_path.or(write_folder_storage_path),
        table_url,
    )? {
        return Ok(data_location);
    }
    resolve_data_location_from_properties(properties, table_url)
}

pub(crate) fn resolve_data_location_from_property_value(
    value: Option<&str>,
    table_url: &Url,
) -> Result<Option<Url>> {
    let Some(raw) = value.map(str::trim) else {
        return Ok(None);
    };
    if raw.is_empty() {
        return Ok(None);
    }

    let normalized_path = raw.replace('\\', "/");
    let mut data_url = match (
        crate::utils::parse_absolute_url(raw),
        crate::utils::file_url_from_absolute_path(&normalized_path),
    ) {
        (Some(property_url), _) => property_url,
        (None, Some(file_url)) => file_url,
        (None, None) => table_url.join(&normalized_path).map_err(|error| {
            DataFusionError::Plan(format!("Invalid Iceberg data path: {error}"))
        })?,
    };

    let schemes_are_compatible = data_url.scheme() == table_url.scheme()
        || matches!(
            (table_url.scheme(), data_url.scheme()),
            ("s3", "s3a") | ("s3a", "s3")
        );
    if !schemes_are_compatible || data_url.authority() != table_url.authority() {
        return Err(DataFusionError::Plan(format!(
            "Iceberg data path {data_url} must use the same object store as table location \
             {table_url}"
        )));
    }

    if !data_url.path().ends_with('/') {
        data_url.set_path(&format!("{}/", data_url.path()));
    }
    Ok(Some(data_url))
}

pub(crate) fn resolve_data_location_from_properties(
    properties: &HashMap<String, String>,
    table_url: &Url,
) -> Result<Url> {
    resolve_data_location_from_property_value(
        properties
            .get("write.data.path")
            .or_else(|| properties.get("write.folder-storage.path"))
            .map(String::as_str),
        table_url,
    )?
    .map_or_else(
        || {
            table_url.join("data/").map_err(|error| {
                DataFusionError::Plan(format!("Invalid default Iceberg data path: {error}"))
            })
        },
        Ok,
    )
}

pub(crate) fn parquet_file_name(file_prefix: &str) -> String {
    format!("{file_prefix}-{}.parquet", uuid::Uuid::new_v4())
}

pub(crate) fn manifest_file_path(data_url: &Url, relative_path: &str) -> String {
    data_url.join(&format!("./{relative_path}")).map_or_else(
        |_| format!("{}{relative_path}", data_url.as_str()),
        |url| url.to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table_url() -> std::result::Result<Url, url::ParseError> {
        Url::parse("file:///tmp/iceberg/table/")
    }

    #[test]
    fn data_location_prefers_write_data_path_option()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let properties =
            HashMap::from([("write.data.path".to_string(), "property-data".to_string())]);
        let table_url = table_url()?;

        let actual = resolve_data_location_from_options_and_properties(
            Some("option-data"),
            Some("folder-data"),
            &properties,
            &table_url,
        )?;

        assert_eq!(
            actual,
            Url::parse("file:///tmp/iceberg/table/option-data/")?
        );
        Ok(())
    }

    #[test]
    fn data_location_uses_folder_storage_path_option_before_table_properties()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let properties =
            HashMap::from([("write.data.path".to_string(), "property-data".to_string())]);
        let table_url = table_url()?;

        let actual = resolve_data_location_from_options_and_properties(
            None,
            Some("folder-data"),
            &properties,
            &table_url,
        )?;

        assert_eq!(
            actual,
            Url::parse("file:///tmp/iceberg/table/folder-data/")?
        );
        Ok(())
    }

    #[test]
    fn data_location_falls_back_to_table_properties_then_default()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let table_url = table_url()?;
        let properties = HashMap::from([(
            "write.folder-storage.path".to_string(),
            "property-folder".to_string(),
        )]);

        let actual =
            resolve_data_location_from_options_and_properties(None, None, &properties, &table_url)?;
        assert_eq!(
            actual,
            Url::parse("file:///tmp/iceberg/table/property-folder/")?
        );

        let actual = resolve_data_location_from_options_and_properties(
            None,
            None,
            &HashMap::new(),
            &table_url,
        )?;
        assert_eq!(actual, Url::parse("file:///tmp/iceberg/table/data/")?);
        Ok(())
    }

    #[test]
    fn data_location_accepts_absolute_paths_outside_table_root()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let table_url = table_url()?;
        let actual = resolve_data_location_from_property_value(
            Some("file:///tmp/iceberg/external-data/"),
            &table_url,
        )?;

        assert_eq!(
            actual,
            Some(Url::parse("file:///tmp/iceberg/external-data/")?)
        );
        Ok(())
    }

    #[test]
    fn data_location_accepts_s3_scheme_aliases() -> std::result::Result<(), url::ParseError> {
        let table_url = Url::parse("s3://bucket/table/")?;
        let result =
            resolve_data_location_from_property_value(Some("s3a://bucket/data/"), &table_url);
        let expected = Url::parse("s3a://bucket/data/")?;

        assert!(matches!(result, Ok(Some(actual)) if actual == expected));
        Ok(())
    }

    #[test]
    fn data_location_rejects_different_object_store() -> std::result::Result<(), url::ParseError> {
        let table_url = Url::parse("s3://table-bucket/table/")?;
        let result =
            resolve_data_location_from_property_value(Some("s3://data-bucket/data/"), &table_url);

        assert!(
            matches!(result, Err(DataFusionError::Plan(message)) if message.contains("must use the same object store"))
        );
        Ok(())
    }
}
