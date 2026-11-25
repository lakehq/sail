use std::collections::HashMap;

use datafusion_common::{Result, plan_datafusion_err, plan_err};
use sail_common::storage::{
    S3StorageConnection, S3StorageCredential, ScopedStorageCredential, StorageSecret,
};
use serde::Deserialize;
use url::Url;

#[derive(Deserialize)]
pub struct IcebergStorageCredential {
    pub prefix: String,
    pub config: HashMap<String, String>,
}

#[derive(Deserialize)]
pub(super) struct LoadCredentialsResponse {
    #[serde(rename = "storage-credentials")]
    pub credentials: Vec<IcebergStorageCredential>,
}

/// Normalize REST FileIO properties without exposing them as table properties.
pub fn iceberg_storage_credentials(
    table_location: &str,
    config: &HashMap<String, String>,
    credentials: &[IcebergStorageCredential],
) -> Result<Vec<ScopedStorageCredential>> {
    let entries = if credentials.is_empty() {
        if !config.contains_key("s3.access-key-id") && !config.contains_key("s3.secret-access-key")
        {
            return Ok(vec![]);
        }
        vec![IcebergStorageCredential {
            prefix: table_location.to_string(),
            config: config.clone(),
        }]
    } else {
        credentials
            .iter()
            .map(|entry| {
                let mut properties = config.clone();
                properties.extend(entry.config.clone());
                IcebergStorageCredential {
                    prefix: entry.prefix.clone(),
                    config: properties,
                }
            })
            .collect()
    };
    let mut parsed = Vec::with_capacity(entries.len());
    for entry in entries {
        let prefix = normalize_prefix(&entry.prefix)?;
        let properties = &entry.config;
        let secret = |key: &str| -> Result<StorageSecret> {
            properties
                .get(key)
                .filter(|s| !s.is_empty())
                .cloned()
                .map(StorageSecret::new)
                .ok_or_else(|| {
                    plan_datafusion_err!("Missing Iceberg storage credential property {key}")
                })
        };
        let expiry = |key: &str| -> Result<Option<i64>> {
            properties
                .get(key)
                .map(|value| {
                    value
                        .parse::<i64>()
                        .map_err(|_| plan_datafusion_err!("Invalid Iceberg storage expiry {key}"))
                })
                .transpose()
        };
        let expires_at_ms = [
            expiry("s3.session-token-expires-at-ms")?,
            expiry("expiration-time")?,
        ]
        .into_iter()
        .flatten()
        .min();
        let path_style_access = boolean_property(properties, "s3.path-style-access", false)?;
        let endpoint = properties.get("s3.endpoint").cloned();
        if let Some(endpoint) = &endpoint {
            validate_http_endpoint(endpoint)?;
        }
        let sse_type = properties
            .get("s3.sse.type")
            .filter(|value| value.as_str() != "none")
            .cloned();
        if sse_type
            .as_ref()
            .is_some_and(|value| !matches!(value.as_str(), "s3" | "kms" | "dsse-kms"))
        {
            return plan_err!("Unsupported Iceberg s3.sse.type");
        }
        if parsed
            .iter()
            .any(|entry: &ScopedStorageCredential| entry.prefix == prefix)
        {
            return plan_err!("Duplicate storage credential prefix");
        }
        parsed.push(ScopedStorageCredential {
            prefix,
            refresh: None,
            s3: S3StorageCredential {
                connection: S3StorageConnection {
                    region: properties
                        .get("s3.region")
                        .or_else(|| properties.get("client.region"))
                        .cloned()
                        .unwrap_or_else(|| "us-east-1".to_string()),
                    endpoint,
                    path_style_access,
                    sse_type,
                    sse_key: properties.get("s3.sse.key").cloned(),
                },
                access_key_id: secret("s3.access-key-id")?,
                secret_access_key: secret("s3.secret-access-key")?,
                session_token: properties
                    .get("s3.session-token")
                    .cloned()
                    .map(StorageSecret::new),
                expires_at_ms,
            },
        });
    }
    Ok(parsed)
}

pub fn normalize_prefix(raw: &str) -> Result<String> {
    let mut prefix =
        Url::parse(raw).map_err(|_| plan_datafusion_err!("Invalid storage credential prefix"))?;
    if !matches!(prefix.scheme(), "s3" | "s3a" | "s3n") {
        return plan_err!("Credential vending currently supports S3 storage");
    }
    if prefix.host_str().is_none()
        || !prefix.username().is_empty()
        || prefix.password().is_some()
        || prefix.query().is_some()
        || prefix.fragment().is_some()
    {
        return plan_err!("Invalid storage credential prefix");
    }
    prefix
        .set_scheme("s3")
        .map_err(|_| plan_datafusion_err!("Invalid S3 scheme"))?;
    if prefix.path() == "/" {
        prefix.set_path("");
    }
    Ok(prefix.to_string())
}

pub fn boolean_property(
    properties: &HashMap<String, String>,
    key: &str,
    default: bool,
) -> Result<bool> {
    match properties.get(key) {
        None => Ok(default),
        Some(value) if value.eq_ignore_ascii_case("true") => Ok(true),
        Some(value) if value.eq_ignore_ascii_case("false") => Ok(false),
        Some(_) => plan_err!("Invalid Iceberg boolean property {key}"),
    }
}

pub fn validate_http_endpoint(endpoint: &str) -> Result<Url> {
    let url = Url::parse(endpoint)
        .map_err(|_| plan_datafusion_err!("Invalid storage access endpoint"))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return plan_err!("Invalid storage access endpoint");
    }
    Ok(url)
}
