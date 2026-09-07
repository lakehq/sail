use datafusion_common::{Result, plan_datafusion_err, plan_err};
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use sail_common::storage::{
    S3StorageConnection, S3StorageCredential, ScopedStorageCredential, StorageAccessSpec,
    StorageCredentialRequest, StorageCredentialSource, StorageSecret,
};
use serde::Deserialize;

use super::CredentialClient;
use super::iceberg::{normalize_prefix, validate_http_endpoint};

#[derive(Deserialize)]
pub(super) struct TemporaryCredentials {
    aws_temp_credentials: Option<AwsTemporaryCredentials>,
    expiration_time: Option<i64>,
}

#[derive(Deserialize)]
struct AwsTemporaryCredentials {
    access_key_id: StorageSecret,
    secret_access_key: StorageSecret,
    session_token: Option<StorageSecret>,
}

pub(super) fn parse_credentials(
    prefix: &str,
    connection: S3StorageConnection,
    response: TemporaryCredentials,
) -> Result<ScopedStorageCredential> {
    let credentials = response.aws_temp_credentials.ok_or_else(|| {
        plan_datafusion_err!("Unity Catalog did not return supported S3 credentials")
    })?;
    if credentials.access_key_id.expose().is_empty()
        || credentials.secret_access_key.expose().is_empty()
        || credentials
            .session_token
            .as_ref()
            .is_some_and(|token| token.expose().is_empty())
    {
        return plan_err!("Unity Catalog returned incomplete S3 credentials");
    }
    Ok(ScopedStorageCredential {
        prefix: normalize_prefix(prefix)?,
        s3: S3StorageCredential {
            connection,
            access_key_id: credentials.access_key_id,
            secret_access_key: credentials.secret_access_key,
            session_token: credentials.session_token,
            expires_at_ms: response.expiration_time,
        },
        refresh: None,
    })
}

/// Unity vends identity; connection settings come from the driver's S3 configuration.
fn s3_connection() -> Result<S3StorageConnection> {
    let builder = AmazonS3Builder::from_env();
    let get = |key| builder.get_config_value(&key);
    let endpoint = get(AmazonS3ConfigKey::S3Endpoint).or_else(|| get(AmazonS3ConfigKey::Endpoint));
    if let Some(endpoint) = &endpoint {
        validate_http_endpoint(endpoint)?;
    }
    let virtual_hosted = get(AmazonS3ConfigKey::VirtualHostedStyleRequest)
        .map(|value| value.parse::<bool>())
        .transpose()
        .map_err(|_| plan_datafusion_err!("Invalid S3 addressing configuration"))?
        .unwrap_or(false);
    let sse_type = get("aws_server_side_encryption".parse::<AmazonS3ConfigKey>()?)
        .map(|value| match value.as_str() {
            "AES256" => Ok("s3".to_string()),
            "aws:kms" => Ok("kms".to_string()),
            "aws:kms:dsse" => Ok("dsse-kms".to_string()),
            _ => plan_err!("Unsupported delegated S3 encryption configuration"),
        })
        .transpose()?;
    Ok(S3StorageConnection {
        region: get(AmazonS3ConfigKey::Region).unwrap_or_else(|| "us-east-1".to_string()),
        endpoint,
        path_style_access: !virtual_hosted,
        sse_type,
        sse_key: get("aws_sse_kms_key_id".parse::<AmazonS3ConfigKey>()?),
    })
}

pub async fn storage_access(
    location: &str,
    source: StorageCredentialSource,
) -> Result<StorageAccessSpec> {
    if !matches!(
        source.request,
        StorageCredentialRequest::UnityTable { .. } | StorageCredentialRequest::UnityPath { .. }
    ) {
        return plan_err!("Expected a Unity Catalog credential request");
    }
    let prefix = normalize_prefix(&format!("{}/", location.trim_end_matches('/')))?;
    let connection = s3_connection()?;
    let response = CredentialClient::new(Some(&source))?
        .fetch(&source)
        .await
        .map_err(|error| plan_datafusion_err!("{error}"))?
        .json::<TemporaryCredentials>()
        .await
        .map_err(|_| plan_datafusion_err!("Invalid Unity Catalog storage credential response"))?;
    let mut credential = parse_credentials(&prefix, connection, response)?;
    credential.refresh = Some(source);
    Ok(StorageAccessSpec {
        credentials: vec![credential],
    })
}
