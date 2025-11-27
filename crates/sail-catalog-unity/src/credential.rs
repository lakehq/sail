// https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
//
// Copyright (2020) QP Hou and a number of other contributors.
// Portions Copyright (2025) LakeSail, Inc.
// Modified in 2025 by LakeSail, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// [CREDIT]: https://github.com/delta-io/delta-rs/blob/d7dea29162451fd00b9579e3d7fb546d95fb5e4a/crates/catalog-unity/src/credential.rs

use std::process::Command;
use std::time::{Duration, Instant};

use reqwest::header::{HeaderValue, ACCEPT};
use reqwest::{Client, Method, Response};
use sail_catalog::error::{CatalogError, CatalogResult};
use serde::de::DeserializeOwned;
use serde::Deserialize;

use crate::token::{TemporaryToken, TokenCache};

// https://learn.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/authentication

const DATABRICKS_RESOURCE_SCOPE: &str = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d";
const DATABRICKS_WORKSPACE_SCOPE: &str = "all-apis";
const CONTENT_TYPE_JSON: &str = "application/json";
const MSI_SECRET_ENV_KEY: &str = "IDENTITY_HEADER";
const MSI_API_VERSION: &str = "2019-08-01";

/// A list of known Azure authority hosts
pub mod authority_hosts {
    #[allow(unused)]
    /// China-based Azure Authority Host
    pub const AZURE_CHINA: &str = "https://login.chinacloudapi.cn";
    #[allow(unused)]
    /// Germany-based Azure Authority Host
    pub const AZURE_GERMANY: &str = "https://login.microsoftonline.de";
    #[allow(unused)]
    /// US Government Azure Authority Host
    pub const AZURE_GOVERNMENT: &str = "https://login.microsoftonline.us";
    /// Public Cloud Azure Authority Host
    pub const AZURE_PUBLIC_CLOUD: &str = "https://login.microsoftonline.com";
}

/// Trait for providing authorization tokens for catalog requests
#[async_trait::async_trait]
pub trait TokenCredential: std::fmt::Debug + Send + Sync + 'static {
    /// get the token
    async fn fetch_token(&self, client: &Client) -> CatalogResult<TemporaryToken<String>>;
}

/// Provides credentials for use when signing requests
#[derive(Debug)]
pub enum CredentialProvider {
    /// static bearer token
    BearerToken(String),

    /// a credential to fetch expiring auth tokens
    TokenCredential(TokenCache<String>, Box<dyn TokenCredential>),
}

#[derive(Deserialize, Debug)]
struct TokenResponse {
    access_token: String,
    expires_in: u64,
}

/// The same thing as the azure oauth provider, but uses the databricks api to
/// get tokens directly from the workspace.
#[derive(Debug, Clone)]
pub struct WorkspaceOAuthProvider {
    token_url: String,
    client_id: String,
    client_secret: String,
}

async fn non200_or_json<T: DeserializeOwned>(response: Response) -> CatalogResult<T> {
    if !response.status().is_success() {
        response
            .json()
            .await
            .map_err(|e| CatalogError::InvalidArgument(format!("Invalid credentials: {e}")))
    } else {
        Ok(response
            .json()
            .await
            .map_err(|e| CatalogError::External(format!("Failed to parse token response: {e}")))?)
    }
}

impl WorkspaceOAuthProvider {
    pub fn new(
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
        workspace_host: impl Into<String>,
    ) -> Self {
        Self {
            token_url: format!("{}/oidc/v1/token", workspace_host.into()),
            client_id: client_id.into(),
            client_secret: client_secret.into(),
        }
    }
}

#[async_trait::async_trait]
impl TokenCredential for WorkspaceOAuthProvider {
    async fn fetch_token(&self, client: &Client) -> CatalogResult<TemporaryToken<String>> {
        let response = client
            .request(Method::POST, &self.token_url)
            .header(ACCEPT, HeaderValue::from_static(CONTENT_TYPE_JSON))
            .form(&[
                ("client_id", self.client_id.as_str()),
                ("client_secret", self.client_secret.as_str()),
                ("scope", DATABRICKS_WORKSPACE_SCOPE),
                ("grant_type", "client_credentials"),
            ])
            .send()
            .await
            .map_err(|e| CatalogError::External(format!("Failed to fetch token: {e}")))?;

        let response: TokenResponse = non200_or_json(response).await?;

        Ok(TemporaryToken {
            token: response.access_token,
            expiry: Some(Instant::now() + Duration::from_secs(response.expires_in)),
        })
    }
}

/// Encapsulates the logic to perform an OAuth token challenge
#[derive(Debug, Clone)]
pub struct ClientSecretOAuthProvider {
    token_url: String,
    client_id: String,
    client_secret: String,
}

impl ClientSecretOAuthProvider {
    /// Create a new [`ClientSecretOAuthProvider`] for an azure backed store
    pub fn new(
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
        authority_id: impl AsRef<str>,
        authority_host: Option<impl Into<String>>,
    ) -> Self {
        let authority_host = authority_host
            .map(|h| h.into())
            .unwrap_or_else(|| authority_hosts::AZURE_PUBLIC_CLOUD.to_owned());

        Self {
            token_url: format!(
                "{authority_host}/{}/oauth2/v2.0/token",
                authority_id.as_ref()
            ),
            client_id: client_id.into(),
            client_secret: client_secret.into(),
        }
    }
}

#[async_trait::async_trait]
impl TokenCredential for ClientSecretOAuthProvider {
    /// Fetch a token
    async fn fetch_token(&self, client: &Client) -> CatalogResult<TemporaryToken<String>> {
        let response = client
            .request(Method::POST, &self.token_url)
            .header(ACCEPT, HeaderValue::from_static(CONTENT_TYPE_JSON))
            .form(&[
                ("client_id", self.client_id.as_str()),
                ("client_secret", self.client_secret.as_str()),
                ("scope", &format!("{DATABRICKS_RESOURCE_SCOPE}/.default")),
                ("grant_type", "client_credentials"),
            ])
            .send()
            .await
            .map_err(|e| CatalogError::External(format!("Failed to fetch token: {e}")))?;

        let response: TokenResponse = non200_or_json(response).await?;

        Ok(TemporaryToken {
            token: response.access_token,
            expiry: Some(Instant::now() + Duration::from_secs(response.expires_in)),
        })
    }
}

mod az_cli_date_format {
    use chrono::{DateTime, TimeZone};
    use serde::{self, Deserialize, Deserializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<chrono::Local>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        // expiresOn from azure cli uses the local timezone
        let date = chrono::NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S.%6f")
            .map_err(serde::de::Error::custom)?;
        chrono::Local
            .from_local_datetime(&date)
            .single()
            .ok_or(serde::de::Error::custom(
                "azure cli returned ambiguous expiry date",
            ))
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AzureCliTokenResponse {
    pub access_token: String,
    #[serde(with = "az_cli_date_format")]
    pub expires_on: chrono::DateTime<chrono::Local>,
    pub token_type: String,
}

/// Credential for acquiring access tokens via the Azure CLI
#[derive(Default, Debug)]
pub struct AzureCliCredential {
    _private: (),
}

impl AzureCliCredential {
    /// Create a new instance of [`AzureCliCredential`]
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl TokenCredential for AzureCliCredential {
    /// Fetch a token
    async fn fetch_token(&self, _client: &Client) -> CatalogResult<TemporaryToken<String>> {
        // on window az is a cmd, and it should be called like this
        // see https://doc.rust-lang.org/nightly/std/process/struct.Command.html
        let program = if cfg!(target_os = "windows") {
            "cmd"
        } else {
            "az"
        };
        let mut args = Vec::new();
        if cfg!(target_os = "windows") {
            args.push("/C");
            args.push("az");
        }
        args.push("account");
        args.push("get-access-token");
        args.push("--output");
        args.push("json");
        args.push("--resource");
        args.push(DATABRICKS_RESOURCE_SCOPE);

        match Command::new(program).args(args).output() {
            Ok(az_output) if az_output.status.success() => {
                let output = std::str::from_utf8(&az_output.stdout).map_err(|e| {
                    CatalogError::External(format!(
                        "Azure CLI az response is not a valid utf-8 string: {e}"
                    ))
                })?;

                let token_response = serde_json::from_str::<AzureCliTokenResponse>(output)
                    .map_err(|err| {
                        CatalogError::External(format!(
                            "Azure CLI failed serializing token response: {err:?}"
                        ))
                    })?;
                if !token_response.token_type.eq_ignore_ascii_case("bearer") {
                    return Err(CatalogError::External(format!(
                        "Got unexpected token type from Azure CLI: {0}",
                        token_response.token_type
                    )));
                }
                let duration =
                    token_response.expires_on.naive_local() - chrono::Local::now().naive_local();
                Ok(TemporaryToken {
                    token: token_response.access_token,
                    expiry: Some(
                        Instant::now()
                            + duration.to_std().map_err(|e| {
                                CatalogError::External(format!(
                                    "Azure CLI az returned invalid lifetime: {e}"
                                ))
                            })?,
                    ),
                })
            }
            Ok(az_output) => {
                let message = String::from_utf8_lossy(&az_output.stderr);
                Err(CatalogError::External(format!("Azure CLI: {message}")))
            }
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => Err(CatalogError::External(format!(
                    "Azure CLI not installed: {e}"
                ))),
                error_kind => Err(CatalogError::External(format!(
                    "Azure CLI io error: {error_kind}"
                ))),
            },
        }
    }
}

/// Credential for using workload identity federation
///
/// <https://learn.microsoft.com/en-us/azure/active-directory/develop/workload-identity-federation>
#[derive(Debug)]
pub struct WorkloadIdentityOAuthProvider {
    token_url: String,
    client_id: String,
    federated_token_file: String,
}

impl WorkloadIdentityOAuthProvider {
    /// Create a new [`WorkloadIdentityOAuthProvider`]
    pub fn new(
        client_id: impl Into<String>,
        federated_token_file: impl Into<String>,
        tenant_id: impl AsRef<str>,
        authority_host: Option<String>,
    ) -> Self {
        let authority_host =
            authority_host.unwrap_or_else(|| authority_hosts::AZURE_PUBLIC_CLOUD.to_owned());

        Self {
            token_url: format!("{authority_host}/{}/oauth2/v2.0/token", tenant_id.as_ref()),
            client_id: client_id.into(),
            federated_token_file: federated_token_file.into(),
        }
    }
}

#[async_trait::async_trait]
impl TokenCredential for WorkloadIdentityOAuthProvider {
    /// Fetch a token
    async fn fetch_token(&self, client: &Client) -> CatalogResult<TemporaryToken<String>> {
        let token_str = std::fs::read_to_string(&self.federated_token_file).map_err(|e| {
            CatalogError::InvalidArgument(format!(
                "failed reading federated token file {}: {e}",
                self.federated_token_file
            ))
        })?;

        // https://learn.microsoft.com/en-us/azure/active-directory/develop/v2-oauth2-client-creds-grant-flow#third-case-access-token-request-with-a-federated-credential
        let response = client
            .request(Method::POST, &self.token_url)
            .header(ACCEPT, HeaderValue::from_static(CONTENT_TYPE_JSON))
            .form(&[
                ("client_id", self.client_id.as_str()),
                (
                    "client_assertion_type",
                    "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
                ),
                ("client_assertion", token_str.as_str()),
                ("scope", &format!("{DATABRICKS_RESOURCE_SCOPE}/.default")),
                ("grant_type", "client_credentials"),
            ])
            .send()
            .await
            .map_err(|e| CatalogError::External(format!("Failed to fetch token: {e}")))?;

        let response: TokenResponse = non200_or_json(response).await?;

        Ok(TemporaryToken {
            token: response.access_token,
            expiry: Some(Instant::now() + Duration::from_secs(response.expires_in)),
        })
    }
}

fn expires_in_string<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    let v = String::deserialize(deserializer)?;
    v.parse::<u64>().map_err(serde::de::Error::custom)
}

// NOTE: expires_on is a String version of unix epoch time, not an integer.
// <https://learn.microsoft.com/en-gb/azure/active-directory/managed-identities-azure-resources/how-to-use-vm-token#get-a-token-using-http>
#[derive(Debug, Clone, Deserialize)]
struct MsiTokenResponse {
    pub access_token: String,
    #[serde(deserialize_with = "expires_in_string")]
    pub expires_in: u64,
}

/// Attempts authentication using a managed identity that has been assigned to the deployment environment.
///
/// This authentication type works in Azure VMs, App Service and Azure Functions applications, as well as the Azure Cloud Shell
/// <https://learn.microsoft.com/en-gb/azure/active-directory/managed-identities-azure-resources/how-to-use-vm-token#get-a-token-using-http>
#[derive(Debug)]
pub struct ImdsManagedIdentityOAuthProvider {
    msi_endpoint: String,
    client_id: Option<String>,
    object_id: Option<String>,
    msi_res_id: Option<String>,
}

impl ImdsManagedIdentityOAuthProvider {
    /// Create a new [`ImdsManagedIdentityOAuthProvider`] for an azure backed store
    pub fn new(
        client_id: Option<String>,
        object_id: Option<String>,
        msi_res_id: Option<String>,
        msi_endpoint: Option<String>,
    ) -> Self {
        let msi_endpoint = msi_endpoint
            .unwrap_or_else(|| "http://169.254.169.254/metadata/identity/oauth2/token".to_owned());

        Self {
            msi_endpoint,
            client_id,
            object_id,
            msi_res_id,
        }
    }
}

#[async_trait::async_trait]
impl TokenCredential for ImdsManagedIdentityOAuthProvider {
    /// Fetch a token
    async fn fetch_token(&self, client: &Client) -> Result<TemporaryToken<String>, CatalogError> {
        let resource_scope = format!("{DATABRICKS_RESOURCE_SCOPE}/.default");
        let mut query_items = vec![
            ("api-version", MSI_API_VERSION),
            ("resource", &resource_scope),
        ];

        let mut identity = None;
        if let Some(client_id) = &self.client_id {
            identity = Some(("client_id", client_id));
        }
        if let Some(object_id) = &self.object_id {
            identity = Some(("object_id", object_id));
        }
        if let Some(msi_res_id) = &self.msi_res_id {
            identity = Some(("msi_res_id", msi_res_id));
        }
        if let Some((key, value)) = identity {
            query_items.push((key, value));
        }

        let mut builder = client
            .request(Method::GET, &self.msi_endpoint)
            .header("metadata", "true")
            .query(&query_items);

        if let Ok(val) = std::env::var(MSI_SECRET_ENV_KEY) {
            builder = builder.header("x-identity-header", val);
        };

        let response = builder
            .send()
            .await
            .map_err(|e| CatalogError::External(format!("Failed to fetch token: {e}")))?;

        let response: MsiTokenResponse = non200_or_json(response).await?;

        Ok(TemporaryToken {
            token: response.access_token,
            expiry: Some(Instant::now() + Duration::from_secs(response.expires_in)),
        })
    }
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use reqwest::Client;
    use tempfile::NamedTempFile;
    use wiremock::matchers::{body_string_contains, header, method, path, path_regex, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    #[tokio::test]
    async fn test_managed_identity() {
        let server = MockServer::start().await;

        std::env::set_var(MSI_SECRET_ENV_KEY, "env-secret");

        let client = Client::new();

        Mock::given(method("GET"))
            .and(path("/metadata/identity/oauth2/token"))
            .and(query_param("client_id", "client_id"))
            .and(header("x-identity-header", "env-secret"))
            .and(header("metadata", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "TOKEN",
                "refresh_token": "",
                "expires_in": "3599",
                "expires_on": "1506484173",
                "not_before": "1506480273",
                "resource": "https://management.azure.com/",
                "token_type": "Bearer"
            })))
            .mount(&server)
            .await;

        let credential = ImdsManagedIdentityOAuthProvider::new(
            Some("client_id".into()),
            None,
            None,
            Some(format!("{}/metadata/identity/oauth2/token", server.uri())),
        );

        let token = credential.fetch_token(&client).await.unwrap();

        assert_eq!(&token.token, "TOKEN");
    }

    #[tokio::test]
    async fn test_invalid_response_code() {
        let server = MockServer::start().await;
        let client = Client::new();

        Mock::given(method("POST"))
            .and(path("/oidc/v1/token"))
            .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
                "error": "invalid_client",
                "error_id": "abc123",
                "error_description": "Client authentication failed"
            })))
            .mount(&server)
            .await;

        let credential = WorkspaceOAuthProvider::new("client_id", "client_secret", server.uri());

        let token = credential.fetch_token(&client).await;

        assert!(token.is_err());
    }

    #[tokio::test]
    async fn test_workload_identity() {
        let server = MockServer::start().await;
        let token_file = NamedTempFile::new().unwrap();
        let tenant = "tenant";
        std::fs::write(token_file.path(), "federated-token").unwrap();

        let client = Client::new();

        Mock::given(method("POST"))
            .and(path_regex(format!("/{tenant}/oauth2/v2.0/token")))
            .and(body_string_contains("federated-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "TOKEN",
                "refresh_token": "",
                "expires_in": 3599,
                "expires_on": "1506484173",
                "not_before": "1506480273",
                "resource": "https://management.azure.com/",
                "token_type": "Bearer"
            })))
            .mount(&server)
            .await;

        let credential = WorkloadIdentityOAuthProvider::new(
            "client_id",
            token_file.path().to_str().unwrap(),
            tenant,
            Some(format!("{}/{tenant}/oauth2/v2.0/token", server.uri())),
        );

        let token = credential.fetch_token(&client).await.unwrap();

        assert_eq!(&token.token, "TOKEN");
    }
}
