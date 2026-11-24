// SPDX-License-Identifier: Apache-2.0
// SPDX-FileComment: Credit: https://github.com/delta-io/delta-rs/blob/16621c499106dbaac11560441e0c59ed9346b4b9/crates/catalog-unity/src/lib.rs
// SPDX-FileAttributionText: License: https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
// SPDX-FileCopyrightText: Copyright (2020) QP Hou and a number of other contributors.
// SPDX-FileCopyrightText: Portions Copyright (2025) LakeSail, Inc.
// SPDX-FileContributor: Modified by LakeSail, Inc. in 2025

use std::collections::HashMap;
use std::ffi::OsString;
use std::str::FromStr;

use sail_catalog::error::{CatalogError, CatalogResult};
use secrecy::{ExposeSecret, SecretString};

use crate::credential::{
    AzureCliCredential, ClientSecretOAuthProvider, CredentialProvider,
    ImdsManagedIdentityOAuthProvider, WorkloadIdentityOAuthProvider, WorkspaceOAuthProvider,
};
use crate::provider::{DEFAULT_URI, WORKSPACE_API_PATH};
use crate::token::TokenCache;

pub enum UnityCatalogConfigKey {
    /// Url of a Databricks workspace
    ///
    /// Supported keys:
    /// - `unity_workspace_url`
    /// - `databricks_workspace_url`
    /// - `workspace_url`
    ///
    /// Deprecated: Please use the DATABRICKS_HOST env variable
    WorkspaceUrl,

    /// Host of the Databricks workspace
    Host,

    /// Access token to authorize API requests
    ///
    /// Supported keys:
    /// - `unity_access_token`
    /// - `databricks_access_token`
    /// - `access_token`
    ///
    /// Deprecated: Please use the DATABRICKS_TOKEN env variable
    AccessToken,

    /// Token to use for Databricks Unity
    Token,

    /// Service principal client id for authorizing requests
    ///
    /// Supported keys:
    /// - `azure_client_id`
    /// - `unity_client_id`
    /// - `client_id`
    ClientId,

    /// Service principal client secret for authorizing requests
    ///
    /// Supported keys:
    /// - `azure_client_secret`
    /// - `unity_client_secret`
    /// - `client_secret`
    ClientSecret,

    /// Authority (tenant) id used in oauth flows
    ///
    /// Supported keys:
    /// - `azure_tenant_id`
    /// - `unity_tenant_id`
    /// - `tenant_id`
    AuthorityId,

    /// Authority host used in oauth flows
    ///
    /// Supported keys:
    /// - `azure_authority_host`
    /// - `unity_authority_host`
    /// - `authority_host`
    AuthorityHost,

    /// Endpoint to request a imds managed identity token
    ///
    /// Supported keys:
    /// - `azure_msi_endpoint`
    /// - `azure_identity_endpoint`
    /// - `identity_endpoint`
    /// - `msi_endpoint`
    MsiEndpoint,

    /// Msi resource id for use with managed identity authentication
    ///
    /// Supported keys:
    /// - `azure_msi_resource_id`
    /// - `msi_resource_id`
    MsiResourceId,

    /// Object id for use with managed identity authentication
    ///
    /// Supported keys:
    /// - `azure_object_id`
    /// - `object_id`
    ObjectId,

    /// File containing token for Azure AD workload identity federation
    ///
    /// Supported keys:
    /// - `azure_federated_token_file`
    /// - `federated_token_file`
    FederatedTokenFile,

    /// Use azure cli for acquiring access token
    ///
    /// Supported keys:
    /// - `azure_use_azure_cli`
    /// - `use_azure_cli`
    UseAzureCli,

    /// Allow http url (e.g. http://localhost:8080/api/2.1/...)
    /// Supported keys:
    /// - `unity_allow_http_url`
    AllowHttpUrl,

    /// The value of the `User-Agent` header of the requests to the catalog
    /// and of the requests for an access token
    ///
    /// Supported keys:
    /// - `unity_user_agent`
    /// - `user_agent`
    UserAgent,
}

impl FromStr for UnityCatalogConfigKey {
    type Err = CatalogError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "access_token"
            | "unity_access_token"
            | "uc_access_token"
            | "databricks_access_token"
            | "databricks_token" => Ok(UnityCatalogConfigKey::AccessToken),
            "authority_host"
            | "unity_authority_host"
            | "uc_authority_host"
            | "databricks_authority_host" => Ok(UnityCatalogConfigKey::AuthorityHost),
            "authority_id"
            | "unity_authority_id"
            | "uc_authority_id"
            | "databricks_authority_id" => Ok(UnityCatalogConfigKey::AuthorityId),
            "client_id" | "unity_client_id" | "uc_client_id" | "databricks_client_id" => {
                Ok(UnityCatalogConfigKey::ClientId)
            }
            "client_secret"
            | "unity_client_secret"
            | "uc_client_secret"
            | "databricks_client_secret" => Ok(UnityCatalogConfigKey::ClientSecret),
            "federated_token_file"
            | "unity_federated_token_file"
            | "uc_federated_token_file"
            | "databricks_federated_token_file" => Ok(UnityCatalogConfigKey::FederatedTokenFile),
            "host" => Ok(UnityCatalogConfigKey::Host),
            "msi_endpoint"
            | "unity_msi_endpoint"
            | "uc_msi_endpoint"
            | "databricks_msi_endpoint" => Ok(UnityCatalogConfigKey::MsiEndpoint),
            "msi_resource_id"
            | "unity_msi_resource_id"
            | "uc_msi_resource_id"
            | "databricks_msi_resource_id" => Ok(UnityCatalogConfigKey::MsiResourceId),
            "object_id" | "unity_object_id" | "uc_object_id" | "databricks_object_id" => {
                Ok(UnityCatalogConfigKey::ObjectId)
            }
            "token" => Ok(UnityCatalogConfigKey::Token),
            "use_azure_cli"
            | "unity_use_azure_cli"
            | "uc_use_azure_cli"
            | "databricks_use_azure_cli" => Ok(UnityCatalogConfigKey::UseAzureCli),
            "workspace_url"
            | "unity_workspace_url"
            | "uc_workspace_url"
            | "databricks_workspace_url"
            | "databricks_host"
            | "unity_host"
            | "uc_host" => Ok(UnityCatalogConfigKey::WorkspaceUrl),
            "allow_http_url"
            | "unity_allow_http_url"
            | "uc_allow_http_url"
            | "databricks_allow_http_url" => Ok(UnityCatalogConfigKey::AllowHttpUrl),
            "user_agent" | "unity_user_agent" | "uc_user_agent" | "databricks_user_agent" => {
                Ok(UnityCatalogConfigKey::UserAgent)
            }
            _ => Err(CatalogError::InvalidArgument(format!(
                "Unknown UnityCatalogConfigKey: {s}",
            ))),
        }
    }
}

impl AsRef<str> for UnityCatalogConfigKey {
    fn as_ref(&self) -> &str {
        match self {
            UnityCatalogConfigKey::AccessToken => "unity_access_token",
            UnityCatalogConfigKey::AllowHttpUrl => "unity_allow_http_url",
            UnityCatalogConfigKey::AuthorityHost => "unity_authority_host",
            UnityCatalogConfigKey::AuthorityId => "unity_authority_id",
            UnityCatalogConfigKey::ClientId => "unity_client_id",
            UnityCatalogConfigKey::ClientSecret => "unity_client_secret",
            UnityCatalogConfigKey::FederatedTokenFile => "unity_federated_token_file",
            UnityCatalogConfigKey::Host => "databricks_host",
            UnityCatalogConfigKey::MsiEndpoint => "unity_msi_endpoint",
            UnityCatalogConfigKey::MsiResourceId => "unity_msi_resource_id",
            UnityCatalogConfigKey::ObjectId => "unity_object_id",
            UnityCatalogConfigKey::UseAzureCli => "unity_use_azure_cli",
            UnityCatalogConfigKey::UserAgent => "unity_user_agent",
            UnityCatalogConfigKey::Token => "databricks_token",
            UnityCatalogConfigKey::WorkspaceUrl => "unity_workspace_url",
        }
    }
}

#[derive(Debug)]
pub struct UnityCatalogConfig {
    pub uri: String,
    pub bearer_token: Option<SecretString>,
    pub client_id: Option<String>,
    pub client_secret: Option<SecretString>,
    pub authority_id: Option<String>,
    pub authority_host: Option<String>,
    pub msi_endpoint: Option<String>,
    pub msi_resource_id: Option<String>,
    pub object_id: Option<String>,
    pub federated_token_file: Option<String>,
    pub use_azure_cli: bool,
    pub allow_http_url: bool,
    /// The value of the `User-Agent` header, or `None` for [`UNITY_USER_AGENT`].
    pub user_agent: Option<String>,
}

/// The value of the `User-Agent` header of the requests to Unity Catalog when the
/// `user_agent` option is not set, such as `Sail/0.7.3 Delta Spark`.
///
/// Databricks accepts the requests of the Unity Catalog Delta API that write (the storage
/// credentials for writing, staging tables, creating a table, and commits) only from
/// clients that it recognizes by the products in the `User-Agent` header, and answers the
/// others with HTTP 400. The `Delta` and `Spark` products say that Sail implements the
/// Delta protocol and the Spark API. They have no version, since Sail does not follow the
/// version of one release of Delta Lake or of Spark.
/// Remove the two products when Databricks recognizes Sail by its own name.
/// See the section "User-Agent" in `docs/guide/catalog/unity.md`.
pub(crate) const UNITY_USER_AGENT: &str =
    concat!("Sail/", env!("CARGO_PKG_VERSION"), " Delta Spark");

/// Where the `User-Agent` header of the requests to Unity Catalog is documented.
const USER_AGENT_DOCUMENTATION: &str =
    "https://docs.lakesail.com/sail/latest/guide/catalog/unity.html#user-agent";

/// Returns the message of the error for an HTTP 400 response of Unity Catalog. When the
/// response says that the server refuses the `User-Agent` header of the request, the
/// message starts with what to do about it, and the answer of the server follows.
///
/// The Delta REST API of Unity Catalog on Databricks answers such a request with "The UC
/// Delta API requires clients to identify the calling application in the User-Agent
/// header. The provided User-Agent '...' is insufficient. ...". Each of the two sentences
/// is recognized on its own, so the hint stays if the wording of one of them changes.
pub(crate) fn bad_request_message(message: String) -> String {
    let lower = message.to_ascii_lowercase();
    let refused = lower.contains("user-agent")
        && (lower.contains("identify the calling application")
            || lower.contains("is insufficient"));
    if !refused {
        return message;
    }
    format!(
        "Unity Catalog rejected the `User-Agent` header that Sail sent. Set the `user_agent` \
         option of the catalog to a value that the server accepts, see \
         {USER_AGENT_DOCUMENTATION}. {message}"
    )
}

/// Returns the `User-Agent` header for a configured value.
///
/// The value is a list of products as in RFC 9110, such as `MyEngine/1.2 MyConnector/0.4`.
/// It is sent as it is, so only an empty value and a value that cannot be a header value
/// are refused.
pub(crate) fn user_agent_header(value: &str) -> CatalogResult<reqwest::header::HeaderValue> {
    if value.trim().is_empty() {
        return Err(CatalogError::InvalidArgument(
            "The Unity Catalog option `user_agent` must not be empty".to_string(),
        ));
    }
    reqwest::header::HeaderValue::from_str(value).map_err(|e| {
        CatalogError::InvalidArgument(format!(
            "The Unity Catalog option `user_agent` is not a valid header value: {e}"
        ))
    })
}

fn str_is_truthy(value: &str) -> bool {
    let value = value.trim();
    value.eq_ignore_ascii_case("1")
        | value.eq_ignore_ascii_case("true")
        | value.eq_ignore_ascii_case("on")
        | value.eq_ignore_ascii_case("yes")
        | value.eq_ignore_ascii_case("y")
}

/// Returns the URI of the Unity Catalog API for a workspace URL.
///
/// The API path is added if the URL has no path, since the host of a workspace
/// (e.g. `DATABRICKS_HOST`) is configured without it.
fn workspace_api_uri(workspace_url: &str) -> String {
    match reqwest::Url::parse(workspace_url) {
        Ok(url) if url.has_host() && matches!(url.path(), "" | "/") => {
            format!("{}{WORKSPACE_API_PATH}", url.origin().ascii_serialization())
        }
        _ => workspace_url.to_string(),
    }
}

impl UnityCatalogConfig {
    pub fn new(
        uri: Option<String>,
        token: &Option<SecretString>,
        options: Option<HashMap<String, String>>,
    ) -> CatalogResult<Self> {
        Self::new_with_env(uri, token, options, std::env::vars_os())
    }

    fn new_with_env(
        uri: Option<String>,
        token: &Option<SecretString>,
        options: Option<HashMap<String, String>>,
        env: impl IntoIterator<Item = (OsString, OsString)>,
    ) -> CatalogResult<Self> {
        let mut config = Self {
            uri: uri.unwrap_or_else(|| DEFAULT_URI.to_string()),
            bearer_token: None,
            client_id: None,
            client_secret: None,
            authority_id: None,
            authority_host: None,
            msi_endpoint: None,
            msi_resource_id: None,
            object_id: None,
            federated_token_file: None,
            use_azure_cli: false,
            allow_http_url: false,
            user_agent: None,
        };

        if let Some(token) = token {
            let config_key = UnityCatalogConfigKey::from_str("token")?;
            config.set_option(config_key, token.expose_secret().to_string())?;
        }

        if let Some(opts) = options {
            config.apply_options(opts)?;
        }

        config.apply_env(env)?;

        Ok(config)
    }

    fn apply_env(
        &mut self,
        env: impl IntoIterator<Item = (OsString, OsString)>,
    ) -> CatalogResult<()> {
        for (os_key, os_value) in env {
            if let (Some(key), Some(value)) = (os_key.to_str(), os_value.to_str()) {
                let key = key.trim();
                if (key.to_ascii_uppercase().starts_with("UNITY_")
                    || key.to_ascii_uppercase().starts_with("DATABRICKS_")
                    || key.to_ascii_uppercase().starts_with("UC_"))
                    && let Ok(config_key) =
                        UnityCatalogConfigKey::from_str(&key.to_ascii_lowercase())
                {
                    self.set_option(config_key, value.to_string())?;
                }
            }
        }
        Ok(())
    }

    pub fn apply_options(&mut self, options: HashMap<String, String>) -> CatalogResult<()> {
        for (key, value) in options {
            let config_key = UnityCatalogConfigKey::from_str(&key)?;
            self.set_option(config_key, value)?;
        }
        Ok(())
    }

    pub fn set_option(&mut self, key: UnityCatalogConfigKey, value: String) -> CatalogResult<()> {
        match key {
            UnityCatalogConfigKey::AccessToken | UnityCatalogConfigKey::Token => {
                self.bearer_token = Some(SecretString::new(value.into_boxed_str()));
            }
            UnityCatalogConfigKey::AllowHttpUrl => {
                self.allow_http_url = str_is_truthy(&value);
            }
            UnityCatalogConfigKey::ClientId => {
                self.client_id = Some(value);
            }
            UnityCatalogConfigKey::ClientSecret => {
                self.client_secret = Some(SecretString::new(value.into_boxed_str()));
            }
            UnityCatalogConfigKey::AuthorityId => {
                self.authority_id = Some(value);
            }
            UnityCatalogConfigKey::AuthorityHost => {
                self.authority_host = Some(value);
            }
            UnityCatalogConfigKey::Host | UnityCatalogConfigKey::WorkspaceUrl => {
                self.uri = workspace_api_uri(&value);
            }
            UnityCatalogConfigKey::MsiEndpoint => {
                self.msi_endpoint = Some(value);
            }
            UnityCatalogConfigKey::MsiResourceId => {
                self.msi_resource_id = Some(value);
            }
            UnityCatalogConfigKey::ObjectId => {
                self.object_id = Some(value);
            }
            UnityCatalogConfigKey::FederatedTokenFile => {
                self.federated_token_file = Some(value);
            }
            UnityCatalogConfigKey::UseAzureCli => {
                self.use_azure_cli = str_is_truthy(&value);
            }
            UnityCatalogConfigKey::UserAgent => {
                user_agent_header(&value)?;
                self.user_agent = Some(value);
            }
        }
        Ok(())
    }

    pub fn get_credential_provider(&self) -> Option<CredentialProvider> {
        if let Some(token) = &self.bearer_token {
            return Some(CredentialProvider::BearerToken(
                token.expose_secret().to_string(),
            ));
        }

        if let (Some(client_id), Some(client_secret), Some(authority_id)) =
            (&self.client_id, &self.client_secret, &self.authority_id)
        {
            return Some(CredentialProvider::TokenCredential(
                TokenCache::default(),
                self.user_agent.clone(),
                Box::new(ClientSecretOAuthProvider::new(
                    client_id,
                    client_secret.expose_secret(),
                    authority_id,
                    self.authority_host.as_ref(),
                )),
            ));
        }

        if let (Some(client_id), Some(client_secret)) = (&self.client_id, &self.client_secret) {
            return Some(CredentialProvider::TokenCredential(
                TokenCache::default(),
                self.user_agent.clone(),
                Box::new(WorkspaceOAuthProvider::new(
                    client_id,
                    client_secret.expose_secret(),
                    &self.uri,
                )),
            ));
        }

        if let (Some(client_id), Some(federated_token_file), Some(authority_id)) = (
            &self.client_id,
            &self.federated_token_file,
            &self.authority_id,
        ) {
            return Some(CredentialProvider::TokenCredential(
                TokenCache::default(),
                self.user_agent.clone(),
                Box::new(WorkloadIdentityOAuthProvider::new(
                    client_id,
                    federated_token_file,
                    authority_id,
                    self.authority_host.clone(),
                )),
            ));
        }

        if self.use_azure_cli {
            return Some(CredentialProvider::TokenCredential(
                TokenCache::default(),
                self.user_agent.clone(),
                Box::new(AzureCliCredential::new()),
            ));
        }

        if self.msi_endpoint.is_some() || self.msi_resource_id.is_some() {
            return Some(CredentialProvider::TokenCredential(
                TokenCache::default(),
                self.user_agent.clone(),
                Box::new(ImdsManagedIdentityOAuthProvider::new(
                    self.client_id.clone(),
                    self.object_id.clone(),
                    self.msi_resource_id.clone(),
                    self.msi_endpoint.clone(),
                )),
            ));
        }

        None
    }
}

#[expect(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;

    fn env(vars: &[(&str, &str)]) -> Vec<(OsString, OsString)> {
        vars.iter()
            .map(|(key, value)| (OsString::from(key), OsString::from(value)))
            .collect()
    }

    #[test]
    fn test_env_takes_precedence_over_explicit_configuration() {
        let vars = env(&[
            ("DATABRICKS_HOST", "https://env.cloud.databricks.com"),
            ("DATABRICKS_TOKEN", "env-token"),
        ]);
        let token = Some(SecretString::from("config-token"));
        let options = HashMap::from([("client_id".to_string(), "config-client".to_string())]);
        let config = UnityCatalogConfig::new_with_env(
            Some("https://config.cloud.databricks.com/api/2.1/unity-catalog".to_string()),
            &token,
            Some(options),
            vars,
        )
        .unwrap();
        assert_eq!(
            config.uri,
            "https://env.cloud.databricks.com/api/2.1/unity-catalog"
        );
        assert_eq!(config.bearer_token.unwrap().expose_secret(), "env-token");
        // The explicit configuration is used for what the environment variables do not set.
        assert_eq!(config.client_id.as_deref(), Some("config-client"));

        let config = UnityCatalogConfig::new_with_env(
            Some("https://config.cloud.databricks.com/api/2.1/unity-catalog".to_string()),
            &token,
            None,
            env(&[]),
        )
        .unwrap();
        assert_eq!(
            config.uri,
            "https://config.cloud.databricks.com/api/2.1/unity-catalog"
        );
        assert_eq!(config.bearer_token.unwrap().expose_secret(), "config-token");

        let config = UnityCatalogConfig::new_with_env(None, &None, None, env(&[])).unwrap();
        assert_eq!(config.uri, DEFAULT_URI);
    }

    #[test]
    fn test_workspace_api_uri() {
        for (workspace_url, expected) in [
            (
                "https://dbc-1.cloud.databricks.com",
                "https://dbc-1.cloud.databricks.com/api/2.1/unity-catalog",
            ),
            (
                "https://adb-1.2.azuredatabricks.net/",
                "https://adb-1.2.azuredatabricks.net/api/2.1/unity-catalog",
            ),
            (
                "http://localhost:8080",
                "http://localhost:8080/api/2.1/unity-catalog",
            ),
            // A URL with a path is used as it is.
            (
                "https://dbc-1.cloud.databricks.com/api/2.1/unity-catalog",
                "https://dbc-1.cloud.databricks.com/api/2.1/unity-catalog",
            ),
            ("https://example.com/unity/", "https://example.com/unity/"),
            ("not a url", "not a url"),
        ] {
            assert_eq!(workspace_api_uri(workspace_url), expected);
        }
    }

    #[test]
    fn test_workspace_oauth_is_not_used_with_authority_id() {
        let vars = env(&[
            ("DATABRICKS_CLIENT_ID", "client"),
            ("DATABRICKS_CLIENT_SECRET", "secret"),
        ]);
        let uri = Some("https://dbc-1.cloud.databricks.com/api/2.1/unity-catalog".to_string());
        let config =
            UnityCatalogConfig::new_with_env(uri.clone(), &None, None, vars.clone()).unwrap();
        let output = format!("{:?}", config.get_credential_provider().unwrap());
        assert!(
            output.contains("\"https://dbc-1.cloud.databricks.com/oidc/v1/token\""),
            "unexpected credential provider: {output}"
        );

        let mut vars = vars;
        vars.extend(env(&[("DATABRICKS_AUTHORITY_ID", "tenant")]));
        let config = UnityCatalogConfig::new_with_env(uri, &None, None, vars).unwrap();
        let output = format!("{:?}", config.get_credential_provider().unwrap());
        assert!(
            output.contains("\"https://login.microsoftonline.com/tenant/oauth2/v2.0/token\""),
            "unexpected credential provider: {output}"
        );
    }

    #[test]
    fn test_user_agent_default_and_environment_variables() {
        // The default names Sail first, and then the products that Databricks looks for.
        assert_eq!(
            UNITY_USER_AGENT,
            format!("{} Delta Spark", sail_common::utils::http::SAIL_USER_AGENT)
        );
        assert!(user_agent_header(UNITY_USER_AGENT).is_ok());
        let config = UnityCatalogConfig::new_with_env(None, &None, None, env(&[])).unwrap();
        assert_eq!(config.user_agent, None);

        for key in ["DATABRICKS_USER_AGENT", "UNITY_USER_AGENT", "UC_USER_AGENT"] {
            let vars = env(&[(key, "ExampleEngine/1.2")]);
            let config = UnityCatalogConfig::new_with_env(None, &None, None, vars).unwrap();
            assert_eq!(
                config.user_agent.as_deref(),
                Some("ExampleEngine/1.2"),
                "{key}"
            );
        }
    }

    #[test]
    fn test_bad_request_message_has_a_hint_only_for_a_refused_user_agent() {
        let hint = "Set the `user_agent` option of the catalog";
        for refusal in [
            "The UC Delta API requires clients to identify the calling application in the \
             User-Agent header. The provided User-Agent 'Sail/0.7.3' is insufficient.",
            "The provided user-agent 'X/1' is insufficient",
            "Clients must identify the calling application in the User-Agent header",
        ] {
            let message = bad_request_message(format!("Failed to create table: {refusal}"));
            assert!(message.starts_with("Unity Catalog rejected"), "{message}");
            assert!(message.contains(hint), "{message}");
            assert!(message.contains(USER_AGENT_DOCUMENTATION), "{message}");
            assert!(
                message.ends_with(&format!("Failed to create table: {refusal}")),
                "{message}"
            );
        }
        for other in [
            "The table name is not valid",
            "The request is missing the required field 'name'",
            "The privileges of the principal are insufficient",
            "The header User-Agent is too long",
            "Table name 'user-agent' is not valid",
        ] {
            assert_eq!(bad_request_message(other.to_string()), other);
        }
    }
}
