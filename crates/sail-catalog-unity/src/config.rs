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

// [CREDIT]: https://github.com/delta-io/delta-rs/blob/16621c499106dbaac11560441e0c59ed9346b4b9/crates/catalog-unity/src/lib.rs

use std::collections::HashMap;
use std::str::FromStr;

use sail_catalog::error::{CatalogError, CatalogResult};
use sail_catalog::utils::quote_name_if_needed;
use secrecy::{ExposeSecret, SecretString};

use crate::credential::{
    AzureCliCredential, ClientSecretOAuthProvider, CredentialProvider,
    ImdsManagedIdentityOAuthProvider, WorkloadIdentityOAuthProvider, WorkspaceOAuthProvider,
};
use crate::provider::DEFAULT_URI;
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
            UnityCatalogConfigKey::Token => "databricks_token",
            UnityCatalogConfigKey::WorkspaceUrl => "unity_workspace_url",
        }
    }
}

#[derive(Debug)]
pub struct UnityCatalogConfig {
    pub default_catalog: String,
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
    pub credential_provider: Option<CredentialProvider>,
}

fn str_is_truthy(value: &str) -> bool {
    let value = value.trim();
    value.eq_ignore_ascii_case("1")
        | value.eq_ignore_ascii_case("true")
        | value.eq_ignore_ascii_case("on")
        | value.eq_ignore_ascii_case("yes")
        | value.eq_ignore_ascii_case("y")
}

impl UnityCatalogConfig {
    pub fn new(
        default_catalog: Option<String>,
        uri: Option<String>,
        token: &Option<SecretString>,
        options: Option<HashMap<String, String>>,
    ) -> CatalogResult<Self> {
        let mut config = Self {
            default_catalog: quote_name_if_needed(
                &default_catalog.unwrap_or_else(|| "unity".to_string()),
            ),
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
            credential_provider: None,
        };

        if let Some(token) = token {
            let config_key = UnityCatalogConfigKey::from_str("token")?;
            config.set_option(config_key, token.expose_secret().to_string())?;
        }

        if let Some(opts) = options {
            config.apply_options(opts)?;
        }

        config.apply_env()?;

        config.credential_provider = config.get_credential_provider();

        Ok(config)
    }

    pub fn apply_env(&mut self) -> CatalogResult<()> {
        for (os_key, os_value) in std::env::vars_os() {
            if let (Some(key), Some(value)) = (os_key.to_str(), os_value.to_str()) {
                let key = key.trim();
                if key.to_ascii_uppercase().starts_with("UNITY_")
                    || key.to_ascii_uppercase().starts_with("DATABRICKS_")
                    || key.to_ascii_uppercase().starts_with("UC_")
                {
                    if let Ok(config_key) =
                        UnityCatalogConfigKey::from_str(&key.to_ascii_lowercase())
                    {
                        self.set_option(config_key, value.to_string())?;
                    }
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
                self.uri = value;
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
        }
        Ok(())
    }

    pub fn get_credential_provider(&self) -> Option<CredentialProvider> {
        if let Some(token) = &self.bearer_token {
            return Some(CredentialProvider::BearerToken(
                token.expose_secret().to_string(),
            ));
        }

        if let (Some(client_id), Some(client_secret)) = (&self.client_id, &self.client_secret) {
            if let Some(authority_id) = &self.authority_id {
                return Some(CredentialProvider::TokenCredential(
                    TokenCache::default(),
                    Box::new(ClientSecretOAuthProvider::new(
                        client_id,
                        client_secret.expose_secret(),
                        authority_id,
                        self.authority_host.as_ref(),
                    )),
                ));
            }

            return Some(CredentialProvider::TokenCredential(
                TokenCache::default(),
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
                Box::new(AzureCliCredential::new()),
            ));
        }

        if self.msi_endpoint.is_some() || self.msi_resource_id.is_some() {
            return Some(CredentialProvider::TokenCredential(
                TokenCache::default(),
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
