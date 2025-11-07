// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// [CREDIT]: https://github.com/delta-io/delta-rs/blob/16621c499106dbaac11560441e0c59ed9346b4b9/crates/catalog-unity/src/lib.rs

use std::str::FromStr;

use sail_catalog::error::CatalogError;

// CHECK HERE
#[allow(dead_code)]
pub enum UnityCatalogConfigKey {
    /// Url of a Databricks workspace
    ///
    /// Supported keys:
    /// - `unity_workspace_url`
    /// - `databricks_workspace_url`
    /// - `workspace_url`
    #[deprecated(since = "0.17.0", note = "Please use the DATABRICKS_HOST env variable")]
    WorkspaceUrl,

    /// Host of the Databricks workspace
    Host,

    /// Access token to authorize API requests
    ///
    /// Supported keys:
    /// - `unity_access_token`
    /// - `databricks_access_token`
    /// - `access_token`
    #[deprecated(
        since = "0.17.0",
        note = "Please use the DATABRICKS_TOKEN env variable"
    )]
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

    /// Object id for use with managed identity authentication
    ///
    /// Supported keys:
    /// - `azure_object_id`
    /// - `object_id`
    ObjectId,

    /// Msi resource id for use with managed identity authentication
    ///
    /// Supported keys:
    /// - `azure_msi_resource_id`
    /// - `msi_resource_id`
    MsiResourceId,

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

    #[allow(deprecated)]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "access_token"
            | "unity_access_token"
            | "databricks_access_token"
            | "databricks_token" => Ok(UnityCatalogConfigKey::AccessToken),
            "authority_host" | "unity_authority_host" | "databricks_authority_host" => {
                Ok(UnityCatalogConfigKey::AuthorityHost)
            }
            "authority_id" | "unity_authority_id" | "databricks_authority_id" => {
                Ok(UnityCatalogConfigKey::AuthorityId)
            }
            "client_id" | "unity_client_id" | "databricks_client_id" => {
                Ok(UnityCatalogConfigKey::ClientId)
            }
            "client_secret" | "unity_client_secret" | "databricks_client_secret" => {
                Ok(UnityCatalogConfigKey::ClientSecret)
            }
            "federated_token_file"
            | "unity_federated_token_file"
            | "databricks_federated_token_file" => Ok(UnityCatalogConfigKey::FederatedTokenFile),
            "host" => Ok(UnityCatalogConfigKey::Host),
            "msi_endpoint" | "unity_msi_endpoint" | "databricks_msi_endpoint" => {
                Ok(UnityCatalogConfigKey::MsiEndpoint)
            }
            "msi_resource_id" | "unity_msi_resource_id" | "databricks_msi_resource_id" => {
                Ok(UnityCatalogConfigKey::MsiResourceId)
            }
            "object_id" | "unity_object_id" | "databricks_object_id" => {
                Ok(UnityCatalogConfigKey::ObjectId)
            }
            "token" => Ok(UnityCatalogConfigKey::Token),
            "use_azure_cli" | "unity_use_azure_cli" | "databricks_use_azure_cli" => {
                Ok(UnityCatalogConfigKey::UseAzureCli)
            }
            "workspace_url"
            | "unity_workspace_url"
            | "databricks_workspace_url"
            | "databricks_host" => Ok(UnityCatalogConfigKey::WorkspaceUrl),
            "allow_http_url" | "unity_allow_http_url" => Ok(UnityCatalogConfigKey::AllowHttpUrl),
            _ => Err(CatalogError::InvalidArgument(format!(
                "Unknown UnityCatalogConfigKey: {s}",
            ))),
        }
    }
}

#[allow(deprecated)]
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
