use object_store::azure::{AzureCredential, AzureCredentialProvider, MicrosoftAzureBuilder};
use sail_catalog::credentials::CatalogCredentials;
use sail_catalog::error::{CatalogError, CatalogResult};
use tokio::sync::OnceCell;

#[derive(Debug)]
pub enum OneLakeCredentials {
    Static {
        bearer_token: String,
    },
    Dynamic {
        workspace: String,
        provider: OnceCell<AzureCredentialProvider>,
    },
}

#[async_trait::async_trait]
impl CatalogCredentials for OneLakeCredentials {
    async fn retrieve(&self) -> CatalogResult<Option<String>> {
        match self {
            Self::Static { bearer_token } => Ok(Some(bearer_token.clone())),
            Self::Dynamic {
                workspace,
                provider,
            } => {
                let provider = provider
                    .get_or_try_init(|| async {
                        let mut builder = MicrosoftAzureBuilder::from_env()
                            .with_account("onelake")
                            .with_container_name(workspace)
                            .with_use_fabric_endpoint(true);

                        if std::env::var("AZURE_STORAGE_TOKEN").is_err() {
                            if let Ok(token) = std::env::var("AZURE_ACCESS_TOKEN") {
                                builder = builder.with_bearer_token_authorization(token);
                            }
                        }

                        let store = builder.build().map_err(|e| {
                            CatalogError::External(format!(
                                "Failed to build OneLake Azure credential provider: {e}"
                            ))
                        })?;
                        Ok::<AzureCredentialProvider, CatalogError>(store.credentials().clone())
                    })
                    .await?;

                let credential = provider.get_credential().await.map_err(|e| {
                    CatalogError::External(format!("Failed to get OneLake token: {e}"))
                })?;

                match credential.as_ref() {
                    AzureCredential::BearerToken(token) => Ok(Some(token.clone())),
                    AzureCredential::AccessKey(_) | AzureCredential::SASToken(_) => {
                        Err(CatalogError::External(
                            "OneLake catalog requires Microsoft Entra bearer token authentication; Azure access keys and SAS tokens are not supported".to_string(),
                        ))
                    }
                }
            }
        }
    }
}
