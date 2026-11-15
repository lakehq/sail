use std::fmt::{Debug, Formatter};

use object_store::azure::{AzureCredential, AzureCredentialProvider, MicrosoftAzureBuilder};
use sail_catalog::credentials::CatalogCredentials;
use sail_catalog::error::{CatalogError, CatalogResult};
use tokio::sync::OnceCell;

pub enum OneLakeCredentials {
    Static {
        bearer_token: String,
    },
    Dynamic {
        workspace: String,
        provider: OnceCell<AzureCredentialProvider>,
    },
}

// The trait requires `Debug`. A static token is usable exactly as it is printed, and the Azure
// credential provider can hold an account key, so neither reaches the debug output.
impl Debug for OneLakeCredentials {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Static { .. } => f
                .debug_struct("Static")
                .field("bearer_token", &"[REDACTED]")
                .finish(),
            Self::Dynamic { workspace, .. } => f
                .debug_struct("Dynamic")
                .field("workspace", workspace)
                .field("provider", &"[REDACTED]")
                .finish(),
        }
    }
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

                        if std::env::var("AZURE_STORAGE_TOKEN").is_err()
                            && let Ok(token) = std::env::var("AZURE_ACCESS_TOKEN")
                        {
                            builder = builder.with_bearer_token_authorization(token);
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::StaticCredentialProvider;

    use super::*;

    #[test]
    fn debug_output_hides_the_static_token() {
        let credentials = OneLakeCredentials::Static {
            bearer_token: "s3cr3t-token".to_string(),
        };
        let output = format!("{credentials:?}");
        assert!(
            !output.contains("s3cr3t-token"),
            "unexpected debug output: {output}"
        );
    }

    #[test]
    fn debug_output_keeps_the_workspace_and_hides_the_provider() {
        let provider: AzureCredentialProvider = Arc::new(StaticCredentialProvider::new(
            AzureCredential::BearerToken("s3cr3t-token".to_string()),
        ));
        let credentials = OneLakeCredentials::Dynamic {
            workspace: "my-workspace".to_string(),
            provider: OnceCell::new_with(Some(provider)),
        };
        let output = format!("{credentials:?}");
        assert!(
            output.contains("my-workspace") && !output.contains("s3cr3t-token"),
            "unexpected debug output: {output}"
        );
    }
}
