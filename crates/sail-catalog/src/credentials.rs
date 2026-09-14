use std::fmt::Debug;
use std::path::PathBuf;

use sail_common::storage::{CatalogCredentialSource, OAuth2ClientCredentials};
use sail_common::utils::oauth::OAuth2Client;

use crate::error::{CatalogError, CatalogResult};

#[async_trait::async_trait]
pub trait CatalogCredentials: Debug + Send + Sync + 'static {
    async fn retrieve(&self) -> CatalogResult<Option<String>>;

    async fn reject(&self, _token: &str) {}

    async fn export_for_execution(&self) -> CatalogResult<CatalogCredentialSource> {
        Err(CatalogError::UnsupportedCapability(
            "worker credential refresh requires transferable catalog authentication; driver-local token sources are unsupported".to_string(),
        ))
    }
}

#[derive(Debug, Default)]
pub struct EmptyCatalogCredentials;

#[async_trait::async_trait]
impl CatalogCredentials for EmptyCatalogCredentials {
    async fn export_for_execution(&self) -> CatalogResult<CatalogCredentialSource> {
        Ok(CatalogCredentialSource::None)
    }

    async fn retrieve(&self) -> CatalogResult<Option<String>> {
        Ok(None)
    }
}

pub struct StaticCatalogCredentials {
    credential: String,
}

impl StaticCatalogCredentials {
    pub fn new(credential: String) -> Self {
        Self { credential }
    }
}

#[async_trait::async_trait]
impl CatalogCredentials for StaticCatalogCredentials {
    async fn export_for_execution(&self) -> CatalogResult<CatalogCredentialSource> {
        Ok(CatalogCredentialSource::Bearer(
            sail_common::storage::StorageSecret::new(self.credential.clone()),
        ))
    }

    async fn retrieve(&self) -> CatalogResult<Option<String>> {
        Ok(Some(self.credential.clone()))
    }
}

/// Credentials backed by a token file on disk, such as a kubelet-projected
/// service account token. The file is read on every call, so a rotated token
/// is picked up without restarting the server. The Iceberg REST provider reads
/// the credential fresh for each request and retries once on a `401`, so a
/// token that rotates mid-operation is recovered without an in-memory cache.
#[derive(Debug)]
pub struct FileCatalogCredentials {
    path: PathBuf,
}

impl FileCatalogCredentials {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

#[async_trait::async_trait]
impl CatalogCredentials for FileCatalogCredentials {
    async fn retrieve(&self) -> CatalogResult<Option<String>> {
        let credential = tokio::fs::read_to_string(&self.path)
            .await
            .map_err(|e| {
                CatalogError::External(format!(
                    "failed to read token file {}: {e}",
                    self.path.display()
                ))
            })?
            .trim()
            .to_string();
        if credential.is_empty() {
            return Err(CatalogError::External(format!(
                "token file {} is empty",
                self.path.display()
            )));
        }
        Ok(Some(credential))
    }
}

impl Debug for StaticCatalogCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StaticCatalogCredentials([REDACTED])")
    }
}

#[derive(Debug)]
pub struct OAuthCatalogCredentials {
    client: OAuth2Client,
}

impl OAuthCatalogCredentials {
    pub fn try_new(source: OAuth2ClientCredentials) -> CatalogResult<Self> {
        Ok(Self {
            client: OAuth2Client::new(source)
                .map_err(|error| CatalogError::InvalidArgument(error.to_string()))?,
        })
    }
}

#[async_trait::async_trait]
impl CatalogCredentials for OAuthCatalogCredentials {
    async fn retrieve(&self) -> CatalogResult<Option<String>> {
        self.client
            .token()
            .await
            .map(|token| Some(token.expose().to_string()))
            .map_err(|error| CatalogError::External(error.to_string()))
    }

    async fn reject(&self, token: &str) {
        self.client.reject(token).await;
    }

    async fn export_for_execution(&self) -> CatalogResult<CatalogCredentialSource> {
        Ok(CatalogCredentialSource::OAuth2(
            self.client.source().clone(),
        ))
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used)]

    use std::fs::File;
    use std::io::Write;
    use std::path::Path;

    use tempfile::TempDir;

    use super::*;

    fn write_token(path: &Path, contents: &str) {
        let mut file = File::create(path).unwrap();
        file.write_all(contents.as_bytes()).unwrap();
    }

    #[tokio::test]
    async fn retrieve_returns_token_from_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("token");
        write_token(&path, "s3cr3t-token");

        let credentials = FileCatalogCredentials::new(&path);
        assert_eq!(
            credentials.retrieve().await.unwrap(),
            Some("s3cr3t-token".to_string())
        );
    }

    #[tokio::test]
    async fn retrieve_trims_surrounding_whitespace() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("token");
        write_token(&path, "  s3cr3t-token\n\n");

        let credentials = FileCatalogCredentials::new(&path);
        assert_eq!(
            credentials.retrieve().await.unwrap(),
            Some("s3cr3t-token".to_string())
        );
    }

    #[tokio::test]
    async fn retrieve_rereads_rotated_token() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("token");
        write_token(&path, "first-token");

        let credentials = FileCatalogCredentials::new(&path);
        assert_eq!(
            credentials.retrieve().await.unwrap(),
            Some("first-token".to_string())
        );

        // Every call reads the file, so a rotated token (for example kubelet
        // swapping a projected service account token) is picked up on the next
        // retrieve without restarting the server.
        write_token(&path, "second-token");
        assert_eq!(
            credentials.retrieve().await.unwrap(),
            Some("second-token".to_string())
        );
    }

    #[tokio::test]
    async fn retrieve_reports_error_for_missing_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("does-not-exist");

        let credentials = FileCatalogCredentials::new(&path);
        let error = credentials.retrieve().await.unwrap_err();
        assert!(
            matches!(error, CatalogError::External(_)),
            "unexpected error variant: {error:?}"
        );
    }

    #[tokio::test]
    async fn retrieve_reports_error_for_empty_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("token");
        write_token(&path, "\n  \n");

        let credentials = FileCatalogCredentials::new(&path);
        let error = credentials.retrieve().await.unwrap_err();
        assert!(
            matches!(&error, CatalogError::External(message) if message.contains("empty")),
            "unexpected error: {error:?}"
        );

        // Once the file holds a token again, the next retrieve returns it.
        write_token(&path, "recovered-token");
        assert_eq!(
            credentials.retrieve().await.unwrap(),
            Some("recovered-token".to_string())
        );
    }

    #[tokio::test]
    async fn oauth_tokens_are_shared_and_renewed_after_expiry() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        let token_count = AtomicUsize::new(0);
        Mock::given(method("POST")).and(path("/token"))
            .respond_with(move |_request: &wiremock::Request| {
                let token = format!("token-{}", token_count.fetch_add(1, Ordering::SeqCst));
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"access_token": token, "token_type": "bearer", "expires_in": 1}))
            }).expect(2).mount(&server).await;
        let source = OAuth2ClientCredentials {
            endpoint: format!("{}/token", server.uri()),
            client_id: "client".to_string(),
            client_secret: sail_common::storage::StorageSecret::new("secret".to_string()),
            scope: None,
        };
        let credential = OAuthCatalogCredentials::try_new(source.clone()).unwrap();
        let (a, b, c) = tokio::join!(
            credential.retrieve(),
            credential.retrieve(),
            credential.retrieve()
        );
        for token in [a, b, c] {
            assert_eq!(token.unwrap().as_deref(), Some("token-0"));
        }
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        assert_eq!(
            credential.retrieve().await.unwrap().as_deref(),
            Some("token-1")
        );
        credential.reject("token-0").await;
        assert_eq!(
            credential.retrieve().await.unwrap().as_deref(),
            Some("token-1")
        );
        assert_eq!(
            credential.export_for_execution().await.unwrap(),
            CatalogCredentialSource::OAuth2(source)
        );
    }

    #[tokio::test]
    async fn oauth_errors_do_not_disclose_the_token_endpoint_or_response() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(401).set_body_string("sensitive-response"))
            .expect(1)
            .mount(&server)
            .await;
        let credential = OAuthCatalogCredentials::try_new(OAuth2ClientCredentials {
            endpoint: format!("{}/token?private-parameter=hidden", server.uri()),
            client_id: "client".to_string(),
            client_secret: sail_common::storage::StorageSecret::new("client-secret".to_string()),
            scope: None,
        })
        .unwrap();
        let error = credential.retrieve().await.unwrap_err().to_string();
        assert!(error.contains("401"));
        for value in ["sensitive-response", "private-parameter", "client-secret"] {
            assert!(!error.contains(value));
            assert!(!format!("{credential:?}").contains(value));
        }
    }
}
