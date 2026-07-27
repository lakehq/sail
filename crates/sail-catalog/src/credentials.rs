use std::fmt::Debug;
use std::path::PathBuf;

use crate::error::{CatalogError, CatalogResult};

#[async_trait::async_trait]
pub trait CatalogCredentials: Debug + Send + Sync + 'static {
    async fn retrieve(&self) -> CatalogResult<Option<String>>;
}

#[derive(Debug, Default)]
pub struct EmptyCatalogCredentials;

#[async_trait::async_trait]
impl CatalogCredentials for EmptyCatalogCredentials {
    async fn retrieve(&self) -> CatalogResult<Option<String>> {
        Ok(None)
    }
}

#[derive(Debug)]
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
}
