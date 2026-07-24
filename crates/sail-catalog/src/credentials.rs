use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime};

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

/// How long a token read from a [`FileCatalogCredentials`] file is reused before
/// the file is checked again. Kubelet refreshes projected service account tokens
/// well before they expire (by default once 80% of their lifetime has elapsed),
/// so a short interval keeps the in-memory token fresh without reading the file
/// on every catalog request.
const FILE_CREDENTIALS_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

/// Credentials backed by a token file on disk, such as a kubelet-projected
/// service account token. The token is cached in memory and re-read when the
/// file's modification time changes or the refresh interval elapses, so a
/// rotated token is picked up without restarting the server.
#[derive(Debug)]
pub struct FileCatalogCredentials {
    path: PathBuf,
    cached: Mutex<Option<CachedCredential>>,
}

#[derive(Debug, Clone)]
struct CachedCredential {
    credential: String,
    modified: Option<SystemTime>,
    read_at: Instant,
}

impl FileCatalogCredentials {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            cached: Mutex::new(None),
        }
    }
}

#[async_trait::async_trait]
impl CatalogCredentials for FileCatalogCredentials {
    async fn retrieve(&self) -> CatalogResult<Option<String>> {
        let modified = tokio::fs::metadata(&self.path)
            .await
            .and_then(|metadata| metadata.modified())
            .ok();
        {
            let cached = self
                .cached
                .lock()
                .map_err(|e| CatalogError::Internal(format!("token file cache poisoned: {e}")))?;
            if let Some(cached) = cached.as_ref() {
                let fresh = cached.read_at.elapsed() < FILE_CREDENTIALS_REFRESH_INTERVAL;
                let changed = modified.is_some() && modified != cached.modified;
                if fresh && !changed {
                    return Ok(Some(cached.credential.clone()));
                }
            }
        }
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
        let mut cached = self
            .cached
            .lock()
            .map_err(|e| CatalogError::Internal(format!("token file cache poisoned: {e}")))?;
        *cached = Some(CachedCredential {
            credential: credential.clone(),
            modified,
            read_at: Instant::now(),
        });
        Ok(Some(credential))
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used)]

    use std::fs::{File, FileTimes};
    use std::io::Write;
    use std::path::Path;
    use std::time::{Duration, SystemTime};

    use tempfile::TempDir;

    use super::*;

    fn write_token(path: &Path, contents: &str) {
        let mut file = File::create(path).unwrap();
        file.write_all(contents.as_bytes()).unwrap();
    }

    fn set_modified(path: &Path, modified: SystemTime) {
        let file = File::options().write(true).open(path).unwrap();
        file.set_times(FileTimes::new().set_modified(modified))
            .unwrap();
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
    async fn retrieve_rereads_when_modification_time_changes() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("token");
        write_token(&path, "first-token");
        let earlier = SystemTime::UNIX_EPOCH + Duration::from_secs(1_600_000_000);
        set_modified(&path, earlier);

        let credentials = FileCatalogCredentials::new(&path);
        assert_eq!(
            credentials.retrieve().await.unwrap(),
            Some("first-token".to_string())
        );

        // Rotate the token and advance the file's modification time. The refresh
        // interval has not elapsed, so the mtime change is the only thing that can
        // trigger a re-read. This mirrors kubelet swapping a projected token.
        write_token(&path, "second-token");
        set_modified(&path, earlier + Duration::from_secs(60));

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

        // An empty read must not be cached: once the file holds a token again,
        // the next retrieve returns it.
        write_token(&path, "recovered-token");
        assert_eq!(
            credentials.retrieve().await.unwrap(),
            Some("recovered-token".to_string())
        );
    }
}
