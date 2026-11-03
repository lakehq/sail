use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use chrono::Utc;
use datafusion::execution::object_store::ObjectStoreRegistry;
use log::info;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use sail_common::config::{LineageConfig, LineageTransportType};
use sail_common::runtime::RuntimeHandle;
use sail_object_store::DynamicObjectStoreRegistry;
use secrecy::{ExposeSecret, SecretString};
use tokio::io::AsyncWriteExt;
use url::Url;
use uuid::Uuid;

use crate::error::{LineageError, LineageResult};

/// A destination for serialized OpenLineage events.
#[derive(Debug)]
pub enum Transport {
    /// Writes events to the application log.
    Console,
    /// Appends events to a file as JSON lines.
    File { path: PathBuf },
    /// Posts events to an OpenLineage HTTP backend.
    Http {
        client: reqwest::Client,
        url: String,
        api_key: Option<SecretString>,
    },
    /// Writes each event as a separate JSON object under a directory of an object store.
    ObjectStore {
        store: Arc<dyn ObjectStore>,
        prefix: Path,
        sequence: AtomicU64,
    },
}

impl Transport {
    pub fn try_new(config: &LineageConfig, runtime: &RuntimeHandle) -> LineageResult<Self> {
        match config.transport {
            LineageTransportType::Console => Ok(Transport::Console),
            LineageTransportType::File => {
                let Some(path) = &config.file.path else {
                    return Err(LineageError::InvalidConfig(
                        "the file path is required for the OpenLineage file transport".to_string(),
                    ));
                };
                Ok(Transport::File {
                    path: PathBuf::from(path),
                })
            }
            LineageTransportType::Http => {
                let client = reqwest::Client::builder()
                    .timeout(Duration::from_secs(config.http.timeout_secs))
                    .build()
                    .map_err(|e| LineageError::InvalidConfig(e.to_string()))?;
                let url = join_url(&config.http.url, &config.http.endpoint);
                url::Url::parse(&url).map_err(|e| {
                    LineageError::InvalidConfig(format!("invalid OpenLineage URL {url}: {e}"))
                })?;
                Ok(Transport::Http {
                    client,
                    url,
                    api_key: config.http.api_key.clone(),
                })
            }
            LineageTransportType::ObjectStore => {
                let Some(url) = &config.object_store.url else {
                    return Err(LineageError::InvalidConfig(
                        "the URL is required for the OpenLineage object store transport"
                            .to_string(),
                    ));
                };
                let invalid = |e: &dyn std::fmt::Display| {
                    LineageError::InvalidConfig(format!(
                        "invalid OpenLineage object store URL {url}: {e}"
                    ))
                };
                let url = Url::parse(url).map_err(|e| invalid(&e))?;
                let prefix = Path::from_url_path(url.path()).map_err(|e| invalid(&e))?;
                let store = DynamicObjectStoreRegistry::new(runtime.clone())
                    .get_store(&url)
                    .map_err(|e| invalid(&e))?;
                Ok(Transport::ObjectStore {
                    store,
                    prefix,
                    sequence: AtomicU64::new(0),
                })
            }
        }
    }

    pub async fn send(&self, event: &str) -> LineageResult<()> {
        match self {
            Transport::Console => {
                info!(target: "openlineage", "{event}");
                Ok(())
            }
            Transport::File { path } => {
                let mut file = tokio::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)
                    .await?;
                let mut line = String::with_capacity(event.len() + 1);
                line.push_str(event);
                line.push('\n');
                file.write_all(line.as_bytes()).await?;
                file.flush().await?;
                Ok(())
            }
            Transport::Http {
                client,
                url,
                api_key,
            } => {
                let mut request = client
                    .post(url)
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .body(event.to_string());
                if let Some(key) = api_key {
                    request = request.bearer_auth(key.expose_secret());
                }
                let response = request.send().await?;
                let status = response.status();
                if !status.is_success() {
                    let body = response.text().await.unwrap_or_default();
                    return Err(LineageError::Transport(format!(
                        "OpenLineage backend returned {status}: {body}"
                    )));
                }
                Ok(())
            }
            Transport::ObjectStore {
                store,
                prefix,
                sequence,
            } => {
                // The sequence keeps lexicographic order equal to emission order for events
                // written in the same millisecond, so consumers can process a listing in order.
                // The random suffix avoids collisions between servers sharing the same prefix.
                let name = format!(
                    "{}_{:010}_{}.json",
                    Utc::now().timestamp_millis(),
                    sequence.fetch_add(1, Ordering::Relaxed),
                    Uuid::new_v4().simple()
                );
                store
                    .put(
                        &prefix.clone().join(name),
                        PutPayload::from(event.to_string()),
                    )
                    .await?;
                Ok(())
            }
        }
    }
}

fn join_url(base: &str, endpoint: &str) -> String {
    let base = base.trim_end_matches('/');
    let endpoint = endpoint.trim_start_matches('/');
    if endpoint.is_empty() {
        base.to_string()
    } else {
        format!("{base}/{endpoint}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_join_url() {
        assert_eq!(
            join_url("http://localhost:5000/", "/api/v1/lineage"),
            "http://localhost:5000/api/v1/lineage"
        );
        assert_eq!(
            join_url("http://localhost:5000", ""),
            "http://localhost:5000"
        );
    }

    #[tokio::test]
    async fn test_object_store_transport_writes_objects() -> LineageResult<()> {
        use futures::TryStreamExt;
        use object_store::memory::InMemory;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let prefix = Path::from_url_path("/lineage/dag_id=d/task=t")
            .map_err(|e| LineageError::Transport(e.to_string()))?;
        let transport = Transport::ObjectStore {
            store: Arc::clone(&store),
            prefix: prefix.clone(),
            sequence: AtomicU64::new(0),
        };
        transport.send("{\"a\":1}").await?;
        transport.send("{\"a\":2}").await?;
        let mut objects = store.list(Some(&prefix)).try_collect::<Vec<_>>().await?;
        objects.sort_by(|a, b| a.location.cmp(&b.location));
        assert_eq!(objects.len(), 2);
        let mut contents = vec![];
        for object in &objects {
            assert!(object.location.as_ref().ends_with(".json"));
            let bytes = store.get(&object.location).await?.bytes().await?;
            contents.push(String::from_utf8_lossy(&bytes).into_owned());
        }
        assert_eq!(contents, vec!["{\"a\":1}", "{\"a\":2}"]);
        Ok(())
    }

    #[tokio::test]
    async fn test_file_transport_appends_lines() -> LineageResult<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("events.jsonl");
        let transport = Transport::File { path: path.clone() };
        transport.send("{\"a\":1}").await?;
        transport.send("{\"a\":2}").await?;
        let content = tokio::fs::read_to_string(&path).await?;
        assert_eq!(content, "{\"a\":1}\n{\"a\":2}\n");
        Ok(())
    }
}
