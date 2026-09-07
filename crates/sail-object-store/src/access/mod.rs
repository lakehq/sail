pub mod iceberg;
mod router;

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use datafusion::catalog::Session;
use datafusion::execution::TaskContext;
use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use datafusion::execution::object_store::ObjectStoreRegistry;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::execution::session_state::{SessionState, SessionStateBuilder};
use datafusion_common::{Result, plan_datafusion_err};
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey, AwsCredential};
use object_store::{CredentialProvider, ObjectStore};
use sail_common::storage::{
    CatalogCredentialSource, IcebergCredentialSource, S3StorageConnection, ScopedStorageCredential,
    StorageAccessSpec,
};
use sail_common::utils::oauth::{OAuth2Client, OAuth2Error};
use tokio::sync::Mutex;
use url::Url;

use self::iceberg::{
    LoadCredentialsResponse, iceberg_storage_credentials, normalize_prefix, validate_http_endpoint,
};
use self::router::{CredentialRoutingStore, StorageRoute};

struct CredentialState {
    credentials: Vec<ScopedStorageCredential>,
    refresh_at_ms: Option<i64>,
    failures: u32,
    failure: Option<RefreshFailure>,
}

struct CredentialSession {
    source: Option<IcebergCredentialSource>,
    state: Mutex<CredentialState>,
    client: reqwest::Client,
    oauth: Option<OAuth2Client>,
}

#[derive(Debug, Clone, thiserror::Error)]
enum RefreshError {
    #[error("Storage credential refresh request failed")]
    Transport,
    #[error("Storage credential refresh returned HTTP {0}")]
    Status(u16),
    #[error("Storage credential refresh authentication failed: {0}")]
    Authentication(#[from] OAuth2Error),
    #[error("Invalid storage credential refresh response")]
    Response,
    #[error("Storage credential refresh changed routing or connection configuration")]
    Routing,
    #[error("Storage credential refresh returned expired credentials")]
    Expired,
}

impl RefreshError {
    fn is_transient(&self) -> bool {
        match self {
            Self::Transport | Self::Authentication(OAuth2Error::Transport) => true,
            Self::Status(status) | Self::Authentication(OAuth2Error::Status(status)) => {
                matches!(status, 408 | 429 | 500..=599)
            }
            _ => false,
        }
    }

    fn into_store_error(self) -> object_store::Error {
        object_store::Error::Generic {
            store: "credential vending",
            source: Box::new(self),
        }
    }
}

struct RefreshFailure {
    error: RefreshError,
    retry_at_ms: i64,
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn refresh_at(credentials: &[ScopedStorageCredential], now: i64) -> Option<i64> {
    credentials
        .iter()
        .filter_map(|entry| entry.s3.expires_at_ms)
        .map(|expiry| expiry.saturating_sub((expiry.saturating_sub(now).max(0) / 5).min(300_000)))
        .min()
}

fn access_error(message: &'static str) -> object_store::Error {
    object_store::Error::Generic {
        store: "credential vending",
        source: Box::new(std::io::Error::other(message)),
    }
}

impl CredentialSession {
    fn new(credentials: Vec<ScopedStorageCredential>) -> Result<Arc<Self>> {
        let source = credentials.first().and_then(|entry| entry.refresh.clone());
        if credentials.is_empty() || credentials.iter().any(|entry| entry.refresh != source) {
            return Err(plan_datafusion_err!(
                "Storage credentials require one refresh source per session"
            ));
        }
        let mut headers = reqwest::header::HeaderMap::new();
        let mut oauth = None;
        if let Some(source) = &source {
            validate_http_endpoint(&source.endpoint)?;
            for (name, value) in &source.headers {
                let name = reqwest::header::HeaderName::from_bytes(name.as_bytes())
                    .map_err(|_| plan_datafusion_err!("Invalid credential refresh header"))?;
                let mut value = reqwest::header::HeaderValue::from_str(value.expose())
                    .map_err(|_| plan_datafusion_err!("Invalid credential refresh header"))?;
                value.set_sensitive(true);
                headers.insert(name, value);
            }
            if let CatalogCredentialSource::OAuth2(credential) = &source.authentication {
                oauth = Some(OAuth2Client::new(credential.clone()).map_err(|_| {
                    plan_datafusion_err!("Invalid credential refresh OAuth2 configuration")
                })?);
            }
            if let CatalogCredentialSource::Bearer(token) = &source.authentication {
                reqwest::header::HeaderValue::from_str(&format!("Bearer {}", token.expose()))
                    .map_err(|_| plan_datafusion_err!("Invalid credential refresh bearer token"))?;
            }
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(30))
            .default_headers(headers)
            .build()
            .map_err(|_| plan_datafusion_err!("Cannot create storage credentials client"))?;
        Ok(Arc::new(Self {
            state: Mutex::new(CredentialState {
                refresh_at_ms: refresh_at(&credentials, now_ms()),
                credentials,
                failures: 0,
                failure: None,
            }),
            source,
            client,
            oauth,
        }))
    }

    async fn fetch(
        &self,
        source: &IcebergCredentialSource,
    ) -> std::result::Result<LoadCredentialsResponse, RefreshError> {
        for attempt in 0..2 {
            let token = match &source.authentication {
                CatalogCredentialSource::None => None,
                CatalogCredentialSource::Bearer(token) => Some(token.clone()),
                CatalogCredentialSource::OAuth2(_) => Some(
                    self.oauth
                        .as_ref()
                        .ok_or(RefreshError::Response)?
                        .token()
                        .await?,
                ),
            };
            let mut request = self
                .client
                .get(&source.endpoint)
                .header("X-Iceberg-Access-Delegation", "vended-credentials");
            if let Some(token) = &token {
                request = request.bearer_auth(token.expose());
            }
            let response = request.send().await.map_err(|_| RefreshError::Transport)?;
            if response.status() == reqwest::StatusCode::UNAUTHORIZED
                && attempt == 0
                && let (Some(oauth), Some(token)) = (&self.oauth, &token)
            {
                oauth.reject(token.expose()).await;
                continue;
            }
            if !response.status().is_success() {
                return Err(RefreshError::Status(response.status().as_u16()));
            }
            return response.json().await.map_err(|_| RefreshError::Response);
        }
        Err(RefreshError::Status(401))
    }

    fn refreshed_credentials(
        previous: &[ScopedStorageCredential],
        response: LoadCredentialsResponse,
    ) -> std::result::Result<Vec<ScopedStorageCredential>, RefreshError> {
        let mut credentials = Vec::new();
        for mut entry in response.credentials {
            entry.prefix = normalize_prefix(&entry.prefix).map_err(|_| RefreshError::Response)?;
            let Some(previous) = previous
                .iter()
                .find(|previous| previous.prefix == entry.prefix)
            else {
                // A table endpoint may also return credentials owned by another refresh source.
                continue;
            };
            if credentials
                .iter()
                .any(|credential: &ScopedStorageCredential| credential.prefix == entry.prefix)
            {
                return Err(RefreshError::Routing);
            }
            let config = connection_properties(&previous.s3.connection);
            let mut parsed = iceberg_storage_credentials("", &config, &[entry])
                .map_err(|_| RefreshError::Response)?;
            let credential = parsed.first_mut().ok_or(RefreshError::Response)?;
            if credential.s3.connection != previous.s3.connection {
                return Err(RefreshError::Routing);
            }
            if credential
                .s3
                .expires_at_ms
                .is_some_and(|expiry| expiry <= now_ms())
            {
                return Err(RefreshError::Expired);
            }
            credential.refresh = previous.refresh.clone();
            credentials.extend(parsed);
        }
        if credentials.len() != previous.len() {
            return Err(RefreshError::Routing);
        }
        Ok(credentials)
    }

    async fn credential(
        &self,
        prefix: &str,
        connection: &S3StorageConnection,
    ) -> object_store::Result<Arc<AwsCredential>> {
        let mut state = self.state.lock().await;
        if state
            .refresh_at_ms
            .is_some_and(|refresh_at| now_ms() >= refresh_at)
            && let Some(source) = &self.source
            && state
                .failure
                .as_ref()
                .is_none_or(|failure| now_ms() >= failure.retry_at_ms)
        {
            let result = self
                .fetch(source)
                .await
                .and_then(|response| Self::refreshed_credentials(&state.credentials, response));
            match result {
                Ok(credentials) => {
                    state.refresh_at_ms = refresh_at(&credentials, now_ms());
                    state.credentials = credentials;
                    state.failures = 0;
                    state.failure = None;
                }
                Err(error) => {
                    state.failures = state.failures.saturating_add(1);
                    let delay = (500_i64 << state.failures.min(6)).min(30_000);
                    let jitter = rand::random_range(0..=delay / 4);
                    state.failure = Some(RefreshFailure {
                        error,
                        retry_at_ms: now_ms().saturating_add(delay + jitter),
                    });
                }
            }
        }
        let credential = state
            .credentials
            .iter()
            .find(|entry| entry.prefix == prefix && &entry.s3.connection == connection)
            .ok_or_else(|| access_error("Storage credential route is unavailable"))?;
        if let Some(failure) = &state.failure
            && (!failure.error.is_transient()
                || credential
                    .s3
                    .expires_at_ms
                    .is_none_or(|expiry| expiry <= now_ms()))
        {
            return Err(failure.error.clone().into_store_error());
        }
        if credential
            .s3
            .expires_at_ms
            .is_some_and(|expiry| expiry <= now_ms())
        {
            return Err(access_error("Vended storage credentials have expired"));
        }
        Ok(Arc::new(AwsCredential {
            key_id: credential.s3.access_key_id.expose().to_string(),
            secret_key: credential.s3.secret_access_key.expose().to_string(),
            token: credential
                .s3
                .session_token
                .as_ref()
                .map(|token| token.expose().to_string()),
        }))
    }
}

fn connection_properties(connection: &S3StorageConnection) -> HashMap<String, String> {
    let mut config = HashMap::from([
        ("s3.region".to_string(), connection.region.clone()),
        (
            "s3.path-style-access".to_string(),
            connection.path_style_access.to_string(),
        ),
    ]);
    for (key, value) in [
        ("s3.endpoint", &connection.endpoint),
        ("s3.sse.type", &connection.sse_type),
        ("s3.sse.key", &connection.sse_key),
    ] {
        if let Some(value) = value {
            config.insert(key.to_string(), value.clone());
        }
    }
    config
}

struct VendedS3CredentialProvider {
    session: Arc<CredentialSession>,
    prefix: String,
    connection: S3StorageConnection,
}

impl fmt::Debug for VendedS3CredentialProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("VendedS3CredentialProvider")
    }
}

#[async_trait::async_trait]
impl CredentialProvider for VendedS3CredentialProvider {
    type Credential = AwsCredential;
    async fn get_credential(&self) -> object_store::Result<Arc<AwsCredential>> {
        self.session
            .credential(&self.prefix, &self.connection)
            .await
    }
}

#[derive(Debug)]
struct StorageAccessRegistry {
    stores: dashmap::DashMap<String, Arc<dyn ObjectStore>>,
    base: Arc<dyn ObjectStoreRegistry>,
}

fn origin(url: &Url) -> String {
    let scheme = match url.scheme() {
        "s3a" | "s3n" => "s3",
        scheme => scheme,
    };
    format!("{scheme}://{}", url.authority())
}

impl ObjectStoreRegistry for StorageAccessRegistry {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        self.stores.insert(origin(url), store)
    }

    fn get_store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>> {
        if let Some(store) = self.stores.get(&origin(url)) {
            return Ok(store.value().clone());
        }
        if matches!(url.scheme(), "file" | "memory") {
            return self.base.get_store(url);
        }
        Err(plan_datafusion_err!(
            "No delegated storage access for object-store origin"
        ))
    }
}

pub fn storage_runtime(
    base: &Arc<RuntimeEnv>,
    spec: &StorageAccessSpec,
) -> Result<Arc<RuntimeEnv>> {
    let mut groups: HashMap<Option<IcebergCredentialSource>, Vec<ScopedStorageCredential>> =
        HashMap::new();
    for entry in &spec.credentials {
        groups
            .entry(entry.refresh.clone())
            .or_default()
            .push(entry.clone());
    }
    let sessions = groups
        .into_iter()
        .map(|(source, credentials)| Ok((source, CredentialSession::new(credentials)?)))
        .collect::<Result<HashMap<_, _>>>()?;
    let mut routes: HashMap<String, Vec<StorageRoute>> = HashMap::new();
    for entry in &spec.credentials {
        let url = Url::parse(&entry.prefix)
            .map_err(|_| plan_datafusion_err!("Invalid storage credential prefix"))?;
        let bucket = url
            .host_str()
            .ok_or_else(|| plan_datafusion_err!("Missing S3 bucket"))?;
        let connection = &entry.s3.connection;
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region(&connection.region)
            .with_virtual_hosted_style_request(!connection.path_style_access)
            .with_credentials(Arc::new(VendedS3CredentialProvider {
                session: sessions
                    .get(&entry.refresh)
                    .ok_or_else(|| plan_datafusion_err!("Missing storage credential session"))?
                    .clone(),
                prefix: entry.prefix.clone(),
                connection: connection.clone(),
            }));
        if let Some(endpoint) = &connection.endpoint {
            let endpoint_url = validate_http_endpoint(endpoint)?;
            builder = builder
                .with_endpoint(endpoint)
                .with_allow_http(endpoint_url.scheme() == "http");
        }
        if let Some(sse_type) = &connection.sse_type {
            let value = match sse_type.as_str() {
                "s3" => "AES256",
                "kms" => "aws:kms",
                "dsse-kms" => "aws:kms:dsse",
                _ => return Err(plan_datafusion_err!("Unsupported storage encryption")),
            };
            builder = builder.with_config(
                "aws_server_side_encryption".parse::<AmazonS3ConfigKey>()?,
                value,
            );
        }
        if let Some(key) = &connection.sse_key {
            builder = builder.with_config("aws_sse_kms_key_id".parse::<AmazonS3ConfigKey>()?, key);
        }
        let store = builder
            .build()
            .map_err(|_| plan_datafusion_err!("Invalid delegated S3 configuration"))?;
        let mut prefix = object_store::path::Path::from_url_path(url.path())?.to_string();
        if !prefix.is_empty() && url.path().ends_with('/') {
            prefix.push('/');
        }
        routes
            .entry(origin(&url))
            .or_default()
            .push((prefix, Arc::new(store)));
    }
    let stores = routes
        .into_iter()
        .map(|(origin, routes)| {
            (
                origin,
                Arc::new(CredentialRoutingStore::new(routes)) as Arc<dyn ObjectStore>,
            )
        })
        .collect();
    RuntimeEnvBuilder::from_runtime_env(base)
        .with_object_store_registry(Arc::new(StorageAccessRegistry {
            stores,
            base: base.object_store_registry.clone(),
        }))
        .with_cache_manager(CacheManagerConfig::default())
        .build_arc()
}

pub fn storage_task_context(context: &TaskContext, runtime: Arc<RuntimeEnv>) -> Arc<TaskContext> {
    Arc::new(TaskContext::new(
        context.task_id(),
        context.session_id(),
        context.session_config().clone(),
        context.scalar_functions().clone(),
        context.higher_order_functions().clone(),
        context.aggregate_functions().clone(),
        context.window_functions().clone(),
        runtime,
    ))
}

pub fn storage_session(session: &dyn Session, spec: &StorageAccessSpec) -> Result<SessionState> {
    let state = session
        .as_any()
        .downcast_ref::<SessionState>()
        .ok_or_else(|| plan_datafusion_err!("Storage access requires a SessionState"))?;
    Ok(SessionStateBuilder::new_from_existing(state.clone())
        .with_runtime_env(storage_runtime(session.runtime_env(), spec)?)
        .build())
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests;
