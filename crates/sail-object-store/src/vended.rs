// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use datafusion::execution::object_store::ObjectStoreRegistry;
use datafusion_common::{Result as DataFusionResult, plan_datafusion_err};
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use log::warn;
use object_store::aws::AwsCredential;
use object_store::azure::{AzureCredential, MicrosoftAzureBuilder};
use object_store::gcp::{GcpCredential, GoogleCloudStorageBuilder};
use object_store::path::Path;
use object_store::{
    CopyOptions, CredentialProvider, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, RenameOptions, Result,
};
use percent_encoding::percent_decode_str;
use sail_common::runtime::RuntimeHandle;
use sail_common_datafusion::catalog::{StorageAccess, StorageCredential, TableStorageCredentials};
use sail_common_datafusion::extension::SessionExtension;
use tonic::codegen::Bytes;
use url::Url;

use crate::layers::lazy::LazyObjectStore;
use crate::layers::logging::LoggingObjectStore;
use crate::layers::runtime::RuntimeAwareObjectStore;
use crate::s3::get_s3_object_store_with_credentials;

/// A credential is replaced when it expires within this time.
const REFRESH_MARGIN: Duration = Duration::from_secs(300);
/// A credential that is not expired yet is replaced at most once within this time.
const REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// Gets the storage credentials for an access level from where they are issued.
/// The error message must not contain secret values.
pub type StorageCredentialFetch = Arc<
    dyn Fn(
            StorageAccess,
        ) -> BoxFuture<'static, std::result::Result<TableStorageCredentials, String>>
        + Send
        + Sync,
>;

fn credential_error(message: String) -> object_store::Error {
    object_store::Error::Generic {
        store: "vended credentials",
        source: message.into(),
    }
}

/// A credential of an object store that is created from a storage credential.
pub trait VendedCredential: Sized + Send + Sync + 'static {
    fn try_from_credential(credential: StorageCredential) -> Option<Self>;
}

impl VendedCredential for AwsCredential {
    fn try_from_credential(credential: StorageCredential) -> Option<Self> {
        match credential {
            StorageCredential::S3 {
                access_key_id,
                secret_access_key,
                session_token,
            } => Some(AwsCredential {
                key_id: access_key_id,
                secret_key: secret_access_key,
                token: session_token,
            }),
            _ => None,
        }
    }
}

impl VendedCredential for AzureCredential {
    fn try_from_credential(credential: StorageCredential) -> Option<Self> {
        let StorageCredential::AzureSas { token } = credential else {
            return None;
        };
        let token = percent_decode_str(&token).decode_utf8().ok()?;
        let pairs = token
            .trim_start_matches('?')
            .split('&')
            .filter(|x| !x.trim().is_empty())
            .map(|x| {
                let (key, value) = x.trim().split_once('=')?;
                Some((key.to_string(), value.to_string()))
            })
            .collect::<Option<Vec<_>>>()?;
        Some(AzureCredential::SASToken(pairs))
    }
}

impl VendedCredential for GcpCredential {
    fn try_from_credential(credential: StorageCredential) -> Option<Self> {
        match credential {
            StorageCredential::GcsOauth { token } => Some(GcpCredential { bearer: token }),
            _ => None,
        }
    }
}

struct CurrentCredential<C> {
    credential: Arc<C>,
    expires_at_ms: Option<i64>,
    fetched_at: Instant,
}

impl<C> CurrentCredential<C> {
    /// Returns the time until the credential expires, or nothing if it does not expire.
    fn remaining(&self) -> Option<Duration> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|x| x.as_millis() as i64)
            .unwrap_or(i64::MAX);
        self.expires_at_ms.map(|expires_at_ms| {
            Duration::from_millis(expires_at_ms.saturating_sub(now).max(0) as u64)
        })
    }

    fn is_expired(&self) -> bool {
        self.remaining().is_some_and(|x| x.is_zero())
    }

    fn needs_refresh(&self) -> bool {
        self.remaining().is_some_and(|x| {
            x.is_zero() || (x <= REFRESH_MARGIN && self.fetched_at.elapsed() >= REFRESH_INTERVAL)
        })
    }
}

/// Provides the storage credential issued for an access level to an object store.
/// The credential is fetched when it is first used and before it expires.
pub struct VendedCredentialProvider<C> {
    access: StorageAccess,
    fetch: StorageCredentialFetch,
    current: tokio::sync::Mutex<Option<CurrentCredential<C>>>,
}

impl<C: VendedCredential> VendedCredentialProvider<C> {
    pub fn try_new(
        access: StorageAccess,
        initial: Option<TableStorageCredentials>,
        fetch: StorageCredentialFetch,
    ) -> Result<Self> {
        let current = initial.map(Self::current).transpose()?;
        Ok(Self {
            access,
            fetch,
            current: tokio::sync::Mutex::new(current),
        })
    }

    fn current(credentials: TableStorageCredentials) -> Result<CurrentCredential<C>> {
        let credential = C::try_from_credential(credentials.credential).ok_or_else(|| {
            credential_error("the storage credential cannot be used for the storage".to_string())
        })?;
        Ok(CurrentCredential {
            credential: Arc::new(credential),
            expires_at_ms: credentials.expires_at_ms,
            fetched_at: Instant::now(),
        })
    }
}

impl<C> fmt::Debug for VendedCredentialProvider<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VendedCredentialProvider")
            .field("access", &self.access)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl<C: VendedCredential> CredentialProvider for VendedCredentialProvider<C> {
    type Credential = C;

    async fn get_credential(&self) -> Result<Arc<C>> {
        // The lock is held while the credential is fetched,
        // so that concurrent requests fetch the credential once.
        let mut current = self.current.lock().await;
        if let Some(current) = current.as_ref()
            && !current.needs_refresh()
        {
            return Ok(Arc::clone(&current.credential));
        }
        let fetched = match (self.fetch)(self.access).await {
            Ok(credentials) => Self::current(credentials)?,
            Err(message) => {
                // The current credential is used for as long as it is valid.
                if let Some(current) = current.as_mut()
                    && !current.is_expired()
                {
                    current.fetched_at = Instant::now();
                    return Ok(Arc::clone(&current.credential));
                }
                return Err(credential_error(message));
            }
        };
        let credential = Arc::clone(&fetched.credential);
        *current = Some(fetched);
        Ok(credential)
    }
}

/// The description of an object store that accesses a storage location
/// with the credentials issued for an access level.
pub struct VendedStoreRequest<'a> {
    pub location: &'a Url,
    pub access: StorageAccess,
    /// A credential of the kind that is issued for the storage location.
    pub kind: &'a StorageCredential,
    /// The credentials to start with, if they are fetched already.
    pub initial: Option<TableStorageCredentials>,
    pub fetch: StorageCredentialFetch,
}

pub type VendedStoreFactory =
    Arc<dyn Fn(VendedStoreRequest<'_>) -> Result<Arc<dyn ObjectStore>> + Send + Sync>;

/// Creates the object store in the same way as the object store that uses
/// the credentials of the environment, except for the credentials.
fn create_vended_store(
    runtime: &RuntimeHandle,
    request: VendedStoreRequest<'_>,
) -> Result<Arc<dyn ObjectStore>> {
    let VendedStoreRequest {
        location,
        access,
        kind,
        initial,
        fetch,
    } = request;
    let store = RuntimeAwareObjectStore::try_new(
        || {
            let store: Arc<dyn ObjectStore> = match kind {
                StorageCredential::S3 { .. } => {
                    let provider = Arc::new(VendedCredentialProvider::<AwsCredential>::try_new(
                        access, initial, fetch,
                    )?);
                    let url = location.clone();
                    Arc::new(LazyObjectStore::new(move || {
                        let url = url.clone();
                        let provider = Arc::clone(&provider);
                        async move { get_s3_object_store_with_credentials(&url, Some(provider)).await }
                    }))
                }
                StorageCredential::AzureSas { .. } => {
                    let provider = VendedCredentialProvider::<AzureCredential>::try_new(
                        access, initial, fetch,
                    )?;
                    Arc::new(
                        MicrosoftAzureBuilder::from_env()
                            .with_url(location.to_string())
                            .with_credentials(Arc::new(provider))
                            .build()?,
                    )
                }
                StorageCredential::GcsOauth { .. } => {
                    let provider =
                        VendedCredentialProvider::<GcpCredential>::try_new(access, initial, fetch)?;
                    Arc::new(
                        GoogleCloudStorageBuilder::from_env()
                            .with_url(location.to_string())
                            .with_credentials(Arc::new(provider))
                            .build()?,
                    )
                }
            };
            Ok(Arc::new(LoggingObjectStore::new(store)) as Arc<dyn ObjectStore>)
        },
        runtime.io().clone(),
    )?;
    Ok(Arc::new(store))
}

#[derive(Debug, Clone)]
struct Route {
    prefix: Path,
    owner: String,
    read: Arc<dyn ObjectStore>,
    write: Arc<dyn ObjectStore>,
}

fn select_route<'a>(routes: &'a [Route], location: &Path) -> Option<&'a Route> {
    routes
        .iter()
        .filter(|route| location.prefix_matches(&route.prefix))
        .max_by_key(|route| route.prefix.parts().count())
}

/// An object store that accesses the locations under registered path prefixes
/// with the object stores registered for the prefix, and all other locations with
/// the default object store.
///
/// Each prefix has an object store for reading and an object store for writing,
/// so that write credentials are only needed when the location is written.
#[derive(Debug)]
pub struct PrefixRoutingObjectStore {
    default: Arc<dyn ObjectStore>,
    routes: RwLock<Vec<Route>>,
}

impl PrefixRoutingObjectStore {
    pub fn new(default: Arc<dyn ObjectStore>) -> Self {
        Self {
            default,
            routes: RwLock::new(Vec::new()),
        }
    }

    fn routes(&self) -> Vec<Route> {
        self.routes
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Registers the object stores for a path prefix, replacing those registered before.
    pub fn set_route(
        &self,
        prefix: Path,
        owner: &str,
        read: Arc<dyn ObjectStore>,
        write: Arc<dyn ObjectStore>,
    ) {
        let mut routes = self.routes.write().unwrap_or_else(|e| e.into_inner());
        routes.retain(|route| route.prefix != prefix);
        routes.push(Route {
            prefix,
            owner: owner.to_string(),
            read,
            write,
        });
    }

    fn has_route(&self, prefix: &Path, owner: &str) -> bool {
        self.routes
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .any(|route| &route.prefix == prefix && route.owner == owner)
    }

    fn reader(&self, location: Option<&Path>) -> Arc<dyn ObjectStore> {
        let routes = self.routes.read().unwrap_or_else(|e| e.into_inner());
        location
            .and_then(|location| select_route(&routes, location))
            .map_or_else(
                || Arc::clone(&self.default),
                |route| Arc::clone(&route.read),
            )
    }

    fn writer(&self, location: &Path) -> Arc<dyn ObjectStore> {
        let routes = self.routes.read().unwrap_or_else(|e| e.into_inner());
        select_route(&routes, location).map_or_else(
            || Arc::clone(&self.default),
            |route| Arc::clone(&route.write),
        )
    }
}

impl fmt::Display for PrefixRoutingObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PrefixRoutingObjectStore({})", self.default)
    }
}

#[async_trait::async_trait]
#[warn(clippy::missing_trait_methods)]
impl ObjectStore for PrefixRoutingObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.writer(location)
            .put_opts(location, payload, opts)
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.writer(location)
            .put_multipart_opts(location, opts)
            .await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.reader(Some(location))
            .get_opts(location, options)
            .await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.reader(Some(location))
            .get_ranges(location, ranges)
            .await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let default = Arc::clone(&self.default);
        let routes = self.routes();
        locations
            .map(move |location| {
                let store = location.as_ref().ok().map(|location| {
                    select_route(&routes, location)
                        .map_or_else(|| Arc::clone(&default), |route| Arc::clone(&route.write))
                });
                async move {
                    let location = location?;
                    if let Some(store) = store {
                        store.delete(&location).await?;
                    }
                    Ok(location)
                }
            })
            .buffered(10)
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.reader(prefix).list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.reader(prefix).list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.reader(prefix).list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.writer(to).copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.writer(to).rename_opts(from, to, options).await
    }
}

/// The object stores of a session that access table locations with the storage
/// credentials issued by catalogs.
///
/// The object stores are registered in the object store registry of the session,
/// so the credentials are not visible to other sessions.
pub struct VendedObjectStores {
    enabled: bool,
    factory: VendedStoreFactory,
    /// The object stores registered in the object store registry of the session,
    /// by URL scheme and authority.
    stores: Mutex<HashMap<(String, String), Arc<PrefixRoutingObjectStore>>>,
    reported_disabled: AtomicBool,
    /// The owners for which a refusal to issue credentials is reported.
    reported_refused: Mutex<HashSet<String>>,
}

impl SessionExtension for VendedObjectStores {
    fn name() -> &'static str {
        "vended object stores"
    }
}

impl VendedObjectStores {
    /// Creates the object stores of a session. Storage credentials issued by catalogs
    /// are not used if `enabled` is false.
    pub fn new(runtime: RuntimeHandle, enabled: bool) -> Self {
        let mut stores = Self::with_factory(Arc::new(move |request| {
            create_vended_store(&runtime, request)
        }));
        stores.enabled = enabled;
        stores
    }

    pub fn with_factory(factory: VendedStoreFactory) -> Self {
        Self {
            enabled: true,
            factory,
            stores: Mutex::new(HashMap::new()),
            reported_disabled: AtomicBool::new(false),
            reported_refused: Mutex::new(HashSet::new()),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Reports once that the storage credentials issued by catalogs are not used.
    pub fn report_disabled(&self) {
        if !self.reported_disabled.swap(true, Ordering::Relaxed) {
            warn!(
                "storage credentials issued by catalogs are not used in cluster mode; \
                 tables are accessed with the storage credentials of the environment"
            );
        }
    }

    /// Reports once for the owner that the catalog refuses to issue storage credentials.
    /// Returns whether this is the first report for the owner.
    pub fn report_refused(&self, owner: &str, table: &str, message: &str) -> bool {
        let first = self
            .reported_refused
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(owner.to_string());
        if first {
            warn!(
                "the catalog does not issue storage credentials for table {table}, \
                 so the table is accessed with the storage credentials of the environment: \
                 {message}"
            );
        }
        first
    }

    fn key(location: &Url) -> (String, String) {
        (
            location.scheme().to_string(),
            location.authority().to_string(),
        )
    }

    fn prefix(location: &Url) -> DataFusionResult<Path> {
        Path::from_url_path(location.path())
            .map_err(|e| plan_datafusion_err!("invalid storage location {location}: {e}"))
    }

    /// Whether the storage location is accessed with the credentials issued for the owner.
    pub fn contains(&self, location: &Url, owner: &str) -> bool {
        let Ok(prefix) = Self::prefix(location) else {
            return false;
        };
        self.stores
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&Self::key(location))
            .is_some_and(|store| store.has_route(&prefix, owner))
    }

    /// Accesses the storage location with the credentials issued for the owner
    /// in this session from now on. The credentials for reading are given, and
    /// the credentials for writing are fetched when the location is first written.
    pub fn install(
        &self,
        registry: &dyn ObjectStoreRegistry,
        location: &Url,
        owner: &str,
        initial: TableStorageCredentials,
        fetch: StorageCredentialFetch,
    ) -> DataFusionResult<()> {
        let prefix = Self::prefix(location)?;
        let kind = initial.credential.clone();
        let read = (self.factory)(VendedStoreRequest {
            location,
            access: StorageAccess::Read,
            kind: &kind,
            initial: Some(initial),
            fetch: Arc::clone(&fetch),
        })?;
        let write = (self.factory)(VendedStoreRequest {
            location,
            access: StorageAccess::ReadWrite,
            kind: &kind,
            initial: None,
            fetch,
        })?;
        let mut stores = self.stores.lock().unwrap_or_else(|e| e.into_inner());
        let store = match stores.get(&Self::key(location)) {
            Some(store) => Arc::clone(store),
            None => {
                let store = Arc::new(PrefixRoutingObjectStore::new(registry.get_store(location)?));
                registry.register_store(location, Arc::clone(&store) as Arc<dyn ObjectStore>);
                stores.insert(Self::key(location), Arc::clone(&store));
                store
            }
        };
        store.set_route(prefix, owner, read, write);
        Ok(())
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use futures::TryStreamExt;
    use object_store::memory::InMemory;
    use tokio::runtime::Handle;

    use super::*;
    use crate::DynamicObjectStoreRegistry;

    fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    fn s3_credentials(key: &str, expires_at_ms: Option<i64>) -> TableStorageCredentials {
        TableStorageCredentials {
            credential: StorageCredential::S3 {
                access_key_id: key.to_string(),
                secret_access_key: "secret-value".to_string(),
                session_token: Some("token-value".to_string()),
            },
            expires_at_ms,
        }
    }

    /// Returns a fetch function that issues credentials named after the access level and
    /// the number of the request, and the access levels requested so far.
    fn counting_fetch(
        expires_at_ms: Option<i64>,
    ) -> (StorageCredentialFetch, Arc<Mutex<Vec<StorageAccess>>>) {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let fetch: StorageCredentialFetch = {
            let requests = Arc::clone(&requests);
            Arc::new(move |access| {
                let mut requests = requests.lock().unwrap();
                requests.push(access);
                let key = format!("{access:?}-{}", requests.len());
                Box::pin(async move { Ok(s3_credentials(&key, expires_at_ms)) })
            })
        };
        (fetch, requests)
    }

    fn failing_fetch() -> StorageCredentialFetch {
        Arc::new(|_| Box::pin(async { Err("the catalog refused the request".to_string()) }))
    }

    async fn key_id(provider: &VendedCredentialProvider<AwsCredential>) -> String {
        provider.get_credential().await.unwrap().key_id.clone()
    }

    #[tokio::test]
    async fn credential_provider_fetches_missing_and_expiring_credentials() {
        // A credential that does not expire, or expires late, is kept.
        for expires_at_ms in [None, Some(now_ms() + 3_600_000)] {
            let (fetch, requests) = counting_fetch(None);
            let provider = VendedCredentialProvider::<AwsCredential>::try_new(
                StorageAccess::Read,
                Some(s3_credentials("initial", expires_at_ms)),
                fetch,
            )
            .unwrap();
            assert_eq!(key_id(&provider).await, "initial");
            assert!(requests.lock().unwrap().is_empty());
        }

        // A missing credential is fetched once for the access level of the provider.
        let (fetch, requests) = counting_fetch(Some(now_ms() + 3_600_000));
        let provider = VendedCredentialProvider::<AwsCredential>::try_new(
            StorageAccess::ReadWrite,
            None,
            fetch,
        )
        .unwrap();
        assert_eq!(key_id(&provider).await, "ReadWrite-1");
        assert_eq!(key_id(&provider).await, "ReadWrite-1");
        assert_eq!(*requests.lock().unwrap(), vec![StorageAccess::ReadWrite]);

        // An expired credential is replaced.
        let (fetch, requests) = counting_fetch(Some(now_ms() + 3_600_000));
        let provider = VendedCredentialProvider::<AwsCredential>::try_new(
            StorageAccess::Read,
            Some(s3_credentials("initial", Some(now_ms() - 1))),
            fetch,
        )
        .unwrap();
        assert_eq!(key_id(&provider).await, "Read-1");
        assert_eq!(key_id(&provider).await, "Read-1");
        assert_eq!(requests.lock().unwrap().len(), 1);

        // A credential that expires soon is replaced once it is not fetched just now.
        let (fetch, requests) = counting_fetch(Some(now_ms() + 3_600_000));
        let provider = VendedCredentialProvider::<AwsCredential>::try_new(
            StorageAccess::Read,
            Some(s3_credentials("initial", Some(now_ms() + 60_000))),
            fetch,
        )
        .unwrap();
        assert_eq!(key_id(&provider).await, "initial");
        assert!(requests.lock().unwrap().is_empty());
        if let Some(current) = provider.current.lock().await.as_mut() {
            current.fetched_at = Instant::now().checked_sub(REFRESH_INTERVAL).unwrap();
        }
        assert_eq!(key_id(&provider).await, "Read-1");
    }

    #[tokio::test]
    async fn credential_provider_reports_fetch_errors_without_secrets() {
        // The current credential is used while it is valid.
        let provider = VendedCredentialProvider::<AwsCredential>::try_new(
            StorageAccess::Read,
            Some(s3_credentials("initial", Some(now_ms() + 60_000))),
            failing_fetch(),
        )
        .unwrap();
        if let Some(current) = provider.current.lock().await.as_mut() {
            current.fetched_at = Instant::now().checked_sub(REFRESH_INTERVAL).unwrap();
        }
        assert_eq!(key_id(&provider).await, "initial");

        let provider = VendedCredentialProvider::<AwsCredential>::try_new(
            StorageAccess::Read,
            Some(s3_credentials("initial", Some(now_ms() - 1))),
            failing_fetch(),
        )
        .unwrap();
        let error = provider.get_credential().await.unwrap_err();
        let message = format!("{error} {error:?} {provider:?}");
        assert!(
            message.contains("the catalog refused the request"),
            "{message}"
        );
        for secret in ["initial", "secret-value", "token-value"] {
            assert!(!message.contains(secret), "{message}");
        }

        // A credential for another storage is not used.
        let (fetch, _) = counting_fetch(None);
        let provider =
            VendedCredentialProvider::<GcpCredential>::try_new(StorageAccess::Read, None, fetch)
                .unwrap();
        let error = provider.get_credential().await.unwrap_err();
        let message = format!("{error} {error:?}");
        assert!(
            !message.contains("secret-value") && !message.contains("Read-1"),
            "{message}"
        );
    }

    #[test]
    fn storage_credentials_are_converted_and_redacted() {
        let sas = StorageCredential::AzureSas {
            token: "?sv=2024-01-01&sig=a%2Bb%3D&se=2030".to_string(),
        };
        assert_eq!(
            format!("{sas:?}"),
            "StorageCredential::AzureSas(<redacted>)"
        );
        let pairs = match AzureCredential::try_from_credential(sas) {
            Some(AzureCredential::SASToken(pairs)) => pairs,
            _ => vec![],
        };
        assert_eq!(
            pairs,
            vec![
                ("sv".to_string(), "2024-01-01".to_string()),
                ("sig".to_string(), "a+b=".to_string()),
                ("se".to_string(), "2030".to_string()),
            ]
        );
        let credentials = s3_credentials("key-value", None);
        let debug = format!("{credentials:?}");
        for secret in ["key-value", "secret-value", "token-value"] {
            assert!(!debug.contains(secret), "{debug}");
        }
        assert!(AwsCredential::try_from_credential(credentials.credential).is_some());
        let gcs = StorageCredential::GcsOauth {
            token: "token".to_string(),
        };
        assert!(AwsCredential::try_from_credential(gcs.clone()).is_none());
        assert_eq!(
            GcpCredential::try_from_credential(gcs).unwrap().bearer,
            "token"
        );
    }

    async fn paths(store: &dyn ObjectStore, prefix: Option<&str>) -> Vec<String> {
        let prefix = prefix.map(Path::from);
        let mut paths = store
            .list(prefix.as_ref())
            .map_ok(|x| x.location.to_string())
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        paths.sort();
        paths
    }

    #[tokio::test]
    async fn routing_store_routes_by_prefix_and_operation() {
        let default = Arc::new(InMemory::new());
        let read = Arc::new(InMemory::new());
        let write = Arc::new(InMemory::new());
        let nested = Arc::new(InMemory::new());
        let store = PrefixRoutingObjectStore::new(default.clone());
        store.set_route(Path::from("data/items"), "a", read.clone(), write.clone());
        store.set_route(
            Path::from("data/items/nested"),
            "b",
            nested.clone(),
            nested.clone(),
        );
        assert!(store.has_route(&Path::from("data/items"), "a"));
        assert!(!store.has_route(&Path::from("data/items"), "b"));
        assert!(!store.has_route(&Path::from("data"), "a"));

        for path in [
            "data/items/x",
            "data/items2/x",
            "data/x",
            "data/items/nested/x",
        ] {
            store.put(&Path::from(path), path.into()).await.unwrap();
        }
        // The locations under a prefix are written with the store for writing,
        // and a prefix only covers whole path segments.
        assert_eq!(paths(write.as_ref(), None).await, vec!["data/items/x"]);
        assert_eq!(
            paths(nested.as_ref(), None).await,
            vec!["data/items/nested/x"]
        );
        assert_eq!(
            paths(default.as_ref(), None).await,
            vec!["data/items2/x", "data/x"]
        );
        assert!(paths(read.as_ref(), None).await.is_empty());

        // The locations under a prefix are read with the store for reading.
        let path = Path::from("data/items/x");
        assert!(matches!(
            store.get(&path).await,
            Err(object_store::Error::NotFound { .. })
        ));
        read.put(&path, "read".into()).await.unwrap();
        assert_eq!(
            store.get(&path).await.unwrap().bytes().await.unwrap(),
            "read"
        );
        assert_eq!(store.head(&path).await.unwrap().size, 4);
        assert_eq!(
            store.get_ranges(&path, &[0..2, 2..4]).await.unwrap(),
            vec!["re", "ad"]
        );
        assert_eq!(
            paths(&store, Some("data/items")).await,
            vec!["data/items/x"]
        );
        assert_eq!(
            store
                .list_with_delimiter(Some(&Path::from("data/items")))
                .await
                .unwrap()
                .objects
                .len(),
            1
        );
        assert_eq!(
            paths(&store, Some("data/items/nested")).await,
            vec!["data/items/nested/x"]
        );
        // A listing above the prefixes is not covered by the credentials of a prefix.
        assert_eq!(paths(&store, None).await, vec!["data/items2/x", "data/x"]);
        assert_eq!(
            paths(&store, Some("data")).await,
            vec!["data/items2/x", "data/x"]
        );

        store
            .copy(&Path::from("data/items/x"), &Path::from("data/items/y"))
            .await
            .unwrap();
        assert_eq!(
            paths(write.as_ref(), None).await,
            vec!["data/items/x", "data/items/y"]
        );
        let deleted = store
            .delete_stream(
                futures::stream::iter([Ok(Path::from("data/items/x")), Ok(Path::from("data/x"))])
                    .boxed(),
            )
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(deleted.len(), 2);
        assert_eq!(paths(write.as_ref(), None).await, vec!["data/items/y"]);
        assert_eq!(paths(default.as_ref(), None).await, vec!["data/items2/x"]);

        // The stores of a prefix are replaced.
        store.set_route(
            Path::from("data/items"),
            "c",
            default.clone(),
            default.clone(),
        );
        assert!(!store.has_route(&Path::from("data/items"), "a"));
        assert!(store.get(&path).await.is_err());
    }

    /// Returns the object stores of a session that keep the data of each access level
    /// in memory, and the requests for object stores.
    #[expect(clippy::type_complexity)]
    fn memory_stores() -> (
        VendedObjectStores,
        Arc<InMemory>,
        Arc<InMemory>,
        Arc<Mutex<Vec<(String, StorageAccess, bool)>>>,
    ) {
        let read = Arc::new(InMemory::new());
        let write = Arc::new(InMemory::new());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let factory: VendedStoreFactory = {
            let (read, write, requests) = (read.clone(), write.clone(), requests.clone());
            Arc::new(move |request| {
                requests.lock().unwrap().push((
                    request.location.to_string(),
                    request.access,
                    request.initial.is_some(),
                ));
                Ok(match request.access {
                    StorageAccess::Read => read.clone() as Arc<dyn ObjectStore>,
                    StorageAccess::ReadWrite => write.clone(),
                })
            })
        };
        (
            VendedObjectStores::with_factory(factory),
            read,
            write,
            requests,
        )
    }

    fn registry() -> DynamicObjectStoreRegistry {
        let handle = Handle::current();
        DynamicObjectStoreRegistry::new(RuntimeHandle::new(handle.clone(), handle))
    }

    #[tokio::test]
    async fn vended_object_stores_are_registered_for_the_session_only() {
        let location = Url::parse("memory:///warehouse/items").unwrap();
        let other = Url::parse("memory:///warehouse/other/").unwrap();
        let path = Path::from("warehouse/items/_delta_log/0.json");
        let (fetch, _) = counting_fetch(None);

        let session = registry();
        let (stores, read, write, requests) = memory_stores();
        assert!(stores.is_enabled());
        assert!(!stores.contains(&location, "table"));
        stores
            .install(
                &session,
                &location,
                "table",
                s3_credentials("k", None),
                fetch.clone(),
            )
            .unwrap();
        stores
            .install(&session, &other, "other", s3_credentials("k", None), fetch)
            .unwrap();
        assert!(stores.contains(&location, "table"));
        assert!(stores.contains(&Url::parse("memory:///warehouse/items/").unwrap(), "table"));
        assert!(stores.contains(&other, "other"));
        assert!(!stores.contains(&location, "other"));
        assert!(!stores.contains(&Url::parse("memory:///warehouse").unwrap(), "table"));
        // The credentials for reading are given, and those for writing are fetched later.
        assert_eq!(
            requests.lock().unwrap()[..2],
            [
                (location.to_string(), StorageAccess::Read, true),
                (location.to_string(), StorageAccess::ReadWrite, false),
            ]
        );

        let store = session.get_store(&location).unwrap();
        store.put(&path, "commit".into()).await.unwrap();
        assert_eq!(paths(write.as_ref(), None).await, vec![path.to_string()]);
        read.put(&path, "commit".into()).await.unwrap();
        assert_eq!(
            store.get(&path).await.unwrap().bytes().await.unwrap(),
            "commit"
        );
        // The other locations of the bucket are accessed as before.
        store
            .put(&Path::from("warehouse/x"), "x".into())
            .await
            .unwrap();
        assert_eq!(paths(write.as_ref(), None).await.len(), 1);

        // Another session does not access the location with the credentials.
        let session = registry();
        let store = session.get_store(&location).unwrap();
        assert!(store.get(&path).await.is_err());

        let disabled = VendedObjectStores::new(
            RuntimeHandle::new(Handle::current(), Handle::current()),
            false,
        );
        assert!(!disabled.is_enabled());
        assert!(disabled.report_refused("table", "items", "forbidden"));
        assert!(!disabled.report_refused("table", "items", "forbidden"));
        assert!(disabled.report_refused("other", "items", "forbidden"));
    }
}
