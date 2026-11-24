// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    Result,
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
use crate::s3::{BucketRegions, get_s3_object_store_with_credentials};

/// A credential is replaced when it expires within this time.
const REFRESH_MARGIN: Duration = Duration::from_secs(300);
/// A credential that is not expired yet is replaced at most once within this time,
/// and a failed replacement is repeated after this time.
const REFRESH_INTERVAL: Duration = Duration::from_secs(30);
/// A credential without an expiration time is replaced after this time.
const DEFAULT_LIFETIME: Duration = Duration::from_secs(1800);
/// When no credential is valid and the credential cannot be fetched,
/// the requests within this time fail without another fetch.
const FAILURE_BACKOFF: Duration = Duration::from_secs(5);
/// A fetch of the credential that does not finish within this time fails.
const FETCH_TIMEOUT: Duration = Duration::from_secs(60);
/// The number of locations that are grouped by object store when they are deleted.
const DELETE_GROUP_SIZE: usize = 1000;

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
    fetched_at: tokio::time::Instant,
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

    /// Whether the credential is valid but should be replaced.
    /// A credential without an expiration time is replaced regularly,
    /// since the storage may stop accepting it at a time that is not known.
    fn needs_refresh(&self) -> bool {
        let age = self.fetched_at.elapsed();
        age >= REFRESH_INTERVAL
            && self
                .remaining()
                .map_or(age >= DEFAULT_LIFETIME, |x| x <= REFRESH_MARGIN)
    }
}

struct CredentialState<C> {
    current: Option<CurrentCredential<C>>,
    /// The time and the error of the last fetch, if it failed.
    failure: Option<(tokio::time::Instant, String)>,
}

struct CredentialSource<C> {
    access: StorageAccess,
    fetch: StorageCredentialFetch,
    state: Mutex<CredentialState<C>>,
    /// Held while the credential is fetched, so that it is fetched once at a time.
    refresh: Arc<tokio::sync::Mutex<()>>,
}

impl<C: VendedCredential> CredentialSource<C> {
    fn state(&self) -> MutexGuard<'_, CredentialState<C>> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn current(
        credentials: TableStorageCredentials,
    ) -> std::result::Result<CurrentCredential<C>, String> {
        let credential = C::try_from_credential(credentials.credential)
            .ok_or_else(|| "the storage credential cannot be used for the storage".to_string())?;
        Ok(CurrentCredential {
            credential: Arc::new(credential),
            expires_at_ms: credentials.expires_at_ms,
            fetched_at: tokio::time::Instant::now(),
        })
    }

    /// Fetches the credential. The state is not locked while the credential is fetched.
    /// A fetch that times out or returns an expired credential fails.
    async fn fetch(&self) -> Result<Arc<C>> {
        let fetched = match tokio::time::timeout(FETCH_TIMEOUT, (self.fetch)(self.access)).await {
            Ok(Ok(credentials)) => Self::current(credentials).and_then(|current| {
                if current.is_expired() {
                    Err("the storage credential has expired when it is issued".to_string())
                } else {
                    Ok(current)
                }
            }),
            Ok(Err(message)) => Err(message),
            Err(_) => Err(format!(
                "the storage credential is not issued within {} seconds",
                FETCH_TIMEOUT.as_secs()
            )),
        };
        let mut state = self.state();
        match fetched {
            Ok(fetched) => {
                let credential = Arc::clone(&fetched.credential);
                state.current = Some(fetched);
                state.failure = None;
                Ok(credential)
            }
            Err(message) => {
                state.failure = Some((tokio::time::Instant::now(), message.clone()));
                Err(credential_error(message))
            }
        }
    }
}

/// Provides the storage credential issued for an access level to an object store.
/// The credential is fetched when it is first used and before it expires.
pub struct VendedCredentialProvider<C> {
    source: Arc<CredentialSource<C>>,
}

impl<C: VendedCredential> VendedCredentialProvider<C> {
    pub fn try_new(
        access: StorageAccess,
        initial: Option<TableStorageCredentials>,
        fetch: StorageCredentialFetch,
    ) -> Result<Self> {
        let current = initial
            .map(CredentialSource::current)
            .transpose()
            .map_err(credential_error)?;
        Ok(Self {
            source: Arc::new(CredentialSource {
                access,
                fetch,
                state: Mutex::new(CredentialState {
                    current,
                    failure: None,
                }),
                refresh: Arc::new(tokio::sync::Mutex::new(())),
            }),
        })
    }
}

impl<C> fmt::Debug for VendedCredentialProvider<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VendedCredentialProvider")
            .field("access", &self.source.access)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl<C: VendedCredential> CredentialProvider for VendedCredentialProvider<C> {
    type Credential = C;

    async fn get_credential(&self) -> Result<Arc<C>> {
        let source = &self.source;
        {
            let state = source.state();
            if let Some(current) = state.current.as_ref()
                && !current.is_expired()
            {
                // A valid credential is replaced in the background, so that the requests
                // to the storage do not wait for the catalog.
                let refresh = current.needs_refresh()
                    && state
                        .failure
                        .as_ref()
                        .is_none_or(|(at, _)| at.elapsed() >= REFRESH_INTERVAL);
                let credential = Arc::clone(&current.credential);
                drop(state);
                if refresh && let Ok(guard) = Arc::clone(&source.refresh).try_lock_owned() {
                    let source = Arc::clone(source);
                    tokio::spawn(async move {
                        let _guard = guard;
                        if let Err(e) = source.fetch().await {
                            warn!("failed to replace a storage credential before it expires: {e}");
                        }
                    });
                }
                return Ok(credential);
            }
        }
        // No credential is valid, so the requests wait for the one that fetches it.
        let _guard = source.refresh.lock().await;
        {
            let state = source.state();
            if let Some(current) = state.current.as_ref()
                && !current.is_expired()
            {
                return Ok(Arc::clone(&current.credential));
            }
            if let Some((at, message)) = state.failure.as_ref()
                && at.elapsed() < FAILURE_BACKOFF
            {
                return Err(credential_error(message.clone()));
            }
        }
        source.fetch().await
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
    regions: &Arc<BucketRegions>,
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
                    let regions = Arc::clone(regions);
                    Arc::new(LazyObjectStore::new(move || {
                        let url = url.clone();
                        let provider = Arc::clone(&provider);
                        let regions = Arc::clone(&regions);
                        async move {
                            get_s3_object_store_with_credentials(
                                &url,
                                Some(provider),
                                Some(regions.as_ref()),
                            )
                            .await
                        }
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
    /// For object stores that do not use credentials issued for the owner, whether an
    /// access to the storage failed in a way that missing access can cause.
    not_issued_failed: Option<Arc<AtomicBool>>,
    read: Arc<dyn ObjectStore>,
    write: Arc<dyn ObjectStore>,
}

/// Returns the error of an object store as a message with the reason why the catalog
/// issued no storage credentials for the location.
///
/// The message has the causes of the error and is the innermost error, since the error
/// that a client gets for a failed statement can be the innermost error only.
fn not_issued_error(
    source: Box<dyn std::error::Error + Send + Sync>,
    reason: &str,
) -> Box<dyn std::error::Error + Send + Sync> {
    let mut message = source.to_string();
    let mut cause = source.source();
    while let Some(error) = cause {
        let text = error.to_string();
        if !message.contains(&text) {
            message.push_str(": ");
            message.push_str(&text);
        }
        cause = error.source();
    }
    format!("{message} ({reason})").into()
}

/// An object store that accesses a location for which the catalog issued no storage
/// credentials with the object store of the environment, and adds the reason of the
/// catalog to the errors that can be caused by missing access to the storage.
/// The kind of the error is kept. Other errors, such as an object that is not found,
/// are returned as they are.
#[derive(Debug)]
struct NotIssuedObjectStore {
    inner: Arc<dyn ObjectStore>,
    reason: Arc<str>,
    /// Whether such an error occurred, after which the catalog is asked for credentials
    /// again when the table is next used, since the environment may have no access.
    failed: Arc<AtomicBool>,
}

impl NotIssuedObjectStore {
    fn explain(
        reason: &Arc<str>,
        failed: &AtomicBool,
        error: object_store::Error,
    ) -> object_store::Error {
        use object_store::Error;

        let explain = |source| {
            failed.store(true, Ordering::Relaxed);
            not_issued_error(source, reason)
        };
        match error {
            Error::Generic { store, source } => Error::Generic {
                store,
                source: explain(source),
            },
            Error::Unauthenticated { path, source } => Error::Unauthenticated {
                path,
                source: explain(source),
            },
            Error::PermissionDenied { path, source } => Error::PermissionDenied {
                path,
                source: explain(source),
            },
            error => error,
        }
    }

    fn explained<T>(&self, result: Result<T>) -> Result<T> {
        result.map_err(|e| Self::explain(&self.reason, &self.failed, e))
    }

    fn explained_stream<T: 'static>(
        &self,
        stream: BoxStream<'static, Result<T>>,
    ) -> BoxStream<'static, Result<T>> {
        let reason = Arc::clone(&self.reason);
        let failed = Arc::clone(&self.failed);
        stream
            .map(move |result| result.map_err(|e| Self::explain(&reason, &failed, e)))
            .boxed()
    }
}

impl fmt::Display for NotIssuedObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

#[async_trait::async_trait]
#[warn(clippy::missing_trait_methods)]
impl ObjectStore for NotIssuedObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.explained(self.inner.put_opts(location, payload, opts).await)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.explained(self.inner.put_multipart_opts(location, opts).await)
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.explained(self.inner.get_opts(location, options).await)
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.explained(self.inner.get_ranges(location, ranges).await)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.explained_stream(self.inner.delete_stream(locations))
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.explained_stream(self.inner.list(prefix))
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.explained_stream(self.inner.list_with_offset(prefix, offset))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.explained(self.inner.list_with_delimiter(prefix).await)
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.explained(self.inner.copy_opts(from, to, options).await)
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.explained(self.inner.rename_opts(from, to, options).await)
    }
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
        self.push_route(Route {
            prefix,
            owner: owner.to_string(),
            not_issued_failed: None,
            read,
            write,
        });
    }

    /// Accesses the path prefix with the default object store, and adds the reason
    /// why the catalog issued no storage credentials for the owner to the errors.
    fn set_not_issued_route(&self, prefix: Path, owner: &str, reason: &str) {
        let failed = Arc::new(AtomicBool::new(false));
        let store: Arc<dyn ObjectStore> = Arc::new(NotIssuedObjectStore {
            inner: Arc::clone(&self.default),
            reason: Arc::from(reason),
            failed: Arc::clone(&failed),
        });
        self.push_route(Route {
            prefix,
            owner: owner.to_string(),
            not_issued_failed: Some(failed),
            read: Arc::clone(&store),
            write: store,
        });
    }

    fn push_route(&self, route: Route) {
        let mut routes = self.routes.write().unwrap_or_else(|e| e.into_inner());
        routes.retain(|x| x.prefix != route.prefix);
        routes.push(route);
    }

    /// Removes the object stores registered for the owner.
    pub fn remove_routes(&self, owner: &str) {
        self.routes
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|route| route.owner != owner);
    }

    /// Whether the path prefix is accessed with credentials issued for the owner.
    fn has_route(&self, prefix: &Path, owner: &str) -> bool {
        self.routes
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .any(|route| {
                route.not_issued_failed.is_none() && &route.prefix == prefix && route.owner == owner
            })
    }

    /// Whether an access to the path prefix failed without credentials issued for the
    /// owner, in a way that missing access can cause.
    fn not_issued_failed(&self, prefix: &Path, owner: &str) -> bool {
        self.routes
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .any(|route| {
                &route.prefix == prefix
                    && route.owner == owner
                    && route
                        .not_issued_failed
                        .as_ref()
                        .is_some_and(|failed| failed.load(Ordering::Relaxed))
            })
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
        let routes = self.routes();
        if routes.is_empty() {
            return self.default.delete_stream(locations);
        }
        // The locations are deleted in groups, each with the batched deletion
        // of the store for writing the locations.
        let default = Arc::clone(&self.default);
        locations
            .chunks(DELETE_GROUP_SIZE)
            .flat_map(move |locations| {
                let mut errors = Vec::new();
                let mut groups: Vec<(Arc<dyn ObjectStore>, Vec<Path>)> = Vec::new();
                for location in locations {
                    let location = match location {
                        Ok(location) => location,
                        Err(e) => {
                            errors.push(Err(e));
                            continue;
                        }
                    };
                    let store =
                        select_route(&routes, &location).map_or(&default, |route| &route.write);
                    match groups.iter_mut().find(|(x, _)| Arc::ptr_eq(x, store)) {
                        Some((_, group)) => group.push(location),
                        None => groups.push((Arc::clone(store), vec![location])),
                    }
                }
                let deleted = groups.into_iter().map(|(store, group)| {
                    store.delete_stream(futures::stream::iter(group.into_iter().map(Ok)).boxed())
                });
                futures::stream::iter(errors).chain(futures::stream::iter(deleted).flatten())
            })
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
    /// The time until which no credentials are requested for a storage location
    /// and an owner, since they were not issued when they were last requested.
    not_issued: Mutex<HashMap<(String, String), tokio::time::Instant>>,
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
        // The region of a bucket is discovered once for all tables in the bucket.
        let regions = Arc::new(BucketRegions::default());
        let mut stores = Self::with_factory(Arc::new(move |request| {
            create_vended_store(&runtime, &regions, request)
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
            not_issued: Mutex::new(HashMap::new()),
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

    /// Remembers for the given time that no credentials are issued for the storage
    /// location and the owner.
    pub fn set_not_issued(&self, location: &Url, owner: &str, duration: Duration) {
        let now = tokio::time::Instant::now();
        let mut not_issued = self.not_issued.lock().unwrap_or_else(|e| e.into_inner());
        not_issued.retain(|_, until| *until > now);
        not_issued.insert(
            (owner.to_string(), location.as_str().to_string()),
            now + duration,
        );
    }

    /// Whether it is remembered that no credentials are issued for the storage
    /// location and the owner.
    ///
    /// It is forgotten once an access to the storage location with the credentials of
    /// the environment fails in a way that missing access can cause, so that the catalog
    /// is asked again when the table is next used.
    pub fn is_not_issued(&self, location: &Url, owner: &str) -> bool {
        let remembered = self
            .not_issued
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&(owner.to_string(), location.as_str().to_string()))
            .is_some_and(|until| *until > tokio::time::Instant::now());
        if !remembered {
            return false;
        }
        let Ok(prefix) = Self::prefix(location) else {
            return true;
        };
        !self
            .stores
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&Self::key(location))
            .is_some_and(|store| store.not_issued_failed(&prefix, owner))
    }

    /// Stops accessing storage locations with the credentials issued for the owner,
    /// and forgets what is remembered about the owner.
    pub fn remove(&self, owner: &str) {
        let stores = self.stores.lock().unwrap_or_else(|e| e.into_inner());
        for store in stores.values() {
            store.remove_routes(owner);
        }
        drop(stores);
        self.not_issued
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|(x, _), _| x != owner);
        self.reported_refused
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(owner);
    }

    /// Accesses the storage location with the credentials issued for the owner
    /// in this session from now on, instead of the locations registered for the owner
    /// before. The credentials for reading are given, and the credentials for writing
    /// are fetched when the location is first written.
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
        // A credential that the catalog issued for writing is also the first credential
        // for writing, so that the catalog is not asked again before it expires.
        let initial_write = (initial.access == StorageAccess::ReadWrite).then(|| initial.clone());
        let read = (self.factory)(VendedStoreRequest {
            location,
            access: StorageAccess::Read,
            kind: &kind,
            initial: Some(initial),
            fetch: Arc::clone(&fetch),
        })?;
        // The object store for writing is created when the location is first written.
        let write: Arc<dyn ObjectStore> = {
            let factory = Arc::clone(&self.factory);
            let location = location.clone();
            Arc::new(LazyObjectStore::new(move || {
                let store = factory(VendedStoreRequest {
                    location: &location,
                    access: StorageAccess::ReadWrite,
                    kind: &kind,
                    initial: initial_write.clone(),
                    fetch: Arc::clone(&fetch),
                });
                async move { store }
            }))
        };
        self.routing_store(registry, location, owner)?
            .set_route(prefix, owner, read, write);
        Ok(())
    }

    /// Keeps accessing the storage location with the object store of the environment
    /// after the catalog refused or failed to issue credentials for the owner, and adds
    /// the reason to the errors of the storage that missing access can cause, so that
    /// the reason of the catalog is not lost when the environment has no access either.
    pub fn install_not_issued(
        &self,
        registry: &dyn ObjectStoreRegistry,
        location: &Url,
        owner: &str,
        reason: &str,
    ) -> DataFusionResult<()> {
        let prefix = Self::prefix(location)?;
        self.routing_store(registry, location, owner)?
            .set_not_issued_route(prefix, owner, reason);
        Ok(())
    }

    /// Returns the object store that is registered for the storage location in the
    /// session, without the routes of the owner.
    fn routing_store(
        &self,
        registry: &dyn ObjectStoreRegistry,
        location: &Url,
        owner: &str,
    ) -> DataFusionResult<Arc<PrefixRoutingObjectStore>> {
        let mut stores = self.stores.lock().unwrap_or_else(|e| e.into_inner());
        // The table may have been dropped and created again at another location.
        for store in stores.values() {
            store.remove_routes(owner);
        }
        Ok(match stores.get(&Self::key(location)) {
            Some(store) => Arc::clone(store),
            None => {
                let store = Arc::new(PrefixRoutingObjectStore::new(registry.get_store(location)?));
                registry.register_store(location, Arc::clone(&store) as Arc<dyn ObjectStore>);
                stores.insert(Self::key(location), Arc::clone(&store));
                store
            }
        })
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use futures::{TryFutureExt, TryStreamExt};
    use object_store::ObjectStoreExt;
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
            access: StorageAccess::Read,
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

    async fn key_id(provider: &VendedCredentialProvider<AwsCredential>) -> String {
        provider.get_credential().await.unwrap().key_id.clone()
    }

    /// Lets the given time pass for the credentials fetched so far.
    /// The tests that call this run with the time paused.
    async fn age(by: Duration) {
        tokio::time::advance(by).await;
    }

    /// Waits until the provider no longer fetches a credential in the background.
    async fn wait_for_refresh(provider: &VendedCredentialProvider<AwsCredential>) {
        drop(provider.source.refresh.lock().await);
    }

    #[tokio::test(start_paused = true)]
    async fn credential_provider_fetches_missing_and_expiring_credentials() {
        // A credential that does not expire soon is kept.
        for expires_at_ms in [None, Some(now_ms() + 3_600_000)] {
            let (fetch, requests) = counting_fetch(None);
            let provider = VendedCredentialProvider::<AwsCredential>::try_new(
                StorageAccess::Read,
                Some(s3_credentials("initial", expires_at_ms)),
                fetch,
            )
            .unwrap();
            age(REFRESH_INTERVAL).await;
            assert_eq!(key_id(&provider).await, "initial");
            wait_for_refresh(&provider).await;
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

        // An expired credential is replaced before it is used.
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

        // A credential that expires soon is replaced once it is not fetched just now,
        // and is used until it is replaced.
        let (fetch, requests) = counting_fetch(Some(now_ms() + 3_600_000));
        let provider = VendedCredentialProvider::<AwsCredential>::try_new(
            StorageAccess::Read,
            Some(s3_credentials("initial", Some(now_ms() + 60_000))),
            fetch,
        )
        .unwrap();
        assert_eq!(key_id(&provider).await, "initial");
        wait_for_refresh(&provider).await;
        assert!(requests.lock().unwrap().is_empty());
        age(REFRESH_INTERVAL).await;
        assert_eq!(key_id(&provider).await, "initial");
        wait_for_refresh(&provider).await;
        assert_eq!(key_id(&provider).await, "Read-1");
        assert_eq!(*requests.lock().unwrap(), vec![StorageAccess::Read]);

        // A credential without an expiration time is replaced after a while.
        let (fetch, requests) = counting_fetch(None);
        let provider = VendedCredentialProvider::<AwsCredential>::try_new(
            StorageAccess::Read,
            Some(s3_credentials("initial", None)),
            fetch,
        )
        .unwrap();
        age(DEFAULT_LIFETIME).await;
        assert_eq!(key_id(&provider).await, "initial");
        wait_for_refresh(&provider).await;
        assert_eq!(key_id(&provider).await, "Read-1");
        assert_eq!(requests.lock().unwrap().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn credential_provider_does_not_wait_for_a_stalled_catalog() {
        // The catalog accepts the request for a new credential but does not answer.
        let requests = Arc::new(Mutex::new(0));
        let fetch: StorageCredentialFetch = {
            let requests = Arc::clone(&requests);
            Arc::new(move |_| {
                *requests.lock().unwrap() += 1;
                Box::pin(futures::future::pending())
            })
        };
        let provider = VendedCredentialProvider::<AwsCredential>::try_new(
            StorageAccess::Read,
            Some(s3_credentials("initial", Some(now_ms() + 60_000))),
            fetch,
        )
        .unwrap();
        age(REFRESH_INTERVAL).await;
        // The valid credential is used while the new one is fetched once.
        for _ in 0..3 {
            let key = tokio::time::timeout(Duration::from_secs(5), key_id(&provider))
                .await
                .unwrap();
            assert_eq!(key, "initial");
            tokio::task::yield_now().await;
        }
        assert_eq!(*requests.lock().unwrap(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn credential_provider_fails_when_the_catalog_stalls_without_a_valid_credential() {
        // The catalog accepts the request for a new credential but does not answer,
        // and the current credential has expired.
        let requests = Arc::new(Mutex::new(0));
        let fetch: StorageCredentialFetch = {
            let requests = Arc::clone(&requests);
            Arc::new(move |_| {
                *requests.lock().unwrap() += 1;
                Box::pin(futures::future::pending())
            })
        };
        let provider = VendedCredentialProvider::<AwsCredential>::try_new(
            StorageAccess::Read,
            Some(s3_credentials("initial", Some(now_ms() - 1))),
            fetch,
        )
        .unwrap();
        // The request fails once the fetch times out, and the requests right after
        // the failure do not wait for the catalog again.
        for _ in 0..2 {
            let error = provider.get_credential().await.unwrap_err();
            assert!(error.to_string().contains("not issued within"), "{error}");
        }
        assert_eq!(*requests.lock().unwrap(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn credential_provider_fails_when_the_catalog_issues_an_expired_credential() {
        let (fetch, requests) = counting_fetch(Some(now_ms() - 1));
        let provider =
            VendedCredentialProvider::<AwsCredential>::try_new(StorageAccess::Read, None, fetch)
                .unwrap();
        // The requests right after the failure do not ask the catalog again.
        for _ in 0..3 {
            let error = provider.get_credential().await.unwrap_err();
            assert!(error.to_string().contains("expired"), "{error}");
        }
        assert_eq!(requests.lock().unwrap().len(), 1);
        age(FAILURE_BACKOFF).await;
        assert!(provider.get_credential().await.is_err());
        assert_eq!(requests.lock().unwrap().len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn credential_provider_reports_fetch_errors_without_secrets() {
        // The current credential is used while it is valid, and the failed fetch
        // is not repeated right away.
        let requests = Arc::new(Mutex::new(0));
        let fetch: StorageCredentialFetch = {
            let requests = Arc::clone(&requests);
            Arc::new(move |_| {
                *requests.lock().unwrap() += 1;
                Box::pin(async { Err("the catalog refused the request".to_string()) })
            })
        };
        let provider = VendedCredentialProvider::<AwsCredential>::try_new(
            StorageAccess::Read,
            Some(s3_credentials("initial", Some(now_ms() + 60_000))),
            fetch.clone(),
        )
        .unwrap();
        age(REFRESH_INTERVAL).await;
        assert_eq!(key_id(&provider).await, "initial");
        wait_for_refresh(&provider).await;
        assert_eq!(key_id(&provider).await, "initial");
        wait_for_refresh(&provider).await;
        assert_eq!(*requests.lock().unwrap(), 1);

        // Without a valid credential, the requests fail with the error of the catalog,
        // and the requests right after a failure do not ask the catalog again.
        *requests.lock().unwrap() = 0;
        let provider = VendedCredentialProvider::<AwsCredential>::try_new(
            StorageAccess::Read,
            Some(s3_credentials("initial", Some(now_ms() - 1))),
            fetch,
        )
        .unwrap();
        for _ in 0..2 {
            let error = provider.get_credential().await.unwrap_err();
            let message = format!("{error} {error:?} {provider:?}");
            assert!(
                message.contains("the catalog refused the request"),
                "{message}"
            );
            for secret in ["initial", "secret-value", "token-value"] {
                assert!(!message.contains(secret), "{message}");
            }
        }
        assert_eq!(*requests.lock().unwrap(), 1);
        age(FAILURE_BACKOFF).await;
        assert!(provider.get_credential().await.is_err());
        assert_eq!(*requests.lock().unwrap(), 2);

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

    #[test]
    fn not_issued_errors_keep_the_kind_and_are_the_innermost_error() {
        let reason: Arc<str> = Arc::from("the catalog refused: no access");
        let source = std::io::Error::other(std::io::Error::other("invalid token"));
        let failed = AtomicBool::new(false);
        let error = NotIssuedObjectStore::explain(
            &reason,
            &failed,
            object_store::Error::Unauthenticated {
                path: "a/b".to_string(),
                source: Box::new(source),
            },
        );
        assert!(matches!(error, object_store::Error::Unauthenticated { .. }));
        let mut innermost: &dyn std::error::Error = &error;
        while let Some(source) = innermost.source() {
            innermost = source;
        }
        let message = innermost.to_string();
        assert!(
            message.contains("invalid token")
                && message.ends_with("(the catalog refused: no access)"),
            "{message}"
        );

        assert!(failed.swap(false, Ordering::Relaxed));
        let error = NotIssuedObjectStore::explain(
            &reason,
            &failed,
            object_store::Error::NotFound {
                path: "a/b".to_string(),
                source: "missing".into(),
            },
        );
        assert!(!error.to_string().contains("refused"), "{error}");
        assert!(!failed.load(Ordering::Relaxed));
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

    /// An object store in memory that records the number of locations
    /// in each batched deletion.
    #[derive(Debug, Default)]
    struct DeleteCountingStore {
        inner: InMemory,
        deletes: Arc<Mutex<Vec<usize>>>,
    }

    impl fmt::Display for DeleteCountingStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "DeleteCountingStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for DeleteCountingStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, Result<Path>>,
        ) -> BoxStream<'static, Result<Path>> {
            let inner = self.inner.clone();
            let deletes = Arc::clone(&self.deletes);
            locations
                .try_collect::<Vec<_>>()
                .map_ok(move |locations| {
                    deletes.lock().unwrap().push(locations.len());
                    inner
                        .delete_stream(futures::stream::iter(locations.into_iter().map(Ok)).boxed())
                })
                .try_flatten_stream()
                .boxed()
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn routing_store_deletes_locations_in_batches() {
        let default = Arc::new(DeleteCountingStore::default());
        let write = Arc::new(DeleteCountingStore::default());
        let store = PrefixRoutingObjectStore::new(default.clone());
        let locations =
            |names: &[&str]| names.iter().map(|x| Ok(Path::from(*x))).collect::<Vec<_>>();
        let delete = |locations: Vec<Result<Path>>| {
            store
                .delete_stream(futures::stream::iter(locations).boxed())
                .try_collect::<Vec<_>>()
        };

        // Without routes, the stream is deleted by the default store.
        assert_eq!(
            delete(locations(&["a/1", "a/2", "a/3"]))
                .await
                .unwrap()
                .len(),
            3
        );
        assert_eq!(*default.deletes.lock().unwrap(), vec![3]);

        // With routes, the locations are grouped by store.
        store.set_route(Path::from("t"), "a", write.clone(), write.clone());
        default.deletes.lock().unwrap().clear();
        let many = (0..DELETE_GROUP_SIZE + 2)
            .map(|i| {
                if i % 2 == 0 {
                    format!("t/{i}")
                } else {
                    format!("a/{i}")
                }
            })
            .collect::<Vec<_>>();
        let many = many.iter().map(|x| x.as_str()).collect::<Vec<_>>();
        let deleted = delete(locations(&many)).await.unwrap();
        assert_eq!(deleted.len(), DELETE_GROUP_SIZE + 2);
        assert_eq!(
            *default.deletes.lock().unwrap(),
            vec![DELETE_GROUP_SIZE / 2, 1]
        );
        assert_eq!(
            *write.deletes.lock().unwrap(),
            vec![DELETE_GROUP_SIZE / 2, 1]
        );

        // The errors of the input are returned.
        let mut input = locations(&["t/x"]);
        input.push(Err(object_store::Error::NotImplemented {
            operation: "test".to_string(),
            implementer: "test".to_string(),
        }));
        assert!(delete(input).await.is_err());
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
        // The catalog issued a credential for writing the other location.
        let read_write = TableStorageCredentials {
            access: StorageAccess::ReadWrite,
            ..s3_credentials("k", None)
        };
        stores
            .install(&session, &other, "other", read_write, fetch)
            .unwrap();
        assert!(stores.contains(&location, "table"));
        assert!(stores.contains(&Url::parse("memory:///warehouse/items/").unwrap(), "table"));
        assert!(stores.contains(&other, "other"));
        assert!(!stores.contains(&location, "other"));
        assert!(!stores.contains(&Url::parse("memory:///warehouse").unwrap(), "table"));
        // The credentials for reading are given, and the object store for writing
        // is created when the location is first written.
        assert_eq!(
            *requests.lock().unwrap(),
            [
                (location.to_string(), StorageAccess::Read, true),
                (other.to_string(), StorageAccess::Read, true),
            ]
        );

        let store = session.get_store(&location).unwrap();
        store.put(&path, "commit".into()).await.unwrap();
        assert_eq!(
            requests.lock().unwrap()[2..],
            [(location.to_string(), StorageAccess::ReadWrite, false)]
        );
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
        // A credential that was issued for writing is the first credential for writing.
        store
            .put(&Path::from("warehouse/other/x"), "x".into())
            .await
            .unwrap();
        assert_eq!(
            requests.lock().unwrap()[3..],
            [(other.to_string(), StorageAccess::ReadWrite, true)]
        );

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

    #[tokio::test]
    async fn vended_object_stores_follow_the_tables_of_the_owners() {
        let tab = Url::parse("memory:///b/tab").unwrap();
        let table = Url::parse("memory:///b/table").unwrap();
        let session = registry();
        let default = session.get_store(&tab).unwrap();
        let (stores, _, write, _) = memory_stores();
        let (fetch, _) = counting_fetch(None);
        stores
            .install(
                &session,
                &tab,
                "t",
                s3_credentials("k", None),
                fetch.clone(),
            )
            .unwrap();
        // A prefix does not cover a location whose last segment only starts with it.
        let store = session.get_store(&table).unwrap();
        for path in ["b/tab/x", "b/table/x"] {
            store.put(&Path::from(path), "x".into()).await.unwrap();
        }
        assert_eq!(paths(write.as_ref(), None).await, vec!["b/tab/x"]);
        assert_eq!(paths(default.as_ref(), None).await, vec!["b/table/x"]);
        assert!(!stores.contains(&table, "t"));

        // The table is created again at another location.
        stores
            .install(&session, &table, "t", s3_credentials("k", None), fetch)
            .unwrap();
        assert!(stores.contains(&table, "t"));
        assert!(!stores.contains(&tab, "t"));

        // The table is dropped.
        stores.set_not_issued(&tab, "t", Duration::from_secs(60));
        stores.set_not_issued(&tab, "u", Duration::from_secs(60));
        assert!(stores.is_not_issued(&tab, "t"));
        assert!(!stores.is_not_issued(&table, "t"));
        stores.remove("t");
        assert!(!stores.contains(&table, "t"));
        assert!(!stores.is_not_issued(&tab, "t"));
        assert!(stores.is_not_issued(&tab, "u"));
        store
            .put(&Path::from("b/table/y"), "y".into())
            .await
            .unwrap();
        assert_eq!(
            paths(default.as_ref(), None).await,
            vec!["b/table/x", "b/table/y"]
        );

        // What is remembered expires.
        stores.set_not_issued(&tab, "u", Duration::ZERO);
        assert!(!stores.is_not_issued(&tab, "u"));
    }
}
