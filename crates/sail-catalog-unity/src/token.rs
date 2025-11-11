// [CREDIT]: https://github.com/delta-io/delta-rs/blob/d7dea29162451fd00b9579e3d7fb546d95fb5e4a/crates/catalog-unity/src/client/token.rs

use std::future::Future;
use std::time::Instant;

use tokio::sync::Mutex;

/// A temporary authentication token with an associated expiry
#[derive(Debug, Clone)]
pub struct TemporaryToken<T> {
    /// The temporary credential
    pub token: T,
    /// The instant at which this credential is no longer valid
    /// None means the credential does not expire
    pub expiry: Option<Instant>,
}

/// Provides [`TokenCache::get_or_insert_with`] which can be used to cache a
/// [`TemporaryToken`] based on its expiry
#[derive(Debug)]
pub struct TokenCache<T> {
    cache: Mutex<Option<TemporaryToken<T>>>,
}

impl<T> Default for TokenCache<T> {
    fn default() -> Self {
        Self {
            cache: Default::default(),
        }
    }
}

impl<T: Clone + Send> TokenCache<T> {
    /// Get current token or update with a given closure
    pub async fn get_or_insert_with<F, Fut, E>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<TemporaryToken<T>, E>> + Send,
    {
        let now = Instant::now();
        let mut locked = self.cache.lock().await;

        if let Some(cached) = locked.as_ref() {
            match cached.expiry {
                Some(ttl)
                    if ttl
                        .checked_duration_since(now)
                        .unwrap_or_default()
                        .as_secs()
                        > 300 =>
                {
                    return Ok(cached.token.clone());
                }
                None => return Ok(cached.token.clone()),
                _ => (),
            }
        }

        let cached = f().await?;
        let token = cached.token.clone();
        *locked = Some(cached);

        Ok(token)
    }
}
