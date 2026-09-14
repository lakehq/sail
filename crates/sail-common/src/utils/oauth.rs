use std::time::{Duration, Instant};

use serde::Deserialize;
use tokio::sync::Mutex;

use crate::storage::{OAuth2ClientCredentials, StorageSecret};

#[derive(Debug, Clone, thiserror::Error)]
pub enum OAuth2Error {
    #[error("Invalid OAuth2 client credentials configuration")]
    Configuration,
    #[error("OAuth2 token request failed")]
    Transport,
    #[error("OAuth2 token request returned HTTP {0}")]
    Status(u16),
    #[error("Invalid OAuth2 token response")]
    Response,
}

struct AccessToken {
    value: StorageSecret,
    refresh_at: Option<Instant>,
}

/// Shares renewable catalog authentication without serializing an HTTP client or cached token.
pub struct OAuth2Client {
    source: OAuth2ClientCredentials,
    client: reqwest::Client,
    token: Mutex<Option<AccessToken>>,
}

impl std::fmt::Debug for OAuth2Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OAuth2Client [REDACTED]")
    }
}

impl OAuth2Client {
    pub fn new(source: OAuth2ClientCredentials) -> Result<Self, OAuth2Error> {
        let endpoint =
            reqwest::Url::parse(&source.endpoint).map_err(|_| OAuth2Error::Configuration)?;
        if !matches!(endpoint.scheme(), "http" | "https")
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.fragment().is_some()
            || source.client_id.is_empty()
            || source.client_secret.expose().is_empty()
        {
            return Err(OAuth2Error::Configuration);
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|_| OAuth2Error::Configuration)?;
        Ok(Self {
            source,
            client,
            token: Mutex::new(None),
        })
    }

    pub fn source(&self) -> &OAuth2ClientCredentials {
        &self.source
    }

    pub async fn token(&self) -> Result<StorageSecret, OAuth2Error> {
        let mut cached = self.token.lock().await;
        if let Some(token) = cached.as_ref()
            && token
                .refresh_at
                .is_none_or(|expiry| Instant::now() < expiry)
        {
            return Ok(token.value.clone());
        }
        let started = Instant::now();
        let mut form = vec![
            ("grant_type", "client_credentials"),
            ("client_id", self.source.client_id.as_str()),
            ("client_secret", self.source.client_secret.expose()),
        ];
        if let Some(scope) = &self.source.scope {
            form.push(("scope", scope.as_str()));
        }
        let response = self
            .client
            .post(&self.source.endpoint)
            .form(&form)
            .send()
            .await
            .map_err(|_| OAuth2Error::Transport)?;
        if !response.status().is_success() {
            return Err(OAuth2Error::Status(response.status().as_u16()));
        }
        #[derive(Deserialize)]
        struct TokenResponse {
            access_token: String,
            token_type: String,
            expires_in: Option<u64>,
        }
        let response: TokenResponse = response.json().await.map_err(|_| OAuth2Error::Response)?;
        if response.access_token.is_empty()
            || !response.token_type.eq_ignore_ascii_case("bearer")
            || response.expires_in == Some(0)
        {
            return Err(OAuth2Error::Response);
        }
        reqwest::header::HeaderValue::from_str(&format!("Bearer {}", response.access_token))
            .map_err(|_| OAuth2Error::Response)?;
        let refresh_at = response
            .expires_in
            .map(|seconds| {
                let lifetime = Duration::from_secs(seconds);
                let expiry = started.checked_add(lifetime).ok_or(OAuth2Error::Response)?;
                if Instant::now() >= expiry {
                    return Err(OAuth2Error::Response);
                }
                Ok(expiry - (lifetime / 5).min(Duration::from_secs(60)))
            })
            .transpose()?;
        let value = StorageSecret::new(response.access_token);
        *cached = Some(AccessToken {
            value: value.clone(),
            refresh_at,
        });
        Ok(value)
    }

    /// A delayed rejection must not discard a token already replaced by another request.
    pub async fn reject(&self, rejected: &str) {
        let mut cached = self.token.lock().await;
        if cached
            .as_ref()
            .is_some_and(|token| token.value.expose() == rejected)
        {
            *cached = None;
        }
    }
}
