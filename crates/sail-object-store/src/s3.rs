use std::fmt::Formatter;
use std::sync::Arc;

use async_trait::async_trait;
use aws_config::identity::IdentityCache;
use aws_config::{BehaviorVersion, SdkConfig};
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_credential_types::Credentials;
use aws_smithy_async::rt::sleep::TokioSleep;
use aws_smithy_async::time::SystemTimeSource;
use aws_smithy_runtime_api::client::identity::{
    ResolveCachedIdentity, SharedIdentityCache, SharedIdentityResolver,
};
use aws_smithy_runtime_api::client::runtime_components::{
    RuntimeComponents, RuntimeComponentsBuilder,
};
use aws_smithy_types::config_bag::ConfigBag;
use log::debug;
use object_store::aws::{
    resolve_bucket_region, AmazonS3, AmazonS3Builder, AmazonS3ConfigKey, AwsCredential,
};
use object_store::{ClientOptions, CredentialProvider, Result};
use tokio::sync::OnceCell;
use url::Url;

static DEFAULT_AWS_CONFIG: OnceCell<SdkConfig> = OnceCell::const_new();

#[derive(Debug)]
struct IdentityDataError;

impl std::fmt::Display for IdentityDataError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "IdentityDataError")
    }
}

impl std::error::Error for IdentityDataError {}

/// Provide AWS credentials for S3.
/// Cached credentials are used when available.
///
/// See also: <https://github.com/awslabs/aws-sdk-rust/discussions/923>
#[derive(Debug)]
pub(crate) struct S3CredentialProvider {
    identity_cache: SharedIdentityCache,
    identity_resolver: SharedIdentityResolver,
    runtime_components: RuntimeComponents,
    config_bag: ConfigBag,
}

impl S3CredentialProvider {
    pub fn try_new(
        provider: SharedCredentialsProvider,
        cache: SharedIdentityCache,
    ) -> Result<Self> {
        let runtime_components = RuntimeComponentsBuilder::for_tests()
            .with_time_source(Some(SystemTimeSource::new()))
            .with_sleep_impl(Some(TokioSleep::new()))
            .build()
            .map_err(|e| object_store::Error::Generic {
                store: "S3",
                source: Box::new(e),
            })?;
        Ok(Self {
            identity_cache: cache,
            identity_resolver: SharedIdentityResolver::new(provider),
            runtime_components,
            config_bag: ConfigBag::base(),
        })
    }
}

#[async_trait]
impl CredentialProvider for S3CredentialProvider {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> Result<Arc<Self::Credential>> {
        let identity = self
            .identity_cache
            .resolve_cached_identity(
                self.identity_resolver.clone(),
                &self.runtime_components,
                &self.config_bag,
            )
            .await
            .map_err(|e| object_store::Error::Generic {
                store: "S3",
                source: e,
            })?;
        let Some(creds) = identity.data::<Credentials>() else {
            return Err(object_store::Error::Generic {
                store: "S3",
                source: Box::new(IdentityDataError),
            });
        };
        Ok(Arc::new(AwsCredential {
            key_id: creds.access_key_id().to_string(),
            secret_key: creds.secret_access_key().to_string(),
            token: creds.session_token().map(|t| t.to_string()),
        }))
    }
}

pub async fn get_s3_object_store(url: &Url) -> Result<AmazonS3> {
    let bucket = url.authority();
    let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);
    debug!("Creating S3 object store for url: {url}, bucket (url authority): {bucket}");

    let config = DEFAULT_AWS_CONFIG
        .get_or_init(|| aws_config::defaults(BehaviorVersion::latest()).load())
        .await;
    debug!("Using AWS config: {config:#?}");

    if let Some(provider) = config.credentials_provider() {
        let cache = config
            .identity_cache()
            .unwrap_or_else(|| IdentityCache::lazy().build());
        let credentials = S3CredentialProvider::try_new(provider, cache)?;
        builder = builder.with_credentials(Arc::new(credentials));
    }

    let region = match config.region() {
        Some(region) if !region.as_ref().is_empty() => Some(region.to_string()),
        Some(_) | None => {
            if builder
                .get_config_value(&AmazonS3ConfigKey::Region)
                .is_none()
            {
                debug!("Resolving S3 bucket region for url: {url} bucket: {bucket}");
                Some(resolve_bucket_region(bucket, &ClientOptions::default()).await?)
            } else {
                None
            }
        }
    };

    if let Some(region) = region {
        builder = builder.with_region(region);
    }

    builder.build()
}
