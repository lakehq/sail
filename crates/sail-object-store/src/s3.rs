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
use datafusion_common::plan_datafusion_err;
use log::debug;
use object_store::aws::{
    resolve_bucket_region, AmazonS3, AmazonS3Builder, AmazonS3ConfigKey, AwsCredential,
};
use object_store::{ClientOptions, CredentialProvider};
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
    ) -> object_store::Result<Self> {
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

    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
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

pub async fn get_s3_object_store(url: &Url) -> object_store::Result<AmazonS3> {
    debug!("Creating S3 object store for url: {url}");
    let mut builder = AmazonS3Builder::from_env();
    let config = DEFAULT_AWS_CONFIG
        .get_or_init(|| aws_config::defaults(BehaviorVersion::latest()).load())
        .await;

    if let Some(provider) = config.credentials_provider() {
        let cache = config
            .identity_cache()
            .unwrap_or_else(|| IdentityCache::lazy().build());
        let credentials = S3CredentialProvider::try_new(provider, cache)?;
        builder = builder.with_credentials(Arc::new(credentials));
    }

    let mut builder = parse_s3_url(builder, url)?;
    let bucket = builder
        .get_config_value(&AmazonS3ConfigKey::Bucket)
        .ok_or_else(|| object_store::Error::Generic {
            store: "S3",
            source: Box::new(plan_datafusion_err!(
                "S3 bucket name must be specified in url: {url}"
            )),
        })?;
    let region = builder.get_config_value(&AmazonS3ConfigKey::Region);
    debug!("S3 object store bucket: {bucket} region from builder: {region:?}");

    if region.is_none_or(|r| r.is_empty()) {
        let region = match config.region() {
            Some(region) if !region.as_ref().is_empty() => region.to_string(),
            _ => {
                debug!("Resolving S3 bucket region for url: {url} bucket: {bucket}");
                resolve_bucket_region(bucket.as_str(), &ClientOptions::default()).await?
            }
        };
        debug!("S3 object store bucket: {bucket} resolved region: {region}");
        builder = builder.with_region(region);
    }

    builder.build()
}

pub fn parse_s3_url(
    mut builder: AmazonS3Builder,
    url: &Url,
) -> object_store::Result<AmazonS3Builder> {
    let scheme = url.scheme();
    let host = url.host_str().ok_or_else(|| object_store::Error::Generic {
        store: "S3",
        source: Box::new(plan_datafusion_err!(
            "URL did not match any known pattern for scheme: {url}"
        )),
    })?;
    let first_path_segment = url.path_segments().into_iter().flatten().next();
    debug!("Parsing S3 url: {url} scheme: {scheme} host: {host} first_path_segment: {first_path_segment:?}");

    match scheme {
        "s3" | "s3a" => {
            builder = builder.with_bucket_name(host);
            if let Some(bucket_prefix) = host.strip_suffix("--x-s3") {
                if let Some(_bucket_az) = bucket_prefix.rsplit_once("--") {
                    builder = builder.with_s3_express(true);
                }
            }
        }
        "http" | "https" => {
            if scheme == "http" {
                builder = builder.with_allow_http(true);
            }
            match host.split('.').collect::<Vec<&str>>()[..] {
                // Support for path-style continues for buckets created on/before Sept. 30, 2020:
                // https://aws.amazon.com/blogs/aws/amazon-s3-path-deprecation-plan-the-rest-of-the-story/
                ["s3", "amazonaws", "com"] => {
                    if let Some(bucket) = first_path_segment {
                        builder = builder.with_bucket_name(bucket);
                        builder = builder.with_virtual_hosted_style_request(false);
                    }
                }
                ["s3", region, "amazonaws", "com"] => {
                    builder = builder.with_region(region);
                    if let Some(bucket) = first_path_segment {
                        builder = builder.with_bucket_name(bucket);
                        builder = builder.with_virtual_hosted_style_request(false);
                    }
                }
                [bucket, "s3", "amazonaws", "com"] => {
                    builder = builder.with_bucket_name(bucket);
                    builder = builder.with_virtual_hosted_style_request(true);
                }
                [bucket, "s3", region, "amazonaws", "com"] => {
                    builder = builder.with_bucket_name(bucket);
                    builder = builder.with_region(region);
                    builder = builder.with_virtual_hosted_style_request(true);
                }
                [bucket, "s3-accelerate", "amazonaws", "com"] => {
                    builder = builder.with_bucket_name(bucket);
                    builder = builder
                        .with_endpoint(format!("{scheme}://{bucket}.s3-accelerate.amazonaws.com"));
                    builder = builder.with_virtual_hosted_style_request(true);
                }
                [bucket, "s3-accelerate", "dualstack", "amazonaws", "com"] => {
                    builder = builder.with_bucket_name(bucket);
                    builder = builder.with_endpoint(format!(
                        "{scheme}://{bucket}.s3-accelerate.dualstack.amazonaws.com"
                    ));
                    builder = builder.with_virtual_hosted_style_request(true);
                }
                [account, "r2", "cloudflarestorage", "com"] => {
                    builder = builder.with_region("auto");
                    builder = builder
                        .with_endpoint(format!("{scheme}://{account}.r2.cloudflarestorage.com"));
                    if let Some(bucket) = first_path_segment {
                        builder = builder.with_bucket_name(bucket);
                    }
                }
                [bucket, _s3express_zone_id, region, "amazonaws", "com"] => {
                    builder = builder.with_bucket_name(bucket);
                    builder = builder.with_region(region);
                    builder = builder.with_s3_express(true);
                }
                _ => {
                    return Err(object_store::Error::Generic {
                        store: "S3",
                        source: Box::new(plan_datafusion_err!(
                            "URL did not match any known pattern for scheme: {url}"
                        )),
                    })
                }
            }
        }
        scheme => {
            return Err(object_store::Error::Generic {
                store: "S3",
                source: Box::new(plan_datafusion_err!(
                    "Unknown url scheme cannot be parsed into storage location: {scheme}"
                )),
            });
        }
    };

    Ok(builder)
}
