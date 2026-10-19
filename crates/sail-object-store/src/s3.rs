use std::fmt::Formatter;
use std::sync::Arc;

use async_trait::async_trait;
use aws_config::identity::IdentityCache;
use aws_config::{BehaviorVersion, SdkConfig};
use aws_credential_types::Credentials;
use aws_credential_types::provider::SharedCredentialsProvider;
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
    AmazonS3, AmazonS3Builder, AmazonS3ConfigKey, AwsCredential, resolve_bucket_region,
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

    if should_resolve_bucket_region(&builder, url)? {
        debug!("Resolving S3 bucket region for url: {url} bucket: {bucket}");
        let region = resolve_bucket_region(bucket.as_str(), &ClientOptions::default()).await?;
        debug!("S3 object store bucket: {bucket} resolved region: {region}");
        builder = builder.with_region(region);
    } else if region.is_none_or(|r| r.is_empty()) {
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

/// Discover ordinary AWS bucket regions without overriding explicit URL or endpoint settings.
fn should_resolve_bucket_region(
    builder: &AmazonS3Builder,
    url: &Url,
) -> object_store::Result<bool> {
    // Parse without environment settings to distinguish a URL's region from the
    // process default, which may belong to compute rather than this bucket.
    let url_builder = parse_s3_url(AmazonS3Builder::new(), url)?;
    Ok(url.scheme() != "oss"
        && url_builder
            .get_config_value(&AmazonS3ConfigKey::Region)
            .is_none()
        && url_builder
            .get_config_value(&AmazonS3ConfigKey::Endpoint)
            .is_none()
        && builder
            .get_config_value(&AmazonS3ConfigKey::Endpoint)
            .is_none_or(|endpoint| endpoint.is_empty())
        && builder
            .get_config_value(&AmazonS3ConfigKey::S3Express)
            .as_deref()
            != Some("true"))
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
    debug!(
        "Parsing S3 url: {url} scheme: {scheme} host: {host} first_path_segment: {first_path_segment:?}"
    );

    match scheme {
        "s3" | "s3a" | "oss" => {
            builder = builder.with_bucket_name(host);
            if let Some(bucket_prefix) = host.strip_suffix("--x-s3")
                && let Some(_bucket_az) = bucket_prefix.rsplit_once("--")
            {
                builder = builder.with_s3_express(true);
            }
        }
        "http" | "https" => {
            if scheme == "http" {
                builder = builder.with_allow_http(true);
            }
            let endpoint = || url[..url::Position::BeforePath].to_string();
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
                [bucket, "s3", region, "aliyuncs", "com"] if region.starts_with("oss-") => {
                    builder = builder.with_bucket_name(bucket);
                    builder = builder.with_region(region.trim_start_matches("oss-"));
                    builder = builder.with_endpoint(endpoint());
                    builder = builder.with_virtual_hosted_style_request(true);
                }
                ["s3", region, "aliyuncs", "com"] if region.starts_with("oss-") => {
                    builder = builder.with_region(region.trim_start_matches("oss-"));
                    builder = builder.with_endpoint(endpoint());
                    builder = builder.with_virtual_hosted_style_request(false);
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
                    });
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

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// Bucket discovery ignores process regions while preserving explicit storage settings.
    #[test]
    fn bucket_region_discovery_preserves_explicit_configuration() {
        for (raw_url, discover) in [
            ("s3://bucket/path", true),
            ("s3a://bucket/path", true),
            ("https://bucket.s3.amazonaws.com/path", true),
            ("https://s3.amazonaws.com/bucket/path", true),
            ("https://bucket.s3.ap-south-1.amazonaws.com/path", false),
            ("https://s3.ap-south-1.amazonaws.com/bucket/path", false),
            ("https://bucket.s3-accelerate.amazonaws.com/path", false),
            (
                "https://account.r2.cloudflarestorage.com/bucket/path",
                false,
            ),
            ("oss://bucket/path", false),
            ("https://bucket.s3.oss-cn-hangzhou.aliyuncs.com/path", false),
            ("s3://bucket--use1-az4--x-s3/path", false),
        ] {
            let url = Url::parse(raw_url).unwrap();
            for region in [None, Some("us-east-1")] {
                let mut builder = AmazonS3Builder::new();
                if let Some(region) = region {
                    builder = builder.with_region(region);
                }
                let builder = parse_s3_url(builder, &url).unwrap();
                assert_eq!(
                    should_resolve_bucket_region(&builder, &url).unwrap(),
                    discover,
                    "url: {raw_url}, process region: {region:?}",
                );
            }
        }

        let url = Url::parse("s3://bucket/path").unwrap();
        let builder = parse_s3_url(
            AmazonS3Builder::new()
                .with_region("us-east-1")
                .with_endpoint("http://localhost:9000"),
            &url,
        )
        .unwrap();
        assert!(!should_resolve_bucket_region(&builder, &url).unwrap());
    }

    #[test]
    fn parse_oss_url_sets_bucket() {
        let url = Url::parse("oss://bucket/path/to/data").unwrap();
        let builder = parse_s3_url(AmazonS3Builder::from_env(), &url).unwrap();

        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::Bucket),
            Some("bucket".to_string())
        );
    }

    #[test]
    fn parse_aliyun_oss_virtual_hosted_endpoint() {
        let url =
            Url::parse("https://bucket.s3.oss-cn-hangzhou.aliyuncs.com:9443/path/to/data").unwrap();
        let builder = parse_s3_url(AmazonS3Builder::from_env(), &url).unwrap();

        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::Bucket),
            Some("bucket".to_string())
        );
        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::Region),
            Some("cn-hangzhou".to_string())
        );
        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::Endpoint),
            Some("https://bucket.s3.oss-cn-hangzhou.aliyuncs.com:9443".to_string())
        );
    }

    #[test]
    fn parse_aliyun_oss_path_style_endpoint() {
        let url =
            Url::parse("https://s3.oss-cn-hangzhou.aliyuncs.com:9443/bucket/path/to/data").unwrap();
        let builder = parse_s3_url(AmazonS3Builder::from_env(), &url).unwrap();

        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::Bucket),
            Some("bucket".to_string())
        );
        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::Region),
            Some("cn-hangzhou".to_string())
        );
        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::Endpoint),
            Some("https://s3.oss-cn-hangzhou.aliyuncs.com:9443".to_string())
        );
    }
}
