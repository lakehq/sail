// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::fmt::Formatter;
use std::sync::{Arc, Mutex};

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
    AmazonS3, AmazonS3Builder, AmazonS3ConfigKey, AwsCredential, AwsCredentialProvider,
    resolve_bucket_region,
};
use object_store::{ClientOptions, CredentialProvider};
use sail_common_datafusion::catalog::S3Endpoint;
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
    get_s3_object_store_with_credentials(url, None, None, None).await
}

/// The regions of buckets that are discovered already.
#[derive(Debug, Default)]
pub(crate) struct BucketRegions(Mutex<HashMap<String, String>>);

impl BucketRegions {
    fn get(&self, bucket: &str) -> Option<String> {
        let regions = self.0.lock().unwrap_or_else(|e| e.into_inner());
        regions.get(bucket).cloned()
    }

    fn set(&self, bucket: &str, region: &str) {
        let mut regions = self.0.lock().unwrap_or_else(|e| e.into_inner());
        regions.insert(bucket.to_string(), region.to_string());
    }
}

/// Discovers the region of the bucket, unless it is among the given regions.
async fn discover_bucket_region(
    bucket: &str,
    regions: Option<&BucketRegions>,
) -> object_store::Result<String> {
    if let Some(region) = regions.and_then(|x| x.get(bucket)) {
        return Ok(region);
    }
    let region = resolve_bucket_region(bucket, &ClientOptions::default()).await?;
    if let Some(regions) = regions {
        regions.set(bucket, &region);
    }
    Ok(region)
}

/// Creates the S3 object store that uses the given credentials if any,
/// or the credentials of the environment otherwise.
/// The region of the bucket is not discovered if it is among the given regions.
/// The requests go to the given S3-compatible storage if any, instead of AWS S3
/// or the storage that the environment configures.
pub(crate) async fn get_s3_object_store_with_credentials(
    url: &Url,
    credentials: Option<AwsCredentialProvider>,
    regions: Option<&BucketRegions>,
    endpoint: Option<&S3Endpoint>,
) -> object_store::Result<AmazonS3> {
    debug!("Creating S3 object store for url: {url}");
    let mut builder = AmazonS3Builder::from_env();
    let config = DEFAULT_AWS_CONFIG
        .get_or_init(|| aws_config::defaults(BehaviorVersion::latest()).load())
        .await;

    if let Some(credentials) = credentials {
        builder = builder.with_credentials(credentials);
    } else if let Some(provider) = config.credentials_provider() {
        let cache = config
            .identity_cache()
            .unwrap_or_else(|| IdentityCache::lazy().build());
        let credentials = S3CredentialProvider::try_new(provider, cache)?;
        builder = builder.with_credentials(Arc::new(credentials));
    }

    let mut builder = parse_s3_url(builder, url)?;
    if let Some(endpoint) = endpoint {
        return with_s3_endpoint(builder, endpoint).build();
    }
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
        let region = discover_bucket_region(bucket.as_str(), regions).await?;
        debug!("S3 object store bucket: {bucket} resolved region: {region}");
        builder = builder.with_region(region);
    } else if region.is_none_or(|r| r.is_empty()) {
        let region = match config.region() {
            Some(region) if !region.as_ref().is_empty() => region.to_string(),
            _ => {
                debug!("Resolving S3 bucket region for url: {url} bucket: {bucket}");
                discover_bucket_region(bucket.as_str(), regions).await?
            }
        };
        debug!("S3 object store bucket: {bucket} resolved region: {region}");
        builder = builder.with_region(region);
    }

    builder.build()
}

/// Sends the requests to the S3-compatible storage with path-style addressing.
/// The region of the bucket is not discovered, since the bucket is not in AWS S3.
fn with_s3_endpoint(builder: AmazonS3Builder, endpoint: &S3Endpoint) -> AmazonS3Builder {
    // The region of the environment is not for this storage.
    let region = endpoint.region.as_deref().unwrap_or("us-east-1");
    builder
        .with_endpoint(endpoint.url.as_str())
        .with_allow_http(endpoint.url.starts_with("http://"))
        .with_virtual_hosted_style_request(false)
        .with_s3_express(false)
        .with_region(region)
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

    /// The requests for an S3-compatible storage go to its endpoint with path-style
    /// addressing and are signed with its region and the given credential, whatever
    /// the environment configures and whatever is known about a bucket of the same name.
    #[tokio::test]
    async fn s3_endpoint_requests_use_path_style_and_the_region_of_the_endpoint() {
        use object_store::path::Path;
        use object_store::{ObjectStoreExt, StaticCredentialProvider};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (sender, mut requests) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let mut request = vec![];
                while !request.ends_with(b"\r\n\r\n") {
                    let Ok(byte) = stream.read_u8().await else {
                        break;
                    };
                    request.push(byte);
                }
                let _ = sender.send(String::from_utf8_lossy(&request).to_string());
                let response = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });
        let regions = BucketRegions::default();
        regions.set("bucket", "eu-central-1");
        let url = Url::parse("s3://bucket/table").unwrap();
        for (region, expected) in [(Some("xx-test-1"), "xx-test-1"), (None, "us-east-1")] {
            let endpoint = S3Endpoint {
                url: format!("http://{address}"),
                region: region.map(|x| x.to_string()),
            };
            let credentials = Arc::new(StaticCredentialProvider::new(AwsCredential {
                key_id: "vended-key".to_string(),
                secret_key: "secret".to_string(),
                token: None,
            }));
            let store = get_s3_object_store_with_credentials(
                &url,
                Some(credentials),
                Some(&regions),
                Some(&endpoint),
            )
            .await
            .unwrap();
            let result = store.get(&Path::from("table/part.parquet")).await;
            assert!(matches!(result, Err(object_store::Error::NotFound { .. })));
            let request = requests.recv().await.unwrap().to_ascii_lowercase();
            assert!(
                request.starts_with("get /bucket/table/part.parquet http/1.1\r\n"),
                "{request}"
            );
            assert!(
                request.contains(&format!("\r\nhost: {address}\r\n")),
                "{request}"
            );
            assert!(
                request.contains("credential=vended-key/")
                    && request.contains(&format!("/{expected}/s3/aws4_request")),
                "{request}"
            );
        }
        // The settings of the environment for another storage are replaced.
        let builder = AmazonS3Builder::new()
            .with_endpoint("http://localhost:9000")
            .with_region("eu-central-1")
            .with_virtual_hosted_style_request(true);
        let endpoint = S3Endpoint {
            url: "https://minio.example.com".to_string(),
            region: Some("xx-test-1".to_string()),
        };
        let builder = with_s3_endpoint(parse_s3_url(builder, &url).unwrap(), &endpoint);
        for (key, value) in [
            (AmazonS3ConfigKey::Endpoint, "https://minio.example.com"),
            (AmazonS3ConfigKey::Region, "xx-test-1"),
            (AmazonS3ConfigKey::VirtualHostedStyleRequest, "false"),
            (AmazonS3ConfigKey::Bucket, "bucket"),
        ] {
            assert_eq!(builder.get_config_value(&key).as_deref(), Some(value));
        }
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
