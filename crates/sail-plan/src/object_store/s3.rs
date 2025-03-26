use std::sync::Arc;

use async_trait::async_trait;
use aws_config::{BehaviorVersion, SdkConfig};
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use object_store::aws::{resolve_bucket_region, AmazonS3, AmazonS3Builder, AwsCredential};
use object_store::{ClientOptions, CredentialProvider, Result};
use tokio::sync::OnceCell;
use url::Url;

static DEFAULT_AWS_CONFIG: OnceCell<SdkConfig> = OnceCell::const_new();

#[derive(Debug)]
pub(super) struct S3CredentialProvider {
    credentials: SharedCredentialsProvider,
}

impl S3CredentialProvider {
    pub fn new(credentials: SharedCredentialsProvider) -> Self {
        Self { credentials }
    }
}

#[async_trait]
impl CredentialProvider for S3CredentialProvider {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> Result<Arc<Self::Credential>> {
        let creds = self.credentials.provide_credentials().await.map_err(|e| {
            object_store::Error::Generic {
                store: "S3",
                source: Box::new(e),
            }
        })?;
        Ok(Arc::new(AwsCredential {
            key_id: creds.access_key_id().to_string(),
            secret_key: creds.secret_access_key().to_string(),
            token: creds.session_token().map(|t| t.to_string()),
        }))
    }
}

pub(super) async fn get_s3_object_store(url: &Url) -> Result<AmazonS3> {
    let config = DEFAULT_AWS_CONFIG
        .get_or_init(|| aws_config::defaults(BehaviorVersion::latest()).load())
        .await;
    let bucket = url.authority();
    let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);
    let region = match config.region() {
        Some(region) if !region.as_ref().is_empty() => region.to_string(),
        Some(_) | None => resolve_bucket_region(bucket, &ClientOptions::default()).await?,
    };
    builder = builder.with_region(region);
    if let Some(credentials) = config.credentials_provider() {
        let credentials = S3CredentialProvider::new(credentials);
        builder = builder.with_credentials(Arc::new(credentials));
    }
    builder.build()
}
