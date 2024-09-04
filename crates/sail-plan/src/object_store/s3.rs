use std::sync::Arc;

use async_trait::async_trait;
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use object_store::aws::AwsCredential;
use object_store::CredentialProvider;

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

    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
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
