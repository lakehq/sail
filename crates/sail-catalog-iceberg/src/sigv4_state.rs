// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use aws_config::BehaviorVersion;
use aws_config::identity::IdentityCache;
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
use sail_catalog::error::{CatalogError, CatalogResult};
use secrecy::{ExposeSecret, SecretString};

use crate::r#gen::RequestSigner;
use crate::sigv4::{PayloadHashMode, SigV4Signer};

#[derive(Debug, Clone)]
pub struct SigV4Credentials {
    pub access_key_id: String,
    pub secret_access_key: SecretString,
    pub session_token: Option<SecretString>,
}

#[derive(Debug, Clone)]
pub struct SigV4Options {
    pub region: Option<String>,
    pub service: String,
    pub credentials: Option<SigV4Credentials>,
}

pub(crate) struct SigV4State {
    signer: Option<Arc<SigV4Signer>>,
    identity_cache: SharedIdentityCache,
    identity_resolver: Option<SharedIdentityResolver>,
    runtime_components: RuntimeComponents,
    config_bag: ConfigBag,
}

impl SigV4State {
    pub(crate) async fn from_options(options: &SigV4Options) -> CatalogResult<SigV4State> {
        let runtime_components = RuntimeComponentsBuilder::for_tests()
            .with_time_source(Some(SystemTimeSource::new()))
            .with_sleep_impl(Some(TokioSleep::new()))
            .build()
            .map_err(|e| {
                CatalogError::External(format!("failed to build AWS runtime components: {e}"))
            })?;

        let mut config_loader = aws_config::defaults(BehaviorVersion::latest());
        if let Some(region) = &options.region {
            config_loader = config_loader.region(aws_config::Region::new(region.clone()));
        }
        if let Some(credentials) = &options.credentials {
            config_loader = config_loader.credentials_provider(Credentials::new(
                credentials.access_key_id.clone(),
                credentials.secret_access_key.expose_secret(),
                credentials
                    .session_token
                    .as_ref()
                    .map(|session_token| session_token.expose_secret().to_string()),
                None,
                "sail-catalog-iceberg-sigv4-options",
            ));
        }
        let sdk_conf = config_loader.load().await;

        Ok(SigV4State {
            signer: sdk_conf.region().map(|region| {
                Arc::new(SigV4Signer::new(
                    region.to_string(),
                    options.service.clone(),
                    PayloadHashMode::IcebergRest,
                ))
            }),
            identity_cache: sdk_conf
                .identity_cache()
                .unwrap_or_else(|| IdentityCache::lazy().build()),
            identity_resolver: sdk_conf
                .credentials_provider()
                .map(SharedIdentityResolver::new),
            runtime_components,
            config_bag: ConfigBag::base(),
        })
    }

    pub(crate) async fn new_request_signer(&self) -> CatalogResult<SigV4RequestSigner> {
        let Some(signer) = &self.signer else {
            return Err(CatalogError::InvalidArgument(
                "SigV4 signing region is not configured; set signing_region or AWS_REGION"
                    .to_string(),
            ));
        };
        let Some(identity_resolver) = &self.identity_resolver else {
            return Err(CatalogError::InvalidArgument(
                "no AWS credentials found for SigV4 signing".to_string(),
            ));
        };
        let identity = self
            .identity_cache
            .resolve_cached_identity(
                identity_resolver.clone(),
                &self.runtime_components,
                &self.config_bag,
            )
            .await
            .map_err(|e| {
                CatalogError::External(format!("failed to resolve AWS credentials for SigV4: {e}"))
            })?;
        let credentials = identity.data::<Credentials>().ok_or_else(|| {
            CatalogError::External("resolved AWS identity is not a credential".to_string())
        })?;
        Ok(SigV4RequestSigner {
            signer: signer.clone(),
            credentials: credentials.clone(),
        })
    }
}

#[derive(Debug)]
pub(crate) struct SigV4RequestSigner {
    signer: Arc<SigV4Signer>,
    credentials: Credentials,
}

impl RequestSigner for SigV4RequestSigner {
    fn sign(
        &self,
        request: &mut reqwest::Request,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.signer.sign(request, &self.credentials)?)
    }
}
