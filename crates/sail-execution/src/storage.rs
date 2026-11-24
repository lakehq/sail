// SPDX-License-Identifier: Apache-2.0

//! The storage credentials that catalogs issue for tables stay in the session of
//! the driver. The workers of the session get the storage locations that are accessed
//! with such credentials with each task batch, and request the credentials from
//! the driver when a location is first read or written, and before they expire.

use sail_common_datafusion::catalog::{
    S3Endpoint, StorageAccess, StorageCredential, TableStorageCredentials,
};
use sail_object_store::{StorageRoute, StorageRoutes};

use crate::driver::r#gen as driver;
use crate::driver::r#gen::get_storage_credentials_response::Credential;
use crate::error::{ExecutionError, ExecutionResult};
use crate::worker::r#gen as worker;

impl From<&StorageRoutes> for worker::StorageRoutes {
    fn from(value: &StorageRoutes) -> Self {
        Self {
            version: value.version,
            token: value.token.clone(),
            routes: value
                .routes
                .iter()
                .map(|route| worker::StorageRoute {
                    location: route.location.clone(),
                    owner: route.owner.clone(),
                    not_issued: route.not_issued.clone(),
                    version: route.version,
                })
                .collect(),
        }
    }
}

impl From<worker::StorageRoutes> for StorageRoutes {
    fn from(value: worker::StorageRoutes) -> Self {
        Self {
            version: value.version,
            token: value.token,
            routes: value
                .routes
                .into_iter()
                .map(|route| StorageRoute {
                    location: route.location,
                    owner: route.owner,
                    version: route.version,
                    not_issued: route.not_issued,
                })
                .collect(),
        }
    }
}

impl From<StorageAccess> for driver::StorageAccess {
    fn from(value: StorageAccess) -> Self {
        match value {
            StorageAccess::Read => Self::Read,
            StorageAccess::ReadWrite => Self::ReadWrite,
        }
    }
}

impl From<driver::StorageAccess> for StorageAccess {
    fn from(value: driver::StorageAccess) -> Self {
        match value {
            driver::StorageAccess::Read => Self::Read,
            driver::StorageAccess::ReadWrite => Self::ReadWrite,
        }
    }
}

impl From<S3Endpoint> for driver::S3StorageEndpoint {
    fn from(value: S3Endpoint) -> Self {
        Self {
            url: value.url,
            region: value.region,
        }
    }
}

impl From<driver::S3StorageEndpoint> for S3Endpoint {
    fn from(value: driver::S3StorageEndpoint) -> Self {
        Self {
            url: value.url,
            region: value.region,
        }
    }
}

impl From<TableStorageCredentials> for driver::GetStorageCredentialsResponse {
    fn from(value: TableStorageCredentials) -> Self {
        let credential = match value.credential {
            StorageCredential::S3 {
                access_key_id,
                secret_access_key,
                session_token,
                endpoint,
            } => Credential::S3(driver::S3StorageCredential {
                access_key_id,
                secret_access_key,
                session_token,
                endpoint: endpoint.map(Into::into),
            }),
            StorageCredential::S3Environment { endpoint } => {
                Credential::S3Environment(endpoint.into())
            }
            StorageCredential::AzureSas { token } => {
                Credential::AzureSas(driver::AzureSasStorageCredential { token })
            }
            StorageCredential::GcsOauth { token } => {
                Credential::GcsOauth(driver::GcsOauthStorageCredential { token })
            }
        };
        Self {
            credential: Some(credential),
            expires_at_ms: value.expires_at_ms,
        }
    }
}

/// Returns the storage credentials in the response to a request for the access level.
pub(crate) fn storage_credentials_from_response(
    response: driver::GetStorageCredentialsResponse,
    access: StorageAccess,
) -> ExecutionResult<TableStorageCredentials> {
    let driver::GetStorageCredentialsResponse {
        credential,
        expires_at_ms,
    } = response;
    let credential = match credential {
        Some(Credential::S3(driver::S3StorageCredential {
            access_key_id,
            secret_access_key,
            session_token,
            endpoint,
        })) => StorageCredential::S3 {
            access_key_id,
            secret_access_key,
            session_token,
            endpoint: endpoint.map(Into::into),
        },
        Some(Credential::S3Environment(endpoint)) => StorageCredential::S3Environment {
            endpoint: endpoint.into(),
        },
        Some(Credential::AzureSas(driver::AzureSasStorageCredential { token })) => {
            StorageCredential::AzureSas { token }
        }
        Some(Credential::GcsOauth(driver::GcsOauthStorageCredential { token })) => {
            StorageCredential::GcsOauth { token }
        }
        None => {
            return Err(ExecutionError::InvalidArgument(
                "the driver returned no storage credential".to_string(),
            ));
        }
    };
    Ok(TableStorageCredentials {
        credential,
        expires_at_ms,
        access,
    })
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn storage_routes_are_sent_without_showing_the_token() {
        let routes = StorageRoutes {
            version: 3,
            token: vec![7; 32],
            routes: vec![
                StorageRoute {
                    location: "s3://bucket/table".to_string(),
                    owner: "[\"c\", \"s\", \"t\"]".to_string(),
                    version: 1,
                    not_issued: None,
                },
                StorageRoute {
                    location: "s3://bucket/other".to_string(),
                    owner: "other".to_string(),
                    version: 3,
                    not_issued: Some("no privilege".to_string()),
                },
            ],
        };
        let message = worker::StorageRoutes::from(&routes);
        let request = worker::RunTaskBatchRequest {
            job_id: 1,
            stage: 0,
            tasks: vec![],
            definition: vec![],
            peers: vec![],
            storage_routes: Some(message.clone()),
        };
        let debug = format!("{request:?} {message:?}");
        assert!(debug.contains("s3://bucket/table"), "{debug}");
        assert!(debug.contains("<redacted>"), "{debug}");
        assert!(!debug.contains("7, 7"), "{debug}");
        assert_eq!(StorageRoutes::from(message), routes);
    }

    #[tokio::test]
    async fn task_requests_have_no_storage_credentials() {
        use std::sync::Arc;

        use object_store::ObjectStore;
        use object_store::memory::InMemory;
        use prost::Message;
        use sail_common::runtime::RuntimeHandle;
        use sail_object_store::{
            DynamicObjectStoreRegistry, StorageCredentialFetch, VendedObjectStores,
            VendedStoreFactory,
        };

        let credentials = || TableStorageCredentials {
            credential: StorageCredential::S3 {
                access_key_id: "key-value".to_string(),
                secret_access_key: "secret-value".to_string(),
                session_token: Some("token-value".to_string()),
                endpoint: None,
            },
            expires_at_ms: None,
            access: StorageAccess::ReadWrite,
        };
        let factory: VendedStoreFactory =
            Arc::new(|_| Ok(Arc::new(InMemory::new()) as Arc<dyn ObjectStore>));
        let stores = VendedObjectStores::with_factory(factory);
        let handle = tokio::runtime::Handle::current();
        let registry = DynamicObjectStoreRegistry::new(RuntimeHandle::new(handle.clone(), handle));
        let fetch: StorageCredentialFetch =
            Arc::new(move |_| Box::pin(async move { Ok(credentials()) }));
        let location = url::Url::parse("memory:///warehouse/items").unwrap();
        stores
            .install(&registry, &location, "table", credentials(), fetch)
            .unwrap();
        let routes = stores.routes().unwrap();
        let request = worker::RunTaskBatchRequest {
            job_id: 1,
            stage: 0,
            tasks: vec![],
            definition: vec![],
            peers: vec![],
            storage_routes: Some(routes.as_ref().into()),
        };
        let bytes = request.encode_to_vec();
        let text = format!("{} {request:?}", String::from_utf8_lossy(&bytes));
        assert!(text.contains("memory:///warehouse/items"), "{text}");
        for secret in ["key-value", "secret-value", "token-value"] {
            assert!(!text.contains(secret), "{text}");
        }
    }

    #[test]
    fn storage_credentials_are_sent_for_each_storage() {
        for credential in [
            StorageCredential::S3 {
                access_key_id: "key".to_string(),
                secret_access_key: "secret".to_string(),
                session_token: Some("token".to_string()),
                endpoint: None,
            },
            StorageCredential::S3 {
                access_key_id: "key".to_string(),
                secret_access_key: "secret".to_string(),
                session_token: None,
                endpoint: Some(S3Endpoint {
                    url: "http://storage:9000".to_string(),
                    region: Some("us-east-1".to_string()),
                }),
            },
            StorageCredential::S3Environment {
                endpoint: S3Endpoint {
                    url: "http://storage:9000".to_string(),
                    region: None,
                },
            },
            StorageCredential::AzureSas {
                token: "sas".to_string(),
            },
            StorageCredential::GcsOauth {
                token: "oauth".to_string(),
            },
        ] {
            for (expires_at_ms, access) in [
                (Some(1_700_000_000_000), StorageAccess::Read),
                (None, StorageAccess::ReadWrite),
            ] {
                let credentials = TableStorageCredentials {
                    credential: credential.clone(),
                    expires_at_ms,
                    access,
                };
                let response = driver::GetStorageCredentialsResponse::from(credentials.clone());
                assert_eq!(
                    storage_credentials_from_response(response, access).unwrap(),
                    credentials
                );
                assert_eq!(
                    StorageAccess::from(driver::StorageAccess::from(access)),
                    access
                );
            }
        }
        let empty = driver::GetStorageCredentialsResponse {
            credential: None,
            expires_at_ms: None,
        };
        assert!(storage_credentials_from_response(empty, StorageAccess::Read).is_err());
    }
}
