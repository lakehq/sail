use std::collections::HashMap;
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use object_store::ObjectStoreExt;
use object_store::path::Path;
use sail_common::storage::{IcebergCredentialSource, StorageAccessSpec, StorageSecret};
use serde_json::json;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::iceberg::{IcebergStorageCredential, iceberg_storage_credentials};
use super::*;

fn entry(prefix: &str, endpoint: &str, expiry: Option<i64>) -> IcebergStorageCredential {
    let mut config = HashMap::from([
        ("s3.access-key-id".to_string(), "test-key".to_string()),
        (
            "s3.secret-access-key".to_string(),
            "test-secret".to_string(),
        ),
        ("s3.session-token".to_string(), "test-token".to_string()),
        ("s3.endpoint".to_string(), endpoint.to_string()),
        ("s3.path-style-access".to_string(), "true".to_string()),
    ]);
    if let Some(expiry) = expiry {
        config.insert("expiration-time".to_string(), expiry.to_string());
    }
    IcebergStorageCredential {
        prefix: prefix.to_string(),
        config,
    }
}

fn spec(endpoint: &str, expiry: Option<i64>) -> StorageAccessSpec {
    StorageAccessSpec {
        credentials: iceberg_storage_credentials(
            "",
            &HashMap::new(),
            &[entry("s3://bucket/table", endpoint, expiry)],
        )
        .unwrap(),
        refresh: Some(IcebergCredentialSource {
            endpoint: format!("{endpoint}/credentials"),
            bearer_token: Some(StorageSecret::new("catalog-token".to_string())),
        }),
    }
}

fn response(expiry: i64) -> serde_json::Value {
    json!({"storage-credentials": [{"prefix": "s3://bucket/table", "config": {
        "s3.access-key-id": "refreshed-key", "s3.secret-access-key": "refreshed-secret",
        "s3.session-token": "refreshed-token", "expiration-time": expiry.to_string()
    }}]})
}

#[test]
fn parses_config_fallback_and_scoped_precedence() {
    let defaults = entry("s3://bucket/", "http://localhost:9000", None).config;
    let fallback = iceberg_storage_credentials("s3://bucket/", &defaults, &[]).unwrap();
    assert_eq!(fallback[0].prefix, "s3://bucket");
    assert_eq!(fallback[0].s3.expires_at_ms, None);
    let mut nested = entry("s3://bucket/table", "http://localhost:9001", Some(123));
    nested
        .config
        .insert("s3.sse.type".to_string(), "kms".to_string());
    nested
        .config
        .insert("s3.sse.key".to_string(), "test-kms-key".to_string());
    let parsed = iceberg_storage_credentials("s3://bucket", &defaults, &[nested]).unwrap();
    assert_eq!(
        parsed[0].s3.connection.endpoint.as_deref(),
        Some("http://localhost:9001")
    );
    assert_eq!(
        parsed[0].s3.connection.sse_key.as_deref(),
        Some("test-kms-key")
    );
    assert_eq!(parsed[0].s3.expires_at_ms, Some(123));
    let incomplete = IcebergStorageCredential {
        prefix: "s3://bucket/table".to_string(),
        config: HashMap::new(),
    };
    assert!(iceberg_storage_credentials("s3://bucket", &defaults, &[incomplete]).is_err());
}

#[tokio::test]
async fn expired_bootstrap_refreshes_once_and_signs_worker_io() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/credentials"))
        .and(header("Authorization", "Bearer catalog-token"))
        .and(header("X-Iceberg-Access-Delegation", "vended-credentials"))
        .respond_with(ResponseTemplate::new(200).set_body_json(response(now_ms() + 300_000)))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/bucket/table/file"))
        .and(header("x-amz-security-token", "refreshed-token"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"data"))
        .expect(8)
        .mount(&server)
        .await;
    let base = SessionContext::new();
    let runtime = storage_runtime(&base.runtime_env(), &spec(&server.uri(), Some(0))).unwrap();
    let store = runtime
        .object_store(
            datafusion::execution::object_store::ObjectStoreUrl::parse("s3://bucket").unwrap(),
        )
        .unwrap();
    let reads = (0..8).map(|_| {
        let store = store.clone();
        async move {
            store
                .get(&Path::from("table/file"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
        }
    });
    for bytes in futures::future::join_all(reads).await {
        assert_eq!(bytes.as_ref(), b"data");
    }
    let requests = server.received_requests().await.unwrap();
    assert!(
        requests
            .iter()
            .filter(|request| request.url.path() == "/bucket/table/file")
            .all(|request| request.headers["authorization"]
                .to_str()
                .unwrap()
                .contains("Credential=refreshed-key/"))
    );
    assert!(store.get(&Path::from("table-other/file")).await.is_err());
    assert!(
        runtime
            .object_store(
                datafusion::execution::object_store::ObjectStoreUrl::parse("s3://other-bucket")
                    .unwrap()
            )
            .is_err()
    );
}

#[tokio::test]
async fn live_store_renews_after_expiry_without_reloading_metadata() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/credentials"))
        .respond_with(ResponseTemplate::new(200).set_body_json(response(now_ms() + 300_000)))
        .expect(1)
        .mount(&server)
        .await;
    let session = CredentialSession::new(spec(&server.uri(), Some(now_ms() + 5000))).unwrap();
    let connection = session.spec.credentials[0].s3.connection.clone();
    assert_eq!(
        session
            .credential("s3://bucket/table", &connection)
            .await
            .unwrap()
            .key_id,
        "test-key"
    );
    tokio::time::sleep(Duration::from_millis(5100)).await;
    assert_eq!(
        session
            .credential("s3://bucket/table", &connection)
            .await
            .unwrap()
            .key_id,
        "refreshed-key"
    );
}

#[tokio::test]
async fn unchanged_credentials_and_optional_expiry_are_valid() {
    let server = MockServer::start().await;
    let spec = spec(&server.uri(), None);
    let session = CredentialSession::new(spec.clone()).unwrap();
    let response = json!({"storage-credentials": [{"prefix":"s3a://bucket/table/", "config": entry("", &server.uri(), None).config}]});
    Mock::given(method("GET"))
        .and(path("/credentials"))
        .respond_with(ResponseTemplate::new(200).set_body_json(response))
        .expect(1)
        .mount(&server)
        .await;
    session.state.lock().await.refresh_at_ms = 0;
    for _ in 0..2 {
        let credential = session
            .credential("s3://bucket/table", &spec.credentials[0].s3.connection)
            .await
            .unwrap();
        assert_eq!(credential.key_id, "test-key");
    }
}

#[tokio::test]
async fn failed_refresh_does_not_disclose_response_or_use_ambient_credentials() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/credentials"))
        .respond_with(ResponseTemplate::new(403).set_body_string("sensitive-provider-body"))
        .expect(1)
        .mount(&server)
        .await;
    let session = CredentialSession::new(spec(&server.uri(), Some(0))).unwrap();
    let error = session
        .credential(
            "s3://bucket/table",
            &session.spec.credentials[0].s3.connection,
        )
        .await
        .unwrap_err();
    assert!(!error.to_string().contains("sensitive-provider-body"));
    assert!(error.to_string().contains("403"));
}

#[tokio::test]
async fn connection_change_is_rejected_before_credentials_are_installed() {
    let server = MockServer::start().await;
    let mut response = response(now_ms() + 300_000);
    response["storage-credentials"][0]["config"]["s3.endpoint"] = json!("http://localhost:1");
    Mock::given(method("GET"))
        .and(path("/credentials"))
        .respond_with(ResponseTemplate::new(200).set_body_json(response))
        .mount(&server)
        .await;
    let session = CredentialSession::new(spec(&server.uri(), Some(0))).unwrap();
    assert!(
        session
            .credential(
                "s3://bucket/table",
                &session.spec.credentials[0].s3.connection
            )
            .await
            .is_err()
    );
    assert_eq!(
        session.state.lock().await.credentials[0]
            .s3
            .access_key_id
            .expose(),
        "test-key"
    );
}

#[tokio::test]
async fn refresh_must_preserve_every_prefix_without_duplicates() {
    let server = MockServer::start().await;
    let mut spec = spec(&server.uri(), Some(0));
    let mut second = spec.credentials[0].clone();
    second.prefix = "s3://bucket/another-table".to_string();
    spec.credentials.push(second);
    let mut response = response(now_ms() + 300_000);
    let repeated = response["storage-credentials"][0].clone();
    response["storage-credentials"]
        .as_array_mut()
        .unwrap()
        .push(repeated);
    Mock::given(method("GET"))
        .and(path("/credentials"))
        .respond_with(ResponseTemplate::new(200).set_body_json(response))
        .mount(&server)
        .await;
    let session = CredentialSession::new(spec).unwrap();
    assert!(
        session
            .credential(
                "s3://bucket/table",
                &session.spec.credentials[0].s3.connection
            )
            .await
            .is_err()
    );
    assert_eq!(
        session.state.lock().await.credentials,
        session.spec.credentials
    );
}

#[tokio::test]
async fn longest_path_segment_route_and_cache_isolation() {
    let root = Arc::new(object_store::memory::InMemory::new());
    let nested = Arc::new(object_store::memory::InMemory::new());
    root.put(&Path::from("table-x/file"), "root".into())
        .await
        .unwrap();
    nested
        .put(&Path::from("table/file"), "nested".into())
        .await
        .unwrap();
    let router =
        CredentialRoutingStore::new(vec![(Path::default(), root), (Path::from("table"), nested)]);
    assert_eq!(
        router
            .get(&Path::from("table/file"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        "nested"
    );
    assert_eq!(
        router
            .get(&Path::from("table-x/file"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        "root"
    );
    let base = SessionContext::new().runtime_env();
    let a = storage_runtime(&base, &spec("http://localhost:1", None)).unwrap();
    let b = storage_runtime(&base, &spec("http://localhost:2", None)).unwrap();
    let private_origin = Url::parse("s3://private-bucket").unwrap();
    a.object_store_registry.register_store(
        &private_origin,
        Arc::new(object_store::memory::InMemory::new()),
    );
    assert!(a.object_store_registry.get_store(&private_origin).is_ok());
    assert!(b.object_store_registry.get_store(&private_origin).is_err());
    assert!(
        base.object_store_registry
            .get_store(&private_origin)
            .is_err()
    );
    assert!(!Arc::ptr_eq(
        &a.cache_manager.get_file_metadata_cache(),
        &b.cache_manager.get_file_metadata_cache()
    ));
    assert!(!Arc::ptr_eq(
        &a.cache_manager.get_file_metadata_cache(),
        &base.cache_manager.get_file_metadata_cache()
    ));
    let spec = spec("http://localhost:1", None);
    let debug = format!("{spec:?}");
    for value in ["test-key", "test-secret", "test-token", "catalog-token"] {
        assert!(!debug.contains(value));
    }
    assert!(
        serde_json::to_string(&spec)
            .unwrap()
            .contains("test-secret")
    );
}
