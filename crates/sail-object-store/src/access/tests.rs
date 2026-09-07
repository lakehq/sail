use std::collections::HashMap;
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
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
    let mut credentials = iceberg_storage_credentials(
        "",
        &HashMap::new(),
        &[entry("s3://bucket/table", endpoint, expiry)],
    )
    .unwrap();
    credentials[0].refresh = Some(IcebergCredentialSource {
        endpoint: format!("{endpoint}/credentials"),
        authentication: CatalogCredentialSource::Bearer(StorageSecret::new(
            "catalog-token".to_string(),
        )),
        headers: Default::default(),
    });
    StorageAccessSpec { credentials }
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
    let inherited = iceberg_storage_credentials("s3://bucket", &defaults, &[incomplete]).unwrap();
    assert_eq!(inherited[0].s3.access_key_id.expose(), "test-key");
}

#[tokio::test]
async fn missing_expiry_does_not_schedule_a_refresh() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/credentials"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    let spec = spec(&server.uri(), None);
    let session = CredentialSession::new(spec.credentials.clone()).unwrap();
    for _ in 0..2 {
        assert_eq!(
            session
                .credential("s3://bucket/table", &spec.credentials[0].s3.connection)
                .await
                .unwrap()
                .key_id,
            "test-key"
        );
    }
    assert_eq!(session.state.lock().await.refresh_at_ms, None);
}

#[tokio::test]
async fn independent_refresh_sources_preserve_other_routes_and_share_each_source() {
    let server = MockServer::start().await;
    let mut spec = spec(&server.uri(), Some(0));
    let mut second = spec.credentials[0].clone();
    second.prefix = "s3://bucket/second".to_string();
    second.refresh.as_mut().unwrap().endpoint = format!("{}/second-credentials", server.uri());
    let mut third = spec.credentials[0].clone();
    third.prefix = "s3://bucket/third".to_string();
    spec.credentials.extend([second, third]);
    let mut first_response = response(now_ms() + 600_000);
    let mut third_response = first_response["storage-credentials"][0].clone();
    third_response["prefix"] = json!("s3://bucket/third");
    first_response["storage-credentials"]
        .as_array_mut()
        .unwrap()
        .push(third_response);
    let mut second_response = response(now_ms() + 600_000);
    second_response["storage-credentials"][0]["prefix"] = json!("s3://bucket/second");
    second_response["storage-credentials"][0]["config"]["s3.session-token"] = json!("second-token");
    Mock::given(method("GET"))
        .and(path("/credentials"))
        .respond_with(ResponseTemplate::new(200).set_body_json(first_response))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/second-credentials"))
        .respond_with(ResponseTemplate::new(200).set_body_json(second_response))
        .expect(1)
        .mount(&server)
        .await;
    for (file, token) in [
        ("table", "refreshed-token"),
        ("third", "refreshed-token"),
        ("second", "second-token"),
    ] {
        Mock::given(method("GET"))
            .and(path(format!("/bucket/{file}/file")))
            .and(header("x-amz-security-token", token))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(file.as_bytes()))
            .expect(1)
            .mount(&server)
            .await;
    }
    let runtime = storage_runtime(&SessionContext::new().runtime_env(), &spec).unwrap();
    let store = runtime
        .object_store(
            datafusion::execution::object_store::ObjectStoreUrl::parse("s3://bucket").unwrap(),
        )
        .unwrap();
    for file in ["table", "third", "second"] {
        let bytes = store
            .get(&Path::from(format!("{file}/file")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(bytes.as_ref(), file.as_bytes());
    }
}

#[tokio::test]
async fn transient_refresh_failure_uses_valid_credentials_and_backs_off() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/credentials"))
        .respond_with(ResponseTemplate::new(503).set_body_string("private-provider-response"))
        .expect(1)
        .mount(&server)
        .await;
    let spec = spec(&server.uri(), Some(now_ms() + 600_000));
    let session = CredentialSession::new(spec.credentials.clone()).unwrap();
    session.state.lock().await.refresh_at_ms = Some(0);
    for _ in 0..8 {
        assert_eq!(
            session
                .credential("s3://bucket/table", &spec.credentials[0].s3.connection)
                .await
                .unwrap()
                .key_id,
            "test-key"
        );
    }
    {
        let mut state = session.state.lock().await;
        assert!(state.failure.as_ref().unwrap().retry_at_ms > now_ms());
        state.credentials[0].s3.expires_at_ms = Some(0);
    }
    let error = session
        .credential("s3://bucket/table", &spec.credentials[0].s3.connection)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("503"));
    assert!(!error.to_string().contains("private-provider-response"));
    server.verify().await;
    server.reset().await;
    Mock::given(method("GET"))
        .and(path("/credentials"))
        .respond_with(ResponseTemplate::new(200).set_body_json(response(now_ms() + 600_000)))
        .expect(1)
        .mount(&server)
        .await;
    session
        .state
        .lock()
        .await
        .failure
        .as_mut()
        .unwrap()
        .retry_at_ms = 0;
    assert_eq!(
        session
            .credential("s3://bucket/table", &spec.credentials[0].s3.connection)
            .await
            .unwrap()
            .key_id,
        "refreshed-key"
    );
    assert!(session.state.lock().await.failure.is_none());
}

#[tokio::test]
async fn rejected_authorization_does_not_use_still_valid_storage_credentials() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/credentials"))
        .respond_with(ResponseTemplate::new(403))
        .expect(1)
        .mount(&server)
        .await;
    let spec = spec(&server.uri(), Some(now_ms() + 600_000));
    let session = CredentialSession::new(spec.credentials.clone()).unwrap();
    session.state.lock().await.refresh_at_ms = Some(0);
    for _ in 0..2 {
        let error = session
            .credential("s3://bucket/table", &spec.credentials[0].s3.connection)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("403"));
    }
}

#[tokio::test]
async fn oauth_refresh_reacquires_rejected_bearer_and_preserves_request_context() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use sail_common::storage::OAuth2ClientCredentials;
    let server = MockServer::start().await;
    let count = Arc::new(AtomicUsize::new(0));
    let token_count = count.clone();
    Mock::given(method("POST"))
        .and(path("/oauth/token"))
        .respond_with(move |request: &wiremock::Request| {
            let mut form = Url::parse("https://unused.example").unwrap();
            form.set_query(Some(std::str::from_utf8(&request.body).unwrap()));
            let form = form.query_pairs().collect::<HashMap<_, _>>();
            assert_eq!(form.get("grant_type").unwrap(), "client_credentials");
            assert_eq!(form.get("client_id").unwrap(), "worker-client");
            assert_eq!(form.get("client_secret").unwrap(), "oauth-secret&value");
            assert_eq!(form.get("scope").unwrap(), "catalog");
            let token = if token_count.fetch_add(1, Ordering::SeqCst) == 0 {
                "rejected-token"
            } else {
                "fresh-token"
            };
            ResponseTemplate::new(200).set_body_json(
                json!({"access_token": token, "token_type": "Bearer", "expires_in": 3600}),
            )
        })
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/credentials"))
        .and(header("authorization", "Bearer rejected-token"))
        .respond_with(ResponseTemplate::new(401))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/credentials"))
        .and(header("authorization", "Bearer fresh-token"))
        .and(header("x-catalog-context", "table-context"))
        .and(wiremock::matchers::query_param("referenced-by", "db.view"))
        .respond_with(ResponseTemplate::new(200).set_body_json(response(now_ms() + 600_000)))
        .expect(1)
        .mount(&server)
        .await;
    let mut spec = spec(&server.uri(), Some(0));
    let source = spec.credentials[0].refresh.as_mut().unwrap();
    source.endpoint.push_str("?referenced-by=db.view");
    source.headers.insert(
        "x-catalog-context".to_string(),
        StorageSecret::new("table-context".to_string()),
    );
    source.authentication = CatalogCredentialSource::OAuth2(OAuth2ClientCredentials {
        endpoint: format!("{}/oauth/token", server.uri()),
        client_id: "worker-client".to_string(),
        client_secret: StorageSecret::new("oauth-secret&value".to_string()),
        scope: Some("catalog".to_string()),
    });
    let spec: StorageAccessSpec =
        serde_json::from_slice(&serde_json::to_vec(&spec).unwrap()).unwrap();
    let session = CredentialSession::new(spec.credentials.clone()).unwrap();
    assert_eq!(
        session
            .credential("s3://bucket/table", &spec.credentials[0].s3.connection)
            .await
            .unwrap()
            .key_id,
        "refreshed-key"
    );
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
    assert!(store.get(&Path::from("other-table/file")).await.is_err());
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
    let session =
        CredentialSession::new(spec(&server.uri(), Some(now_ms() + 5000)).credentials).unwrap();
    let connection = session.state.lock().await.credentials[0]
        .s3
        .connection
        .clone();
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
    let session = CredentialSession::new(spec.credentials.clone()).unwrap();
    let response = json!({"storage-credentials": [{"prefix":"s3a://bucket/table", "config": entry("", &server.uri(), None).config}]});
    Mock::given(method("GET"))
        .and(path("/credentials"))
        .respond_with(ResponseTemplate::new(200).set_body_json(response))
        .expect(1)
        .mount(&server)
        .await;
    session.state.lock().await.refresh_at_ms = Some(0);
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
    let session = CredentialSession::new(spec(&server.uri(), Some(0)).credentials).unwrap();
    let connection = session.state.lock().await.credentials[0]
        .s3
        .connection
        .clone();
    let error = session
        .credential("s3://bucket/table", &connection)
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
    let session = CredentialSession::new(spec(&server.uri(), Some(0)).credentials).unwrap();
    let connection = session.state.lock().await.credentials[0]
        .s3
        .connection
        .clone();
    assert!(
        session
            .credential("s3://bucket/table", &connection)
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
    let session = CredentialSession::new(spec.credentials.clone()).unwrap();
    let connection = session.state.lock().await.credentials[0]
        .s3
        .connection
        .clone();
    assert!(
        session
            .credential("s3://bucket/table", &connection)
            .await
            .is_err()
    );
    assert_eq!(session.state.lock().await.credentials, spec.credentials);
}

#[tokio::test]
async fn longest_prefix_route_and_cache_isolation() {
    let root = Arc::new(object_store::memory::InMemory::new());
    let nested = Arc::new(object_store::memory::InMemory::new());
    root.put(&Path::from("table-x/file"), "root".into())
        .await
        .unwrap();
    nested
        .put(&Path::from("table/file"), "nested".into())
        .await
        .unwrap();
    let router = CredentialRoutingStore::new(vec![
        (String::new(), root.clone()),
        ("table/".to_string(), nested),
    ]);
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
    let directory = Path::from("table");
    for listing in [
        router.list(Some(&directory)),
        router.list_with_offset(Some(&directory), &Path::from("table/aaa")),
    ] {
        let objects = listing
            .map_ok(|object| object.location)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(objects, vec![Path::from("table/file")]);
    }
    let listing = router.list_with_delimiter(Some(&directory)).await.unwrap();
    assert_eq!(listing.objects.len(), 1);
    assert_eq!(listing.objects[0].location, Path::from("table/file"));
    let literal = CredentialRoutingStore::new(vec![("table".to_string(), root)]);
    assert_eq!(
        literal
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
