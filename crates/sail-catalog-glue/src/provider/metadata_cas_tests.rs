#![expect(clippy::expect_used, clippy::panic)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use sail_catalog::lakehouse::{IcebergMetadataCasOutcome as Outcome, IcebergMetadataCasRequest};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::*;

#[tokio::test]
async fn cas_requires_version_and_never_replays_an_ambiguous_mutation() {
    for fault in [
        "success",
        "conflict",
        "503",
        "disconnect",
        "malformed",
        "missing-version",
        "aliases",
        "stale",
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let endpoint = format!("http://{}", listener.local_addr().expect("address"));
        let writes = Arc::new(Mutex::new(Vec::<Value>::new()));
        let recorded = writes.clone();
        let server = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.expect("accept");
                let mut bytes = vec![];
                let header_end = loop {
                    let byte = socket.read_u8().await.expect("request header");
                    bytes.push(byte);
                    if bytes.ends_with(b"\r\n\r\n") {
                        break bytes.len();
                    }
                };
                let header = String::from_utf8_lossy(&bytes).to_lowercase();
                let size: usize = header
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length:"))
                    .expect("length")
                    .trim()
                    .parse()
                    .expect("size");
                let is_update = header.contains("awsglue.updatetable");
                bytes.resize(header_end + size, 0);
                socket
                    .read_exact(&mut bytes[header_end..])
                    .await
                    .expect("body");
                let request: Value = serde_json::from_slice(&bytes[header_end..]).expect("JSON");
                let (status, response) = if is_update {
                    recorded.lock().expect("writes").push(request);
                    if fault == "disconnect" {
                        continue;
                    }
                    match fault {
                        "conflict" => (400, json!({"__type":"ConcurrentModificationException", "Message":"stale version"}).to_string()),
                        "503" => (503, json!({"__type":"InternalServiceException", "Message":"lost outcome"}).to_string()),
                        "malformed" => (200, "not-json".into()),
                        _ => (200, "{}".into()),
                    }
                } else {
                    let mut table = json!({"Name":"t", "VersionId":"version-7", "StorageDescriptor":{"Location":"s3://bucket/t"},
                        "Parameters":{"table_type":"ICEBERG", "metadata_location":"old", "metadata-location":"old", "MeTaDaTa-LoCaTiOn":"old", "owner":"preserved"}});
                    if fault == "aliases" {
                        table["Parameters"]["metadata-location"] = json!("inconsistent");
                    }
                    if fault == "missing-version" {
                        table.as_object_mut().expect("object").remove("VersionId");
                    }
                    (200, json!({"Table":table}).to_string())
                };
                let response = format!(
                    "HTTP/1.1 {status} Response\r\nContent-Type: application/x-amz-json-1.1\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",
                    response.len()
                );
                socket
                    .write_all(response.as_bytes())
                    .await
                    .expect("response");
            }
        });
        let provider = GlueCatalogProvider::new_with_static_credentials(
            "glue".into(),
            GlueCatalogConfig {
                region: Some("us-east-1".into()),
                endpoint_url: Some(endpoint),
                ..Default::default()
            },
            "test".into(),
            "test".into(),
            None,
        );
        let request = IcebergMetadataCasRequest::new(
            if fault == "stale" { "stale" } else { "old" }.into(),
            "new".into(),
        )
        .expect("request");
        let outcome = tokio::time::timeout(
            Duration::from_secs(15),
            provider.compare_and_swap_iceberg_metadata(
                &Namespace::try_from(vec!["db".to_string()]).expect("namespace"),
                "t",
                request,
            ),
        )
        .await
        .expect("bounded CAS")
        .expect("outcome");
        match fault {
            "success" => assert_eq!(outcome, Outcome::Committed),
            "conflict" | "stale" => {
                assert!(matches!(outcome, Outcome::Conflict { .. }), "{outcome:?}")
            }
            "missing-version" | "aliases" => {
                assert!(matches!(outcome, Outcome::Rejected { .. }), "{outcome:?}")
            }
            _ => assert!(matches!(outcome, Outcome::Unknown { .. }), "{outcome:?}"),
        }
        server.abort();
        let writes = writes.lock().expect("writes");
        assert_eq!(
            writes.len(),
            usize::from(!matches!(fault, "missing-version" | "aliases" | "stale")),
            "{fault}: publication must not be replayed"
        );
        if let Some(write) = writes.first() {
            assert_eq!(write["VersionId"], "version-7");
            assert_eq!(
                write["TableInput"]["Parameters"]["metadata_location"],
                "new"
            );
            assert_eq!(write["TableInput"]["Parameters"]["owner"], "preserved");
            assert_eq!(
                write["TableInput"]["Parameters"]
                    .as_object()
                    .expect("parameters")
                    .keys()
                    .filter(|key| {
                        sail_common_datafusion::catalog::managed::is_metadata_location_key(key)
                    })
                    .count(),
                1
            );
        }
    }
}
