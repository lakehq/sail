#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;

use sail_catalog::provider::{CatalogProvider, Namespace};
use sail_catalog_iceberg::apis::configuration::Configuration;
use sail_catalog_iceberg::IcebergRestCatalogProvider;
use sail_common::runtime::RuntimeHandle;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

async fn setup_catalog() -> (
    IcebergRestCatalogProvider,
    ContainerAsync<GenericImage>,
    ContainerAsync<GenericImage>,
    ContainerAsync<GenericImage>,
) {
    let minio = GenericImage::new("minio/minio", "RELEASE.2025-09-07T16-13-09Z")
        .with_wait_for(WaitFor::message_on_stderr("MinIO Object Storage Server"))
        .with_exposed_port(ContainerPort::Tcp(9000))
        .with_exposed_port(ContainerPort::Tcp(9001))
        .with_env_var("MINIO_ROOT_USER", "admin")
        .with_env_var("MINIO_ROOT_PASSWORD", "password")
        .with_env_var("MINIO_DOMAIN", "minio")
        .with_cmd(vec!["server", "/data", "--console-address", ":9001"])
        .start()
        .await
        .expect("Failed to start MinIO");

    let minio_host = minio.get_host().await.expect("get host");
    let minio_port = minio.get_host_port_ipv4(9000).await.expect("get port");
    let minio_endpoint = format!("http://{minio_host}:{minio_port}");

    let mc_cmd = format!(
        "until (/usr/bin/mc alias set minio {minio_endpoint} admin password) do echo '...waiting...' && sleep 1; done; /usr/bin/mc rm -r --force minio/icebergdata; /usr/bin/mc mb minio/icebergdata; /usr/bin/mc policy set public minio/icebergdata; tail -f /dev/null",
    );

    let _mc = GenericImage::new("minio/mc", "RELEASE.2025-08-13T08-35-41Z")
        .with_env_var("AWS_ACCESS_KEY_ID", "admin")
        .with_env_var("AWS_SECRET_ACCESS_KEY", "password")
        .with_env_var("AWS_REGION", "us-east-1")
        .with_cmd(vec!["/bin/sh", "-c", &mc_cmd])
        .start()
        .await
        .expect("Failed to start MC");

    let rest = GenericImage::new("apache/iceberg-rest-fixture", "latest")
        .with_wait_for(WaitFor::message_on_stderr(
            "INFO org.eclipse.jetty.server.Server - Started ",
        ))
        .with_exposed_port(ContainerPort::Tcp(8181))
        .with_env_var("AWS_ACCESS_KEY_ID", "admin")
        .with_env_var("AWS_SECRET_ACCESS_KEY", "password")
        .with_env_var("AWS_REGION", "us-east-1")
        .with_env_var(
            "CATALOG_CATALOG__IMPL",
            "org.apache.iceberg.jdbc.JdbcCatalog",
        )
        .with_env_var(
            "CATALOG_URI",
            "jdbc:sqlite:file:/tmp/iceberg_rest_mode=memory",
        )
        .with_env_var("CATALOG_WAREHOUSE", "s3://icebergdata/demo")
        .with_env_var("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
        .with_env_var("CATALOG_S3_ENDPOINT", &minio_endpoint)
        .start()
        .await
        .expect("Failed to start REST catalog");

    let rest_host = rest.get_host().await.expect("get host");
    let rest_port = rest.get_host_port_ipv4(8181).await.expect("get port");
    let rest_url = format!("http://{rest_host}:{rest_port}");

    let runtime = RuntimeHandle::new(tokio::runtime::Handle::current(), None);
    let config = Arc::new(Configuration {
        base_path: rest_url,
        user_agent: None,
        client: reqwest::Client::new(),
        basic_auth: None,
        oauth_access_token: None,
        bearer_access_token: None,
        api_key: None,
    });

    let catalog = IcebergRestCatalogProvider::new(
        "test_catalog".to_string(),
        "".to_string(),
        config,
        runtime,
    );

    // CHECK HERE
    let _config_result = catalog.load_config(Some("s3://icebergdata/demo")).await;

    (catalog, minio, _mc, rest)
}

#[tokio::test]
async fn test_get_non_exist_namespace() {
    let (catalog, _minio, _mc, _rest) = setup_catalog().await;

    let namespace = Namespace::try_from(vec!["test_get_non_exist_namespace".to_string()]).unwrap();
    let result = catalog.get_database(&namespace).await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        sail_catalog::error::CatalogError::NotFound(_, _)
    ));
}
