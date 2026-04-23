#![expect(clippy::unwrap_used)]

mod common;

use sail_catalog::provider::CatalogProvider;
use sail_catalog_hms::{HmsCatalogConfig, HmsCatalogProvider};
use sail_common::runtime::RuntimeHandle;

const HIVE_METASTORE_PORT: u16 = 9083;

#[tokio::test]
#[ignore = "requires the HMS testcontainers harness"]
async fn test_failover_from_dead_primary_endpoint() {
    let context = common::setup_hms_catalog("hms_failover_dead_primary").await;
    let host = context.container.get_host().await.unwrap();
    let port = context
        .container
        .get_host_port_ipv4(HIVE_METASTORE_PORT)
        .await
        .unwrap();

    let provider = HmsCatalogProvider::new(
        "hms_failover".to_string(),
        HmsCatalogConfig {
            uris: vec!["127.0.0.1:1".to_string(), format!("{host}:{port}")],
            warehouse: None,
            thrift_transport: None,
            auth: None,
            kerberos_service_principal: None,
            sasl_qop_min: None,
            connect_timeout_secs: None,
        },
        RuntimeHandle::new(
            tokio::runtime::Handle::current(),
            tokio::runtime::Handle::current(),
        ),
    );

    let databases = provider.list_databases(None).await.unwrap();
    assert!(!databases.is_empty());
}
