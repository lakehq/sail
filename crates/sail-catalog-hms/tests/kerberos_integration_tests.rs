#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use arrow::datatypes::DataType;
use sail_catalog::provider::{CatalogProvider, DropDatabaseOptions, DropTableOptions, Namespace};

use crate::common::{
    KerberosHmsTestContext, col, is_kerberos_hms_subprocess,
    run_kerberos_hms_catalog_test_in_subprocess, setup_kerberos_hms_catalog_from_subprocess_env,
    simple_database_options, simple_table_options,
};

#[tokio::test]
#[ignore = "requires the Kerberos HMS testcontainers harness"]
async fn test_kerberos_hms_database_and_table_round_trip() {
    run_kerberos_hms_catalog_test_in_subprocess(
        "test_kerberos_hms_database_and_table_round_trip",
        "kerberos_hms_database_and_table_round_trip_subprocess",
    )
    .await;
}

#[tokio::test]
#[ignore = "subprocess helper for the Kerberos HMS testcontainers harness"]
async fn kerberos_hms_database_and_table_round_trip_subprocess() {
    if !is_kerberos_hms_subprocess() {
        return;
    }
    let context = setup_kerberos_hms_catalog_from_subprocess_env(true).await;
    kerberos_hms_database_and_table_round_trip(context).await;
}

async fn kerberos_hms_database_and_table_round_trip(context: KerberosHmsTestContext) {
    let catalog = &context.catalog;
    let db_name = "kerberos_hms_db".to_string();
    let namespace = Namespace::try_from(vec![db_name.clone()]).unwrap();

    catalog
        .create_database(&namespace, simple_database_options())
        .await
        .unwrap();
    let database = catalog.get_database(&namespace).await.unwrap();
    assert_eq!(database.database, vec![db_name.clone()]);

    let table_name = "items".to_string();
    let table = catalog
        .create_table(
            &namespace,
            &table_name,
            simple_table_options(
                &table_name,
                vec![col("id", DataType::Int64), col("value", DataType::Utf8)],
            ),
        )
        .await
        .unwrap();
    assert_eq!(table.name, table_name);

    let fetched = catalog.get_table(&namespace, &table_name).await.unwrap();
    assert_eq!(fetched.name, table_name);

    catalog
        .drop_table(
            &namespace,
            &table_name,
            DropTableOptions {
                if_exists: false,
                purge: false,
            },
        )
        .await
        .unwrap();
    catalog
        .drop_database(
            &namespace,
            DropDatabaseOptions {
                if_exists: false,
                cascade: false,
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires the Kerberos HMS testcontainers harness"]
async fn test_kerberos_hms_rejects_missing_credentials() {
    run_kerberos_hms_catalog_test_in_subprocess(
        "test_kerberos_hms_rejects_missing_credentials",
        "kerberos_hms_rejects_missing_credentials_subprocess",
    )
    .await;
}

#[tokio::test]
#[ignore = "subprocess helper for the Kerberos HMS testcontainers harness"]
async fn kerberos_hms_rejects_missing_credentials_subprocess() {
    if !is_kerberos_hms_subprocess() {
        return;
    }
    let context = setup_kerberos_hms_catalog_from_subprocess_env(false).await;
    let error = context.catalog.list_databases(None).await.unwrap_err();

    let message = error.to_string().to_ascii_lowercase();
    assert!(
        message.contains("kerberos") || message.contains("gssapi") || message.contains("sasl"),
        "expected kerberos-related failure, got: {error}"
    );
}
