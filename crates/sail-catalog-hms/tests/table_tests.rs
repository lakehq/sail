#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use arrow::datatypes::DataType;
use sail_catalog::error::{CatalogError, CatalogObject};
use sail_catalog::provider::{CatalogProvider, CreateViewColumnOptions, CreateViewOptions};
use sail_common_datafusion::catalog::TableKind;

use crate::common::{
    col, setup_with_database, simple_table_options, simple_table_options_with_format,
};

#[tokio::test]
#[ignore]
async fn test_create_get_drop_table() {
    let test_name = "test_create_get_drop_table";
    let context = setup_with_database(test_name).await;
    let catalog = &context.catalog;
    let namespace = &context.namespace;

    let created = catalog
        .create_table(
            namespace,
            "items",
            simple_table_options(test_name, vec![col("id", DataType::Int64)]),
        )
        .await
        .unwrap();

    assert_eq!(created.name, "items");
    match created.kind {
        TableKind::Table { format, .. } => assert_eq!(format, "parquet"),
        other => panic!("expected table kind, got {other:?}"),
    }

    let fetched = catalog.get_table(namespace, "items").await.unwrap();
    assert_eq!(fetched.name, "items");

    let tables = catalog.list_tables(namespace).await.unwrap();
    assert!(tables.iter().any(|table| table.name == "items"));

    catalog
        .drop_table(
            namespace,
            "items",
            sail_catalog::provider::DropTableOptions {
                if_exists: false,
                purge: false,
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
#[ignore]
async fn test_supported_formats_round_trip() {
    let test_name = "test_supported_formats_round_trip";
    let context = setup_with_database(test_name).await;
    let catalog = &context.catalog;
    let namespace = &context.namespace;

    let cases = [
        ("parquet_items", "parquet", "parquet"),
        ("csv_items", "csv", "csv"),
        ("delta_items", "delta", "delta"),
        ("deltalake_items", "deltalake", "delta"),
        ("textfile_items", "textfile", "textfile"),
        ("json_items", "json", "json"),
        ("orc_items", "orc", "orc"),
        ("avro_items", "avro", "avro"),
    ];

    for (table_name, requested_format, expected_format) in cases {
        let created = catalog
            .create_table(
                namespace,
                table_name,
                simple_table_options_with_format(
                    &format!("{test_name}_{table_name}"),
                    vec![col("id", DataType::Int64), col("value", DataType::Utf8)],
                    requested_format,
                ),
            )
            .await
            .unwrap();

        match created.kind {
            TableKind::Table { format, .. } => assert_eq!(format, expected_format),
            other => panic!("expected table kind, got {other:?}"),
        }

        let fetched = catalog.get_table(namespace, table_name).await.unwrap();
        match fetched.kind {
            TableKind::Table { format, .. } => assert_eq!(format, expected_format),
            other => panic!("expected table kind, got {other:?}"),
        }
    }
}

#[tokio::test]
#[ignore]
async fn test_get_table_not_found_for_view() {
    let test_name = "test_get_table_not_found_for_view";
    let context = setup_with_database(test_name).await;
    let catalog = &context.catalog;
    let namespace = &context.namespace;

    catalog
        .create_view(
            namespace,
            "v_items",
            CreateViewOptions {
                columns: vec![CreateViewColumnOptions {
                    name: "id".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    comment: None,
                }],
                definition: "SELECT 1 AS id".to_string(),
                if_not_exists: false,
                replace: false,
                comment: None,
                properties: vec![],
            },
        )
        .await
        .unwrap();

    let error = catalog.get_table(namespace, "v_items").await.unwrap_err();
    assert!(matches!(
        error,
        CatalogError::NotFound(CatalogObject::Table, _)
    ));
}

#[tokio::test]
#[ignore]
async fn test_list_tables_excludes_views() {
    let test_name = "test_list_tables_excludes_views";
    let context = setup_with_database(test_name).await;
    let catalog = &context.catalog;
    let namespace = &context.namespace;

    catalog
        .create_table(
            namespace,
            "items",
            simple_table_options(test_name, vec![col("id", DataType::Int64)]),
        )
        .await
        .unwrap();
    catalog
        .create_view(
            namespace,
            "v_items",
            CreateViewOptions {
                columns: vec![CreateViewColumnOptions {
                    name: "id".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    comment: None,
                }],
                definition: "SELECT id FROM items".to_string(),
                if_not_exists: false,
                replace: false,
                comment: None,
                properties: vec![],
            },
        )
        .await
        .unwrap();

    let tables = catalog.list_tables(namespace).await.unwrap();
    assert!(tables.iter().any(|table| table.name == "items"));
    assert!(tables.iter().all(|table| table.name != "v_items"));
}
