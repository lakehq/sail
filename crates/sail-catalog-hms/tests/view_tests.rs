#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use arrow::datatypes::DataType;
use sail_catalog::error::{CatalogError, CatalogObject};
use sail_catalog::provider::{CatalogProvider, CreateViewColumnOptions};
use sail_common_datafusion::catalog::TableKind;

use crate::common::{col, setup_with_database, simple_table_options};

#[tokio::test]
#[ignore]
async fn test_create_get_drop_view() {
    let context = setup_with_database("test_create_get_drop_view").await;
    let catalog = &context.catalog;
    let namespace = &context.namespace;

    let created = catalog
        .create_view(
            namespace,
            "v_items",
            sail_catalog::provider::CreateViewOptions {
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

    assert_eq!(created.name, "v_items");
    match created.kind {
        TableKind::View { definition, .. } => assert_eq!(definition, "SELECT 1 AS id"),
        other => panic!("expected view kind, got {other:?}"),
    }

    let fetched = catalog.get_view(namespace, "v_items").await.unwrap();
    assert_eq!(fetched.name, "v_items");

    let views = catalog.list_views(namespace).await.unwrap();
    assert!(views.iter().any(|view| view.name == "v_items"));

    catalog
        .drop_view(
            namespace,
            "v_items",
            sail_catalog::provider::DropViewOptions { if_exists: false },
        )
        .await
        .unwrap();
}

#[tokio::test]
#[ignore]
async fn test_get_view_not_found_for_table() {
    let test_name = "test_get_view_not_found_for_table";
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

    let error = catalog.get_view(namespace, "items").await.unwrap_err();
    assert!(matches!(
        error,
        CatalogError::NotFound(CatalogObject::View, _)
    ));
}

#[tokio::test]
#[ignore]
async fn test_list_views_excludes_tables() {
    let test_name = "test_list_views_excludes_tables";
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
            sail_catalog::provider::CreateViewOptions {
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

    let views = catalog.list_views(namespace).await.unwrap();
    assert!(views.iter().any(|view| view.name == "v_items"));
    assert!(views.iter().all(|view| view.name != "items"));
}
