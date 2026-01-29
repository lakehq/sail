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

//! Integration tests for Glue catalog view operations.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use arrow::datatypes::DataType;
use common::{col, setup_with_database, simple_table_options, simple_view_options, view_col};
use sail_catalog::provider::{
    CatalogProvider, CreateViewColumnOptions, CreateViewOptions, DropViewOptions,
};

/// Tests view creation in Glue catalog.
///
/// - Creates a view with multiple column types and metadata (comment, properties)
/// - Verifies returned view has correct name, database, definition, columns, and properties
/// - Duplicate creation without `if_not_exists` fails
/// - Duplicate creation with `if_not_exists=true` returns existing view unchanged
#[tokio::test]
#[ignore]
async fn test_create_view() {
    let (catalog, _container, namespace) = setup_with_database("test_create_view").await;

    // Create a view with various column types
    let columns = vec![
        CreateViewColumnOptions {
            comment: Some("Primary key".to_string()),
            ..view_col("id", DataType::Int64)
        },
        view_col("name", DataType::Utf8),
        view_col("price", DataType::Float64),
    ];

    let created_view = catalog
        .create_view(
            &namespace,
            "product_view",
            CreateViewOptions {
                columns,
                definition: "SELECT id, name, price FROM products".to_string(),
                if_not_exists: false,
                replace: false,
                comment: Some("View of products".to_string()),
                properties: vec![("owner".to_string(), "test_user".to_string())],
            },
        )
        .await
        .unwrap();

    assert_eq!(created_view.name, "product_view");
    assert_eq!(
        created_view.database,
        vec!["test_create_view_db".to_string()]
    );

    // Verify the view kind
    match &created_view.kind {
        sail_common_datafusion::catalog::TableKind::View {
            definition,
            columns,
            comment,
            properties,
        } => {
            assert_eq!(definition, "SELECT id, name, price FROM products");
            assert_eq!(columns.len(), 3);
            assert_eq!(comment, &Some("View of products".to_string()));
            assert!(properties
                .iter()
                .any(|(k, v)| k == "owner" && v == "test_user"));
        }
        _ => panic!("Expected View kind"),
    }

    // Test creating duplicate without if_not_exists - should fail
    let result = catalog
        .create_view(
            &namespace,
            "product_view",
            simple_view_options("SELECT 1", vec![view_col("id", DataType::Int32)]),
        )
        .await;
    assert!(result.is_err());

    // Test creating with if_not_exists=true - should succeed and return existing
    let existing_view = catalog
        .create_view(
            &namespace,
            "product_view",
            CreateViewOptions {
                columns: vec![view_col("different", DataType::Int32)],
                definition: "SELECT 2".to_string(),
                if_not_exists: true,
                replace: false,
                comment: Some("Different comment".to_string()),
                properties: vec![],
            },
        )
        .await
        .unwrap();

    // Should return the original view, not the new definition
    assert_eq!(existing_view.name, "product_view");
    match &existing_view.kind {
        sail_common_datafusion::catalog::TableKind::View { comment, .. } => {
            assert_eq!(comment, &Some("View of products".to_string()));
        }
        _ => panic!("Expected View kind"),
    }
}

/// Tests retrieving a view from Glue catalog.
///
/// - Non-existent view returns `NotFound` error
/// - Creates a view with columns, definition, comment, and properties
/// - Retrieves and verifies all fields match (name, database, definition, columns, comment, properties)
/// - Verifies column details including data types and comments
#[tokio::test]
#[ignore]
async fn test_get_view() {
    let (catalog, _container, namespace) = setup_with_database("test_get_view").await;

    // Test getting non-existent view returns NotFound error
    let result = catalog.get_view(&namespace, "nonexistent").await;
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        sail_catalog::error::CatalogError::NotFound(_, _)
    ));

    // Create a view
    let columns = vec![
        CreateViewColumnOptions {
            comment: Some("The ID".to_string()),
            ..view_col("id", DataType::Int64)
        },
        view_col("value", DataType::Utf8),
    ];

    catalog
        .create_view(
            &namespace,
            "test_view",
            CreateViewOptions {
                columns,
                definition: "SELECT id, value FROM source_table".to_string(),
                if_not_exists: false,
                replace: false,
                comment: Some("Test view description".to_string()),
                properties: vec![("key1".to_string(), "value1".to_string())],
            },
        )
        .await
        .unwrap();

    // Now get the view and verify
    let view = catalog.get_view(&namespace, "test_view").await.unwrap();

    assert_eq!(view.name, "test_view");
    assert_eq!(view.database, vec!["test_get_view_db".to_string()]);

    match &view.kind {
        sail_common_datafusion::catalog::TableKind::View {
            definition,
            columns,
            comment,
            properties,
        } => {
            assert_eq!(definition, "SELECT id, value FROM source_table");
            assert_eq!(columns.len(), 2);
            assert_eq!(comment, &Some("Test view description".to_string()));
            assert!(properties.iter().any(|(k, v)| k == "key1" && v == "value1"));

            // Check column details
            let id_col = columns.iter().find(|c| c.name == "id").unwrap();
            assert_eq!(id_col.data_type, DataType::Int64);
            assert_eq!(id_col.comment, Some("The ID".to_string()));

            let value_col = columns.iter().find(|c| c.name == "value").unwrap();
            assert_eq!(value_col.data_type, DataType::Utf8);
        }
        _ => panic!("Expected View kind"),
    }
}

/// Tests that `get_view` returns `NotFound` when called on a table.
///
/// - Creates a table (not a view)
/// - Calling `get_view` on the table name returns `NotFound` error
#[tokio::test]
#[ignore]
async fn test_get_view_not_found_for_table() {
    let (catalog, _container, namespace) = setup_with_database("test_view_not_table").await;

    // Create a table (not a view)
    catalog
        .create_table(
            &namespace,
            "actual_table",
            simple_table_options(vec![col("id", DataType::Int32)]),
        )
        .await
        .unwrap();

    // Trying to get_view on a table should return NotFound
    let result = catalog.get_view(&namespace, "actual_table").await;
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        sail_catalog::error::CatalogError::NotFound(_, _)
    ));
}

/// Tests listing views in a database.
///
/// - Empty database returns empty list
/// - Creates multiple views and a table
/// - `list_views` returns only views, not tables
/// - All created views are present in the list
#[tokio::test]
#[ignore]
async fn test_list_views() {
    let (catalog, _container, namespace) = setup_with_database("test_list_views").await;

    // Empty list initially
    let views = catalog.list_views(&namespace).await.unwrap();
    assert!(views.is_empty());

    // Create multiple views
    let view_names = ["view_alpha", "view_beta", "view_gamma"];

    for view_name in &view_names {
        catalog
            .create_view(
                &namespace,
                view_name,
                simple_view_options(
                    &format!("SELECT * FROM {view_name}_source"),
                    vec![view_col("id", DataType::Int32)],
                ),
            )
            .await
            .unwrap();
    }

    // Also create a table to verify it's not included in list_views
    catalog
        .create_table(
            &namespace,
            "a_table",
            simple_table_options(vec![col("id", DataType::Int32)]),
        )
        .await
        .unwrap();

    // List views
    let views = catalog.list_views(&namespace).await.unwrap();

    // Verify only views are returned (not the table)
    assert_eq!(views.len(), view_names.len());

    let returned_names: Vec<&str> = views.iter().map(|v| v.name.as_str()).collect();
    for name in &view_names {
        assert!(
            returned_names.contains(name),
            "Expected view '{name}' not found in list"
        );
    }
    assert!(
        !returned_names.contains(&"a_table"),
        "Table should not appear in list_views"
    );
}

/// Tests dropping views from Glue catalog.
///
/// - Creates a view and verifies it exists
/// - Drops the view and verifies it no longer exists
/// - Dropping non-existent view with `if_exists=false` fails
/// - Dropping non-existent view with `if_exists=true` succeeds
#[tokio::test]
#[ignore]
async fn test_drop_view() {
    let (catalog, _container, namespace) = setup_with_database("test_drop_view").await;

    // Create a view
    catalog
        .create_view(
            &namespace,
            "drop_me",
            simple_view_options("SELECT 1 AS id", vec![view_col("id", DataType::Int32)]),
        )
        .await
        .unwrap();

    // Verify view exists
    let view = catalog.get_view(&namespace, "drop_me").await;
    assert!(view.is_ok());

    // Drop the view
    let drop_options = DropViewOptions { if_exists: false };
    catalog
        .drop_view(&namespace, "drop_me", drop_options)
        .await
        .unwrap();

    // Verify view no longer exists
    let result = catalog.get_view(&namespace, "drop_me").await;
    assert!(result.is_err());

    // Dropping non-existent view with if_exists=false should error
    let drop_options = DropViewOptions { if_exists: false };
    let result = catalog
        .drop_view(&namespace, "nonexistent", drop_options)
        .await;
    assert!(result.is_err());

    // Dropping non-existent view with if_exists=true should succeed
    let drop_options = DropViewOptions { if_exists: true };
    let result = catalog
        .drop_view(&namespace, "nonexistent", drop_options)
        .await;
    assert!(result.is_ok());
}
