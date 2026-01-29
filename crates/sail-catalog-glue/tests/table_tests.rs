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

//! Integration tests for Glue catalog table operations.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use common::{col, setup_with_database, simple_table_options};
use sail_catalog::provider::{
    CatalogPartitionField, CatalogProvider, CreateTableColumnOptions, CreateTableOptions,
    DropTableOptions,
};

/// Tests table creation in Glue catalog.
///
/// - Creates a table with multiple column types, nullability, comments, location, format, partitioning, and properties
/// - Verifies returned table has correct name, database, columns, comment, location, format, partition_by, and properties
/// - Duplicate creation without `if_not_exists` fails
/// - Duplicate creation with `if_not_exists=true` returns existing table unchanged
#[tokio::test]
#[ignore]
async fn test_create_table() {
    let (catalog, _container, namespace) = setup_with_database("test_create_table").await;

    // Test creating a table with various column types
    let columns = vec![
        CreateTableColumnOptions {
            nullable: false,
            comment: Some("Primary key".to_string()),
            ..col("id", DataType::Int64)
        },
        col("name", DataType::Utf8),
        col("price", DataType::Float64),
        CreateTableColumnOptions {
            nullable: false,
            ..col("category", DataType::Utf8)
        },
    ];

    let created_table = catalog
        .create_table(
            &namespace,
            "products",
            CreateTableOptions {
                columns,
                comment: Some("Product catalog table".to_string()),
                constraints: vec![],
                location: Some("s3://bucket/products".to_string()),
                format: "parquet".to_string(),
                partition_by: vec![CatalogPartitionField {
                    column: "category".to_string(),
                    transform: None,
                }],
                sort_by: vec![],
                bucket_by: None,
                if_not_exists: false,
                replace: false,
                options: vec![],
                properties: vec![("owner".to_string(), "test_user".to_string())],
            },
        )
        .await
        .unwrap();

    assert_eq!(created_table.name, "products");
    assert_eq!(
        created_table.database,
        vec!["test_create_table_db".to_string()]
    );

    // Verify the table kind
    match &created_table.kind {
        sail_common_datafusion::catalog::TableKind::Table {
            columns,
            comment,
            location,
            format,
            partition_by,
            properties,
            ..
        } => {
            assert_eq!(columns.len(), 4);
            assert_eq!(comment, &Some("Product catalog table".to_string()));
            assert_eq!(location, &Some("s3://bucket/products".to_string()));
            assert_eq!(format, "parquet");
            assert_eq!(partition_by, &vec!["category".to_string()]);
            assert!(properties
                .iter()
                .any(|(k, v)| k == "owner" && v == "test_user"));
        }
        _ => panic!("Expected Table kind"),
    }

    // Test creating duplicate without if_not_exists - should fail
    let result = catalog
        .create_table(
            &namespace,
            "products",
            simple_table_options(vec![col("id", DataType::Int32)]),
        )
        .await;
    assert!(result.is_err());

    // Test creating with if_not_exists=true - should succeed and return existing
    let existing_table = catalog
        .create_table(
            &namespace,
            "products",
            CreateTableOptions {
                columns: vec![col("different", DataType::Int32)],
                comment: Some("Different comment".to_string()),
                if_not_exists: true,
                ..simple_table_options(vec![])
            },
        )
        .await
        .unwrap();

    // Should return the original table, not the new definition
    assert_eq!(existing_table.name, "products");
    match &existing_table.kind {
        sail_common_datafusion::catalog::TableKind::Table { comment, .. } => {
            assert_eq!(comment, &Some("Product catalog table".to_string()));
        }
        _ => panic!("Expected Table kind"),
    }
}

/// Tests retrieving a table from Glue catalog.
///
/// - Non-existent table returns `NotFound` error
/// - Creates a table with columns, comment, location, format, and properties
/// - Retrieves and verifies all fields match (name, database, columns, comment, location, format, properties)
/// - Verifies column details including data types and comments
#[tokio::test]
#[ignore]
async fn test_get_table() {
    let (catalog, _container, namespace) = setup_with_database("test_get_table").await;

    // Test getting non-existent table returns NotFound error
    let result = catalog.get_table(&namespace, "nonexistent").await;
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        sail_catalog::error::CatalogError::NotFound(_, _)
    ));

    // Create a table
    let columns = vec![
        CreateTableColumnOptions {
            nullable: false,
            comment: Some("The ID".to_string()),
            ..col("id", DataType::Int64)
        },
        col("value", DataType::Utf8),
    ];

    catalog
        .create_table(
            &namespace,
            "test_table",
            CreateTableOptions {
                columns,
                comment: Some("Test table description".to_string()),
                constraints: vec![],
                location: Some("s3://bucket/test_table".to_string()),
                format: "parquet".to_string(),
                partition_by: vec![],
                sort_by: vec![],
                bucket_by: None,
                if_not_exists: false,
                replace: false,
                options: vec![],
                properties: vec![("key1".to_string(), "value1".to_string())],
            },
        )
        .await
        .unwrap();

    // Now get the table and verify
    let table = catalog.get_table(&namespace, "test_table").await.unwrap();

    assert_eq!(table.name, "test_table");
    assert_eq!(table.database, vec!["test_get_table_db".to_string()]);

    match &table.kind {
        sail_common_datafusion::catalog::TableKind::Table {
            columns,
            comment,
            location,
            format,
            properties,
            ..
        } => {
            assert_eq!(columns.len(), 2);
            assert_eq!(comment, &Some("Test table description".to_string()));
            assert_eq!(location, &Some("s3://bucket/test_table".to_string()));
            assert_eq!(format, "parquet");
            assert!(properties.iter().any(|(k, v)| k == "key1" && v == "value1"));

            // Check column details
            let id_col = columns.iter().find(|c| c.name == "id").unwrap();
            assert_eq!(id_col.data_type, DataType::Int64);
            assert_eq!(id_col.comment, Some("The ID".to_string()));

            let value_col = columns.iter().find(|c| c.name == "value").unwrap();
            assert_eq!(value_col.data_type, DataType::Utf8);
        }
        _ => panic!("Expected Table kind"),
    }
}

/// Tests all supported Arrow data types can be created as Glue table columns.
///
/// - Simple types: boolean, tinyint, smallint, int, bigint, float, double, string, binary, date, timestamp
/// - Parameterized type: decimal(10,2)
/// - Complex types: array, struct, map
/// - Verifies all 15 column types round-trip correctly through Glue
#[tokio::test]
#[ignore]
async fn test_column_types() {
    let (catalog, _container, namespace) = setup_with_database("test_column_types").await;

    // Create columns with all supported types
    let columns = vec![
        // Simple types
        col("col_boolean", DataType::Boolean),
        col("col_tinyint", DataType::Int8),
        col("col_smallint", DataType::Int16),
        col("col_int", DataType::Int32),
        col("col_bigint", DataType::Int64),
        col("col_float", DataType::Float32),
        col("col_double", DataType::Float64),
        col("col_string", DataType::Utf8),
        col("col_binary", DataType::Binary),
        col("col_date", DataType::Date32),
        col(
            "col_timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ),
        // Parameterized type
        col("col_decimal", DataType::Decimal128(10, 2)),
        // Complex types
        col(
            "col_array",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        ),
        col(
            "col_struct",
            DataType::Struct(Fields::from(vec![
                Field::new("name", DataType::Utf8, true),
                Field::new("value", DataType::Int32, true),
            ])),
        ),
        col(
            "col_map",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Int32, true),
                    ])),
                    false,
                )),
                false,
            ),
        ),
    ];

    let created_table = catalog
        .create_table(
            &namespace,
            "all_types",
            CreateTableOptions {
                columns,
                comment: Some("Table with all supported column types".to_string()),
                location: Some("s3://bucket/all_types".to_string()),
                ..simple_table_options(vec![])
            },
        )
        .await
        .unwrap();

    assert_eq!(created_table.name, "all_types");

    // Verify column count
    match &created_table.kind {
        sail_common_datafusion::catalog::TableKind::Table { columns, .. } => {
            assert_eq!(columns.len(), 15, "Expected 15 columns");

            // Verify each column type was round-tripped correctly
            let find_col = |name: &str| columns.iter().find(|c| c.name == name).unwrap();

            assert_eq!(find_col("col_boolean").data_type, DataType::Boolean);
            assert_eq!(find_col("col_tinyint").data_type, DataType::Int8);
            assert_eq!(find_col("col_smallint").data_type, DataType::Int16);
            assert_eq!(find_col("col_int").data_type, DataType::Int32);
            assert_eq!(find_col("col_bigint").data_type, DataType::Int64);
            assert_eq!(find_col("col_float").data_type, DataType::Float32);
            assert_eq!(find_col("col_double").data_type, DataType::Float64);
            assert_eq!(find_col("col_string").data_type, DataType::Utf8);
            assert_eq!(find_col("col_binary").data_type, DataType::Binary);
            assert_eq!(find_col("col_date").data_type, DataType::Date32);
            assert_eq!(
                find_col("col_timestamp").data_type,
                DataType::Timestamp(TimeUnit::Microsecond, None)
            );
            assert_eq!(
                find_col("col_decimal").data_type,
                DataType::Decimal128(10, 2)
            );

            // Complex types - verify structure
            assert!(matches!(find_col("col_array").data_type, DataType::List(_)));
            assert!(matches!(
                find_col("col_struct").data_type,
                DataType::Struct(_)
            ));
            assert!(matches!(find_col("col_map").data_type, DataType::Map(_, _)));
        }
        _ => panic!("Expected Table kind"),
    }
}

/// Tests that unsupported column types are rejected.
///
/// - Attempts to create a table with a Union type column
/// - Verifies creation fails with an error
#[tokio::test]
#[ignore]
async fn test_unsupported_column_types() {
    let (catalog, _container, namespace) = setup_with_database("unsupported_types_test").await;

    let unsupported_columns = vec![col(
        "col_union",
        DataType::Union(
            arrow::datatypes::UnionFields::try_new(
                vec![0, 1],
                vec![
                    Field::new("int_field", DataType::Int32, true),
                    Field::new("str_field", DataType::Utf8, true),
                ],
            )
            .unwrap(),
            arrow::datatypes::UnionMode::Sparse,
        ),
    )];

    let result = catalog
        .create_table(
            &namespace,
            "unsupported_types",
            CreateTableOptions {
                columns: unsupported_columns,
                location: Some("s3://bucket/unsupported".to_string()),
                ..simple_table_options(vec![])
            },
        )
        .await;

    assert!(result.is_err(), "Expected error for unsupported Union type");
}

/// Tests all supported storage formats are accepted and round-trip correctly.
///
/// - Creates tables with formats: parquet, csv, json, orc, avro
/// - Retrieves each table and verifies the format is preserved
#[tokio::test]
#[ignore]
async fn test_storage_formats() {
    let (catalog, _container, namespace) = setup_with_database("format_test").await;

    let formats = ["parquet", "csv", "json", "orc", "avro"];

    for format in formats {
        let table_name = format!("test_{format}_table");

        let columns = vec![
            CreateTableColumnOptions {
                nullable: false,
                ..col("id", DataType::Int32)
            },
            col("name", DataType::Utf8),
        ];

        let created_table = catalog
            .create_table(
                &namespace,
                &table_name,
                CreateTableOptions {
                    columns,
                    comment: Some(format!("Table with {format} format")),
                    constraints: vec![],
                    location: Some(format!("s3://bucket/{table_name}")),
                    format: format.to_string(),
                    partition_by: vec![],
                    sort_by: vec![],
                    bucket_by: None,
                    if_not_exists: false,
                    replace: false,
                    options: vec![],
                    properties: vec![],
                },
            )
            .await
            .unwrap_or_else(|e| panic!("Failed to create {format} table: {e}"));

        assert_eq!(created_table.name, table_name);

        let retrieved_table = catalog
            .get_table(&namespace, &table_name)
            .await
            .unwrap_or_else(|e| panic!("Failed to get {format} table: {e}"));

        match &retrieved_table.kind {
            sail_common_datafusion::catalog::TableKind::Table {
                format: detected_format,
                ..
            } => {
                assert_eq!(
                    detected_format, format,
                    "Format mismatch for {format}: expected '{format}', got '{detected_format}'"
                );
            }
            _ => panic!("Expected Table kind for {format}"),
        }
    }
}

/// Tests listing tables in a database.
///
/// - Creates multiple tables in a database
/// - `list_tables` returns all created tables
/// - All table names are present in the list
#[tokio::test]
#[ignore]
async fn test_list_tables() {
    let (catalog, _container, namespace) = setup_with_database("test_list_tables").await;

    // Create multiple tables
    let table_names = ["table_alpha", "table_beta", "table_gamma"];

    for table_name in &table_names {
        catalog
            .create_table(
                &namespace,
                table_name,
                simple_table_options(vec![col("id", DataType::Int32)]),
            )
            .await
            .unwrap();
    }

    // List tables
    let tables = catalog.list_tables(&namespace).await.unwrap();

    // Verify all tables are returned
    assert_eq!(tables.len(), table_names.len());

    let returned_names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
    for name in &table_names {
        assert!(
            returned_names.contains(name),
            "Expected table '{name}' not found in list"
        );
    }
}

/// Tests dropping tables from Glue catalog.
///
/// - Creates a table and verifies it exists
/// - Drops the table and verifies it no longer exists
/// - Dropping non-existent table with `if_exists=false` fails
/// - Dropping non-existent table with `if_exists=true` succeeds
#[tokio::test]
#[ignore]
async fn test_drop_table() {
    let (catalog, _container, namespace) = setup_with_database("test_drop_table").await;

    // Create a table
    catalog
        .create_table(
            &namespace,
            "drop_me",
            simple_table_options(vec![col("id", DataType::Int32)]),
        )
        .await
        .unwrap();

    // Verify table exists
    let table = catalog.get_table(&namespace, "drop_me").await;
    assert!(table.is_ok());

    // Drop the table
    let drop_options = DropTableOptions {
        if_exists: false,
        purge: false,
    };
    catalog
        .drop_table(&namespace, "drop_me", drop_options)
        .await
        .unwrap();

    // Verify table no longer exists
    let result = catalog.get_table(&namespace, "drop_me").await;
    assert!(result.is_err());

    // Dropping non-existent table with if_exists=false should error
    let drop_options = DropTableOptions {
        if_exists: false,
        purge: false,
    };
    let result = catalog
        .drop_table(&namespace, "nonexistent", drop_options)
        .await;
    assert!(result.is_err());

    // Dropping non-existent table with if_exists=true should succeed
    let drop_options = DropTableOptions {
        if_exists: true,
        purge: false,
    };
    let result = catalog
        .drop_table(&namespace, "nonexistent", drop_options)
        .await;
    assert!(result.is_ok());
}
