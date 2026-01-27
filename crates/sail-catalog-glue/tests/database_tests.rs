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

//! Integration tests for Glue catalog database operations.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use common::{setup_glue_catalog, simple_database_options};
use sail_catalog::provider::{
    CatalogProvider, CreateDatabaseOptions, DropDatabaseOptions, Namespace,
};

/// Tests database creation in Glue catalog.
///
/// - Creates a database with comment, location, and properties
/// - Verifies returned database has correct name, comment, location, and properties
/// - Duplicate creation without `if_not_exists` fails
/// - Duplicate creation with `if_not_exists=true` returns existing database unchanged
#[tokio::test]
#[ignore]
async fn test_create_database() {
    let (catalog, _moto) = setup_glue_catalog("test_create_database").await;

    let namespace = Namespace::try_from(vec!["test_create_database".to_string()]).unwrap();

    let created_db = catalog
        .create_database(
            &namespace,
            CreateDatabaseOptions {
                if_not_exists: false,
                comment: Some("test comment".to_string()),
                location: Some("s3://bucket/path".to_string()),
                properties: vec![("key1".to_string(), "value1".to_string())],
            },
        )
        .await
        .unwrap();

    assert_eq!(
        created_db.database,
        vec!["test_create_database".to_string()]
    );
    assert_eq!(created_db.comment, Some("test comment".to_string()));
    assert_eq!(created_db.location, Some("s3://bucket/path".to_string()));
    assert!(created_db
        .properties
        .iter()
        .any(|(k, v)| k == "key1" && v == "value1"));

    // Test creating duplicate without if_not_exists - should fail
    let result = catalog
        .create_database(&namespace, simple_database_options())
        .await;
    assert!(result.is_err());

    // Test creating with if_not_exists=true
    let namespace2 = Namespace::try_from(vec!["test_create_database_2".to_string()]).unwrap();
    let created_db2 = catalog
        .create_database(
            &namespace2,
            CreateDatabaseOptions {
                if_not_exists: true,
                ..simple_database_options()
            },
        )
        .await
        .unwrap();

    assert_eq!(
        created_db2.database,
        vec!["test_create_database_2".to_string()]
    );

    // Test creating existing database with if_not_exists=true - should return existing
    let created_again = catalog
        .create_database(
            &namespace2,
            CreateDatabaseOptions {
                if_not_exists: true,
                comment: Some("should be ignored".to_string()),
                location: Some("should be ignored".to_string()),
                properties: vec![("ignored".to_string(), "ignored".to_string())],
            },
        )
        .await
        .unwrap();

    assert_eq!(
        created_again.database,
        vec!["test_create_database_2".to_string()]
    );
    // Original values should be preserved (comment was None)
    assert_eq!(created_again.comment, None);
}

/// Tests retrieving a database from Glue catalog.
///
/// - Non-existent database returns `NotFound` error
/// - Creates a database with comment, location, and properties
/// - Retrieves and verifies all fields match (name, comment, location, properties)
#[tokio::test]
#[ignore]
async fn test_get_database() {
    let (catalog, _moto) = setup_glue_catalog("test_get_database").await;

    let namespace = Namespace::try_from(vec!["get_test_db".to_string()]).unwrap();
    let properties = vec![
        ("owner".to_string(), "test_user".to_string()),
        ("team".to_string(), "data_eng".to_string()),
    ];

    // Test getting non-existent database returns NotFound error
    let result = catalog.get_database(&namespace).await;
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        sail_catalog::error::CatalogError::NotFound(_, _)
    ));

    // Create the database
    let created_db = catalog
        .create_database(
            &namespace,
            CreateDatabaseOptions {
                if_not_exists: false,
                comment: Some("Get test description".to_string()),
                location: Some("s3://bucket/get-test".to_string()),
                properties: properties.clone(),
            },
        )
        .await
        .unwrap();

    assert_eq!(created_db.database, vec!["get_test_db".to_string()]);

    // Now get it and verify all fields
    let db = catalog.get_database(&namespace).await.unwrap();

    assert_eq!(db.database, vec!["get_test_db".to_string()]);
    assert_eq!(db.comment, Some("Get test description".to_string()));
    assert_eq!(db.location, Some("s3://bucket/get-test".to_string()));
    for (key, value) in &properties {
        assert!(db.properties.iter().any(|(k, v)| k == key && v == value));
    }
}

/// Tests dropping databases from Glue catalog.
///
/// - Dropping non-existent database with `if_exists=false` returns `NotFound` error
/// - Dropping non-existent database with `if_exists=true` succeeds
/// - Creates a database and verifies it exists
/// - Drops the database and verifies it no longer exists
#[tokio::test]
#[ignore]
async fn test_drop_database() {
    let (catalog, _moto) = setup_glue_catalog("test_drop_database").await;

    let namespace = Namespace::try_from(vec!["drop_test_db".to_string()]).unwrap();

    // Test dropping non-existent database without if_exists - should fail
    let result = catalog
        .drop_database(
            &namespace,
            DropDatabaseOptions {
                if_exists: false,
                cascade: false,
            },
        )
        .await;
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        sail_catalog::error::CatalogError::NotFound(_, _)
    ));

    // Test dropping non-existent database with if_exists - should succeed
    let result = catalog
        .drop_database(
            &namespace,
            DropDatabaseOptions {
                if_exists: true,
                cascade: false,
            },
        )
        .await;
    assert!(result.is_ok());

    // Create the database
    catalog
        .create_database(
            &namespace,
            CreateDatabaseOptions {
                comment: Some("To be dropped".to_string()),
                ..simple_database_options()
            },
        )
        .await
        .unwrap();

    // Verify it exists
    assert!(catalog.get_database(&namespace).await.is_ok());

    // Drop it
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

    // Verify it's gone
    assert!(catalog.get_database(&namespace).await.is_err());
}

/// Tests listing databases in Glue catalog.
///
/// - Empty catalog returns empty list
/// - Creates multiple databases
/// - `list_databases` returns all created databases
#[tokio::test]
#[ignore]
async fn test_list_databases() {
    let (catalog, _moto) = setup_glue_catalog("test_list_databases").await;

    let databases = catalog.list_databases(None).await.unwrap();
    assert!(databases.is_empty());

    let ns1 = Namespace::try_from(vec!["list_db_one".to_string()]).unwrap();
    let ns2 = Namespace::try_from(vec!["list_db_two".to_string()]).unwrap();
    let ns3 = Namespace::try_from(vec!["other_db".to_string()]).unwrap();

    catalog
        .create_database(&ns1, simple_database_options())
        .await
        .unwrap();

    catalog
        .create_database(&ns2, simple_database_options())
        .await
        .unwrap();

    catalog
        .create_database(&ns3, simple_database_options())
        .await
        .unwrap();

    let databases = catalog.list_databases(None).await.unwrap();
    assert_eq!(databases.len(), 3);

    let db_names: Vec<_> = databases.iter().map(|d| d.database.clone()).collect();
    assert!(db_names.contains(&vec!["list_db_one".to_string()]));
    assert!(db_names.contains(&vec!["list_db_two".to_string()]));
    assert!(db_names.contains(&vec!["other_db".to_string()]));
}
