#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use sail_catalog::provider::{CatalogProvider, Namespace};

use crate::common::{setup_hms_catalog, simple_database_options};

#[tokio::test]
#[ignore]
async fn test_create_get_drop_database() {
    let context = setup_hms_catalog("test_create_get_drop_database").await;
    let catalog = &context.catalog;
    let namespace = Namespace::try_from(vec!["test_create_get_drop_database"]).unwrap();

    let created = catalog
        .create_database(&namespace, simple_database_options())
        .await
        .unwrap();
    assert_eq!(
        created.database,
        vec!["test_create_get_drop_database".to_string()]
    );

    let fetched = catalog.get_database(&namespace).await.unwrap();
    assert_eq!(
        fetched.database,
        vec!["test_create_get_drop_database".to_string()]
    );

    let databases = catalog.list_databases(None).await.unwrap();
    assert!(databases
        .iter()
        .any(|db| db.database == vec!["test_create_get_drop_database".to_string()]));

    catalog
        .drop_database(
            &namespace,
            sail_catalog::provider::DropDatabaseOptions {
                if_exists: false,
                cascade: false,
            },
        )
        .await
        .unwrap();
}
