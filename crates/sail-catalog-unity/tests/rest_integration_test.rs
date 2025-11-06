// CHECK HERE
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::todo,
    unused_imports
)]

use std::collections::HashMap;
use std::time::Duration;

use sail_catalog::provider::{
    CatalogProvider, CreateDatabaseOptions, DatabaseStatus, DropDatabaseOptions, Namespace,
    RuntimeAwareCatalogProvider,
};
use sail_catalog_unity::unity::{types, Client};
use sail_catalog_unity::{UnityCatalogProvider, UNITY_CATALOG_PROP_URI};
use sail_common::runtime::RuntimeHandle;
use testcontainers::core::{ContainerPort, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, Healthcheck, ImageExt};

const DEFAULT_CATALOG: &str = "sail_test_catalog";

async fn setup_catalog() -> (
    RuntimeAwareCatalogProvider<UnityCatalogProvider>,
    ContainerAsync<GenericImage>,
    ContainerAsync<GenericImage>,
    Client,
) {
    let network = "rest_bridge";

    let postgres = GenericImage::new("postgres", "latest")
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "test")
        .with_env_var("POSTGRES_PASSWORD", "test")
        .with_env_var("POSTGRES_DB", "test")
        .with_network(network)
        .start()
        .await
        .expect("Failed to start PostgreSQL");

    let postgres_host = postgres
        .get_bridge_ip_address()
        .await
        .expect("get bridge ip");

    let hibernate_config = format!(
        "hibernate.connection.driver_class=org.postgresql.Driver
  hibernate.connection.url=jdbc:postgresql://{postgres_host}:5432/test
  hibernate.connection.username=test
  hibernate.connection.password=test
  hibernate.hbm2ddl.auto=update
  "
    );

    let temp_dir = std::env::temp_dir();
    let hibernate_path = temp_dir.join(format!("hibernate_unity_test_{postgres_host}.properties"));
    std::fs::write(&hibernate_path, hibernate_config)
        .expect("Failed to write hibernate.properties");

    let unity_catalog = GenericImage::new("unitycatalog/unitycatalog", "v0.3.0")
        .with_wait_for(WaitFor::message_on_stdout(
            "###################################################################",
        ))
        .with_exposed_port(ContainerPort::Tcp(8080))
        .with_mount(Mount::bind_mount(
            hibernate_path.to_str().unwrap(),
            "/home/unitycatalog/etc/conf/hibernate.properties",
        ))
        .with_network(network)
        .start()
        .await
        .expect("Failed to start Unity Catalog");

    let host = unity_catalog.get_host().await.expect("get host");
    let port = unity_catalog
        .get_host_port_ipv4(8080)
        .await
        .expect("get port");

    let rest_url = format!("http://{host}:{port}/api/2.1/unity-catalog");

    let runtime = RuntimeHandle::new(
        tokio::runtime::Handle::current(),
        tokio::runtime::Handle::current(),
        true,
    );

    let catalog = RuntimeAwareCatalogProvider::try_new(
        || {
            let props = HashMap::from([(UNITY_CATALOG_PROP_URI.to_string(), rest_url.clone())]);
            let provider =
                UnityCatalogProvider::new("sail".to_string(), DEFAULT_CATALOG.to_string(), props);
            Ok(provider)
        },
        runtime.io().clone(),
    )
    .expect("Failed to create runtime-aware catalog");

    let client = Client::new(&rest_url);
    for attempt in 1..=5 {
        match client
            .create_catalog()
            .body(
                types::CreateCatalog::builder()
                    .name(DEFAULT_CATALOG)
                    .comment(Some("Main catalog for testing".to_string())),
            )
            .send()
            .await
        {
            Ok(_) => break,
            Err(e) if attempt == 5 => panic!("Failed to create catalog after 5 attempts: {e}"),
            Err(_) => tokio::time::sleep(tokio::time::Duration::from_secs(1)).await,
        }
    }

    (catalog, unity_catalog, postgres, client)
}

#[tokio::test]
// #[ignore]
async fn test_create_schema() {
    let (unity_catalog, _unity_container, _postgres_container, _client) = setup_catalog().await;

    let namespace = Namespace::try_from(vec!["test_create_schema".to_string()]).unwrap();
    let full_namespace = Namespace::try_from(vec![
        DEFAULT_CATALOG.to_string(),
        "test_create_schema".to_string(),
    ])
    .unwrap();

    let created_db = unity_catalog
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

    let DatabaseStatus {
        catalog,
        database,
        comment,
        location,
        properties,
    } = created_db;

    let mut static_properties: Vec<_> = properties
        .iter()
        .filter(|(k, _)| !["schema_id", "updated_at", "created_at"].contains(&k.as_str()))
        .cloned()
        .collect();
    static_properties.sort();

    let mut expected_properties: Vec<(String, String)> = vec![
        ("comment".to_string(), "test comment".to_string()),
        ("location".to_string(), "s3://bucket/path".to_string()),
        ("key1".to_string(), "value1".to_string()),
    ];
    expected_properties.sort();

    assert_eq!(properties.len(), 6);
    assert_eq!(static_properties, expected_properties);
    assert!(properties
        .iter()
        .any(|(k, v)| k == "schema_id" && !v.is_empty()));
    assert!(properties
        .iter()
        .any(|(k, v)| k == "updated_at" && !v.is_empty()));
    assert!(properties
        .iter()
        .any(|(k, v)| k == "created_at" && !v.is_empty()));

    assert_eq!(catalog, DEFAULT_CATALOG.to_string());
    assert_eq!(database, Vec::<String>::from(full_namespace.clone()));
    assert_eq!(comment, Some("test comment".to_string()));
    assert_eq!(location, Some("s3://bucket/path".to_string()));

    let result = unity_catalog
        .create_database(
            &namespace,
            CreateDatabaseOptions {
                if_not_exists: false,
                comment: None,
                location: None,
                properties: vec![],
            },
        )
        .await;
    assert!(result.is_err());

    let result = unity_catalog
        .create_database(
            &full_namespace,
            CreateDatabaseOptions {
                if_not_exists: false,
                comment: None,
                location: None,
                properties: vec![],
            },
        )
        .await;
    assert!(result.is_err());

    let namespace2 = Namespace::try_from(vec!["test_create_namespace_2".to_string()]).unwrap();
    let full_namespace2 = Namespace::try_from(vec![
        DEFAULT_CATALOG.to_string(),
        "test_create_namespace_2".to_string(),
    ])
    .unwrap();
    let created_db2 = unity_catalog
        .create_database(
            &full_namespace2,
            CreateDatabaseOptions {
                if_not_exists: true,
                comment: None,
                location: None,
                properties: vec![],
            },
        )
        .await
        .unwrap();

    assert_eq!(
        created_db2.database,
        Vec::<String>::from(full_namespace2.clone())
    );
    assert_eq!(created_db2.comment, None);
    assert_eq!(created_db2.location, None);

    let created_again = unity_catalog
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

    assert_eq!(created_again.database, Vec::<String>::from(full_namespace2));
    assert_eq!(created_again.comment, None);
    assert_eq!(created_again.location, None,);
}

#[tokio::test]
// #[ignore]
async fn test_get_non_exist_schema() {
    let (unity_catalog, _unity_container, _postgres_container, _client) = setup_catalog().await;

    let namespace = Namespace::try_from(vec!["test_get_non_exist_schema".to_string()]).unwrap();
    let result = unity_catalog.get_database(&namespace).await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        sail_catalog::error::CatalogError::NotFound(_, _)
    ));
}

#[tokio::test]
// #[ignore]
async fn test_get_schema() {
    let (unity_catalog, _unity_container, _postgres_container, _client) = setup_catalog().await;

    let namespace = Namespace::try_from(vec!["apple".to_string()]).unwrap();
    let full_namespace =
        Namespace::try_from(vec![DEFAULT_CATALOG.to_string(), "apple".to_string()]).unwrap();
    let properties = vec![
        ("owner".to_string(), "Lake".to_string()),
        ("community".to_string(), "Sail".to_string()),
    ];

    assert!(unity_catalog.get_database(&namespace).await.is_err());
    assert!(unity_catalog.get_database(&full_namespace).await.is_err());

    let created_db = unity_catalog
        .create_database(
            &namespace,
            CreateDatabaseOptions {
                if_not_exists: false,
                comment: None,
                location: None,
                properties: properties.clone(),
            },
        )
        .await
        .unwrap();

    assert_eq!(
        created_db.database,
        Vec::<String>::from(full_namespace.clone())
    );
    for (key, value) in &properties {
        assert!(created_db
            .properties
            .iter()
            .any(|(k, v)| k == key && v == value));
    }

    let get_db = unity_catalog.get_database(&namespace).await.unwrap();
    assert_eq!(get_db.database, Vec::<String>::from(full_namespace.clone()));
    for (key, value) in &properties {
        assert!(get_db
            .properties
            .iter()
            .any(|(k, v)| k == key && v == value));
    }

    let get_db = unity_catalog.get_database(&full_namespace).await.unwrap();
    assert_eq!(get_db.database, Vec::<String>::from(full_namespace));
    for (key, value) in &properties {
        assert!(get_db
            .properties
            .iter()
            .any(|(k, v)| k == key && v == value));
    }
}

#[tokio::test]
// #[ignore]
async fn test_list_schemas() {
    let (unity_catalog, _unity_container, _postgres_container, _client) = setup_catalog().await;

    let ns1 = Namespace::try_from(vec!["ios".to_string()]).unwrap();
    let ns1_full =
        Namespace::try_from(vec![DEFAULT_CATALOG.to_string(), "ios".to_string()]).unwrap();
    let ns1_properties = vec![
        ("owner".to_string(), "Lake".to_string()),
        ("community".to_string(), "Sail".to_string()),
    ];

    let ns2 = Namespace::try_from(vec!["macos".to_string()]).unwrap();
    let ns2_full =
        Namespace::try_from(vec![DEFAULT_CATALOG.to_string(), "macos".to_string()]).unwrap();
    let ns2_properties = vec![
        ("owner".to_string(), "Meow".to_string()),
        ("community".to_string(), "Peow".to_string()),
    ];

    let parent = Namespace::try_from(vec![DEFAULT_CATALOG.to_string()]).unwrap();

    assert!(unity_catalog.list_databases(None).await.unwrap().is_empty());
    assert!(unity_catalog
        .list_databases(Some(&parent))
        .await
        .unwrap()
        .is_empty());

    unity_catalog
        .create_database(
            &ns1,
            CreateDatabaseOptions {
                if_not_exists: false,
                comment: None,
                location: None,
                properties: ns1_properties,
            },
        )
        .await
        .unwrap();

    unity_catalog
        .create_database(
            &ns2,
            CreateDatabaseOptions {
                if_not_exists: false,
                comment: None,
                location: None,
                properties: ns2_properties,
            },
        )
        .await
        .unwrap();

    let dbs = unity_catalog.list_databases(Some(&parent)).await.unwrap();

    assert_eq!(dbs.len(), 2);
    assert!(dbs
        .iter()
        .any(|db| db.database == Vec::<String>::from(ns1_full.clone())));
    assert!(dbs
        .iter()
        .any(|db| db.database == Vec::<String>::from(ns2_full.clone())));
}
