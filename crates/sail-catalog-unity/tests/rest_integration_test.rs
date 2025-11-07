// CHECK HERE
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::todo,
    unused_imports
)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::{DataType, Field, Fields};
use sail_catalog::provider::{
    CatalogProvider, CatalogTableConstraint, CatalogTableSort, CreateDatabaseOptions,
    CreateTableColumnOptions, CreateTableOptions, DatabaseStatus, DropDatabaseOptions,
    DropTableOptions, Namespace, RuntimeAwareCatalogProvider, TableKind,
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

    let dbs = unity_catalog.list_databases(None).await.unwrap();
    assert_eq!(dbs.len(), 2);
    assert!(dbs
        .iter()
        .any(|db| db.database == Vec::<String>::from(ns1_full.clone())));
    assert!(dbs
        .iter()
        .any(|db| db.database == Vec::<String>::from(ns2_full.clone())));
}

#[tokio::test]
// #[ignore]
async fn test_drop_schema() {
    let (unity_catalog, _unity_container, _postgres_container, _client) = setup_catalog().await;

    let namespace = Namespace::try_from(vec!["test_drop_schema".to_string()]).unwrap();
    let full_namespace = Namespace::try_from(vec![
        DEFAULT_CATALOG.to_string(),
        "test_drop_schema".to_string(),
    ])
    .unwrap();

    unity_catalog
        .create_database(
            &namespace,
            CreateDatabaseOptions {
                if_not_exists: false,
                comment: None,
                location: None,
                properties: vec![],
            },
        )
        .await
        .unwrap();

    let result = unity_catalog.get_database(&namespace).await;
    assert!(result.is_ok());
    let result = unity_catalog.get_database(&full_namespace).await;
    assert!(result.is_ok());

    unity_catalog
        .drop_database(
            &namespace,
            DropDatabaseOptions {
                if_exists: false,
                cascade: false,
            },
        )
        .await
        .unwrap();

    let result = unity_catalog.get_database(&namespace).await;
    assert!(result.is_err());
    let result = unity_catalog.get_database(&full_namespace).await;
    assert!(result.is_err());

    let result = unity_catalog
        .drop_database(
            &full_namespace,
            DropDatabaseOptions {
                if_exists: false,
                cascade: false,
            },
        )
        .await;
    assert!(result.is_err());

    let result = unity_catalog
        .drop_database(
            &namespace,
            DropDatabaseOptions {
                if_exists: true,
                cascade: false,
            },
        )
        .await;
    assert!(result.is_ok());

    let namespace = Namespace::try_from(vec!["test_drop_schema_2".to_string()]).unwrap();
    let full_namespace = Namespace::try_from(vec![
        DEFAULT_CATALOG.to_string(),
        "test_drop_schema_2".to_string(),
    ])
    .unwrap();

    unity_catalog
        .create_database(
            &namespace,
            CreateDatabaseOptions {
                if_not_exists: false,
                comment: None,
                location: None,
                properties: vec![],
            },
        )
        .await
        .unwrap();

    let result = unity_catalog.get_database(&namespace).await;
    assert!(result.is_ok());
    let result = unity_catalog.get_database(&full_namespace).await;
    assert!(result.is_ok());

    unity_catalog
        .drop_database(
            &namespace,
            DropDatabaseOptions {
                if_exists: false,
                cascade: true,
            },
        )
        .await
        .unwrap();

    let result = unity_catalog.get_database(&namespace).await;
    assert!(result.is_err());
    let result = unity_catalog.get_database(&full_namespace).await;
    assert!(result.is_err());
}

#[tokio::test]
// #[ignore]
async fn test_create_table() {
    let (unity_catalog, _unity_container, _postgres_container, _client) = setup_catalog().await;

    let ns = Namespace::try_from(vec!["test_create_table".to_string()]).unwrap();
    let properties = vec![
        ("owner".to_string(), "Lake".to_string()),
        ("community".to_string(), "Sail".to_string()),
    ];

    unity_catalog
        .create_database(
            &ns,
            CreateDatabaseOptions {
                if_not_exists: false,
                comment: None,
                location: None,
                properties,
            },
        )
        .await
        .unwrap();

    let column_options = vec![
        CreateTableColumnOptions {
            name: "foo".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            comment: None,
            default: None,
            generated_always_as: None,
        },
        CreateTableColumnOptions {
            name: "bar".to_string(),
            data_type: DataType::List(Arc::new(Field::new(
                "element",
                DataType::Struct(Fields::from(vec![
                    Field::new("a", DataType::Utf8, true),
                    Field::new("b", DataType::Int32, false),
                ])),
                true,
            ))),
            nullable: false,
            comment: Some("meow".to_string()),
            default: None,
            generated_always_as: None,
        },
        CreateTableColumnOptions {
            name: "baz".to_string(),
            data_type: DataType::Map(
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
            nullable: true,
            comment: None,
            default: None,
            generated_always_as: None,
        },
        CreateTableColumnOptions {
            name: "mew".to_string(),
            data_type: DataType::Struct(Fields::from(vec![
                Field::new("a", DataType::Utf8, true),
                Field::new("b", DataType::Int32, false),
            ])),
            nullable: true,
            comment: None,
            default: None,
            generated_always_as: None,
        },
    ];

    let table = unity_catalog
        .create_table(
            &ns,
            "t1",
            CreateTableOptions {
                columns: column_options.clone(),
                comment: Some("peow".to_string()),
                constraints: vec![],
                location: Some("s3://deltadata/custom/path/meow".to_string()),
                format: "delta".to_string(),
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
        .unwrap();
    eprintln!("CHECK HERE CREATE TABLE RESULT: {table:?}");

    let TableKind::Table {
        catalog,
        database,
        columns,
        comment,
        constraints,
        location,
        format,
        partition_by,
        sort_by,
        bucket_by,
        options,
        properties,
    } = table.kind
    else {
        panic!("Expected TableKind::Table");
    };

    let mut static_properties: Vec<_> = properties
        .iter()
        .filter(|(k, _)| {
            ![
                "metadata-location",
                "metadata.last-updated-ms",
                "metadata.table-uuid",
            ]
            .contains(&k.as_str())
        })
        .cloned()
        .collect();
    static_properties.sort();

    let mut expected_properties: Vec<(String, String)> = vec![
        ("comment".to_string(), "peow".to_string()),
        ("metadata.current-schema-id".to_string(), "0".to_string()),
        ("metadata.current-snapshot-id".to_string(), "-1".to_string()),
        (
            "metadata.default-sort-order-id".to_string(),
            "0".to_string(),
        ),
        ("metadata.default-spec-id".to_string(), "0".to_string()),
        ("metadata.format-version".to_string(), "2".to_string()),
        ("metadata.last-column-id".to_string(), "3".to_string()),
        ("metadata.last-partition-id".to_string(), "999".to_string()),
        ("metadata.last-sequence-number".to_string(), "0".to_string()),
        (
            "metadata.partition-statistics".to_string(),
            "[]".to_string(),
        ),
        ("metadata.statistics".to_string(), "[]".to_string()),
        (
            "write.parquet.compression-codec".to_string(),
            "zstd".to_string(),
        ),
    ];
    expected_properties.sort();

    assert_eq!(properties.len(), 15);
    assert_eq!(static_properties, expected_properties);
    assert!(properties.iter().any(|(k, v)| k == "metadata-location"
        && v.starts_with("s3://deltadata/demo/test_create_table.apple.ios/t1/metadata/")));
    assert!(properties
        .iter()
        .any(|(k, v)| k == "metadata.last-updated-ms" && !v.is_empty()));
    assert!(properties
        .iter()
        .any(|(k, v)| k == "metadata.table-uuid" && !v.is_empty()));

    assert_eq!(table.name, "t1".to_string());
    assert_eq!(catalog, "test".to_string());
    assert_eq!(database, Vec::<String>::from(ns.clone()));
    assert_eq!(comment, Some("peow".to_string()));
    assert_eq!(constraints, vec![]);
    assert_eq!(
        location,
        Some("s3://deltadata/demo/test_create_table.apple.ios/t1".to_string())
    );
    assert_eq!(format, "delta".to_string());
    assert_eq!(partition_by, Vec::<String>::new());
    assert_eq!(sort_by, vec![]);
    assert_eq!(bucket_by, None);
    assert_eq!(options, Vec::<(String, String)>::new());
    assert_eq!(columns.len(), 3);
    assert!(
        columns.contains(&sail_catalog::provider::TableColumnStatus {
            name: "foo".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            comment: None,
            default: None,
            generated_always_as: None,
            is_partition: false,
            is_bucket: false,
            is_cluster: false,
        })
    );
    assert!(
        columns.contains(&sail_catalog::provider::TableColumnStatus {
            name: "bar".to_string(),
            data_type: DataType::Int32,
            nullable: false,
            comment: Some("meow".to_string()),
            default: None,
            generated_always_as: None,
            is_partition: false,
            is_bucket: false,
            is_cluster: false,
        })
    );
    assert!(
        columns.contains(&sail_catalog::provider::TableColumnStatus {
            name: "baz".to_string(),
            data_type: DataType::Boolean,
            nullable: true,
            comment: None,
            default: None,
            generated_always_as: None,
            is_partition: false,
            is_bucket: false,
            is_cluster: false,
        })
    );

    let result = unity_catalog
        .create_table(
            &ns,
            "t1",
            CreateTableOptions {
                columns: column_options.clone(),
                comment: Some("peow".to_string()),
                constraints: vec![],
                location: Some("s3://deltadata/custom/path/meow".to_string()),
                format: "delta".to_string(),
                partition_by: vec![],
                sort_by: vec![],
                bucket_by: None,
                if_not_exists: false,
                replace: false,
                options: vec![],
                properties: vec![],
            },
        )
        .await;
    assert!(result.is_err());

    let result = unity_catalog
        .create_table(
            &ns,
            "t1",
            CreateTableOptions {
                columns: column_options.clone(),
                comment: Some("peow".to_string()),
                constraints: vec![],
                location: Some("s3://deltadata/custom/path/meow".to_string()),
                format: "delta".to_string(),
                partition_by: vec![],
                sort_by: vec![],
                bucket_by: None,
                if_not_exists: true,
                replace: false,
                options: vec![],
                properties: vec![],
            },
        )
        .await;
    assert!(result.is_ok());

    let table = unity_catalog
        .create_table(
            &ns,
            "t2",
            CreateTableOptions {
                columns: column_options.clone(),
                comment: Some("test table".to_string()),
                constraints: vec![],
                location: Some("s3://deltadata/custom/path/meow2".to_string()),
                format: "delta".to_string(),
                partition_by: vec!["baz".to_string()],
                sort_by: vec![],
                bucket_by: None,
                if_not_exists: false,
                replace: false,
                options: vec![("key1".to_string(), "value1".to_string())],
                properties: vec![
                    ("owner".to_string(), "mr. meow".to_string()),
                    ("team".to_string(), "data-eng".to_string()),
                ],
            },
        )
        .await
        .unwrap();

    let TableKind::Table {
        catalog,
        database,
        columns,
        comment,
        constraints,
        location,
        format,
        partition_by,
        sort_by,
        bucket_by,
        options,
        properties,
    } = table.kind
    else {
        panic!("Expected TableKind::Table");
    };

    assert_eq!(table.name, "t2".to_string());
    assert_eq!(catalog, "test".to_string());
    assert_eq!(database, Vec::<String>::from(ns.clone()));
    assert_eq!(comment, Some("test table".to_string()));
    assert!(constraints.is_empty());
    assert_eq!(
        location,
        Some("s3://deltadata/custom/path/meow".to_string())
    );
    assert_eq!(format, "delta".to_string());
    assert_eq!(partition_by, vec!["baz".to_string()]);
    assert!(sort_by.is_empty());
    assert_eq!(bucket_by, None);
    assert_eq!(options, vec![("key1".to_string(), "value1".to_string())]);
    assert_eq!(properties.len(), 17);
    assert!(properties.contains(&("owner".to_string(), "mr. meow".to_string())));
    assert!(properties.contains(&("team".to_string(), "data-eng".to_string())));
    assert_eq!(columns.len(), 3);
    assert!(
        columns.contains(&sail_catalog::provider::TableColumnStatus {
            name: "foo".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            comment: None,
            default: None,
            generated_always_as: None,
            is_partition: false,
            is_bucket: false,
            is_cluster: false,
        })
    );
    assert!(
        columns.contains(&sail_catalog::provider::TableColumnStatus {
            name: "bar".to_string(),
            data_type: DataType::Int32,
            nullable: false,
            comment: Some("meow".to_string()),
            default: None,
            generated_always_as: None,
            is_partition: false,
            is_bucket: false,
            is_cluster: false,
        })
    );
    assert!(
        columns.contains(&sail_catalog::provider::TableColumnStatus {
            name: "baz".to_string(),
            data_type: DataType::Boolean,
            nullable: true,
            comment: None,
            default: None,
            generated_always_as: None,
            is_partition: true,
            is_bucket: false,
            is_cluster: false,
        })
    );
}

#[tokio::test]
// #[ignore]
async fn test_drop_table() {
    let (unity_catalog, _unity_container, _postgres_container, _client) = setup_catalog().await;

    let namespace = Namespace::try_from(vec!["test_drop_table".to_string()]).unwrap();

    unity_catalog
        .create_database(
            &namespace,
            CreateDatabaseOptions {
                if_not_exists: false,
                comment: None,
                location: None,
                properties: vec![],
            },
        )
        .await
        .unwrap();

    let column_options = vec![CreateTableColumnOptions {
        name: "id".to_string(),
        data_type: DataType::Int32,
        nullable: false,
        comment: None,
        default: None,
        generated_always_as: None,
    }];

    unity_catalog
        .create_table(
            &namespace,
            "t1",
            CreateTableOptions {
                columns: column_options.clone(),
                comment: None,
                constraints: vec![],
                location: Some("s3://deltadata/custom/path/meow".to_string()),
                format: "delta".to_string(),
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
        .unwrap();

    let result = unity_catalog.get_table(&namespace, "t1").await;
    assert!(result.is_ok());

    unity_catalog
        .drop_table(
            &namespace,
            "t1",
            DropTableOptions {
                if_exists: false,
                purge: false,
            },
        )
        .await
        .unwrap();

    let result = unity_catalog.get_table(&namespace, "t1").await;
    assert!(result.is_err());

    let result = unity_catalog
        .drop_table(
            &namespace,
            "t1",
            DropTableOptions {
                if_exists: false,
                purge: false,
            },
        )
        .await;
    assert!(result.is_err());

    let result = unity_catalog
        .drop_table(
            &namespace,
            "t1",
            DropTableOptions {
                if_exists: true,
                purge: false,
            },
        )
        .await;
    assert!(result.is_ok());
}
