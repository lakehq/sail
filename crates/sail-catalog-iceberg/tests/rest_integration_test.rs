#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use arrow::datatypes::DataType;
use sail_catalog::provider::{
    CatalogProvider, CreateDatabaseOptions, CreateTableColumnOptions, CreateTableOptions,
    DropDatabaseOptions, Namespace,
};
use sail_catalog_iceberg::{IcebergRestCatalogProvider, REST_CATALOG_PROP_URI};
use sail_common::runtime::RuntimeHandle;
use std::collections::HashMap;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

async fn setup_catalog() -> (
    IcebergRestCatalogProvider,
    ContainerAsync<GenericImage>,
    ContainerAsync<GenericImage>,
    ContainerAsync<GenericImage>,
) {
    let minio = GenericImage::new("minio/minio", "RELEASE.2025-05-24T17-08-30Z")
        .with_wait_for(WaitFor::message_on_stderr("MinIO Object Storage Server"))
        .with_exposed_port(ContainerPort::Tcp(9000))
        .with_exposed_port(ContainerPort::Tcp(9001))
        .with_env_var("MINIO_ROOT_USER", "admin")
        .with_env_var("MINIO_ROOT_PASSWORD", "password")
        .with_env_var("MINIO_DOMAIN", "minio")
        .with_cmd(vec!["server", "/data", "--console-address", ":9001"])
        .start()
        .await
        .expect("Failed to start MinIO");

    let minio_host = minio.get_host().await.expect("get host");
    let minio_port = minio.get_host_port_ipv4(9000).await.expect("get port");
    let minio_endpoint = format!("http://{minio_host}:{minio_port}");

    let mc_cmd = format!(
        "until (/usr/bin/mc alias set minio {minio_endpoint} admin password) do echo '...waiting...' && sleep 1; done; /usr/bin/mc rm -r --force minio/icebergdata; /usr/bin/mc mb minio/icebergdata; /usr/bin/mc policy set public minio/icebergdata; tail -f /dev/null",
    );

    let _mc = GenericImage::new("minio/mc", "RELEASE.2025-05-21T01-59-54Z")
        .with_env_var("AWS_ACCESS_KEY_ID", "admin")
        .with_env_var("AWS_SECRET_ACCESS_KEY", "password")
        .with_env_var("AWS_REGION", "us-east-1")
        .with_cmd(vec!["/bin/sh", "-c", &mc_cmd])
        .start()
        .await
        .expect("Failed to start MC");

    let rest = GenericImage::new("apache/iceberg-rest-fixture", "latest")
        .with_wait_for(WaitFor::message_on_stderr(
            "INFO org.eclipse.jetty.server.Server - Started ",
        ))
        .with_exposed_port(ContainerPort::Tcp(8181))
        .with_env_var("AWS_ACCESS_KEY_ID", "admin")
        .with_env_var("AWS_SECRET_ACCESS_KEY", "password")
        .with_env_var("AWS_REGION", "us-east-1")
        .with_env_var(
            "CATALOG_CATALOG__IMPL",
            "org.apache.iceberg.jdbc.JdbcCatalog",
        )
        .with_env_var(
            "CATALOG_URI",
            "jdbc:sqlite:file:/tmp/iceberg_rest_mode=memory",
        )
        .with_env_var("CATALOG_WAREHOUSE", "s3://icebergdata/demo")
        .with_env_var("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
        .with_env_var("CATALOG_S3_ENDPOINT", &minio_endpoint)
        .start()
        .await
        .expect("Failed to start REST catalog");

    let rest_host = rest.get_host().await.expect("get host");
    let rest_port = rest.get_host_port_ipv4(8181).await.expect("get port");
    let rest_url = format!("http://{rest_host}:{rest_port}");

    let runtime = RuntimeHandle::new(
        tokio::runtime::Handle::current(),
        Some(tokio::runtime::Handle::current()),
    );
    let props = HashMap::from([(REST_CATALOG_PROP_URI.to_string(), rest_url)]);
    let catalog = IcebergRestCatalogProvider::new(runtime, "test".to_string(), props);
    (catalog, minio, _mc, rest)
}

#[tokio::test]
async fn test_get_non_exist_namespace() {
    let (catalog, _minio, _mc, _rest) = setup_catalog().await;

    let namespace = Namespace::try_from(vec!["test_get_non_exist_namespace".to_string()]).unwrap();
    let result = catalog.get_database(&namespace).await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        sail_catalog::error::CatalogError::NotFound(_, _)
    ));
}

#[tokio::test]
async fn test_get_namespace() {
    let (catalog, _minio, _mc, _rest) = setup_catalog().await;

    let namespace = Namespace::try_from(vec!["apple".to_string(), "ios".to_string()]).unwrap();
    let properties = vec![
        ("owner".to_string(), "Lake".to_string()),
        ("community".to_string(), "Sail".to_string()),
    ];

    assert!(catalog.get_database(&namespace).await.is_err());

    let created_db = catalog
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

    assert_eq!(created_db.database, Vec::<String>::from(namespace.clone()));
    for (key, value) in &properties {
        assert!(created_db
            .properties
            .iter()
            .any(|(k, v)| k == key && v == value));
    }

    let get_db = catalog.get_database(&namespace).await.unwrap();
    assert_eq!(get_db.database, vec![namespace.to_string()]);
    for (key, value) in &properties {
        assert!(get_db
            .properties
            .iter()
            .any(|(k, v)| k == key && v == value));
    }
}

#[tokio::test]
async fn test_list_namespace() {
    let (catalog, _minio, _mc, _rest) = setup_catalog().await;

    let ns1 =
        Namespace::try_from(vec!["test_list_namespace".to_string(), "ios".to_string()]).unwrap();
    let ns1_properties = vec![
        ("owner".to_string(), "Lake".to_string()),
        ("community".to_string(), "Sail".to_string()),
    ];

    let ns2 =
        Namespace::try_from(vec!["test_list_namespace".to_string(), "macos".to_string()]).unwrap();
    let ns2_properties = vec![
        ("owner".to_string(), "Meow".to_string()),
        ("community".to_string(), "Peow".to_string()),
    ];

    let parent = Namespace::try_from(vec!["test_list_namespace".to_string()]).unwrap();

    assert!(catalog.list_databases(Some(&parent)).await.is_err());

    catalog
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

    catalog
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

    let dbs = catalog.list_databases(Some(&parent)).await.unwrap();

    assert_eq!(dbs.len(), 2);
    assert!(dbs
        .iter()
        .any(|db| db.database == Vec::<String>::from(ns1.clone())));
    assert!(dbs
        .iter()
        .any(|db| db.database == Vec::<String>::from(ns2.clone())));
}

#[tokio::test]
async fn test_list_empty_namespace() {
    let (catalog, _minio, _mc, _rest) = setup_catalog().await;

    let ns_apple = Namespace::try_from(vec![
        "test_list_empty_namespace".to_string(),
        "apple".to_string(),
    ])
    .unwrap();
    let properties = vec![
        ("owner".to_string(), "Lake".to_string()),
        ("community".to_string(), "Sail".to_string()),
    ];

    assert!(catalog.list_databases(Some(&ns_apple)).await.is_err());

    catalog
        .create_database(
            &ns_apple,
            CreateDatabaseOptions {
                if_not_exists: false,
                comment: None,
                location: None,
                properties,
            },
        )
        .await
        .unwrap();

    let dbs = catalog.list_databases(Some(&ns_apple)).await.unwrap();
    assert!(dbs.is_empty());
}

#[tokio::test]
async fn test_list_root_namespace() {
    let (catalog, _minio, _mc, _rest) = setup_catalog().await;

    let ns1 = Namespace::try_from(vec![
        "test_list_root_namespace".to_string(),
        "apple".to_string(),
        "ios".to_string(),
    ])
    .unwrap();
    let ns1_properties = vec![
        ("owner".to_string(), "Lake".to_string()),
        ("community".to_string(), "Sail".to_string()),
    ];

    let ns2 = Namespace::try_from(vec![
        "test_list_root_namespace".to_string(),
        "google".to_string(),
        "android".to_string(),
    ])
    .unwrap();
    let ns2_properties = vec![
        ("owner".to_string(), "Meow".to_string()),
        ("community".to_string(), "Peow".to_string()),
    ];

    let parent = Namespace::try_from(vec!["test_list_root_namespace".to_string()]).unwrap();

    assert!(catalog.list_databases(Some(&parent)).await.is_err());

    catalog
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

    catalog
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

    let dbs = catalog.list_databases(None).await.unwrap();
    assert_eq!(dbs.len(), 1);
    assert_eq!(
        dbs[0].database,
        vec!["test_list_root_namespace".to_string()]
    );
}

#[tokio::test]
async fn test_list_empty_multi_level_namespace() {
    let (catalog, _minio, _mc, _rest) = setup_catalog().await;

    let ns_apple = Namespace::try_from(vec![
        "test_list_empty_multi_level_namespace".to_string(),
        "a_a".to_string(),
        "apple".to_string(),
    ])
    .unwrap();
    let properties = vec![
        ("owner".to_string(), "Lake".to_string()),
        ("community".to_string(), "Sail".to_string()),
    ];

    assert!(catalog.list_databases(Some(&ns_apple)).await.is_err());

    catalog
        .create_database(
            &ns_apple,
            CreateDatabaseOptions {
                if_not_exists: false,
                comment: None,
                location: None,
                properties,
            },
        )
        .await
        .unwrap();

    let dbs = catalog.list_databases(Some(&ns_apple)).await.unwrap();
    assert!(dbs.is_empty());
}

#[tokio::test]
async fn test_create_namespace() {
    let (catalog, _minio, _mc, _rest) = setup_catalog().await;

    let namespace = Namespace::try_from(vec!["test_create_namespace".to_string()]).unwrap();

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

    assert_eq!(created_db.database, Vec::<String>::from(namespace.clone()));
    assert_eq!(created_db.comment, Some("test comment".to_string()));
    assert_eq!(created_db.location, Some("s3://bucket/path".to_string()));
    assert!(created_db
        .properties
        .iter()
        .any(|(k, v)| k == "key1" && v == "value1"));

    let result = catalog
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

    let namespace2 = Namespace::try_from(vec!["test_create_namespace_2".to_string()]).unwrap();
    let created_db2 = catalog
        .create_database(
            &namespace2,
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
        Vec::<String>::from(namespace2.clone())
    );
    assert_eq!(created_db2.comment, None);
    assert_eq!(
        created_db2.location,
        Some("s3://icebergdata/demo/test_create_namespace_2".to_string())
    );

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

    assert_eq!(created_again.database, Vec::<String>::from(namespace2));
    assert_eq!(created_again.comment, None);
    assert_eq!(
        created_again.location,
        Some("s3://icebergdata/demo/test_create_namespace_2".to_string())
    );
}

#[tokio::test]
async fn test_drop_namespace() {
    let (catalog, _minio, _mc, _rest) = setup_catalog().await;

    let namespace = Namespace::try_from(vec!["test_drop_namespace".to_string()]).unwrap();

    catalog
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

    let result = catalog.get_database(&namespace).await;
    assert!(result.is_ok());

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

    let result = catalog.get_database(&namespace).await;
    assert!(result.is_err());

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
}

#[tokio::test]
async fn test_create_table() {
    let (catalog, _minio, _mc, _rest) = setup_catalog().await;

    let ns = Namespace::try_from(vec![
        "test_create_table".to_string(),
        "apple".to_string(),
        "ios".to_string(),
    ])
    .unwrap();
    let properties = vec![
        ("owner".to_string(), "Lake".to_string()),
        ("community".to_string(), "Sail".to_string()),
    ];

    catalog
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

    let columns = vec![
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
            data_type: DataType::Int32,
            nullable: false,
            comment: None,
            default: None,
            generated_always_as: None,
        },
        CreateTableColumnOptions {
            name: "baz".to_string(),
            data_type: DataType::Boolean,
            nullable: true,
            comment: None,
            default: None,
            generated_always_as: None,
        },
    ];

    let table = catalog
        .create_table(
            &ns,
            "t1",
            CreateTableOptions {
                columns,
                comment: None,
                constraints: vec![],
                location: None,
                format: "iceberg".to_string(),
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

    assert_eq!(table.name, "t1");
}
