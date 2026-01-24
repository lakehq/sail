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

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::HashMap;

use arrow::datatypes::DataType;
use sail_catalog::provider::{
    CatalogProvider, CreateDatabaseOptions, CreateTableColumnOptions, CreateTableOptions,
    CreateViewColumnOptions, CreateViewOptions, DropDatabaseOptions, DropTableOptions,
    DropViewOptions, Namespace, RuntimeAwareCatalogProvider,
};
use sail_catalog_iceberg::{IcebergRestCatalogProvider, REST_CATALOG_PROP_URI};
use sail_common::runtime::RuntimeHandle;
use sail_common_datafusion::catalog::{
    CatalogTableConstraint, CatalogTableSort, TableColumnStatus, TableKind,
};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

async fn setup_catalog(
    network_name: &str,
) -> (
    RuntimeAwareCatalogProvider<IcebergRestCatalogProvider>,
    ContainerAsync<GenericImage>,
    ContainerAsync<GenericImage>,
    ContainerAsync<GenericImage>,
) {
    let network = format!("iceberg_{}", network_name);

    let minio = GenericImage::new("minio/minio", "RELEASE.2025-05-24T17-08-30Z")
        .with_wait_for(WaitFor::message_on_stderr("MinIO Object Storage Server"))
        .with_exposed_port(ContainerPort::Tcp(9000))
        .with_exposed_port(ContainerPort::Tcp(9001))
        .with_env_var("MINIO_ROOT_USER", "admin")
        .with_env_var("MINIO_ROOT_PASSWORD", "password")
        .with_network(&network)
        .with_cmd(vec!["server", "/data", "--console-address", ":9001"])
        .start()
        .await
        .expect("Failed to start MinIO");

    let minio_ip = minio.get_bridge_ip_address().await.expect("get bridge ip");
    let minio_internal_endpoint = format!("http://{minio_ip}:9000");

    let mc = GenericImage::new("minio/mc", "RELEASE.2025-05-21T01-59-54Z")
        .with_wait_for(WaitFor::message_on_stdout(
            "Bucket created successfully `minio/icebergdata`",
        ))
        .with_entrypoint("/bin/sh")
        .with_cmd(vec![
            "-c",
            format!("until (/usr/bin/mc alias set minio {minio_internal_endpoint} admin password) do echo '...waiting...' && sleep 1; done; /usr/bin/mc rm -r --force minio/icebergdata; /usr/bin/mc mb minio/icebergdata; /usr/bin/mc policy set public minio/icebergdata; tail -f /dev/null").as_str(),
        ])
        .with_env_var("AWS_ACCESS_KEY_ID", "admin")
        .with_env_var("AWS_SECRET_ACCESS_KEY", "password")
        .with_env_var("AWS_REGION", "us-east-1")
        .with_network(&network)
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
        .with_env_var("CATALOG_S3_ENDPOINT", minio_internal_endpoint)
        .with_network(&network)
        .start()
        .await
        .expect("Failed to start REST catalog");

    let rest_host = rest.get_host().await.expect("get host");
    let rest_port = rest.get_host_port_ipv4(8181).await.expect("get port");
    let rest_url = format!("http://{rest_host}:{rest_port}");

    let runtime = RuntimeHandle::new(
        tokio::runtime::Handle::current(),
        tokio::runtime::Handle::current(),
        true,
    );

    let catalog = RuntimeAwareCatalogProvider::try_new(
        || {
            let props = HashMap::from([(REST_CATALOG_PROP_URI.to_string(), rest_url)]);
            let provider = IcebergRestCatalogProvider::new("test".to_string(), props);
            Ok(provider)
        },
        runtime.io().clone(),
    )
    .expect("Failed to create runtime-aware catalog");

    (catalog, minio, mc, rest)
}

#[tokio::test]
#[ignore]
async fn test_create_namespace() {
    let (rest_catalog, _minio, _mc, _rest) = setup_catalog("test_create_namespace_network").await;

    let namespace = Namespace::try_from(vec!["test_create_namespace".to_string()]).unwrap();

    let created_db = rest_catalog
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

    let result = rest_catalog
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
    let created_db2 = rest_catalog
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

    let created_again = rest_catalog
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
#[ignore]
async fn test_get_non_exist_namespace() {
    let (rest_catalog, _minio, _mc, _rest) =
        setup_catalog("test_get_non_exist_namespace_network").await;

    let namespace = Namespace::try_from(vec!["test_get_non_exist_namespace".to_string()]).unwrap();
    let result = rest_catalog.get_database(&namespace).await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        sail_catalog::error::CatalogError::NotFound(_, _)
    ));
}

#[tokio::test]
#[ignore]
async fn test_get_namespace() {
    let (rest_catalog, _minio, _mc, _rest) = setup_catalog("test_get_namespace_network").await;

    let namespace = Namespace::try_from(vec!["apple".to_string(), "ios".to_string()]).unwrap();
    let properties = vec![
        ("owner".to_string(), "Lake".to_string()),
        ("community".to_string(), "Sail".to_string()),
    ];

    assert!(rest_catalog.get_database(&namespace).await.is_err());

    let created_db = rest_catalog
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

    let get_db = rest_catalog.get_database(&namespace).await.unwrap();
    assert_eq!(get_db.database, vec![namespace.to_string()]);
    for (key, value) in &properties {
        assert!(get_db
            .properties
            .iter()
            .any(|(k, v)| k == key && v == value));
    }
}

#[tokio::test]
#[ignore]
async fn test_list_namespaces() {
    let (rest_catalog, _minio, _mc, _rest) = setup_catalog("test_list_namespaces_network").await;

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

    assert!(rest_catalog.list_databases(Some(&parent)).await.is_err());

    rest_catalog
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

    rest_catalog
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

    let dbs = rest_catalog.list_databases(Some(&parent)).await.unwrap();

    assert_eq!(dbs.len(), 2);
    assert!(dbs
        .iter()
        .any(|db| db.database == Vec::<String>::from(ns1.clone())));
    assert!(dbs
        .iter()
        .any(|db| db.database == Vec::<String>::from(ns2.clone())));
}

#[tokio::test]
#[ignore]
async fn test_list_empty_namespaces() {
    let (rest_catalog, _minio, _mc, _rest) =
        setup_catalog("test_list_empty_namespaces_network").await;

    let ns_apple = Namespace::try_from(vec![
        "test_list_empty_namespace".to_string(),
        "apple".to_string(),
    ])
    .unwrap();
    let properties = vec![
        ("owner".to_string(), "Lake".to_string()),
        ("community".to_string(), "Sail".to_string()),
    ];

    assert!(rest_catalog.list_databases(Some(&ns_apple)).await.is_err());

    rest_catalog
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

    let dbs = rest_catalog.list_databases(Some(&ns_apple)).await.unwrap();
    assert!(dbs.is_empty());
}

#[tokio::test]
#[ignore]
async fn test_list_root_namespaces() {
    let (rest_catalog, _minio, _mc, _rest) =
        setup_catalog("test_list_root_namespaces_network").await;

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

    assert!(rest_catalog.list_databases(Some(&parent)).await.is_err());

    rest_catalog
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

    rest_catalog
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

    let dbs = rest_catalog.list_databases(None).await.unwrap();
    assert_eq!(dbs.len(), 1);
    assert_eq!(
        dbs[0].database,
        vec!["test_list_root_namespace".to_string()]
    );
}

#[tokio::test]
#[ignore]
async fn test_list_empty_multi_level_namespaces() {
    let (rest_catalog, _minio, _mc, _rest) =
        setup_catalog("test_list_empty_multi_level_namespaces_network").await;

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

    assert!(rest_catalog.list_databases(Some(&ns_apple)).await.is_err());

    rest_catalog
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

    let dbs = rest_catalog.list_databases(Some(&ns_apple)).await.unwrap();
    assert!(dbs.is_empty());
}

#[tokio::test]
#[ignore]
async fn test_drop_namespace() {
    let (rest_catalog, _minio, _mc, _rest) = setup_catalog("test_drop_namespace_network").await;

    let namespace = Namespace::try_from(vec!["test_drop_namespace".to_string()]).unwrap();

    rest_catalog
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

    let result = rest_catalog.get_database(&namespace).await;
    assert!(result.is_ok());

    rest_catalog
        .drop_database(
            &namespace,
            DropDatabaseOptions {
                if_exists: false,
                cascade: false,
            },
        )
        .await
        .unwrap();

    let result = rest_catalog.get_database(&namespace).await;
    assert!(result.is_err());

    let result = rest_catalog
        .drop_database(
            &namespace,
            DropDatabaseOptions {
                if_exists: false,
                cascade: false,
            },
        )
        .await;
    assert!(result.is_err());

    let result = rest_catalog
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
#[ignore]
async fn test_create_table() {
    let (rest_catalog, _minio, _mc, _rest) = setup_catalog("test_create_table_network").await;

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

    rest_catalog
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
            data_type: DataType::Int32,
            nullable: false,
            comment: Some("meow".to_string()),
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

    let table = rest_catalog
        .create_table(
            &ns,
            "t1",
            CreateTableOptions {
                columns: column_options.clone(),
                comment: Some("peow".to_string()),
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

    let TableKind::Table {
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
        && v.starts_with("s3://icebergdata/demo/test_create_table.apple.ios/t1/metadata/")));
    assert!(properties
        .iter()
        .any(|(k, v)| k == "metadata.last-updated-ms" && !v.is_empty()));
    assert!(properties
        .iter()
        .any(|(k, v)| k == "metadata.table-uuid" && !v.is_empty()));

    assert_eq!(table.name, "t1".to_string());
    assert_eq!(table.catalog, Some("test".to_string()));
    assert_eq!(table.database, Vec::<String>::from(ns.clone()));
    assert_eq!(comment, Some("peow".to_string()));
    assert_eq!(constraints, vec![]);
    assert_eq!(
        location,
        Some("s3://icebergdata/demo/test_create_table.apple.ios/t1".to_string())
    );
    assert_eq!(format, "iceberg".to_string());
    assert_eq!(partition_by, Vec::<String>::new());
    assert_eq!(sort_by, vec![]);
    assert_eq!(bucket_by, None);
    assert_eq!(options, Vec::<(String, String)>::new());
    assert_eq!(columns.len(), 3);
    assert!(
        columns.contains(&sail_common_datafusion::catalog::TableColumnStatus {
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
        columns.contains(&sail_common_datafusion::catalog::TableColumnStatus {
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
        columns.contains(&sail_common_datafusion::catalog::TableColumnStatus {
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

    let result = rest_catalog
        .create_table(
            &ns,
            "t1",
            CreateTableOptions {
                columns: column_options.clone(),
                comment: Some("peow".to_string()),
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
        .await;
    assert!(result.is_err());

    let result = rest_catalog
        .create_table(
            &ns,
            "t1",
            CreateTableOptions {
                columns: column_options.clone(),
                comment: Some("peow".to_string()),
                constraints: vec![],
                location: None,
                format: "iceberg".to_string(),
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

    let table = rest_catalog
        .create_table(
            &ns,
            "t2",
            CreateTableOptions {
                columns: column_options.clone(),
                comment: Some("test table".to_string()),
                constraints: vec![CatalogTableConstraint::PrimaryKey {
                    name: Some("pk_bar".to_string()),
                    columns: vec!["bar".to_string()],
                }],
                location: Some("s3://icebergdata/custom/path/meow".to_string()),
                format: "iceberg".to_string(),
                partition_by: vec!["baz".to_string()],
                sort_by: vec![
                    CatalogTableSort {
                        column: "bar".to_string(),
                        ascending: false,
                    },
                    CatalogTableSort {
                        column: "foo".to_string(),
                        ascending: true,
                    },
                ],
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
    assert_eq!(table.catalog, Some("test".to_string()));
    assert_eq!(table.database, Vec::<String>::from(ns.clone()));
    assert_eq!(comment, Some("test table".to_string()));
    assert_eq!(constraints.len(), 1);
    assert!(matches!(
        &constraints[0],
        CatalogTableConstraint::PrimaryKey { columns, .. } if columns == &vec!["bar".to_string()]
    ));
    assert_eq!(
        location,
        Some("s3://icebergdata/custom/path/meow".to_string())
    );
    assert_eq!(format, "iceberg".to_string());
    assert_eq!(partition_by, vec!["baz".to_string()]);
    assert_eq!(sort_by.len(), 2);
    assert!(sort_by.contains(&CatalogTableSort {
        column: "bar".to_string(),
        ascending: false,
    }));
    assert!(sort_by.contains(&CatalogTableSort {
        column: "foo".to_string(),
        ascending: true,
    }));
    assert_eq!(bucket_by, None);
    assert_eq!(options, vec![("key1".to_string(), "value1".to_string())]);
    assert_eq!(properties.len(), 17);
    assert!(properties.contains(&("owner".to_string(), "mr. meow".to_string())));
    assert!(properties.contains(&("team".to_string(), "data-eng".to_string())));
    assert_eq!(columns.len(), 3);
    assert!(
        columns.contains(&sail_common_datafusion::catalog::TableColumnStatus {
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
        columns.contains(&sail_common_datafusion::catalog::TableColumnStatus {
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
        columns.contains(&sail_common_datafusion::catalog::TableColumnStatus {
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
#[ignore]
async fn test_get_table() {
    let (rest_catalog, _minio, _mc, _rest) = setup_catalog("test_get_table_network").await;

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

    rest_catalog
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
            data_type: DataType::Int32,
            nullable: false,
            comment: Some("meow".to_string()),
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

    rest_catalog
        .create_table(
            &ns,
            "t2",
            CreateTableOptions {
                columns: column_options.clone(),
                comment: Some("test table".to_string()),
                constraints: vec![CatalogTableConstraint::PrimaryKey {
                    name: Some("pk_bar".to_string()),
                    columns: vec!["bar".to_string()],
                }],
                location: Some("s3://icebergdata/custom/path/meow".to_string()),
                format: "iceberg".to_string(),
                partition_by: vec!["baz".to_string()],
                sort_by: vec![
                    CatalogTableSort {
                        column: "bar".to_string(),
                        ascending: false,
                    },
                    CatalogTableSort {
                        column: "foo".to_string(),
                        ascending: true,
                    },
                ],
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

    let table = rest_catalog.get_table(&ns, "t2").await.unwrap();
    let TableKind::Table {
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
        ("comment".to_string(), "test table".to_string()),
        ("metadata.current-schema-id".to_string(), "0".to_string()),
        ("metadata.current-snapshot-id".to_string(), "-1".to_string()),
        (
            "metadata.default-sort-order-id".to_string(),
            "1".to_string(),
        ),
        ("metadata.default-spec-id".to_string(), "0".to_string()),
        ("metadata.format-version".to_string(), "2".to_string()),
        ("metadata.last-column-id".to_string(), "3".to_string()),
        ("metadata.last-partition-id".to_string(), "1000".to_string()),
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
        ("owner".to_string(), "mr. meow".to_string()),
        ("team".to_string(), "data-eng".to_string()),
    ];
    expected_properties.sort();

    assert_eq!(properties.len(), 17);
    assert_eq!(static_properties, expected_properties);
    assert!(properties.iter().any(|(k, v)| k == "metadata-location"
        && v.starts_with("s3://icebergdata/custom/path/meow/metadata/")));
    assert!(properties
        .iter()
        .any(|(k, v)| k == "metadata.last-updated-ms" && !v.is_empty()));
    assert!(properties
        .iter()
        .any(|(k, v)| k == "metadata.table-uuid" && !v.is_empty()));

    assert_eq!(table.name, "t2".to_string());
    assert_eq!(table.catalog, Some("test".to_string()));
    assert_eq!(table.database, Vec::<String>::from(ns.clone()));
    assert_eq!(comment, Some("test table".to_string()));
    assert_eq!(constraints.len(), 1);
    assert!(matches!(
        &constraints[0],
        CatalogTableConstraint::PrimaryKey { columns, .. } if columns == &vec!["bar".to_string()]
    ));
    assert_eq!(
        location,
        Some("s3://icebergdata/custom/path/meow".to_string())
    );
    assert_eq!(format, "iceberg".to_string());
    assert_eq!(partition_by, vec!["baz".to_string()]);
    assert_eq!(sort_by.len(), 2);
    assert!(sort_by.contains(&CatalogTableSort {
        column: "bar".to_string(),
        ascending: false,
    }));
    assert!(sort_by.contains(&CatalogTableSort {
        column: "foo".to_string(),
        ascending: true,
    }));
    assert_eq!(bucket_by, None);
    assert_eq!(options, vec![("key1".to_string(), "value1".to_string())]);
    assert_eq!(columns.len(), 3);
    assert!(
        columns.contains(&sail_common_datafusion::catalog::TableColumnStatus {
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
        columns.contains(&sail_common_datafusion::catalog::TableColumnStatus {
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
        columns.contains(&sail_common_datafusion::catalog::TableColumnStatus {
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
#[ignore]
async fn test_list_tables() {
    let (rest_catalog, _minio, _mc, _rest) = setup_catalog("test_list_tables_network").await;

    let ns = Namespace::try_from(vec!["test_list_tables".to_string()]).unwrap();

    rest_catalog
        .create_database(
            &ns,
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

    let tables = rest_catalog.list_tables(&ns).await.unwrap();
    assert!(tables.is_empty());

    rest_catalog
        .create_table(
            &ns,
            "table1",
            CreateTableOptions {
                columns: column_options.clone(),
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

    rest_catalog
        .create_table(
            &ns,
            "table2",
            CreateTableOptions {
                columns: column_options.clone(),
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

    let tables = rest_catalog.list_tables(&ns).await.unwrap();
    assert_eq!(tables.len(), 2);
    assert!(tables.iter().any(|t| t.name == "table1"));
    assert!(tables.iter().any(|t| t.name == "table2"));
    for table in &tables {
        let TableKind::Table { format, .. } = &table.kind else {
            panic!("Expected TableKind::Table");
        };
        assert_eq!(table.catalog, Some("test".to_string()));
        assert_eq!(table.database, vec!["test_list_tables".to_string()]);
        assert_eq!(format, "iceberg");
    }
}

#[tokio::test]
#[ignore]
async fn test_drop_table() {
    let (rest_catalog, _minio, _mc, _rest) = setup_catalog("test_drop_table_network").await;

    let namespace = Namespace::try_from(vec!["test_drop_table".to_string()]).unwrap();

    rest_catalog
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

    rest_catalog
        .create_table(
            &namespace,
            "t1",
            CreateTableOptions {
                columns: column_options.clone(),
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

    let result = rest_catalog.get_table(&namespace, "t1").await;
    assert!(result.is_ok());

    rest_catalog
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

    let result = rest_catalog.get_table(&namespace, "t1").await;
    assert!(result.is_err());

    let result = rest_catalog
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

    let result = rest_catalog
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

    rest_catalog
        .create_table(
            &namespace,
            "t2",
            CreateTableOptions {
                columns: column_options.clone(),
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

    let result = rest_catalog.get_table(&namespace, "t2").await;
    assert!(result.is_ok());

    rest_catalog
        .drop_table(
            &namespace,
            "t2",
            DropTableOptions {
                if_exists: false,
                purge: true,
            },
        )
        .await
        .unwrap();

    let result = rest_catalog.get_table(&namespace, "t2").await;
    assert!(result.is_err());
}

#[tokio::test]
#[ignore]
async fn test_create_view() {
    let (rest_catalog, _minio, _mc, _rest) = setup_catalog("test_create_view_network").await;

    let ns = Namespace::try_from(vec!["test_create_view".to_string()]).unwrap();

    rest_catalog
        .create_database(
            &ns,
            CreateDatabaseOptions {
                if_not_exists: false,
                comment: None,
                location: None,
                properties: vec![],
            },
        )
        .await
        .unwrap();

    let column_options = vec![
        CreateViewColumnOptions {
            name: "col1".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            comment: None,
        },
        CreateViewColumnOptions {
            name: "col2".to_string(),
            data_type: DataType::Int32,
            nullable: false,
            comment: Some("important column".to_string()),
        },
    ];

    let view = rest_catalog
        .create_view(
            &ns,
            "view1",
            CreateViewOptions {
                columns: column_options.clone(),
                comment: Some("test view".to_string()),
                properties: vec![],
                definition: "SELECT * FROM table1".to_string(),
                if_not_exists: false,
                replace: false,
            },
        )
        .await
        .unwrap();

    let TableKind::View {
        columns,
        comment,
        properties,
        definition,
    } = view.kind
    else {
        panic!("Expected TableKind::View");
    };

    let mut static_properties: Vec<_> = properties
        .iter()
        .filter(|(k, _)| k != "metadata-location" && k != "metadata.view-uuid")
        .cloned()
        .collect();
    static_properties.sort();

    let mut expected_properties: Vec<(String, String)> = vec![
        ("comment".to_string(), "test view".to_string()),
        ("metadata.format-version".to_string(), "1".to_string()),
        (
            "metadata.location".to_string(),
            "s3://icebergdata/demo/test_create_view/view1".to_string(),
        ),
        ("metadata.current-version-id".to_string(), "1".to_string()),
    ];
    expected_properties.sort();

    assert_eq!(properties.len(), 6);
    assert!(properties.iter().any(|(k, v)| k == "metadata-location"
        && v.starts_with("s3://icebergdata/demo/test_create_view/view1/metadata/")));
    assert!(properties
        .iter()
        .any(|(k, v)| k == "metadata.view-uuid" && !v.is_empty()));
    assert_eq!(static_properties, expected_properties);

    assert_eq!(view.name, "view1".to_string());
    assert_eq!(view.catalog, Some("test".to_string()));
    assert_eq!(view.database, Vec::<String>::from(ns.clone()));
    assert_eq!(comment, Some("test view".to_string()));
    assert_eq!(definition, "SELECT * FROM table1".to_string());
    assert_eq!(columns.len(), 2);
    assert!(columns.contains(&TableColumnStatus {
        name: "col1".to_string(),
        data_type: DataType::Utf8,
        nullable: true,
        comment: None,
        default: None,
        generated_always_as: None,
        is_partition: false,
        is_bucket: false,
        is_cluster: false,
    }));
    assert!(columns.contains(&TableColumnStatus {
        name: "col2".to_string(),
        data_type: DataType::Int32,
        nullable: false,
        comment: Some("important column".to_string()),
        default: None,
        generated_always_as: None,
        is_partition: false,
        is_bucket: false,
        is_cluster: false,
    }));

    let result = rest_catalog
        .create_view(
            &ns,
            "view1",
            CreateViewOptions {
                columns: column_options.clone(),
                comment: Some("test view".to_string()),
                properties: vec![],
                definition: "SELECT * FROM table1".to_string(),
                if_not_exists: false,
                replace: false,
            },
        )
        .await;
    assert!(result.is_err());

    let result = rest_catalog
        .create_view(
            &ns,
            "view1",
            CreateViewOptions {
                columns: column_options.clone(),
                comment: Some("test view".to_string()),
                properties: vec![],
                definition: "SELECT * FROM table1".to_string(),
                if_not_exists: true,
                replace: false,
            },
        )
        .await;
    assert!(result.is_ok());

    let view = rest_catalog
        .create_view(
            &ns,
            "view2",
            CreateViewOptions {
                columns: column_options.clone(),
                comment: Some("another view".to_string()),
                properties: vec![
                    ("owner".to_string(), "alice".to_string()),
                    ("team".to_string(), "analytics".to_string()),
                ],
                definition: "SELECT col1, col2 FROM table2 WHERE col2 > 10".to_string(),
                if_not_exists: false,
                replace: false,
            },
        )
        .await
        .unwrap();

    let TableKind::View {
        columns,
        comment,
        properties,
        definition,
    } = view.kind
    else {
        panic!("Expected TableKind::View");
    };

    assert_eq!(view.name, "view2".to_string());
    assert_eq!(view.catalog, Some("test".to_string()));
    assert_eq!(view.database, Vec::<String>::from(ns.clone()));
    assert_eq!(columns.len(), 2);
    assert!(columns.contains(&TableColumnStatus {
        name: "col1".to_string(),
        data_type: DataType::Utf8,
        nullable: true,
        comment: None,
        default: None,
        generated_always_as: None,
        is_partition: false,
        is_bucket: false,
        is_cluster: false,
    }));
    assert!(columns.contains(&TableColumnStatus {
        name: "col2".to_string(),
        data_type: DataType::Int32,
        nullable: false,
        comment: Some("important column".to_string()),
        default: None,
        generated_always_as: None,
        is_partition: false,
        is_bucket: false,
        is_cluster: false,
    }));
    assert_eq!(comment, Some("another view".to_string()));
    assert_eq!(
        definition,
        "SELECT col1, col2 FROM table2 WHERE col2 > 10".to_string()
    );
    assert!(properties.contains(&("owner".to_string(), "alice".to_string())));
    assert!(properties.contains(&("team".to_string(), "analytics".to_string())));
}

#[tokio::test]
#[ignore]
async fn test_get_view() {
    let (rest_catalog, _minio, _mc, _rest) = setup_catalog("test_get_view_network").await;

    let ns = Namespace::try_from(vec!["test_get_view".to_string()]).unwrap();

    rest_catalog
        .create_database(
            &ns,
            CreateDatabaseOptions {
                if_not_exists: false,
                comment: None,
                location: None,
                properties: vec![],
            },
        )
        .await
        .unwrap();

    let column_options = vec![
        CreateViewColumnOptions {
            name: "foo".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            comment: None,
        },
        CreateViewColumnOptions {
            name: "bar".to_string(),
            data_type: DataType::Int32,
            nullable: false,
            comment: Some("meow".to_string()),
        },
    ];

    rest_catalog
        .create_view(
            &ns,
            "view1",
            CreateViewOptions {
                columns: column_options.clone(),
                comment: Some("test view".to_string()),
                properties: vec![
                    ("owner".to_string(), "bob".to_string()),
                    ("version".to_string(), "1.0".to_string()),
                ],
                definition: "SELECT foo, bar FROM source_table".to_string(),
                if_not_exists: false,
                replace: false,
            },
        )
        .await
        .unwrap();

    let view = rest_catalog.get_view(&ns, "view1").await.unwrap();

    let TableKind::View {
        columns,
        comment,
        properties,
        definition,
    } = view.kind
    else {
        panic!("Expected TableKind::View");
    };

    assert_eq!(view.name, "view1".to_string());
    assert_eq!(view.catalog, Some("test".to_string()));
    assert_eq!(view.database, Vec::<String>::from(ns.clone()));
    assert_eq!(comment, Some("test view".to_string()));
    assert_eq!(definition, "SELECT foo, bar FROM source_table".to_string());
    assert_eq!(columns.len(), 2);
    assert!(columns.contains(&TableColumnStatus {
        name: "foo".to_string(),
        data_type: DataType::Utf8,
        nullable: true,
        comment: None,
        default: None,
        generated_always_as: None,
        is_partition: false,
        is_bucket: false,
        is_cluster: false,
    }));
    assert!(columns.contains(&TableColumnStatus {
        name: "bar".to_string(),
        data_type: DataType::Int32,
        nullable: false,
        comment: Some("meow".to_string()),
        default: None,
        generated_always_as: None,
        is_partition: false,
        is_bucket: false,
        is_cluster: false,
    }));
    assert!(properties.contains(&("owner".to_string(), "bob".to_string())));
    assert!(properties.contains(&("version".to_string(), "1.0".to_string())));

    let result = rest_catalog.get_view(&ns, "nonexistent").await;
    assert!(result.is_err());
}

#[tokio::test]
#[ignore]
async fn test_list_views() {
    let (rest_catalog, _minio, _mc, _rest) = setup_catalog("test_list_views_network").await;

    let ns = Namespace::try_from(vec!["test_list_views".to_string()]).unwrap();

    rest_catalog
        .create_database(
            &ns,
            CreateDatabaseOptions {
                if_not_exists: false,
                comment: None,
                location: None,
                properties: vec![],
            },
        )
        .await
        .unwrap();

    let column_options = vec![CreateViewColumnOptions {
        name: "id".to_string(),
        data_type: DataType::Int32,
        nullable: false,
        comment: None,
    }];

    let views = rest_catalog.list_views(&ns).await.unwrap();
    assert!(views.is_empty());

    rest_catalog
        .create_view(
            &ns,
            "view1",
            CreateViewOptions {
                columns: column_options.clone(),
                comment: None,
                properties: vec![],
                definition: "SELECT * FROM t1".to_string(),
                if_not_exists: false,
                replace: false,
            },
        )
        .await
        .unwrap();

    rest_catalog
        .create_view(
            &ns,
            "view2",
            CreateViewOptions {
                columns: column_options.clone(),
                comment: None,
                properties: vec![],
                definition: "SELECT * FROM t2".to_string(),
                if_not_exists: false,
                replace: false,
            },
        )
        .await
        .unwrap();

    let views = rest_catalog.list_views(&ns).await.unwrap();
    assert_eq!(views.len(), 2);
    assert!(views.iter().any(|v| v.name == "view1"));
    assert!(views.iter().any(|v| v.name == "view2"));

    for view in &views {
        let TableKind::View { .. } = &view.kind else {
            panic!("Expected TableKind::View");
        };
        assert_eq!(view.catalog, Some("test".to_string()));
        assert_eq!(view.database, vec!["test_list_views".to_string()]);
    }
}

#[tokio::test]
#[ignore]
async fn test_drop_view() {
    let (rest_catalog, _minio, _mc, _rest) = setup_catalog("test_drop_view_network").await;

    let namespace = Namespace::try_from(vec!["test_drop_view".to_string()]).unwrap();

    rest_catalog
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

    let column_options = vec![CreateViewColumnOptions {
        name: "id".to_string(),
        data_type: DataType::Int32,
        nullable: false,
        comment: None,
    }];

    rest_catalog
        .create_view(
            &namespace,
            "view1",
            CreateViewOptions {
                columns: column_options.clone(),
                comment: None,
                properties: vec![],
                definition: "SELECT * FROM t1".to_string(),
                if_not_exists: false,
                replace: false,
            },
        )
        .await
        .unwrap();

    let result = rest_catalog.get_view(&namespace, "view1").await;
    assert!(result.is_ok());

    rest_catalog
        .drop_view(&namespace, "view1", DropViewOptions { if_exists: false })
        .await
        .unwrap();

    let result = rest_catalog.get_view(&namespace, "view1").await;
    assert!(result.is_err());

    let result = rest_catalog
        .drop_view(&namespace, "view1", DropViewOptions { if_exists: false })
        .await;
    assert!(result.is_err());

    let result = rest_catalog
        .drop_view(&namespace, "view1", DropViewOptions { if_exists: true })
        .await;
    assert!(result.is_ok());
}
