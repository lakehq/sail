mod common;

use common::{setup_glue_catalog, simple_database_options};
use sail_catalog::provider::{CatalogProvider, Namespace, CreateTableOptions, CreateTableColumnOptions};
use sail_catalog_glue::{GlueCatalogConfig, GlueCatalogProvider};
use arrow::datatypes::DataType;

#[tokio::test]
#[ignore]
async fn test_database_list_cache() {
    // We use the moto container from setup_glue_catalog but we want to control the config
    let (catalog_no_cache, moto) = setup_glue_catalog("test_database_list_cache").await;
    
    let host = moto.get_host().await.expect("get host");
    let port = moto.get_host_port_ipv4(5000).await.expect("get port");
    let endpoint = format!("http://{host}:{port}");

    // 1. Test without cache (default behavior)
    let ns1 = Namespace::try_from(vec!["db1".to_string()]).unwrap();
    catalog_no_cache.create_database(&ns1, simple_database_options()).await.unwrap();

    let dbs = catalog_no_cache.list_databases(None).await.unwrap();
    assert_eq!(dbs.len(), 1);

    let ns2 = Namespace::try_from(vec!["db2".to_string()]).unwrap();
    catalog_no_cache.create_database(&ns2, simple_database_options()).await.unwrap();

    let dbs = catalog_no_cache.list_databases(None).await.unwrap();
    assert_eq!(dbs.len(), 2, "Without cache, the new database should be visible immediately");

    // 2. Test with cache ENABLED
    let config_with_cache = GlueCatalogConfig {
        region: Some("us-east-1".to_string()),
        endpoint_url: Some(endpoint.clone()),
        cache_db_enable: true,
        cache_table_enable: true,
        cache_ttl_secs: 1800,
    };
    let catalog_with_cache = GlueCatalogProvider::new("test_cache".to_string(), config_with_cache);

    // Initial list (should be 2 databases from previous steps)
    let dbs = catalog_with_cache.list_databases(None).await.unwrap();
    assert_eq!(dbs.len(), 2);

    // Create a new database directly via Glue (or another provider instance)
    let ns3 = Namespace::try_from(vec!["db3".to_string()]).unwrap();
    catalog_no_cache.create_database(&ns3, simple_database_options()).await.unwrap();

    // List again with the cached provider
    let dbs = catalog_with_cache.list_databases(None).await.unwrap();
    assert_eq!(dbs.len(), 2, "With cache enabled, the new database from ANOTHER provider should NOT be visible yet (HIT)");

    // 3. Test invalidation within the SAME provider instance
    let ns4 = Namespace::try_from(vec!["db4".to_string()]).unwrap();
    catalog_with_cache.create_database(&ns4, simple_database_options()).await.unwrap();
    
    // List again with the SAME cached provider
    let dbs = catalog_with_cache.list_databases(None).await.unwrap();
    assert_eq!(dbs.len(), 4, "After create_database on the same provider, the cache should be invalidated and show 4 dbs (db1, db2, db3, db4)");
}

#[tokio::test]
#[ignore]
async fn test_table_list_cache() {
    let (catalog_no_cache, moto) = setup_glue_catalog("test_table_list_cache").await;
    
    let host = moto.get_host().await.expect("get host");
    let port = moto.get_host_port_ipv4(5000).await.expect("get port");
    let endpoint = format!("http://{host}:{port}");

    let ns = Namespace::try_from(vec!["test_db".to_string()]).unwrap();
    catalog_no_cache.create_database(&ns, simple_database_options()).await.unwrap();

    let table_options = CreateTableOptions {
        columns: vec![CreateTableColumnOptions {
            name: "c1".to_string(),
            data_type: DataType::Int32,
            nullable: false,
            comment: None,
            default: None,
            generated_always_as: None,
        }],
        comment: None,
        constraints: vec![],
        location: Some("s3://bucket/table1".to_string()),
        format: "parquet".to_string(),
        partition_by: vec![],
        sort_by: vec![],
        bucket_by: None,
        if_not_exists: false,
        replace: false,
        properties: vec![],
    };

    catalog_no_cache.create_table(&ns, "table1", table_options.clone()).await.unwrap();

    // Test with cache ENABLED
    let config_with_cache = GlueCatalogConfig {
        region: Some("us-east-1".to_string()),
        endpoint_url: Some(endpoint),
        cache_db_enable: true,
        cache_table_enable: true,
        cache_ttl_secs: 1800,
    };
    let catalog_with_cache = GlueCatalogProvider::new("test_cache_table".to_string(), config_with_cache);

    // Initial list
    let tables = catalog_with_cache.list_tables(&ns).await.unwrap();
    assert_eq!(tables.len(), 1);

    // Create another table via no-cache provider
    catalog_no_cache.create_table(&ns, "table2", table_options.clone()).await.unwrap();

    // List again with cached provider
    let tables = catalog_with_cache.list_tables(&ns).await.unwrap();
    assert_eq!(tables.len(), 1, "With table cache enabled, the new table from ANOTHER provider should NOT be visible yet (HIT)");

    // Create a third table via SAME cached provider
    catalog_with_cache.create_table(&ns, "table3", table_options).await.unwrap();

    // List again with the SAME cached provider
    let tables = catalog_with_cache.list_tables(&ns).await.unwrap();
    assert_eq!(tables.len(), 3, "After create_table on the same provider, the cache should be invalidated and show 3 tables");
}
