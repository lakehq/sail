#![expect(clippy::expect_used)]

use std::sync::Arc;

use super::*;

mod wire;

#[test]
fn metadata_aliases_publish_only_canonical_pointer_and_predecessor() {
    use sail_catalog::lakehouse::IcebergMetadataCasRequest;
    for aliases in [
        vec![
            "metadata_location",
            "metadata-location",
            "MeTaDaTa-LoCaTiOn",
        ],
        vec!["metadata-location"],
        vec!["MeTaDaTa-LoCaTiOn"],
    ] {
        let mut parameters = aliases
            .iter()
            .map(|key| ((*key).into(), "old".into()))
            .collect();
        let request = IcebergMetadataCasRequest::new("old".into(), "new".into()).expect("request");
        update_metadata_parameters(&mut parameters, &request);
        assert_eq!(
            parameters
                .get("previous_metadata_location")
                .map(String::as_str),
            Some("old")
        );
        assert_eq!(
            parameters.get("metadata_location").map(String::as_str),
            Some("new")
        );
        for alias in aliases {
            assert_eq!(
                parameters.get(alias).map(String::as_str),
                (alias == "metadata_location").then_some("new")
            );
        }
    }
}

#[test]
fn inconsistent_metadata_aliases_are_not_normalized() {
    use sail_catalog::lakehouse::{IcebergMetadataCasOutcome, IcebergMetadataCasRequest};
    let request = IcebergMetadataCasRequest::new("old".into(), "new".into()).expect("request");
    for key in ["metadata_location", "MeTaDaTa-LoCaTiOn"] {
        assert!(matches!(
            request.validate_base([(key, "old"), ("metadata-location", "different")]),
            Err(IcebergMetadataCasOutcome::Rejected { .. })
        ));
    }
}

#[tokio::test]
async fn hms_advertises_lock_backed_publication_by_default() {
    use sail_catalog::lakehouse::LakehouseCapability;
    use sail_catalog::provider::CatalogProvider;
    let handle = tokio::runtime::Handle::current();
    let provider = HmsCatalogProvider::try_new(
        "hms".into(),
        crate::provider::HmsCatalogConfig {
            uris: vec!["127.0.0.1:1".into()],
            ..Default::default()
        },
        sail_common::runtime::RuntimeHandle::new(handle.clone(), handle),
    )
    .expect("provider");
    assert_eq!(
        provider.lakehouse_capabilities(),
        vec![LakehouseCapability::IcebergMetadataLocationCas]
    );
}

#[tokio::test]
#[ignore = "requires an isolated HMS service at SAIL_TEST_HMS_URI"]
async fn real_hms_competing_and_stale_metadata_cas() {
    use std::collections::HashMap;

    use sail_catalog::lakehouse::{
        IcebergMetadataCasOutcome as Outcome, IcebergMetadataCasRequest as Request,
    };
    use sail_catalog::provider::{CatalogProvider, Namespace};
    use sail_common::runtime::RuntimeHandle;
    use sail_common_hms::hms::{Database, FieldSchema, SerDeInfo, StorageDescriptor};

    use crate::provider::HmsCatalogConfig;
    let uri = std::env::var("SAIL_TEST_HMS_URI").expect("isolated HMS URI");
    let handle = tokio::runtime::Handle::current();
    let provider = Arc::new(
        HmsCatalogProvider::try_new(
            "hms".into(),
            HmsCatalogConfig {
                uris: vec![uri],
                ..Default::default()
            },
            RuntimeHandle::new(handle.clone(), handle),
        )
        .expect("provider"),
    );
    let (_, client) = provider.current_client().await.expect("client");
    let db = format!(
        "sail_cas_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    );
    assert!(matches!(
        client
            .create_database(Database {
                name: Some(db.clone()),
                ..Default::default()
            })
            .await
            .expect("create database"),
        MaybeException::Ok(())
    ));
    let namespace = Namespace::try_from(vec![db.clone()]).expect("namespace");
    for aliases in [
        vec!["metadata_location"],
        vec!["metadata-location"],
        vec!["MeTaDaTa-LoCaTiOn"],
        vec![
            "metadata_location",
            "metadata-location",
            "MeTaDaTa-LoCaTiOn",
        ],
    ] {
        let table = "items";
        let mut parameters = HashMap::from([("table_type".into(), "ICEBERG".into())]);
        parameters.extend(aliases.iter().map(|key| ((*key).into(), "old".into())));
        assert!(matches!(
            client
                .create_table(Table {
                    db_name: Some(db.clone()),
                    table_name: Some(table.into()),
                    table_type: Some("EXTERNAL_TABLE".into()),
                    parameters: Some(parameters),
                    partition_keys: Some(vec![]),
                    sd: Some(StorageDescriptor {
                        cols: Some(vec![FieldSchema {
                            name: Some("id".into()),
                            r#type: Some("bigint".into()),
                            ..Default::default()
                        }]),
                        location: Some(format!("file:///tmp/{db}/{table}")),
                        input_format: Some("org.apache.hadoop.mapred.TextInputFormat".into()),
                        output_format: Some(
                            "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat".into()
                        ),
                        serde_info: Some(SerDeInfo {
                            serialization_lib: Some(
                                "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe".into()
                            ),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                })
                .await
                .expect("create table"),
            MaybeException::Ok(())
        ));
        let mut tasks = vec![];
        for replacement in ["new-a", "new-b"] {
            let provider = provider.clone();
            let namespace = namespace.clone();
            tasks.push(tokio::spawn(async move {
                provider
                    .compare_and_swap_iceberg_metadata(
                        &namespace,
                        "items",
                        Request::new("old".into(), replacement.into()).expect("request"),
                    )
                    .await
                    .expect("CAS")
            }));
        }
        let mut outcomes = vec![];
        for task in tasks {
            outcomes.push(task.await.expect("task"));
        }
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, Outcome::Committed))
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, Outcome::Conflict { .. }))
                .count(),
            1
        );
        let current = HmsCatalogProvider::get_table_with_client(&client, &db, table, "read")
            .await
            .expect("table");
        let parameters = current.parameters.expect("parameters");
        assert!(matches!(
            metadata_location_value(parameters.iter().map(|(k, v)| (k.as_str(), v.as_str()))),
            Some("new-a" | "new-b")
        ));
        let canonical = parameters
            .get("metadata_location")
            .expect("canonical pointer");
        assert!(matches!(canonical.as_str(), "new-a" | "new-b"));
        for key in &aliases {
            assert_eq!(
                parameters.get(*key),
                (*key == "metadata_location").then_some(canonical)
            );
        }
        assert!(matches!(
            provider
                .compare_and_swap_iceberg_metadata(
                    &namespace,
                    table,
                    Request::new("old".into(), "stale".into()).expect("request")
                )
                .await
                .expect("stale CAS"),
            Outcome::Conflict { .. }
        ));
        // Simulate a cooperative writer: lock, reload, advance the canonical key, unlock.
        let official_lock = acquire_table_lock(&client, &db, table).await.expect("lock");
        let mut official =
            HmsCatalogProvider::get_table_with_client(&client, &db, table, "official base")
                .await
                .expect("table");
        official
            .parameters
            .as_mut()
            .expect("parameters")
            .insert("metadata_location".into(), "official-new".into());
        locked_publication(&client, &db, table, official)
            .await
            .expect("cooperative append");
        release_table_lock(&client, &db, table, official_lock)
            .await
            .expect("unlock");
        assert!(matches!(
            provider
                .compare_and_swap_iceberg_metadata(
                    &namespace,
                    table,
                    Request::new("official-new".into(), "sail-reentry".into()).expect("request")
                )
                .await
                .expect("Sail reentry"),
            Outcome::Committed
        ));
        let after = HmsCatalogProvider::get_table_with_client(&client, &db, table, "after reentry")
            .await
            .expect("table");
        let parameters = after.parameters.expect("parameters");
        assert_eq!(
            parameters.get("metadata_location").map(String::as_str),
            Some("sail-reentry")
        );
        assert_eq!(
            parameters
                .keys()
                .filter(
                    |key| sail_common_datafusion::catalog::managed::is_metadata_location_key(
                        key.trim()
                    )
                )
                .count(),
            1
        );
        assert!(matches!(
            client
                .drop_table(db.clone(), table.into(), false)
                .await
                .expect("drop table"),
            MaybeException::Ok(())
        ));
    }
    assert!(matches!(
        client
            .drop_database(db, false, false)
            .await
            .expect("drop database"),
        MaybeException::Ok(())
    ));
}
