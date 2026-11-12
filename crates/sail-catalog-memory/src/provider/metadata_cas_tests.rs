#![expect(clippy::expect_used)]
#![expect(clippy::panic, reason = "unexpected test outcomes must fail the test")]

use sail_catalog::lakehouse::{
    IcebergMetadataCasOutcome as Outcome, IcebergMetadataCasRequest as Request,
};

use super::*;

fn pointer_update(key: &str, expected: Option<&str>, replacement: &str) -> AlterTableOptions {
    let mut properties = vec![(key.into(), replacement.into())];
    if let Some(expected) = expected {
        properties.push((" PrEvIoUs_MeTaDaTa_LoCaTiOn ".into(), expected.into()));
    }
    AlterTableOptions::SetTableProperties { properties }
}

#[tokio::test]
async fn ordinary_publication_preserves_canonical_pointer_and_atomicity() {
    for captured_key in [
        "metadata-location",
        "metadata_location",
        " MeTaDaTa-LoCaTiOn ",
    ] {
        let namespace = Namespace::try_from(vec!["db".to_string()]).expect("namespace");
        let provider = MemoryCatalogProvider::new("memory".into(), namespace.clone(), None);
        provider
            .databases
            .get_mut(&namespace)
            .expect("db")
            .tables
            .insert(
                "t".into(),
                TableStatus {
                    catalog: Some("memory".into()),
                    database: vec!["db".into()],
                    name: "t".into(),
                    kind: TableKind::Table {
                        columns: vec![],
                        comment: None,
                        constraints: vec![],
                        location: None,
                        format: "iceberg".into(),
                        partition_by: vec![],
                        sort_by: vec![],
                        bucket_by: None,
                        properties: vec![
                            (captured_key.into(), "A".into()),
                            ("owner".into(), "preserved".into()),
                        ],
                        is_external: true,
                    },
                },
            );
        assert_eq!(
            provider
                .compare_and_swap_iceberg_metadata(
                    &namespace,
                    "t",
                    Request::new("A".into(), "B".into()).expect("request")
                )
                .await
                .expect("CAS"),
            Outcome::Committed
        );
        provider
            .alter_table_atomically(
                &namespace,
                "t",
                vec![pointer_update(captured_key, Some("B"), "C")],
            )
            .await
            .expect("ordinary publication");
        let status = provider.get_table(&namespace, "t").await.expect("table");
        let TableKind::Table { properties, .. } = status.kind else {
            panic!("table")
        };
        assert_eq!(
            properties,
            vec![
                ("owner".into(), "preserved".into()),
                ("metadata_location".into(), "C".into()),
                ("previous_metadata_location".into(), "B".into())
            ]
        );
        assert_eq!(
            provider
                .compare_and_swap_iceberg_metadata(
                    &namespace,
                    "t",
                    Request::new("C".into(), "D".into()).expect("request")
                )
                .await
                .expect("CAS"),
            Outcome::Committed
        );

        {
            let mut db = provider.databases.get_mut(&namespace).expect("db");
            let TableKind::Table { properties, .. } =
                &mut db.tables.get_mut("t").expect("table").kind
            else {
                panic!("table")
            };
            properties.extend([
                (" metadata-location ".into(), "D".into()),
                ("metadata_location".into(), "D".into()),
            ]);
        }
        // Equal aliases collapse, chained options see the immediately preceding pointer,
        // and an update without a predecessor preserves the existing predecessor.
        provider
            .alter_table_atomically(
                &namespace,
                "t",
                vec![
                    AlterTableOptions::SetTableProperties {
                        properties: vec![
                            (captured_key.into(), "E".into()),
                            ("METADATA_LOCATION".into(), "E".into()),
                            ("previous_metadata_location".into(), "D".into()),
                        ],
                    },
                    pointer_update(captured_key, Some("E"), "F"),
                    pointer_update(captured_key, None, "D"),
                ],
            )
            .await
            .expect("chained publication");
        let TableKind::Table { properties, .. } = provider
            .get_table(&namespace, "t")
            .await
            .expect("table")
            .kind
        else {
            panic!("table")
        };
        assert_eq!(
            properties,
            vec![
                ("owner".into(), "preserved".into()),
                ("previous_metadata_location".into(), "E".into()),
                ("metadata_location".into(), "D".into())
            ]
        );
        for failure in ["stale", "later", "incoming", "previous", "current"] {
            if failure == "current" {
                let mut db = provider.databases.get_mut(&namespace).expect("db");
                let TableKind::Table { properties, .. } =
                    &mut db.tables.get_mut("t").expect("table").kind
                else {
                    panic!("table")
                };
                properties.push((" metadata-location ".into(), "conflict".into()));
            }
            let TableKind::Table {
                properties: before, ..
            } = provider
                .get_table(&namespace, "t")
                .await
                .expect("table")
                .kind
            else {
                panic!("table")
            };
            let generation = provider
                .databases
                .get(&namespace)
                .expect("db")
                .table_generations
                .get("t")
                .copied();
            let mut options = vec![pointer_update(
                captured_key,
                Some(if failure == "stale" { "C" } else { "D" }),
                "E",
            )];
            match failure {
                "later" => options.push(AlterTableOptions::UnsetTableProperties {
                    keys: vec!["missing".into()],
                    if_exists: false,
                }),
                "incoming" | "previous" => {
                    let AlterTableOptions::SetTableProperties { properties } = &mut options[0]
                    else {
                        panic!("properties")
                    };
                    properties.push((
                        if failure == "incoming" {
                            "METADATA_LOCATION"
                        } else {
                            "previous_metadata_location"
                        }
                        .into(),
                        "conflict".into(),
                    ));
                }
                _ => {}
            }
            assert!(
                provider
                    .alter_table_atomically(&namespace, "t", options)
                    .await
                    .is_err(),
                "{failure}"
            );
            let TableKind::Table {
                properties: after, ..
            } = provider
                .get_table(&namespace, "t")
                .await
                .expect("table")
                .kind
            else {
                panic!("table")
            };
            assert_eq!(after, before);
            assert_eq!(
                provider
                    .databases
                    .get(&namespace)
                    .expect("db")
                    .table_generations
                    .get("t")
                    .copied(),
                generation
            );
        }
        provider
            .alter_table_atomically(
                &namespace,
                "t",
                vec![AlterTableOptions::SetTableProperties {
                    properties: vec![("owner".into(), "updated".into())],
                }],
            )
            .await
            .expect("generic properties ignore pointer aliases");
        {
            let mut db = provider.databases.get_mut(&namespace).expect("db");
            let TableKind::Table {
                format, properties, ..
            } = &mut db.tables.get_mut("t").expect("table").kind
            else {
                panic!("table")
            };
            *format = "delta".into();
            *properties = vec![("metadata_location".into(), "D".into())];
        }
        provider
            .alter_table_atomically(
                &namespace,
                "t",
                vec![pointer_update("metadata-location", None, "E")],
            )
            .await
            .expect("non-Iceberg update");
        let TableKind::Table { properties, .. } = provider
            .get_table(&namespace, "t")
            .await
            .expect("table")
            .kind
        else {
            panic!("table")
        };
        assert_eq!(
            properties,
            vec![
                ("metadata_location".into(), "D".into()),
                ("metadata-location".into(), "E".into())
            ]
        );
    }
}

#[tokio::test]
async fn competing_pointer_replacements_have_exactly_one_winner() {
    let namespace = Namespace::try_from(vec!["db".to_string()]).expect("namespace");
    let provider = Arc::new(MemoryCatalogProvider::new(
        "memory".into(),
        namespace.clone(),
        None,
    ));
    provider
        .databases
        .get_mut(&namespace)
        .expect("db")
        .tables
        .insert(
            "t".into(),
            TableStatus {
                catalog: Some("memory".into()),
                database: vec!["db".into()],
                name: "t".into(),
                kind: TableKind::Table {
                    columns: vec![],
                    comment: None,
                    constraints: vec![],
                    location: Some("memory://bucket/t".into()),
                    format: "iceberg".into(),
                    partition_by: vec![],
                    sort_by: vec![],
                    bucket_by: None,
                    properties: vec![
                        ("metadata_location".into(), "v1".into()),
                        ("metadata-location".into(), "v1".into()),
                        ("MeTaDaTa-LoCaTiOn".into(), "v1".into()),
                        ("metadata_location".into(), "v1".into()),
                        ("owner".into(), "unchanged".into()),
                    ],
                    is_external: true,
                },
            },
        );
    let mut tasks = vec![];
    for version in ["v2-a", "v2-b"] {
        let provider = provider.clone();
        let namespace = namespace.clone();
        tasks.push(tokio::spawn(async move {
            provider
                .compare_and_swap_iceberg_metadata(
                    &namespace,
                    "t",
                    Request::new("v1".into(), version.into()).expect("request"),
                )
                .await
                .expect("CAS")
        }));
    }
    let mut wins = 0;
    let mut conflicts = 0;
    for task in tasks {
        match task.await.expect("task") {
            Outcome::Committed => wins += 1,
            Outcome::Conflict { .. } => conflicts += 1,
            outcome => panic!("unexpected outcome: {outcome:?}"),
        }
    }
    assert_eq!((wins, conflicts), (1, 1));
    let status = provider.get_table(&namespace, "t").await.expect("table");
    let TableKind::Table { properties, .. } = status.kind else {
        panic!("table")
    };
    assert!(properties.contains(&("owner".into(), "unchanged".into())));
    assert!(properties.contains(&("previous_metadata_location".into(), "v1".into())));
    assert_eq!(
        properties
            .iter()
            .filter(
                |(key, _)| sail_common_datafusion::catalog::managed::is_metadata_location_key(key)
            )
            .count(),
        1
    );
    let request = Request::new("v1".into(), "v2".into()).expect("request");
    assert!(matches!(
        request.validate_base([
            ("metadata_location", "v1"),
            ("metadata-location", "different")
        ]),
        Err(Outcome::Rejected { .. })
    ));
    assert!(Request::new("".into(), "v2".into()).is_err());
    assert!(Request::new("v1".into(), "".into()).is_err());
    assert!(Request::new("v1".into(), "v1".into()).is_err());
}
