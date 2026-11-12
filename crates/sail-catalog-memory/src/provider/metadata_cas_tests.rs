#![expect(clippy::expect_used)]
#![expect(clippy::panic, reason = "unexpected test outcomes must fail the test")]

use sail_catalog::lakehouse::{
    IcebergMetadataCasOutcome as Outcome, IcebergMetadataCasRequest as Request,
};

use super::*;

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
