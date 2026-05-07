//! Ignored live-HMS test proving Spark metadata survives a real Thrift
//! write/read cycle through the internal `format` and `columns` fields rather
//! than through raw `spark.sql.*` properties.  Runs only when
//! `cargo nextest run --run-ignored ignored-only` is used with a live HMS
//! container.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use arrow::datatypes::DataType;
use sail_catalog::provider::CatalogProvider;
use sail_common_datafusion::catalog::TableKind;

use crate::common::{col, setup_with_database, simple_table_options_with_format};

/// Verifies that Spark metadata injected by `inject_spark_metadata` survives a
/// full HMS Thrift write → read round trip, surfaced through the parsed
/// `format` and `columns` fields of `TableStatus`.
///
/// The `create_table` call writes `spark.sql.sources.provider` and
/// `spark.sql.sources.schema` into the HMS table parameters.  On the read
/// path, `table_to_status` consumes these internal properties to populate the
/// `format` and `columns` fields of `TableKind::Table`, then filters them
/// from the user-visible `properties` map via `filter_spark_properties`.
/// This is by design: `spark.sql.*` keys are internal bookkeeping and must
/// not leak into the public properties surface.
///
/// This test is `#[ignore]`d because it requires a live Hive Metastore
/// container.
#[tokio::test]
#[ignore]
async fn spark_metadata_properties_survive_hms_round_trip() {
    let test_name = "spark_metadata_properties_survive_hms_round_trip";
    let context = setup_with_database(test_name).await;
    let catalog = &context.catalog;
    let namespace = &context.namespace;

    // Create a table with a non-Hive format so that inject_spark_metadata
    // writes spark.sql.sources.provider and spark.sql.sources.schema into
    // the HMS table parameters.
    let created = catalog
        .create_table(
            namespace,
            "spark_md_table",
            simple_table_options_with_format(
                test_name,
                vec![col("id", DataType::Int64), col("name", DataType::Utf8)],
                "parquet",
            ),
        )
        .await
        .unwrap();

    // Verify the created table reports the Spark provider format.
    match &created.kind {
        TableKind::Table { format, .. } => {
            assert_eq!(
                format, "parquet",
                "created table format should come from spark.sql.sources.provider"
            );
        }
        other => panic!("expected TableKind::Table, got {other:?}"),
    }

    // Re-read the table from HMS and verify Spark metadata survived through
    // the internal format and columns fields.
    let fetched = catalog
        .get_table(namespace, "spark_md_table")
        .await
        .unwrap();

    match &fetched.kind {
        TableKind::Table {
            format, columns, ..
        } => {
            // The format must be "parquet" — populated from the
            // spark.sql.sources.provider property that was written during
            // create_table and must survive the HMS round trip.
            assert_eq!(
                format, "parquet",
                "fetched table format should survive HMS round trip"
            );

            // Verify the schema was also reconstructed from Spark properties.
            let col_names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
            assert_eq!(
                col_names,
                &["id", "name"],
                "columns should be read from spark.sql.sources.schema"
            );

            let id_col = columns.iter().find(|c| c.name == "id").unwrap();
            assert_eq!(id_col.data_type, DataType::Int64);
            let name_col = columns.iter().find(|c| c.name == "name").unwrap();
            assert_eq!(name_col.data_type, DataType::Utf8);
        }
        other => panic!("expected TableKind::Table, got {other:?}"),
    }

    // Verify that spark.sql.* internal properties are correctly filtered from
    // the user-visible properties map.  The provider and schema are consumed
    // internally (populating `format` and `columns` above) and must not leak
    // into the public properties surface.
    let has_spark_sql_key = fetched
        .kind
        .properties()
        .iter()
        .any(|(k, _)| k.starts_with("spark.sql."));
    assert!(
        !has_spark_sql_key,
        "spark.sql.* properties must be filtered from user-visible properties. \
         Properties: {:?}",
        fetched.kind.properties()
    );
}
