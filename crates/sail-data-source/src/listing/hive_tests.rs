use std::ops::Not;
use std::sync::atomic::Ordering;

use datafusion::execution::cache::TableScopedPath;
use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::{cast, col, lit};
use datafusion::prelude::SessionContext;

use super::*;
use crate::listing::internal::tests::{Event, RecordingStore};

fn state(cache_size: usize) -> Result<SessionState> {
    let runtime = RuntimeEnvBuilder::new()
        .with_cache_manager(CacheManagerConfig::default().with_list_files_cache_limit(cache_size))
        .build_arc()?;
    Ok(SessionContext::new_with_config_rt(Default::default(), runtime).state())
}

fn columns() -> Vec<(String, DataType)> {
    vec![("p".into(), DataType::Int32), ("q".into(), DataType::Int32)]
}

async fn put(store: &RecordingStore, path: &str) -> Result<()> {
    // These are literal object keys: Path::from would escape '%' in Hive aliases again.
    store
        .put(
            &Path::parse(path)?,
            bytes::Bytes::from_static(b"data").into(),
        )
        .await?;
    Ok(())
}

fn listing(store: Arc<RecordingStore>, url: &ListingTableUrl) -> TableListingStore {
    TableListingStore::new(
        store,
        url.prefix().clone(),
        ListingPolicy::from_url(url.get_url()),
    )
}

#[tokio::test]
async fn explicit_partition_nullability_does_not_change_physical_file_fields() -> Result<()> {
    use datafusion::arrow::array::Int32Array;
    use datafusion::parquet::arrow::ArrowWriter;
    use sail_common_datafusion::datasource::{DataSource, SourceInfo};

    use crate::formats::parquet::ParquetDataSource;
    use crate::listing::table::ListingTableSource;

    let fields = vec![
        Field::new("rid", DataType::Int32, false),
        Field::new("p", DataType::Int32, false),
        Field::new("q", DataType::Int32, false),
    ];
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![fields[0].clone()])),
        vec![Arc::new(Int32Array::from(vec![7]))],
    )?;
    let mut writer = ArrowWriter::try_new(Vec::new(), batch.schema(), None)?;
    writer.write(&batch)?;
    let store = Arc::new(RecordingStore::default());
    store
        .put(
            &Path::parse("table/p=1/q=2/file.parquet")?,
            bytes::Bytes::from(writer.into_inner()?).into(),
        )
        .await?;
    let url = ListingTableUrl::parse("memory://bucket/table/")?;
    let state = state(0)?;
    state
        .runtime_env()
        .register_object_store(url.get_url(), store);
    for validate in [true, false] {
        let source = ParquetDataSource::default()
            .create_source(
                &state,
                SourceInfo {
                    paths: vec![url.to_string()],
                    lakehouse_table: None,
                    schema: Some(Schema::new(fields.clone())),
                    constraints: Default::default(),
                    partition_by: vec!["p".into(), "q".into()],
                    bucket_by: None,
                    sort_order: vec![],
                    options: vec![],
                    read_case_sensitive: true,
                    validate_partition_columns: validate,
                },
            )
            .await?;
        let source = source.downcast_ref::<ListingTableSource>().unwrap();
        assert_eq!(
            source.config().schema.file_schema().as_ref(),
            &Schema::new(vec![fields[0].clone()])
        );
        assert!(
            source
                .config()
                .schema
                .table_partition_cols()
                .iter()
                .all(|field| field.is_nullable())
        );
        assert_eq!(source.config().validate_partition_columns, validate);
    }
    Ok(())
}

#[tokio::test]
async fn selective_listing_keeps_raw_aliases_and_validates_excluded_directory_levels() -> Result<()>
{
    for address in [
        "memory://bucket/table/",
        "file:///table/",
        "hdfs://namenode/table/",
    ] {
        let store = Arc::new(RecordingStore::default());
        let selected = [
            "table/p=1/q=2/a",
            "table/p=1/%71=2/b",
            "table/%70=1/q=%32/c",
            "table/p=%31/%71=%32/d",
        ];
        for path in selected {
            put(&store, path).await?;
        }
        put(&store, "table/p=1/q=4/excluded").await?;
        for i in 0..50 {
            put(&store, &format!("table/p=9/q=3/{i}")).await?;
        }
        // Reserved data must not cause either casting or recursive enumeration.
        put(&store, "table/.sail-staging/p=invalid/q=bad/file").await?;
        // Match the ordinary directory URL produced by resolve_listing_urls.
        let url = ListingTableUrl::parse(address)?.with_glob("[!._]*")?;
        for path in selected {
            let meta = store.head(&Path::parse(path)?).await?;
            assert_eq!(meta.location.as_ref(), path);
            for ignore_subdirectory in [true, false] {
                assert!(url.contains(&meta.location, ignore_subdirectory));
            }
            assert!(!has_hidden_path_component(&url, &meta.location));
        }
        let listing = listing(store.clone(), &url);
        let state = state(0)?;
        let filters = [col("p").eq(lit(1_i32)).and(col("q").eq(lit(2_i32)))];
        let columns = columns();
        let files = hive_partitioned_files(&state, &listing, &url, &filters, &columns, true, None)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let mut actual = files
            .iter()
            .map(|file| file.object_meta.location.to_string())
            .collect::<Vec<_>>();
        actual.sort();
        let mut expected = selected.map(str::to_string).to_vec();
        expected.sort();
        assert_eq!(actual, expected);
        let events = store.events.lock().unwrap();
        assert!(events.contains(&Event::Delimiter(Path::from("table/p=9"))));
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e, Event::Delimiter(_)))
                .count(),
            5
        );
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e, Event::List(_)))
                .count(),
            4
        );
        assert!(!events.iter().any(|e| matches!(e, Event::List(p) if p.as_ref() == "table" || p.as_ref().starts_with("table/p=9"))));
        assert!(!events.iter().any(|e| matches!(e, Event::List(p) | Event::Delimiter(p) if p.as_ref().contains(".sail-staging"))));
        assert_eq!(store.yielded.load(Ordering::SeqCst), 4);
    }
    Ok(())
}

#[tokio::test]
async fn malformed_excluded_keys_fall_back_and_validate_before_any_consumption() -> Result<()> {
    for filters in [vec![], vec![col("p").eq(lit(1_i32))]] {
        let store = Arc::new(RecordingStore::default());
        put(&store, "table/p=1/q=2/first").await?;
        put(&store, "table/p=9/q=invalid/last").await?;
        let url = ListingTableUrl::parse("memory://bucket/table/")?;
        let listing = listing(store.clone(), &url);
        let state = state(0)?;
        let columns = columns();
        // An error at construction, before a caller can take(1) or gather enough statistics.
        assert!(
            hive_partitioned_files(&state, &listing, &url, &filters, &columns, true, None)
                .await
                .is_err()
        );
        assert!(
            store
                .events
                .lock()
                .unwrap()
                .contains(&Event::List(Path::from("table")))
        );
        let files = hive_partitioned_files(&state, &listing, &url, &filters, &columns, false, None)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        if filters.is_empty() {
            assert_eq!(files.len(), 2);
            assert!(files.iter().any(|f| f.partition_values
                == vec![ScalarValue::Int32(Some(9)), ScalarValue::Int32(None)]));
        } else {
            assert_eq!(files.len(), 1);
        }
    }
    Ok(())
}

#[tokio::test]
async fn flat_listing_has_no_unconditional_delimiter_fanout() -> Result<()> {
    for (filters, probes, rows) in [
        (vec![], 0, 2),
        (vec![col("q").eq(lit(2_i32))], 0, 1),
        (
            vec![col("p").eq(lit(1_i32)).or(col("q").eq(lit(3_i32)))],
            0,
            2,
        ),
        (
            vec![col("p").eq(lit(1_i32)).and(col("q").eq(lit(2_i32))).not()],
            0,
            1,
        ),
        (vec![col("p").gt(lit(0_i32))], 1, 2),
    ] {
        let store = Arc::new(RecordingStore::default());
        put(&store, "table/p=1/q=2/a").await?;
        put(&store, "table/p=9/q=3/b").await?;
        let url = ListingTableUrl::parse("memory://bucket/table/")?;
        let listing = listing(store.clone(), &url);
        let state = state(0)?;
        let columns = columns();
        let files = hive_partitioned_files(&state, &listing, &url, &filters, &columns, true, None)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(files.len(), rows);
        let events = store.events.lock().unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e, Event::Delimiter(_)))
                .count(),
            probes
        );
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e, Event::List(_)))
                .count(),
            1
        );
        assert!(events.contains(&Event::List(Path::from("table"))));
        assert_eq!(store.yielded.load(Ordering::SeqCst), 2);
    }
    Ok(())
}

#[tokio::test]
async fn list_only_backend_falls_back_but_permission_errors_propagate() -> Result<()> {
    for denied in [
        None,
        Some(Path::from("table")),
        Some(Path::from("table/p=9")),
    ] {
        let store = Arc::new(RecordingStore {
            delimiter_not_implemented: denied.is_none(),
            denied_listing: denied.clone(),
            ..Default::default()
        });
        put(&store, "table/p=1/q=2/a").await?;
        put(&store, "table/p=9/q=3/b").await?;
        let url = ListingTableUrl::parse("hf://datasets/table/")?;
        let listing = listing(store.clone(), &url);
        let state = state(0)?;
        let filters = [col("p").eq(lit(1_i32))];
        let columns = columns();
        for strict in [true, false] {
            let files =
                hive_partitioned_files(&state, &listing, &url, &filters, &columns, strict, None)
                    .await;
            if denied.is_some() {
                assert!(files.is_err());
                assert!(
                    !store
                        .events
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|e| matches!(e, Event::List(_)))
                );
            } else {
                assert_eq!(files?.try_collect::<Vec<_>>().await?.len(), 1);
                assert!(
                    store
                        .events
                        .lock()
                        .unwrap()
                        .contains(&Event::List(Path::from("table")))
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn cached_listing_is_complete_on_cold_and_warm_selective_reads() -> Result<()> {
    let store = Arc::new(RecordingStore::default());
    put(&store, "table/p=1/q=2/a").await?;
    put(&store, "table/p=9/q=3/b").await?;
    put(&store, "table/.sail-staging/p=bad/q=bad/file").await?;
    let url = ListingTableUrl::parse("memory://bucket/table/")?;
    let listing = listing(store.clone(), &url);
    let state = state(1024 * 1024)?;
    let columns = columns();
    for p in [1_i32, 9, 1] {
        let filters = [col("p").eq(lit(p))];
        let files = hive_partitioned_files(&state, &listing, &url, &filters, &columns, true, None)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].partition_values[0], ScalarValue::Int32(Some(p)));
    }
    assert_eq!(
        *store.events.lock().unwrap(),
        vec![Event::List(Path::from("table"))]
    );
    let cache = state
        .runtime_env()
        .cache_manager
        .get_list_files_cache()
        .unwrap();
    let cached = cache
        .get(&TableScopedPath {
            table: None,
            path: url.prefix().clone(),
        })
        .unwrap();
    assert_eq!(cached.files.len(), 2);
    Ok(())
}

#[tokio::test]
async fn irregular_and_hidden_only_keys_preserve_file_membership_on_fallback() -> Result<()> {
    for (extra, size) in [
        ("table/p=9/q=invalid/_hidden", 4),
        ("table/p=9/q=invalid/zero", 0),
        ("table/p=9/incomplete", 4),
        ("table/irregular/file", 4),
    ] {
        let store = Arc::new(RecordingStore::default());
        put(&store, "table/p=1/q=2/a").await?;
        store
            .put(&Path::from(extra), bytes::Bytes::from(vec![0; size]).into())
            .await?;
        let url = ListingTableUrl::parse("memory://bucket/table/")?;
        let listing = listing(store.clone(), &url);
        let state = state(0)?;
        let filters = [col("p").eq(lit(1_i32))];
        let columns = columns();
        let files = hive_partitioned_files(&state, &listing, &url, &filters, &columns, true, None)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(files.len(), 1);
        assert!(
            store
                .events
                .lock()
                .unwrap()
                .contains(&Event::List(Path::from("table")))
        );
    }
    Ok(())
}

#[tokio::test]
async fn url_and_filename_globs_and_full_predicate_errors_are_not_relaxed() -> Result<()> {
    let store = Arc::new(RecordingStore::default());
    put(&store, "table/p=1/q=2/keep.parquet").await?;
    put(&store, "table/p=1/q=3/drop.parquet").await?;
    put(&store, "table/p=9/q=4/keep.parquet").await?;
    let url = ListingTableUrl::parse("memory://bucket/table/")?.with_glob("keep.*")?;
    let listing = listing(store.clone(), &url);
    let state = state(0)?;
    let filters = [col("p").eq(lit(1_i32))];
    let columns = columns();
    let files = hive_partitioned_files(&state, &listing, &url, &filters, &columns, true, None)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(files.len(), 1);
    assert_eq!(
        *store.events.lock().unwrap(),
        vec![Event::List(Path::from("table"))]
    );
    let url = ListingTableUrl::parse("memory://bucket/table/")?;
    let glob = PathGlobFilter::parse("keep.*")?;
    let files = hive_partitioned_files(
        &state,
        &listing,
        &url,
        &filters,
        &columns,
        true,
        Some(&glob),
    )
    .await?
    .try_collect::<Vec<_>>()
    .await?;
    assert_eq!(files.len(), 1);

    put(&store, "table/p=1/q=invalid/keep.parquet").await?;
    let columns = vec![("p".into(), DataType::Int32), ("q".into(), DataType::Utf8)];
    let cast_filter = cast(col("q"), DataType::Int32).eq(lit(2_i32));
    for filter in [
        col("p").eq(lit(1_i32)).and(cast_filter.clone()),
        cast_filter,
    ] {
        let filters = [filter];
        let result =
            hive_partitioned_files(&state, &listing, &url, &filters, &columns, false, None).await;
        let result = match result {
            Ok(files) => files.try_collect::<Vec<_>>().await,
            Err(error) => Err(error),
        };
        assert!(result.is_err());
    }
    Ok(())
}

#[tokio::test]
async fn speculative_directory_predicate_errors_use_file_membership() -> Result<()> {
    for suffix in [false, true] {
        for member in ["_hidden", "zero", "data"] {
            for strict in [true, false] {
                let store = Arc::new(RecordingStore::default());
                put(&store, "table/p=1/q=2/keep").await?;
                put(&store, "table/p=9/q=3/excluded").await?;
                let directory = if suffix { "p=1/q=bad" } else { "p=bad/q=3" };
                let path = Path::parse(format!("table/{directory}/{member}"))?;
                let bytes = if member == "zero" {
                    bytes::Bytes::new()
                } else {
                    bytes::Bytes::from_static(b"data")
                };
                store.put(&path, bytes.into()).await?;
                let columns = if suffix {
                    vec![("p".into(), DataType::Int32), ("q".into(), DataType::Utf8)]
                } else {
                    vec![("p".into(), DataType::Utf8), ("q".into(), DataType::Int32)]
                };
                let filters = if suffix {
                    vec![
                        col("p")
                            .eq(lit(1_i32))
                            .and(cast(col("q"), DataType::Int32).eq(lit(2_i32))),
                    ]
                } else {
                    vec![cast(col("p"), DataType::Int32).eq(lit(1_i32))]
                };
                let state = state(0)?;
                let url = ListingTableUrl::parse("memory://bucket/table/")?;
                let listing = listing(store.clone(), &url);
                let result = hive_partitioned_files(
                    &state, &listing, &url, &filters, &columns, strict, None,
                )
                .await;
                let result = match result {
                    Ok(files) => files.try_collect::<Vec<_>>().await,
                    Err(error) => Err(error),
                };
                if member == "data" {
                    assert!(result.is_err());
                } else {
                    let files = result?;
                    assert_eq!(files.len(), 1);
                    assert_eq!(
                        files[0].object_meta.location,
                        Path::from("table/p=1/q=2/keep")
                    );
                }
                let events = store.events.lock().unwrap();
                assert_eq!(
                    events
                        .iter()
                        .filter(|event| matches!(event, Event::List(_)))
                        .collect::<Vec<_>>(),
                    vec![&Event::List(Path::from("table"))]
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn filename_glob_does_not_suppress_partition_validation_or_predicate_errors() -> Result<()> {
    let store = Arc::new(RecordingStore::default());
    put(&store, "table/p=1/q=2/keep.parquet").await?;
    put(&store, "table/p=9/q=invalid/drop.parquet").await?;
    let url = ListingTableUrl::parse("memory://bucket/table/")?;
    let listing = listing(store, &url);
    let state = state(0)?;
    let glob = PathGlobFilter::parse("keep.parquet")?;
    let columns = columns();
    for filters in [vec![], vec![col("p").eq(lit(1_i32))]] {
        assert!(
            hive_partitioned_files(
                &state,
                &listing,
                &url,
                &filters,
                &columns,
                true,
                Some(&glob)
            )
            .await
            .is_err()
        );
        let files = hive_partitioned_files(
            &state,
            &listing,
            &url,
            &filters,
            &columns,
            false,
            Some(&glob),
        )
        .await?
        .try_collect::<Vec<_>>()
        .await?;
        assert_eq!(files.len(), 1);
        assert_eq!(
            files[0].partition_values,
            vec![ScalarValue::Int32(Some(1)), ScalarValue::Int32(Some(2))]
        );
    }
    let columns = vec![("p".into(), DataType::Int32), ("q".into(), DataType::Utf8)];
    let filters = [cast(col("q"), DataType::Int32).eq(lit(2_i32))];
    for strict in [true, false] {
        let result = hive_partitioned_files(
            &state,
            &listing,
            &url,
            &filters,
            &columns,
            strict,
            Some(&glob),
        )
        .await;
        let result = match result {
            Ok(files) => files.try_collect::<Vec<_>>().await,
            Err(error) => Err(error),
        };
        assert!(result.is_err());
    }
    Ok(())
}

#[tokio::test]
async fn flat_listing_budget_includes_selected_terminal_requests() -> Result<()> {
    for selected in [MAX_HIVE_DISCOVERY_REQUESTS - 1, MAX_HIVE_DISCOVERY_REQUESTS] {
        let store = Arc::new(RecordingStore::default());
        for p in 0..=selected {
            put(&store, &format!("table/p={p}/file")).await?;
        }
        let state = state(0)?;
        let url = ListingTableUrl::parse("memory://bucket/table/")?;
        let listing = listing(store.clone(), &url);
        let columns = [("p".into(), DataType::Int32)];
        let filters = [col("p").gt(lit(0_i32))];
        let files = hive_partitioned_files(&state, &listing, &url, &filters, &columns, true, None)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(files.len(), selected);
        let events = store.events.lock().unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, Event::Delimiter(_)))
                .count(),
            1
        );
        if selected < MAX_HIVE_DISCOVERY_REQUESTS {
            assert_eq!(events.len(), MAX_HIVE_DISCOVERY_REQUESTS);
            assert!(!events.contains(&Event::List(Path::from("table"))));
            assert_eq!(store.yielded.load(Ordering::SeqCst), selected);
        } else {
            assert_eq!(
                *events,
                vec![
                    Event::Delimiter(Path::from("table")),
                    Event::List(Path::from("table"))
                ]
            );
            assert_eq!(store.yielded.load(Ordering::SeqCst), selected + 1);
        }
    }
    Ok(())
}

#[tokio::test]
async fn flat_listing_budget_reserves_pending_directories_and_terminal_lists() -> Result<()> {
    use std::collections::HashSet;

    for layout in ["single-level", "many-roots", "many-deep-children"] {
        let store = Arc::new(RecordingStore::default());
        let mut expected = HashSet::new();
        for i in 0..1024_i32 {
            let (path, key) = match layout {
                "single-level" => (format!("table/p={i}/file"), vec![i]),
                "many-roots" => (format!("table/p={i}/q=1/file"), vec![i, 1]),
                _ => (format!("table/p=1/q={i}/r=1/file"), vec![1, i, 1]),
            };
            put(&store, &path).await?;
            if key[0] > 0 {
                expected.insert(
                    key.into_iter()
                        .map(|value| ScalarValue::Int32(Some(value)))
                        .collect::<Vec<_>>(),
                );
            }
        }
        if layout == "many-deep-children" {
            put(&store, "table/p=0/q=0/r=1/file").await?;
        }
        let columns = match layout {
            "single-level" => vec![("p".into(), DataType::Int32)],
            "many-roots" => columns(),
            _ => vec![
                ("p".into(), DataType::Int32),
                ("q".into(), DataType::Int32),
                ("r".into(), DataType::Int32),
            ],
        };
        let state = state(0)?;
        let url = ListingTableUrl::parse("memory://bucket/table/")?;
        let listing = listing(store.clone(), &url);
        let filters = [col("p").gt(lit(0_i32))];
        let files = hive_partitioned_files(&state, &listing, &url, &filters, &columns, true, None)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(files.len(), expected.len());
        assert_eq!(
            files
                .into_iter()
                .map(|file| file.partition_values)
                .collect::<HashSet<_>>(),
            expected
        );
        let events = store.events.lock().unwrap();
        let probes = if layout == "many-deep-children" { 2 } else { 1 };
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, Event::Delimiter(_)))
                .count(),
            probes
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, Event::List(_)))
                .collect::<Vec<_>>(),
            vec![&Event::List(Path::from("table"))]
        );
        assert!(events.len() <= MAX_HIVE_DISCOVERY_REQUESTS + 1);
        let objects = if layout == "many-deep-children" {
            1025
        } else {
            1024
        };
        assert_eq!(store.yielded.load(Ordering::SeqCst), objects);
    }
    Ok(())
}
