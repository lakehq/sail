#![expect(
    clippy::expect_used,
    reason = "benchmark setup and operations must fail immediately on an invalid result"
)]

use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use futures::executor::block_on;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use sail_iceberg::table::find_latest_metadata_file;
use url::Url;

const SAMPLES: usize = 11;

fn selected(name: &str) -> bool {
    std::env::var("SAIL_BENCH_FILTER")
        .map(|filter| name.contains(&filter))
        .unwrap_or(true)
}

fn report(name: &str, iterations: usize, mut nanos: Vec<f64>) {
    nanos.sort_by(f64::total_cmp);
    println!(
        "{name}\titerations={iterations}\tmin_ns={:.1}\tmedian_ns={:.1}\tmax_ns={:.1}",
        nanos[0],
        nanos[SAMPLES / 2],
        nanos[SAMPLES - 1]
    );
}

fn measure(name: &str, iterations: usize, store: &Arc<dyn ObjectStore>, table_url: &Url) {
    if !selected(name) {
        return;
    }

    block_on(async {
        black_box(
            find_latest_metadata_file(store, table_url)
                .await
                .expect("warm metadata discovery"),
        );
    });

    let mut nanos = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let started = Instant::now();
        block_on(async {
            for _ in 0..iterations {
                black_box(
                    find_latest_metadata_file(store, table_url)
                        .await
                        .expect("discover latest metadata"),
                );
            }
        });
        nanos.push(started.elapsed().as_nanos() as f64 / iterations as f64);
    }
    report(name, iterations, nanos);
}

fn benchmark_store(metadata_files: usize, hint: Option<&str>) -> Arc<dyn ObjectStore> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    block_on(async {
        for version in 1..=metadata_files {
            let path = Path::from(format!(
                "events/metadata/{version:05}-benchmark.metadata.json"
            ));
            store
                .put(&path, PutPayload::from(Bytes::new()))
                .await
                .expect("seed metadata file");
        }

        store
            .put(
                &Path::from("events/metadata/ignored.parquet"),
                PutPayload::from(Bytes::new()),
            )
            .await
            .expect("seed non-metadata file");

        if let Some(hint) = hint {
            store
                .put(
                    &Path::from("events/metadata/version-hint.text"),
                    PutPayload::from(Bytes::copy_from_slice(hint.as_bytes())),
                )
                .await
                .expect("seed version hint");
        }
    });
    store
}

fn main() {
    let table_url = Url::parse("memory://warehouse/events").expect("table URL");

    for (metadata_files, iterations) in [(32, 2_000), (256, 500), (4_096, 20)] {
        let store = benchmark_store(metadata_files, None);
        measure(
            &format!("metadata_discovery_latest_{metadata_files}"),
            iterations,
            &store,
            &table_url,
        );

        let hinted_version = (metadata_files / 2).to_string();
        let store = benchmark_store(metadata_files, Some(&hinted_version));
        measure(
            &format!("metadata_discovery_numeric_hint_{metadata_files}"),
            iterations,
            &store,
            &table_url,
        );
    }
}
