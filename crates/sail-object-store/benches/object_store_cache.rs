use std::future::Future;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use futures::future::try_join_all;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use sail_object_store::{CacheConfig, CachingObjectStore};

const MIB: usize = 1024 * 1024;
const OBJECT_SIZE: usize = 16 * MIB;
const PAGE_SIZE: usize = MIB;
const SAMPLES: usize = 11;

fn pattern(len: usize) -> Bytes {
    Bytes::from(
        (0..len)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>(),
    )
}

fn selected(name: &str) -> bool {
    std::env::var("SAIL_BENCH_FILTER")
        .map(|filter| name.contains(&filter))
        .unwrap_or(true)
}

fn measure<F, Fut>(name: &str, iterations: usize, bytes_per_iteration: usize, mut run: F)
where
    F: FnMut(usize) -> Fut,
    Fut: Future<Output = ()>,
{
    if !selected(name) {
        return;
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark runtime");
    runtime.block_on(run((iterations / 10).max(1)));

    let mut nanos = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let started = Instant::now();
        runtime.block_on(run(iterations));
        nanos.push(started.elapsed().as_nanos() as f64 / iterations as f64);
    }
    nanos.sort_by(f64::total_cmp);
    let median = nanos[SAMPLES / 2];
    let minimum = nanos[0];
    let maximum = nanos[SAMPLES - 1];
    let throughput_mib = if bytes_per_iteration == 0 {
        0.0
    } else {
        bytes_per_iteration as f64 * 1_000_000_000.0 / median / MIB as f64
    };
    println!(
        "{name}\titerations={iterations}\tmin_ns={minimum:.1}\tmedian_ns={median:.1}\tmax_ns={maximum:.1}\tthroughput_mib_s={throughput_mib:.1}"
    );
}

fn main() {
    let setup_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("setup runtime");
    let path = Path::from("benchmark/data.bin");
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    setup_runtime.block_on(async {
        inner
            .put(&path, PutPayload::from(pattern(OBJECT_SIZE)))
            .await
            .expect("seed object");
    });
    let cache = CachingObjectStore::new(
        inner.clone(),
        CacheConfig {
            page_size: PAGE_SIZE,
            memory_capacity_bytes: 32 * MIB,
            metadata_capacity_bytes: MIB,
            parallelism: 8,
        },
    );
    setup_runtime.block_on(async {
        let result = cache.get(&path).await.expect("prime object cache");
        black_box(result.bytes().await.expect("read primed object"));
    });

    measure("raw_head", 20_000, 0, |iterations| {
        let inner = &inner;
        let path = &path;
        async move {
            for _ in 0..iterations {
                black_box(inner.head(path).await.expect("raw head"));
            }
        }
    });
    measure("cached_head", 20_000, 0, |iterations| {
        let cache = &cache;
        let path = &path;
        async move {
            for _ in 0..iterations {
                black_box(cache.head(path).await.expect("cached head"));
            }
        }
    });
    measure("raw_range_4k", 10_000, 4096, |iterations| {
        let inner = &inner;
        let path = &path;
        async move {
            for _ in 0..iterations {
                black_box(
                    inner
                        .get_range(path, 65_536..69_632)
                        .await
                        .expect("raw range"),
                );
            }
        }
    });
    measure("cached_range_4k", 10_000, 4096, |iterations| {
        let cache = &cache;
        let path = &path;
        async move {
            for _ in 0..iterations {
                black_box(
                    cache
                        .get_range(path, 65_536..69_632)
                        .await
                        .expect("cached range"),
                );
            }
        }
    });
    measure("cached_range_unaligned_2m", 128, 2 * MIB, |iterations| {
        let cache = &cache;
        let path = &path;
        async move {
            for _ in 0..iterations {
                black_box(
                    cache
                        .get_range(path, (512 * 1024) as u64..(2 * MIB + 512 * 1024) as u64)
                        .await
                        .expect("cached multi-page range"),
                );
            }
        }
    });

    let ranges = (0..32)
        .map(|index| {
            let start = index * 512 * 1024 + 4096;
            start as u64..(start + 4096) as u64
        })
        .collect::<Vec<_>>();
    measure("cached_get_ranges_32x4k", 256, 32 * 4096, |iterations| {
        let cache = &cache;
        let path = &path;
        let ranges = &ranges;
        async move {
            for _ in 0..iterations {
                black_box(
                    cache
                        .get_ranges(path, ranges)
                        .await
                        .expect("cached range batch"),
                );
            }
        }
    });
    measure("cached_concurrent_16x4k", 512, 16 * 4096, |iterations| {
        let cache = &cache;
        let path = &path;
        async move {
            for _ in 0..iterations {
                let reads = (0..16).map(|index| {
                    let start = index * MIB + 8192;
                    cache.get_range(path, start as u64..(start + 4096) as u64)
                });
                black_box(try_join_all(reads).await.expect("concurrent cache hits"));
            }
        }
    });
    measure("cached_full_stream_16m", 16, OBJECT_SIZE, |iterations| {
        let cache = &cache;
        let path = &path;
        async move {
            for _ in 0..iterations {
                let result = cache.get(path).await.expect("cached full get");
                black_box(result.bytes().await.expect("cached full bytes"));
            }
        }
    });
}
