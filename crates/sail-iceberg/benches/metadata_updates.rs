#![expect(
    clippy::expect_used,
    reason = "benchmark setup and operations must fail immediately on an invalid result"
)]

use std::hint::black_box;
use std::time::Instant;

use sail_iceberg::spec::{TableMetadata, TableUpdate};
use serde_json::{Value, json};

const MIB: usize = 1024 * 1024;
const SAMPLES: usize = 11;

fn selected(name: &str) -> bool {
    std::env::var("SAIL_BENCH_FILTER")
        .map(|filter| name.contains(&filter))
        .unwrap_or(true)
}

fn report(name: &str, iterations: usize, bytes_per_iteration: usize, mut nanos: Vec<f64>) {
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

fn measure(name: &str, iterations: usize, bytes_per_iteration: usize, mut run: impl FnMut()) {
    if !selected(name) {
        return;
    }
    for _ in 0..(iterations / 10).max(1) {
        run();
    }

    let mut nanos = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let started = Instant::now();
        for _ in 0..iterations {
            run();
        }
        nanos.push(started.elapsed().as_nanos() as f64 / iterations as f64);
    }
    report(name, iterations, bytes_per_iteration, nanos);
}

fn snapshot_value(snapshot_id: i64) -> Value {
    json!({
        "snapshot-id": snapshot_id,
        "parent-snapshot-id": (snapshot_id > 1).then_some(snapshot_id - 1),
        "sequence-number": snapshot_id,
        "timestamp-ms": 1_700_000_000_000_i64 + snapshot_id,
        "manifest-list": format!("s3://warehouse/events/metadata/snap-{snapshot_id}.avro"),
        "summary": {
            "operation": "append",
            "added-data-files": "1",
            "added-records": "1000"
        },
        "schema-id": 0
    })
}

fn metadata_json(snapshot_count: usize) -> Vec<u8> {
    let snapshots = (1..=snapshot_count)
        .map(|id| snapshot_value(id as i64))
        .collect::<Vec<_>>();
    let snapshot_log = (1..=snapshot_count)
        .map(|id| {
            json!({
                "timestamp-ms": 1_700_000_000_000_i64 + id as i64,
                "snapshot-id": id
            })
        })
        .collect::<Vec<_>>();
    let current_snapshot = (snapshot_count > 0).then_some(snapshot_count as i64);
    let refs = current_snapshot.map_or_else(
        || json!({}),
        |snapshot_id| {
            json!({
                "main": {"snapshot-id": snapshot_id, "type": "branch"}
            })
        },
    );
    let properties = (0..32)
        .map(|index| {
            (
                format!("benchmark.property.{index}"),
                format!("value-{index}"),
            )
        })
        .collect::<std::collections::HashMap<_, _>>();

    serde_json::to_vec(&json!({
        "format-version": 2,
        "table-uuid": "00000000-0000-0000-0000-000000000001",
        "location": "s3://warehouse/events",
        "last-sequence-number": snapshot_count,
        "last-updated-ms": 1_700_000_000_000_i64,
        "last-column-id": 4,
        "current-schema-id": 0,
        "schemas": [{
            "type": "struct",
            "schema-id": 0,
            "fields": [
                {"id": 1, "name": "event_id", "required": true, "type": "long"},
                {"id": 2, "name": "category", "required": false, "type": "string"},
                {"id": 3, "name": "event_time", "required": true, "type": "timestamptz"},
                {"id": 4, "name": "payload", "required": false, "type": "string"}
            ]
        }],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "last-partition-id": 999,
        "default-sort-order-id": 0,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "properties": properties,
        "current-snapshot-id": current_snapshot,
        "snapshots": snapshots,
        "snapshot-log": snapshot_log,
        "metadata-log": [],
        "refs": refs
    }))
    .expect("serialize benchmark metadata")
}

fn append_updates(snapshot_id: i64) -> Vec<TableUpdate> {
    serde_json::from_value(json!([
        {
            "action": "add-snapshot",
            "snapshot": snapshot_value(snapshot_id)
        },
        {
            "action": "set-snapshot-ref",
            "ref-name": "main",
            "snapshot-id": snapshot_id,
            "type": "branch"
        }
    ]))
    .expect("deserialize benchmark updates")
}

fn main() {
    let update_json = serde_json::to_value(append_updates(1)).expect("serialize updates");
    measure("updates_deserialize_append", 20_000, 0, || {
        black_box(
            serde_json::from_value::<Vec<TableUpdate>>(black_box(update_json.clone()))
                .expect("deserialize updates"),
        );
    });

    for (snapshot_count, iterations) in [(0, 5_000), (100, 500), (1_000, 50)] {
        let bytes = metadata_json(snapshot_count);
        let metadata = TableMetadata::from_json(&bytes).expect("parse benchmark metadata");
        measure(
            &format!("metadata_current_snapshot_{snapshot_count}"),
            100_000,
            0,
            || {
                black_box(black_box(&metadata).current_snapshot());
            },
        );
        measure(
            &format!("metadata_parse_{snapshot_count}"),
            iterations,
            bytes.len(),
            || {
                black_box(
                    TableMetadata::from_json(black_box(&bytes)).expect("parse table metadata"),
                );
            },
        );
        measure(
            &format!("metadata_serialize_{snapshot_count}"),
            iterations,
            bytes.len(),
            || {
                black_box(serde_json::to_value(black_box(&metadata)).expect("serialize metadata"));
            },
        );
        measure(
            &format!("metadata_roundtrip_{snapshot_count}"),
            iterations,
            bytes.len(),
            || {
                let input =
                    TableMetadata::from_json(black_box(&bytes)).expect("parse table metadata");
                black_box(serde_json::to_value(&input).expect("serialize updated metadata"));
            },
        );
    }
}
