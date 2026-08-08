#![expect(
    clippy::expect_used,
    reason = "benchmark fixtures must fail immediately when their schema is invalid"
)]

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use sail_iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, FormatVersion, Manifest, ManifestContentType,
    ManifestMetadata, ManifestWriterBuilder, NestedField, PartitionSpec, PrimitiveType, Schema,
    Type,
};

const SAMPLES: usize = 9;

fn selected(name: &str) -> bool {
    std::env::var("SAIL_BENCH_FILTER")
        .map(|filter| name.contains(&filter))
        .unwrap_or(true)
}

fn measure(name: &str, iterations: usize, mut run: impl FnMut()) {
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
    nanos.sort_by(f64::total_cmp);
    println!(
        "{name}\titerations={iterations}\tmin_ns={:.1}\tmedian_ns={:.1}\tmax_ns={:.1}",
        nanos[0],
        nanos[SAMPLES / 2],
        nanos[SAMPLES - 1]
    );
}

fn data_file(index: usize) -> DataFile {
    DataFile {
        content: DataContentType::Data,
        file_path: format!("s3://warehouse/events/data/{index:08}.parquet"),
        file_format: DataFileFormat::Parquet,
        partition: Vec::new(),
        record_count: 10_000,
        file_size_in_bytes: 1_048_576,
        column_sizes: HashMap::from([(1, 800_000)]),
        value_counts: HashMap::from([(1, 10_000)]),
        null_value_counts: HashMap::from([(1, 0)]),
        nan_value_counts: HashMap::new(),
        lower_bounds: HashMap::new(),
        upper_bounds: HashMap::new(),
        block_size_in_bytes: None,
        key_metadata: None,
        split_offsets: vec![0, 524_288],
        equality_ids: Vec::new(),
        sort_order_id: None,
        first_row_id: None,
        partition_spec_id: 0,
        referenced_data_file: None,
        content_offset: None,
        content_size_in_bytes: None,
    }
}

fn manifest_bytes(entry_count: usize) -> Vec<u8> {
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![Arc::new(NestedField::new(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
            true,
        ))])
        .build()
        .expect("valid benchmark schema");
    let metadata = ManifestMetadata::new(
        Arc::new(schema),
        0,
        PartitionSpec::builder().with_spec_id(0).build(),
        FormatVersion::V2,
        ManifestContentType::Data,
    );
    let mut writer = ManifestWriterBuilder::new(Some(42), None, metadata).build();
    for index in 0..entry_count {
        writer.add(data_file(index));
    }
    writer
        .to_avro_bytes_v2()
        .expect("serialize benchmark manifest")
}

fn main() {
    for (entries, iterations) in [(1, 1_000), (100, 40), (1_000, 5)] {
        let bytes = manifest_bytes(entries);
        measure(&format!("manifest_parse/{entries}"), iterations, || {
            black_box(Manifest::parse_avro(black_box(bytes.as_slice())))
                .expect("parse benchmark manifest");
        });
    }
}
