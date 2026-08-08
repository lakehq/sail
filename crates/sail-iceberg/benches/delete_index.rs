#![expect(
    clippy::expect_used,
    reason = "benchmark fixtures must fail immediately when their index is invalid"
)]

use std::collections::HashMap;
use std::hint::black_box;
use std::time::Instant;

use sail_iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, DeleteFileIndex, DeleteFileRef, Literal,
    PrimitiveLiteral,
};

const SAMPLES: usize = 11;
const DATA_PATH: &str = "s3://warehouse/events/data/00000001.parquet";

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

fn partition() -> Vec<Option<Literal>> {
    vec![
        Some(Literal::Primitive(PrimitiveLiteral::String(
            "us-west".to_string(),
        ))),
        Some(Literal::Primitive(PrimitiveLiteral::Int(2026))),
    ]
}

fn data_file(content: DataContentType, path: String) -> DataFile {
    DataFile {
        content,
        file_path: path,
        file_format: DataFileFormat::Parquet,
        partition: partition(),
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
        equality_ids: vec![1],
        sort_order_id: None,
        first_row_id: None,
        partition_spec_id: 7,
        referenced_data_file: None,
        content_offset: None,
        content_size_in_bytes: None,
    }
}

fn delete_ref(
    index: usize,
    content: DataContentType,
    referenced_data_file: Option<String>,
    is_unpartitioned_spec: bool,
) -> DeleteFileRef {
    let mut file = data_file(
        content,
        format!("s3://warehouse/events/delete/{index:08}.parquet"),
    );
    file.referenced_data_file = referenced_data_file;
    DeleteFileRef {
        data_file: file,
        data_sequence_number: 20,
        partition_spec_id: 7,
        is_unpartitioned_spec,
    }
}

fn index_with(
    path_deletes: usize,
    global_deletes: usize,
    partition_position_deletes: usize,
    partition_equality_deletes: usize,
) -> DeleteFileIndex {
    let mut index = DeleteFileIndex::new();
    for offset in 0..path_deletes {
        index
            .insert(delete_ref(
                offset,
                DataContentType::PositionDeletes,
                Some(DATA_PATH.to_string()),
                false,
            ))
            .expect("valid path delete");
    }
    for offset in 0..global_deletes {
        index
            .insert(delete_ref(
                100 + offset,
                DataContentType::EqualityDeletes,
                None,
                true,
            ))
            .expect("valid global equality delete");
    }
    for offset in 0..partition_position_deletes {
        index
            .insert(delete_ref(
                200 + offset,
                DataContentType::PositionDeletes,
                None,
                false,
            ))
            .expect("valid partition position delete");
    }
    for offset in 0..partition_equality_deletes {
        index
            .insert(delete_ref(
                300 + offset,
                DataContentType::EqualityDeletes,
                None,
                false,
            ))
            .expect("valid partition equality delete");
    }
    index
}

fn benchmark(name: &str, iterations: usize, index: &DeleteFileIndex, data_file: &DataFile) {
    measure(name, iterations, || {
        black_box(index.for_data_file(black_box(data_file), black_box(10)));
    });
}

fn main() {
    let data = data_file(DataContentType::Data, DATA_PATH.to_string());
    benchmark(
        "delete_index/empty",
        200_000,
        &DeleteFileIndex::new(),
        &data,
    );
    benchmark(
        "delete_index/path_8",
        20_000,
        &index_with(8, 0, 0, 0),
        &data,
    );
    benchmark(
        "delete_index/global_equality_8",
        20_000,
        &index_with(0, 8, 0, 0),
        &data,
    );
    benchmark(
        "delete_index/partition_8",
        20_000,
        &index_with(0, 0, 4, 4),
        &data,
    );
    benchmark(
        "delete_index/mixed_32",
        10_000,
        &index_with(8, 8, 8, 8),
        &data,
    );
}
