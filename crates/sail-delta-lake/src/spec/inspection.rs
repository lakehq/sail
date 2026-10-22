//! Delta Spark inspection and maintenance command schemas.
//!
//! TODO: Consider Sail-only `SELECT ... FROM <table>.detail` / `.history` relations.
//! TODO: Consider `.files`, `.tombstones`, `.metadata`, `.protocol`, `.transactions`, and
//! `.domain_metadata` relations for reconciled snapshot state, with optional time travel.
//! TODO: Consider `.partitions` and version-bounded `.changes` relations for statistics and CDF.
//! TODO: Consider `.log_entries`, `.checkpoints`, `.sidecars`, `.checksums`, and
//! `.log_compactions` relations for retained log artifacts. These extensions are deferred;
//! no Delta metadata-relation suffixes are currently registered.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeltaInspectionType {
    Detail,
    History,
}

/// Internal descriptors for Delta Spark SQL commands, not public CALL procedures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeltaCommandType {
    DescribeDetail,
    DescribeHistory,
    Vacuum,
    Optimize,
}

impl DeltaCommandType {
    pub fn parse(name: &str) -> Option<Self> {
        [
            Self::DescribeDetail,
            Self::DescribeHistory,
            Self::Vacuum,
            Self::Optimize,
        ]
        .into_iter()
        .find(|kind| kind.name().eq_ignore_ascii_case(name))
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::DescribeDetail => "describe_detail",
            Self::DescribeHistory => "describe_history",
            Self::Vacuum => "vacuum",
            Self::Optimize => "optimize",
        }
    }
}

fn string_map() -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Utf8, true),
                ]
                .into(),
            ),
            false,
        )),
        false,
    )
}

pub(crate) fn history_schema() -> SchemaRef {
    let strings = |names: &[&str]| {
        DataType::Struct(
            names
                .iter()
                .map(|name| Field::new(*name, DataType::Utf8, true))
                .collect::<Vec<_>>()
                .into(),
        )
    };
    Arc::new(Schema::new(vec![
        Field::new("version", DataType::Int64, true),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("userId", DataType::Utf8, true),
        Field::new("userName", DataType::Utf8, true),
        Field::new("operation", DataType::Utf8, true),
        Field::new("operationParameters", string_map(), true),
        Field::new(
            "job",
            strings(&[
                "jobId",
                "jobName",
                "jobRunId",
                "runId",
                "jobOwnerId",
                "triggerType",
            ]),
            true,
        ),
        Field::new("notebook", strings(&["notebookId"]), true),
        Field::new("clusterId", DataType::Utf8, true),
        Field::new("readVersion", DataType::Int64, true),
        Field::new("isolationLevel", DataType::Utf8, true),
        Field::new("isBlindAppend", DataType::Boolean, true),
        Field::new("operationMetrics", string_map(), true),
        Field::new("userMetadata", DataType::Utf8, true),
        Field::new("engineInfo", DataType::Utf8, true),
    ]))
}

pub(crate) fn detail_schema() -> SchemaRef {
    let strings = DataType::List(Arc::new(Field::new("element", DataType::Utf8, true)));
    let timestamp = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    Arc::new(Schema::new(vec![
        Field::new("format", DataType::Utf8, true),
        Field::new("id", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("location", DataType::Utf8, true),
        Field::new("createdAt", timestamp.clone(), true),
        Field::new("lastModified", timestamp, true),
        Field::new("partitionColumns", strings.clone(), true),
        Field::new("clusteringColumns", strings.clone(), true),
        Field::new("numFiles", DataType::Int64, true),
        Field::new("sizeInBytes", DataType::Int64, true),
        Field::new("properties", string_map(), true),
        Field::new("minReaderVersion", DataType::Int32, true),
        Field::new("minWriterVersion", DataType::Int32, true),
        Field::new("tableFeatures", strings, true),
    ]))
}

pub(crate) fn vacuum_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("path", DataType::Utf8, true)]))
}

pub(crate) fn optimize_schema() -> SchemaRef {
    fn structure(fields: Vec<Field>) -> DataType {
        DataType::Struct(fields.into())
    }
    fn long(name: &str) -> Field {
        Field::new(name, DataType::Int64, false)
    }
    let file_sizes = structure(vec![
        Field::new("min", DataType::Int64, true),
        Field::new("max", DataType::Int64, true),
        Field::new("avg", DataType::Float64, false),
        long("totalFiles"),
        long("totalSize"),
    ]);
    let zfiles = structure(vec![long("num"), long("size")]);
    let cfiles = structure(vec![long("numFiles"), long("size")]);
    let zorder = structure(vec![
        Field::new("strategyName", DataType::Utf8, true),
        Field::new("inputCubeFiles", zfiles.clone(), true),
        Field::new("inputOtherFiles", zfiles.clone(), true),
        long("inputNumCubes"),
        Field::new("mergedFiles", zfiles, true),
        long("numOutputCubes"),
        Field::new("mergedNumCubes", DataType::Int64, true),
    ]);
    let clustering = structure(vec![
        Field::new("inputZCubeFiles", cfiles.clone(), true),
        Field::new("inputOtherFiles", cfiles.clone(), true),
        long("inputNumZCubes"),
        Field::new("mergedFiles", cfiles, true),
        long("numOutputZCubes"),
    ]);
    let parallelism = structure(
        [
            "maxClusterActiveParallelism",
            "minClusterActiveParallelism",
            "maxSessionActiveParallelism",
            "minSessionActiveParallelism",
        ]
        .into_iter()
        .map(|name| Field::new(name, DataType::Int64, true))
        .collect(),
    );
    let metrics = structure(vec![
        long("numFilesAdded"),
        long("numFilesRemoved"),
        Field::new("filesAdded", file_sizes.clone(), true),
        Field::new("filesRemoved", file_sizes, true),
        long("partitionsOptimized"),
        Field::new("zOrderStats", zorder, true),
        Field::new("clusteringStats", clustering, true),
        long("numBins"),
        long("numBatches"),
        long("totalConsideredFiles"),
        long("totalFilesSkipped"),
        Field::new("preserveInsertionOrder", DataType::Boolean, false),
        long("numFilesSkippedToReduceWriteAmplification"),
        long("numBytesSkippedToReduceWriteAmplification"),
        long("startTimeMs"),
        long("endTimeMs"),
        long("totalClusterParallelism"),
        long("totalScheduledTasks"),
        Field::new("autoCompactParallelismStats", parallelism, true),
        Field::new(
            "deletionVectorStats",
            structure(vec![
                long("numDeletionVectorsRemoved"),
                long("numDeletionVectorRowsRemoved"),
            ]),
            true,
        ),
        long("numTableColumns"),
        long("numTableColumnsWithStats"),
    ]);
    Arc::new(Schema::new(vec![
        Field::new("path", DataType::Utf8, true),
        Field::new("metrics", metrics, true),
    ]))
}
