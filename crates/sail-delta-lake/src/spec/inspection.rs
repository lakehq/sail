//! Delta inspection and maintenance schemas.
//!
//! Relation suffixes and the `delta` procedure namespace are Sail SQL extensions, not names
//! prescribed by PROTOCOL.md. State relations use the protocol action schemas; `detail` and
//! `history` use the DESCRIBE DETAIL and DESCRIBE HISTORY result schemas.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

use super::{
    DeltaResult, add_struct_type, domain_metadata_struct_type, metadata_struct_type,
    protocol_struct_type, remove_struct_type, transaction_struct_type,
};

/// The inspection surface includes both reconciled state and retained log artifacts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeltaMetadataRelationType {
    /// Table details returned by DESCRIBE DETAIL.
    Detail,
    /// Retained commit provenance, not snapshot state or a data-change feed.
    History,
    /// Active logical AddFile actions, including deletion-vector identities.
    Files,
    /// Retained, reconciled RemoveFile actions; retention can remove old tombstones.
    Tombstones,
    /// Current metaData action, including schema and table configuration.
    Metadata,
    /// Current protocol action; feature lists are explicit, not inferred.
    Protocol,
    /// Latest retained SetTransaction per application ID.
    Transactions,
    /// Latest live domainMetadata per domain; removed domains are absent.
    DomainMetadata,
    /// File statistics grouped by logical partition values.
    Partitions,
    /// Row-level change data over a version/timestamp range (including inferred changes).
    Changes,
    /// Retained JSON commit actions, with their commit versions.
    LogEntries,
    /// Complete classic, multipart, or v2 checkpoints and the last-checkpoint hint.
    Checkpoints,
    /// Sidecars referenced by retained v2 checkpoints, not arbitrary directory contents.
    Sidecars,
    /// Per-version .crc statistics; these are optional and are not commit authority.
    Checksums,
    /// Compacted JSON log ranges, which do not preserve commit provenance.
    LogCompactions,
}

impl DeltaMetadataRelationType {
    pub const ALL: &'static [Self] = &[
        Self::Detail,
        Self::History,
        Self::Files,
        Self::Tombstones,
        Self::Metadata,
        Self::Protocol,
        Self::Transactions,
        Self::DomainMetadata,
        Self::Partitions,
        Self::Changes,
        Self::LogEntries,
        Self::Checkpoints,
        Self::Sidecars,
        Self::Checksums,
        Self::LogCompactions,
    ];

    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|kind| kind.name().eq_ignore_ascii_case(name))
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Detail => "detail",
            Self::History => "history",
            Self::Files => "files",
            Self::Tombstones => "tombstones",
            Self::Metadata => "metadata",
            Self::Protocol => "protocol",
            Self::Transactions => "transactions",
            Self::DomainMetadata => "domain_metadata",
            Self::Partitions => "partitions",
            Self::Changes => "changes",
            Self::LogEntries => "log_entries",
            Self::Checkpoints => "checkpoints",
            Self::Sidecars => "sidecars",
            Self::Checksums => "checksums",
            Self::LogCompactions => "log_compactions",
        }
    }

    pub fn is_supported(self) -> bool {
        matches!(
            self,
            Self::Detail
                | Self::History
                | Self::Files
                | Self::Tombstones
                | Self::Metadata
                | Self::Protocol
                | Self::Transactions
                | Self::DomainMetadata
        )
    }

    pub fn unsupported_reason(self) -> String {
        format!(
            "Delta metadata table '{}' is recognized but not implemented",
            self.name()
        )
    }

    pub(crate) fn schema(self) -> DeltaResult<SchemaRef> {
        let action = match self {
            Self::Detail => return Ok(detail_schema()),
            Self::History => return Ok(history_schema()),
            Self::Files => add_struct_type(),
            Self::Tombstones => remove_struct_type(),
            Self::Metadata => metadata_struct_type(),
            Self::Protocol => protocol_struct_type(),
            Self::Transactions => transaction_struct_type(),
            Self::DomainMetadata => domain_metadata_struct_type(),
            _ => return Err(super::DeltaError::generic(self.unsupported_reason())),
        };
        Ok(Arc::new(Schema::try_from(&action)?))
    }
}

/// Table utilities and protocol checkpoint/log-compaction maintenance.
/// Ordinary CREATE/ALTER/DROP and row-level DML keep their existing SQL interfaces.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeltaProcedureType {
    DescribeDetail,
    DescribeHistory,
    /// FULL/LITE/inventory candidate discovery, retention checks, dry run, and deletion.
    Vacuum,
    /// Bin packing, predicate selection, Z-order, and liquid clustering.
    Optimize,
    /// Restore a version or timestamp by committing new add/remove actions.
    Restore,
    /// Convert a supported external format in place.
    ConvertToDelta,
    /// Generate symlink-format manifests for external readers.
    Generate,
    /// Clone a source snapshot into a separate target table.
    Clone,
    /// PURGE rewrites and UniForm compatibility upgrades.
    Reorg,
    UpgradeTableProtocol,
    AddFeatureSupport,
    /// Feature removal, checkpoint protection, and optional history truncation.
    DropFeature,
    Checkpoint,
    CompactLog,
}

impl DeltaProcedureType {
    pub const ALL: &'static [Self] = &[
        Self::DescribeDetail,
        Self::DescribeHistory,
        Self::Vacuum,
        Self::Optimize,
        Self::Restore,
        Self::ConvertToDelta,
        Self::Generate,
        Self::Clone,
        Self::Reorg,
        Self::UpgradeTableProtocol,
        Self::AddFeatureSupport,
        Self::DropFeature,
        Self::Checkpoint,
        Self::CompactLog,
    ];

    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|kind| kind.name().eq_ignore_ascii_case(name))
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::DescribeDetail => "describe_detail",
            Self::DescribeHistory => "describe_history",
            Self::Vacuum => "vacuum",
            Self::Optimize => "optimize",
            Self::Restore => "restore",
            Self::ConvertToDelta => "convert_to_delta",
            Self::Generate => "generate",
            Self::Clone => "clone",
            Self::Reorg => "reorg",
            Self::UpgradeTableProtocol => "upgrade_table_protocol",
            Self::AddFeatureSupport => "add_feature_support",
            Self::DropFeature => "drop_feature",
            Self::Checkpoint => "checkpoint",
            Self::CompactLog => "compact_log",
        }
    }

    pub fn unsupported_reason(self) -> String {
        format!(
            "Delta procedure '{}' is recognized but not implemented",
            self.name()
        )
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
