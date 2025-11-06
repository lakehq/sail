/// Credit: <https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/kernel/models/fields.rs>
use std::sync::{Arc, LazyLock};

use delta_kernel::schema::{ArrayType, DataType, MapType, StructField, StructType};

// TODO: ActionType mapping removed; use direct field statics and schema accessors instead

// https://github.com/delta-io/delta/blob/master/PROTOCOL.md#change-metadata
static METADATA_FIELD: LazyLock<StructField> = LazyLock::new(|| {
    StructField::new(
        "metaData",
        StructType::try_new(vec![
            StructField::new("id", DataType::STRING, true),
            StructField::new("name", DataType::STRING, true),
            StructField::new("description", DataType::STRING, true),
            StructField::new(
                "format",
                StructType::try_new(vec![
                    StructField::new("provider", DataType::STRING, true),
                    StructField::new(
                        "options",
                        MapType::new(DataType::STRING, DataType::STRING, true),
                        false,
                    ),
                ])
                .expect("Failed to construct format StructType in METADATA_FIELD"),
                false,
            ),
            StructField::new("schemaString", DataType::STRING, true),
            StructField::new(
                "partitionColumns",
                ArrayType::new(DataType::STRING, false),
                true,
            ),
            StructField::new("createdTime", DataType::LONG, true),
            StructField::new(
                "configuration",
                MapType::new(DataType::STRING, DataType::STRING, true),
                false,
            ),
        ])
        .expect("Failed to construct StructType for METADATA_FIELD"),
        true,
    )
});
// https://github.com/delta-io/delta/blob/master/PROTOCOL.md#protocol-evolution
static PROTOCOL_FIELD: LazyLock<StructField> = LazyLock::new(|| {
    StructField::new(
        "protocol",
        StructType::try_new(vec![
            StructField::new("minReaderVersion", DataType::INTEGER, true),
            StructField::new("minWriterVersion", DataType::INTEGER, true),
            StructField::new(
                "readerFeatures",
                ArrayType::new(DataType::STRING, true),
                true,
            ),
            StructField::new(
                "writerFeatures",
                ArrayType::new(DataType::STRING, true),
                true,
            ),
        ])
        .expect("Failed to construct StructType for PROTOCOL_FIELD"),
        true,
    )
});
// https://github.com/delta-io/delta/blob/master/PROTOCOL.md#commit-provenance-information
static COMMIT_INFO_FIELD: LazyLock<StructField> = LazyLock::new(|| {
    StructField::new(
        "commitInfo",
        StructType::try_new(vec![
            StructField::new("timestamp", DataType::LONG, false),
            StructField::new("operation", DataType::STRING, false),
            StructField::new("isolationLevel", DataType::STRING, true),
            StructField::new("isBlindAppend", DataType::BOOLEAN, true),
            StructField::new("txnId", DataType::STRING, true),
            StructField::new("readVersion", DataType::LONG, true),
            StructField::new(
                "operationParameters",
                MapType::new(DataType::STRING, DataType::STRING, true),
                true,
            ),
            StructField::new(
                "operationMetrics",
                MapType::new(DataType::STRING, DataType::STRING, true),
                true,
            ),
        ])
        .expect("Failed to construct StructType for COMMIT_INFO_FIELD"),
        true,
    )
});
// https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-file-and-remove-file
static ADD_FIELD: LazyLock<StructField> = LazyLock::new(|| {
    StructField::new(
        "add",
        StructType::try_new(vec![
            StructField::new("path", DataType::STRING, true),
            partition_values_field(),
            StructField::new("size", DataType::LONG, true),
            StructField::new("modificationTime", DataType::LONG, true),
            StructField::new("dataChange", DataType::BOOLEAN, true),
            StructField::new("stats", DataType::STRING, true),
            tags_field(),
            deletion_vector_field(),
            StructField::new("baseRowId", DataType::LONG, true),
            StructField::new("defaultRowCommitVersion", DataType::LONG, true),
            StructField::new("clusteringProvider", DataType::STRING, true),
        ])
        .expect("Failed to construct StructType for ADD_FIELD"),
        true,
    )
});
// https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-file-and-remove-file
static REMOVE_FIELD: LazyLock<StructField> = LazyLock::new(|| {
    StructField::new(
        "remove",
        StructType::try_new(vec![
            StructField::new("path", DataType::STRING, true),
            StructField::new("deletionTimestamp", DataType::LONG, true),
            StructField::new("dataChange", DataType::BOOLEAN, true),
            StructField::new("extendedFileMetadata", DataType::BOOLEAN, true),
            partition_values_field(),
            StructField::new("size", DataType::LONG, true),
            StructField::new("stats", DataType::STRING, true),
            tags_field(),
            deletion_vector_field(),
            StructField::new("baseRowId", DataType::LONG, true),
            StructField::new("defaultRowCommitVersion", DataType::LONG, true),
        ])
        .expect("Failed to construct StructType for REMOVE_FIELD"),
        true,
    )
});
// TODO implement support for this checkpoint
#[expect(dead_code)]
static REMOVE_FIELD_CHECKPOINT: LazyLock<StructField> = LazyLock::new(|| {
    StructField::new(
        "remove",
        StructType::try_new(vec![
            StructField::new("path", DataType::STRING, false),
            StructField::new("deletionTimestamp", DataType::LONG, true),
            StructField::new("dataChange", DataType::BOOLEAN, false),
        ])
        .expect("Failed to construct StructType for REMOVE_FIELD_CHECKPOINT"),
        true,
    )
});
// https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-cdc-file
static CDC_FIELD: LazyLock<StructField> = LazyLock::new(|| {
    StructField::new(
        "cdc",
        StructType::try_new(vec![
            StructField::new("path", DataType::STRING, false),
            partition_values_field(),
            StructField::new("size", DataType::LONG, false),
            StructField::new("dataChange", DataType::BOOLEAN, false),
            tags_field(),
        ])
        .expect("Failed to construct StructType for CDC_FIELD"),
        true,
    )
});
// https://github.com/delta-io/delta/blob/master/PROTOCOL.md#transaction-identifiers
static TXN_FIELD: LazyLock<StructField> = LazyLock::new(|| {
    StructField::new(
        "txn",
        StructType::try_new(vec![
            StructField::new("appId", DataType::STRING, false),
            StructField::new("version", DataType::LONG, false),
            StructField::new("lastUpdated", DataType::LONG, true),
        ])
        .expect("Failed to construct StructType for TXN_FIELD"),
        true,
    )
});
// https://github.com/delta-io/delta/blob/master/PROTOCOL.md#domain-metadata
static DOMAIN_METADATA_FIELD: LazyLock<StructField> = LazyLock::new(|| {
    StructField::new(
        "domainMetadata",
        StructType::try_new(vec![
            StructField::new("domain", DataType::STRING, false),
            StructField::new(
                "configuration",
                MapType::new(DataType::STRING, DataType::STRING, true),
                true,
            ),
            StructField::new("removed", DataType::BOOLEAN, false),
        ])
        .expect("Failed to construct StructType for DOMAIN_METADATA_FIELD"),
        true,
    )
});
// https://github.com/delta-io/delta/blob/master/PROTOCOL.md#checkpoint-metadata
#[allow(dead_code)]
static CHECKPOINT_METADATA_FIELD: LazyLock<StructField> = LazyLock::new(|| {
    StructField::new(
        "checkpointMetadata",
        StructType::try_new(vec![
            StructField::new("flavor", DataType::STRING, false),
            tags_field(),
        ])
        .expect("Failed to construct StructType for CHECKPOINT_METADATA_FIELD"),
        true,
    )
});
// https://github.com/delta-io/delta/blob/master/PROTOCOL.md#sidecar-file-information
#[allow(dead_code)]
static SIDECAR_FIELD: LazyLock<StructField> = LazyLock::new(|| {
    StructField::new(
        "sidecar",
        StructType::try_new(vec![
            StructField::new("path", DataType::STRING, false),
            StructField::new("sizeInBytes", DataType::LONG, true),
            StructField::new("modificationTime", DataType::LONG, false),
            StructField::new("type", DataType::STRING, false),
            tags_field(),
        ])
        .expect("Failed to construct StructType for SIDECAR_FIELD"),
        true,
    )
});

static LOG_SCHEMA: LazyLock<StructType> = LazyLock::new(|| {
    StructType::try_new(vec![
        ADD_FIELD.clone(),
        CDC_FIELD.clone(),
        COMMIT_INFO_FIELD.clone(),
        DOMAIN_METADATA_FIELD.clone(),
        METADATA_FIELD.clone(),
        PROTOCOL_FIELD.clone(),
        REMOVE_FIELD.clone(),
        TXN_FIELD.clone(),
    ])
    .expect("Failed to construct StructType for LOG_SCHEMA")
});

fn tags_field() -> StructField {
    StructField::new(
        "tags",
        MapType::new(DataType::STRING, DataType::STRING, true),
        true,
    )
}

fn partition_values_field() -> StructField {
    StructField::new(
        "partitionValues",
        MapType::new(DataType::STRING, DataType::STRING, true),
        true,
    )
}

fn deletion_vector_field() -> StructField {
    StructField::new(
        "deletionVector",
        DataType::Struct(Box::new(
            StructType::try_new(vec![
                StructField::new("storageType", DataType::STRING, false),
                StructField::new("pathOrInlineDv", DataType::STRING, false),
                StructField::new("offset", DataType::INTEGER, true),
                StructField::new("sizeInBytes", DataType::INTEGER, false),
                StructField::new("cardinality", DataType::LONG, false),
            ])
            .expect("Failed to construct StructType for deletion_vector_field"),
        )),
        true,
    )
}

pub(crate) fn log_schema_ref() -> &'static Arc<StructType> {
    static LOG_SCHEMA_REF: LazyLock<Arc<StructType>> =
        LazyLock::new(|| Arc::new(LOG_SCHEMA.clone()));

    &LOG_SCHEMA_REF
}
