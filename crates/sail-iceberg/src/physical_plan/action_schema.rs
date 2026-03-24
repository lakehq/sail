use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};

use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result};
use serde::{Deserialize, Serialize};

use crate::spec::types::values::{Literal, PrimitiveLiteral};
use crate::spec::{
    DataContentType, DataFile, DataFileFormat, Operation, PartitionSpec, Schema as IcebergSchema,
    TableRequirement,
};

pub const COL_ACTION: &str = "action";

static ACTION_FIELDS: LazyLock<Vec<FieldRef>> = LazyLock::new(|| {
    #[expect(
        clippy::unwrap_used,
        reason = "ACTION_FIELDS is a process-global constant."
    )]
    let fields = iceberg_action_fields_build()
        .map_err(|msg| format!("iceberg action fields initialization failed: {msg}"))
        .unwrap();
    fields
});
static ACTION_SCHEMA: LazyLock<SchemaRef> =
    LazyLock::new(|| Arc::new(Schema::new((*ACTION_FIELDS).clone())));

#[derive(Debug, Clone, Default, PartialEq)]
pub struct CommitMeta {
    pub table_uri: String,
    pub row_count: u64,
    pub operation: Operation,
    pub requirements: Vec<TableRequirement>,
    pub schema: Option<IcebergSchema>,
    pub partition_spec: Option<PartitionSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitMetaAction {
    pub table_uri: String,
    pub row_count: u64,
    pub operation: String,
    /// Requirements are relatively small but hard to trace into Arrow schema; keep as JSON.
    pub requirements_json: String,
    /// Optional Iceberg Schema JSON (rare) to avoid huge Arrow schema.
    pub schema_json: Option<String>,
    /// Optional PartitionSpec JSON (rare) to avoid huge Arrow schema.
    pub partition_spec_json: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartitionValue {
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    String(String),
    /// Stored as decimal string because `serde_arrow` does not support `i128` natively.
    Int128(String),
    /// Stored as decimal string because `serde_arrow` does not support `u128` natively.
    UInt128(String),
    Binary(Vec<u8>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddFileAction {
    pub content: String,
    pub file_path: String,
    pub file_format: String,
    pub partition: Vec<Option<PartitionValue>>,
    pub record_count: u64,
    pub file_size_in_bytes: u64,
    pub column_sizes: BTreeMap<i32, u64>,
    pub value_counts: BTreeMap<i32, u64>,
    pub null_value_counts: BTreeMap<i32, u64>,
    pub split_offsets: Vec<i64>,
    pub partition_spec_id: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteFileAction {
    pub file_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExecAction {
    #[serde(rename = "add")]
    Add(AddFileAction),
    #[serde(rename = "delete")]
    Delete(DeleteFileAction),
    #[serde(rename = "commit_meta")]
    CommitMeta(CommitMetaAction),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionRow {
    pub action: ExecAction,
}

fn map_type_i32_u64() -> DataType {
    // Arrow Map is represented as `Map<entries: Struct<keys: Int32, values: UInt64>>`.
    let entries_struct = DataType::Struct(
        vec![
            Arc::new(Field::new("keys", DataType::Int32, false)),
            Arc::new(Field::new("values", DataType::UInt64, false)),
        ]
        .into(),
    );
    let entries_field = Arc::new(Field::new("entries", entries_struct, false));
    DataType::Map(entries_field, false)
}

fn iceberg_action_tracing_options(
) -> std::result::Result<serde_arrow::schema::TracingOptions, String> {
    use serde_arrow::schema::TracingOptions;

    TracingOptions::default()
        .map_as_struct(false)
        .strings_as_large_utf8(false)
        .sequence_as_large_list(false)
        .overwrite(
            "action.add.column_sizes",
            Field::new("column_sizes", map_type_i32_u64(), false),
        )
        .and_then(|opts| {
            opts.overwrite(
                "action.add.value_counts",
                Field::new("value_counts", map_type_i32_u64(), false),
            )
        })
        .and_then(|opts| {
            opts.overwrite(
                "action.add.null_value_counts",
                Field::new("null_value_counts", map_type_i32_u64(), false),
            )
        })
        .map_err(|e| format!("failed to build serde_arrow tracing options: {e}"))
}

fn iceberg_action_fields_build() -> std::result::Result<Vec<FieldRef>, String> {
    use serde_arrow::schema::SchemaLike;

    Vec::<FieldRef>::from_type::<ActionRow>(iceberg_action_tracing_options()?)
        .map_err(|e| format!("ActionRow schema tracing failed: {e}"))
}

fn iceberg_action_fields() -> Result<&'static Vec<FieldRef>> {
    Ok(&*ACTION_FIELDS)
}

pub fn iceberg_action_schema() -> Result<SchemaRef> {
    Ok(Arc::clone(&*ACTION_SCHEMA))
}

fn parse_operation(s: &str) -> Result<Operation> {
    match s {
        "append" => Ok(Operation::Append),
        "replace" => Ok(Operation::Replace),
        "overwrite" => Ok(Operation::Overwrite),
        "delete" => Ok(Operation::Delete),
        other => Err(DataFusionError::Plan(format!(
            "unknown iceberg operation '{other}'"
        ))),
    }
}

impl From<PrimitiveLiteral> for PartitionValue {
    fn from(p: PrimitiveLiteral) -> Self {
        match p {
            PrimitiveLiteral::Boolean(v) => PartitionValue::Boolean(v),
            PrimitiveLiteral::Int(v) => PartitionValue::Int(v),
            PrimitiveLiteral::Long(v) => PartitionValue::Long(v),
            PrimitiveLiteral::Float(v) => PartitionValue::Float(v.0),
            PrimitiveLiteral::Double(v) => PartitionValue::Double(v.0),
            PrimitiveLiteral::String(v) => PartitionValue::String(v),
            PrimitiveLiteral::Int128(v) => PartitionValue::Int128(v.to_string()),
            PrimitiveLiteral::UInt128(v) => PartitionValue::UInt128(v.to_string()),
            PrimitiveLiteral::Binary(v) => PartitionValue::Binary(v),
        }
    }
}

impl TryFrom<PartitionValue> for PrimitiveLiteral {
    type Error = DataFusionError;

    fn try_from(v: PartitionValue) -> Result<Self> {
        match v {
            PartitionValue::Boolean(x) => Ok(PrimitiveLiteral::Boolean(x)),
            PartitionValue::Int(x) => Ok(PrimitiveLiteral::Int(x)),
            PartitionValue::Long(x) => Ok(PrimitiveLiteral::Long(x)),
            PartitionValue::Float(x) => Ok(PrimitiveLiteral::Float(ordered_float::OrderedFloat(x))),
            PartitionValue::Double(x) => {
                Ok(PrimitiveLiteral::Double(ordered_float::OrderedFloat(x)))
            }
            PartitionValue::String(x) => Ok(PrimitiveLiteral::String(x)),
            PartitionValue::Int128(s) => {
                s.parse::<i128>()
                    .map(PrimitiveLiteral::Int128)
                    .map_err(|e| {
                        DataFusionError::Plan(format!(
                            "failed to parse i128 partition literal: {e}"
                        ))
                    })
            }
            PartitionValue::UInt128(s) => {
                s.parse::<u128>()
                    .map(PrimitiveLiteral::UInt128)
                    .map_err(|e| {
                        DataFusionError::Plan(format!(
                            "failed to parse u128 partition literal: {e}"
                        ))
                    })
            }
            PartitionValue::Binary(x) => Ok(PrimitiveLiteral::Binary(x)),
        }
    }
}

impl TryFrom<DataFile> for AddFileAction {
    type Error = DataFusionError;

    fn try_from(df: DataFile) -> Result<Self> {
        let partition = df
            .partition
            .into_iter()
            .map(|opt| match opt {
                None => Ok(None),
                Some(Literal::Primitive(p)) => Ok(Some(p.into())),
                Some(other) => Err(DataFusionError::Internal(format!(
                    "unsupported non-primitive partition literal in DataFile: {other:?}"
                ))),
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(AddFileAction {
            content: df.content.as_action_str().to_string(),
            file_path: df.file_path,
            file_format: df.file_format.as_action_str().to_string(),
            partition,
            record_count: df.record_count,
            file_size_in_bytes: df.file_size_in_bytes,
            column_sizes: df.column_sizes.into_iter().collect(),
            value_counts: df.value_counts.into_iter().collect(),
            null_value_counts: df.null_value_counts.into_iter().collect(),
            split_offsets: df.split_offsets,
            partition_spec_id: df.partition_spec_id,
        })
    }
}

impl TryFrom<AddFileAction> for DataFile {
    type Error = DataFusionError;

    fn try_from(a: AddFileAction) -> Result<Self> {
        let content = match a.content.as_str() {
            "Data" => DataContentType::Data,
            "PositionDeletes" => DataContentType::PositionDeletes,
            "EqualityDeletes" => DataContentType::EqualityDeletes,
            other => {
                return Err(DataFusionError::Plan(format!(
                    "unknown DataContentType string '{other}'"
                )))
            }
        };
        let file_format = match a.file_format.as_str() {
            "Avro" => DataFileFormat::Avro,
            "Orc" => DataFileFormat::Orc,
            "Parquet" => DataFileFormat::Parquet,
            "Puffin" => DataFileFormat::Puffin,
            other => {
                return Err(DataFusionError::Plan(format!(
                    "unknown DataFileFormat string '{other}'"
                )))
            }
        };

        let partition = a
            .partition
            .into_iter()
            .map(|opt| match opt {
                None => Ok(None),
                Some(pv) => Ok(Some(Literal::Primitive(pv.try_into()?))),
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(DataFile {
            content,
            file_path: a.file_path,
            file_format,
            partition,
            record_count: a.record_count,
            file_size_in_bytes: a.file_size_in_bytes,
            column_sizes: a.column_sizes.into_iter().collect(),
            value_counts: a.value_counts.into_iter().collect(),
            null_value_counts: a.null_value_counts.into_iter().collect(),
            nan_value_counts: Default::default(),
            lower_bounds: Default::default(),
            upper_bounds: Default::default(),
            block_size_in_bytes: None,
            key_metadata: None,
            split_offsets: a.split_offsets,
            equality_ids: Vec::new(),
            sort_order_id: None,
            first_row_id: None,
            partition_spec_id: a.partition_spec_id,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        })
    }
}

pub fn encode_actions(rows: Vec<ActionRow>) -> Result<RecordBatch> {
    // TODO: consolidate the logic once we have corresponding utility functions.
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(iceberg_action_schema()?));
    }
    serde_arrow::to_record_batch(iceberg_action_fields()?, &rows)
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

pub fn encode_add_data_files(data_files: Vec<DataFile>) -> Result<RecordBatch> {
    if data_files.is_empty() {
        return Ok(RecordBatch::new_empty(iceberg_action_schema()?));
    }
    let rows = data_files
        .into_iter()
        .map(|df| {
            Ok(ActionRow {
                action: ExecAction::Add(df.try_into()?),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    encode_actions(rows)
}

pub fn encode_commit_meta(meta: CommitMeta) -> Result<RecordBatch> {
    let requirements_json = serde_json::to_string(&meta.requirements)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let schema_json = meta
        .schema
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let partition_spec_json = meta
        .partition_spec
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let rows = vec![ActionRow {
        action: ExecAction::CommitMeta(CommitMetaAction {
            table_uri: meta.table_uri,
            row_count: meta.row_count,
            operation: meta.operation.as_str().to_string(),
            requirements_json,
            schema_json,
            partition_spec_json,
        }),
    }];
    encode_actions(rows)
}

pub fn decode_actions_and_meta_from_batch(
    batch: &RecordBatch,
) -> Result<(Vec<DataFile>, Vec<DataFile>, Option<CommitMeta>)> {
    let rows: Vec<ActionRow> = serde_arrow::from_record_batch(batch)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let mut adds: Vec<DataFile> = Vec::new();
    let deletes: Vec<DataFile> = Vec::new();
    let mut meta: Option<CommitMeta> = None;

    for row in rows {
        match row.action {
            ExecAction::Add(a) => adds.push(a.try_into()?),
            ExecAction::Delete(_d) => {
                // Delete files are not implemented in local commits yet; plumb through later.
                return Err(DataFusionError::NotImplemented(
                    "Iceberg delete file actions are not implemented".to_string(),
                ));
            }
            ExecAction::CommitMeta(m) => {
                let requirements: Vec<TableRequirement> =
                    serde_json::from_str(&m.requirements_json)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let schema: Option<IcebergSchema> = m
                    .schema_json
                    .as_deref()
                    .map(serde_json::from_str::<IcebergSchema>)
                    .transpose()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let partition_spec: Option<PartitionSpec> = m
                    .partition_spec_json
                    .as_deref()
                    .map(serde_json::from_str::<PartitionSpec>)
                    .transpose()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                meta = Some(CommitMeta {
                    table_uri: m.table_uri,
                    row_count: m.row_count,
                    operation: parse_operation(&m.operation)?,
                    requirements,
                    schema,
                    partition_spec,
                });
            }
        }
    }

    Ok((adds, deletes, meta))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::compute::concat_batches;

    use super::*;

    #[test]
    fn encode_add_and_commit_meta_roundtrip() -> Result<()> {
        let df = DataFile {
            content: DataContentType::Data,
            file_path: "s3://bucket/a.parquet".to_string(),
            file_format: DataFileFormat::Parquet,
            partition: vec![Some(Literal::Primitive(PrimitiveLiteral::Int(1)))],
            record_count: 10,
            file_size_in_bytes: 123,
            column_sizes: HashMap::from([(1, 10u64)]),
            value_counts: HashMap::from([(1, 10u64)]),
            null_value_counts: HashMap::new(),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            block_size_in_bytes: None,
            key_metadata: None,
            split_offsets: vec![0],
            equality_ids: vec![],
            sort_order_id: None,
            first_row_id: None,
            partition_spec_id: 0,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        };

        let meta = CommitMeta {
            table_uri: "s3://bucket/table".to_string(),
            row_count: 10,
            operation: Operation::Append,
            requirements: vec![TableRequirement::NotExist],
            schema: None,
            partition_spec: None,
        };

        let schema = iceberg_action_schema()?;
        let batches = vec![
            encode_add_data_files(vec![df.clone()])?,
            encode_commit_meta(meta)?,
        ];
        let merged = concat_batches(&schema, &batches)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let (adds, _deletes, meta) = decode_actions_and_meta_from_batch(&merged)?;
        assert_eq!(adds.len(), 1);
        assert_eq!(adds[0].file_path, df.file_path);
        assert_eq!(adds[0].record_count, df.record_count);
        assert!(meta.is_some());
        Ok(())
    }
}
