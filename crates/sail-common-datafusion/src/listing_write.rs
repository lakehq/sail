use std::sync::Arc;

use datafusion::arrow::array::{Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_common::{Result, exec_datafusion_err};
use serde::{Deserialize, Serialize};

pub const LISTING_WRITE_MANIFEST_COLUMN: &str = "__sail_listing_write_task_manifest";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ListingWriteFile {
    pub staging_path: String,
    pub final_relative_path: String,
    pub size: u64,
    pub row_count: u64,
    pub e_tag: Option<String>,
    pub version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ListingWriteTaskManifest {
    pub write_id: String,
    pub job_id: u64,
    pub stage: u64,
    pub partition: u64,
    pub attempt: u64,
    pub row_count: u64,
    pub files: Vec<ListingWriteFile>,
}

pub fn listing_write_manifest_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        LISTING_WRITE_MANIFEST_COLUMN,
        DataType::Utf8,
        false,
    )]))
}

pub fn encode_listing_write_manifest(manifest: &ListingWriteTaskManifest) -> Result<RecordBatch> {
    let value = serde_json::to_string(manifest).map_err(|error| {
        exec_datafusion_err!("failed to encode listing write task manifest: {error}")
    })?;
    Ok(RecordBatch::try_new(
        listing_write_manifest_schema(),
        vec![Arc::new(StringArray::from(vec![value]))],
    )?)
}

pub fn decode_listing_write_manifests(
    batch: &RecordBatch,
) -> Result<Vec<ListingWriteTaskManifest>> {
    let values = batch
        .column_by_name(LISTING_WRITE_MANIFEST_COLUMN)
        .ok_or_else(|| exec_datafusion_err!("missing listing write task manifest column"))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| exec_datafusion_err!("listing write task manifest must be Utf8"))?;
    (0..values.len())
        .map(|index| {
            if values.is_null(index) {
                return Err(exec_datafusion_err!(
                    "listing write task manifest must not be null"
                ));
            }
            serde_json::from_str(values.value(index)).map_err(|error| {
                exec_datafusion_err!("failed to decode listing write task manifest: {error}")
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listing_write_manifest_round_trip() -> Result<()> {
        let manifest = ListingWriteTaskManifest {
            write_id: "write-id".to_string(),
            job_id: 1,
            stage: 2,
            partition: 3,
            attempt: 4,
            row_count: 5,
            files: vec![ListingWriteFile {
                staging_path: "table/_temporary/sail/write-id/part.parquet".to_string(),
                final_relative_path: "p=a%2Fb/part.parquet".to_string(),
                size: 6,
                row_count: 5,
                e_tag: Some("etag".to_string()),
                version: Some("version".to_string()),
            }],
        };
        let batch = encode_listing_write_manifest(&manifest)?;
        assert_eq!(decode_listing_write_manifests(&batch)?, vec![manifest]);
        Ok(())
    }
}
