// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Int64Array, MapArray, StringArray, TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::{MemTable, TableProvider};

use crate::spec::{Snapshot, TableMetadata};

/// Builds Spark-compatible rows for Iceberg's `snapshots` metadata table.
///
/// This table is intentionally materialized from the current metadata JSON. Snapshot
/// metadata is small, and doing so keeps metadata inspection independent from manifest
/// and data-file scan planning.
pub(crate) fn snapshots_table(metadata: &TableMetadata) -> Result<Arc<dyn TableProvider>> {
    let batch = snapshots_record_batch(&metadata.snapshots)?;
    let schema = batch.schema();
    Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
}

fn snapshots_record_batch(snapshots: &[Snapshot]) -> Result<RecordBatch> {
    let mut committed_at = Vec::with_capacity(snapshots.len());
    let mut summary_keys = Vec::new();
    let mut summary_values = Vec::new();
    let mut summary_offsets = Vec::with_capacity(snapshots.len() + 1);
    summary_offsets.push(0_u32);

    for snapshot in snapshots {
        committed_at.push(snapshot.timestamp_ms().checked_mul(1_000).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "Iceberg snapshot timestamp is out of range: {}",
                snapshot.timestamp_ms()
            ))
        })?);

        let mut properties = snapshot
            .summary()
            .additional_properties
            .iter()
            .collect::<Vec<_>>();
        properties.sort_unstable_by_key(|(key, _)| *key);
        for (key, value) in properties {
            summary_keys.push(key.as_str());
            summary_values.push(value.as_str());
        }
        summary_offsets.push(u32::try_from(summary_keys.len()).map_err(|_| {
            DataFusionError::Plan("Iceberg snapshot summary is too large".to_string())
        })?);
    }

    let committed_at = TimestampMicrosecondArray::from(committed_at).with_timezone("UTC");
    let snapshot_id = Int64Array::from_iter_values(snapshots.iter().map(Snapshot::snapshot_id));
    let parent_id = Int64Array::from(
        snapshots
            .iter()
            .map(Snapshot::parent_snapshot_id)
            .collect::<Vec<_>>(),
    );
    let operation = StringArray::from(
        snapshots
            .iter()
            .map(|snapshot| Some(snapshot.summary().operation.as_str()))
            .collect::<Vec<_>>(),
    );
    let manifest_list = StringArray::from(
        snapshots
            .iter()
            .map(|snapshot| {
                (!snapshot.manifest_list().is_empty()).then(|| snapshot.manifest_list())
            })
            .collect::<Vec<_>>(),
    );
    let summary_values = StringArray::from(summary_values);
    let summary =
        MapArray::new_from_strings(summary_keys.into_iter(), &summary_values, &summary_offsets)?;

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "committed_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("snapshot_id", DataType::Int64, false),
        Field::new("parent_id", DataType::Int64, true),
        Field::new("operation", DataType::Utf8, true),
        Field::new("manifest_list", DataType::Utf8, true),
        Field::new("summary", summary.data_type().clone(), true),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(committed_at),
        Arc::new(snapshot_id),
        Arc::new(parent_id),
        Arc::new(operation),
        Arc::new(manifest_list),
        Arc::new(summary),
    ];
    Ok(RecordBatch::try_new(schema, columns)?)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Int64Array, TimestampMicrosecondArray};
    use datafusion::arrow::datatypes::{DataType, TimeUnit};

    use super::snapshots_record_batch;
    use crate::spec::{Operation, Snapshot, Summary};

    #[test]
    fn snapshots_use_spark_schema_and_preserve_snapshot_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let first = Snapshot::builder()
            .with_snapshot_id(10)
            .with_timestamp_ms(1_000)
            .with_manifest_list("metadata/first.avro")
            .with_summary(Summary::new(Operation::Append).with_property("added-records", "1"))
            .build()?;
        let second = Snapshot::builder()
            .with_snapshot_id(20)
            .with_parent_snapshot_id(10)
            .with_timestamp_ms(2_000)
            .with_manifest_list("metadata/second.avro")
            .with_summary(Summary::new(Operation::Overwrite))
            .build()?;

        let batch = snapshots_record_batch(&[first, second])?;
        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or("committed_at type")?
                .values(),
            &[1_000_000, 2_000_000]
        );
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("snapshot_id type")?
                .values(),
            &[10, 20]
        );
        assert!(batch.schema().field(5).data_type().is_nested());
        Ok(())
    }
}
