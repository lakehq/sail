use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::arrow::array::{Array, ArrayRef, RecordBatch, StructArray};
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::common::{Result, internal_datafusion_err};
use parquet_variant_compute::VariantArrayBuilder;
use parquet_variant_json::JsonToVariant;
use sail_common_datafusion::array::record_batch::cast_record_batch_relaxed_tz;
use sail_common_datafusion::array::serde::ArrowSerializer;
use sail_common_datafusion::system::catalog::{MetricRow, SystemTable};
use serde::{Deserialize, Serialize};

pub fn build_rows<T>(table: SystemTable, rows: Vec<T>) -> Result<RecordBatch>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    ArrowSerializer::build_record_batch_with_schema(&rows, table.schema())
}

pub fn build_metrics(rows: Vec<MetricRow>) -> Result<RecordBatch> {
    #[derive(Serialize, Deserialize)]
    struct MetricMetadataRow {
        timestamp: DateTime<Utc>,
        name: String,
        attributes: BTreeMap<String, String>,
    }

    let metadata = rows
        .iter()
        .map(|row| MetricMetadataRow {
            timestamp: row.timestamp,
            name: row.name.clone(),
            attributes: row.attributes.clone(),
        })
        .collect::<Vec<_>>();
    let table_schema = SystemTable::Metrics.schema();
    let metadata_schema = Arc::new(Schema::new(table_schema.fields()[..3].to_vec()));
    let metadata_batch =
        ArrowSerializer::build_record_batch_with_schema(&metadata, metadata_schema)?;
    let mut values = VariantArrayBuilder::new(rows.len());
    for row in rows {
        let value = serde_json::to_string(&row.value)
            .map_err(|error| internal_datafusion_err!("failed to serialize metric: {error}"))?;
        values.append_json(&value)?;
    }
    let values: StructArray = values.build().into();
    let mut fields = metadata_batch
        .schema()
        .fields()
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    fields.push(Arc::new(Field::new(
        "value",
        values.data_type().clone(),
        false,
    )));
    let mut columns = metadata_batch.columns().to_vec();
    columns.push(Arc::new(values) as ArrayRef);
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
    cast_record_batch_relaxed_tz(&batch, &table_schema)
}
