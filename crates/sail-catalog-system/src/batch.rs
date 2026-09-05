use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::arrow::array::{ArrayRef, RecordBatch, StructArray};
use datafusion::arrow::datatypes::Schema;
use datafusion::common::{Result, internal_datafusion_err};
use parquet_variant::{Variant, VariantBuilderExt};
use parquet_variant_compute::VariantArrayBuilder;
use sail_common_datafusion::array::record_batch::cast_array_recursively;
use sail_common_datafusion::array::serde::ArrowSerializer;
use sail_system_store::catalog::{
    JobRow, MetricRow, OptionRow, SessionRow, StageRow, SystemTable, TaskRow, WorkerRow,
};
use sail_system_store::types::{MetricNumber, MetricValue};
use serde::{Deserialize, Serialize};

pub(crate) trait SystemTableRow {
    fn build_record_batch(table: SystemTable, rows: Vec<Self>) -> Result<RecordBatch>
    where
        Self: Sized;
}

pub(crate) fn build_rows<T>(table: SystemTable, rows: Vec<T>) -> Result<RecordBatch>
where
    T: SystemTableRow,
{
    T::build_record_batch(table, rows)
}

macro_rules! impl_serialized_system_table_row {
    ($($row:ty),+ $(,)?) => {
        $(
            impl SystemTableRow for $row {
                fn build_record_batch(table: SystemTable, rows: Vec<Self>) -> Result<RecordBatch> {
                    ArrowSerializer::build_record_batch_with_schema(&rows, table.schema())
                }
            }
        )+
    };
}

impl_serialized_system_table_row!(JobRow, OptionRow, SessionRow, StageRow, TaskRow, WorkerRow);

impl SystemTableRow for MetricRow {
    fn build_record_batch(_table: SystemTable, rows: Vec<Self>) -> Result<RecordBatch> {
        build_metrics(rows)
    }
}

fn build_metrics(rows: Vec<MetricRow>) -> Result<RecordBatch> {
    #[derive(Serialize, Deserialize)]
    struct MetricMetadataRow {
        timestamp: DateTime<Utc>,
        start_timestamp: Option<DateTime<Utc>>,
        name: String,
        attributes: BTreeMap<String, String>,
    }

    let metadata = rows
        .iter()
        .map(|row| MetricMetadataRow {
            timestamp: row.timestamp,
            start_timestamp: row.start_timestamp,
            name: row.name.clone(),
            attributes: row.attributes.clone(),
        })
        .collect::<Vec<_>>();
    let table_schema = SystemTable::Metrics.schema();
    let value_field = table_schema.field_with_name("value")?;
    let metadata_schema = Arc::new(Schema::new(
        table_schema
            .fields()
            .iter()
            .filter(|field| field.name() != value_field.name())
            .cloned()
            .collect::<Vec<_>>(),
    ));
    let metadata_batch =
        ArrowSerializer::build_record_batch_with_schema(&metadata, metadata_schema)?;
    let mut values = VariantArrayBuilder::new(rows.len());
    for row in rows {
        append_metric_value(&mut values, row.value);
    }
    let values: StructArray = values.build().into();
    let values = cast_array_recursively(&(Arc::new(values) as ArrayRef), value_field.data_type())?;
    let mut metadata_columns = metadata_batch.columns().iter();
    let columns = table_schema
        .fields()
        .iter()
        .map(|field| {
            if field.name() == value_field.name() {
                Ok(values.clone())
            } else {
                metadata_columns.next().cloned().ok_or_else(|| {
                    internal_datafusion_err!("missing metric metadata column: {}", field.name())
                })
            }
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(table_schema, columns).map_err(Into::into)
}

fn append_metric_value(values: &mut VariantArrayBuilder, value: MetricValue) {
    let mut metric = values.new_object();
    match value {
        MetricValue::Count(MetricNumber::Integer(value)) => metric.insert("count", value),
        MetricValue::Count(MetricNumber::Float(value)) => metric.insert("count", value),
        MetricValue::Gauge(MetricNumber::Integer(value)) => metric.insert("gauge", value),
        MetricValue::Gauge(MetricNumber::Float(value)) => metric.insert("gauge", value),
        MetricValue::Histogram(histogram) => {
            let mut value = metric.new_object("histogram");
            value.insert("count", histogram.count);
            value.insert("sum", histogram.sum.map_or(Variant::Null, Variant::from));
            value.insert("min", histogram.min.map_or(Variant::Null, Variant::from));
            value.insert("max", histogram.max.map_or(Variant::Null, Variant::from));

            let mut bucket_counts = value.new_list("bucket_counts");
            bucket_counts.extend(histogram.bucket_counts);
            bucket_counts.finish();

            let mut explicit_bounds = value.new_list("explicit_bounds");
            explicit_bounds.extend(histogram.explicit_bounds);
            explicit_bounds.finish();

            value.finish();
        }
    }
    metric.finish();
}
