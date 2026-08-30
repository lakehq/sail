use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, RecordBatch, StructArray};
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::common::{Result, internal_datafusion_err};
use parquet_variant_compute::VariantArrayBuilder;
use parquet_variant_json::JsonToVariant;
use sail_common_datafusion::array::record_batch::cast_record_batch_relaxed_tz;
use sail_common_datafusion::array::serde::ArrowSerializer;
use sail_common_datafusion::system::catalog::SystemTable;
use sail_common_datafusion::system::predicate::{MapValueFilter, TimestampMicros, ValueFilter};
use sail_common_datafusion::system::reader::read_ordered_map;
use sail_common_datafusion::{candidate_key_bound, candidate_set};
use serde::{Deserialize, Serialize};

use super::SystemEventActor;
use crate::system_event::{
    JobPrimaryKey, OptionPrimaryKey, SessionPrimaryKey, StagePrimaryKey, TaskPrimaryKey,
    WorkerPrimaryKey,
};

candidate_key_bound! {
    JobPrimaryKey => JobPrimaryKeyBound {
        session_id: String,
        job_id: u64,
    }
}

candidate_key_bound! {
    StagePrimaryKey => StagePrimaryKeyBound {
        session_id: String,
        job_id: u64,
        stage: u64,
    }
}

candidate_key_bound! {
    TaskPrimaryKey => TaskPrimaryKeyBound {
        session_id: String,
        job_id: u64,
        stage: u64,
        partition: u64,
        attempt: u64,
    }
}

candidate_key_bound! {
    OptionPrimaryKey => OptionPrimaryKeyBound {
        key: String,
    }
}

candidate_key_bound! {
    SessionPrimaryKey => SessionPrimaryKeyBound {
        session_id: String,
    }
}

candidate_key_bound! {
    WorkerPrimaryKey => WorkerPrimaryKeyBound {
        session_id: String,
        worker_id: u64,
    }
}

impl SystemEventActor {
    pub(super) fn read_metrics(
        &self,
        timestamp: ValueFilter<TimestampMicros>,
        name: ValueFilter<String>,
        attributes: Vec<MapValueFilter<String, String>>,
        fetch: usize,
    ) -> Result<RecordBatch> {
        #[derive(Serialize, Deserialize)]
        struct MetricMetadataRow {
            timestamp: chrono::DateTime<chrono::Utc>,
            name: String,
            attributes: BTreeMap<String, String>,
        }

        let rows = self
            .store
            .metrics
            .scan(timestamp, name, attributes, fetch)?;
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

    pub(super) fn read_jobs(
        &self,
        session_id: ValueFilter<String>,
        job_id: ValueFilter<u64>,
        fetch: usize,
    ) -> Result<RecordBatch> {
        let candidates = candidate_set! {
            JobPrimaryKey => JobPrimaryKeyBound {
                session_id: String => &session_id.domain,
                job_id: u64 => &job_id.domain,
            }
        };
        let rows = read_ordered_map(
            &self.store.jobs,
            candidates,
            |row| Ok((session_id.predicate)(&row.session_id)? && (job_id.predicate)(&row.job_id)?),
            fetch,
        )?;
        Self::build_batch(SystemTable::Jobs, rows)
    }

    pub(super) fn read_stages(
        &self,
        session_id: ValueFilter<String>,
        job_id: ValueFilter<u64>,
        stage: ValueFilter<u64>,
        fetch: usize,
    ) -> Result<RecordBatch> {
        let candidates = candidate_set! {
            StagePrimaryKey => StagePrimaryKeyBound {
                session_id: String => &session_id.domain,
                job_id: u64 => &job_id.domain,
                stage: u64 => &stage.domain,
            }
        };
        let rows = read_ordered_map(
            &self.store.stages,
            candidates,
            |row| {
                Ok((session_id.predicate)(&row.session_id)?
                    && (job_id.predicate)(&row.job_id)?
                    && (stage.predicate)(&row.stage)?)
            },
            fetch,
        )?;
        Self::build_batch(SystemTable::Stages, rows)
    }

    pub(super) fn read_tasks(
        &self,
        session_id: ValueFilter<String>,
        job_id: ValueFilter<u64>,
        stage: ValueFilter<u64>,
        partition: ValueFilter<u64>,
        attempt: ValueFilter<u64>,
        fetch: usize,
    ) -> Result<RecordBatch> {
        let candidates = candidate_set! {
            TaskPrimaryKey => TaskPrimaryKeyBound {
                session_id: String => &session_id.domain,
                job_id: u64 => &job_id.domain,
                stage: u64 => &stage.domain,
                partition: u64 => &partition.domain,
                attempt: u64 => &attempt.domain,
            }
        };
        let rows = read_ordered_map(
            &self.store.tasks,
            candidates,
            |row| {
                Ok((session_id.predicate)(&row.session_id)?
                    && (job_id.predicate)(&row.job_id)?
                    && (stage.predicate)(&row.stage)?
                    && (partition.predicate)(&row.partition)?
                    && (attempt.predicate)(&row.attempt)?)
            },
            fetch,
        )?;
        Self::build_batch(SystemTable::Tasks, rows)
    }

    pub(super) fn read_options(
        &self,
        key: ValueFilter<String>,
        fetch: usize,
    ) -> Result<RecordBatch> {
        let candidates = candidate_set! {
            OptionPrimaryKey => OptionPrimaryKeyBound {
                key: String => &key.domain,
            }
        };
        let rows = read_ordered_map(
            &self.store.options,
            candidates,
            |row| (key.predicate)(&row.key),
            fetch,
        )?;
        Self::build_batch(SystemTable::Options, rows)
    }

    pub(super) fn read_sessions(
        &self,
        session_id: ValueFilter<String>,
        fetch: usize,
    ) -> Result<RecordBatch> {
        let candidates = candidate_set! {
            SessionPrimaryKey => SessionPrimaryKeyBound {
                session_id: String => &session_id.domain,
            }
        };
        let rows = read_ordered_map(
            &self.store.sessions,
            candidates,
            |row| (session_id.predicate)(&row.session_id),
            fetch,
        )?;
        Self::build_batch(SystemTable::Sessions, rows)
    }

    pub(super) fn read_workers(
        &self,
        session_id: ValueFilter<String>,
        worker_id: ValueFilter<u64>,
        fetch: usize,
    ) -> Result<RecordBatch> {
        let candidates = candidate_set! {
            WorkerPrimaryKey => WorkerPrimaryKeyBound {
                session_id: String => &session_id.domain,
                worker_id: u64 => &worker_id.domain,
            }
        };
        let rows = read_ordered_map(
            &self.store.workers,
            candidates,
            |row| {
                Ok((session_id.predicate)(&row.session_id)?
                    && (worker_id.predicate)(&row.worker_id)?)
            },
            fetch,
        )?;
        Self::build_batch(SystemTable::Workers, rows)
    }

    fn build_batch<T>(table: SystemTable, rows: Vec<T>) -> Result<RecordBatch>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        ArrowSerializer::build_record_batch_with_schema(&rows, table.schema())
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
    use opentelemetry_proto::tonic::metrics::v1::{
        Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point,
    };
    use sail_common_datafusion::system::catalog::SystemTable;
    use sail_common_datafusion::system::predicate::{Predicates, ValueFilter};

    use super::SystemEventActor;

    #[test]
    fn builds_metrics_record_batch_with_map_and_variant_columns() -> datafusion::common::Result<()>
    {
        let mut actor = SystemEventActor::default();
        actor.store.metrics.apply(vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "test.gauge".to_string(),
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            attributes: vec![KeyValue {
                                key: "worker".to_string(),
                                value: Some(AnyValue {
                                    value: Some(any_value::Value::StringValue("1".to_string())),
                                }),
                                key_strindex: 0,
                            }],
                            time_unix_nano: 1_000,
                            value: Some(number_data_point::Value::AsInt(42)),
                            ..Default::default()
                        }],
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }]);

        let batch = actor.read_metrics(
            ValueFilter::all(Predicates::always_true()),
            ValueFilter::all(Predicates::always_true()),
            vec![],
            usize::MAX,
        )?;

        assert_eq!(batch.schema(), SystemTable::Metrics.schema());
        assert_eq!(batch.num_rows(), 1);
        Ok(())
    }
}
