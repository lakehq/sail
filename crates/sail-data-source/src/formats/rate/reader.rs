use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use datafusion::arrow::array::{Int64Array, RecordBatch, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use datafusion_common::{arrow_datafusion_err, plan_err, DataFusionError, Result};
use futures::Stream;
use rand::Rng;

use crate::formats::rate::options::TableRateOptions;

#[derive(Debug, Clone)]
pub struct RateTableProvider {
    options: TableRateOptions,
    schema: SchemaRef,
}

impl RateTableProvider {
    pub fn try_new(options: TableRateOptions, schema: SchemaRef) -> Result<Self> {
        Self::validate_schema(&schema)?;
        Ok(Self { options, schema })
    }

    fn validate_schema(schema: &Schema) -> Result<()> {
        match schema.fields.iter().as_slice() {
            [t, v] => {
                if !matches!(
                    t.data_type(),
                    DataType::Timestamp(TimeUnit::Microsecond, Some(_tz))
                ) {
                    plan_err!("invalid timestamp type for rate table")
                } else if !matches!(v.data_type(), DataType::Int64) {
                    plan_err!("invalid value type for rate table")
                } else {
                    Ok(())
                }
            }
            _ => {
                plan_err!("invalid schema for rate table")
            }
        }
    }
}

#[async_trait]
impl TableProvider for RateTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(RateSourceExec::try_new(
            self.options.clone(),
            Arc::clone(&self.schema),
        )?))
    }
}

#[derive(Debug)]
pub struct RateSourceExec {
    options: TableRateOptions,
    time_zone: Arc<str>,
    properties: PlanProperties,
}

impl RateSourceExec {
    pub fn try_new(options: TableRateOptions, schema: SchemaRef) -> Result<Self> {
        let time_zone = Self::infer_time_zone(&schema)?;
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(options.num_partitions),
            EmissionType::Both,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        );
        Ok(Self {
            options,
            time_zone,
            properties,
        })
    }

    pub fn options(&self) -> &TableRateOptions {
        &self.options
    }

    fn infer_time_zone(schema: &Schema) -> Result<Arc<str>> {
        match schema.fields.iter().as_slice() {
            [t, _] => {
                if let DataType::Timestamp(_, Some(tz)) = t.data_type() {
                    Ok(Arc::clone(tz))
                } else {
                    plan_err!("invalid timestamp type for rate table schema")
                }
            }
            _ => {
                plan_err!("invalid schema for rate table")
            }
        }
    }
}

impl DisplayAs for RateSourceExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "{}: rows_per_second={}, num_partitions={}",
            self.name(),
            self.options.rows_per_second,
            self.options.num_partitions
        )
    }
}

impl ExecutionPlan for RateSourceExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            plan_err!("{} cannot have children", self.name())
        } else {
            Ok(self)
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition >= self.options.num_partitions {
            return plan_err!(
                "invalid partition {} for {} with {} partition(s)",
                partition,
                self.name(),
                self.options.num_partitions
            );
        }
        let output: Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>> =
            if self.options.rows_per_second == 0 {
                let output = futures::stream::unfold((), |()| async move {
                    tokio::time::sleep(Duration::MAX).await;
                    None
                });
                Box::pin(output)
            } else {
                let rows_per_second_per_partition =
                    (self.options.rows_per_second / self.options.num_partitions).max(1);
                let resolution = rows_per_second_per_partition.min(1_000);
                let batch_size = rows_per_second_per_partition / resolution;
                let interval = Duration::from_secs(1) / (resolution as u32);
                let output = futures::stream::unfold(
                    (self.schema(), Arc::clone(&self.time_zone)),
                    move |(schema, tz)| async move {
                        tokio::time::sleep(interval).await;
                        let mut rng = rand::rng();
                        let timestamp = chrono::Utc::now().timestamp_micros();
                        let values = (0..batch_size).map(|_| rng.random()).collect::<Vec<i64>>();
                        let batch = RecordBatch::try_new(
                            schema.clone(),
                            vec![
                                Arc::new(
                                    TimestampMicrosecondArray::from(vec![timestamp; batch_size])
                                        .with_timezone(Arc::clone(&tz)),
                                ),
                                Arc::new(Int64Array::from(values)),
                            ],
                        )
                        .map_err(|e| arrow_datafusion_err!(e));
                        Some((batch, (schema, tz)))
                    },
                );
                Box::pin(output)
            };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            output,
        )))
    }
}
