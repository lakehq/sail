mod options;
mod reader;

use std::ops::Deref;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::catalog::{Session, TableProvider};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{plan_err, Result};
use sail_common_datafusion::datasource::{SinkInfo, SourceInfo, TableFormat};
use sail_common_datafusion::streaming::source::StreamSourceTableProvider;

use crate::formats::rate::options::resolve_rate_read_options;
pub use crate::formats::rate::options::TableRateOptions;
pub use crate::formats::rate::reader::RateSourceExec;
use crate::formats::rate::reader::RateStreamSource;

/// Generate record batches at a fixed rate for testing purposes.
/// The record batches contain two columns, a timestamp and an integer value.
#[derive(Debug)]
pub struct RateTableFormat;

#[async_trait]
impl TableFormat for RateTableFormat {
    fn name(&self) -> &str {
        "rate"
    }

    async fn create_provider(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableProvider>> {
        let SourceInfo {
            paths: _,
            schema,
            constraints,
            partition_by,
            bucket_by,
            sort_order,
            options,
        } = info;
        if !constraints.deref().is_empty() {
            return plan_err!("the rate table format does not support constraints");
        }
        if !partition_by.is_empty() {
            return plan_err!("the rate table format does not support partitioning");
        }
        if bucket_by.is_some() || !sort_order.is_empty() {
            return plan_err!("the rate table format does not support bucketing");
        }

        let schema = match schema {
            Some(schema) if !schema.fields.is_empty() => schema,
            _ => {
                let tz = Arc::from(
                    ctx.config()
                        .options()
                        .execution
                        .time_zone
                        .clone()
                        .unwrap_or_else(|| "UTC".to_string()),
                );
                Schema::new(vec![
                    Arc::new(Field::new(
                        "timestamp",
                        DataType::Timestamp(TimeUnit::Microsecond, Some(tz)),
                        true,
                    )),
                    Arc::new(Field::new("value", DataType::Int64, true)),
                ])
            }
        };
        let options = resolve_rate_read_options(options)?;
        let source = RateStreamSource::try_new(options, Arc::new(schema))?;
        Ok(Arc::new(StreamSourceTableProvider::new(Arc::new(source))))
    }

    async fn create_writer(
        &self,
        _ctx: &dyn Session,
        _info: SinkInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan_err!("the rate table format does not support writing")
    }
}
