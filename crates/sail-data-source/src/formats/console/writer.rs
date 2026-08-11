use std::io::Write;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use datafusion_common::{Result, plan_err};
use futures::StreamExt;
use sail_common_datafusion::display::{ArrayFormatter, FormatOptions};

#[derive(Debug)]
pub struct ConsoleSinkExec {
    input: Arc<dyn ExecutionPlan>,
    session_timezone: Arc<str>,
    properties: Arc<PlanProperties>,
}

impl ConsoleSinkExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, session_timezone: Arc<str>) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::new(Schema::empty())),
            Partitioning::UnknownPartitioning(
                input.properties().output_partitioning().partition_count(),
            ),
            EmissionType::Final,
            // The node returns no data, so it is bounded.
            Boundedness::Bounded,
        ));
        Self {
            input,
            session_timezone,
            properties,
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn session_timezone(&self) -> &Arc<str> {
        &self.session_timezone
    }
}

impl DisplayAs for ConsoleSinkExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl ExecutionPlan for ConsoleSinkExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match (children.pop(), children.is_empty()) {
            (Some(child), true) => Ok(Arc::new(ConsoleSinkExec::new(
                child,
                Arc::clone(&self.session_timezone),
            ))),
            _ => plan_err!("{} should have exactly one child", self.name()),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = self.input.execute(partition, context)?;
        let session_timezone = Arc::clone(&self.session_timezone);
        let output = futures::stream::once(async move {
            stream
                .enumerate()
                .for_each(|(i, batch)| {
                    let session_timezone = Arc::clone(&session_timezone);
                    async move {
                        let text = match batch {
                            Ok(batch) => match format_console_batch(&batch, &session_timezone) {
                                Ok(batch) => batch,
                                Err(e) => {
                                    format!("error formatting batch: {e}")
                                }
                            },
                            Err(e) => {
                                format!("error: {e}")
                            }
                        };
                        let mut stdout = std::io::stdout().lock();
                        let _ = writeln!(stdout, "partition {partition} batch {i}");
                        let _ = writeln!(stdout, "{text}");
                    }
                })
                .await;
            futures::stream::empty()
        })
        .flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            output,
        )))
    }
}

fn format_console_batch(batch: &RecordBatch, session_timezone: &str) -> Result<String> {
    let options = FormatOptions::default().with_timestamp_timezone(Some(session_timezone));
    let columns = batch
        .columns()
        .iter()
        .map(|array| {
            let formatter = ArrayFormatter::try_new(array.as_ref(), &options)?;
            let values = (0..array.len())
                .map(|row| {
                    if array.is_null(row) {
                        Ok(None)
                    } else {
                        Ok(Some(formatter.value(row).try_to_string()?))
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(StringArray::from(values)) as ArrayRef)
        })
        .collect::<Result<Vec<_>>>()?;
    let schema = Arc::new(Schema::new(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| Field::new(field.name(), DataType::Utf8, field.is_nullable()))
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema, columns)?;
    Ok(pretty_format_batches(&[batch])?.to_string())
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::TimestampMicrosecondArray;
    use datafusion::arrow::datatypes::TimeUnit;

    use super::*;

    #[test]
    fn formats_ltz_in_session_timezone() -> Result<()> {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "t",
                DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
                false,
            )])),
            vec![Arc::new(
                TimestampMicrosecondArray::from(vec![-3_723_000_000]).with_timezone("UTC"),
            )],
        )?;

        assert_eq!(
            format_console_batch(&batch, "+01:02:03")?,
            "+---------------------+\n\
             | t                   |\n\
             +---------------------+\n\
             | 1970-01-01 00:00:00 |\n\
             +---------------------+"
        );
        Ok(())
    }
}
