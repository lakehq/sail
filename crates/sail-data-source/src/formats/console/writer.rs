use std::io::Write;
use std::sync::Arc;

use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::{
    ArrayFormatter as ArrowArrayFormatter, ArrayFormatterFactory, DisplayIndex,
    FormatOptions as ArrowFormatOptions, FormatResult,
};
use datafusion::arrow::util::pretty::pretty_format_batches_with_options;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use datafusion_common::{Result, plan_err};
use futures::StreamExt;
use sail_common_datafusion::array::record_batch::retag_record_batch_timestamp_timezone;
use sail_common_datafusion::display::{
    ArrayFormatter as SailArrayFormatter, FormatOptions as SailFormatOptions,
};

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
    let batch = retag_record_batch_timestamp_timezone(batch, session_timezone)?;
    let factory = ConsoleFormatterFactory;
    let options = ArrowFormatOptions::default().with_formatter_factory(Some(&factory));
    Ok(pretty_format_batches_with_options(&[batch], &options)?.to_string())
}

#[derive(Debug)]
struct ConsoleFormatterFactory;

impl ArrayFormatterFactory for ConsoleFormatterFactory {
    fn create_array_formatter<'a>(
        &self,
        array: &'a dyn Array,
        options: &ArrowFormatOptions<'a>,
        _field: Option<&'a datafusion::arrow::datatypes::Field>,
    ) -> Result<Option<ArrowArrayFormatter<'a>>, ArrowError> {
        let sail_options = SailFormatOptions::default().with_null("");
        let formatter = SailArrayFormatter::try_new(array, &sail_options)?;
        Ok(Some(ArrowArrayFormatter::new(
            Box::new(ConsoleDisplayIndex(formatter)),
            options.safe(),
        )))
    }
}

struct ConsoleDisplayIndex<'a>(SailArrayFormatter<'a>);

impl DisplayIndex for ConsoleDisplayIndex<'_> {
    fn write(&self, idx: usize, output: &mut dyn std::fmt::Write) -> FormatResult {
        self.0.value(idx).write(output).map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Int32Array, TimestampMicrosecondArray};
    use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};

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

    #[test]
    fn preserves_empty_null_cells() -> Result<()> {
        let batch = RecordBatch::try_from_iter(vec![(
            "i",
            Arc::new(Int32Array::from(vec![None, Some(1)])) as _,
        )])?;

        assert_eq!(
            format_console_batch(&batch, "UTC")?,
            "+---+\n\
             | i |\n\
             +---+\n\
             |   |\n\
             | 1 |\n\
             +---+"
        );
        Ok(())
    }
}
