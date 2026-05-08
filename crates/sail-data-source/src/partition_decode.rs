use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringArray, StringBuilder};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::{exec_err, DataFusionError, Result};
use futures::StreamExt;

#[derive(Clone)]
pub struct PartitionDecodeTableProvider {
    inner: Arc<dyn TableProvider>,
    partition_columns: Arc<[String]>,
}

impl PartitionDecodeTableProvider {
    pub fn try_new(
        inner: Arc<dyn TableProvider>,
        partition_columns: Vec<String>,
    ) -> Result<Arc<dyn TableProvider>> {
        if partition_columns.is_empty() {
            return Ok(inner);
        }
        let schema = inner.schema();
        let mut partition_columns_to_decode = Vec::with_capacity(partition_columns.len());
        for column in &partition_columns {
            let index = schema.index_of(column)?;
            if matches!(schema.field(index).data_type(), DataType::Utf8) {
                partition_columns_to_decode.push(column.clone());
            }
        }
        if partition_columns_to_decode.is_empty() {
            return Ok(inner);
        }
        Ok(Arc::new(Self {
            inner,
            partition_columns: partition_columns_to_decode.into(),
        }))
    }

    fn projected_partition_indices(&self, projection: Option<&Vec<usize>>) -> Result<Vec<usize>> {
        let schema = self.inner.schema();
        let partition_indices = self
            .partition_columns
            .iter()
            .map(|column| schema.index_of(column).map_err(DataFusionError::from))
            .collect::<Result<Vec<_>>>()?;
        let output = match projection {
            Some(projection) => projection
                .iter()
                .enumerate()
                .filter_map(|(output_index, input_index)| {
                    partition_indices
                        .contains(input_index)
                        .then_some(output_index)
                })
                .collect(),
            None => partition_indices,
        };
        Ok(output)
    }
}

impl Debug for PartitionDecodeTableProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionDecodeTableProvider")
            .field("partition_columns", &self.partition_columns)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for PartitionDecodeTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let partition_indices = self.projected_partition_indices(projection)?;
        let input = self.inner.scan(state, projection, &[], None).await?;
        if partition_indices.is_empty() {
            return Ok(input);
        }
        Ok(Arc::new(PartitionDecodeExec::new(input, partition_indices)))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }
}

#[derive(Clone, Debug)]
struct PartitionDecodeExec {
    input: Arc<dyn ExecutionPlan>,
    partition_indices: Arc<[usize]>,
    properties: Arc<PlanProperties>,
}

impl PartitionDecodeExec {
    fn new(input: Arc<dyn ExecutionPlan>, partition_indices: Vec<usize>) -> Self {
        let properties = Arc::new(input.properties().as_ref().clone());
        Self {
            input,
            partition_indices: partition_indices.into(),
            properties,
        }
    }

    fn decode_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let mut columns = batch.columns().to_vec();
        for index in self.partition_indices.iter().copied() {
            let array = columns.get(index).ok_or_else(|| {
                DataFusionError::Internal(format!("partition column index {index} out of range"))
            })?;
            columns[index] = decode_string_array(array)?;
        }
        RecordBatch::try_new(batch.schema(), columns).map_err(Into::into)
    }
}

impl DisplayAs for PartitionDecodeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "PartitionDecodeExec")
    }
}

impl ExecutionPlan for PartitionDecodeExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
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
        if children.len() != 1 {
            return exec_err!("PartitionDecodeExec expects one child");
        }
        let input = children.remove(0);
        Ok(Arc::new(Self::new(
            input,
            self.partition_indices.iter().copied().collect(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::execution::SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let schema = self.schema();
        let exec = self.clone();
        let stream = input.map(move |batch| batch.and_then(|batch| exec.decode_batch(batch)));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

fn decode_string_array(array: &ArrayRef) -> Result<ArrayRef> {
    let array = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Internal("expected Utf8 partition array".to_string()))?;
    let mut builder = StringBuilder::with_capacity(array.len(), array.get_buffer_memory_size());
    for index in 0..array.len() {
        if array.is_null(index) {
            builder.append_null();
        } else {
            builder.append_value(unescape_partition_path_name(array.value(index))?);
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn unescape_partition_path_name(value: &str) -> Result<String> {
    let bytes = value.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            let hex = &value[index + 1..index + 3];
            if let Ok(byte) = u8::from_str_radix(hex, 16) {
                output.push(byte);
                index += 3;
                continue;
            }
        }
        output.push(bytes[index]);
        index += 1;
    }
    String::from_utf8(output).map_err(|e| {
        DataFusionError::Execution(format!(
            "invalid UTF-8 partition path segment '{value}': {e}"
        ))
    })
}
