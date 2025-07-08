use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::sink::DataSink;
use datafusion::datasource::MemTable;
use datafusion::execution::context::{SessionContext, TaskContext};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, SendableRecordBatchStream};
use datafusion_common::Result;
use futures::{StreamExt, TryStreamExt};
use sail_delta_lake::{open_table_with_object_store, WriteBuilder, SaveMode};

/// Delta Lake data sink implementation
#[derive(Debug)]
pub struct DeltaDataSink {
    options: HashMap<String, String>,
    object_store_url: datafusion::datasource::object_store::ObjectStoreUrl,
    table_paths: Vec<datafusion::datasource::listing::ListingTableUrl>,
    schema: SchemaRef,
}

impl DeltaDataSink {
    /// Create a new DeltaDataSink
    pub fn new(
        options: HashMap<String, String>,
        object_store_url: datafusion::datasource::object_store::ObjectStoreUrl,
        table_paths: Vec<datafusion::datasource::listing::ListingTableUrl>,
    ) -> Self {
        Self {
            options,
            object_store_url,
            table_paths,
            // TODO: get the correct schema from Delta Lake table
            schema: Arc::new(datafusion::arrow::datatypes::Schema::empty()),
        }
    }
}

impl DisplayAs for DeltaDataSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DeltaDataSink(table_paths={:?})", self.table_paths)
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: delta")?;
                write!(f, "table_paths={:?}", self.table_paths)
            }
        }
    }
}

#[async_trait]
impl DataSink for DeltaDataSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<u64> {
        // 1. collect all record batches
        let batches: Vec<_> = data
            .try_collect()
            .await
            .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;

        if batches.is_empty() {
            return Ok(0);
        }

        // 2. calculate total rows
        let total_rows: u64 = batches.iter().map(|batch| batch.num_rows() as u64).sum();

        // 3. get schema and create MemTable
        let schema = batches[0].schema();
        let mem_table = MemTable::try_new(schema.clone(), vec![batches])
            .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;

        // 4. create SessionContext and DataFrame
        // create a new SessionContext from TaskContext
        let ctx = SessionContext::new();
        let dataframe = ctx
            .read_table(Arc::new(mem_table))
            .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;

        // 5. get table path (use the first path)
        let table_path = self.table_paths.first()
            .ok_or_else(|| datafusion_common::DataFusionError::Plan("No table path provided".to_string()))?
            .as_str();

        // 6. open or create Delta table
        // TODO: here we need to get the correct ObjectStore instance
        // return an error for now, need more complete implementation
        // the complete implementation needs:
        // 1. get ObjectStore from context
        // 2. open table with open_table_with_object_store
        // 3. create WriteBuilder and execute write
        Err(datafusion_common::DataFusionError::Plan(
            "Delta table write implementation needs ObjectStore integration".to_string()
        ))
    }
}
