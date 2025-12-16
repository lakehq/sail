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

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, BooleanArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_physical_expr::{Distribution, EquivalenceProperties};
use futures::stream::StreamExt;
use futures::TryStreamExt;
use url::Url;

use crate::kernel::models::Add;
use crate::storage::{get_object_store_from_context, StorageConfig};
use crate::table::open_table_with_object_store;

/// An ExecutionPlan that converts a stream of touched file paths into a stream of JSON-serialized
/// Delta `Add` actions.
#[derive(Debug)]
pub struct DeltaFileLookupExec {
    input: Arc<dyn ExecutionPlan>,
    table_url: Url,
    version: i64,
    cache: PlanProperties,
}

impl DeltaFileLookupExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, table_url: Url, version: i64) -> Self {
        let schema = Self::output_schema();
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            input,
            table_url,
            version,
            cache,
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn table_url(&self) -> &Url {
        &self.table_url
    }

    pub fn version(&self) -> i64 {
        self.version
    }

    fn output_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("add", DataType::Utf8, true),
            Field::new("partition_scan", DataType::Boolean, false),
        ]))
    }
}

impl DisplayAs for DeltaFileLookupExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "DeltaFileLookupExec(table_url={}, version={})",
                self.table_url, self.version
            ),
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: delta")?;
                write!(f, "table_url={}, version={}", self.table_url, self.version)
            }
        }
    }
}

#[async_trait]
impl ExecutionPlan for DeltaFileLookupExec {
    fn name(&self) -> &'static str {
        "DeltaFileLookupExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("DeltaFileLookupExec requires exactly one child");
        }
        Ok(Arc::new(DeltaFileLookupExec::new(
            children[0].clone(),
            self.table_url.clone(),
            self.version,
        )))
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        // We intentionally require single partition so we can de-duplicate paths globally and
        // avoid re-loading the snapshot multiple times.
        vec![Distribution::SinglePartition]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("DeltaFileLookupExec only supports single partition");
        }

        struct LookupState {
            input_stream: SendableRecordBatchStream,
            context: Arc<TaskContext>,
            table_url: Url,
            version: i64,
            file_map: Option<Arc<HashMap<String, Add>>>,
            seen: HashSet<String>,
            out_schema: SchemaRef,
        }

        let input_stream = self.input.execute(0, context.clone())?;
        let out_schema = Self::output_schema();

        let state = LookupState {
            input_stream,
            context,
            table_url: self.table_url.clone(),
            version: self.version,
            file_map: None,
            seen: HashSet::new(),
            out_schema: out_schema.clone(),
        };

        let stream = futures::stream::unfold(state, |mut st| async move {
            let file_map = if let Some(ref file_map) = st.file_map {
                file_map.clone()
            } else {
                let object_store = match get_object_store_from_context(&st.context, &st.table_url) {
                    Ok(s) => s,
                    Err(e) => return Some((Err(e), st)),
                };

                let mut table = match open_table_with_object_store(
                    st.table_url.clone(),
                    object_store,
                    StorageConfig,
                )
                .await
                {
                    Ok(t) => t,
                    Err(e) => return Some((Err(DataFusionError::External(Box::new(e))), st)),
                };

                if let Err(e) = table.load_version(st.version).await {
                    return Some((Err(DataFusionError::External(Box::new(e))), st));
                }

                let snapshot = match table.snapshot() {
                    Ok(s) => s.clone(),
                    Err(e) => return Some((Err(DataFusionError::External(Box::new(e))), st)),
                };

                let mut files = snapshot.snapshot().files(table.log_store().as_ref(), None);
                let mut map: HashMap<String, Add> = HashMap::new();
                while let Some(view) = match files.try_next().await {
                    Ok(v) => v,
                    Err(e) => return Some((Err(DataFusionError::External(Box::new(e))), st)),
                } {
                    map.insert(view.path().as_ref().to_string(), view.add_action());
                }

                let file_map = Arc::new(map);
                st.file_map = Some(file_map.clone());
                file_map
            };

            loop {
                let batch = match st.input_stream.next().await {
                    None => return None,
                    Some(r) => match r {
                        Ok(b) => b,
                        Err(e) => return Some((Err(e), st)),
                    },
                };

                if batch.num_rows() == 0 || batch.num_columns() == 0 {
                    continue;
                }

                let paths = match batch.column(0).as_any().downcast_ref::<StringArray>() {
                    Some(a) => a,
                    None => {
                        return Some((
                            Err(DataFusionError::Plan(
                                "Touched file plan must yield a Utf8 path column".to_string(),
                            )),
                            st,
                        ))
                    }
                };

                let mut out_adds: Vec<Option<String>> = Vec::new();
                for path in paths.iter().flatten() {
                    if !st.seen.insert(path.to_string()) {
                        continue;
                    }
                    if let Some(add) = file_map.get(path) {
                        match serde_json::to_string(add) {
                            Ok(s) => out_adds.push(Some(s)),
                            Err(e) => {
                                return Some((Err(DataFusionError::External(Box::new(e))), st))
                            }
                        }
                    }
                }

                if out_adds.is_empty() {
                    continue;
                }

                let add_array = Arc::new(StringArray::from(out_adds));
                let partition_scan = Arc::new(BooleanArray::from(vec![false; add_array.len()]));
                let out_batch = match RecordBatch::try_new(
                    st.out_schema.clone(),
                    vec![add_array, partition_scan],
                ) {
                    Ok(b) => b,
                    Err(e) => {
                        return Some((Err(DataFusionError::ArrowError(Box::new(e), None)), st))
                    }
                };
                return Some((Ok(out_batch), st));
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(out_schema, stream)))
    }
}
