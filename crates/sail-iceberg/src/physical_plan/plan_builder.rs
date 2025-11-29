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

use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use sail_common_datafusion::datasource::PhysicalSinkMode;
use url::Url;

use crate::options::TableIcebergOptions;
use crate::physical_plan::writer_exec::IcebergWriterExec;

pub struct IcebergTableConfig {
    pub table_url: Url,
    pub partition_columns: Vec<String>,
    pub table_exists: bool,
    pub options: TableIcebergOptions,
}

pub struct IcebergPlanBuilder<'a> {
    input: Arc<dyn ExecutionPlan>,
    table_config: IcebergTableConfig,
    sink_mode: PhysicalSinkMode,
    sort_order: Option<Vec<PhysicalSortExpr>>,
    #[allow(unused)]
    session: &'a dyn Session,
}

impl<'a> IcebergPlanBuilder<'a> {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_config: IcebergTableConfig,
        sink_mode: PhysicalSinkMode,
        sort_order: Option<Vec<PhysicalSortExpr>>,
        session: &'a dyn Session,
    ) -> Self {
        Self {
            input,
            table_config,
            sink_mode,
            sort_order,
            session,
        }
    }

    pub async fn build(self) -> Result<Arc<dyn ExecutionPlan>> {
        self.add_projection_node(self.input.clone())
            .and_then(|plan| self.add_repartition_node(plan))
            .and_then(|plan| self.add_sort_node(plan))
            .and_then(|plan| self.add_writer_node(plan))
            .and_then(|plan| self.add_commit_node(plan))
    }

    fn add_projection_node(&self, input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let input_schema = input.schema();
        let mut projection_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();
        let mut part_idx = std::collections::HashMap::new();
        let part_set: std::collections::HashSet<&String> =
            self.table_config.partition_columns.iter().collect();

        for (i, f) in input_schema.fields().iter().enumerate() {
            if part_set.contains(f.name()) {
                part_idx.insert(f.name().clone(), i);
            } else {
                projection_exprs.push((Arc::new(Column::new(f.name(), i)), f.name().clone()));
            }
        }

        for name in &self.table_config.partition_columns {
            let idx = *part_idx.get(name).ok_or_else(|| {
                datafusion::common::DataFusionError::Plan(format!(
                    "Partition column '{}' not found in schema",
                    name
                ))
            })?;
            projection_exprs.push((Arc::new(Column::new(name, idx)), name.clone()));
        }

        Ok(Arc::new(ProjectionExec::try_new(projection_exprs, input)?))
    }

    fn add_repartition_node(
        &self,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let repartitioning = if self.table_config.partition_columns.is_empty() {
            Partitioning::RoundRobinBatch(4)
        } else {
            let schema = input.schema();
            let n = schema.fields().len();
            let k = self.table_config.partition_columns.len();
            let exprs: Vec<Arc<dyn PhysicalExpr>> = (n - k..n)
                .zip(self.table_config.partition_columns.iter())
                .map(|(idx, name)| Arc::new(Column::new(name, idx)) as Arc<dyn PhysicalExpr>)
                .collect();
            Partitioning::Hash(exprs, 4)
        };

        Ok(Arc::new(RepartitionExec::try_new(input, repartitioning)?))
    }

    fn add_sort_node(&self, input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(sort_exprs) = self.sort_order.clone() {
            let lex = LexOrdering::new(sort_exprs).ok_or_else(|| {
                datafusion::common::DataFusionError::Internal("Invalid sort order".to_string())
            })?;
            Ok(Arc::new(SortExec::new(lex, input)))
        } else {
            Ok(input)
        }
    }

    fn add_writer_node(&self, input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(IcebergWriterExec::new(
            input,
            self.table_config.table_url.clone(),
            self.table_config.partition_columns.clone(),
            self.sink_mode.clone(),
            self.table_config.table_exists,
            self.table_config.options.clone(),
        )))
    }

    fn add_commit_node(&self, input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(
            crate::physical_plan::commit::commit_exec::IcebergCommitExec::new(
                input,
                self.table_config.table_url.clone(),
            ),
        ))
    }
}
