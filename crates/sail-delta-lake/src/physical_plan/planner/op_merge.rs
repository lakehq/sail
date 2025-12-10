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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{BooleanArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::{DataFusionError, NullEquality};
use datafusion::datasource::memory::MemTable;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{
    BinaryExpr, CaseExpr, CastExpr, Column, InListExpr, IsNotNullExpr, Literal, NotExpr,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion::physical_plan::joins::{HashJoinExec, NestedLoopJoinExec, PartitionMode};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{JoinSide, JoinType, Result, ScalarValue};
use futures::TryStreamExt;
use sail_common_datafusion::datasource::{MergeInfo as PhysicalMergeInfo, PhysicalSinkMode};
use url::Url;

use super::context::PlannerContext;
use crate::datasource::{
    DataFusionMixins, DeltaScanConfigBuilder, DeltaTableProvider, PATH_COLUMN,
};
use crate::kernel::models::Add;
use crate::options::TableDeltaOptions;
use crate::physical_plan::{DeltaCommitExec, DeltaRemoveActionsExec, DeltaWriterExec};
use crate::storage::{LogStoreRef, StorageConfig};
use crate::table::{open_table_with_object_store, DeltaTableState};

pub async fn build_merge_plan(
    ctx: &PlannerContext<'_>,
    merge_info: PhysicalMergeInfo,
) -> Result<Arc<dyn ExecutionPlan>> {
    let builder = DeltaMergePlanBuilder::new(
        ctx.table_url().clone(),
        merge_info,
        ctx.session(),
        ctx.options().clone(),
    );
    builder.build().await
}

/// Builder for MERGE INTO execution plans.
pub struct DeltaMergePlanBuilder<'a> {
    table_url: Url,
    merge_info: PhysicalMergeInfo,
    session: &'a dyn Session,
    options: TableDeltaOptions,
}

const SOURCE_PRESENT_COLUMN: &str = "__sail_merge_source_row_present";

type PhysicalExprRef = Arc<dyn PhysicalExpr>;
type PhysicalExprPair = (PhysicalExprRef, PhysicalExprRef);
type ColumnCaseBranches = Vec<PhysicalExprPair>;
type ColumnCasesMap = HashMap<String, ColumnCaseBranches>;

/// DSL extension trait for building physical expressions with less boilerplate
trait PhysExprExt {
    fn and(self, other: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr>;
    fn or(self, other: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr>;
    fn not(self) -> Arc<dyn PhysicalExpr>;
    fn not_null(self) -> Arc<dyn PhysicalExpr>;
}

impl PhysExprExt for Arc<dyn PhysicalExpr> {
    fn and(self, other: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(self, Operator::And, other))
    }

    fn or(self, other: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(self, Operator::Or, other))
    }

    fn not(self) -> Arc<dyn PhysicalExpr> {
        Arc::new(NotExpr::new(self))
    }

    fn not_null(self) -> Arc<dyn PhysicalExpr> {
        Arc::new(IsNotNullExpr::new(self))
    }
}

fn lit_bool(val: bool) -> Arc<dyn PhysicalExpr> {
    Arc::new(Literal::new(ScalarValue::Boolean(Some(val))))
}

#[derive(Debug, Clone)]
struct JoinConditionAnalysis {
    join_keys: Vec<PhysicalExprPair>,
    residual_filter: Option<Arc<dyn PhysicalExpr>>,
    target_only_filters: Vec<Arc<dyn PhysicalExpr>>,
}

/// Context object that encapsulates all schema-related information for a MERGE operation.
struct MergeSchemaContext {
    target_physical_schema: SchemaRef,
    source_physical_schema: SchemaRef,
    table_schema: SchemaRef,
    /// Logical column name -> physical column name mapping
    logical_to_physical: HashMap<String, String>,
    /// Target column name -> index in target schema
    target_col_idx: HashMap<String, usize>,
    /// Source column name -> index in joined schema (offset by target length)
    source_col_idx: HashMap<String, usize>,
    /// Physical target column name -> index mapping (for logical_to_physical mapped names)
    physical_target_col_idx: HashMap<String, usize>,
    /// Index of PATH_COLUMN in target schema
    path_col_idx: usize,
    /// Index of SOURCE_PRESENT_COLUMN in joined schema
    source_present_col_idx: usize,
    /// Number of target columns
    num_target: usize,
}

impl MergeSchemaContext {
    fn new(
        target_physical_schema: SchemaRef,
        source_physical_schema: SchemaRef,
        table_schema: SchemaRef,
        logical_target_schema: SchemaRef,
    ) -> Result<Self> {
        let target_fields = target_physical_schema.fields();
        let source_fields = source_physical_schema.fields();
        let num_target = target_fields.len();

        // Build logical to physical mapping
        let mut logical_to_physical = HashMap::new();
        if logical_target_schema.fields().len() == table_schema.fields().len() {
            for (idx, logical_field) in logical_target_schema.fields().iter().enumerate() {
                let physical_field = table_schema.field(idx);
                logical_to_physical
                    .insert(logical_field.name().clone(), physical_field.name().clone());
            }
        }

        // Build target column index map
        let mut target_col_idx = HashMap::new();
        for (idx, field) in target_fields.iter().enumerate() {
            target_col_idx.insert(field.name().clone(), idx);
        }

        // Build physical target column index map
        let mut physical_target_col_idx = HashMap::new();
        for (logical_name, physical_name) in &logical_to_physical {
            if let Some(idx) = target_col_idx.get(logical_name) {
                physical_target_col_idx.insert(physical_name.clone(), *idx);
            }
        }

        // Build source column index map (offset by num_target for joined schema)
        let mut source_col_idx = HashMap::new();
        for (idx, field) in source_fields.iter().enumerate() {
            source_col_idx.insert(field.name().clone(), num_target + idx);
        }

        // Find PATH_COLUMN index
        let path_col_idx = target_fields
            .iter()
            .enumerate()
            .find(|(_, f)| f.name() == PATH_COLUMN)
            .map(|(idx, _)| idx)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Column '{}' not found in target schema",
                    PATH_COLUMN
                ))
            })?;

        // Find SOURCE_PRESENT_COLUMN index in joined schema
        let source_present_idx_in_source = source_fields
            .iter()
            .enumerate()
            .find(|(_, f)| f.name() == SOURCE_PRESENT_COLUMN)
            .map(|(idx, _)| idx)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Column '{}' not found in source schema",
                    SOURCE_PRESENT_COLUMN
                ))
            })?;
        let source_present_col_idx = num_target + source_present_idx_in_source;

        Ok(Self {
            target_physical_schema,
            source_physical_schema,
            table_schema,
            logical_to_physical,
            target_col_idx,
            source_col_idx,
            physical_target_col_idx,
            path_col_idx,
            source_present_col_idx,
            num_target,
        })
    }

    /// Get a physical column expression from the target side.
    /// Handles both direct column names and logical-to-physical column name mappings.
    fn target_col_expr(&self, name: &str, data_type: &DataType) -> Result<Arc<dyn PhysicalExpr>> {
        // Try physical_target_col_idx first (for mapped names), then target_col_idx
        if let Some(idx) = self
            .physical_target_col_idx
            .get(name)
            .copied()
            .or_else(|| self.target_col_idx.get(name).copied())
        {
            let target_name = self
                .target_physical_schema
                .fields()
                .get(idx)
                .map(|f| f.name().clone())
                .unwrap_or_else(|| name.to_string());
            Ok(Arc::new(Column::new(target_name.as_str(), idx)) as Arc<dyn PhysicalExpr>)
        } else {
            Ok(Arc::new(Literal::new(Self::typed_null(data_type)?)) as Arc<dyn PhysicalExpr>)
        }
    }

    fn typed_null(data_type: &DataType) -> Result<ScalarValue> {
        ScalarValue::try_from(data_type)
    }

    /// Get a physical column expression from the source side by ordinal position
    fn source_col_expr_by_idx(&self, idx: usize) -> Option<Arc<dyn PhysicalExpr>> {
        if idx < self.source_physical_schema.fields().len() {
            let field = &self.source_physical_schema.fields()[idx];
            Some(Arc::new(Column::new(field.name(), self.num_target + idx)) as Arc<dyn PhysicalExpr>)
        } else {
            None
        }
    }

    /// Get the PATH column expression
    fn path_col_expr(&self) -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new(PATH_COLUMN, self.path_col_idx)) as Arc<dyn PhysicalExpr>
    }

    /// Get the SOURCE_PRESENT column expression
    fn source_present_col_expr(&self) -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new(
            SOURCE_PRESENT_COLUMN,
            self.source_present_col_idx,
        )) as Arc<dyn PhysicalExpr>
    }

    /// Build commonly used predicates
    fn target_present_expr(&self) -> Arc<dyn PhysicalExpr> {
        self.path_col_expr().not_null()
    }

    fn source_present_expr(&self) -> Arc<dyn PhysicalExpr> {
        self.source_present_col_expr().not_null()
    }

    /// Align an expression's column references to the join schema
    fn align_expr(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        schema: &SchemaRef,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let schema = Arc::clone(schema);
        let target_col_idx = &self.target_col_idx;
        let source_col_idx = &self.source_col_idx;
        let logical_to_physical = &self.logical_to_physical;

        expr.clone()
            .transform_up(|e| {
                if let Some(col) = e.as_any().downcast_ref::<Column>() {
                    let mut target_name: Option<String> = None;
                    let name = col.name();
                    let mut new_index: Option<usize> = None;

                    if let Some(mapped) = logical_to_physical.get(name) {
                        target_name = Some(mapped.clone());
                        new_index = target_col_idx.get(mapped).cloned();
                    } else if let Some(idx) = target_col_idx.get(name) {
                        new_index = Some(*idx);
                    } else if let Some(idx) = source_col_idx.get(name) {
                        new_index = Some(*idx);
                    } else if let Some(idx) = schema.fields().iter().position(|f| f.name() == name)
                    {
                        new_index = Some(idx);
                        target_name = Some(schema.field(idx).name().clone());
                    }

                    if let Some(idx) = new_index {
                        let final_name = target_name.unwrap_or_else(|| name.to_string());
                        if idx != col.index() || final_name != name {
                            log::trace!(
                                "align_expr remap: name={} old_idx={} final_name={} new_idx={}",
                                name,
                                col.index(),
                                final_name.as_str(),
                                idx
                            );
                            let updated = Arc::new(Column::new(final_name.as_str(), idx))
                                as Arc<dyn PhysicalExpr>;
                            return Ok(Transformed::yes(updated));
                        }
                    }
                }
                Ok(Transformed::no(e))
            })
            .data()
    }
}

/// Helper struct to find files affected by a MERGE operation.
/// Encapsulates the logic for building a plan to identify touched files,
/// collecting file paths, and loading their Add actions.
struct AffectedFilesFinder<'a> {
    snapshot: &'a DeltaTableState,
    log_store: LogStoreRef,
    session: &'a dyn Session,
    merge_info: &'a PhysicalMergeInfo,
}

impl<'a> AffectedFilesFinder<'a> {
    fn new(
        snapshot: &'a DeltaTableState,
        log_store: LogStoreRef,
        session: &'a dyn Session,
        merge_info: &'a PhysicalMergeInfo,
    ) -> Self {
        Self {
            snapshot,
            log_store,
            session,
            merge_info,
        }
    }

    /// Find all files touched by the merge operation
    async fn find_touched_files(
        &self,
        join_plan: Arc<dyn ExecutionPlan>,
        ctx: &MergeSchemaContext,
    ) -> Result<Vec<Add>> {
        let touched_plan = self.build_touched_file_plan(join_plan, ctx)?;

        let touched_paths = if let Some(plan) = touched_plan {
            self.collect_touched_paths(plan).await?
        } else {
            HashSet::new()
        };

        self.load_add_actions_for_paths(&touched_paths).await
    }

    fn build_touched_file_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        ctx: &MergeSchemaContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let target_present = ctx.target_present_expr();
        let source_present = ctx.source_present_expr();

        let matched_pred = target_present.clone().and(source_present.clone());
        let not_matched_by_source_pred = target_present.clone().and(source_present.clone().not());

        let mut rewrite_pred: Option<Arc<dyn PhysicalExpr>> = None;
        let accumulate = |existing: &mut Option<Arc<dyn PhysicalExpr>>,
                          expr: Arc<dyn PhysicalExpr>| {
            *existing = Some(match existing.take() {
                Some(prev) => prev.or(expr),
                None => expr,
            });
        };

        if !self.merge_info.rewrite_matched_predicates.is_empty()
            || !self
                .merge_info
                .rewrite_not_matched_by_source_predicates
                .is_empty()
        {
            for pred in &self.merge_info.rewrite_matched_predicates {
                let aligned = ctx.align_expr(pred, &input.schema())?;
                accumulate(&mut rewrite_pred, matched_pred.clone().and(aligned));
            }
            for pred in &self.merge_info.rewrite_not_matched_by_source_predicates {
                let aligned = ctx.align_expr(pred, &input.schema())?;
                accumulate(
                    &mut rewrite_pred,
                    not_matched_by_source_pred.clone().and(aligned),
                );
            }
        } else {
            for clause in &self.merge_info.matched_clauses {
                let mut pred = matched_pred.clone();
                if let Some(cond) = &clause.condition {
                    pred = pred.and(Arc::clone(cond));
                }
                use sail_common_datafusion::datasource::MergeMatchedActionInfo as MMAI;
                match &clause.action {
                    MMAI::Delete | MMAI::UpdateAll | MMAI::UpdateSet(_) => {
                        accumulate(&mut rewrite_pred, pred);
                    }
                }
            }

            for clause in &self.merge_info.not_matched_by_source_clauses {
                let mut pred = not_matched_by_source_pred.clone();
                if let Some(cond) = &clause.condition {
                    pred = pred.and(Arc::clone(cond));
                }
                use sail_common_datafusion::datasource::MergeNotMatchedBySourceActionInfo as NMBAI;
                match &clause.action {
                    NMBAI::Delete | NMBAI::UpdateSet(_) => {
                        accumulate(&mut rewrite_pred, pred);
                    }
                }
            }
        }

        let rewrite_pred = match rewrite_pred {
            Some(pred) => pred,
            None => return Ok(None),
        };

        let filter = Arc::new(FilterExec::try_new(rewrite_pred, input)?);
        let base_path_col = ctx.path_col_expr();
        let projected_path = Arc::new(CastExpr::new(base_path_col.clone(), DataType::Utf8, None))
            as Arc<dyn PhysicalExpr>;
        let projection =
            ProjectionExec::try_new(vec![(projected_path, PATH_COLUMN.to_string())], filter)?;
        Ok(Some(Arc::new(projection)))
    }

    async fn collect_touched_paths(&self, plan: Arc<dyn ExecutionPlan>) -> Result<HashSet<String>> {
        let task_ctx = self.session.task_ctx();
        let batches = collect(plan, task_ctx).await?;
        let mut paths = HashSet::new();
        for batch in batches {
            if batch.num_columns() == 0 {
                continue;
            }
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Plan(
                        "Touched file plan must yield a Utf8 path column".to_string(),
                    )
                })?;
            for value in array.iter().flatten() {
                paths.insert(value.to_string());
            }
        }
        Ok(paths)
    }

    async fn load_add_actions_for_paths(&self, paths: &HashSet<String>) -> Result<Vec<Add>> {
        if paths.is_empty() {
            return Ok(Vec::new());
        }

        let mut stream = self
            .snapshot
            .snapshot()
            .files(self.log_store.as_ref(), None);
        let mut adds = Vec::new();
        while let Some(view) = stream
            .try_next()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
        {
            let add = view.add_action();
            if paths.contains(&add.path) {
                adds.push(add);
            }
        }
        Ok(adds)
    }
}

impl<'a> DeltaMergePlanBuilder<'a> {
    pub fn new(
        table_url: Url,
        merge_info: PhysicalMergeInfo,
        session: &'a dyn Session,
        options: TableDeltaOptions,
    ) -> Self {
        Self {
            table_url,
            merge_info,
            session,
            options,
        }
    }

    fn analyze_join_condition(&self) -> Result<JoinConditionAnalysis> {
        let num_target = self.merge_info.target_schema.fields().len();
        let num_source = self.merge_info.source_schema.fields().len();
        let mut join_keys = Vec::with_capacity(self.merge_info.join_keys.len());

        for (left, right) in &self.merge_info.join_keys {
            let left = Self::remap_expr_for_side(left, 0, num_target, num_target + num_source)?;
            let right =
                Self::remap_expr_for_side(right, num_target, num_source, num_target + num_source)?;
            join_keys.push((left, right));
        }

        Ok(JoinConditionAnalysis {
            residual_filter: self.merge_info.join_filter.clone(),
            target_only_filters: self.merge_info.target_only_filters.clone(),
            join_keys,
        })
    }

    fn remap_expr_for_side(
        expr: &Arc<dyn PhysicalExpr>,
        offset: usize,
        valid_len: usize,
        total_len: usize,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        expr.clone()
            .transform_up(|node| {
                if let Some(column) = node.as_any().downcast_ref::<Column>() {
                    if column.index() >= total_len {
                        return Err(DataFusionError::Plan(format!(
                            "Join condition references column index {} beyond schema with {} fields",
                            column.index(),
                            total_len
                        )));
                    }
                    if column.index() < offset || column.index() >= offset + valid_len {
                        return Err(DataFusionError::Plan(format!(
                            "Join condition column index {} is outside the bounds of the expected join input range [{}, {})",
                            column.index(),
                            offset,
                            offset + valid_len
                        )));
                    }
                    let updated_idx = column.index() - offset;
                    if updated_idx != column.index() {
                        let updated = Arc::new(Column::new(column.name(), updated_idx))
                            as Arc<dyn PhysicalExpr>;
                        return Ok(Transformed::yes(updated));
                    }
                }
                Ok(Transformed::no(node))
            })
            .data()
    }

    pub async fn build(self) -> Result<Arc<dyn ExecutionPlan>> {
        let object_store = self
            .session
            .runtime_env()
            .object_store_registry
            .get_store(&self.table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let storage_config = StorageConfig;
        let table =
            open_table_with_object_store(self.table_url.clone(), object_store, storage_config)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let log_store = table.log_store();
        let snapshot_state = table
            .snapshot()
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .clone();
        let table_schema = snapshot_state
            .snapshot()
            .arrow_schema()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partition_columns = snapshot_state.metadata().partition_columns().clone();

        // Enable schema evolution if requested by MERGE options.
        let mut options = self.options.clone();
        if self.merge_info.with_schema_evolution {
            options.merge_schema = true;
        }

        let join_analysis = self.analyze_join_condition()?;

        let target_plan = self
            .build_target_plan(
                snapshot_state.clone(),
                log_store.clone(),
                &join_analysis.target_only_filters,
            )
            .await?;
        let target_plan = self.augment_target_plan(target_plan)?;
        let source_plan = self.augment_source_plan(Arc::clone(&self.merge_info.source))?;
        let target_physical_schema = target_plan.schema();
        let source_physical_schema = source_plan.schema();

        let target_physical_fields: Vec<String> = target_physical_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let source_physical_fields: Vec<String> = source_physical_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        log::trace!(
            "merge_target_physical_fields: {:?}",
            &target_physical_fields
        );
        log::trace!(
            "merge_source_physical_fields: {:?}",
            &source_physical_fields
        );

        // Build schema context to encapsulate all schema-related logic
        let logical_target_schema =
            Arc::new(self.merge_info.target_schema.as_ref().as_arrow().clone());
        let ctx = MergeSchemaContext::new(
            Arc::clone(&target_physical_schema),
            Arc::clone(&source_physical_schema),
            table_schema.clone(),
            logical_target_schema,
        )?;

        let join_for_ident = self.build_join_plan(
            Arc::clone(&target_plan),
            Arc::clone(&source_plan),
            &join_analysis,
        )?;

        // Find affected files using the AffectedFilesFinder
        let finder = AffectedFilesFinder::new(
            &snapshot_state,
            log_store.clone(),
            self.session,
            &self.merge_info,
        );
        let touched_adds = finder
            .find_touched_files(Arc::clone(&join_for_ident), &ctx)
            .await?;

        // Extract touched paths for filtering
        let touched_paths: HashSet<String> =
            touched_adds.iter().map(|add| add.path.clone()).collect();

        let join_plan =
            self.build_join_plan(target_plan, Arc::clone(&source_plan), &join_analysis)?;

        let filtered = self.build_merge_row_filter(join_plan, &ctx)?;
        let filtered = self.filter_to_touched_rows(filtered, &ctx, &touched_paths)?;
        let projected = self.build_merge_projection(filtered, &ctx)?;

        let writer = Arc::new(DeltaWriterExec::new(
            Arc::clone(&projected),
            self.table_url.clone(),
            options,
            partition_columns.clone(),
            PhysicalSinkMode::Append,
            true,
            table_schema.clone(),
            None,
        ));

        let mut action_inputs: Vec<Arc<dyn ExecutionPlan>> =
            vec![Arc::clone(&writer) as Arc<dyn ExecutionPlan>];

        if !touched_adds.is_empty() {
            let remove_source = self.create_add_actions_plan(&touched_adds).await?;
            let remove_plan = Arc::new(DeltaRemoveActionsExec::new(remove_source));
            action_inputs.push(remove_plan);
        }

        let commit_input: Arc<dyn ExecutionPlan> = if action_inputs.len() == 1 {
            writer
        } else {
            UnionExec::try_new(action_inputs)?
        };

        let commit = Arc::new(DeltaCommitExec::new(
            commit_input,
            self.table_url.clone(),
            partition_columns,
            true, // table exists
            table_schema,
            PhysicalSinkMode::Append,
        ));

        Ok(commit)
    }

    async fn build_target_plan(
        &self,
        snapshot_state: DeltaTableState,
        log_store: LogStoreRef,
        target_filters: &[Arc<dyn PhysicalExpr>],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let scan_config = DeltaScanConfigBuilder::new()
            .with_file_column(true)
            .build(&snapshot_state)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let provider = DeltaTableProvider::try_new(snapshot_state, log_store, scan_config)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let mut plan = provider
            .scan(self.session, None, &[], None)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        for filter in target_filters {
            let aligned = self.align_filter_to_target_schema(filter, &plan.schema())?;
            plan = Arc::new(FilterExec::try_new(aligned, plan)?);
        }

        Ok(plan)
    }

    fn align_filter_to_target_schema(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        schema: &SchemaRef,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        expr.clone()
            .transform_up(|node| {
                if let Some(col) = node.as_any().downcast_ref::<Column>() {
                    if let Ok(idx) = schema.index_of(col.name()) {
                        if idx != col.index() {
                            let updated =
                                Arc::new(Column::new(col.name(), idx)) as Arc<dyn PhysicalExpr>;
                            return Ok(Transformed::yes(updated));
                        }
                    }
                }
                Ok(Transformed::no(node))
            })
            .data()
    }

    fn augment_target_plan(
        &self,
        target: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let logical_schema = self.merge_info.target_schema.as_ref().as_arrow().clone();
        let logical_field_names: Vec<String> = logical_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let physical_field_names: Vec<String> = target
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let mut projection_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();
        for (idx, field) in target.schema().fields().iter().enumerate() {
            let alias = logical_schema
                .fields()
                .get(idx)
                .map(|f| f.name().clone())
                .unwrap_or_else(|| field.name().clone());
            projection_exprs.push((
                Arc::new(Column::new(field.name(), idx)) as Arc<dyn PhysicalExpr>,
                alias,
            ));
        }
        let projected_names: Vec<String> = projection_exprs
            .iter()
            .map(|(_, name)| name.clone())
            .collect();
        log::trace!(
            "merge_augment_target_logical_fields: {:?}",
            &logical_field_names
        );
        log::trace!(
            "merge_augment_target_physical_fields: {:?}",
            &physical_field_names
        );
        log::trace!("merge_augment_target_output_fields: {:?}", &projected_names);
        Ok(Arc::new(ProjectionExec::try_new(projection_exprs, target)?))
    }

    fn augment_source_plan(
        &self,
        source: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut projection_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();
        let logical_schema = self.merge_info.source_schema.as_ref().as_arrow().clone();
        let logical_field_names: Vec<String> = logical_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let logical_metadata: Vec<HashMap<String, String>> = logical_schema
            .fields()
            .iter()
            .map(|f| f.metadata().clone())
            .collect();
        let physical_names: Vec<String> = source
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        for (idx, field) in source.schema().fields().iter().enumerate() {
            let name = logical_schema
                .fields()
                .get(idx)
                .map(|f| f.name().clone())
                .unwrap_or_else(|| field.name().clone());
            projection_exprs.push((
                Arc::new(Column::new(field.name(), idx)) as Arc<dyn PhysicalExpr>,
                name,
            ));
        }
        let projected_names: Vec<String> = projection_exprs
            .iter()
            .map(|(_, name)| name.clone())
            .collect();
        log::trace!(
            "merge_augment_source_logical_fields: {:?}",
            &logical_field_names
        );
        log::trace!(
            "merge_augment_source_logical_metadata: {:?}",
            &logical_metadata
        );
        log::trace!(
            "merge_augment_source_physical_fields: {:?}",
            &physical_names
        );
        log::trace!("merge_augment_source_output_fields: {:?}", &projected_names);

        projection_exprs.push((
            Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))) as Arc<dyn PhysicalExpr>,
            SOURCE_PRESENT_COLUMN.to_string(),
        ));

        Ok(Arc::new(ProjectionExec::try_new(projection_exprs, source)?))
    }

    fn build_join_plan(
        &self,
        target: Arc<dyn ExecutionPlan>,
        source: Arc<dyn ExecutionPlan>,
        join_analysis: &JoinConditionAnalysis,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !join_analysis.join_keys.is_empty() {
            let filter = if let Some(expr) = &join_analysis.residual_filter {
                Some(self.build_join_filter(expr.clone())?)
            } else {
                None
            };

            let join = HashJoinExec::try_new(
                target,
                source,
                join_analysis.join_keys.clone(),
                filter,
                &JoinType::Full,
                None,
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNothing,
            )?;
            Ok(Arc::new(join))
        } else {
            let filter = self.build_join_filter(Arc::clone(&self.merge_info.on_condition))?;
            let join =
                NestedLoopJoinExec::try_new(target, source, Some(filter), &JoinType::Full, None)?;
            Ok(Arc::new(join))
        }
    }

    fn build_join_filter(&self, condition: Arc<dyn PhysicalExpr>) -> Result<JoinFilter> {
        let mut column_indices = Vec::new();
        let mut fields = Vec::new();

        let target_arrow = self.merge_info.target_schema.as_ref().as_arrow().clone();
        for (idx, field) in target_arrow.fields().iter().enumerate() {
            fields.push(field.as_ref().clone());
            column_indices.push(ColumnIndex {
                index: idx,
                side: JoinSide::Left,
            });
        }

        let source_arrow = self.merge_info.source_schema.as_ref().as_arrow().clone();
        for (idx, field) in source_arrow.fields().iter().enumerate() {
            fields.push(field.as_ref().clone());
            column_indices.push(ColumnIndex {
                index: idx,
                side: JoinSide::Right,
            });
        }

        let schema = Arc::new(Schema::new(fields));
        Ok(JoinFilter::new(condition, column_indices, schema))
    }

    fn filter_to_touched_rows(
        &self,
        input: Arc<dyn ExecutionPlan>,
        ctx: &MergeSchemaContext,
        touched_paths: &HashSet<String>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let path_col = ctx.path_col_expr();
        let target_present = ctx.target_present_expr();

        let touched_literals: Vec<Arc<dyn PhysicalExpr>> = touched_paths
            .iter()
            .map(|path| {
                Arc::new(Literal::new(ScalarValue::Utf8(Some(path.clone()))))
                    as Arc<dyn PhysicalExpr>
            })
            .collect();

        let path_in_touched: Arc<dyn PhysicalExpr> = if touched_literals.is_empty() {
            lit_bool(false)
        } else {
            Arc::new(InListExpr::new(
                path_col.clone(),
                touched_literals,
                false,
                None,
            ))
        };

        let touched_targets = target_present.clone().and(path_in_touched);
        let source_only = target_present.clone().not();
        let keep_pred = touched_targets.or(source_only);

        Ok(Arc::new(FilterExec::try_new(keep_pred, input)?))
    }

    /// Build a row-level filter that keeps:
    /// - target rows that are not deleted, and
    /// - source-only rows that are inserted.
    fn build_merge_row_filter(
        &self,
        input: Arc<dyn ExecutionPlan>,
        ctx: &MergeSchemaContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input_schema = input.schema();

        let target_present = ctx.target_present_expr();
        let source_present = ctx.source_present_expr();

        let matched_pred = target_present.clone().and(source_present.clone());
        let not_matched_by_source_pred = target_present.clone().and(source_present.clone().not());
        let not_matched_by_target_pred = target_present.clone().not().and(source_present.clone());

        let mut delete_pred: Option<Arc<dyn PhysicalExpr>> = None;
        let mut insert_pred: Option<Arc<dyn PhysicalExpr>> = None;

        // Matched clauses
        for clause in &self.merge_info.matched_clauses {
            let mut pred = matched_pred.clone();
            if let Some(cond) = &clause.condition {
                pred = pred.and(Arc::clone(cond));
            }
            let pred = ctx.align_expr(&pred, &input_schema)?;

            use sail_common_datafusion::datasource::MergeMatchedActionInfo as MMAI;
            match &clause.action {
                MMAI::Delete => {
                    delete_pred = Some(match delete_pred {
                        Some(existing) => existing.or(pred),
                        None => pred,
                    });
                }
                MMAI::UpdateAll | MMAI::UpdateSet(_) => {}
            }
        }

        // NOT MATCHED BY SOURCE clauses
        for clause in &self.merge_info.not_matched_by_source_clauses {
            let mut pred = not_matched_by_source_pred.clone();
            if let Some(cond) = &clause.condition {
                pred = pred.and(Arc::clone(cond));
            }
            let pred = ctx.align_expr(&pred, &input_schema)?;

            use sail_common_datafusion::datasource::MergeNotMatchedBySourceActionInfo as NMBAI;
            match &clause.action {
                NMBAI::Delete => {
                    delete_pred = Some(match delete_pred {
                        Some(existing) => existing.or(pred),
                        None => pred,
                    });
                }
                NMBAI::UpdateSet(_) => {}
            }
        }

        // NOT MATCHED BY TARGET clauses (insert-only)
        for clause in &self.merge_info.not_matched_by_target_clauses {
            let mut pred = not_matched_by_target_pred.clone();
            if let Some(cond) = &clause.condition {
                pred = pred.and(Arc::clone(cond));
            }
            let pred = ctx.align_expr(&pred, &input_schema)?;

            use sail_common_datafusion::datasource::MergeNotMatchedByTargetActionInfo as NMTI;
            match &clause.action {
                NMTI::InsertAll | NMTI::InsertColumns { .. } => {
                    insert_pred = Some(match insert_pred {
                        Some(existing) => existing.or(pred),
                        None => pred,
                    });
                }
            }
        }

        let delete_expr = delete_pred.unwrap_or_else(|| lit_bool(false));
        let insert_expr = insert_pred.unwrap_or_else(|| lit_bool(false));

        let keep_or_update = target_present.clone().and(delete_expr.not());
        let active_expr = keep_or_update.or(insert_expr);

        log::trace!("build_merge_row_filter active_expr: {:?}", &active_expr);

        let filter = FilterExec::try_new(active_expr, input)?;
        Ok(Arc::new(filter))
    }

    /// Build the projection that computes final table columns after applying
    /// MERGE clauses. Rows have already been filtered for delete/insert
    /// semantics by `build_merge_row_filter`.
    fn build_merge_projection(
        &self,
        input: Arc<dyn ExecutionPlan>,
        ctx: &MergeSchemaContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input_schema = input.schema();
        let table_schema = &ctx.table_schema;

        log::trace!(
            "merge_projection_target_fields: {:?}",
            ctx.target_physical_schema
                .fields()
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>()
        );
        log::trace!(
            "merge_projection_source_fields: {:?}",
            ctx.source_physical_schema
                .fields()
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>()
        );
        log::trace!(
            "merge_projection_table_fields: {:?}",
            table_schema
                .fields()
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>()
        );

        let target_present = ctx.target_present_expr();
        let source_present = ctx.source_present_expr();

        let matched_pred = target_present.clone().and(source_present.clone());
        let not_matched_by_source_pred = target_present.clone().and(source_present.clone().not());
        let not_matched_by_target_pred = target_present.clone().not().and(source_present.clone());

        log::trace!(
            "merge_projection_target_idx_by_name: {:?}",
            &ctx.target_col_idx
        );

        // Precompute target/source column expressions for each output column.
        let mut target_exprs: HashMap<String, PhysicalExprRef> = HashMap::new();
        let mut source_exprs: HashMap<String, PhysicalExprRef> = HashMap::new();

        for (i, field) in table_schema.fields().iter().enumerate() {
            let name = field.name().clone();

            // Use the simplified target_col_expr method from context
            let target_expr = ctx.target_col_expr(&name, field.data_type())?;

            let source_expr: PhysicalExprRef = ctx.source_col_expr_by_idx(i).unwrap_or_else(|| {
                Arc::new(Literal::new(
                    Self::typed_null(field.data_type()).unwrap_or(ScalarValue::Null),
                )) as PhysicalExprRef
            });

            target_exprs.insert(name.clone(), target_expr);
            source_exprs.insert(name.clone(), source_expr);
        }

        // For each output column, collect CASE when/then branches
        let mut column_cases: ColumnCasesMap = HashMap::new();
        for field in table_schema.fields() {
            column_cases.insert(field.name().clone(), Vec::new());
        }

        // Matched clauses
        for clause in &self.merge_info.matched_clauses {
            let mut pred = matched_pred.clone();
            if let Some(cond) = &clause.condition {
                pred = pred.and(Arc::clone(cond));
            }
            let pred = ctx.align_expr(&pred, &input_schema)?;

            use sail_common_datafusion::datasource::{
                MergeAssignmentInfo, MergeMatchedActionInfo as MMAI,
            };
            match &clause.action {
                MMAI::Delete => {
                    // Values are irrelevant for deleted rows (already filtered out)
                }
                MMAI::UpdateAll => {
                    for field in table_schema.fields() {
                        let name = field.name();
                        let src_expr = match source_exprs.get(name).cloned() {
                            Some(expr) => expr,
                            None => Arc::new(Literal::new(Self::typed_null(field.data_type())?))
                                as PhysicalExprRef,
                        };
                        if let Some(cases) = column_cases.get_mut(name) {
                            cases.push((pred.clone(), src_expr));
                        }
                    }
                }
                MMAI::UpdateSet(assignments) => {
                    let mut assign_map: HashMap<String, Arc<dyn PhysicalExpr>> = HashMap::new();
                    for MergeAssignmentInfo { column, value } in assignments {
                        let phys_name = ctx
                            .logical_to_physical
                            .get(column.as_str())
                            .cloned()
                            .unwrap_or_else(|| column.clone());
                        let aligned_value = ctx.align_expr(value, &input_schema)?;
                        assign_map.insert(phys_name, aligned_value);
                    }
                    log::trace!("UpdateSet assignments: {:?}", &assign_map);
                    for (col, value_expr) in assign_map {
                        if let Some(cases) = column_cases.get_mut(col.as_str()) {
                            log::trace!(
                                "Pushing UpdateSet case for column {}: {:?}",
                                col.as_str(),
                                &value_expr
                            );
                            cases.push((pred.clone(), Arc::clone(&value_expr)));
                        }
                    }
                }
            }
        }

        // NOT MATCHED BY SOURCE clauses
        for clause in &self.merge_info.not_matched_by_source_clauses {
            let mut pred = not_matched_by_source_pred.clone();
            if let Some(cond) = &clause.condition {
                pred = pred.and(Arc::clone(cond));
            }
            let pred = ctx.align_expr(&pred, &input_schema)?;

            use sail_common_datafusion::datasource::{
                MergeAssignmentInfo, MergeNotMatchedBySourceActionInfo as NMBAI,
            };
            match &clause.action {
                NMBAI::Delete => {
                    // Values are irrelevant for deleted rows (already filtered out)
                }
                NMBAI::UpdateSet(assignments) => {
                    let mut assign_map: HashMap<String, Arc<dyn PhysicalExpr>> = HashMap::new();
                    for MergeAssignmentInfo { column, value } in assignments {
                        let phys_name = ctx
                            .logical_to_physical
                            .get(column.as_str())
                            .cloned()
                            .unwrap_or_else(|| column.clone());
                        let aligned_value = ctx.align_expr(value, &input_schema)?;
                        assign_map.insert(phys_name, aligned_value);
                    }
                    for (col, value_expr) in assign_map {
                        if let Some(cases) = column_cases.get_mut(col.as_str()) {
                            cases.push((pred.clone(), Arc::clone(&value_expr)));
                        }
                    }
                }
            }
        }

        // NOT MATCHED BY TARGET clauses (insert-only)
        for clause in &self.merge_info.not_matched_by_target_clauses {
            let mut pred = not_matched_by_target_pred.clone();
            if let Some(cond) = &clause.condition {
                pred = pred.and(Arc::clone(cond));
            }
            let pred = ctx.align_expr(&pred, &input_schema)?;

            use sail_common_datafusion::datasource::MergeNotMatchedByTargetActionInfo as NMTI;
            match &clause.action {
                NMTI::InsertAll => {
                    for field in table_schema.fields() {
                        let name = field.name();
                        let src_expr = match source_exprs.get(name).cloned() {
                            Some(expr) => expr,
                            None => Arc::new(Literal::new(Self::typed_null(field.data_type())?))
                                as PhysicalExprRef,
                        };
                        if let Some(cases) = column_cases.get_mut(name) {
                            cases.push((pred.clone(), src_expr));
                        }
                    }
                }
                NMTI::InsertColumns { columns, values } => {
                    for (col, value_expr) in columns.iter().zip(values.iter()) {
                        let phys_name = ctx
                            .logical_to_physical
                            .get(col.as_str())
                            .cloned()
                            .unwrap_or_else(|| col.clone());
                        let aligned_value = ctx.align_expr(value_expr, &input_schema)?;
                        if let Some(cases) = column_cases.get_mut(phys_name.as_str()) {
                            cases.push((pred.clone(), aligned_value));
                        }
                    }
                }
            }
        }

        // Build projection expressions for final table columns
        let mut projection_exprs: Vec<(PhysicalExprRef, String)> = Vec::new();
        for field in table_schema.fields() {
            let name = field.name();
            let default_expr = match target_exprs.get(name).cloned() {
                Some(expr) => expr,
                None => {
                    Arc::new(Literal::new(Self::typed_null(field.data_type())?)) as PhysicalExprRef
                }
            };

            let cases = column_cases.remove(name).unwrap_or_default();
            let expr: PhysicalExprRef = if cases.is_empty() {
                default_expr
            } else {
                Arc::new(CaseExpr::try_new(None, cases, Some(default_expr))?) as PhysicalExprRef
            };

            projection_exprs.push((expr, name.clone()));
        }

        Ok(Arc::new(ProjectionExec::try_new(projection_exprs, input)?))
    }

    fn typed_null(data_type: &DataType) -> Result<ScalarValue> {
        ScalarValue::try_from(data_type)
    }

    async fn create_add_actions_plan(&self, adds: &[Add]) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("add", DataType::Utf8, true),
            Field::new("partition_scan", DataType::Boolean, false),
        ]));

        let batch = if adds.is_empty() {
            RecordBatch::new_empty(schema.clone())
        } else {
            let add_strings: Result<Vec<Option<String>>> = adds
                .iter()
                .map(|add| {
                    serde_json::to_string(add)
                        .map(Some)
                        .map_err(|e| DataFusionError::External(Box::new(e)))
                })
                .collect();
            let add_array = Arc::new(StringArray::from(add_strings?));
            let bools = vec![false; adds.len()];
            let partition_scan = Arc::new(BooleanArray::from(bools));
            RecordBatch::try_new(schema.clone(), vec![add_array, partition_scan])?
        };

        let table = MemTable::try_new(schema, vec![vec![batch]])?;
        table
            .scan(self.session, None, &[], None)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }
}
