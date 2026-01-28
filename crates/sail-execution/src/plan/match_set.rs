use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::new_null_array;
use datafusion::arrow::array::{BinaryBuilder, BooleanArray, BooleanBuilder, UInt64Array};
use datafusion::arrow::compute::{concat_batches, filter_record_batch, not};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{exec_datafusion_err, plan_err, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use futures::StreamExt;

const MATCH_SET_CHUNK_BITS: usize = 64 * 1024;
const MATCH_SET_CHUNK_BYTES: usize = MATCH_SET_CHUNK_BITS / 8;

pub(crate) fn match_set_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("chunk_index", DataType::UInt64, false),
        Field::new("bitmap", DataType::Binary, false),
    ]))
}

pub(crate) fn build_match_set_batch<I>(row_ids: I) -> Result<RecordBatch>
where
    I: IntoIterator<Item = u64>,
{
    let mut combined: BTreeMap<u64, Vec<u8>> = BTreeMap::new();
    for row_id in row_ids {
        let chunk_index = row_id / MATCH_SET_CHUNK_BITS as u64;
        let offset = (row_id % MATCH_SET_CHUNK_BITS as u64) as usize;
        let byte_index = offset / 8;
        let bit_index = offset % 8;
        let entry = combined
            .entry(chunk_index)
            .or_insert_with(|| vec![0; MATCH_SET_CHUNK_BYTES]);
        entry[byte_index] |= 1u8 << bit_index;
    }
    let mut indices = Vec::with_capacity(combined.len());
    let mut bitmap_builder =
        BinaryBuilder::with_capacity(combined.len(), combined.len() * MATCH_SET_CHUNK_BYTES);
    for (chunk_index, bitmap) in combined {
        indices.push(chunk_index);
        bitmap_builder.append_value(&bitmap);
    }
    let index_array = UInt64Array::from(indices);
    let bitmap_array = bitmap_builder.finish();
    Ok(RecordBatch::try_new(
        match_set_schema(),
        vec![Arc::new(index_array), Arc::new(bitmap_array)],
    )?)
}

fn build_match_set_batch_from_bitmaps(combined: BTreeMap<u64, Vec<u8>>) -> Result<RecordBatch> {
    let mut indices = Vec::with_capacity(combined.len());
    let mut bitmap_builder =
        BinaryBuilder::with_capacity(combined.len(), combined.len() * MATCH_SET_CHUNK_BYTES);
    for (chunk_index, bitmap) in combined {
        indices.push(chunk_index);
        bitmap_builder.append_value(&bitmap);
    }
    let index_array = UInt64Array::from(indices);
    let bitmap_array = bitmap_builder.finish();
    Ok(RecordBatch::try_new(
        match_set_schema(),
        vec![Arc::new(index_array), Arc::new(bitmap_array)],
    )?)
}

#[derive(Debug, Clone)]
pub struct BuildMatchSetExec {
    input: Arc<dyn ExecutionPlan>,
    row_id_index: usize,
    properties: PlanProperties,
}

impl BuildMatchSetExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, row_id_index: usize) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(match_set_schema()),
            input.output_partitioning().clone(),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            input,
            row_id_index,
            properties,
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn row_id_index(&self) -> usize {
        self.row_id_index
    }
}

impl DisplayAs for BuildMatchSetExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "BuildMatchSetExec")
    }
}

impl ExecutionPlan for BuildMatchSetExec {
    fn name(&self) -> &str {
        "BuildMatchSetExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let child = children.pop();
        match (child, children.is_empty()) {
            (Some(input), true) => Ok(Arc::new(Self::new(input, self.row_id_index))),
            _ => plan_err!("BuildMatchSetExec should have one child"),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let schema = match_set_schema();
        let row_id_index = self.row_id_index;
        let output = futures::stream::once(async move {
            let batch = build_match_set_from_row_ids(input, row_id_index).await?;
            Ok(batch)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, output)))
    }
}

#[derive(Debug, Clone)]
pub struct ApplyMatchSetExec {
    left: Arc<dyn ExecutionPlan>,
    match_set: Arc<dyn ExecutionPlan>,
    row_id_index: usize,
    join_type: datafusion::common::JoinType,
    output_schema: SchemaRef,
    properties: PlanProperties,
}

impl ApplyMatchSetExec {
    pub fn new(
        left: Arc<dyn ExecutionPlan>,
        match_set: Arc<dyn ExecutionPlan>,
        row_id_index: usize,
        join_type: datafusion::common::JoinType,
        output_schema: SchemaRef,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            left.output_partitioning().clone(),
            left.pipeline_behavior(),
            left.boundedness(),
        );
        Self {
            left,
            match_set,
            row_id_index,
            join_type,
            output_schema,
            properties,
        }
    }

    pub fn left(&self) -> &Arc<dyn ExecutionPlan> {
        &self.left
    }

    pub fn match_set(&self) -> &Arc<dyn ExecutionPlan> {
        &self.match_set
    }

    pub fn row_id_index(&self) -> usize {
        self.row_id_index
    }

    pub fn join_type(&self) -> datafusion::common::JoinType {
        self.join_type
    }

    pub fn output_schema(&self) -> &SchemaRef {
        &self.output_schema
    }
}

impl DisplayAs for ApplyMatchSetExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ApplyMatchSetExec")
    }
}

impl ExecutionPlan for ApplyMatchSetExec {
    fn name(&self) -> &str {
        "ApplyMatchSetExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.match_set]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 2 {
            return plan_err!("ApplyMatchSetExec should have two children");
        }
        let match_set = children.pop().ok_or_else(|| {
            datafusion::error::DataFusionError::Plan(
                "ApplyMatchSetExec should have two children, got 1".to_string(),
            )
        })?;
        let left = children.pop().ok_or_else(|| {
            datafusion::error::DataFusionError::Plan(
                "ApplyMatchSetExec should have two children, got 0".to_string(),
            )
        })?;
        Ok(Arc::new(Self::new(
            left,
            match_set,
            self.row_id_index,
            self.join_type,
            self.output_schema.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let left = self.left.execute(partition, context.clone())?;
        let match_set = self.match_set.execute(0, context)?;
        let row_id_index = self.row_id_index;
        let join_type = self.join_type;
        let output_schema = self.output_schema.clone();
        let output = futures::stream::once(async move {
            let bitmap = read_match_set(match_set).await?;
            let batch =
                apply_match_set(left, row_id_index, join_type, &bitmap, &output_schema).await?;
            Ok(batch)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            output,
        )))
    }
}

#[derive(Debug, Clone)]
pub struct MatchSetOrExec {
    input: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
}

impl MatchSetOrExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let properties = PlanProperties::new(
            input.properties().equivalence_properties().clone(),
            input.properties().output_partitioning().clone(),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self { input, properties }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for MatchSetOrExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "MatchSetOrExec")
    }
}

impl ExecutionPlan for MatchSetOrExec {
    fn name(&self) -> &str {
        "MatchSetOrExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let child = children.pop();
        match (child, children.is_empty()) {
            (Some(input), true) => Ok(Arc::new(Self::new(input))),
            _ => plan_err!("MatchSetOrExec should have one child"),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let schema = match_set_schema();
        let output = futures::stream::once(async move {
            let batch = collect_match_set(input).await?;
            Ok(batch)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, output)))
    }
}

async fn collect_match_set(mut stream: SendableRecordBatchStream) -> Result<RecordBatch> {
    let mut combined: BTreeMap<u64, Vec<u8>> = BTreeMap::new();
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        if batch.num_columns() != 2 {
            return Err(exec_datafusion_err!(
                "match set expects 2 columns, got {}",
                batch.num_columns()
            ));
        }
        let chunk_indices = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| exec_datafusion_err!("match set chunk_index must be UInt64"))?;
        let bitmaps = batch
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::BinaryArray>()
            .ok_or_else(|| exec_datafusion_err!("match set bitmap must be Binary"))?;
        for row in 0..batch.num_rows() {
            let chunk_index = chunk_indices.value(row);
            let bitmap = bitmaps.value(row);
            if bitmap.len() != MATCH_SET_CHUNK_BYTES {
                return Err(exec_datafusion_err!(
                    "match set bitmap length mismatch: expected {}, got {}",
                    MATCH_SET_CHUNK_BYTES,
                    bitmap.len()
                ));
            }
            let entry = combined
                .entry(chunk_index)
                .or_insert_with(|| vec![0; MATCH_SET_CHUNK_BYTES]);
            for (dest, src) in entry.iter_mut().zip(bitmap.iter()) {
                *dest |= *src;
            }
        }
    }
    build_match_set_batch_from_bitmaps(combined)
}

async fn build_match_set_from_row_ids(
    mut stream: SendableRecordBatchStream,
    row_id_index: usize,
) -> Result<RecordBatch> {
    let mut ids = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let row_ids = batch
            .column(row_id_index)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| exec_datafusion_err!("row id column must be UInt64"))?;
        ids.extend(row_ids.iter().flatten());
    }
    build_match_set_batch(ids)
}

async fn read_match_set(mut stream: SendableRecordBatchStream) -> Result<BTreeMap<u64, Vec<u8>>> {
    let mut combined: BTreeMap<u64, Vec<u8>> = BTreeMap::new();
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let chunk_indices = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| exec_datafusion_err!("match set chunk_index must be UInt64"))?;
        let bitmaps = batch
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::BinaryArray>()
            .ok_or_else(|| exec_datafusion_err!("match set bitmap must be Binary"))?;
        for row in 0..batch.num_rows() {
            let chunk_index = chunk_indices.value(row);
            let bitmap = bitmaps.value(row);
            if bitmap.len() != MATCH_SET_CHUNK_BYTES {
                return Err(exec_datafusion_err!(
                    "match set bitmap length mismatch: expected {}, got {}",
                    MATCH_SET_CHUNK_BYTES,
                    bitmap.len()
                ));
            }
            let entry = combined
                .entry(chunk_index)
                .or_insert_with(|| vec![0; MATCH_SET_CHUNK_BYTES]);
            for (dest, src) in entry.iter_mut().zip(bitmap.iter()) {
                *dest |= *src;
            }
        }
    }
    Ok(combined)
}

async fn apply_match_set(
    mut stream: SendableRecordBatchStream,
    row_id_index: usize,
    join_type: datafusion::common::JoinType,
    bitmap: &BTreeMap<u64, Vec<u8>>,
    output_schema: &SchemaRef,
) -> Result<RecordBatch> {
    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let row_ids = batch
            .column(row_id_index)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| exec_datafusion_err!("row id column must be UInt64"))?;
        let mut builder = BooleanBuilder::with_capacity(row_ids.len());
        for value in row_ids.iter() {
            builder.append_option(value.map(|row_id| match_set_contains(bitmap, row_id)));
        }
        let marks: BooleanArray = builder.finish();
        let batch = match join_type {
            datafusion::common::JoinType::LeftMark => {
                let mut columns = batch
                    .columns()
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, col)| (idx != row_id_index).then_some(Arc::clone(col)))
                    .collect::<Vec<_>>();
                columns.push(Arc::new(marks));
                append_null_columns(&mut columns, output_schema, batch.num_rows());
                RecordBatch::try_new(output_schema.clone(), columns)?
            }
            datafusion::common::JoinType::Left
            | datafusion::common::JoinType::Full
            | datafusion::common::JoinType::LeftAnti
            | datafusion::common::JoinType::LeftSemi => {
                let is_match = matches!(join_type, datafusion::common::JoinType::LeftSemi);
                let mask = if is_match { marks } else { not(&marks)? };
                let filtered = filter_record_batch(&batch, &mask)?;
                let columns = filtered
                    .columns()
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, col)| (idx != row_id_index).then_some(Arc::clone(col)))
                    .collect::<Vec<_>>();
                let mut columns = columns;
                append_null_columns(&mut columns, output_schema, filtered.num_rows());
                RecordBatch::try_new(output_schema.clone(), columns)?
            }
            _ => {
                return Err(exec_datafusion_err!(
                    "ApplyMatchSetExec does not support join type {join_type}"
                ))
            }
        };
        batches.push(batch);
    }
    let schema = output_schema.clone();
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }
    Ok(concat_batches(&schema, &batches)?)
}

fn match_set_contains(bitmap: &BTreeMap<u64, Vec<u8>>, row_id: u64) -> bool {
    let chunk_index = row_id / MATCH_SET_CHUNK_BITS as u64;
    let offset = (row_id % MATCH_SET_CHUNK_BITS as u64) as usize;
    let byte_index = offset / 8;
    let bit_index = offset % 8;
    let Some(chunk) = bitmap.get(&chunk_index) else {
        return false;
    };
    (chunk[byte_index] & (1u8 << bit_index)) != 0
}

fn append_null_columns(
    columns: &mut Vec<Arc<dyn datafusion::arrow::array::Array>>,
    schema: &SchemaRef,
    num_rows: usize,
) {
    while columns.len() < schema.fields().len() {
        let field = &schema.fields()[columns.len()];
        columns.push(new_null_array(field.data_type(), num_rows));
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests use unwrap for brevity")]
mod tests {
    use super::super::row_id::BUILD_ROW_ID_COLUMN;
    use super::*;
    use datafusion::arrow::array::{Int32Array, UInt64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::context::SessionContext;
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::joins::utils::build_join_schema;
    use datafusion::physical_plan::test::TestMemoryExec;

    fn make_left_batch() -> RecordBatch {
        let left_values = Int32Array::from(vec![1, 2, 3]);
        let row_ids = UInt64Array::from(vec![0, 1, 2]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("l", DataType::Int32, false),
            Field::new(BUILD_ROW_ID_COLUMN, DataType::UInt64, false),
        ]));
        RecordBatch::try_new(schema, vec![Arc::new(left_values), Arc::new(row_ids)]).unwrap()
    }

    fn make_match_set() -> RecordBatch {
        build_match_set_batch(vec![0, 2]).unwrap()
    }

    async fn apply_for_join_type(join_type: datafusion::common::JoinType) -> Vec<RecordBatch> {
        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();
        let left_batch = make_left_batch();
        let left_schema = left_batch.schema();
        let match_set_batch = make_match_set();
        let match_set_schema = match_set_batch.schema();
        let left_exec =
            TestMemoryExec::try_new_exec(&[vec![left_batch]], left_schema, None).unwrap();
        let match_set_exec =
            TestMemoryExec::try_new_exec(&[vec![match_set_batch]], match_set_schema, None).unwrap();
        let base_left_schema = Arc::new(Schema::new(vec![Field::new("l", DataType::Int32, false)]));
        let right_schema = Arc::new(Schema::new(vec![Field::new("r", DataType::Int32, true)]));
        let (output_schema, _) =
            build_join_schema(base_left_schema.as_ref(), right_schema.as_ref(), &join_type);
        let exec = ApplyMatchSetExec::new(
            left_exec,
            match_set_exec,
            1,
            join_type,
            Arc::new(output_schema),
        );
        collect(Arc::new(exec), task_ctx).await.unwrap()
    }

    #[tokio::test]
    async fn test_apply_match_set_left() {
        let batches = apply_for_join_type(datafusion::common::JoinType::Left).await;
        let expected = [
            "+---+---+",
            "| l | r |",
            "+---+---+",
            "| 2 |   |",
            "+---+---+",
        ];
        datafusion::assert_batches_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn test_apply_match_set_full() {
        let batches = apply_for_join_type(datafusion::common::JoinType::Full).await;
        let expected = [
            "+---+---+",
            "| l | r |",
            "+---+---+",
            "| 2 |   |",
            "+---+---+",
        ];
        datafusion::assert_batches_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn test_apply_match_set_left_semi() {
        let batches = apply_for_join_type(datafusion::common::JoinType::LeftSemi).await;
        let expected = ["+---+", "| l |", "+---+", "| 1 |", "| 3 |", "+---+"];
        datafusion::assert_batches_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn test_apply_match_set_left_anti() {
        let batches = apply_for_join_type(datafusion::common::JoinType::LeftAnti).await;
        let expected = ["+---+", "| l |", "+---+", "| 2 |", "+---+"];
        datafusion::assert_batches_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn test_apply_match_set_left_mark() {
        let batches = apply_for_join_type(datafusion::common::JoinType::LeftMark).await;
        let expected = [
            "+---+-------+",
            "| l | mark  |",
            "+---+-------+",
            "| 1 | true  |",
            "| 2 | false |",
            "| 3 | true  |",
            "+---+-------+",
        ];
        datafusion::assert_batches_eq!(expected, &batches);
    }
}
