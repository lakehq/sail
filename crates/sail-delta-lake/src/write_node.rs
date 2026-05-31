use std::fmt::Formatter;
use std::sync::Arc;

use datafusion_common::{DFSchema, DFSchemaRef};
use datafusion_expr::expr::Sort;
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common_datafusion::catalog::CatalogPartitionField;
use sail_common_datafusion::datasource::{BucketBy, OptionLayer, SinkMode};
use sail_common_datafusion::utils::items::ItemTaker;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct DeltaWriteNode {
    input: Arc<LogicalPlan>,
    mode: SinkMode,
    partition_by: Vec<CatalogPartitionField>,
    bucket_by: Option<BucketBy>,
    sort_order: Vec<Sort>,
    options: Vec<OptionLayer>,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
}

impl DeltaWriteNode {
    pub fn new(
        input: Arc<LogicalPlan>,
        mode: SinkMode,
        partition_by: Vec<CatalogPartitionField>,
        bucket_by: Option<BucketBy>,
        sort_order: Vec<Sort>,
        options: Vec<OptionLayer>,
    ) -> Self {
        Self {
            input,
            mode,
            partition_by,
            bucket_by,
            sort_order,
            options,
            schema: Arc::new(DFSchema::empty()),
        }
    }

    pub fn mode(&self) -> &SinkMode {
        &self.mode
    }

    pub fn partition_by(&self) -> &[CatalogPartitionField] {
        &self.partition_by
    }

    pub fn bucket_by(&self) -> Option<&BucketBy> {
        self.bucket_by.as_ref()
    }

    pub fn sort_order(&self) -> &[Sort] {
        &self.sort_order
    }

    pub fn options(&self) -> &[OptionLayer] {
        &self.options
    }
}

impl UserDefinedLogicalNodeCore for DeltaWriteNode {
    fn name(&self) -> &str {
        "DeltaWrite"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "DeltaWrite: mode={:?}, partition_by={:?}, sort_order={:?}",
            self.mode, self.partition_by, self.sort_order
        )?;
        Ok(())
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Self> {
        exprs.zero()?;
        Ok(Self {
            input: Arc::new(inputs.one()?),
            mode: self.mode.clone(),
            partition_by: self.partition_by.clone(),
            bucket_by: self.bucket_by.clone(),
            sort_order: self.sort_order.clone(),
            options: self.options.clone(),
            schema: self.schema.clone(),
        })
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![(0..self.input.schema().fields().len()).collect()])
    }
}
