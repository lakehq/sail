use std::fmt::Formatter;

use datafusion::common::{plan_err, DFSchema, DFSchemaRef, Result, TableReference};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use educe::Educe;

use crate::logical::handle::DeltaTableHandle;

#[derive(Debug, Clone, Eq, PartialEq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct DeltaTableScanNode {
    handle: DeltaTableHandle,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    fetch: Option<usize>,
    #[educe(PartialOrd(ignore))]
    original_schema: DFSchemaRef,
    #[educe(PartialOrd(ignore))]
    projected_schema: DFSchemaRef,
}

impl DeltaTableScanNode {
    pub fn try_new(
        qualifier: TableReference,
        handle: DeltaTableHandle,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        fetch: Option<usize>,
    ) -> Result<Self> {
        let original_arrow = handle.inner().schema.clone();
        let projected_arrow = if let Some(projection) = &projection {
            std::sync::Arc::new(original_arrow.project(projection)?)
        } else {
            original_arrow.clone()
        };

        Ok(Self {
            handle,
            projection,
            filters,
            fetch,
            original_schema: std::sync::Arc::new(DFSchema::try_from_qualified_schema(
                qualifier.clone(),
                &original_arrow,
            )?),
            projected_schema: std::sync::Arc::new(DFSchema::try_from_qualified_schema(
                qualifier,
                &projected_arrow,
            )?),
        })
    }

    pub fn handle(&self) -> &DeltaTableHandle {
        &self.handle
    }

    pub fn projection(&self) -> Option<&[usize]> {
        self.projection.as_deref()
    }

    pub fn filters(&self) -> &Vec<Expr> {
        &self.filters
    }

    pub fn fetch(&self) -> Option<usize> {
        self.fetch
    }

    pub fn original_schema(&self) -> &DFSchemaRef {
        &self.original_schema
    }
}

impl UserDefinedLogicalNodeCore for DeltaTableScanNode {
    fn name(&self) -> &str {
        "DeltaTableScan"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.projected_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.filters.clone()
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}: projection={:?}, filters={:?}, fetch={:?}",
            self.name(),
            self.projection,
            self.filters,
            self.fetch
        )
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if !inputs.is_empty() {
            return plan_err!("DeltaTableScanNode should not have inputs");
        }
        Ok(Self {
            handle: self.handle.clone(),
            projection: self.projection.clone(),
            filters: exprs,
            fetch: self.fetch,
            original_schema: self.original_schema.clone(),
            projected_schema: self.projected_schema.clone(),
        })
    }
}
