use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::common::{plan_err, DFSchema, DFSchemaRef, Result, TableReference};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common_datafusion::system::catalog::SystemTable;

#[derive(Debug, Clone, Eq, PartialEq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct SystemTableNode {
    table: SystemTable,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    fetch: Option<usize>,
    #[educe(PartialOrd(ignore))]
    original_schema: DFSchemaRef,
    #[educe(PartialOrd(ignore))]
    projected_schema: DFSchemaRef,
}

impl SystemTableNode {
    pub fn try_new(
        qualifier: TableReference,
        table: SystemTable,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        fetch: Option<usize>,
    ) -> Result<Self> {
        let original_schema = table.schema();
        let projected_schema = if let Some(projection) = &projection {
            Arc::new(original_schema.project(projection)?)
        } else {
            original_schema.clone()
        };
        Ok(Self {
            table,
            projection,
            filters,
            fetch,
            original_schema: Arc::new(DFSchema::try_from_qualified_schema(
                qualifier.clone(),
                &original_schema,
            )?),
            projected_schema: Arc::new(DFSchema::try_from_qualified_schema(
                qualifier,
                &projected_schema,
            )?),
        })
    }

    pub fn table(&self) -> SystemTable {
        self.table
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

impl UserDefinedLogicalNodeCore for SystemTableNode {
    fn name(&self) -> &str {
        "SystemTable"
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
            "{}: table={}, projection={:?}, filters={:?}, fetch={:?}",
            self.name(),
            self.table.name(),
            self.projection,
            self.filters,
            self.fetch
        )
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if !inputs.is_empty() {
            return plan_err!("SystemTableNode should not have inputs");
        }
        Ok(Self {
            table: self.table,
            projection: self.projection.clone(),
            filters: exprs,
            fetch: self.fetch,
            original_schema: self.original_schema.clone(),
            projected_schema: self.projected_schema.clone(),
        })
    }
}
