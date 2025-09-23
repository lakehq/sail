use std::cmp::Ordering;
use std::fmt::Formatter;
use std::hash::Hash;
use std::sync::Arc;

use datafusion::logical_expr::LogicalPlan;
use datafusion_common::{plan_err, DFSchema, DFSchemaRef, Result, TableReference};
use datafusion_expr::{Expr, UserDefinedLogicalNodeCore};
use sail_common_datafusion::rename::expression::expression_before_rename;
use sail_common_datafusion::rename::schema::rename_schema;
use sail_common_datafusion::streaming::event::schema::to_flow_event_schema;
use sail_common_datafusion::streaming::source::StreamSource;

/// A logical plan node that wraps a streaming source after streaming query rewrite.
#[derive(Clone, Debug)]
pub struct StreamSourceWrapperNode {
    source: Arc<dyn StreamSource>,
    names: Option<Vec<String>>,
    schema: DFSchemaRef,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    fetch: Option<usize>,
}

impl PartialEq for StreamSourceWrapperNode {
    fn eq(&self, other: &Self) -> bool {
        self.names == other.names
            && self.schema == other.schema
            && self.projection == other.projection
            && self.filters == other.filters
            && self.fetch == other.fetch
    }
}

impl Eq for StreamSourceWrapperNode {}

impl Hash for StreamSourceWrapperNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.names.hash(state);
        self.schema.hash(state);
        self.projection.hash(state);
        self.filters.hash(state);
        self.fetch.hash(state);
    }
}

#[derive(PartialEq, PartialOrd)]
struct StreamSourceWrapperNodeOrd<'a> {
    names: &'a Option<Vec<String>>,
    projection: &'a Option<Vec<usize>>,
    filters: &'a Vec<Expr>,
    fetch: &'a Option<usize>,
}

impl<'a> From<&'a StreamSourceWrapperNode> for StreamSourceWrapperNodeOrd<'a> {
    fn from(node: &'a StreamSourceWrapperNode) -> Self {
        Self {
            names: &node.names,
            projection: &node.projection,
            filters: &node.filters,
            fetch: &node.fetch,
        }
    }
}

impl PartialOrd for StreamSourceWrapperNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        StreamSourceWrapperNodeOrd::from(self).partial_cmp(&other.into())
    }
}

impl StreamSourceWrapperNode {
    pub fn try_new(
        table_name: TableReference,
        source: Arc<dyn StreamSource>,
        names: Option<Vec<String>>,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        fetch: Option<usize>,
    ) -> Result<Self> {
        let schema = match &names {
            Some(names) => rename_schema(&source.data_schema(), names)?,
            None => source.data_schema(),
        };
        let schema = match &projection {
            Some(projection) => Arc::new(schema.project(projection)?),
            None => schema,
        };
        let schema =
            DFSchema::try_from_qualified_schema(table_name, &to_flow_event_schema(&schema))?;
        let filters = match &names {
            Some(names) => {
                let source_schema = source.data_schema();
                filters
                    .iter()
                    .map(|e| expression_before_rename(e, names, &source_schema, true))
                    .collect::<Result<Vec<_>>>()?
            }
            None => filters,
        };
        Ok(Self {
            source,
            names,
            schema: Arc::new(schema),
            projection,
            filters,
            fetch,
        })
    }

    pub fn source(&self) -> &Arc<dyn StreamSource> {
        &self.source
    }

    pub fn names(&self) -> Option<&Vec<String>> {
        self.names.as_ref()
    }

    pub fn projection(&self) -> Option<&Vec<usize>> {
        self.projection.as_ref()
    }

    pub fn filters(&self) -> &Vec<Expr> {
        &self.filters
    }

    pub fn fetch(&self) -> Option<usize> {
        self.fetch
    }
}

impl UserDefinedLogicalNodeCore for StreamSourceWrapperNode {
    fn name(&self) -> &str {
        "StreamSourceWrapper"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "StreamSourceWrapper")
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if !exprs.is_empty() {
            return plan_err!("{} does not take any expressions", self.name());
        }
        if !inputs.is_empty() {
            return plan_err!("{} does not take any inputs", self.name());
        }
        Ok(self.clone())
    }
}
