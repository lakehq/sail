use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::{internal_err, Result};
use datafusion_physical_expr::{Distribution, EquivalenceProperties};

use crate::rename::record_batch::rename_record_batch_stream;
use crate::rename::schema::rename_schema;

/// A lightweight physical wrapper that renames the output schema.
#[derive(Debug, Clone)]
pub struct RenameExec {
    input: Arc<dyn ExecutionPlan>,
    names: Vec<String>,
    cache: PlanProperties,
}

impl RenameExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>, names: Vec<String>) -> Result<Self> {
        let schema = rename_schema(input.schema().as_ref(), &names)?;
        let props = input.properties();
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema),
            props.partitioning.clone(),
            props.emission_type,
            props.boundedness,
        );
        Ok(Self {
            input,
            names,
            cache,
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn names(&self) -> &[String] {
        &self.names
    }
}

impl DisplayAs for RenameExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "RenameExec")
            }
        }
    }
}

#[async_trait]
impl ExecutionPlan for RenameExec {
    fn name(&self) -> &'static str {
        "RenameExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        self.input.required_input_distribution()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("RenameExec expects exactly one child");
        }
        Ok(Arc::new(Self::try_new(
            Arc::clone(&children[0]),
            self.names.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        let stream = self.input.execute(partition, context)?;
        rename_record_batch_stream(stream, &self.names)
    }
}
