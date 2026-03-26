use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::{exec_err, internal_err, Result};
use sail_catalog::command::CatalogCommand;
use sail_catalog::manager::CatalogManager;
use sail_common_datafusion::extension::SessionExtensionAccessor;

/// A physical plan node that executes a [`CatalogCommand`].
///
/// This node has a single output partition and no children.
/// When executed, it delegates to [`CatalogCommand::execute()`] using the [`TaskContext`]
/// to obtain both the [`CatalogManager`] and any session-level services.
#[derive(Debug, Clone)]
pub struct CatalogCommandExec {
    command: CatalogCommand,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl CatalogCommandExec {
    pub fn new(command: CatalogCommand, schema: SchemaRef) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            command,
            schema,
            properties,
        }
    }

    pub fn command(&self) -> &CatalogCommand {
        &self.command
    }
}

impl DisplayAs for CatalogCommandExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CatalogCommandExec: {}", self.command.name())
    }
}

impl ExecutionPlan for CatalogCommandExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return internal_err!("{} should not have children", self.name());
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return exec_err!(
                "{} expects only partition 0 but got {}",
                self.name(),
                partition
            );
        }
        let command = self.command.clone();
        let schema = self.schema.clone();
        let stream = futures::stream::once(async move {
            let manager = context.extension::<CatalogManager>()?;
            let batch = command
                .execute(context.as_ref(), manager.as_ref())
                .await
                .map_err(|e| datafusion_common::exec_datafusion_err!("{e}"))?;
            Ok(batch)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
