use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::{DataFusionError, Result, internal_err};
use sail_catalog::error::{CatalogError, CatalogObject};
use sail_catalog::lakehouse::{
    BeginTableAccessRequest, ResolveLakehouseTableRequest, TableAccessPurpose,
};
use sail_catalog::manager::CatalogManager;
use sail_common_datafusion::catalog::{LakehouseFormat, LakehouseOperation, TableKind};
use sail_common_datafusion::datasource::{DataSourceRegistry, OptionLayer, SourceInfo};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::lakeprocedure::{
    LakeProcedureAccess, LakeProcedureCall, LakeProcedureExecutionTarget, LakeProcedureResolution,
    LakeProcedureRootPlacement, LakeProcedureTarget,
};
use sail_common_datafusion::utils::items::ItemTaker;

/// Engine-owned physical boundary around a provider-planned procedure implementation.
#[derive(Debug, Clone)]
pub struct LakeProcedureExec {
    call: LakeProcedureCall,
    input: Arc<dyn ExecutionPlan>,
    root_placement: LakeProcedureRootPlacement,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl LakeProcedureExec {
    pub fn new(
        call: LakeProcedureCall,
        input: Arc<dyn ExecutionPlan>,
        root_placement: LakeProcedureRootPlacement,
    ) -> Self {
        let schema = call.invocation.procedure.schema();
        let properties = input.properties().clone();
        Self {
            call,
            input,
            root_placement,
            schema,
            properties,
        }
    }

    pub fn call(&self) -> &LakeProcedureCall {
        &self.call
    }

    pub fn schema_ref(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn root_placement(&self) -> LakeProcedureRootPlacement {
        self.root_placement
    }

    pub fn validate(&self) -> Result<()> {
        self.call.validate()?;
        if self.schema.as_ref() != self.input.schema().as_ref() {
            return internal_err!(
                "lake procedure implementation schema does not match its descriptor"
            );
        }
        Ok(())
    }
}

impl DisplayAs for LakeProcedureExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "LakeProcedureExec: lake_source={}, procedure={}, invocation_id={}",
            self.call.lake_source, self.call.invocation.procedure.name, self.call.invocation_id.0
        )
    }
}

impl ExecutionPlan for LakeProcedureExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        // The provider implementation owns the procedure's distribution shape.
        vec![false]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    #[expect(deprecated)]
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: datafusion::physical_plan::ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_new_children(children)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input = children.one()?;
        let procedure = Self::new(self.call.clone(), input, self.root_placement);
        procedure.validate()?;
        Ok(Arc::new(procedure))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.validate()?;
        self.input.execute(partition, context)
    }
}

/// Revalidates a bound call and reacquires any table access session immediately before execution.
pub async fn prepare_lake_procedure_execution(
    context: &TaskContext,
    call: &LakeProcedureCall,
) -> Result<LakeProcedureExecutionTarget> {
    call.validate()?;
    let manager = context.extension::<CatalogManager>()?;
    let registry = context.extension::<DataSourceRegistry>()?;
    let lake_source = registry.get_lake_source(&call.lake_source)?;
    let provider = lake_source.procedure_provider().ok_or_else(|| {
        catalog_error(CatalogError::NotSupported(format!(
            "lake source '{}' does not provide procedures",
            call.lake_source
        )))
    })?;
    match provider.resolve_procedure(&call.namespace, &call.invocation.procedure.name) {
        LakeProcedureResolution::Supported(procedure) if procedure == call.invocation.procedure => {
        }
        LakeProcedureResolution::Supported(_) => {
            return Err(catalog_error(CatalogError::Conflict(format!(
                "lake procedure '{}' changed after planning",
                call.invocation.procedure.name
            ))));
        }
        LakeProcedureResolution::Unsupported { reason } => {
            return Err(catalog_error(CatalogError::NotSupported(reason)));
        }
        LakeProcedureResolution::Unrecognized => {
            return Err(catalog_error(CatalogError::NotFound(
                CatalogObject::Function,
                call.invocation.procedure.name.clone(),
            )));
        }
    }

    Ok(match (&call.invocation.procedure.target, &call.target) {
        (LakeProcedureTarget::Catalog, None) => LakeProcedureExecutionTarget::Catalog {
            catalog: call.catalog.clone(),
        },
        (LakeProcedureTarget::Table { .. }, Some(target)) => {
            prepare_table_target(
                manager.as_ref(),
                &call.catalog,
                &call.lake_source,
                call.invocation.procedure.access,
                target,
            )
            .await?
        }
        _ => {
            return Err(catalog_error(CatalogError::Conflict(format!(
                "procedure target changed after planning for invocation {}",
                call.invocation_id.0
            ))));
        }
    })
}

async fn prepare_table_target(
    manager: &CatalogManager,
    catalog: &str,
    lake_source: &str,
    access: LakeProcedureAccess,
    target: &sail_common_datafusion::lakeprocedure::LakeProcedureTableTarget,
) -> Result<LakeProcedureExecutionTarget> {
    let table = &target.binding.catalog_table;
    if !table
        .first()
        .is_some_and(|table_catalog| table_catalog.eq_ignore_ascii_case(catalog))
    {
        return Err(catalog_error(CatalogError::InvalidArgument(format!(
            "cannot run procedure from catalog '{catalog}' against table '{}'",
            table.join(".")
        ))));
    }
    let status = manager.get_table(table).await.map_err(catalog_error)?;
    let TableKind::Table {
        location,
        format,
        properties,
        ..
    } = status.kind
    else {
        return Err(catalog_error(CatalogError::InvalidArgument(format!(
            "lakehouse procedure target is not a table: {}",
            table.join(".")
        ))));
    };
    if !format.eq_ignore_ascii_case(lake_source) {
        return Err(catalog_error(CatalogError::Conflict(format!(
            "procedure was bound to lake source '{lake_source}', but table '{}' now has format '{format}'",
            table.join(".")
        ))));
    }

    let resolved = manager
        .resolve_lakehouse_table(
            table,
            ResolveLakehouseTableRequest {
                catalog_table: table.clone(),
                operation: LakehouseOperation::Maintenance,
                requested_format: Some(LakehouseFormat::from_format_name(&format)),
                options: vec![],
            },
        )
        .await
        .map_err(catalog_error)?;
    if !target.binding.matches_execution(&resolved.execution) {
        return Err(catalog_error(CatalogError::StaleMetadata(format!(
            "procedure target binding changed after planning: {}",
            table.join(".")
        ))));
    }
    let purpose = match access {
        LakeProcedureAccess::MetadataRead => TableAccessPurpose::MetadataRead,
        LakeProcedureAccess::MetadataCommit => TableAccessPurpose::Commit,
    };
    let lakehouse_table = match manager
        .begin_table_access(
            table,
            BeginTableAccessRequest {
                context: resolved.execution.clone(),
                purpose,
            },
        )
        .await
    {
        Ok(session) => session.context,
        Err(CatalogError::NotSupported(_) | CatalogError::UnsupportedCapability(_)) => {
            resolved.execution
        }
        Err(error) => return Err(catalog_error(error)),
    };

    Ok(LakeProcedureExecutionTarget::Table(Box::new(SourceInfo {
        paths: location.into_iter().collect(),
        lakehouse_table: Some(lakehouse_table),
        schema: None,
        constraints: Default::default(),
        partition_by: vec![],
        bucket_by: None,
        sort_order: vec![],
        options: vec![OptionLayer::TablePropertyList { items: properties }],
        read_case_sensitive: true,
    })))
}

fn catalog_error(error: CatalogError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
