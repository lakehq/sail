use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::{Extension, LogicalPlan};
use sail_catalog::error::CatalogError;
use sail_catalog::lakehouse::{
    BeginTableAccessRequest, ResolveLakehouseTableRequest, TableAccessPurpose,
};
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::catalog::{
    LakehouseExecutionContext, LakehouseFormat, LakehouseOperation, LakehouseTableBinding,
    TableKind,
};
use sail_common_datafusion::datasource::DataSourceRegistry;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::lakeprocedure::{
    LakeProcedure, LakeProcedureAccess, LakeProcedureCall, LakeProcedureDataType,
    LakeProcedureInvocation, LakeProcedureInvocationId, LakeProcedureResolution,
    LakeProcedureTableTarget, LakeProcedureTarget, LakeProcedureValue,
};
use sail_common_datafusion::literal::LiteralEvaluator;
use uuid::Uuid;

use crate::error::{PlanError, PlanResult};
use crate::procedure::LakeProcedureNode;
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) async fn resolve_command_call_procedure(
        &self,
        procedure_name: spec::ObjectName,
        arguments: Vec<spec::Expr>,
        named_arguments: Vec<(spec::Identifier, spec::Expr)>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let procedure_parts = procedure_name
            .parts()
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<&str>>();
        let (procedure_catalog, procedure_namespace, procedure_leaf) = match procedure_parts
            .as_slice()
        {
            [namespace, procedure] => (None, *namespace, *procedure),
            [catalog, namespace, procedure] => (Some(*catalog), *namespace, *procedure),
            _ => {
                return Err(PlanError::invalid(format!(
                    "procedure name must be <namespace>.<procedure> or <catalog>.<namespace>.<procedure>: {}",
                    procedure_parts.join(".")
                )));
            }
        };

        let manager = self.ctx.extension::<CatalogManager>()?;
        let procedure_catalog = manager.resolve_catalog_reference(procedure_catalog)?;
        let procedure_namespace = vec![procedure_namespace.to_string()];
        let registry = self.ctx.extension::<DataSourceRegistry>()?;
        let mut resolved_procedure = None;
        for (lake_source_name, lake_source) in registry.lake_sources()? {
            let Some(provider) = lake_source.procedure_provider() else {
                continue;
            };
            match provider.resolve_procedure(&procedure_namespace, procedure_leaf) {
                LakeProcedureResolution::Supported(procedure) => {
                    resolved_procedure = Some((lake_source_name, procedure));
                    break;
                }
                LakeProcedureResolution::Unsupported { reason } => {
                    return Err(PlanError::unsupported(reason));
                }
                LakeProcedureResolution::Unrecognized => {}
            }
        }
        let Some((lake_source_name, procedure)) = resolved_procedure else {
            return Err(PlanError::analysis(format!(
                "Procedure not found: {}",
                procedure_parts.join(".")
            )));
        };

        let mut positional_values = Vec::with_capacity(arguments.len());
        for argument in arguments {
            positional_values.push(self.evaluate_procedure_argument(argument, state).await?);
        }
        let mut named_values = Vec::with_capacity(named_arguments.len());
        for (name, argument) in named_arguments {
            named_values.push((
                name.as_ref().to_string(),
                self.evaluate_procedure_argument(argument, state).await?,
            ));
        }

        let invocation = LakeProcedureInvocation {
            arguments: bind_procedure_arguments(&procedure, positional_values, named_values)?,
            procedure,
        };
        let target = match &invocation.procedure.target {
            LakeProcedureTarget::Catalog => None,
            LakeProcedureTarget::Table { parameter } => {
                let Some(LakeProcedureValue::Utf8(table_name)) = invocation.argument(parameter)
                else {
                    return Err(PlanError::invalid(format!(
                        "Procedure target argument '{parameter}' must be a non-null string"
                    )));
                };
                let table_ast = sail_sql_analyzer::parser::parse_object_name(table_name)?;
                let table_name = sail_sql_analyzer::expression::from_ast_object_name(table_ast)?;
                let table_reference: Vec<String> = table_name.into();
                let resolved_table = manager.resolve_table_reference_with_default_catalog(
                    &procedure_catalog,
                    &table_reference,
                )?;
                if !resolved_table
                    .catalog()
                    .eq_ignore_ascii_case(&procedure_catalog)
                {
                    return Err(PlanError::invalid(format!(
                        "Cannot run procedure from catalog '{procedure_catalog}' against table '{}' in catalog '{}'",
                        table_reference.join("."),
                        resolved_table.catalog()
                    )));
                }
                let table_status = manager.get_table_by_reference(&resolved_table).await?;
                let TableKind::Table { format, .. } = &table_status.kind else {
                    return Err(PlanError::invalid(format!(
                        "Lakehouse procedure target is not a table: {}",
                        table_name_for_display(&table_status)
                    )));
                };
                if !format.eq_ignore_ascii_case(&lake_source_name) {
                    return Err(PlanError::invalid(format!(
                        "Procedure '{}' is provided by lake source '{lake_source_name}' and cannot target table '{}' with format '{format}'",
                        invocation.procedure.name,
                        table_name_for_display(&table_status)
                    )));
                }
                let mut canonical_table = vec![resolved_table.catalog().to_string()];
                canonical_table.extend(table_status.database.iter().cloned());
                canonical_table.push(table_status.name.clone());
                let planned_context = resolve_procedure_table_binding(
                    manager.as_ref(),
                    &canonical_table,
                    format,
                    invocation.procedure.access,
                )
                .await?;
                Some(LakeProcedureTableTarget {
                    binding: LakehouseTableBinding::from_execution(&planned_context),
                })
            }
        };
        let call = LakeProcedureCall {
            invocation_id: LakeProcedureInvocationId(Uuid::new_v4().to_string()),
            catalog: procedure_catalog.to_string(),
            namespace: procedure_namespace,
            lake_source: lake_source_name,
            target,
            invocation,
        };
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(LakeProcedureNode::try_new(call)?),
        }))
    }

    async fn evaluate_procedure_argument(
        &self,
        argument: spec::Expr,
        state: &mut PlanResolverState,
    ) -> PlanResult<ScalarValue> {
        let schema = Arc::new(DFSchema::empty());
        let expression = self.resolve_expression(argument, &schema, state).await?;
        LiteralEvaluator::new()
            .evaluate(&expression)
            .map_err(|error| {
                PlanError::invalid(format!(
                    "Procedure arguments must be foldable constants: {error}"
                ))
            })
    }
}

async fn resolve_procedure_table_binding(
    manager: &CatalogManager,
    table: &[String],
    format: &str,
    access: LakeProcedureAccess,
) -> PlanResult<LakehouseExecutionContext> {
    let resolved = manager
        .resolve_lakehouse_table(
            table,
            ResolveLakehouseTableRequest {
                catalog_table: table.to_vec(),
                operation: LakehouseOperation::Maintenance,
                requested_format: Some(LakehouseFormat::from_format_name(format)),
                options: vec![],
            },
        )
        .await?;
    let binding = resolved.execution;
    match manager
        .begin_table_access(
            table,
            BeginTableAccessRequest {
                context: binding.clone(),
                purpose: procedure_access_purpose(access),
            },
        )
        .await
    {
        Ok(_) | Err(CatalogError::NotSupported(_) | CatalogError::UnsupportedCapability(_)) => {
            Ok(binding)
        }
        Err(error) => Err(error.into()),
    }
}

fn procedure_access_purpose(access: LakeProcedureAccess) -> TableAccessPurpose {
    match access {
        LakeProcedureAccess::MetadataRead => TableAccessPurpose::MetadataRead,
        LakeProcedureAccess::MetadataCommit => TableAccessPurpose::Commit,
    }
}

fn bind_procedure_arguments(
    procedure: &LakeProcedure,
    positional: Vec<ScalarValue>,
    named: Vec<(String, ScalarValue)>,
) -> PlanResult<Vec<LakeProcedureValue>> {
    if positional.len() > procedure.parameters.len() {
        return Err(PlanError::invalid(format!(
            "Too many arguments for procedure '{}': expected at most {}, got {}",
            procedure.name,
            procedure.parameters.len(),
            positional.len()
        )));
    }
    let mut values = vec![None; procedure.parameters.len()];
    for (index, value) in positional.into_iter().enumerate() {
        values[index] = Some(value);
    }
    for (name, value) in named {
        let index = procedure
            .parameters
            .iter()
            .position(|parameter| parameter.name.eq_ignore_ascii_case(&name))
            .ok_or_else(|| {
                PlanError::invalid(format!(
                    "Unknown argument '{name}' for procedure '{}'",
                    procedure.name
                ))
            })?;
        if values[index].is_some() {
            return Err(PlanError::invalid(format!(
                "Duplicate argument '{}' for procedure '{}'",
                procedure.parameters[index].name, procedure.name
            )));
        }
        values[index] = Some(value);
    }

    procedure
        .parameters
        .iter()
        .zip(values)
        .map(|(parameter, value)| match value {
            Some(value) => scalar_to_procedure_value(&value, parameter.data_type),
            None if parameter.required => Err(PlanError::missing(format!(
                "Missing required argument '{}' for procedure '{}'",
                parameter.name, procedure.name
            ))),
            None => Ok(LakeProcedureValue::Null),
        })
        .collect()
}

fn scalar_to_procedure_value(
    value: &ScalarValue,
    data_type: LakeProcedureDataType,
) -> PlanResult<LakeProcedureValue> {
    if value.is_null() {
        return Ok(LakeProcedureValue::Null);
    }
    let target = match data_type {
        LakeProcedureDataType::Boolean => DataType::Boolean,
        LakeProcedureDataType::Int32 => DataType::Int32,
        LakeProcedureDataType::Int64 => DataType::Int64,
        LakeProcedureDataType::Utf8 => DataType::Utf8,
        LakeProcedureDataType::TimestampMicros => DataType::Timestamp(TimeUnit::Microsecond, None),
    };
    let value = value.cast_to(&target).map_err(|error| {
        PlanError::invalid(format!(
            "Cannot cast procedure argument {value:?} to {target}: {error}"
        ))
    })?;
    match value {
        ScalarValue::Boolean(Some(value)) => Ok(LakeProcedureValue::Boolean(value)),
        ScalarValue::Int32(Some(value)) => Ok(LakeProcedureValue::Int32(value)),
        ScalarValue::Int64(Some(value)) => Ok(LakeProcedureValue::Int64(value)),
        ScalarValue::Utf8(Some(value)) => Ok(LakeProcedureValue::Utf8(value)),
        ScalarValue::TimestampMicrosecond(Some(value), _) => {
            Ok(LakeProcedureValue::TimestampMicros(value))
        }
        value => Err(PlanError::invalid(format!(
            "Unsupported procedure argument value after cast: {value:?}"
        ))),
    }
}

fn table_name_for_display(status: &sail_common_datafusion::catalog::TableStatus) -> String {
    status
        .catalog
        .iter()
        .chain(status.database.iter())
        .chain(std::iter::once(&status.name))
        .cloned()
        .collect::<Vec<_>>()
        .join(".")
}
