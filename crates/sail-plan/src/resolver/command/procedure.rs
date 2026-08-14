use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::{DFSchema, ScalarValue};
use sail_catalog::command::CatalogCommand;
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::catalog::TableKind;
use sail_common_datafusion::datasource::DataSourceRegistry;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::lakeprocedure::{
    LakeProcedure, LakeProcedureDataType, LakeProcedureInvocation, LakeProcedureResolution,
    LakeProcedureValue,
};
use sail_common_datafusion::literal::LiteralEvaluator;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) async fn resolve_command_call_procedure(
        &self,
        procedure_name: spec::ObjectName,
        arguments: Vec<spec::Expr>,
        named_arguments: Vec<(spec::Identifier, spec::Expr)>,
        state: &mut PlanResolverState,
    ) -> PlanResult<datafusion_expr::LogicalPlan> {
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
        if !procedure_namespace.eq_ignore_ascii_case("system") {
            return Err(PlanError::unsupported(format!(
                "Lakehouse procedures are only available in the system namespace: {}",
                procedure_parts.join(".")
            )));
        }

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

        let raw_table = positional_values.first().or_else(|| {
            named_values
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case("table"))
                .map(|(_, value)| value)
        });
        let raw_table = raw_table.ok_or_else(|| {
            PlanError::missing(format!(
                "Missing required argument 'table' for procedure {procedure_leaf}"
            ))
        })?;
        let table_name = scalar_to_procedure_value(raw_table, LakeProcedureDataType::Utf8)?;
        let LakeProcedureValue::Utf8(table_name) = table_name else {
            return Err(PlanError::invalid(
                "Procedure argument 'table' must be a non-null string",
            ));
        };
        let table_ast = sail_sql_analyzer::parser::parse_object_name(&table_name)?;
        let table_name = sail_sql_analyzer::expression::from_ast_object_name(table_ast)?;
        let table_reference: Vec<String> = table_name.into();

        let manager = self.ctx.extension::<CatalogManager>()?;
        let table_status = manager.get_table(&table_reference).await?;
        if let Some(procedure_catalog) = procedure_catalog
            && !table_status
                .catalog
                .as_deref()
                .is_some_and(|catalog| catalog.eq_ignore_ascii_case(procedure_catalog))
        {
            return Err(PlanError::invalid(format!(
                "Cannot run procedure from catalog '{procedure_catalog}' against table '{}' in catalog '{}'",
                table_name_for_display(&table_status),
                table_status.catalog.as_deref().unwrap_or("<unknown>")
            )));
        }
        let TableKind::Table { format, .. } = &table_status.kind else {
            return Err(PlanError::invalid(format!(
                "Lakehouse procedure target is not a table: {}",
                table_name_for_display(&table_status)
            )));
        };
        let registry = self.ctx.extension::<DataSourceRegistry>()?;
        let lake_source = registry.get_lake_source(format)?;
        let provider = lake_source
            .capabilities()
            .procedure_provider
            .ok_or_else(|| {
                PlanError::unsupported(format!(
                    "Lake source '{format}' does not provide procedures"
                ))
            })?;
        let procedure = match provider.resolve_procedure(procedure_leaf) {
            LakeProcedureResolution::Supported(procedure) => procedure,
            LakeProcedureResolution::Unsupported { reason } => {
                return Err(PlanError::unsupported(reason));
            }
            LakeProcedureResolution::Unrecognized => {
                return Err(PlanError::analysis(format!(
                    "Procedure not found: {}",
                    procedure_parts.join(".")
                )));
            }
        };
        let bound_arguments =
            bind_procedure_arguments(&procedure, positional_values, named_values)?;
        let mut canonical_table = Vec::new();
        canonical_table.extend(table_status.catalog.iter().cloned());
        canonical_table.extend(table_status.database.iter().cloned());
        canonical_table.push(table_status.name.clone());
        if canonical_table.len() == 1 {
            canonical_table = table_reference;
        }

        self.resolve_catalog_command(CatalogCommand::CallProcedure {
            table: canonical_table,
            invocation: LakeProcedureInvocation {
                procedure,
                arguments: bound_arguments,
            },
        })
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
