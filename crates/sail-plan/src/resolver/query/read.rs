use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::datasource::provider_as_source;
use datafusion_common::DFSchema;
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::{LogicalPlan, TableScan, UNNAMED_TABLE};
use sail_catalog::manager::CatalogManager;
use sail_catalog::provider::TableKind;
use sail_common::spec;
use sail_common_datafusion::datasource::SourceInfo;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::rename::logical_plan::rename_logical_plan;
use sail_common_datafusion::rename::table_provider::RenameTableProvider;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_data_source::default_registry;
use sail_python_udf::udf::pyspark_unresolved_udf::PySparkUnresolvedUDF;

use crate::error::{PlanError, PlanResult};
use crate::function::{get_built_in_table_function, is_built_in_generator_function};
use crate::resolver::function::PythonUdtf;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_read_named_table(
        &self,
        table: spec::ReadNamedTable,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::ReadNamedTable {
            name,
            temporal,
            sample,
            options,
        } = table;
        if temporal.is_some() {
            return Err(PlanError::todo("read table AS OF clause"));
        }
        if sample.is_some() {
            return Err(PlanError::todo("read table TABLESAMPLE clause"));
        }

        let table_reference = self.resolve_table_reference(&name)?;
        if let Some(cte) = state.get_cte(&table_reference) {
            return Ok(cte.clone());
        }

        let reference: Vec<String> = name.clone().into();
        let status = self
            .ctx
            .extension::<CatalogManager>()?
            .get_table_or_view(&reference)
            .await?;
        let plan = match status.kind {
            TableKind::Table {
                catalog: _,
                database: _,
                columns,
                comment: _,
                constraints,
                format,
                location,
                partition_by,
                sort_by,
                bucket_by,
                options: table_options,
                properties: _,
            } => {
                let schema = Schema::new(columns.iter().map(|x| x.field()).collect::<Vec<_>>());
                let constraints = self.resolve_catalog_table_constraints(constraints, &schema)?;
                let info = SourceInfo {
                    paths: location.map(|x| vec![x]).unwrap_or_default(),
                    schema: Some(schema),
                    constraints,
                    partition_by,
                    bucket_by: bucket_by.map(|x| x.into()),
                    sort_order: sort_by.into_iter().map(|x| x.into()).collect(),
                    // TODO: detect duplicated keys in each set of options
                    options: vec![
                        table_options.into_iter().collect(),
                        options.into_iter().collect(),
                    ],
                };
                let table_provider = default_registry()
                    .get_format(&format)?
                    .create_provider(&self.ctx.state(), info)
                    .await?;
                let names = state.register_fields(table_provider.schema().fields());
                let table_provider = RenameTableProvider::try_new(table_provider, names)?;
                LogicalPlan::TableScan(TableScan::try_new(
                    table_reference,
                    provider_as_source(Arc::new(table_provider)),
                    None,
                    vec![],
                    None,
                )?)
            }
            TableKind::View { .. } => return Err(PlanError::todo("read view")),
            TableKind::TemporaryView { plan, .. } | TableKind::GlobalTemporaryView { plan, .. } => {
                let names = state.register_fields(plan.schema().inner().fields());
                rename_logical_plan(plan.as_ref().clone(), &names)?
            }
        };
        Ok(plan)
    }

    pub(super) async fn resolve_query_read_udtf(
        &self,
        udtf: spec::ReadUdtf,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let mut scope = state.enter_config_scope();
        let state = scope.state();
        state.config_mut().arrow_allow_large_var_types = true;
        let spec::ReadUdtf {
            name,
            arguments,
            named_arguments,
            options,
        } = udtf;
        if !options.is_empty() {
            return Err(PlanError::todo("ReadType::UDTF options"));
        }
        let Ok(function_name) = <Vec<String>>::from(name).one() else {
            return Err(PlanError::unsupported("qualified table function name"));
        };
        let canonical_function_name = function_name.to_ascii_lowercase();
        if is_built_in_generator_function(&canonical_function_name) {
            let expr = spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                function_name: spec::ObjectName::bare(function_name),
                arguments,
                named_arguments,
                is_distinct: false,
                is_user_defined_function: false,
                is_internal: None,
                ignore_nulls: None,
                filter: None,
                order_by: None,
            });
            self.resolve_query_project(None, vec![expr], state).await
        } else {
            let udf = self.ctx.udf(&canonical_function_name).ok();
            if let Some(f) = udf
                .as_ref()
                .and_then(|x| x.inner().as_any().downcast_ref::<PySparkUnresolvedUDF>())
            {
                if f.eval_type().is_table_function() {
                    let udtf = PythonUdtf {
                        python_version: f.python_version().to_string(),
                        eval_type: f.eval_type(),
                        command: f.command().to_vec(),
                        return_type: f.output_type().clone(),
                    };
                    let input = self.resolve_query_empty(true)?;
                    let arguments = self
                        .resolve_named_expressions(arguments, input.schema(), state)
                        .await?;
                    self.resolve_python_udtf_plan(
                        udtf,
                        &function_name,
                        input,
                        arguments,
                        None,
                        None,
                        f.deterministic(),
                        state,
                    )
                } else {
                    Err(PlanError::invalid(format!(
                        "user-defined function is not a table function: {function_name}"
                    )))
                }
            } else {
                let schema = Arc::new(DFSchema::empty());
                let arguments = self.resolve_expressions(arguments, &schema, state).await?;
                let table_function =
                    if let Ok(f) = self.ctx.table_function(&canonical_function_name) {
                        f
                    } else if let Ok(f) = get_built_in_table_function(&canonical_function_name) {
                        f
                    } else {
                        return Err(PlanError::unsupported(format!(
                            "unknown table function: {function_name}"
                        )));
                    };
                let table_provider = table_function.create_table_provider(&arguments)?;
                let names = state.register_fields(table_provider.schema().fields());
                let table_provider = RenameTableProvider::try_new(table_provider, names)?;
                Ok(LogicalPlan::TableScan(TableScan::try_new(
                    function_name,
                    provider_as_source(Arc::new(table_provider)),
                    None,
                    vec![],
                    None,
                )?))
            }
        }
    }

    pub(super) async fn resolve_query_read_data_source(
        &self,
        source: spec::ReadDataSource,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::ReadDataSource {
            format,
            schema,
            options,
            paths,
            predicates,
        } = source;
        if !predicates.is_empty() {
            return Err(PlanError::todo("data source predicates"));
        }
        let Some(format) = format else {
            return Err(PlanError::invalid("missing data source format"));
        };
        let schema = match schema {
            Some(schema) => Some(self.resolve_schema(schema, state)?),
            None => None,
        };
        let info = SourceInfo {
            paths,
            schema,
            // TODO: detect duplicated keys in the set of options
            constraints: Default::default(),
            partition_by: vec![],
            bucket_by: None,
            sort_order: vec![],
            options: vec![options.into_iter().collect()],
        };
        let table_provider = default_registry()
            .get_format(&format)?
            .create_provider(&self.ctx.state(), info)
            .await?;
        let names = state.register_fields(table_provider.schema().fields());
        let table_provider = RenameTableProvider::try_new(table_provider, names)?;
        Ok(LogicalPlan::TableScan(TableScan::try_new(
            UNNAMED_TABLE,
            provider_as_source(Arc::new(table_provider)),
            None,
            vec![],
            None,
        )?))
    }
}
