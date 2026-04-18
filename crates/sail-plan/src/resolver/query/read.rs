use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::datasource::{provider_as_source, source_as_provider, TableProvider};
use datafusion_common::{DFSchema, ScalarValue, TableReference};
use datafusion_expr::{Expr, LogicalPlan, SubqueryAlias, TableScan, TableSource, UNNAMED_TABLE};
use rand::{rng, RngExt};
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::catalog::{TableColumnStatus, TableKind};
use sail_common_datafusion::datasource::{OptionLayer, SourceInfo, TableFormatRegistry};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::literal::LiteralEvaluator;
use sail_common_datafusion::rename::logical_plan::rename_logical_plan;
use sail_common_datafusion::rename::table_provider::RenameTableProvider;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_python_udf::udf::pyspark_unresolved_udf::PySparkUnresolvedUDF;

use crate::error::{PlanError, PlanResult};
use crate::function::{get_built_in_table_function, is_built_in_generator_function};
use crate::resolver::function::PythonUdtf;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    /// Resolves a named table or view reference into a logical plan node.
    /// Looks up the name in the catalog and produces the appropriate plan
    /// depending on whether it's a table, view, or temporary view.
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

        // Check if the name is in the form `<format>.<path>` where `<format>` is a
        // registered table format. In that case, treat it as a direct data source read.
        if let [format, path] = name.parts() {
            let format = format.as_ref().to_ascii_lowercase();
            let registry = self.ctx.extension::<TableFormatRegistry>()?;
            if registry.get(&format).is_ok() {
                let temporal_options = self
                    .resolve_time_travel_options(&format, temporal, state)
                    .await?;
                let source = spec::ReadDataSource {
                    format: Some(format),
                    schema: None,
                    options: options.into_iter().chain(temporal_options).collect(),
                    paths: vec![path.as_ref().to_string()],
                    predicates: vec![],
                };
                let plan = self.resolve_query_read_data_source(source, state).await?;
                return if let Some(table_sample) = sample {
                    self.apply_table_sample(plan, table_sample, state).await
                } else {
                    Ok(plan)
                };
            }
        }

        let table_reference = self.resolve_table_reference(&name)?;
        if let Some(cte) = state.get_cte(&table_reference) {
            if temporal.is_some() {
                return Err(PlanError::unsupported(
                    "SQL time travel is not supported for CTEs",
                ));
            }
            let plan = cte.clone();
            return if let Some(table_sample) = sample {
                self.apply_table_sample(plan, table_sample, state).await
            } else {
                Ok(plan)
            };
        }

        let reference: Vec<String> = name.clone().into();
        let status = self
            .ctx
            .extension::<CatalogManager>()?
            .get_table_or_view(&reference)
            .await?;
        let plan = match status.kind {
            TableKind::Table {
                columns,
                comment: _,
                constraints,
                format,
                location,
                partition_by,
                sort_by,
                bucket_by,
                options: table_options,
                properties: table_properties,
            } => {
                self.resolve_table_kind_table(
                    columns,
                    constraints,
                    format,
                    location,
                    partition_by,
                    sort_by,
                    bucket_by,
                    table_options,
                    table_properties,
                    temporal,
                    options,
                    table_reference,
                    state,
                )
                .await?
            }
            TableKind::View {
                definition,
                columns,
                ..
            } => {
                if temporal.is_some() {
                    return Err(PlanError::unsupported(
                        "SQL time travel is not supported for views",
                    ));
                }
                self.resolve_table_kind_view(definition, columns, table_reference.clone(), state)
                    .await?
            }
            TableKind::TemporaryView { plan, .. } | TableKind::GlobalTemporaryView { plan, .. } => {
                if temporal.is_some() {
                    return Err(PlanError::unsupported(
                        "SQL time travel is not supported for temporary views",
                    ));
                }
                let names = state.register_fields(plan.schema().inner().fields());
                rename_logical_plan(plan.as_ref().clone(), &names)?
            }
        };

        if let Some(table_sample) = sample {
            self.apply_table_sample(plan, table_sample, state).await
        } else {
            Ok(plan)
        }
    }

    pub(super) async fn resolve_query_read_dynamic_table(
        &self,
        table: spec::ReadDynamicTable,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::ReadDynamicTable {
            name,
            sample,
            options,
        } = table;
        let schema = Arc::new(DFSchema::empty());
        let resolved = self.resolve_expression(name, &schema, state).await?;
        let name_str = self.evaluate_identifier_expr(resolved, state)?;
        let name = sail_sql_analyzer::expression::from_ast_object_name(
            sail_sql_analyzer::parser::parse_object_name(&name_str)?,
        )?;
        self.resolve_query_read_named_table(
            spec::ReadNamedTable {
                name,
                temporal: None,
                sample,
                options,
            },
            state,
        )
        .await
    }

    /// Resolves a physical table into a TableScan logical plan node.
    #[expect(clippy::too_many_arguments)]
    async fn resolve_table_kind_table(
        &self,
        columns: Vec<TableColumnStatus>,
        constraints: Vec<sail_common_datafusion::catalog::CatalogTableConstraint>,
        format: String,
        location: Option<String>,
        partition_by: Vec<sail_common_datafusion::catalog::CatalogPartitionField>,
        sort_by: Vec<sail_common_datafusion::catalog::CatalogTableSort>,
        bucket_by: Option<sail_common_datafusion::catalog::CatalogTableBucketBy>,
        table_options: Vec<(String, String)>,
        table_properties: Vec<(String, String)>,
        temporal: Option<spec::TableTemporal>,
        options: Vec<(String, String)>,
        table_reference: impl Into<TableReference>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let schema = Schema::new(columns.iter().map(|x| x.field()).collect::<Vec<_>>());
        let constraints = self.resolve_catalog_table_constraints(constraints, &schema)?;
        let temporal_options = self
            .resolve_time_travel_options(&format, temporal, state)
            .await?;
        let info = SourceInfo {
            paths: location.map(|x| vec![x]).unwrap_or_default(),
            schema: Some(schema),
            constraints,
            partition_by: partition_by.into_iter().map(|field| field.column).collect(),
            bucket_by: bucket_by.map(|x| x.into()),
            sort_order: sort_by.into_iter().map(|x| x.into()).collect(),
            // TODO: detect duplicated keys in each set of options
            options: vec![
                OptionLayer::TablePropertyList {
                    items: table_options.into_iter().chain(table_properties).collect(),
                },
                OptionLayer::OptionList { items: options },
                OptionLayer::OptionList {
                    items: temporal_options,
                },
            ],
        };
        let registry = self.ctx.extension::<TableFormatRegistry>()?;
        let table_source = registry
            .get(&format)?
            .create_source(&self.ctx.state(), info)
            .await?;
        self.resolve_table_source_with_rename(
            table_source,
            table_reference,
            None,
            vec![],
            None,
            state,
        )
    }

    /// Resolves a persistent view by re-parsing its SQL definition into a logical plan.
    async fn resolve_table_kind_view(
        &self,
        definition: String,
        columns: Vec<TableColumnStatus>,
        table_reference: TableReference,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let ast = sail_sql_analyzer::parser::parse_one_statement(&definition)?;
        let spec_plan = sail_sql_analyzer::statement::from_ast_statement(ast)?;
        let plan = match spec_plan {
            spec::Plan::Query(query_plan) => self.resolve_query_plan(query_plan, state).await?,
            _ => {
                return Err(PlanError::invalid("view definition must be a query"));
            }
        };
        let plan =
            LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(Arc::new(plan), table_reference)?);
        if columns.is_empty() {
            Ok(plan)
        } else {
            let names = state.register_field_names(columns.iter().map(|c| &c.name));
            Ok(rename_logical_plan(plan, &names)?)
        }
    }

    /// Apply TABLESAMPLE clause to a LogicalPlan
    pub(super) async fn apply_table_sample(
        &self,
        plan: LogicalPlan,
        table_sample: spec::TableSample,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::TableSample { method, seed } = table_sample;

        // Convert TableSampleMethod to sample bounds
        let (lower_bound, upper_bound) = match method {
            spec::TableSampleMethod::Percent { value } => {
                let percent = self.evaluate_sample_expr_to_f64(value, state).await?;
                let fraction = percent / 100.0;
                if !(0.0..=1.0).contains(&fraction) {
                    return Err(PlanError::invalid(format!(
                        "Sampling fraction ({fraction}) must be on interval [0, 1]"
                    )));
                }
                (0.0, fraction)
            }
            spec::TableSampleMethod::Rows { value: _ } => {
                return Err(PlanError::todo("TABLESAMPLE with ROWS"));
            }
            spec::TableSampleMethod::Bucket {
                numerator,
                denominator,
            } => {
                if numerator == 0 || numerator > denominator {
                    return Err(PlanError::invalid(format!(
                        "invalid TABLESAMPLE bucket: {numerator} out of {denominator}"
                    )));
                }
                let fraction = 1.0 / denominator as f64;
                let lower = (numerator - 1) as f64 * fraction;
                let upper = numerator as f64 * fraction;
                (lower, upper)
            }
        };

        // Use random seed if not provided
        let seed: i64 = seed.unwrap_or_else(|| {
            let mut r = rng();
            r.random::<i64>()
        });

        // TABLESAMPLE is without replacement
        Self::apply_sample_to_plan(plan, lower_bound, upper_bound, false, seed, state)
    }

    /// Evaluate a sample expression to get a float value.
    /// Resolves the spec expression using an empty schema and uses [LiteralEvaluator]
    /// to support constant expressions beyond just literals.
    async fn evaluate_sample_expr_to_f64(
        &self,
        expr: spec::Expr,
        state: &mut PlanResolverState,
    ) -> PlanResult<f64> {
        let schema = Arc::new(DFSchema::empty());
        let resolved = self.resolve_expression(expr, &schema, state).await?;
        let cast_expr = Expr::Cast(datafusion_expr::expr::Cast {
            expr: Box::new(resolved),
            data_type: DataType::Float64,
        });
        let evaluator = LiteralEvaluator::new();
        let scalar = evaluator
            .evaluate(&cast_expr)
            .map_err(|e| PlanError::invalid(e.to_string()))?;
        match scalar {
            ScalarValue::Float64(Some(v)) => Ok(v),
            _ => Err(PlanError::invalid(
                "TABLESAMPLE requires a numeric expression",
            )),
        }
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
            let catalog_manager = self.ctx.extension::<CatalogManager>()?;
            let udf = catalog_manager.get_function(&canonical_function_name)?;
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
                        &[], // ReadUdtf kwargs come via named_arguments, not NamedArgument exprs
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
                self.resolve_table_provider_with_rename(
                    table_provider,
                    function_name,
                    None,
                    vec![],
                    None,
                    state,
                )
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
            options: vec![OptionLayer::OptionList {
                items: options.into_iter().collect(),
            }],
        };
        let registry = self.ctx.extension::<TableFormatRegistry>()?;
        let table_source = registry
            .get(&format)?
            .create_source(&self.ctx.state(), info)
            .await?;
        self.resolve_table_source_with_rename(
            table_source,
            UNNAMED_TABLE,
            None,
            vec![],
            None,
            state,
        )
    }

    pub(super) fn resolve_table_provider_with_rename(
        &self,
        table_provider: Arc<dyn TableProvider>,
        table_reference: impl Into<TableReference>,
        projection: Option<Vec<usize>>,
        filters: Vec<datafusion_expr::expr::Expr>,
        fetch: Option<usize>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        self.resolve_table_source_with_rename(
            provider_as_source(table_provider),
            table_reference,
            projection,
            filters,
            fetch,
            state,
        )
    }

    pub(super) fn resolve_table_source_with_rename(
        &self,
        table_source: Arc<dyn TableSource>,
        table_reference: impl Into<TableReference>,
        projection: Option<Vec<usize>>,
        filters: Vec<datafusion_expr::expr::Expr>,
        fetch: Option<usize>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let schema = table_source.schema();

        let has_duplicates = {
            let mut seen = HashSet::new();
            schema.fields().iter().any(|f| !seen.insert(f.name()))
        };

        let table_source: Arc<dyn TableSource> = if has_duplicates {
            // Preserve existing behavior by wrapping the underlying TableProvider with renaming,
            // but only if this TableSource is DataFusion's DefaultTableSource.
            let provider = source_as_provider(&table_source).map_err(|e| {
                PlanError::unsupported(format!(
                    "duplicate column names require DefaultTableSource-backed TableProvider: {e}"
                ))
            })?;
            let names = state.register_fields(schema.fields());
            provider_as_source(Arc::new(RenameTableProvider::try_new(provider, names)?))
        } else {
            table_source
        };

        let table_scan = LogicalPlan::TableScan(TableScan::try_new(
            table_reference,
            table_source,
            projection,
            filters,
            fetch,
        )?);

        if !has_duplicates {
            let names = state.register_fields(table_scan.schema().fields());
            Ok(rename_logical_plan(table_scan, &names)?)
        } else {
            Ok(table_scan)
        }
    }
}
