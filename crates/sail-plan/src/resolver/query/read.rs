use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::datasource::{provider_as_source, TableProvider};
use datafusion_common::{DFSchema, TableReference};
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::{LogicalPlan, TableScan, UNNAMED_TABLE};
use rand::{rng, Rng};
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::catalog::TableKind;
use sail_common_datafusion::datasource::{SourceInfo, TableFormatRegistry};
use sail_common_datafusion::extension::SessionExtensionAccessor;
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

        let table_reference = self.resolve_table_reference(&name)?;
        if let Some(cte) = state.get_cte(&table_reference) {
            let plan = cte.clone();
            return if let Some(table_sample) = sample {
                self.apply_table_sample(plan, table_sample, state)
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
                let registry = self.ctx.extension::<TableFormatRegistry>()?;
                let table_provider = registry
                    .get(&format)?
                    .create_provider(&self.ctx.state(), info)
                    .await?;
                self.resolve_table_provider_with_rename(
                    table_provider,
                    table_reference,
                    None,
                    vec![],
                    None,
                    state,
                )?
            }
            TableKind::View { .. } => return Err(PlanError::todo("read view")),
            TableKind::TemporaryView { plan, .. } | TableKind::GlobalTemporaryView { plan, .. } => {
                let names = state.register_fields(plan.schema().inner().fields());
                rename_logical_plan(plan.as_ref().clone(), &names)?
            }
        };

        if let Some(table_sample) = sample {
            self.apply_table_sample(plan, table_sample, state)
        } else {
            Ok(plan)
        }
    }

    /// Apply TABLESAMPLE clause to a LogicalPlan
    fn apply_table_sample(
        &self,
        plan: LogicalPlan,
        table_sample: spec::TableSample,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::TableSample { method, seed } = table_sample;

        // Convert TableSampleMethod to sample bounds
        let (lower_bound, upper_bound) = match method {
            spec::TableSampleMethod::Percent { value } => {
                // Evaluate the percent expression to get a literal value
                let percent = self.evaluate_sample_expr_to_f64(&value)?;
                if !(0.0..=100.0).contains(&percent) {
                    return Err(PlanError::invalid(format!(
                        "TABLESAMPLE percent must be between 0 and 100, got {percent}"
                    )));
                }
                (0.0, percent / 100.0)
            }
            spec::TableSampleMethod::Rows { value: _ } => {
                // ROWS sampling is complex - it requires knowing total row count
                // For now, return a todo error
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

    /// Evaluate a sample expression to get a float value
    fn evaluate_sample_expr_to_f64(&self, expr: &spec::Expr) -> PlanResult<f64> {
        match expr {
            spec::Expr::Literal(lit) => match lit {
                spec::Literal::Int8 { value: Some(i) } => Ok(*i as f64),
                spec::Literal::Int16 { value: Some(i) } => Ok(*i as f64),
                spec::Literal::Int32 { value: Some(i) } => Ok(*i as f64),
                spec::Literal::Int64 { value: Some(l) } => Ok(*l as f64),
                spec::Literal::Float32 { value: Some(f) } => Ok(*f as f64),
                spec::Literal::Float64 { value: Some(d) } => Ok(*d),
                spec::Literal::Decimal128 {
                    value: Some(value),
                    scale,
                    ..
                } => {
                    let divisor = 10_f64.powi(*scale as i32);
                    Ok(*value as f64 / divisor)
                }
                spec::Literal::Decimal256 {
                    value: Some(value),
                    scale,
                    ..
                } => {
                    let divisor = 10_f64.powi(*scale as i32);
                    // i256 doesn't implement Into<f64>, use string conversion
                    let value_str = value.to_string();
                    let value_f64: f64 = value_str
                        .parse()
                        .map_err(|_| PlanError::invalid("invalid decimal value"))?;
                    Ok(value_f64 / divisor)
                }
                _ => Err(PlanError::invalid("TABLESAMPLE requires a numeric literal")),
            },
            // Handle Cast expressions (e.g., CAST(10 AS DOUBLE))
            spec::Expr::Cast { expr, .. } => self.evaluate_sample_expr_to_f64(expr),
            _ => Err(PlanError::invalid(
                "TABLESAMPLE requires a literal expression",
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
            options: vec![options.into_iter().collect()],
        };
        let registry = self.ctx.extension::<TableFormatRegistry>()?;
        let table_provider = registry
            .get(&format)?
            .create_provider(&self.ctx.state(), info)
            .await?;
        self.resolve_table_provider_with_rename(
            table_provider,
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
        let schema = table_provider.schema();

        let has_duplicates = {
            let mut seen = HashSet::new();
            schema.fields().iter().any(|f| !seen.insert(f.name()))
        };

        let table_provider = if has_duplicates {
            let names = state.register_fields(schema.fields());
            Arc::new(RenameTableProvider::try_new(table_provider, names)?)
        } else {
            table_provider
        };

        let table_scan = LogicalPlan::TableScan(TableScan::try_new(
            table_reference,
            provider_as_source(table_provider),
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
