use std::collections::HashSet;
use std::sync::Arc;

use chrono::{SecondsFormat, TimeZone, Utc};
use datafusion::arrow::datatypes::{DataType, Schema, TimeUnit};
use datafusion::datasource::{provider_as_source, source_as_provider, TableProvider};
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchema, ScalarValue, TableReference};
use datafusion_expr::{Expr, LogicalPlan, Projection, TableScan, TableSource, UNNAMED_TABLE};
use rand::{rng, RngExt};
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::catalog::TableKind;
use sail_common_datafusion::datasource::{SourceInfo, TableFormatRegistry};
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
                properties: _,
            } => {
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
                        table_options.into_iter().collect(),
                        options.into_iter().collect(),
                        temporal_options.into_iter().collect(),
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
                )?
            }
            TableKind::View { .. } => {
                if temporal.is_some() {
                    return Err(PlanError::unsupported(
                        "SQL time travel is not supported for views",
                    ));
                }
                return Err(PlanError::todo("read view"));
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

    async fn resolve_time_travel_options(
        &self,
        format: &str,
        temporal: Option<spec::TableTemporal>,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<(String, String)>> {
        let Some(temporal) = temporal else {
            return Ok(vec![]);
        };
        match format.to_ascii_lowercase().as_str() {
            "delta" => {
                self.resolve_delta_time_travel_options(temporal, state)
                    .await
            }
            "iceberg" => {
                self.resolve_iceberg_time_travel_options(temporal, state)
                    .await
            }
            other => Err(PlanError::unsupported(format!(
                "SQL time travel is only supported for Delta and Iceberg tables, got {other}",
            ))),
        }
    }

    async fn resolve_delta_time_travel_options(
        &self,
        temporal: spec::TableTemporal,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<(String, String)>> {
        match temporal {
            spec::TableTemporal::Timestamp { value } => Ok(vec![(
                "timestampAsOf".to_string(),
                self.evaluate_time_travel_timestamp(value, state).await?,
            )]),
            spec::TableTemporal::Version { value } => Ok(vec![(
                "versionAsOf".to_string(),
                self.evaluate_time_travel_version_i64(value, "version", state)
                    .await?
                    .to_string(),
            )]),
        }
    }

    async fn resolve_iceberg_time_travel_options(
        &self,
        temporal: spec::TableTemporal,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<(String, String)>> {
        match temporal {
            spec::TableTemporal::Timestamp { value } => Ok(vec![(
                "timestampAsOf".to_string(),
                self.evaluate_time_travel_timestamp(value, state).await?,
            )]),
            spec::TableTemporal::Version { value } => {
                match self
                    .evaluate_time_travel_version_for_iceberg(value, state)
                    .await?
                {
                    IcebergVersionAsOf::SnapshotId(snapshot_id) => {
                        Ok(vec![("snapshotId".to_string(), snapshot_id.to_string())])
                    }
                    IcebergVersionAsOf::Reference(reference) => {
                        Ok(vec![("ref".to_string(), reference)])
                    }
                }
            }
        }
    }

    async fn evaluate_time_travel_timestamp(
        &self,
        expr: spec::Expr,
        state: &mut PlanResolverState,
    ) -> PlanResult<String> {
        let resolved = self
            .resolve_time_travel_expression(expr, "timestamp", state)
            .await?;
        let timestamp = Expr::Cast(datafusion_expr::expr::Cast {
            expr: Box::new(resolved),
            data_type: DataType::Timestamp(
                TimeUnit::Microsecond,
                self.resolve_timezone(&spec::TimestampType::Configured)?,
            ),
        });
        let scalar = self.execute_time_travel_scalar(timestamp).await?;
        self.normalize_time_travel_timestamp_scalar(scalar)
    }

    async fn evaluate_time_travel_version_i64(
        &self,
        expr: spec::Expr,
        kind: &str,
        state: &mut PlanResolverState,
    ) -> PlanResult<i64> {
        let resolved = self
            .resolve_time_travel_expression(expr, kind, state)
            .await?;
        let scalar = self.execute_time_travel_scalar(resolved).await?;
        self.scalar_to_time_travel_i64(&scalar, kind)
    }

    async fn evaluate_time_travel_version_for_iceberg(
        &self,
        expr: spec::Expr,
        state: &mut PlanResolverState,
    ) -> PlanResult<IcebergVersionAsOf> {
        let resolved = self
            .resolve_time_travel_expression(expr, "version", state)
            .await?;
        let scalar = self.execute_time_travel_scalar(resolved).await?;
        match scalar {
            ScalarValue::Utf8(Some(value))
            | ScalarValue::LargeUtf8(Some(value))
            | ScalarValue::Utf8View(Some(value)) => {
                let value = value.trim().to_string();
                match value.parse::<i64>() {
                    Ok(snapshot_id) => Ok(IcebergVersionAsOf::SnapshotId(snapshot_id)),
                    Err(_) => Ok(IcebergVersionAsOf::Reference(value)),
                }
            }
            _ => Ok(IcebergVersionAsOf::SnapshotId(
                self.scalar_to_time_travel_i64(&scalar, "version")?,
            )),
        }
    }

    async fn resolve_time_travel_expression(
        &self,
        expr: spec::Expr,
        kind: &str,
        state: &mut PlanResolverState,
    ) -> PlanResult<Expr> {
        let schema = Arc::new(DFSchema::empty());
        let resolved = self
            .resolve_expression(expr, &schema, state)
            .await
            .map_err(|e| Self::invalid_time_travel_spec(kind, e))?;
        self.validate_time_travel_expression(&resolved, kind)?;
        Ok(resolved)
    }

    fn validate_time_travel_expression(&self, expr: &Expr, kind: &str) -> PlanResult<()> {
        if expr.any_column_refs() {
            return Err(Self::invalid_time_travel_message(
                kind,
                "expression cannot refer to any columns",
            ));
        }
        if expr.contains_outer() {
            return Err(Self::invalid_time_travel_message(
                kind,
                "expression cannot refer to any outer columns",
            ));
        }
        if expr.is_volatile() {
            return Err(Self::invalid_time_travel_message(
                kind,
                "expression must be deterministic",
            ));
        }
        let mut has_correlated_subquery = false;
        let mut has_volatile_subquery = false;
        expr.apply(|nested| {
            if let Expr::ScalarSubquery(subquery) = nested {
                if !subquery.outer_ref_columns.is_empty()
                    || subquery.subquery.contains_outer_reference()
                {
                    has_correlated_subquery = true;
                    return Ok(TreeNodeRecursion::Stop);
                }
                if Self::logical_plan_has_volatile_expressions(&subquery.subquery)? {
                    has_volatile_subquery = true;
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        if has_correlated_subquery {
            return Err(Self::invalid_time_travel_message(
                kind,
                "scalar subquery cannot be correlated",
            ));
        }
        if has_volatile_subquery {
            return Err(Self::invalid_time_travel_message(
                kind,
                "expression must be deterministic",
            ));
        }
        Ok(())
    }

    fn logical_plan_has_volatile_expressions(
        plan: &LogicalPlan,
    ) -> datafusion_common::Result<bool> {
        let mut contains = false;
        plan.apply(|node| {
            node.apply_expressions(|expr| {
                if expr.is_volatile() {
                    contains = true;
                    Ok(TreeNodeRecursion::Stop)
                } else {
                    Ok(TreeNodeRecursion::Continue)
                }
            })?;
            Ok(if contains {
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })?;
        Ok(contains)
    }

    async fn execute_time_travel_scalar(&self, expr: Expr) -> PlanResult<ScalarValue> {
        let plan = LogicalPlan::Projection(Projection::try_new(
            vec![expr],
            Arc::new(self.resolve_query_empty(true)?),
        )?);
        let batches = self.ctx.execute_logical_plan(plan).await?.collect().await?;
        let mut total_rows = 0usize;
        let mut value = None;
        for batch in batches {
            total_rows += batch.num_rows();
            if value.is_none() && batch.num_rows() > 0 {
                value = Some(ScalarValue::try_from_array(batch.column(0).as_ref(), 0)?);
            }
        }
        match (total_rows, value) {
            (1, Some(value)) => Ok(value),
            (rows, _) => Err(PlanError::invalid(format!(
                "Invalid time travel spec: expression must evaluate to a single value, got {rows} rows",
            ))),
        }
    }

    fn normalize_time_travel_timestamp_scalar(&self, scalar: ScalarValue) -> PlanResult<String> {
        match scalar {
            ScalarValue::TimestampMicrosecond(Some(value), Some(_)) => {
                let datetime = Utc.timestamp_micros(value).single().ok_or_else(|| {
                    PlanError::invalid("Invalid time travel spec: timestamp is out of range")
                })?;
                Ok(datetime.to_rfc3339_opts(SecondsFormat::Micros, true))
            }
            ScalarValue::TimestampMicrosecond(Some(value), None) => {
                let datetime = Utc.timestamp_micros(value).single().ok_or_else(|| {
                    PlanError::invalid("Invalid time travel spec: timestamp is out of range")
                })?;
                Ok(datetime
                    .naive_utc()
                    .format("%Y-%m-%d %H:%M:%S%.6f")
                    .to_string())
            }
            ScalarValue::TimestampMicrosecond(None, _) => Err(PlanError::invalid(
                "Invalid time travel spec: timestamp expression evaluated to NULL",
            )),
            other => Err(PlanError::invalid(format!(
                "Invalid time travel spec: timestamp expression must evaluate to a timestamp, got {other:?}",
            ))),
        }
    }

    fn scalar_to_time_travel_i64(&self, scalar: &ScalarValue, kind: &str) -> PlanResult<i64> {
        match scalar {
            ScalarValue::Int8(Some(value)) => Ok(i64::from(*value)),
            ScalarValue::Int16(Some(value)) => Ok(i64::from(*value)),
            ScalarValue::Int32(Some(value)) => Ok(i64::from(*value)),
            ScalarValue::Int64(Some(value)) => Ok(*value),
            ScalarValue::UInt8(Some(value)) => Ok(i64::from(*value)),
            ScalarValue::UInt16(Some(value)) => Ok(i64::from(*value)),
            ScalarValue::UInt32(Some(value)) => Ok(i64::from(*value)),
            ScalarValue::UInt64(Some(value)) => i64::try_from(*value).map_err(|_| {
                Self::invalid_time_travel_message(kind, "integer value is out of range")
            }),
            ScalarValue::Utf8(Some(value))
            | ScalarValue::LargeUtf8(Some(value))
            | ScalarValue::Utf8View(Some(value)) => value.trim().parse::<i64>().map_err(|_| {
                Self::invalid_time_travel_message(
                    kind,
                    "expression must evaluate to an integer or numeric string",
                )
            }),
            ScalarValue::Null => Err(Self::invalid_time_travel_message(
                kind,
                "expression evaluated to NULL",
            )),
            other => Err(Self::invalid_time_travel_message(
                kind,
                format!("expression must evaluate to an integer, got {other:?}"),
            )),
        }
    }

    fn invalid_time_travel_spec(kind: &str, error: impl std::fmt::Display) -> PlanError {
        Self::invalid_time_travel_message(kind, format!("failed to resolve expression: {error}"))
    }

    fn invalid_time_travel_message(kind: &str, message: impl Into<String>) -> PlanError {
        PlanError::invalid(format!(
            "Invalid time travel spec: {kind} {}.",
            message.into()
        ))
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

enum IcebergVersionAsOf {
    SnapshotId(i64),
    Reference(String),
}
