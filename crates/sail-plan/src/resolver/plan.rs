use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_recursion::async_recursion;
use datafusion::arrow::datatypes as adt;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::file_format::arrow::{ArrowFormat, ArrowFormatFactory};
use datafusion::datasource::file_format::avro::{AvroFormat, AvroFormatFactory};
use datafusion::datasource::file_format::csv::{CsvFormat, CsvFormatFactory};
use datafusion::datasource::file_format::json::{JsonFormat, JsonFormatFactory};
use datafusion::datasource::file_format::parquet::{ParquetFormat, ParquetFormatFactory};
use datafusion::datasource::file_format::{format_as_file_type, FileFormat, FileFormatFactory};
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion::datasource::{provider_as_source, MemTable, TableProvider};
use datafusion::functions::core::expr_ext::FieldAccessor;
use datafusion::logical_expr::{logical_plan as plan, Expr, Extension, LogicalPlan, UNNAMED_TABLE};
use datafusion::prelude::coalesce;
use datafusion_common::config::{ConfigFileType, TableOptions};
use datafusion_common::display::{PlanType, StringifiedPlan, ToStringifiedPlan};
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRewriter};
use datafusion_common::{
    Column, DFSchema, DFSchemaRef, JoinType, ParamValues, ScalarValue, TableReference, ToDFSchema,
};
use datafusion_expr::builder::project;
use datafusion_expr::dml::InsertOp;
use datafusion_expr::expr::{ScalarFunction, Sort};
use datafusion_expr::expr_rewriter::normalize_col;
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::utils::{
    columnize_expr, conjunction, expand_qualified_wildcard, expand_wildcard, expr_as_column_expr,
    find_aggregate_exprs,
};
use datafusion_expr::{
    build_join_schema, col, expr, ident, lit, when, Aggregate, AggregateUDF, BinaryExpr,
    ExprSchemable, LogicalPlanBuilder, Operator, Projection, ScalarUDF, TryCast,
};
use sail_common::spec;
use sail_common::spec::TableFileFormat;
use sail_common_datafusion::utils::{cast_record_batch, read_record_batches, rename_logical_plan};
use sail_python_udf::cereal::pyspark_udf::PySparkUdfPayload;
use sail_python_udf::get_udf_name;
use sail_python_udf::udf::pyspark_batch_collector::PySparkBatchCollectorUDF;
use sail_python_udf::udf::pyspark_cogroup_map_udf::PySparkCoGroupMapUDF;
use sail_python_udf::udf::pyspark_group_map_udf::PySparkGroupMapUDF;
use sail_python_udf::udf::pyspark_map_iter_udf::{PySparkMapIterKind, PySparkMapIterUDF};
use sail_python_udf::udf::pyspark_unresolved_udf::PySparkUnresolvedUDF;

use crate::error::{PlanError, PlanResult};
use crate::extension::function::multi_expr::MultiExpr;
use crate::extension::logical::{
    CatalogCommand, CatalogCommandNode, MapPartitionsNode, RangeNode, ShowStringFormat,
    ShowStringNode, ShowStringStyle, SortWithinPartitionsNode,
};
use crate::extension::source::rename::RenameTableProvider;
use crate::function::{
    get_built_in_table_function, get_outer_built_in_generator_functions,
    is_built_in_generator_function,
};
use crate::resolver::expression::NamedExpr;
use crate::resolver::function::PythonUdtf;
use crate::resolver::state::PlanResolverState;
use crate::resolver::tree::explode::ExplodeRewriter;
use crate::resolver::tree::window::WindowRewriter;
use crate::resolver::tree::PlanRewriter;
use crate::resolver::PlanResolver;
use crate::temp_view::manage_temporary_views;
use crate::utils::ItemTaker;

#[derive(Debug)]
pub struct NamedPlan {
    pub plan: LogicalPlan,
    /// The user-facing fields for query plan,
    /// or `None` for a non-query plan (e.g. a DDL statement).
    pub fields: Option<Vec<String>>,
}

impl PlanResolver<'_> {
    pub async fn resolve_named_plan(&self, plan: spec::Plan) -> PlanResult<NamedPlan> {
        let mut state = PlanResolverState::new();
        match plan {
            spec::Plan::Query(query) => {
                let plan = self.resolve_query_plan(query, &mut state).await?;
                let fields = Some(Self::get_field_names(plan.schema(), &state)?);
                Ok(NamedPlan { plan, fields })
            }
            spec::Plan::Command(command) => {
                let plan = self.resolve_command_plan(command, &mut state).await?;
                Ok(NamedPlan { plan, fields: None })
            }
        }
    }

    /// Resolve query plan.
    /// The resolved plan may contain hidden fields.
    /// If the hidden fields cannot be handled,
    /// [`Self::resolve_query_plan`] should be used instead,
    #[async_recursion]
    pub(super) async fn resolve_query_plan_with_hidden_fields(
        &self,
        plan: spec::QueryPlan,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        use spec::QueryNode;

        let plan_id = plan.plan_id;
        let plan = match plan.node {
            QueryNode::Read {
                read_type,
                is_streaming: _,
            } => match read_type {
                spec::ReadType::NamedTable(table) => {
                    self.resolve_query_read_named_table(table, state).await?
                }
                spec::ReadType::Udtf(udtf) => self.resolve_query_read_udtf(udtf, state).await?,
                spec::ReadType::DataSource(source) => {
                    self.resolve_query_read_data_source(source, state).await?
                }
            },
            QueryNode::Project { input, expressions } => {
                self.resolve_query_project(input.map(|x| *x), expressions, state)
                    .await?
            }
            QueryNode::Filter { input, condition } => {
                self.resolve_query_filter(*input, condition, state).await?
            }
            QueryNode::Join(join) => self.resolve_query_join(join, state).await?,
            QueryNode::SetOperation(op) => self.resolve_query_set_operation(op, state).await?,
            QueryNode::Sort {
                input,
                order,
                is_global,
            } => {
                self.resolve_query_sort(*input, order, is_global, state)
                    .await?
            }
            QueryNode::Limit { input, skip, limit } => {
                self.resolve_query_limit(*input, skip, limit, state).await?
            }
            QueryNode::Aggregate(aggregate) => {
                self.resolve_query_aggregate(aggregate, state).await?
            }
            QueryNode::WithParameters {
                input,
                positional_arguments,
                named_arguments,
            } => {
                self.resolve_query_with_parameters(
                    *input,
                    positional_arguments,
                    named_arguments,
                    state,
                )
                .await?
            }
            QueryNode::LocalRelation { data, schema } => {
                self.resolve_query_local_relation(data, schema, state)
                    .await?
            }
            QueryNode::Sample(sample) => self.resolve_query_sample(sample, state).await?,
            QueryNode::Deduplicate(deduplicate) => {
                self.resolve_query_deduplicate(deduplicate, state).await?
            }
            QueryNode::Range(range) => self.resolve_query_range(range, state).await?,
            QueryNode::SubqueryAlias {
                input,
                alias,
                qualifier,
            } => {
                self.resolve_query_subquery_alias(*input, alias, qualifier, state)
                    .await?
            }
            QueryNode::Repartition {
                input,
                num_partitions,
                shuffle: _,
            } => {
                self.resolve_query_repartition(*input, num_partitions, state)
                    .await?
            }
            QueryNode::ToDf {
                input,
                column_names,
            } => {
                self.resolve_query_to_df(*input, column_names, state)
                    .await?
            }
            QueryNode::WithColumnsRenamed {
                input,
                rename_columns_map,
            } => {
                self.resolve_query_with_columns_renamed(*input, rename_columns_map, state)
                    .await?
            }
            QueryNode::Drop {
                input,
                columns,
                column_names,
            } => {
                self.resolve_query_drop(*input, columns, column_names, state)
                    .await?
            }
            QueryNode::Tail { input, limit } => {
                self.resolve_query_tail(*input, limit, state).await?
            }
            QueryNode::WithColumns { input, aliases } => {
                self.resolve_query_with_columns(*input, aliases, state)
                    .await?
            }
            QueryNode::Hint {
                input,
                name,
                parameters,
            } => {
                self.resolve_query_hint(*input, name, parameters, state)
                    .await?
            }
            QueryNode::Pivot(pivot) => self.resolve_query_pivot(pivot, state).await?,
            QueryNode::Unpivot(unpivot) => self.resolve_query_unpivot(unpivot, state).await?,
            QueryNode::ToSchema { input, schema } => {
                self.resolve_query_to_schema(*input, schema, state).await?
            }
            QueryNode::RepartitionByExpression {
                input,
                partition_expressions,
                num_partitions,
            } => {
                self.resolve_query_repartition_by_expression(
                    *input,
                    partition_expressions,
                    num_partitions,
                    state,
                )
                .await?
            }
            QueryNode::MapPartitions {
                input,
                function,
                is_barrier,
            } => {
                self.resolve_query_map_partitions(*input, function, is_barrier, state)
                    .await?
            }
            QueryNode::CollectMetrics {
                input,
                name,
                metrics,
            } => {
                self.resolve_query_collect_metrics(*input, name, metrics, state)
                    .await?
            }
            QueryNode::Parse(parse) => self.resolve_query_parse(parse, state).await?,
            QueryNode::GroupMap(map) => self.resolve_query_group_map(map, state).await?,
            QueryNode::CoGroupMap(map) => self.resolve_query_co_group_map(map, state).await?,
            QueryNode::WithWatermark(watermark) => {
                self.resolve_query_with_watermark(watermark, state).await?
            }
            QueryNode::ApplyInPandasWithState(apply) => {
                self.resolve_query_apply_in_pandas_with_state(apply, state)
                    .await?
            }
            QueryNode::CachedLocalRelation { .. } => {
                return Err(PlanError::todo("cached local relation"));
            }
            QueryNode::CachedRemoteRelation { .. } => {
                return Err(PlanError::todo("cached remote relation"));
            }
            QueryNode::CommonInlineUserDefinedTableFunction(udtf) => {
                self.resolve_query_common_inline_udtf(udtf, state).await?
            }
            QueryNode::FillNa {
                input,
                columns,
                values,
            } => {
                self.resolve_query_fill_na(*input, columns, values, state)
                    .await?
            }
            QueryNode::DropNa {
                input,
                columns,
                min_non_nulls,
            } => {
                self.resolve_query_drop_na(*input, columns, min_non_nulls, state)
                    .await?
            }
            QueryNode::Replace {
                input,
                columns,
                replacements,
            } => {
                self.resolve_query_replace(*input, columns, replacements, state)
                    .await?
            }
            QueryNode::StatSummary { input, statistics } => {
                self.resolve_query_summary(*input, vec![], statistics, state)
                    .await?
            }
            QueryNode::StatCrosstab {
                input,
                left_column,
                right_column,
            } => {
                self.resolve_query_cross_tab(*input, left_column, right_column, state)
                    .await?
            }
            QueryNode::StatDescribe { input, columns } => {
                let statistics = vec![
                    "count".to_string(),
                    "mean".to_string(),
                    "stddev".to_string(),
                    "min".to_string(),
                    "max".to_string(),
                ];
                self.resolve_query_summary(*input, columns, statistics, state)
                    .await?
            }
            QueryNode::StatCov {
                input,
                left_column,
                right_column,
            } => {
                self.resolve_query_stat_cov(*input, left_column, right_column, state)
                    .await?
            }
            QueryNode::StatCorr {
                input,
                left_column,
                right_column,
                method,
            } => {
                self.resolve_query_stat_corr(*input, left_column, right_column, method, state)
                    .await?
            }
            QueryNode::StatApproxQuantile { .. } => {
                return Err(PlanError::todo("approx quantile"));
            }
            QueryNode::StatFreqItems { .. } => {
                return Err(PlanError::todo("freq items"));
            }
            QueryNode::StatSampleBy { .. } => {
                return Err(PlanError::todo("sample by"));
            }
            QueryNode::Empty { produce_one_row } => {
                LogicalPlan::EmptyRelation(plan::EmptyRelation {
                    produce_one_row,
                    schema: DFSchemaRef::new(DFSchema::empty()),
                })
            }
            QueryNode::Values(values) => self.resolve_query_values(values, state).await?,
            QueryNode::TableAlias {
                input,
                name,
                columns,
            } => {
                self.resolve_query_table_alias(*input, name, columns, state)
                    .await?
            }
            QueryNode::WithCtes {
                input,
                recursive,
                ctes,
            } => {
                self.resolve_query_with_ctes(*input, recursive, ctes, state)
                    .await?
            }
            QueryNode::LateralView {
                input,
                function,
                arguments,
                named_arguments,
                table_alias,
                column_aliases,
                outer,
            } => {
                self.resolve_query_lateral_view(
                    input.map(|x| *x),
                    function,
                    arguments,
                    named_arguments,
                    table_alias,
                    column_aliases,
                    outer,
                    state,
                )
                .await?
            }
        };
        self.verify_query_plan(&plan, state)?;
        self.register_schema_with_plan_id(&plan, plan_id, state)?;
        Ok(plan)
    }

    /// Resolve query plan.
    /// No hidden fields are kept in the resolved plan.
    #[async_recursion]
    pub(super) async fn resolve_query_plan(
        &self,
        plan: spec::QueryPlan,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let plan = self
            .resolve_query_plan_with_hidden_fields(plan, state)
            .await?;
        self.remove_hidden_fields(plan, state)
    }

    pub(super) fn resolve_empty_query_plan(&self) -> PlanResult<LogicalPlan> {
        Ok(LogicalPlan::EmptyRelation(plan::EmptyRelation {
            // allows literal projection with no input
            produce_one_row: true,
            schema: DFSchemaRef::new(DFSchema::empty()),
        }))
    }

    pub(super) async fn resolve_recursive_query_plan(
        &self,
        plan: spec::QueryPlan,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        match plan {
            spec::QueryPlan {
                node:
                    spec::QueryNode::SetOperation(spec::SetOperation {
                        left: _,
                        right: _,
                        set_op_type: _,
                        is_all: _,
                        by_name: _,
                        allow_missing_columns: _,
                    }),
                ..
            } => Err(PlanError::todo("Recursive CTEs")),
            other => self.resolve_query_plan(other, state).await,
        }
    }

    pub(super) async fn resolve_command_plan(
        &self,
        plan: spec::CommandPlan,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        use spec::CommandNode;

        match plan.node {
            CommandNode::ShowString(show) => self.resolve_command_show_string(show, state).await,
            CommandNode::HtmlString(html) => self.resolve_command_html_string(html, state).await,
            CommandNode::CurrentDatabase => {
                self.resolve_catalog_command(CatalogCommand::CurrentDatabase)
            }
            CommandNode::SetCurrentDatabase { database_name } => {
                self.resolve_catalog_command(CatalogCommand::SetCurrentDatabase {
                    database_name: database_name.into(),
                })
            }
            CommandNode::ListDatabases {
                catalog,
                database_pattern,
            } => self.resolve_catalog_command(CatalogCommand::ListDatabases {
                catalog: catalog.map(|x| x.into()),
                database_pattern,
            }),
            CommandNode::ListTables {
                database,
                table_pattern,
            } => self.resolve_catalog_command(CatalogCommand::ListTables {
                database: database
                    .map(|db| self.resolve_schema_reference(&db))
                    .transpose()?,
                table_pattern,
            }),
            CommandNode::ListFunctions {
                database,
                function_pattern,
            } => self.resolve_catalog_command(CatalogCommand::ListFunctions {
                database: database
                    .map(|db| self.resolve_schema_reference(&db))
                    .transpose()?,
                function_pattern,
            }),
            CommandNode::ListColumns { table } => {
                self.resolve_catalog_command(CatalogCommand::ListColumns {
                    table: self.resolve_table_reference(&table)?,
                })
            }
            CommandNode::GetDatabase { database } => {
                self.resolve_catalog_command(CatalogCommand::GetDatabase {
                    database: self.resolve_schema_reference(&database)?,
                })
            }
            CommandNode::GetTable { table } => {
                self.resolve_catalog_command(CatalogCommand::GetTable {
                    table: self.resolve_table_reference(&table)?,
                })
            }
            CommandNode::GetFunction { function } => {
                self.resolve_catalog_command(CatalogCommand::GetFunction {
                    function: self.resolve_table_reference(&function)?,
                })
            }
            CommandNode::DatabaseExists { database } => {
                self.resolve_catalog_command(CatalogCommand::DatabaseExists {
                    database: self.resolve_schema_reference(&database)?,
                })
            }
            CommandNode::TableExists { table } => {
                self.resolve_catalog_command(CatalogCommand::TableExists {
                    table: self.resolve_table_reference(&table)?,
                })
            }
            CommandNode::FunctionExists { function } => {
                self.resolve_catalog_command(CatalogCommand::FunctionExists {
                    function: self.resolve_table_reference(&function)?,
                })
            }
            CommandNode::CreateTable { table, definition } => {
                self.resolve_catalog_create_table(table, definition, state)
                    .await
            }
            CommandNode::DropView {
                view,
                kind,
                if_exists,
            } => self.resolve_catalog_drop_view(view, kind, if_exists).await,
            CommandNode::DropDatabase {
                database,
                if_exists,
                cascade,
            } => self.resolve_catalog_command(CatalogCommand::DropDatabase {
                database: self.resolve_schema_reference(&database)?,
                if_exists,
                cascade,
            }),
            CommandNode::DropFunction {
                function,
                if_exists,
                is_temporary,
            } => self.resolve_catalog_command(CatalogCommand::DropFunction {
                function: self.resolve_table_reference(&function)?,
                if_exists,
                is_temporary,
            }),
            CommandNode::DropTable {
                table,
                if_exists,
                purge,
            } => self.resolve_catalog_command(CatalogCommand::DropTable {
                table: self.resolve_table_reference(&table)?,
                if_exists,
                purge,
            }),
            CommandNode::RecoverPartitions { .. } => {
                Err(PlanError::todo("PlanNode::RecoverPartitions"))
            }
            CommandNode::IsCached { .. } => Err(PlanError::todo("PlanNode::IsCached")),
            CommandNode::CacheTable { .. } => Err(PlanError::todo("PlanNode::CacheTable")),
            CommandNode::UncacheTable { .. } => Err(PlanError::todo("PlanNode::UncacheTable")),
            CommandNode::ClearCache => Err(PlanError::todo("PlanNode::ClearCache")),
            CommandNode::RefreshTable { .. } => Err(PlanError::todo("PlanNode::RefreshTable")),
            CommandNode::RefreshByPath { .. } => Err(PlanError::todo("PlanNode::RefreshByPath")),
            CommandNode::CurrentCatalog => {
                self.resolve_catalog_command(CatalogCommand::CurrentCatalog)
            }
            CommandNode::SetCurrentCatalog { catalog_name } => {
                self.resolve_catalog_command(CatalogCommand::SetCurrentCatalog {
                    catalog_name: catalog_name.into(),
                })
            }
            CommandNode::ListCatalogs { catalog_pattern } => {
                self.resolve_catalog_command(CatalogCommand::ListCatalogs { catalog_pattern })
            }
            CommandNode::CreateCatalog { .. } => Err(PlanError::todo("create catalog")),
            CommandNode::CreateDatabase {
                database,
                definition,
            } => self.resolve_catalog_create_database(database, definition),
            CommandNode::RegisterFunction(function) => {
                self.resolve_catalog_register_function(function, state)
            }
            CommandNode::RegisterTableFunction(function) => {
                self.resolve_catalog_register_table_function(function, state)
            }
            CommandNode::RefreshFunction { .. } => {
                Err(PlanError::todo("CommandNode::RefreshFunction"))
            }
            CommandNode::CreateView { view, definition } => {
                self.resolve_catalog_create_view(view, definition, state)
                    .await
            }
            CommandNode::Write(write) => self.resolve_command_write(write, state).await,
            CommandNode::Explain { mode, input } => {
                self.resolve_command_explain(*input, mode, state).await
            }
            CommandNode::InsertOverwriteDirectory { .. } => {
                Err(PlanError::todo("CommandNode::InsertOverwriteDirectory"))
            }
            CommandNode::InsertInto {
                input,
                table,
                columns,
                partition_spec,
                replace,
                if_not_exists,
                overwrite,
            } => {
                if replace.is_some() {
                    return Err(PlanError::todo("INSERT INTO ... REPLACE"));
                }
                if if_not_exists {
                    return Err(PlanError::todo("IF NOT EXISTS for INSERT"));
                }
                self.resolve_command_insert_into(
                    *input,
                    table,
                    columns,
                    partition_spec,
                    overwrite,
                    state,
                )
                .await
            }
            CommandNode::MergeInto { .. } => Err(PlanError::todo("CommandNode::MergeInto")),
            CommandNode::SetVariable { variable, value } => {
                self.resolve_command_set_variable(variable, value).await
            }
            CommandNode::Update { .. } => Err(PlanError::todo("CommandNode::Update")),
            CommandNode::Delete { .. } => Err(PlanError::todo("CommandNode::Delete")),
            CommandNode::AlterTable { .. } => Err(PlanError::todo("CommandNode::AlterTable")),
            CommandNode::AlterView { .. } => Err(PlanError::todo("CommandNode::AlterView")),
            CommandNode::LoadData { .. } => Err(PlanError::todo("CommandNode::LoadData")),
            CommandNode::AnalyzeTable { .. } => Err(PlanError::todo("CommandNode::AnalyzeTable")),
            CommandNode::AnalyzeTables { .. } => Err(PlanError::todo("CommandNode::AnalyzeTables")),
            CommandNode::DescribeQuery { .. } => Err(PlanError::todo("CommandNode::DescribeQuery")),
            CommandNode::DescribeFunction { .. } => {
                Err(PlanError::todo("CommandNode::DescribeFunction"))
            }
            CommandNode::DescribeCatalog { .. } => {
                Err(PlanError::todo("CommandNode::DescribeCatalog"))
            }
            CommandNode::DescribeDatabase { .. } => {
                Err(PlanError::todo("CommandNode::DescribeDatabase"))
            }
            CommandNode::DescribeTable { .. } => Err(PlanError::todo("CommandNode::DescribeTable")),
            CommandNode::CommentOnCatalog { .. } => {
                Err(PlanError::todo("CommandNode::CommentOnCatalog"))
            }
            CommandNode::CommentOnDatabase { .. } => {
                Err(PlanError::todo("CommandNode::CommentOnDatabase"))
            }
            CommandNode::CommentOnTable { .. } => {
                Err(PlanError::todo("CommandNode::CommentOnTable"))
            }
            CommandNode::CommentOnColumn { .. } => {
                Err(PlanError::todo("CommandNode::CommentOnColumn"))
            }
        }
    }

    async fn resolve_query_read_named_table(
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

        let view = match &table_reference {
            TableReference::Bare { table } => {
                let view = manage_temporary_views(self.ctx, false, |views| {
                    views.get_view(table.as_ref())
                })?;
                view.map(|x| x.as_ref().clone())
            }
            TableReference::Partial { schema, table } => {
                if schema.as_ref() == self.config.global_temp_database.as_str() {
                    let view = manage_temporary_views(self.ctx, true, |views| {
                        views.get_view(table.as_ref())
                    })?;
                    let view = view.ok_or_else(|| {
                        PlanError::invalid(format!("global temporary view not found: {table}"))
                    })?;
                    Some(view.as_ref().clone())
                } else {
                    None
                }
            }
            TableReference::Full { .. } => None,
        };
        if let Some(view) = view {
            let names = state.register_fields(view.schema().inner().fields());
            return Ok(rename_logical_plan(view, &names)?);
        }

        if !options.is_empty() {
            return Err(PlanError::todo("table options"));
        }
        let table_provider = self.ctx.table_provider(table_reference.clone()).await?;
        let names = state.register_fields(table_provider.schema().fields());
        let table_provider = RenameTableProvider::try_new(table_provider, names)?;
        Ok(LogicalPlan::TableScan(plan::TableScan::try_new(
            table_reference,
            provider_as_source(Arc::new(table_provider)),
            None,
            vec![],
            None,
        )?))
    }

    async fn resolve_query_read_udtf(
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
                    let input = self.resolve_empty_query_plan()?;
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
                Ok(LogicalPlan::TableScan(plan::TableScan::try_new(
                    function_name,
                    provider_as_source(Arc::new(table_provider)),
                    None,
                    vec![],
                    None,
                )?))
            }
        }
    }

    async fn resolve_query_read_data_source(
        &self,
        source: spec::ReadDataSource,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::ReadDataSource {
            format,
            schema,
            options: _,
            paths,
            predicates: _,
        } = source;
        if paths.is_empty() {
            return Err(PlanError::invalid("empty data source paths"));
        }
        let urls = self.resolve_listing_urls(paths).await?;
        let (format, extension): (Arc<dyn FileFormat>, _) =
            match format.map(|x| x.to_lowercase()).as_deref() {
                Some("json") => (Arc::new(JsonFormat::default()), ".json"),
                Some("csv") => (Arc::new(CsvFormat::default()), ".csv"),
                Some("parquet") => (Arc::new(ParquetFormat::new()), ".parquet"),
                Some("arrow") => (Arc::new(ArrowFormat), ".arrow"),
                Some("avro") => (Arc::new(AvroFormat), ".avro"),
                other => {
                    return Err(PlanError::unsupported(format!(
                        "unsupported data source format: {:?}",
                        other
                    )))
                }
            };
        let options = ListingOptions::new(format).with_file_extension(extension);
        let schema = self
            .resolve_listing_schema(&urls, &options, schema, state)
            .await?;
        let config = ListingTableConfig::new_with_multi_paths(urls)
            .with_listing_options(options)
            .with_schema(Arc::new(schema));
        let table_provider = Arc::new(ListingTable::try_new(config)?);
        let names = state.register_fields(table_provider.schema().fields());
        let table_provider = RenameTableProvider::try_new(table_provider, names)?;
        Ok(LogicalPlan::TableScan(plan::TableScan::try_new(
            UNNAMED_TABLE,
            provider_as_source(Arc::new(table_provider)),
            None,
            vec![],
            None,
        )?))
    }

    async fn resolve_query_project(
        &self,
        input: Option<spec::QueryPlan>,
        expr: Vec<spec::Expr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = match input {
            Some(x) => self.resolve_query_plan_with_hidden_fields(x, state).await?,
            None => self.resolve_empty_query_plan()?,
        };
        let schema = input.schema();
        let expr = self.resolve_named_expressions(expr, schema, state).await?;
        let (input, expr) = self.rewrite_wildcard(input, expr, state)?;
        let (input, expr) = self.rewrite_projection::<ExplodeRewriter>(input, expr, state)?;
        let (input, expr) = self.rewrite_projection::<WindowRewriter>(input, expr, state)?;
        let expr = self.rewrite_multi_expr(expr)?;
        let has_aggregate = expr.iter().any(|e| {
            e.expr
                .exists(|e| match e {
                    Expr::AggregateFunction(_) => Ok(true),
                    _ => Ok(false),
                })
                .unwrap_or(false)
        });
        if has_aggregate {
            self.rewrite_aggregate(input, expr, vec![], None, false, state)
        } else {
            let expr = self.rewrite_named_expressions(expr, state)?;
            Ok(LogicalPlan::Projection(plan::Projection::try_new(
                expr,
                Arc::new(input),
            )?))
        }
    }

    async fn resolve_query_filter(
        &self,
        input: spec::QueryPlan,
        condition: spec::Expr,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self
            .resolve_query_plan_with_hidden_fields(input, state)
            .await?;
        let schema = input.schema();
        let predicate = self.resolve_expression(condition, schema, state).await?;
        let filter = plan::Filter::try_new(predicate, Arc::new(input))?;
        Ok(LogicalPlan::Filter(filter))
    }

    async fn resolve_query_join(
        &self,
        join: spec::Join,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::Join {
            left,
            right,
            join_type,
            join_criteria,
            join_data_type,
        } = join;
        let left = self.resolve_query_plan(*left, state).await?;
        let right = self.resolve_query_plan(*right, state).await?;
        let join_type = match join_type {
            spec::JoinType::Inner => Some(JoinType::Inner),
            spec::JoinType::LeftOuter => Some(JoinType::Left),
            spec::JoinType::RightOuter => Some(JoinType::Right),
            spec::JoinType::FullOuter => Some(JoinType::Full),
            spec::JoinType::LeftSemi => Some(JoinType::LeftSemi),
            spec::JoinType::RightSemi => Some(JoinType::RightSemi),
            spec::JoinType::LeftAnti => Some(JoinType::LeftAnti),
            spec::JoinType::RightAnti => Some(JoinType::RightAnti),
            spec::JoinType::Cross => None,
        };

        match (join_type, join_criteria) {
            (None, Some(_)) => Err(PlanError::invalid("cross join with join criteria")),
            // When the join criteria are not specified, any join type has the semantics of a cross join.
            (Some(_), None) | (None, None) => {
                if join_data_type.is_some() {
                    return Err(PlanError::invalid("cross join with join data type"));
                }
                Ok(LogicalPlanBuilder::from(left).cross_join(right)?.build()?)
            }
            (Some(join_type), Some(spec::JoinCriteria::On(condition))) => {
                let join_schema = Arc::new(build_join_schema(
                    left.schema(),
                    right.schema(),
                    &join_type,
                )?);
                let condition = self
                    .resolve_expression(condition, &join_schema, state)
                    .await?
                    .unalias_nested()
                    .data;
                let plan = LogicalPlanBuilder::from(left)
                    .join_on(right, join_type, Some(condition))?
                    .build()?;
                Ok(plan)
            }
            (Some(join_type), Some(spec::JoinCriteria::Natural)) => {
                let left_names = Self::get_field_names(left.schema(), state)?;
                let right_names = Self::get_field_names(right.schema(), state)?;
                let using = left_names
                    .iter()
                    .filter(|name| right_names.contains(name))
                    .map(|x| x.clone().into())
                    .collect::<Vec<_>>();
                // We let the column resolver return errors when either plan contains
                // duplicated columns for the natural join key.
                let join_columns =
                    self.resolve_query_join_using_columns(&left, &right, using, state)?;
                self.resolve_query_join_using(left, right, join_type, join_columns, state)
            }
            (Some(join_type), Some(spec::JoinCriteria::Using(using))) => {
                let join_columns =
                    self.resolve_query_join_using_columns(&left, &right, using, state)?;
                self.resolve_query_join_using(left, right, join_type, join_columns, state)
            }
        }
    }

    fn resolve_query_join_using_columns(
        &self,
        left: &LogicalPlan,
        right: &LogicalPlan,
        using: Vec<spec::Identifier>,
        state: &PlanResolverState,
    ) -> PlanResult<Vec<(String, (Column, Column))>> {
        using
            .into_iter()
            .map(|name| {
                let left_column = self.resolve_one_column(left.schema(), name.as_ref(), state)?;
                let right_column = self.resolve_one_column(right.schema(), name.as_ref(), state)?;
                Ok((name.into(), (left_column, right_column)))
            })
            .collect()
    }

    fn resolve_query_join_using(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
        join_type: JoinType,
        join_columns: Vec<(String, (Column, Column))>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let (left_columns, right_columns) = join_columns
            .iter()
            .map(|(_, (left, right))| (left.clone(), right.clone()))
            .unzip::<_, _, Vec<_>, Vec<_>>();

        let builder = LogicalPlanBuilder::from(left).join_detailed(
            right,
            join_type,
            (left_columns.clone(), right_columns.clone()),
            None,
            false,
        )?;
        let builder = match join_type {
            JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                let columns = builder
                    .schema()
                    .columns()
                    .into_iter()
                    .map(|col| {
                        if left_columns.iter().any(|x| x.name == col.name)
                            || right_columns.iter().any(|x| x.name == col.name)
                        {
                            let info = state.get_field_info(col.name())?.clone();
                            let field_id = state.register_hidden_field_name(info.name());
                            for plan_id in info.plan_ids() {
                                state.register_plan_id_for_field(&field_id, plan_id)?;
                            }
                            Ok(Expr::Column(col).alias(field_id))
                        } else {
                            Ok(Expr::Column(col))
                        }
                    })
                    .collect::<PlanResult<Vec<_>>>()?;
                let projections = join_columns
                    .into_iter()
                    .map(|(name, (left, right))| {
                        coalesce(vec![Expr::Column(left), Expr::Column(right)])
                            .alias(state.register_field_name(name))
                    })
                    .chain(columns);
                builder.project(projections)?
            }
            JoinType::LeftSemi
            | JoinType::RightSemi
            | JoinType::LeftAnti
            | JoinType::RightAnti
            | JoinType::LeftMark => builder,
        };
        Ok(builder.build()?)
    }

    async fn resolve_query_set_operation(
        &self,
        op: spec::SetOperation,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        use spec::SetOpType;

        let spec::SetOperation {
            left,
            right,
            set_op_type,
            is_all,
            by_name,
            allow_missing_columns,
        } = op;
        let left = self.resolve_query_plan(*left, state).await?;
        let right = self.resolve_query_plan(*right, state).await?;
        match set_op_type {
            SetOpType::Intersect => Ok(LogicalPlanBuilder::intersect(left, right, is_all)?),
            SetOpType::Union => {
                let (left, right) = if by_name {
                    let left_names = Self::get_field_names(left.schema(), state)?;
                    let right_names = Self::get_field_names(right.schema(), state)?;
                    let (mut left_reordered_columns, mut right_reordered_columns): (
                        Vec<Expr>,
                        Vec<Expr>,
                    ) = left_names
                        .iter()
                        .enumerate()
                        .map(|(left_idx, left_name)| {
                            match right_names
                                .iter()
                                .position(|right_name| left_name.eq_ignore_ascii_case(right_name))
                            {
                                Some(right_idx) => Ok((
                                    Expr::Column(Column::from(
                                        left.schema().qualified_field(left_idx),
                                    )),
                                    Expr::Column(Column::from(
                                        right.schema().qualified_field(right_idx),
                                    )),
                                )),
                                None if allow_missing_columns => Ok((
                                    Expr::Column(Column::from(
                                        left.schema().qualified_field(left_idx),
                                    )),
                                    Expr::Literal(ScalarValue::Null)
                                        .alias(state.register_field_name(left_name)),
                                )),
                                None => Err(PlanError::invalid(format!(
                                    "right column not found: {left_name}"
                                ))),
                            }
                        })
                        .collect::<PlanResult<Vec<(Expr, Expr)>>>()?
                        .into_iter()
                        .unzip();
                    if allow_missing_columns {
                        let (left_extra_columns, right_extra_columns): (Vec<Expr>, Vec<Expr>) =
                            right_names
                                .into_iter()
                                .enumerate()
                                .filter(|(_, right_name)| {
                                    !left_names
                                        .iter()
                                        .any(|left_name| left_name.eq_ignore_ascii_case(right_name))
                                })
                                .map(|(right_idx, right_name)| {
                                    (
                                        Expr::Literal(ScalarValue::Null)
                                            .alias(state.register_field_name(right_name)),
                                        Expr::Column(Column::from(
                                            right.schema().qualified_field(right_idx),
                                        )),
                                    )
                                })
                                .collect::<Vec<(Expr, Expr)>>()
                                .into_iter()
                                .unzip();
                        right_reordered_columns.extend(right_extra_columns);
                        left_reordered_columns.extend(left_extra_columns);
                        (
                            project(left, left_reordered_columns)?,
                            project(right, right_reordered_columns)?,
                        )
                    } else {
                        (left, project(right, right_reordered_columns)?)
                    }
                } else {
                    (left, right)
                };
                if is_all {
                    Ok(LogicalPlanBuilder::new(left).union(right)?.build()?)
                } else {
                    Ok(LogicalPlanBuilder::new(left)
                        .union_distinct(right)?
                        .build()?)
                }
            }
            SetOpType::Except => Ok(LogicalPlanBuilder::except(left, right, is_all)?),
        }
    }

    async fn resolve_query_sort(
        &self,
        input: spec::QueryPlan,
        order: Vec<spec::SortOrder>,
        is_global: bool,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self
            .resolve_query_plan_with_hidden_fields(input, state)
            .await?;
        let sorts = self
            .resolve_query_sort_orders_by_plan(&input, &order, state)
            .await?;
        let sorts = Self::rebase_query_sort_orders(sorts, &input)?;
        if is_global {
            Ok(LogicalPlanBuilder::from(input).sort(sorts)?.build()?)
        } else {
            // TODO: Use the logical plan builder to include logic such as expression rebase.
            //   We can build a plan with a `Sort` node and then replace it with the
            //   `SortWithinPartitions` node using a tree node rewriter.
            Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(SortWithinPartitionsNode::new(Arc::new(input), sorts, None)),
            }))
        }
    }

    /// Rebase sort expressions using aggregation expressions when the aggregate plan
    /// is inside a projection plan.
    /// Usually the [LogicalPlanBuilder] handles rebase, but this particular case is not handled yet.
    /// We do not do so recursively to make sure this workaround is only applied to a particular pattern.
    ///
    /// This workaround is needed for queries where the aggregation expression is aliased.
    /// Here is an example.
    /// ```sql
    /// SELECT a, sum(b) AS s FROM VALUES (1, 2) AS t(a, b) GROUP BY a ORDER BY sum(b)
    /// ```
    fn rebase_query_sort_orders(sorts: Vec<Sort>, plan: &LogicalPlan) -> PlanResult<Vec<Sort>> {
        match plan {
            LogicalPlan::Projection(Projection { input, .. }) => {
                if let LogicalPlan::Aggregate(Aggregate {
                    input,
                    group_expr,
                    aggr_expr,
                    ..
                }) = input.as_ref()
                {
                    let base = group_expr
                        .iter()
                        .cloned()
                        .chain(aggr_expr.iter().cloned())
                        .collect::<Vec<_>>();
                    sorts
                        .into_iter()
                        .map(|x| {
                            let Sort {
                                expr,
                                asc,
                                nulls_first,
                            } = x;
                            let expr = rebase_expression(expr, &base, input.as_ref())?;
                            Ok(Sort {
                                expr,
                                asc,
                                nulls_first,
                            })
                        })
                        .collect::<PlanResult<Vec<_>>>()
                } else {
                    Ok(sorts)
                }
            }
            _ => Ok(sorts),
        }
    }

    /// Resolve sort orders by attempting child plans recursively.
    async fn resolve_query_sort_orders_by_plan(
        &self,
        plan: &LogicalPlan,
        sorts: &[spec::SortOrder],
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<Sort>> {
        let mut results: Vec<Sort> = Vec::with_capacity(sorts.len());
        for sort in sorts {
            let expr = self
                .resolve_query_sort_order_by_plan(plan, sort, state)
                .await?;
            results.push(expr);
        }
        Ok(results)
    }

    /// Resolve a sort order by attempting child plans recursively.
    /// This is needed since the sort order may refer to a column in a child plan,
    /// So we need to use the schema of the child plan to map between user-facing
    /// field name and the opaque field ID.
    #[async_recursion]
    async fn resolve_query_sort_order_by_plan(
        &self,
        plan: &LogicalPlan,
        sort: &spec::SortOrder,
        state: &mut PlanResolverState,
    ) -> PlanResult<Sort> {
        let sort_expr = self
            .resolve_sort_order(sort.clone(), true, plan.schema(), state)
            .await;
        match sort_expr {
            Ok(sort_expr) => Ok(sort_expr),
            Err(_) => {
                let mut sorts = Vec::with_capacity(plan.inputs().len());
                for input_plan in plan.inputs() {
                    let sort_expr = self
                        .resolve_query_sort_order_by_plan(input_plan, sort, state)
                        .await?;
                    sorts.push(sort_expr);
                }
                if sorts.len() != 1 {
                    Err(PlanError::invalid(format!("sort expression: {sort:?}",)))
                } else {
                    Ok(sorts.one()?)
                }
            }
        }
    }

    async fn resolve_query_limit(
        &self,
        input: spec::QueryPlan,
        skip: Option<spec::Expr>,
        limit: Option<spec::Expr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self
            .resolve_query_plan_with_hidden_fields(input, state)
            .await?;
        let skip = if let Some(skip) = skip {
            Some(self.resolve_expression(skip, input.schema(), state).await?)
        } else {
            None
        };
        let limit = if let Some(limit) = limit {
            Some(
                self.resolve_expression(limit, input.schema(), state)
                    .await?,
            )
        } else {
            None
        };
        Ok(LogicalPlan::Limit(plan::Limit {
            skip: skip.map(Box::new),
            fetch: limit.map(Box::new),
            input: Arc::new(input),
        }))
    }

    async fn resolve_query_aggregate(
        &self,
        aggregate: spec::Aggregate,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::Aggregate {
            input,
            grouping,
            aggregate: projections,
            having,
            with_grouping_expressions,
        } = aggregate;

        let input = self
            .resolve_query_plan_with_hidden_fields(*input, state)
            .await?;
        let schema = input.schema();
        let projections = self
            .resolve_named_expressions(projections, schema, state)
            .await?;
        let grouping = self
            .resolve_named_expressions(grouping, schema, state)
            .await?;
        let having = match having {
            Some(having) => Some(self.resolve_expression(having, schema, state).await?),
            None => None,
        };

        self.rewrite_aggregate(
            input,
            projections,
            grouping,
            having,
            with_grouping_expressions,
            state,
        )
    }

    async fn resolve_query_with_parameters(
        &self,
        input: spec::QueryPlan,
        positional: Vec<spec::Literal>,
        named: Vec<(String, spec::Literal)>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self
            .resolve_query_plan_with_hidden_fields(input, state)
            .await?;
        let input = if !positional.is_empty() {
            let params = positional
                .into_iter()
                .map(|arg| self.resolve_literal(arg, state))
                .collect::<PlanResult<_>>()?;
            input.with_param_values(ParamValues::List(params))?
        } else {
            input
        };
        if !named.is_empty() {
            let params = named
                .into_iter()
                .map(|(name, arg)| -> PlanResult<(String, ScalarValue)> {
                    Ok((name, self.resolve_literal(arg, state)?))
                })
                .collect::<PlanResult<_>>()?;
            Ok(input.with_param_values(ParamValues::Map(params))?)
        } else {
            Ok(input)
        }
    }

    async fn resolve_query_local_relation(
        &self,
        data: Option<Vec<u8>>,
        schema: Option<spec::Schema>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let batches = if let Some(data) = data {
            read_record_batches(&data)?
        } else {
            vec![]
        };
        let (schema, batches) = if let Some(schema) = schema {
            let schema: adt::SchemaRef = Arc::new(self.resolve_schema(schema, state)?);
            let batches = batches
                .into_iter()
                .map(|b| Ok(cast_record_batch(b, schema.clone())?))
                .collect::<PlanResult<_>>()?;
            (schema, batches)
        } else if let [batch, ..] = batches.as_slice() {
            (batch.schema(), batches)
        } else {
            return Err(PlanError::invalid("missing schema for local relation"));
        };
        let names = state.register_fields(schema.fields());
        let provider = RenameTableProvider::try_new(
            Arc::new(MemTable::try_new(schema, vec![batches])?),
            names,
        )?;
        Ok(LogicalPlan::TableScan(plan::TableScan::try_new(
            UNNAMED_TABLE,
            provider_as_source(Arc::new(provider)),
            None,
            vec![],
            None,
        )?))
    }

    async fn resolve_query_sample(
        &self,
        _sample: spec::Sample,
        _state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        Err(PlanError::todo("sample"))
    }

    async fn resolve_query_deduplicate(
        &self,
        deduplicate: spec::Deduplicate,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::Deduplicate {
            input,
            column_names,
            all_columns_as_keys,
            within_watermark,
        } = deduplicate;
        let input = self
            .resolve_query_plan_with_hidden_fields(*input, state)
            .await?;
        let schema = input.schema();
        if within_watermark {
            return Err(PlanError::todo("deduplicate within watermark"));
        }
        if !column_names.is_empty() && !all_columns_as_keys {
            let on_expr: Vec<Expr> = self
                .resolve_columns(schema, &column_names, state)?
                .into_iter()
                .map(Expr::Column)
                .collect();
            let select_expr: Vec<Expr> = schema.columns().into_iter().map(Expr::Column).collect();
            Ok(LogicalPlan::Distinct(plan::Distinct::On(
                plan::DistinctOn::try_new(on_expr, select_expr, None, Arc::new(input))?,
            )))
        } else if column_names.is_empty() && all_columns_as_keys {
            Ok(LogicalPlan::Distinct(plan::Distinct::All(Arc::new(input))))
        } else {
            Err(PlanError::invalid(
                "must either specify deduplicate column names or use all columns as keys",
            ))
        }
    }

    async fn resolve_query_range(
        &self,
        range: spec::Range,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::Range {
            start,
            end,
            step,
            num_partitions,
        } = range;
        let start = start.unwrap_or(0);
        // TODO: use parallelism in Spark configuration as the default
        let num_partitions = num_partitions.unwrap_or(1);
        if num_partitions < 1 {
            return Err(PlanError::invalid(format!(
                "invalid number of partitions: {}",
                num_partitions
            )));
        }
        let alias = state.register_field_name("id");
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(RangeNode::try_new(alias, start, end, step, num_partitions)?),
        }))
    }

    async fn resolve_query_subquery_alias(
        &self,
        input: spec::QueryPlan,
        alias: spec::Identifier,
        qualifier: Vec<spec::Identifier>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self
            .resolve_query_plan_with_hidden_fields(input, state)
            .await?;
        Ok(LogicalPlan::SubqueryAlias(plan::SubqueryAlias::try_new(
            Arc::new(input),
            self.resolve_table_reference(&spec::ObjectName::from(qualifier).child(alias))?,
        )?))
    }

    async fn resolve_query_repartition(
        &self,
        input: spec::QueryPlan,
        num_partitions: usize,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self
            .resolve_query_plan_with_hidden_fields(input, state)
            .await?;
        // TODO: handle shuffle partition
        Ok(LogicalPlan::Repartition(plan::Repartition {
            input: Arc::new(input),
            partitioning_scheme: plan::Partitioning::RoundRobinBatch(num_partitions),
        }))
    }

    async fn resolve_query_to_df(
        &self,
        input: spec::QueryPlan,
        columns: Vec<spec::Identifier>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self.resolve_query_plan(input, state).await?;
        let schema = input.schema();
        if columns.len() != schema.fields().len() {
            return Err(PlanError::invalid(format!(
                "number of column names ({}) does not match number of columns ({})",
                columns.len(),
                schema.fields().len()
            )));
        }
        let expr = schema
            .columns()
            .into_iter()
            .zip(columns.into_iter())
            .map(|(col, name)| NamedExpr::new(vec![name.into()], Expr::Column(col)))
            .collect();
        let expr = self.rewrite_named_expressions(expr, state)?;
        Ok(LogicalPlan::Projection(plan::Projection::try_new(
            expr,
            Arc::new(input),
        )?))
    }

    async fn resolve_query_with_columns_renamed(
        &self,
        input: spec::QueryPlan,
        rename_columns_map: Vec<(spec::Identifier, spec::Identifier)>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self.resolve_query_plan(input, state).await?;
        let rename_columns_map: HashMap<String, String> = rename_columns_map
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        let schema = input.schema();
        let expr = schema
            .columns()
            .into_iter()
            .map(|column| {
                let name = state.get_field_info(column.name())?.name();
                match rename_columns_map.get(name) {
                    Some(n) => Ok(NamedExpr::new(vec![n.clone()], Expr::Column(column))),
                    None => Ok(NamedExpr::new(vec![name.to_string()], Expr::Column(column))),
                }
            })
            .collect::<PlanResult<Vec<_>>>()?;
        let expr = self.rewrite_named_expressions(expr, state)?;
        Ok(LogicalPlan::Projection(plan::Projection::try_new(
            expr,
            Arc::new(input),
        )?))
    }

    async fn resolve_query_drop(
        &self,
        input: spec::QueryPlan,
        columns: Vec<spec::Expr>,
        column_names: Vec<spec::Identifier>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self.resolve_query_plan(input, state).await?;
        let schema = input.schema();
        let excluded = columns
            .into_iter()
            .filter_map(|col| {
                let spec::Expr::UnresolvedAttribute { name, plan_id } = col else {
                    return Some(Err(PlanError::invalid("expecting column to drop")));
                };
                let name: Vec<String> = name.into();
                let Ok(name) = name.one() else {
                    // Ignore nested names since they cannot match a column name.
                    // This is not an error in Spark.
                    return None;
                };
                // An error is returned when there are ambiguous columns.
                self.resolve_optional_column(schema, &name, plan_id, state)
                    .transpose()
            })
            .collect::<PlanResult<Vec<_>>>()?;
        let excluded = excluded
            .into_iter()
            .chain(column_names.into_iter().flat_map(|name| {
                let name: String = name.into();
                // The excluded column names are allow to refer to ambiguous columns,
                // so we just check the column name here.
                self.resolve_column_candidates(schema, &name, None, state)
                    .into_iter()
            }))
            .collect::<Vec<_>>();
        let expr: Vec<Expr> = schema
            .columns()
            .into_iter()
            .filter(|column| !excluded.contains(column))
            .map(Expr::Column)
            .collect();
        Ok(LogicalPlan::Projection(plan::Projection::try_new(
            expr,
            Arc::new(input),
        )?))
    }

    async fn resolve_query_tail(
        &self,
        _input: spec::QueryPlan,
        _limit: spec::Expr,
        _state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        Err(PlanError::todo("tail"))
    }

    async fn resolve_query_with_columns(
        &self,
        input: spec::QueryPlan,
        aliases: Vec<spec::Expr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self.resolve_query_plan(input, state).await?;
        let schema = input.schema();
        let mut aliases: HashMap<String, (Expr, bool, Vec<_>)> = async {
            let mut results: HashMap<String, (Expr, bool, Vec<_>)> = HashMap::new();
            for alias in aliases {
                let (name, expr, metadata) = match alias {
                    spec::Expr::Alias {
                        name,
                        expr,
                        metadata,
                    } => {
                        let name = name
                            .one()
                            .map_err(|_| PlanError::invalid("multi-alias for column"))?;
                        (name, *expr, metadata.unwrap_or(Vec::new()))
                    }
                    _ => return Err(PlanError::invalid("alias expression expected for column")),
                };
                let expr = self.resolve_expression(expr, schema, state).await?;
                results.insert(name.into(), (expr, false, metadata));
            }
            Ok(results) as PlanResult<_>
        }
        .await?;
        let mut expr = schema
            .columns()
            .into_iter()
            .map(|column| {
                let name = state.get_field_info(column.name())?.name();
                match aliases.get_mut(name) {
                    Some((e, exists, metadata)) => {
                        *exists = true;
                        if !metadata.is_empty() {
                            Ok(NamedExpr::new(vec![name.to_string()], e.clone())
                                .with_metadata(metadata.clone()))
                        } else {
                            Ok(NamedExpr::new(vec![name.to_string()], e.clone()))
                        }
                    }
                    None => Ok(NamedExpr::new(vec![name.to_string()], Expr::Column(column))),
                }
            })
            .collect::<PlanResult<Vec<_>>>()?;
        for (name, (e, exists, metadata)) in &aliases {
            if !exists {
                if !metadata.is_empty() {
                    expr.push(
                        NamedExpr::new(vec![name.clone()], e.clone())
                            .with_metadata(metadata.clone()),
                    );
                } else {
                    expr.push(NamedExpr::new(vec![name.clone()], e.clone()));
                }
            }
        }
        let (input, expr) = self.rewrite_projection::<ExplodeRewriter>(input, expr, state)?;
        let (input, expr) = self.rewrite_projection::<WindowRewriter>(input, expr, state)?;
        let expr = self.rewrite_multi_expr(expr)?;
        let expr = self.rewrite_named_expressions(expr, state)?;
        Ok(LogicalPlan::Projection(plan::Projection::try_new(
            expr,
            Arc::new(input),
        )?))
    }

    async fn resolve_query_hint(
        &self,
        _input: spec::QueryPlan,
        _name: String,
        _parameters: Vec<spec::Expr>,
        _state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        Err(PlanError::todo("hint"))
    }

    async fn resolve_query_pivot(
        &self,
        _pivot: spec::Pivot,
        _state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        Err(PlanError::todo("pivot"))
    }

    async fn resolve_query_unpivot(
        &self,
        _unpivot: spec::Unpivot,
        _state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        Err(PlanError::todo("unpivot"))
    }

    async fn resolve_query_to_schema(
        &self,
        input: spec::QueryPlan,
        schema: spec::Schema,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self.resolve_query_plan(input, state).await?;
        let target_schema = self.resolve_schema(schema, state)?;
        let input_names = Self::get_field_names(input.schema(), state)?;
        let mut projected_exprs = Vec::new();
        for target_field in target_schema.fields() {
            let target_name = target_field.name();
            let input_idx = input_names
                .iter()
                .position(|input_name| input_name.eq_ignore_ascii_case(target_name))
                .ok_or_else(|| {
                    PlanError::invalid(format!("field not found in input schema: {target_name}"))
                })?;
            let (input_qualifier, input_field) = input.schema().qualified_field(input_idx);
            let expr = Expr::Column(Column::from((input_qualifier, input_field)));
            let expr = if input_field.data_type() == target_field.data_type() {
                expr
            } else {
                expr.cast_to(target_field.data_type(), &input.schema())?
                    .alias_qualified(input_qualifier.cloned(), input_field.name())
            };
            projected_exprs.push(expr);
        }
        let projected_plan =
            LogicalPlan::Projection(plan::Projection::try_new(projected_exprs, Arc::new(input))?);
        Ok(projected_plan)
    }

    async fn resolve_query_repartition_by_expression(
        &self,
        input: spec::QueryPlan,
        partition_expressions: Vec<spec::Expr>,
        num_partitions: Option<usize>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self
            .resolve_query_plan_with_hidden_fields(input, state)
            .await?;
        let schema = input.schema();
        let expr = self
            .resolve_expressions(partition_expressions, schema, state)
            .await?;
        let num_partitions = num_partitions
            .ok_or_else(|| PlanError::todo("rebalance partitioning by expression"))?;
        Ok(LogicalPlan::Repartition(plan::Repartition {
            input: Arc::new(input),
            partitioning_scheme: plan::Partitioning::Hash(expr, num_partitions),
        }))
    }

    async fn resolve_query_map_partitions(
        &self,
        input: spec::QueryPlan,
        function: spec::CommonInlineUserDefinedFunction,
        _is_barrier: bool,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let mut scope = state.enter_config_scope();
        let state = scope.state();
        state.config_mut().arrow_allow_large_var_types = true;
        let spec::CommonInlineUserDefinedFunction {
            function_name,
            deterministic: _,
            arguments,
            function,
        } = function;
        let function_name: String = function_name.into();
        let input = self
            .resolve_query_project(Some(input), arguments, state)
            .await?;
        let input_names = Self::get_field_names(input.schema(), state)?;
        let function = self.resolve_python_udf(function, state)?;
        let output_schema = match function.output_type {
            adt::DataType::Struct(fields) => Arc::new(adt::Schema::new(fields)),
            _ => {
                return Err(PlanError::invalid(
                    "MapPartitions UDF output type must be struct",
                ))
            }
        };
        let output_names = state.register_fields(output_schema.fields());
        let output_qualifiers = vec![None; output_names.len()];
        let payload = PySparkUdfPayload::build(
            &function.python_version,
            &function.command,
            function.eval_type,
            // MapPartitions UDF has the iterator as the only argument
            &[0],
            &self.config.pyspark_udf_config,
        )?;
        let kind = match function.eval_type {
            spec::PySparkUdfType::MapPandasIter => PySparkMapIterKind::Pandas,
            spec::PySparkUdfType::MapArrowIter => PySparkMapIterKind::Arrow,
            _ => {
                return Err(PlanError::invalid(
                    "only MapPandasIter UDF is supported in MapPartitions",
                ));
            }
        };
        let func = PySparkMapIterUDF::new(
            kind,
            get_udf_name(&function_name, &payload),
            payload,
            input_names,
            output_schema,
            self.config.pyspark_udf_config.clone(),
        );
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(MapPartitionsNode::try_new(
                Arc::new(input),
                output_names,
                output_qualifiers,
                Arc::new(func),
            )?),
        }))
    }

    async fn resolve_query_collect_metrics(
        &self,
        _input: spec::QueryPlan,
        _name: String,
        _metrics: Vec<spec::Expr>,
        _state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        Err(PlanError::todo("collect metrics"))
    }

    async fn resolve_query_parse(
        &self,
        _parse: spec::Parse,
        _state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        Err(PlanError::todo("parse"))
    }

    async fn resolve_query_group_map(
        &self,
        map: spec::GroupMap,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let mut scope = state.enter_config_scope();
        let state = scope.state();
        state.config_mut().arrow_allow_large_var_types = true;
        let spec::GroupMap {
            input,
            grouping_expressions: grouping,
            function,
            sorting_expressions,
            initial_input,
            initial_grouping_expressions,
            is_map_groups_with_state,
            output_mode,
            timeout_conf,
        } = map;
        // The following group map fields are not used in PySpark,
        // so there is no plan to support them.
        if !sorting_expressions.is_empty() {
            return Err(PlanError::invalid(
                "sorting expressions not supported in group map",
            ));
        }
        if initial_input.is_some() {
            return Err(PlanError::invalid(
                "initial input not supported in group map",
            ));
        }
        if !initial_grouping_expressions.is_empty() {
            return Err(PlanError::invalid(
                "initial grouping expressions not supported in group map",
            ));
        }
        if is_map_groups_with_state.is_some() {
            return Err(PlanError::invalid(
                "is map groups with state not supported in group map",
            ));
        }
        if output_mode.is_some() {
            return Err(PlanError::invalid("output mode not supported in group map"));
        }
        if timeout_conf.is_some() {
            return Err(PlanError::invalid(
                "timeout configuration not supported in group map",
            ));
        }

        let spec::CommonInlineUserDefinedFunction {
            function_name,
            deterministic,
            arguments,
            function,
        } = function;
        let function_name: String = function_name.into();
        let function = self.resolve_python_udf(function, state)?;
        let output_fields = match function.output_type {
            adt::DataType::Struct(fields) => fields,
            _ => {
                return Err(PlanError::invalid(
                    "GroupMap UDF output type must be struct",
                ))
            }
        };
        let udf_output_type = adt::DataType::List(Arc::new(adt::Field::new_list_field(
            adt::DataType::Struct(output_fields.clone()),
            false,
        )));
        if !matches!(function.eval_type, spec::PySparkUdfType::GroupedMapPandas) {
            return Err(PlanError::invalid(
                "only MapPandasIter UDF is supported in MapPartitions",
            ));
        }
        let input = self.resolve_query_plan(*input, state).await?;
        let schema = input.schema();
        let args = self
            .resolve_named_expressions(arguments, schema, state)
            .await?;
        let grouping = self
            .resolve_named_expressions(grouping, schema, state)
            .await?;
        let (args, offsets) = Self::resolve_group_map_argument_offsets(&args, &grouping)?;
        let input_names = args
            .iter()
            .map(|x| Ok(x.name.clone().one()?))
            .collect::<PlanResult<Vec<_>>>()?;
        let args = args.into_iter().map(|x| x.expr).collect::<Vec<_>>();
        let grouping = grouping.into_iter().map(|x| x.expr).collect::<Vec<_>>();
        let input_types = Self::resolve_expression_types(&args, schema)?;
        let payload = PySparkUdfPayload::build(
            &function.python_version,
            &function.command,
            function.eval_type,
            &offsets,
            &self.config.pyspark_udf_config,
        )?;
        let udaf = PySparkGroupMapUDF::new(
            get_udf_name(&function_name, &payload),
            payload,
            deterministic,
            input_names,
            input_types,
            udf_output_type,
            self.config.pyspark_udf_config.clone(),
        );
        let agg = Expr::AggregateFunction(expr::AggregateFunction {
            func: Arc::new(AggregateUDF::from(udaf)),
            args,
            distinct: false,
            filter: None,
            order_by: None,
            null_treatment: None,
        });
        let output_name = agg.name_for_alias()?;
        let output_col = Column::new_unqualified(&output_name);
        let plan = LogicalPlanBuilder::from(input)
            .aggregate(grouping, vec![agg])?
            .project(vec![Expr::Column(output_col.clone())])?
            .unnest_column(output_col.clone())?
            .project(
                output_fields
                    .iter()
                    .map(|f| {
                        let expr = Expr::Column(output_col.clone()).field(f.name());
                        let name = state.register_field(f);
                        Ok(expr.alias(name))
                    })
                    .collect::<PlanResult<Vec<_>>>()?,
            )?
            .build()?;
        Ok(plan)
    }

    async fn resolve_query_co_group_map(
        &self,
        map: spec::CoGroupMap,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let mut scope = state.enter_config_scope();
        let state = scope.state();
        state.config_mut().arrow_allow_large_var_types = true;
        let spec::CoGroupMap {
            input: left,
            input_grouping_expressions: left_grouping,
            other: right,
            other_grouping_expressions: right_grouping,
            function,
            input_sorting_expressions: left_sorting,
            other_sorting_expressions: right_sorting,
        } = map;
        // The following co-group map fields are not used in PySpark,
        // so there is no plan to support them.
        if !left_sorting.is_empty() || !right_sorting.is_empty() {
            return Err(PlanError::invalid(
                "sorting expressions not supported in co-group map",
            ));
        }

        // prepare the inputs aggregation and the join operation
        let left = self
            .resolve_co_group_map_data(*left, left_grouping, state)
            .await?;
        let right = self
            .resolve_co_group_map_data(*right, right_grouping, state)
            .await?;
        if left.grouping.len() != right.grouping.len() {
            return Err(PlanError::invalid(
                "child plan grouping expressions must have the same length",
            ));
        }
        let on = left
            .grouping
            .iter()
            .zip(right.grouping.iter())
            .map(|(left, right)| left.clone().eq(right.clone()))
            .collect::<Vec<_>>();
        let offsets: Vec<usize> = left
            .offsets
            .into_iter()
            .chain(right.offsets.into_iter())
            .collect();

        // prepare the output mapping UDF
        let spec::CommonInlineUserDefinedFunction {
            function_name,
            deterministic,
            arguments: _, // no arguments are passed for co-group map
            function,
        } = function;
        let function_name: String = function_name.into();
        let function = self.resolve_python_udf(function, state)?;
        let output_fields = match function.output_type {
            adt::DataType::Struct(fields) => fields,
            _ => {
                return Err(PlanError::invalid(
                    "GroupMap UDF output type must be struct",
                ))
            }
        };
        let mapper_output_type = adt::DataType::List(Arc::new(adt::Field::new_list_field(
            adt::DataType::Struct(output_fields.clone()),
            false,
        )));
        if !matches!(function.eval_type, spec::PySparkUdfType::CogroupedMapPandas) {
            return Err(PlanError::invalid(
                "only CoGroupedMapPandas UDF is supported in co-group map",
            ));
        }
        let payload = PySparkUdfPayload::build(
            &function.python_version,
            &function.command,
            function.eval_type,
            &offsets,
            &self.config.pyspark_udf_config,
        )?;
        let udf = PySparkCoGroupMapUDF::try_new(
            get_udf_name(&function_name, &payload),
            payload,
            deterministic,
            left.mapper_input_types,
            left.mapper_input_names,
            right.mapper_input_types,
            right.mapper_input_names,
            mapper_output_type,
            self.config.pyspark_udf_config.clone(),
        )?;
        let mapping = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(ScalarUDF::from(udf)),
            args: vec![left.mapper_input, right.mapper_input],
        });
        let output_name = mapping.name_for_alias()?;
        let output_col = Column::new_unqualified(&output_name);

        let plan = LogicalPlanBuilder::new(left.plan)
            .join_on(right.plan, JoinType::Full, on)?
            .project(vec![mapping])?
            .unnest_column(output_col.clone())?
            .project(
                output_fields
                    .iter()
                    .map(|f| {
                        let expr = Expr::Column(output_col.clone()).field(f.name());
                        let name = state.register_field(f);
                        Ok(expr.alias(name))
                    })
                    .collect::<PlanResult<Vec<_>>>()?,
            )?
            .build()?;
        Ok(plan)
    }

    async fn resolve_co_group_map_data(
        &self,
        plan: spec::QueryPlan,
        grouping: Vec<spec::Expr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<CoGroupMapData> {
        let plan = self.resolve_query_plan(plan, state).await?;
        let schema = plan.schema();
        let grouping = self
            .resolve_named_expressions(grouping, schema, state)
            .await?;
        let args: Vec<_> = schema
            .columns()
            .into_iter()
            .map(|col| {
                Ok(NamedExpr {
                    name: vec![state.get_field_info(&col.name)?.name().to_string()],
                    expr: Expr::Column(col),
                    metadata: vec![],
                })
            })
            .collect::<PlanResult<Vec<_>>>()?;
        let (args, offsets) = Self::resolve_group_map_argument_offsets(&args, &grouping)?;
        let input_names = args
            .iter()
            .map(|x| Ok(x.name.clone().one()?))
            .collect::<PlanResult<Vec<_>>>()?;
        let args = args.into_iter().map(|x| x.expr).collect::<Vec<_>>();
        let group_exprs = grouping
            .into_iter()
            .map(|x| {
                let name = x.name.clone().one()?;
                Ok(x.expr.clone().alias(state.register_field_name(name)))
            })
            .collect::<PlanResult<Vec<_>>>()?;
        let input_types = Self::resolve_expression_types(&args, plan.schema())?;
        let udaf = PySparkBatchCollectorUDF::new(input_types.clone(), input_names.clone());
        let agg = Expr::AggregateFunction(expr::AggregateFunction {
            func: Arc::new(AggregateUDF::from(udaf)),
            args,
            distinct: false,
            filter: None,
            order_by: None,
            null_treatment: None,
        });
        let agg_name = agg.name_for_alias()?;
        let agg_alias = state.register_field_name(&agg_name);
        let agg_col = ident(&agg_name).alias(agg_alias.clone());
        let grouping = group_exprs
            .iter()
            .map(|x| Ok(ident(x.name_for_alias()?)))
            .collect::<PlanResult<Vec<_>>>()?;
        let mut projections = grouping.clone();
        projections.push(agg_col);
        let plan = LogicalPlanBuilder::new(plan)
            .aggregate(group_exprs, vec![agg])?
            .project(projections)?
            .build()?;
        Ok(CoGroupMapData {
            plan,
            grouping,
            mapper_input: ident(agg_alias),
            mapper_input_types: input_types,
            mapper_input_names: input_names,
            offsets,
        })
    }

    async fn resolve_query_with_watermark(
        &self,
        _watermark: spec::WithWatermark,
        _state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        Err(PlanError::todo("with watermark"))
    }

    async fn resolve_query_apply_in_pandas_with_state(
        &self,
        _apply: spec::ApplyInPandasWithState,
        _state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        Err(PlanError::todo("apply in pandas with state"))
    }

    async fn resolve_query_values(
        &self,
        values: Vec<Vec<spec::Expr>>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let schema = Arc::new(DFSchema::empty());
        let values = async {
            let mut results: Vec<Vec<Expr>> = Vec::with_capacity(values.len());
            for value in values {
                let value = self.resolve_expressions(value, &schema, state).await?;
                results.push(value);
            }
            Ok(results) as PlanResult<_>
        }
        .await?;
        let plan = LogicalPlanBuilder::values(values)?.build()?;
        let expr = plan
            .schema()
            .columns()
            .into_iter()
            .enumerate()
            .map(|(i, col)| Expr::Column(col).alias(state.register_field_name(format!("col{i}"))))
            .collect::<Vec<_>>();
        Ok(LogicalPlan::Projection(plan::Projection::try_new(
            expr,
            Arc::new(plan),
        )?))
    }

    async fn resolve_query_table_alias(
        &self,
        input: spec::QueryPlan,
        name: spec::Identifier,
        columns: Vec<spec::Identifier>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self.resolve_query_plan(input, state).await?;
        let schema = input.schema();
        let input = if columns.is_empty() {
            input
        } else {
            if columns.len() != schema.fields().len() {
                return Err(PlanError::invalid(format!(
                    "number of column names ({}) does not match number of columns ({})",
                    columns.len(),
                    schema.fields().len()
                )));
            }
            let expr: Vec<Expr> = schema
                .columns()
                .into_iter()
                .zip(columns.into_iter())
                .map(|(col, name)| Expr::Column(col.clone()).alias(state.register_field_name(name)))
                .collect();
            LogicalPlan::Projection(plan::Projection::try_new(expr, Arc::new(input))?)
        };
        Ok(LogicalPlan::SubqueryAlias(plan::SubqueryAlias::try_new(
            Arc::new(input),
            TableReference::Bare {
                table: Arc::from(String::from(name)),
            },
        )?))
    }

    async fn resolve_query_with_ctes(
        &self,
        input: spec::QueryPlan,
        recursive: bool,
        ctes: Vec<(spec::Identifier, spec::QueryPlan)>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let cte_names = ctes
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<HashSet<_>>();
        if cte_names.len() < ctes.len() {
            return Err(PlanError::invalid(
                "CTE query name specified more than once",
            ));
        }
        let mut scope = state.enter_cte_scope();
        let state = scope.state();
        for (name, query) in ctes.into_iter() {
            let reference = self.resolve_table_reference(&spec::ObjectName::bare(name.clone()))?;
            let plan = if recursive {
                self.resolve_recursive_query_plan(query, state).await?
            } else {
                self.resolve_query_plan(query, state).await?
            };
            let plan = LogicalPlan::SubqueryAlias(plan::SubqueryAlias::try_new(
                Arc::new(plan),
                reference.clone(),
            )?);
            state.insert_cte(reference, plan);
        }
        self.resolve_query_plan(input, state).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn resolve_query_lateral_view(
        &self,
        input: Option<spec::QueryPlan>,
        function: spec::ObjectName,
        arguments: Vec<spec::Expr>,
        named_arguments: Vec<(spec::Identifier, spec::Expr)>,
        table_alias: Option<spec::ObjectName>,
        column_aliases: Option<Vec<spec::Identifier>>,
        outer: bool,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let Ok(function_name) = <Vec<String>>::from(function).one() else {
            return Err(PlanError::unsupported(
                "qualified lateral view function name",
            ));
        };
        let canonical_function_name = function_name.to_ascii_lowercase();
        let mut scope = state.enter_config_scope();
        let state = scope.state();
        if let Ok(f) = self.ctx.udf(&canonical_function_name) {
            if f.inner().as_any().is::<PySparkUnresolvedUDF>() {
                state.config_mut().arrow_allow_large_var_types = true;
            }
        }
        let input = match input {
            Some(x) => self.resolve_query_plan(x, state).await?,
            None => self.resolve_empty_query_plan()?,
        };
        let schema = input.schema().clone();

        if let Ok(f) = self.ctx.udf(&canonical_function_name) {
            if let Some(f) = f.inner().as_any().downcast_ref::<PySparkUnresolvedUDF>() {
                if !f.eval_type().is_table_function() {
                    return Err(PlanError::invalid(format!(
                        "not a table function for UDTF lateral view: {function_name}"
                    )));
                }
                let udtf = PythonUdtf {
                    python_version: f.python_version().to_string(),
                    eval_type: f.eval_type(),
                    command: f.command().to_vec(),
                    return_type: f.output_type().clone(),
                };
                let arguments = self
                    .resolve_named_expressions(arguments, input.schema(), state)
                    .await?;
                let output_names =
                    column_aliases.map(|aliases| aliases.into_iter().map(|x| x.into()).collect());
                let output_qualifier = table_alias
                    .map(|alias| self.resolve_table_reference(&alias))
                    .transpose()?;
                return self.resolve_python_udtf_plan(
                    udtf,
                    &function_name,
                    input,
                    arguments,
                    output_names,
                    output_qualifier,
                    f.deterministic(),
                    state,
                );
            }
        }

        let function_name = if outer {
            get_outer_built_in_generator_functions(&function_name).to_string()
        } else {
            function_name
        };
        let expression = spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
            function_name: spec::ObjectName::bare(function_name),
            arguments,
            named_arguments,
            is_distinct: false,
            is_user_defined_function: false,
            ignore_nulls: None,
            filter: None,
            order_by: None,
        });
        let expression = if let Some(aliases) = column_aliases {
            spec::Expr::Alias {
                expr: Box::new(expression),
                name: aliases,
                metadata: None,
            }
        } else {
            expression
        };
        let expr = self
            .resolve_named_expression(expression, &schema, state)
            .await?;
        let (input, expr) = self.rewrite_wildcard(input, vec![expr], state)?;
        let (input, expr) = self.rewrite_projection::<ExplodeRewriter>(input, expr, state)?;
        let expr = self.rewrite_multi_expr(expr)?;
        let expr = self.rewrite_named_expressions(expr, state)?;
        let expr = if let Some(table_alias) = table_alias {
            let table_reference = self.resolve_table_reference(&table_alias)?;
            expr.into_iter()
                .map(|x| {
                    let name = x.schema_name().to_string();
                    x.alias_qualified(Some(table_reference.clone()), name)
                })
                .collect()
        } else {
            expr
        };
        let projections = schema
            .columns()
            .into_iter()
            .map(Expr::Column)
            .chain(expr.into_iter())
            .collect::<Vec<_>>();
        Ok(LogicalPlan::Projection(plan::Projection::try_new(
            projections,
            Arc::new(input),
        )?))
    }

    async fn resolve_query_common_inline_udtf(
        &self,
        udtf: spec::CommonInlineUserDefinedTableFunction,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let mut scope = state.enter_config_scope();
        let state = scope.state();
        state.config_mut().arrow_allow_large_var_types = true;
        let spec::CommonInlineUserDefinedTableFunction {
            function_name,
            deterministic,
            arguments,
            function,
        } = udtf;
        let function_name: String = function_name.into();
        let function = self.resolve_python_udtf(function, state)?;
        let input = self.resolve_empty_query_plan()?;
        let arguments = self
            .resolve_named_expressions(arguments, input.schema(), state)
            .await?;
        self.resolve_python_udtf_plan(
            function,
            &function_name,
            input,
            arguments,
            None,
            None,
            deterministic,
            state,
        )
    }

    async fn resolve_command_show_string(
        &self,
        show: spec::ShowString,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::ShowString {
            input,
            num_rows,
            truncate,
            vertical,
        } = show;
        let input = self.resolve_query_plan(*input, state).await?;
        // add a `Limit` plan so that the optimizer can push down the limit
        let input = LogicalPlan::Limit(plan::Limit {
            skip: Some(Box::new(lit(0))),
            // fetch one more row so that the proper message can be displayed if there is more data
            fetch: Some(Box::new(lit(num_rows as i64 + 1))),
            input: Arc::new(input),
        });
        let style = match vertical {
            true => ShowStringStyle::Vertical,
            false => ShowStringStyle::Default,
        };
        let format = ShowStringFormat::new(style, truncate);
        let names = Self::get_field_names(input.schema(), state)?;
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(ShowStringNode::try_new(
                Arc::new(input),
                names,
                num_rows,
                format,
                "show_string".to_string(),
            )?),
        }))
    }

    async fn resolve_command_html_string(
        &self,
        html: spec::HtmlString,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::HtmlString {
            input,
            num_rows,
            truncate,
        } = html;
        let input = self.resolve_query_plan(*input, state).await?;
        let format = ShowStringFormat::new(ShowStringStyle::Html, truncate);
        let names = Self::get_field_names(input.schema(), state)?;
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(ShowStringNode::try_new(
                Arc::new(input),
                names,
                num_rows,
                format,
                "html_string".to_string(),
            )?),
        }))
    }

    async fn resolve_command_explain(
        &self,
        input: spec::QueryPlan,
        mode: spec::ExplainMode,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self.resolve_query_plan(input, state).await?;
        let stringified_plans: Vec<StringifiedPlan> =
            vec![input.to_stringified(PlanType::InitialLogicalPlan)];
        let schema = LogicalPlan::explain_schema();
        let schema = schema.to_dfschema_ref()?;
        Ok(LogicalPlan::Explain(plan::Explain {
            verbose: matches!(mode, spec::ExplainMode::Verbose),
            plan: Arc::new(input),
            stringified_plans,
            schema,
            logical_optimization_succeeded: true,
        }))
    }

    async fn resolve_command_write(
        &self,
        write: spec::Write,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        use spec::{SaveMode, SaveType, TableSaveMethod};

        let spec::Write {
            input,
            source,
            save_type,
            mode,
            sort_columns,
            partitioning_columns,
            bucket_by,
            options,
            table_properties: _,
            overwrite_condition: _,
        } = write;
        if !sort_columns.is_empty() {
            return Err(PlanError::unsupported("sort column names"));
        }
        if !partitioning_columns.is_empty() {
            return Err(PlanError::unsupported("partitioning columns"));
        }
        if bucket_by.is_some() {
            return Err(PlanError::unsupported("bucketing"));
        }
        let options: HashMap<_, _> = options.into_iter().collect();
        let partitioning_columns: Vec<String> =
            partitioning_columns.into_iter().map(String::from).collect();

        let mut table_options =
            TableOptions::default_from_session_config(self.ctx.state().config_options());
        let plan = self.resolve_query_plan(*input, state).await?;
        let fields = Self::get_field_names(plan.schema(), state)?;
        let plan = rename_logical_plan(plan, &fields)?;
        let plan = match save_type {
            SaveType::Path(path) => {
                // always write multi-file output
                let path = if path.ends_with(object_store::path::DELIMITER) {
                    path
                } else {
                    format!("{}{}", path, object_store::path::DELIMITER)
                };
                let source = source.ok_or_else(|| PlanError::invalid("missing source"))?;
                // FIXME: option compatibility
                let format_factory: Arc<dyn FileFormatFactory> = match source.as_str() {
                    "json" => {
                        table_options.set_config_format(ConfigFileType::JSON);
                        table_options.alter_with_string_hash_map(&options)?;
                        Arc::new(JsonFormatFactory::new_with_options(table_options.json))
                    }
                    "parquet" => {
                        table_options.set_config_format(ConfigFileType::PARQUET);
                        table_options.alter_with_string_hash_map(&options)?;
                        Arc::new(ParquetFormatFactory::new_with_options(
                            table_options.parquet,
                        ))
                    }
                    "csv" => {
                        table_options.set_config_format(ConfigFileType::CSV);
                        table_options.alter_with_string_hash_map(&options)?;
                        Arc::new(CsvFormatFactory::new_with_options(table_options.csv))
                    }
                    "arrow" => Arc::new(ArrowFormatFactory::new()),
                    "avro" => Arc::new(AvroFormatFactory::new()),
                    _ => {
                        return Err(PlanError::invalid(format!(
                            "unsupported source: {}",
                            source
                        )))
                    }
                };
                LogicalPlanBuilder::copy_to(
                    plan,
                    path,
                    format_as_file_type(format_factory),
                    options,
                    partitioning_columns,
                )?
                .build()?
            }
            SaveType::Table { table, save_method } => {
                let table_ref = self.resolve_table_reference(&table)?;
                match save_method {
                    TableSaveMethod::SaveAsTable => {
                        // FIXME: It is incorrect to have side-effect in the resolver.
                        // FIXME: Should we materialize the table or create a view?
                        let df = DataFrame::new(self.ctx.state(), plan);
                        self.ctx.register_table(table_ref, df.into_view())?;
                        LogicalPlan::EmptyRelation(plan::EmptyRelation {
                            produce_one_row: false,
                            schema: Arc::new(DFSchema::empty()),
                        })
                    }
                    TableSaveMethod::InsertInto => {
                        // TODO: consolidate the logic with `INSERT INTO` command
                        let table_provider = self.ctx.table_provider(table_ref.clone()).await?;
                        let fields: Vec<_> = table_provider
                            .schema()
                            .fields()
                            .iter()
                            .map(|f| f.name().clone())
                            .collect();
                        // TODO: convert input in a way similar to `SqlToRel::insert_to_plan()`
                        let plan = rename_logical_plan(plan, &fields)?;
                        let insert_op = match mode {
                            SaveMode::Append => InsertOp::Append,
                            SaveMode::Overwrite => InsertOp::Overwrite,
                            SaveMode::Replace | SaveMode::CreateOrReplace => InsertOp::Replace,
                            _ => InsertOp::Append,
                        };
                        LogicalPlanBuilder::insert_into(
                            plan,
                            table_ref,
                            &table_provider.schema(),
                            insert_op,
                        )?
                        .build()?
                    }
                }
            }
        };
        Ok(plan)
    }

    fn resolve_catalog_command(&self, command: CatalogCommand) -> PlanResult<LogicalPlan> {
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(CatalogCommandNode::try_new(command, self.config.clone())?),
        }))
    }

    async fn resolve_catalog_create_table(
        &self,
        table: spec::ObjectName,
        definition: spec::TableDefinition,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::TableDefinition {
            schema,
            comment,
            column_defaults,
            constraints,
            location,
            file_format,
            row_format,
            table_partition_cols,
            file_sort_order,
            if_not_exists,
            or_replace,
            unbounded,
            options,
            query,
            definition,
        } = definition;

        if row_format.is_some() {
            return Err(PlanError::todo("ROW FORMAT in CREATE TABLE statement"));
        }

        let (schema, query_logical_plan) = if let Some(query) = query {
            // FIXME: Query plan has cols renamed to #1, #2, etc. So I think there's more work here.
            let plan = self.resolve_query_plan(*query, state).await?;
            (plan.schema().clone(), Some(plan))
        } else {
            let fields = self.resolve_fields(&schema.fields, state)?;
            let schema = Arc::new(DFSchema::from_unqualified_fields(fields, HashMap::new())?);
            (schema, None)
        };

        let column_defaults: Vec<(String, Expr)> = async {
            let mut results: Vec<(String, Expr)> = Vec::with_capacity(column_defaults.len());
            for (name, expr) in column_defaults {
                let expr = self.resolve_expression(expr, &schema, state).await?;
                results.push((name.into(), expr));
            }
            Ok(results) as PlanResult<_>
        }
        .await?;
        let constraints = self.resolve_table_constraints(constraints, &schema)?;
        let location = if let Some(location) = location {
            location
        } else {
            // FIXME: handle name with special characters in path
            // TODO: support path with database name
            let name: String = table
                .parts()
                .last()
                .ok_or_else(|| PlanError::invalid("missing table name"))?
                .clone()
                .into();
            format!(
                "{}{}{}{}",
                self.config.default_warehouse_directory,
                object_store::path::DELIMITER,
                name,
                object_store::path::DELIMITER
            )
        };
        let file_format = if let Some(file_format) = file_format {
            match file_format {
                TableFileFormat::General { format } => format,
                TableFileFormat::Table { .. } => {
                    return Err(PlanError::todo(
                        "STORED AS INPUTFORMAT ... OUTPUTFORMAT ... in CREATE TABLE statement",
                    ));
                }
            }
        } else if unbounded {
            self.config.default_unbounded_table_file_format.clone()
        } else {
            self.config.default_bounded_table_file_format.clone()
        };
        let table_partition_cols: Vec<String> =
            table_partition_cols.into_iter().map(String::from).collect();
        let file_sort_order: Vec<Vec<Sort>> = async {
            let mut results: Vec<Vec<Sort>> = Vec::with_capacity(file_sort_order.len());
            for order in file_sort_order {
                let order = self
                    .resolve_sort_orders(order, true, &schema, state)
                    .await?;
                results.push(order);
            }
            Ok(results) as PlanResult<_>
        }
        .await?;
        let options: Vec<(String, String)> = options
            .into_iter()
            .map(|(k, v)| Ok((k, v)))
            .collect::<PlanResult<Vec<(String, String)>>>()?;

        let copy_to_plan = if let Some(query_logical_plan) = query_logical_plan {
            let mut table_options =
                TableOptions::default_from_session_config(self.ctx.state().config_options());
            let options_map = options.clone().into_iter().collect();
            let format_factory: Arc<dyn FileFormatFactory> =
                match file_format.to_lowercase().as_str() {
                    "json" => {
                        table_options.set_config_format(ConfigFileType::JSON);
                        table_options.alter_with_string_hash_map(&options_map)?;
                        Arc::new(JsonFormatFactory::new_with_options(table_options.json))
                    }
                    "parquet" => {
                        table_options.set_config_format(ConfigFileType::PARQUET);
                        table_options.alter_with_string_hash_map(&options_map)?;
                        Arc::new(ParquetFormatFactory::new_with_options(
                            table_options.parquet,
                        ))
                    }
                    "csv" => {
                        table_options.set_config_format(ConfigFileType::CSV);
                        table_options.alter_with_string_hash_map(&options_map)?;
                        Arc::new(CsvFormatFactory::new_with_options(table_options.csv))
                    }
                    "arrow" => Arc::new(ArrowFormatFactory::new()),
                    "avro" => Arc::new(AvroFormatFactory::new()),
                    _ => {
                        return Err(PlanError::invalid(format!(
                            "unsupported file_format: {}",
                            file_format
                        )))
                    }
                };
            Some(Arc::new(
                LogicalPlanBuilder::copy_to(
                    query_logical_plan,
                    location.clone(),
                    format_as_file_type(format_factory),
                    options_map,
                    table_partition_cols.clone(),
                )?
                .build()?,
            ))
        } else {
            None
        };

        let command = CatalogCommand::CreateTable {
            table: self.resolve_table_reference(&table)?,
            schema,
            comment,
            column_defaults,
            constraints,
            location,
            file_format,
            table_partition_cols,
            file_sort_order,
            if_not_exists,
            or_replace,
            unbounded,
            options,
            definition,
            copy_to_plan,
        };
        self.resolve_catalog_command(command)
    }

    fn resolve_catalog_create_database(
        &self,
        database: spec::ObjectName,
        definition: spec::DatabaseDefinition,
    ) -> PlanResult<LogicalPlan> {
        let spec::DatabaseDefinition {
            if_not_exists,
            comment,
            location,
            properties,
        } = definition;
        let properties = properties.into_iter().collect::<Vec<_>>();
        let command = CatalogCommand::CreateDatabase {
            database: self.resolve_schema_reference(&database)?,
            if_not_exists,
            comment,
            location,
            properties,
        };
        self.resolve_catalog_command(command)
    }

    fn resolve_view_name(view: spec::ObjectName) -> PlanResult<String> {
        let names: Vec<String> = view.into();
        names
            .one()
            .map_err(|_| PlanError::invalid("multi-part view name"))
    }

    async fn resolve_catalog_drop_view(
        &self,
        view: spec::ObjectName,
        kind: Option<spec::ViewKind>,
        if_exists: bool,
    ) -> PlanResult<LogicalPlan> {
        use spec::ViewKind;

        let kind = match kind {
            None => {
                let view = self.resolve_table_reference(&view)?;
                match view {
                    TableReference::Bare { table } => {
                        let temporary = manage_temporary_views(self.ctx, false, |views| {
                            Ok(views.get_view(&table)?.is_some())
                        })?;
                        if temporary {
                            ViewKind::Temporary
                        } else {
                            ViewKind::Default
                        }
                    }
                    TableReference::Partial { schema, .. } => {
                        if schema.as_ref() == self.config.global_temp_database.as_str() {
                            ViewKind::GlobalTemporary
                        } else {
                            ViewKind::Default
                        }
                    }
                    TableReference::Full { .. } => ViewKind::Default,
                }
            }
            Some(x) => x,
        };
        let command = match kind {
            ViewKind::Default => CatalogCommand::DropView {
                view: self.resolve_table_reference(&view)?,
                if_exists,
            },
            ViewKind::Temporary => CatalogCommand::DropTemporaryView {
                view_name: Self::resolve_view_name(view)?,
                is_global: false,
                if_exists,
            },
            ViewKind::GlobalTemporary => CatalogCommand::DropTemporaryView {
                view_name: Self::resolve_view_name(view)?,
                is_global: true,
                if_exists,
            },
        };
        self.resolve_catalog_command(command)
    }

    async fn resolve_catalog_create_view(
        &self,
        view: spec::ObjectName,
        definition: spec::ViewDefinition,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        use spec::ViewKind;

        let spec::ViewDefinition {
            input,
            columns,
            kind,
            replace,
            definition,
        } = definition;
        let input = self.resolve_query_plan(*input, state).await?;
        let input = LogicalPlan::SubqueryAlias(plan::SubqueryAlias::try_new(
            Arc::new(input),
            self.resolve_table_reference(&view)?,
        )?);
        let fields = match columns {
            Some(columns) => columns.into_iter().map(String::from).collect(),
            None => Self::get_field_names(input.schema(), state)?,
        };
        let input = rename_logical_plan(input, &fields)?;
        let command = match kind {
            ViewKind::Default => CatalogCommand::CreateView {
                input: Arc::new(input),
                view: self.resolve_table_reference(&view)?,
                replace,
                definition,
            },
            ViewKind::Temporary => CatalogCommand::CreateTemporaryView {
                input: Arc::new(input),
                view_name: Self::resolve_view_name(view)?,
                is_global: false,
                replace,
                definition,
            },
            ViewKind::GlobalTemporary => CatalogCommand::CreateTemporaryView {
                input: Arc::new(input),
                view_name: Self::resolve_view_name(view)?,
                is_global: true,
                replace,
                definition,
            },
        };
        self.resolve_catalog_command(command)
    }

    fn resolve_catalog_register_function(
        &self,
        function: spec::CommonInlineUserDefinedFunction,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let mut scope = state.enter_config_scope();
        let state = scope.state();
        state.config_mut().arrow_allow_large_var_types = true;
        let spec::CommonInlineUserDefinedFunction {
            function_name,
            deterministic,
            arguments: _,
            function,
        } = function;

        let function_name: String = function_name.into();
        let function_name = function_name.to_ascii_lowercase();
        let function = self.resolve_python_udf(function, state)?;
        let udf = PySparkUnresolvedUDF::new(
            function_name,
            function.python_version,
            function.eval_type,
            function.command,
            function.output_type,
            deterministic,
        );

        let command = CatalogCommand::RegisterFunction {
            udf: ScalarUDF::from(udf),
        };
        self.resolve_catalog_command(command)
    }

    fn resolve_catalog_register_table_function(
        &self,
        function: spec::CommonInlineUserDefinedTableFunction,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let mut scope = state.enter_config_scope();
        let state = scope.state();
        state.config_mut().arrow_allow_large_var_types = true;
        let spec::CommonInlineUserDefinedTableFunction {
            function_name,
            deterministic,
            arguments: _,
            function,
        } = function;
        let function_name: String = function_name.into();
        let function_name = function_name.to_ascii_lowercase();
        let function = self.resolve_python_udtf(function, state)?;
        let udtf = PySparkUnresolvedUDF::new(
            function_name,
            function.python_version,
            function.eval_type,
            function.command,
            function.return_type,
            deterministic,
        );
        // PySpark UDTF is registered as a scalar UDF since it will be used as a stream UDF
        // in the `MapPartitions` plan.
        let command = CatalogCommand::RegisterFunction {
            udf: ScalarUDF::from(udtf),
        };
        self.resolve_catalog_command(command)
    }

    async fn resolve_command_insert_into(
        &self,
        input: spec::QueryPlan,
        table: spec::ObjectName,
        columns: Vec<spec::Identifier>,
        partition_spec: Vec<(spec::Identifier, Option<spec::Expr>)>,
        overwrite: bool,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        if !partition_spec.is_empty() {
            return Err(PlanError::todo("partitioned insert"));
        }

        let input = self.resolve_query_plan(input, state).await?;
        let table_reference = self.resolve_table_reference(&table)?;
        let table_provider = self.ctx.table_provider(table_reference.clone()).await?;
        let schema = if columns.is_empty() {
            table_provider.schema()
        } else {
            self.resolve_schema_projection(table_provider.schema(), &columns)
                .await?
        };
        let df_schema = Arc::new(DFSchema::try_from_qualified_schema(
            table_reference.clone(),
            &schema,
        )?);
        let table_source = provider_as_source(table_provider);
        let fields = schema
            .fields
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>();

        let exprs = table_source
            .schema()
            .fields()
            .iter()
            .map(|field| {
                let expr = match fields.iter().find(|f| f == &field.name()) {
                    Some(matched_field) => Expr::Column(Column::from(matched_field))
                        .cast_to(field.data_type(), &df_schema)?,
                    None => table_source
                        .get_column_default(field.name())
                        .cloned()
                        .unwrap_or_else(|| Expr::Literal(ScalarValue::Null))
                        .cast_to(field.data_type(), &DFSchema::empty())?,
                };
                Ok(expr.alias(field.name()))
            })
            .collect::<PlanResult<Vec<_>>>()?;

        let input = project(rename_logical_plan(input, &fields)?, exprs)?;
        let insert_op = match overwrite {
            // TODO: resolve_command_insert_into should pass in insert_op instead of overwrite
            true => InsertOp::Overwrite,
            false => InsertOp::Append,
        };
        let plan =
            LogicalPlanBuilder::insert_into(input, table_reference, schema.as_ref(), insert_op)?
                .build()?;
        Ok(plan)
    }

    async fn resolve_query_fill_na(
        &self,
        input: spec::QueryPlan,
        columns: Vec<spec::Identifier>,
        values: Vec<spec::Expr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        enum Strategy {
            All { value: Expr },
            Columns { columns: Vec<String>, value: Expr },
            EachColumn { columns: Vec<(String, Expr)> },
        }

        let input = self.resolve_query_plan(input, state).await?;
        let schema = input.schema();
        let values = self.resolve_expressions(values, schema, state).await?;
        let columns: Vec<String> = columns.into_iter().map(|x| x.into()).collect();

        if values.is_empty() {
            return Err(PlanError::invalid("missing fill na values"));
        }
        let strategy = if columns.is_empty() {
            let Ok(value) = values.one() else {
                return Err(PlanError::invalid(
                    "expected one value to fill na for all columns",
                ));
            };
            Strategy::All { value }
        } else if values.len() == 1 {
            let value = values.one()?;
            Strategy::Columns { columns, value }
        } else {
            if values.len() != columns.len() {
                return Err(PlanError::invalid(
                    "fill na number of values does not match number of columns",
                ));
            }
            let columns: Vec<(String, Expr)> =
                columns.into_iter().zip(values.into_iter()).collect();
            Strategy::EachColumn { columns }
        };

        let fill_na_exprs = schema
            .iter()
            .map(|(qualifier, field)| {
                let info = state.get_field_info(field.name())?;
                let value = match &strategy {
                    Strategy::All { value } => Some(value.clone()),
                    Strategy::Columns { columns, value } => columns
                        .iter()
                        .any(|col| info.matches(col, None))
                        .then(|| value.clone()),
                    Strategy::EachColumn { columns } => columns
                        .iter()
                        .find_map(|(col, val)| info.matches(col, None).then(|| val.clone())),
                };
                let column_expr = col((qualifier, field));
                let expr = if let Some(value) = value {
                    let value_type = value.get_type(schema)?;
                    if self.can_cast_fill_na_types(&value_type, field.data_type()) {
                        let value = Expr::TryCast(TryCast {
                            expr: Box::new(value),
                            data_type: field.data_type().clone(),
                        });
                        when(column_expr.clone().is_null(), value).otherwise(column_expr)?
                    } else {
                        column_expr
                    }
                } else {
                    column_expr
                };
                Ok(NamedExpr::new(vec![info.name().to_string()], expr))
            })
            .collect::<PlanResult<Vec<_>>>()?;

        Ok(LogicalPlan::Projection(plan::Projection::try_new(
            self.rewrite_named_expressions(fill_na_exprs, state)?,
            Arc::new(input),
        )?))
    }

    fn can_cast_fill_na_types(&self, from_type: &adt::DataType, to_type: &adt::DataType) -> bool {
        // Spark only supports 4 data types for fill na: bool, long, double, string
        if from_type == to_type {
            return true;
        }
        match (from_type, to_type) {
            (
                adt::DataType::Utf8 | adt::DataType::LargeUtf8,
                adt::DataType::Utf8 | adt::DataType::LargeUtf8,
            ) => true,
            (adt::DataType::Null, _) => true,
            (_, adt::DataType::Null) => true,
            // Only care about checking numeric types because we do TryCast.
            (_, _) => from_type.is_numeric() && to_type.is_numeric(),
        }
    }

    async fn resolve_query_drop_na(
        &self,
        input: spec::QueryPlan,
        columns: Vec<spec::Identifier>,
        min_non_nulls: Option<usize>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self.resolve_query_plan(input, state).await?;
        let schema = input.schema();

        let not_null_exprs = if columns.is_empty() {
            schema
                .columns()
                .into_iter()
                .map(|c| col(c).is_not_null())
                .collect::<Vec<Expr>>()
        } else {
            let columns: Vec<String> = columns.into_iter().map(|x| x.into()).collect();
            schema
                .columns()
                .into_iter()
                .filter(|column| {
                    state
                        .get_field_info(column.name())
                        .is_ok_and(|info| columns.iter().any(|c| info.matches(c, None)))
                })
                .map(|c| col(c).is_not_null())
                .collect::<Vec<Expr>>()
        };

        let filter_expr = match min_non_nulls {
            Some(min_non_nulls) if min_non_nulls > 0 => {
                let non_null_count = not_null_exprs
                    .into_iter()
                    .map(|expr| Ok(when(expr, lit(1)).otherwise(lit(0))?))
                    .try_fold(lit(0), |acc: Expr, predicate: PlanResult<Expr>| {
                        Ok(Expr::BinaryExpr(BinaryExpr::new(
                            Box::new(acc),
                            Operator::Plus,
                            Box::new(predicate?),
                        ))) as PlanResult<Expr>
                    })?;
                non_null_count.gt_eq(lit(min_non_nulls as u32))
            }
            _ => conjunction(not_null_exprs)
                .ok_or_else(|| PlanError::invalid("No columns specified for drop na."))?,
        };

        Ok(LogicalPlan::Filter(plan::Filter::try_new(
            filter_expr,
            Arc::new(input),
        )?))
    }

    async fn resolve_query_replace(
        &self,
        input: spec::QueryPlan,
        columns: Vec<spec::Identifier>,
        replacements: Vec<spec::Replacement>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self.resolve_query_plan(input, state).await?;
        let schema = input.schema();
        let columns: Vec<String> = columns.into_iter().map(|x| x.into()).collect();
        let replacements: Vec<(Expr, Expr)> = replacements
            .into_iter()
            .map(|r| {
                Ok((
                    lit(self.resolve_literal(r.old_value, state)?),
                    lit(self.resolve_literal(r.new_value, state)?),
                ))
            })
            .collect::<PlanResult<_>>()?;

        let replace_exprs = schema
            .iter()
            .map(|(qualifier, field)| {
                let info = state.get_field_info(field.name())?;
                let column_expr = col((qualifier, field));
                let expr =
                    if columns.is_empty() || columns.iter().any(|col| info.matches(col, None)) {
                        let when_then_expr = replacements
                            .iter()
                            .map(|(old, new)| {
                                let new = Expr::TryCast(TryCast {
                                    expr: Box::new(new.clone()),
                                    data_type: field.data_type().clone(),
                                });
                                (Box::new(column_expr.clone().eq(old.clone())), Box::new(new))
                            })
                            .collect();
                        Expr::Case(datafusion_expr::Case {
                            expr: None,
                            when_then_expr,
                            else_expr: Some(Box::new(column_expr)),
                        })
                    } else {
                        column_expr
                    };
                Ok(NamedExpr::new(vec![info.name().to_string()], expr))
            })
            .collect::<PlanResult<Vec<_>>>()?;

        Ok(LogicalPlan::Projection(plan::Projection::try_new(
            self.rewrite_named_expressions(replace_exprs, state)?,
            Arc::new(input),
        )?))
    }

    async fn resolve_command_set_variable(
        &self,
        variable: String,
        value: String,
    ) -> PlanResult<LogicalPlan> {
        let variable = if variable.eq_ignore_ascii_case("timezone")
            || variable.eq_ignore_ascii_case("time.zone")
        {
            "datafusion.execution.time_zone".to_string()
        } else {
            variable
        };
        let statement = plan::Statement::SetVariable(plan::SetVariable { variable, value });

        Ok(LogicalPlan::Statement(statement))
    }

    fn remove_hidden_fields(
        &self,
        plan: LogicalPlan,
        state: &PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let mut columns = vec![];
        let mut has_hidden_columns = false;
        for column in plan.schema().columns() {
            let info = state.get_field_info(column.name())?;
            if info.is_hidden() {
                has_hidden_columns = true;
            } else {
                columns.push(column);
            }
        }
        if has_hidden_columns {
            let plan = LogicalPlanBuilder::new(plan)
                .project(columns.into_iter().map(Expr::Column))?
                .build()?;
            Ok(plan)
        } else {
            Ok(plan)
        }
    }

    /// All resolved plans must have "resolved columns".
    /// If you define new fields in the plan, register the field in the state and use the "resolved field name" to alias the newly created field.
    /// If you fetch an existing field in the plan, you likely have the "unresolved" field name from the spec.
    /// Convert the unresolved field name to the "resolved field name" using the state.
    fn verify_query_plan(&self, plan: &LogicalPlan, state: &PlanResolverState) -> PlanResult<()> {
        let invalid = plan
            .schema()
            .fields()
            .iter()
            .filter_map(|f| {
                if state.get_field_info(f.name()).is_ok() {
                    None
                } else {
                    Some(f.name().to_string())
                }
            })
            .collect::<Vec<_>>();
        if invalid.is_empty() {
            Ok(())
        } else {
            Err(PlanError::internal(format!(
                "a plan resolver bug has produced invalid fields: {invalid:?}",
            )))
        }
    }

    fn register_schema_with_plan_id(
        &self,
        plan: &LogicalPlan,
        plan_id: Option<i64>,
        state: &mut PlanResolverState,
    ) -> PlanResult<()> {
        if let Some(plan_id) = plan_id {
            for field in plan.schema().fields() {
                state.register_plan_id_for_field(field.name(), plan_id)?;
            }
        }
        Ok(())
    }

    fn resolve_expressions_positions(
        &self,
        exprs: Vec<NamedExpr>,
        projections: &[NamedExpr],
    ) -> PlanResult<Vec<NamedExpr>> {
        let num_projections = projections.len() as i64;
        exprs
            .into_iter()
            .map(|named_expr| {
                let NamedExpr { expr, .. } = &named_expr;
                match expr {
                    Expr::Literal(scalar_value) => {
                        let position = match scalar_value {
                            ScalarValue::Int32(Some(position)) => *position as i64,
                            ScalarValue::Int64(Some(position)) => *position,
                            _ => return Ok(named_expr),
                        };
                        if position > 0_i64 && position <= num_projections {
                            Ok(projections[(position - 1) as usize].clone())
                        } else {
                            Err(PlanError::invalid(format!(
                                "Cannot resolve column position {position}. Valid positions are 1 to {num_projections}."
                            )))
                        }
                    }
                    _ => Ok(named_expr),
                }
            })
            .collect()
    }

    async fn resolve_query_stat_cov(
        &self,
        input: spec::QueryPlan,
        left_column: spec::Identifier,
        right_column: spec::Identifier,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self.resolve_query_plan(input, state).await?;
        let covar_samp = Expr::AggregateFunction(expr::AggregateFunction {
            func: datafusion::functions_aggregate::covariance::covar_samp_udaf(),
            args: vec![
                Expr::Column(self.resolve_one_column(
                    input.schema(),
                    left_column.as_ref(),
                    state,
                )?),
                Expr::Column(self.resolve_one_column(
                    input.schema(),
                    right_column.as_ref(),
                    state,
                )?),
            ],
            distinct: false,
            filter: None,
            order_by: None,
            null_treatment: None,
        })
        .alias(state.register_field_name("cov"));
        Ok(LogicalPlanBuilder::from(input)
            .aggregate(Vec::<Expr>::new(), vec![covar_samp])?
            .build()?)
    }

    async fn resolve_query_stat_corr(
        &self,
        input: spec::QueryPlan,
        left_column: spec::Identifier,
        right_column: spec::Identifier,
        method: String,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        if !method.eq_ignore_ascii_case("pearson") {
            return Err(PlanError::unsupported(format!(
                "Unsupported correlation method: {method}. Currently only Pearson is supported.",
            )));
        }
        let input = self.resolve_query_plan(input, state).await?;
        let corr = Expr::AggregateFunction(expr::AggregateFunction {
            func: datafusion::functions_aggregate::correlation::corr_udaf(),
            args: vec![
                Expr::Column(self.resolve_one_column(
                    input.schema(),
                    left_column.as_ref(),
                    state,
                )?),
                Expr::Column(self.resolve_one_column(
                    input.schema(),
                    right_column.as_ref(),
                    state,
                )?),
            ],
            distinct: false,
            filter: None,
            order_by: None,
            null_treatment: None,
        })
        .alias(state.register_field_name("corr"));
        Ok(LogicalPlanBuilder::from(input)
            .aggregate(Vec::<Expr>::new(), vec![corr])?
            .build()?)
    }

    fn rewrite_aggregate(
        &self,
        input: LogicalPlan,
        projections: Vec<NamedExpr>,
        grouping: Vec<NamedExpr>,
        having: Option<Expr>,
        with_grouping_expressions: bool,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let grouping = self.resolve_expressions_positions(grouping, &projections)?;
        let mut aggregate_candidates = projections
            .iter()
            .map(|x| x.expr.clone())
            .collect::<Vec<_>>();
        if let Some(having) = having.as_ref() {
            aggregate_candidates.push(having.clone());
        }
        let aggregate_exprs = find_aggregate_exprs(&aggregate_candidates);
        let group_exprs = grouping.iter().map(|x| x.expr.clone()).collect::<Vec<_>>();
        let plan = LogicalPlanBuilder::from(input)
            .aggregate(group_exprs, aggregate_exprs.clone())?
            .build()?;
        let (grouping_exprs, aggregate_or_grouping_exprs) = {
            let mut grouping_exprs = vec![];
            let mut aggregate_or_grouping_exprs = aggregate_exprs;
            for expr in grouping {
                let NamedExpr {
                    name,
                    expr,
                    metadata,
                } = expr;
                let exprs = match expr {
                    Expr::GroupingSet(g) => g.distinct_expr().into_iter().cloned().collect(),
                    expr => vec![expr],
                };
                if name.len() != exprs.len() {
                    return Err(PlanError::internal(format!(
                        "group-by name count does not match expression count: {name:?} {exprs:?}",
                    )));
                }
                grouping_exprs.extend(exprs.iter().zip(name.into_iter()).map(|(expr, name)| {
                    NamedExpr {
                        name: vec![name],
                        expr: expr.clone(),
                        metadata: metadata.clone(),
                    }
                }));
                aggregate_or_grouping_exprs.extend(exprs);
            }
            (grouping_exprs, aggregate_or_grouping_exprs)
        };
        let projections = if with_grouping_expressions {
            grouping_exprs.into_iter().chain(projections).collect()
        } else {
            projections
        };
        let projections = projections
            .into_iter()
            .map(|x| {
                let NamedExpr {
                    name,
                    expr,
                    metadata,
                } = x;
                let expr = rebase_expression(expr, &aggregate_or_grouping_exprs, &plan)?;
                Ok(NamedExpr {
                    name,
                    expr,
                    metadata,
                })
            })
            .collect::<PlanResult<Vec<_>>>()?;
        let plan = match having {
            Some(having) => {
                let having =
                    rebase_expression(having.clone(), &aggregate_or_grouping_exprs, &plan)?;
                LogicalPlanBuilder::from(plan).having(having)?.build()?
            }
            None => plan,
        };
        let (plan, projections) =
            self.rewrite_projection::<ExplodeRewriter>(plan, projections, state)?;
        let (plan, projections) =
            self.rewrite_projection::<WindowRewriter>(plan, projections, state)?;
        let projections = projections
            .into_iter()
            .map(|x| {
                let NamedExpr {
                    name,
                    expr,
                    metadata: _,
                } = x;
                Ok(expr.alias(state.register_field_name(name.one()?)))
            })
            .collect::<PlanResult<Vec<_>>>()?;
        Ok(LogicalPlanBuilder::from(plan)
            .project(projections)?
            .build()?)
    }

    fn rewrite_wildcard(
        &self,
        input: LogicalPlan,
        expr: Vec<NamedExpr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<(LogicalPlan, Vec<NamedExpr>)> {
        fn to_named_expr(expr: Expr, state: &PlanResolverState) -> PlanResult<Option<NamedExpr>> {
            let Expr::Column(column) = expr else {
                return Err(PlanError::invalid(
                    "column expected for expanded wildcard expression",
                ));
            };
            let info = state.get_field_info(column.name())?;
            if info.is_hidden() {
                return Ok(None);
            }
            Ok(Some(NamedExpr::new(
                vec![info.name().to_string()],
                Expr::Column(column),
            )))
        }

        let schema = input.schema();
        let mut projected = vec![];
        for e in expr {
            let NamedExpr {
                name,
                expr,
                metadata,
            } = e;
            // FIXME: wildcard options do not take into account opaque field IDs
            match expr {
                Expr::Wildcard {
                    qualifier: None,
                    options,
                } => {
                    for e in expand_wildcard(schema, &input, Some(&options))? {
                        projected.extend(to_named_expr(e, state)?)
                    }
                }
                Expr::Wildcard {
                    qualifier: Some(qualifier),
                    options,
                } => {
                    for e in expand_qualified_wildcard(&qualifier, schema, Some(&options))? {
                        projected.extend(to_named_expr(e, state)?)
                    }
                }
                _ => projected.push(NamedExpr {
                    name,
                    expr: columnize_expr(normalize_col(expr, &input)?, &input)?,
                    metadata,
                }),
            }
        }
        Ok((input, projected))
    }

    pub(super) fn rewrite_projection<'s, T>(
        &self,
        input: LogicalPlan,
        expr: Vec<NamedExpr>,
        state: &'s mut PlanResolverState,
    ) -> PlanResult<(LogicalPlan, Vec<NamedExpr>)>
    where
        T: PlanRewriter<'s> + TreeNodeRewriter<Node = Expr>,
    {
        let mut rewriter = T::new_from_plan(input, state);
        let expr = expr
            .into_iter()
            .map(|e| {
                let NamedExpr {
                    name,
                    expr,
                    metadata,
                } = e;
                Ok(NamedExpr {
                    name,
                    expr: expr.rewrite(&mut rewriter)?.data,
                    metadata,
                })
            })
            .collect::<PlanResult<Vec<_>>>()?;
        Ok((rewriter.into_plan(), expr))
    }

    fn rewrite_multi_expr(&self, expr: Vec<NamedExpr>) -> PlanResult<Vec<NamedExpr>> {
        let mut out = vec![];
        for e in expr {
            let NamedExpr {
                name,
                expr,
                metadata,
            } = e;
            match expr {
                Expr::ScalarFunction(ScalarFunction { func, args }) => {
                    if func.inner().as_any().is::<MultiExpr>() {
                        // The metadata from the original expression are ignored.
                        if name.len() == args.len() {
                            for (name, arg) in name.into_iter().zip(args) {
                                out.push(NamedExpr::new(vec![name], arg));
                            }
                        } else {
                            for arg in args {
                                out.push(NamedExpr::try_from_alias_expr(arg)?);
                            }
                        }
                    } else {
                        out.push(NamedExpr {
                            name,
                            expr: func.call(args),
                            metadata,
                        });
                    }
                }
                _ => {
                    out.push(NamedExpr {
                        name,
                        expr,
                        metadata,
                    });
                }
            };
        }
        Ok(out)
    }

    /// Rewrite named expressions to DataFusion expressions.
    /// A field is registered for each name.
    /// If the expression is a column expression, all plan IDs for the column are registered for the field.
    /// This means the column must refer to a **registered field** of the input plan. Otherwise, the column must be wrapped with an alias.
    pub(super) fn rewrite_named_expressions(
        &self,
        expr: Vec<NamedExpr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<Expr>> {
        expr.into_iter()
            .map(|e| {
                let NamedExpr {
                    name,
                    expr,
                    metadata: _, // TODO: set field metadata
                } = e;
                let name = if name.len() == 1 {
                    name.one()?
                } else {
                    let names = format!("({})", name.join(", "));
                    return Err(PlanError::invalid(format!(
                        "one name expected for expression, got: {names}"
                    )));
                };
                let plan_ids = if let Expr::Column(Column { name: field_id, .. }) = &expr {
                    let info = state.get_field_info(field_id)?;
                    info.plan_ids()
                } else {
                    vec![]
                };
                let field_id = state.register_field_name(name);
                for plan_id in plan_ids {
                    state.register_plan_id_for_field(&field_id, plan_id)?;
                }
                Ok(expr.alias(field_id))
            })
            .collect()
    }

    /// Resolves argument offsets for group map operations.
    /// Returns the deduplicated argument expressions and the offset array.
    /// The result offset array `offsets` has the following layout.
    ///   `offsets[0]`: the length of the offset array.
    ///   `offsets[1]`: the number of grouping (key) expressions.
    ///   `offsets[2..offsets[1]+2]`: the offsets of the grouping (key) expressions.
    ///   `offsets[offsets[1]+2..offsets[0]+1]`: the offsets of the data (value) expressions.
    /// See also:
    ///   org.apache.spark.sql.execution.python.PandasGroupUtils#resolveArgOffsets
    fn resolve_group_map_argument_offsets(
        exprs: &[NamedExpr],
        grouping_exprs: &[NamedExpr],
    ) -> PlanResult<(Vec<NamedExpr>, Vec<usize>)> {
        let mut out = exprs.to_vec();
        let mut key_offsets = vec![];
        let mut value_offsets = vec![];
        for expr in grouping_exprs {
            if let Some(pos) = exprs.iter().position(|x| x == expr) {
                key_offsets.push(pos);
            } else {
                let pos = out.len();
                out.push(expr.clone());
                key_offsets.push(pos);
            }
        }
        for i in 0..exprs.len() {
            value_offsets.push(i);
        }
        let mut offsets = Vec::with_capacity(2 + key_offsets.len() + value_offsets.len());
        offsets.push(1 + key_offsets.len() + value_offsets.len());
        offsets.push(key_offsets.len());
        offsets.extend(key_offsets);
        offsets.extend(value_offsets);
        Ok((out, offsets))
    }

    fn resolve_expression_types(
        exprs: &[Expr],
        schema: &DFSchema,
    ) -> PlanResult<Vec<adt::DataType>> {
        exprs
            .iter()
            .map(|arg| {
                let (data_type, _) = arg.data_type_and_nullable(schema)?;
                Ok(data_type)
            })
            .collect::<PlanResult<Vec<_>>>()
    }
}

/// Reference: [datafusion_sql::utils::rebase_expr]
fn rebase_expression(expr: Expr, base: &[Expr], plan: &LogicalPlan) -> PlanResult<Expr> {
    Ok(expr
        .transform_down(|e| {
            if base.contains(&e) {
                Ok(Transformed::yes(expr_as_column_expr(&e, plan)?))
            } else {
                Ok(Transformed::no(e))
            }
        })
        .data()?)
}

struct CoGroupMapData {
    plan: LogicalPlan,
    grouping: Vec<Expr>,
    mapper_input: Expr,
    mapper_input_types: Vec<adt::DataType>,
    mapper_input_names: Vec<String>,
    offsets: Vec<usize>,
}
