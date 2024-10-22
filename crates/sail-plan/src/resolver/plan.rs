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
use datafusion::datasource::function::TableFunction;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion::datasource::{provider_as_source, MemTable, TableProvider};
use datafusion::logical_expr::{logical_plan as plan, Expr, Extension, LogicalPlan, UNNAMED_TABLE};
use datafusion_common::config::{ConfigFileType, TableOptions};
use datafusion_common::display::{PlanType, StringifiedPlan, ToStringifiedPlan};
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRewriter};
use datafusion_common::{
    Column, DFSchema, DFSchemaRef, ParamValues, ScalarValue, TableReference, ToDFSchema,
};
use datafusion_expr::builder::project;
use datafusion_expr::expr::{ScalarFunction, Sort};
use datafusion_expr::expr_rewriter::normalize_col;
use datafusion_expr::utils::{
    columnize_expr, conjunction, expand_qualified_wildcard, expand_wildcard, expr_as_column_expr,
    find_aggregate_exprs,
};
use datafusion_expr::{
    build_join_schema, col, lit, when, BinaryExpr, DmlStatement, ExprSchemable, LogicalPlanBuilder,
    Operator, ScalarUDF, TryCast, WriteOp,
};
use sail_common::spec;
use sail_common::utils::{cast_record_batch, read_record_batches, rename_logical_plan};
use sail_python_udf::udf::pyspark_udtf::PySparkUDTF;
use sail_python_udf::udf::unresolved_pyspark_udf::UnresolvedPySparkUDF;

use crate::error::{PlanError, PlanResult};
use crate::extension::function::multi_expr::MultiExpr;
use crate::extension::logical::{
    CatalogCommand, CatalogCommandNode, CatalogTableFunction, RangeNode, ShowStringFormat,
    ShowStringNode, ShowStringStyle, SortWithinPartitionsNode,
};
use crate::extension::source::rename::RenameTableProvider;
use crate::resolver::expression::NamedExpr;
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
                let fields = Some(state.get_field_names(plan.schema().inner())?);
                Ok(NamedPlan { plan, fields })
            }
            spec::Plan::Command(command) => {
                let plan = self.resolve_command_plan(command, &mut state).await?;
                Ok(NamedPlan { plan, fields: None })
            }
        }
    }

    #[async_recursion]
    pub(super) async fn resolve_query_plan(
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
            QueryNode::Offset { input, offset } => {
                self.resolve_query_offset(*input, offset, state).await?
            }
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
            QueryNode::ReplaceNa { .. } => {
                return Err(PlanError::todo("replace"));
            }
            QueryNode::StatSummary { .. } => {
                return Err(PlanError::todo("summary"));
            }
            QueryNode::StatCrosstab { .. } => {
                return Err(PlanError::todo("crosstab"));
            }
            QueryNode::StatDescribe { .. } => {
                return Err(PlanError::todo("describe"));
            }
            QueryNode::StatCov { .. } => {
                return Err(PlanError::todo("cov"));
            }
            QueryNode::StatCorr { .. } => {
                return Err(PlanError::todo("corr"));
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
        };
        self.verify_query_plan(&plan, state)?;
        self.register_schema_with_plan_id(&plan, plan_id, state)?;
        Ok(plan)
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
                self.resolve_catalog_register_function(function)
            }
            CommandNode::RegisterTableFunction(function) => {
                self.resolve_catalog_register_table_function(function)
            }
            CommandNode::CreateView { view, definition } => {
                self.resolve_catalog_create_view(view, definition, state)
                    .await
            }
            CommandNode::Write(write) => self.resolve_command_write(write, state).await,
            CommandNode::Explain { mode, input } => {
                self.resolve_command_explain(*input, mode, state).await
            }
            CommandNode::InsertInto {
                input,
                table,
                columns,
                partition_spec,
                overwrite,
            } => {
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
            CommandNode::SetVariable { variable, value } => {
                self.resolve_command_set_variable(variable, value).await
            }
            CommandNode::Update {
                input,
                table,
                table_alias,
                assignments,
            } => {
                self.resolve_command_update(*input, table, table_alias, assignments, state)
                    .await
            }
            CommandNode::Delete { table, condition } => {
                self.resolve_command_delete(table, condition, state).await
            }
            CommandNode::AlterTable { .. } => Err(PlanError::todo("CommandNode::AlterTable")),
        }
    }

    async fn resolve_query_read_named_table(
        &self,
        table: spec::ReadNamedTable,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::ReadNamedTable { name, options } = table;

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
            let names = state.register_fields(view.schema().inner());
            return Ok(rename_logical_plan(view, &names)?);
        }

        if !options.is_empty() {
            return Err(PlanError::todo("table options"));
        }
        let table_provider = self.ctx.table_provider(table_reference.clone()).await?;
        let names = state.register_fields(&table_provider.schema());
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
        let spec::ReadUdtf {
            name,
            arguments,
            options,
        } = udtf;
        if !options.is_empty() {
            return Err(PlanError::todo("ReadType::UDTF options"));
        }
        // TODO: Handle qualified table reference.
        let function_name = self.resolve_table_reference(&name)?;
        let function_name = function_name.table();
        let schema = Arc::new(DFSchema::empty());
        let (_, arguments) = self
            .resolve_alias_expressions_and_names(arguments, &schema, state)
            .await?;
        let table_function = self.ctx.table_function(function_name)?;
        let table_provider = table_function.create_table_provider(&arguments)?;
        let names = state.register_fields(&table_provider.schema());
        let table_provider = RenameTableProvider::try_new(table_provider, names)?;
        Ok(LogicalPlan::TableScan(plan::TableScan::try_new(
            function_name,
            provider_as_source(Arc::new(table_provider)),
            None,
            vec![],
            None,
        )?))
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
        let schema = self.resolve_listing_schema(&urls, &options, schema).await?;
        let config = ListingTableConfig::new_with_multi_paths(urls)
            .with_listing_options(options)
            .with_schema(Arc::new(schema));
        let table_provider = Arc::new(ListingTable::try_new(config)?);
        let names = state.register_fields(&table_provider.schema());
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
            Some(x) => self.resolve_query_plan(x, state).await?,
            None => LogicalPlan::EmptyRelation(plan::EmptyRelation {
                // allows literal projection with no input
                produce_one_row: true,
                schema: DFSchemaRef::new(DFSchema::empty()),
            }),
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
        let input = self.resolve_query_plan(input, state).await?;
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
        use spec::JoinType;

        let spec::Join {
            left,
            right,
            join_condition,
            join_type,
            using_columns,
            join_data_type,
        } = join;
        let left = self.resolve_query_plan(*left, state).await?;
        let right = self.resolve_query_plan(*right, state).await?;
        let (join_type, is_cross_join) = match join_type {
            JoinType::Inner => (plan::JoinType::Inner, false),
            JoinType::LeftOuter => (plan::JoinType::Left, false),
            JoinType::RightOuter => (plan::JoinType::Right, false),
            JoinType::FullOuter => (plan::JoinType::Full, false),
            JoinType::LeftSemi => (plan::JoinType::LeftSemi, false),
            JoinType::LeftAnti => (plan::JoinType::LeftAnti, false),
            // use inner join type to build the schema for cross join
            JoinType::Cross => (plan::JoinType::Inner, true),
        };
        let join_schema = Arc::new(build_join_schema(
            left.schema(),
            right.schema(),
            &join_type,
        )?);

        if is_cross_join {
            if join_condition.is_some() {
                return Err(PlanError::invalid("cross join with join condition"));
            }
            if !using_columns.is_empty() {
                return Err(PlanError::invalid("cross join with using columns"));
            }
            if join_data_type.is_some() {
                return Err(PlanError::invalid("cross join with join data type"));
            }
            return Ok(LogicalPlan::CrossJoin(plan::CrossJoin {
                left: Arc::new(left),
                right: Arc::new(right),
                schema: join_schema,
            }));
        }
        // TODO: add more validation logic here and in the plan optimizer
        //  See `LogicalPlanBuilder` for details about such logic.
        if join_condition.is_some() && using_columns.is_empty() {
            let condition = match join_condition {
                Some(condition) => Some(
                    self.resolve_expression(condition, &join_schema, state)
                        .await?
                        .unalias_nested()
                        .data,
                ),
                None => None,
            };
            let plan = LogicalPlanBuilder::from(left)
                .join_on(right, join_type, condition)?
                .build()?;
            Ok(plan)
        } else if join_condition.is_none() && !using_columns.is_empty() {
            let left_names = state.get_field_names(left.schema().inner())?;
            let right_names = state.get_field_names(right.schema().inner())?;
            let on = using_columns
                .into_iter()
                .map(|name| {
                    let name: &str = (&name).into();
                    let left_idx = left_names
                        .iter()
                        .position(|left_name| left_name.eq_ignore_ascii_case(name))
                        .ok_or_else(|| {
                            PlanError::invalid(format!("left column not found: {name}"))
                        })?;
                    let right_idx = right_names
                        .iter()
                        .position(|right_name| right_name.eq_ignore_ascii_case(name))
                        .ok_or_else(|| {
                            PlanError::invalid(format!("right column not found: {name}"))
                        })?;
                    let left_column = Column::from(left.schema().qualified_field(left_idx));
                    let right_column = Column::from(right.schema().qualified_field(right_idx));
                    Ok((Expr::Column(left_column), Expr::Column(right_column)))
                })
                .collect::<PlanResult<Vec<(Expr, Expr)>>>()?;
            Ok(LogicalPlan::Join(plan::Join {
                left: Arc::new(left),
                right: Arc::new(right),
                on,
                filter: None,
                join_type,
                join_constraint: plan::JoinConstraint::Using,
                schema: join_schema,
                null_equals_null: false,
            }))
        } else {
            return Err(PlanError::invalid(
                "expecting either join condition or using columns",
            ));
        }
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
                    let left_names = state.get_field_names(left.schema().inner())?;
                    let right_names = state.get_field_names(right.schema().inner())?;
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
                                        .alias(state.register_field(left_name)),
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
                                            .alias(state.register_field(right_name)),
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
                let (left, right) = (Arc::new(left), Arc::new(right));
                let union_schema = left.schema().clone();
                if is_all {
                    Ok(LogicalPlan::Union(plan::Union {
                        inputs: vec![left, right],
                        schema: union_schema,
                    }))
                } else {
                    Ok(LogicalPlan::Distinct(plan::Distinct::All(Arc::new(
                        LogicalPlan::Union(plan::Union {
                            inputs: vec![left, right],
                            schema: union_schema,
                        }),
                    ))))
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
        let input = self.resolve_query_plan(input, state).await?;
        let schema = input.schema();
        let expr = self.resolve_sort_orders(order, true, schema, state).await?;
        if is_global {
            Ok(LogicalPlan::Sort(plan::Sort {
                expr,
                input: Arc::new(input),
                fetch: None,
            }))
        } else {
            Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(SortWithinPartitionsNode::new(Arc::new(input), expr, None)),
            }))
        }
    }

    async fn resolve_query_limit(
        &self,
        input: spec::QueryPlan,
        skip: usize,
        limit: usize,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self.resolve_query_plan(input, state).await?;
        Ok(LogicalPlan::Limit(plan::Limit {
            skip,
            fetch: Some(limit),
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

        let input = self.resolve_query_plan(*input, state).await?;
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
        let input = self.resolve_query_plan(input, state).await?;
        let input = if !positional.is_empty() {
            let params = positional
                .into_iter()
                .map(|arg| self.resolve_literal(arg))
                .collect::<PlanResult<_>>()?;
            input.with_param_values(ParamValues::List(params))?
        } else {
            input
        };
        if !named.is_empty() {
            let params = named
                .into_iter()
                .map(|(name, arg)| -> PlanResult<(String, ScalarValue)> {
                    Ok((name, self.resolve_literal(arg)?))
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
            read_record_batches(data)?
        } else {
            vec![]
        };
        let (schema, batches) = if let Some(schema) = schema {
            let schema: adt::SchemaRef = Arc::new(self.resolve_schema(schema)?);
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
        let names = state.register_fields(&schema);
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

    async fn resolve_query_offset(
        &self,
        input: spec::QueryPlan,
        offset: usize,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self.resolve_query_plan(input, state).await?;
        Ok(LogicalPlan::Limit(plan::Limit {
            skip: offset,
            fetch: None,
            input: Arc::new(input),
        }))
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
        let input = self.resolve_query_plan(*input, state).await?;
        let schema = input.schema();
        if within_watermark {
            return Err(PlanError::todo("deduplicate within watermark"));
        }
        if !column_names.is_empty() && !all_columns_as_keys {
            let column_names: Vec<String> = column_names.into_iter().map(|x| x.into()).collect();
            let on_expr: Vec<Expr> = schema
                .columns()
                .into_iter()
                .filter(|column| {
                    state
                        .get_field_name(column.name())
                        .is_ok_and(|x| column_names.contains(x))
                })
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
        let resolved = state.register_field("id");
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(RangeNode::try_new(
                resolved,
                start,
                end,
                step,
                num_partitions,
            )?),
        }))
    }

    async fn resolve_query_subquery_alias(
        &self,
        input: spec::QueryPlan,
        alias: spec::Identifier,
        qualifier: Vec<spec::Identifier>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        Ok(LogicalPlan::SubqueryAlias(plan::SubqueryAlias::try_new(
            Arc::new(self.resolve_query_plan(input, state).await?),
            self.resolve_table_reference(&spec::ObjectName::new_qualified(alias, qualifier))?,
        )?))
    }

    async fn resolve_query_repartition(
        &self,
        input: spec::QueryPlan,
        num_partitions: usize,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self.resolve_query_plan(input, state).await?;
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
                let name = state.get_field_name(column.name())?;
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
        let mut excluded_names = vec![];
        let mut excluded_fields = vec![];
        for col in column_names {
            excluded_names.push(col.into());
        }
        for col in columns {
            if let spec::Expr::UnresolvedAttribute { name, plan_id } = col {
                let name: Vec<String> = name.into();
                let name = name
                    .one()
                    .map_err(|_| PlanError::invalid("expecting a single column name to drop"))?;
                if let Some(plan_id) = plan_id {
                    let field = state
                        .get_resolved_field_name_in_plan(plan_id, &name)?
                        .clone();
                    excluded_fields.push(field)
                } else {
                    excluded_names.push(name);
                }
            } else {
                return Err(PlanError::invalid("expecting column name to drop"));
            }
        }
        let expr: Vec<Expr> = schema
            .columns()
            .into_iter()
            .filter(|column| {
                let name = column.name().to_string();
                !excluded_fields.contains(&name)
                    && state
                        .get_field_name(&name)
                        .is_ok_and(|x| !excluded_names.contains(x))
            })
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
        _limit: usize,
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
                let name = state.get_field_name(column.name())?;
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
        _input: spec::QueryPlan,
        _schema: spec::Schema,
        _state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        Err(PlanError::todo("to schema"))
    }

    async fn resolve_query_repartition_by_expression(
        &self,
        input: spec::QueryPlan,
        partition_expressions: Vec<spec::Expr>,
        num_partitions: Option<usize>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self.resolve_query_plan(input, state).await?;
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
        _input: spec::QueryPlan,
        _function: spec::CommonInlineUserDefinedFunction,
        _is_barrier: bool,
        _state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        Err(PlanError::todo("map partitions"))
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
        _map: spec::GroupMap,
        _state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        Err(PlanError::todo("group map"))
    }

    async fn resolve_query_co_group_map(
        &self,
        _map: spec::CoGroupMap,
        _state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        Err(PlanError::todo("co-group map"))
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
            .map(|(i, col)| NamedExpr::new(vec![format!("col{i}")], Expr::Column(col)))
            .collect::<Vec<_>>();
        let expr = self.rewrite_named_expressions(expr, state)?;
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
                .map(|(col, name)| Expr::Column(col.clone()).alias(state.register_field(name)))
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
            let reference =
                self.resolve_table_reference(&spec::ObjectName::new_unqualified(name.clone()))?;
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

    async fn resolve_query_common_inline_udtf(
        &self,
        udtf: spec::CommonInlineUserDefinedTableFunction,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        // TODO: Function arg for if pyspark_udtf or not
        use sail_python_udf::udf::pyspark_udtf::PySparkUDTF;

        let spec::CommonInlineUserDefinedTableFunction {
            function_name,
            deterministic,
            arguments,
            function,
        } = udtf;

        let schema = Arc::new(DFSchema::empty());
        let arguments = self.resolve_expressions(arguments, &schema, state).await?;

        let (return_type, _eval_type, _command, _python_version) = match &function {
            spec::TableFunctionDefinition::PythonUdtf {
                return_type,
                eval_type,
                command,
                python_version,
            } => (return_type, eval_type, command, python_version),
        };

        let return_type: adt::DataType = self.resolve_data_type(return_type.clone())?;
        let return_schema: adt::SchemaRef = match return_type {
            adt::DataType::Struct(ref fields) => {
                Arc::new(adt::Schema::new(fields.clone()))
            },
            _ => {
                return Err(PlanError::invalid(format!(
                    "Invalid Python user-defined table function return type. Expect a struct type, but got {}",
                    return_type
                )))
            }
        };

        let python_udtf: PySparkUDTF = PySparkUDTF::new(
            return_type,
            return_schema,
            function,
            self.config.spark_udf_config.clone(),
            deterministic,
        );

        let table_function = TableFunction::new(function_name.clone(), Arc::new(python_udtf));
        let table_provider = table_function.create_table_provider(&arguments)?;
        let names = state.register_fields(&table_provider.schema());
        let table_provider = RenameTableProvider::try_new(table_provider, names)?;
        Ok(LogicalPlan::TableScan(plan::TableScan::try_new(
            function_name,
            provider_as_source(Arc::new(table_provider)),
            None,
            vec![],
            None,
        )?))
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
            skip: 0,
            // fetch one more row so that the proper message can be displayed if there is more data
            fetch: Some(num_rows + 1),
            input: Arc::new(input),
        });
        let style = match vertical {
            true => ShowStringStyle::Vertical,
            false => ShowStringStyle::Default,
        };
        let format = ShowStringFormat::new("show_string".to_string(), style, truncate);
        let names = state.get_field_names(input.schema().inner())?;
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(ShowStringNode::try_new(
                Arc::new(input),
                names,
                num_rows,
                format,
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
        let format =
            ShowStringFormat::new("html_string".to_string(), ShowStringStyle::Html, truncate);
        let names = state.get_field_names(input.schema().inner())?;
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(ShowStringNode::try_new(
                Arc::new(input),
                names,
                num_rows,
                format,
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
        let fields = state.get_field_names(plan.schema().inner())?;
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
                        let overwrite = mode == SaveMode::Overwrite;
                        LogicalPlanBuilder::insert_into(
                            plan,
                            table_ref,
                            &table_provider.schema(),
                            overwrite,
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
            serde_properties,
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
        if !serde_properties.is_empty() {
            return Err(PlanError::todo(
                "SERDE PROPERTIES in CREATE TABLE statement",
            ));
        }

        let (schema, query_logical_plan) = if let Some(query) = query {
            // FIXME: Query plan has cols renamed to #1, #2, etc. So I think there's more work here.
            let logical_plan = self.resolve_query_plan(*query, state).await?;
            (logical_plan.schema().clone(), Some(logical_plan))
        } else {
            let fields = self.resolve_fields(schema.fields)?;
            let schema = Arc::new(DFSchema::from_unqualified_fields(fields, HashMap::new())?);
            (schema, None)
        };

        let column_defaults: Vec<(String, Expr)> = async {
            let mut results: Vec<(String, Expr)> = Vec::with_capacity(column_defaults.len());
            for column_default in column_defaults {
                let (name, expr) = column_default;
                let expr = self.resolve_expression(expr, &schema, state).await?;
                results.push((name, expr));
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
            let input_format = file_format.input_format;
            let output_format = file_format.output_format;
            if let Some(output_format) = output_format {
                return Err(PlanError::todo(format!("STORED AS INPUTFORMAT: {input_format} OUTPUTFORMAT: {output_format} in CREATE TABLE statement")));
            }
            input_format
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
            None => state.get_field_names(input.schema().inner())?,
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

    // TODO: consolidate duplicated UDF/UDTF code

    fn resolve_catalog_register_function(
        &self,
        function: spec::CommonInlineUserDefinedFunction,
    ) -> PlanResult<LogicalPlan> {
        let spec::CommonInlineUserDefinedFunction {
            function_name,
            deterministic,
            arguments: _,
            function,
        } = function;
        let function_name: &str = function_name.as_str();

        let (output_type, _eval_type, _command, _python_version) = match &function {
            spec::FunctionDefinition::PythonUdf {
                output_type,
                eval_type,
                command,
                python_version,
            } => (output_type, eval_type, command, python_version),
            _ => {
                return Err(PlanError::invalid("UDF function type must be Python UDF"));
            }
        };
        let output_type: adt::DataType = self.resolve_data_type(output_type.clone())?;

        let python_udf = UnresolvedPySparkUDF::new(
            function_name.to_owned(),
            function,
            output_type,
            deterministic,
        );

        let command = CatalogCommand::RegisterFunction {
            udf: ScalarUDF::from(python_udf),
        };
        self.resolve_catalog_command(command)
    }

    fn resolve_catalog_register_table_function(
        &self,
        function: spec::CommonInlineUserDefinedTableFunction,
    ) -> PlanResult<LogicalPlan> {
        let spec::CommonInlineUserDefinedTableFunction {
            function_name,
            deterministic,
            arguments: _,
            function,
        } = function;

        let (return_type, _eval_type, _command, _python_version) = match &function {
            spec::TableFunctionDefinition::PythonUdtf {
                return_type,
                eval_type,
                command,
                python_version,
            } => (return_type, eval_type, command, python_version),
        };

        let return_type: adt::DataType = self.resolve_data_type(return_type.clone())?;
        let return_schema: adt::SchemaRef = match return_type {
            adt::DataType::Struct(ref fields) => {
                Arc::new(adt::Schema::new(fields.clone()))
            },
            _ => {
                return Err(PlanError::invalid(format!(
                    "Invalid Python user-defined table function return type. Expect a struct type, but got {}",
                    return_type
                )))
            }
        };

        let python_udtf: PySparkUDTF = PySparkUDTF::new(
            return_type,
            return_schema,
            function,
            self.config.spark_udf_config.clone(),
            deterministic,
        );

        let command = CatalogCommand::RegisterTableFunction {
            name: function_name,
            udtf: CatalogTableFunction::PySparkUDTF(python_udtf),
        };
        self.resolve_catalog_command(command)
    }

    async fn resolve_command_insert_into(
        &self,
        input: spec::QueryPlan,
        table: spec::ObjectName,
        columns: Vec<spec::Identifier>,
        partition_spec: Vec<spec::Expr>,
        overwrite: bool,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        if !partition_spec.is_empty() {
            return Err(PlanError::todo("partitioned insert"));
        }

        let input = self.resolve_query_plan(input, state).await?;
        let table_reference = self.resolve_table_reference(&table)?;
        let table_provider = self.ctx.table_provider(table_reference.clone()).await?;
        let schema = self
            .resolve_table_schema(&table_reference, &table_provider, &columns)
            .await?;
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
        let plan =
            LogicalPlanBuilder::insert_into(input, table_reference, schema.as_ref(), overwrite)?
                .build()?;
        Ok(plan)
    }

    async fn resolve_command_update(
        &self,
        input: spec::QueryPlan,
        table: spec::ObjectName,
        _table_alias: Option<spec::Identifier>, // We don't need table alias, leaving it here in case we need it in the future.
        assignments: Vec<(spec::ObjectName, spec::Expr)>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        // TODO:
        //  1. Implement `ExecutionPlan` for `WriteOp::Update`.
        //  2. Support UPDATE using Column values.
        let input = self.resolve_query_plan(input, state).await?;
        let table_reference = self.resolve_table_reference(&table)?;
        let table_provider = self.ctx.table_provider(table_reference.clone()).await?;
        let table_schema = self
            .resolve_table_schema(&table_reference, &table_provider, &[])
            .await?;
        let fields = table_schema
            .fields
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>();
        let table_schema = Arc::new(DFSchema::try_from_qualified_schema(
            table_reference.clone(),
            &table_schema,
        )?);

        let mut assignments_map: HashMap<String, Expr> = HashMap::with_capacity(assignments.len());
        for (column, expr) in assignments {
            let expr = self.resolve_expression(expr, &table_schema, state).await?;
            let column_parts: Vec<String> = column.into();
            let column = column_parts
                .last()
                .ok_or_else(|| PlanError::invalid("Expected at least one column in assignment"))?;
            assignments_map.insert(column.into(), expr);
        }

        let exprs: Vec<Expr> = table_schema
            .iter()
            .map(|(_qualifier, field)| {
                let expr = match assignments_map.remove(field.name()) {
                    Some(mut expr) => {
                        if let Expr::Placeholder(placeholder) = &mut expr {
                            placeholder.data_type = placeholder
                                .data_type
                                .take()
                                .or_else(|| Some(field.data_type().clone()));
                        }
                        expr.cast_to(field.data_type(), &input.schema())?
                    }
                    None => Expr::Column(Column::from_name(field.name())),
                };
                Ok(expr.alias(field.name()))
            })
            .collect::<PlanResult<Vec<_>>>()?;

        Ok(LogicalPlan::Dml(DmlStatement::new(
            table_reference,
            table_schema,
            WriteOp::Update,
            Arc::new(project(rename_logical_plan(input, &fields)?, exprs)?),
        )))
    }

    async fn resolve_command_delete(
        &self,
        table: spec::ObjectName,
        condition: Option<spec::Expr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        // TODO:
        //  1. Implement `ExecutionPlan` for `WriteOp::Delete`.
        //  2. Filter condition not working (unable to resolve column name).
        let table_reference = self.resolve_table_reference(&table)?;
        let table_provider = self.ctx.table_provider(table_reference.clone()).await?;
        let table_schema = self
            .resolve_table_schema(&table_reference, &table_provider, &[])
            .await?;
        let table_schema = Arc::new(DFSchema::try_from_qualified_schema(
            table_reference.clone(),
            &table_schema,
        )?);
        let table_source = provider_as_source(table_provider);

        let input =
            LogicalPlanBuilder::scan(table_reference.clone(), table_source, None)?.build()?;
        let input = match condition {
            Some(condition) => {
                let condition = self
                    .resolve_expression(condition, input.schema(), state)
                    .await?;
                LogicalPlan::Filter(plan::Filter::try_new(condition, Arc::new(input))?)
            }
            None => input,
        };

        Ok(LogicalPlan::Dml(DmlStatement::new(
            table_reference,
            table_schema,
            WriteOp::Delete,
            Arc::new(input),
        )))
    }

    async fn resolve_query_fill_na(
        &self,
        input: spec::QueryPlan,
        columns: Vec<spec::Identifier>,
        values: Vec<spec::Expr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        if values.len() > 1 && values.len() != columns.len() {
            return Err(PlanError::invalid(
                "fill na number of values does not match number of columns",
            ));
        }

        let input = self.resolve_query_plan(input, state).await?;
        let schema = input.schema();
        let values = self.resolve_expressions(values, schema, state).await?;
        let columns: Vec<String> = columns.into_iter().map(|x| x.into()).collect();

        let fill_na_exprs = schema
            .columns()
            .into_iter()
            .map(|c| {
                let column_expr = col(c.clone());
                let column_data_type = column_expr.get_type(schema)?;
                let column_name = state.get_field_name(c.name())?;
                // TODO: Avoid checking col == column_name twice
                let expr = if columns.is_empty() || columns.iter().any(|col| col == column_name) {
                    let (value_data_type, value) = if values.len() == 1 {
                        let single_value = values[0].clone();
                        (single_value.get_type(schema)?, single_value)
                    } else {
                        let pos = columns
                            .iter()
                            .position(|col| col == column_name)
                            .ok_or_else(|| {
                                PlanError::invalid("No matching column in specified columns")
                            })?;
                        let value = values
                            .get(pos)
                            .ok_or_else(|| PlanError::invalid("No matching value for column type"))?
                            .clone();
                        let value_data_type = value.get_type(schema)?;
                        (value_data_type, value)
                    };
                    if self.can_cast_fill_na_types(&value_data_type, &column_data_type) {
                        let value = Expr::TryCast(TryCast {
                            expr: Box::new(value),
                            data_type: column_data_type,
                        });
                        when(column_expr.clone().is_null(), value).otherwise(column_expr)?
                    } else {
                        column_expr
                    }
                } else {
                    column_expr
                };
                Ok(NamedExpr::new(vec![column_name.into()], expr))
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
                        .get_field_name(column.name())
                        .is_ok_and(|x| columns.contains(x))
                })
                .map(|c| col(c).is_not_null())
                .collect::<Vec<Expr>>()
        };

        let filter_expr = match min_non_nulls {
            Some(min_non_nulls) => {
                let min_non_nulls = min_non_nulls as u32;
                if min_non_nulls > 0 {
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
                    non_null_count.gt_eq(lit(min_non_nulls))
                } else {
                    conjunction(not_null_exprs)
                        .ok_or_else(|| PlanError::invalid("No columns specified for drop_na."))?
                }
            }
            None => conjunction(not_null_exprs)
                .ok_or_else(|| PlanError::invalid("No columns specified for drop_na."))?,
        };

        Ok(LogicalPlan::Filter(plan::Filter::try_new(
            filter_expr,
            Arc::new(input),
        )?))
    }

    async fn resolve_command_set_variable(
        &self,
        variable: spec::Identifier,
        value: String,
    ) -> PlanResult<LogicalPlan> {
        let statement = plan::Statement::SetVariable(plan::SetVariable {
            variable: variable.into(),
            value,
            schema: DFSchemaRef::new(DFSchema::empty()),
        });

        Ok(LogicalPlan::Statement(statement))
    }

    fn verify_query_plan(&self, plan: &LogicalPlan, state: &PlanResolverState) -> PlanResult<()> {
        let invalid = plan
            .schema()
            .fields()
            .iter()
            .filter_map(|f| {
                if state.get_field_name(f.name()).is_ok() {
                    None
                } else {
                    Some(f.name().to_string())
                }
            })
            .collect::<Vec<_>>();
        if invalid.is_empty() {
            Ok(())
        } else {
            let valid = state.get_fields();
            Err(PlanError::internal(format!(
                "a plan resolver bug has produced invalid fields: {invalid:?}, valid fields: {valid:?}",
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
                let resolved = field.name().clone();
                let name = state.get_field_name(&resolved)?.clone();
                state.register_attribute(plan_id, name, resolved);
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
        let projections = if with_grouping_expressions {
            let mut results = vec![];
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
                let exprs: Vec<_> = exprs
                    .into_iter()
                    .zip(name.into_iter())
                    .map(|(expr, name)| NamedExpr {
                        name: vec![name],
                        expr,
                        metadata: metadata.clone(),
                    })
                    .collect();
                results.extend(exprs);
            }
            results.extend(projections);
            results
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
                Ok(NamedExpr {
                    name,
                    expr: rebase_expression(expr, &aggregate_exprs, &plan)?,
                    metadata,
                })
            })
            .collect::<PlanResult<_>>()?;
        let having = match having {
            Some(having) => Some(rebase_expression(having.clone(), &aggregate_exprs, &plan)?),
            None => None,
        };
        let projections = self.rewrite_named_expressions(projections, state)?;
        let builder = LogicalPlanBuilder::from(plan);
        match having {
            // We must apply the `HAVING` clause as a filter before the projection.
            // It is incorrect to filter after projection due to column renaming.
            Some(having) => Ok(builder.filter(having)?.project(projections)?.build()?),
            None => Ok(builder.project(projections)?.build()?),
        }
    }

    fn rewrite_wildcard(
        &self,
        input: LogicalPlan,
        expr: Vec<NamedExpr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<(LogicalPlan, Vec<NamedExpr>)> {
        let schema = input.schema();
        let mut projected = vec![];
        for e in expr {
            let NamedExpr {
                name,
                expr,
                metadata,
            } = e;
            match expr {
                Expr::Wildcard {
                    qualifier: None,
                    options,
                } => {
                    for e in expand_wildcard(schema, &input, Some(&options))? {
                        projected.push(NamedExpr::try_from_column_expr(e, state)?)
                    }
                }
                Expr::Wildcard {
                    qualifier: Some(qualifier),
                    options,
                } => {
                    for e in expand_qualified_wildcard(&qualifier, schema, Some(&options))? {
                        projected.push(NamedExpr::try_from_column_expr(e, state)?)
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

    fn rewrite_projection<'s, T>(
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

    fn rewrite_named_expressions(
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
                if let Expr::Column(Column {
                    name: column_name, ..
                }) = &expr
                {
                    if state.get_field_name(column_name).ok() == Some(&name) {
                        return Ok(expr);
                    }
                }
                Ok(expr.alias(state.register_field(name)))
            })
            .collect()
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
