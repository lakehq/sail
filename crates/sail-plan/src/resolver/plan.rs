use std::collections::HashMap;
use std::sync::Arc;

use async_recursion::async_recursion;
use datafusion::arrow::datatypes as adt;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::function::TableFunction;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion::datasource::{provider_as_source, MemTable, TableProvider};
use datafusion::execution::context::DataFilePaths;
use datafusion::logical_expr::{logical_plan as plan, Expr, Extension, LogicalPlan, UNNAMED_TABLE};
use datafusion_common::display::{PlanType, StringifiedPlan, ToStringifiedPlan};
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRewriter};
use datafusion_common::{
    Column, DFSchema, DFSchemaRef, ParamValues, ScalarValue, SchemaReference, TableReference,
    ToDFSchema,
};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::expr_rewriter::normalize_col;
use datafusion_expr::utils::{
    columnize_expr, expand_qualified_wildcard, expand_wildcard, expr_as_column_expr,
    find_aggregate_exprs,
};
use datafusion_expr::{build_join_schema, col, LogicalPlanBuilder};
use sail_common::spec;
use sail_common::utils::{cast_record_batch, read_record_batches};

use crate::catalog::CatalogManager;
use crate::error::{PlanError, PlanResult};
use crate::extension::function::multi_expr::MultiExpr;
use crate::extension::logical::{
    CatalogCommand, CatalogCommandNode, RangeNode, ShowStringFormat, ShowStringNode,
    ShowStringStyle, SortWithinPartitionsNode,
};
use crate::extension::source::rename::RenameTableProvider;
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;
use crate::resolver::tree::explode::ExplodeRewriter;
use crate::resolver::tree::window::WindowRewriter;
use crate::resolver::tree::PlanRewriter;
use crate::resolver::PlanResolver;
use crate::utils::ItemTaker;

pub(crate) fn build_schema_reference(name: spec::ObjectName) -> PlanResult<SchemaReference> {
    let names: Vec<String> = name.into();
    match names.as_slice() {
        [a] => Ok(SchemaReference::Bare {
            schema: Arc::from(a.as_str()),
        }),
        [a, b] => Ok(SchemaReference::Full {
            catalog: Arc::from(a.as_str()),
            schema: Arc::from(b.as_str()),
        }),
        _ => Err(PlanError::invalid(format!("schema reference: {:?}", names))),
    }
}

fn build_table_reference(name: spec::ObjectName) -> PlanResult<TableReference> {
    let names: Vec<String> = name.into();
    match names.as_slice() {
        [a] => Ok(TableReference::Bare {
            table: Arc::from(a.as_str()),
        }),
        [a, b] => Ok(TableReference::Partial {
            schema: Arc::from(a.as_str()),
            table: Arc::from(b.as_str()),
        }),
        [a, b, c] => Ok(TableReference::Full {
            catalog: Arc::from(a.as_str()),
            schema: Arc::from(b.as_str()),
            table: Arc::from(c.as_str()),
        }),
        _ => Err(PlanError::invalid(format!("table reference: {:?}", names))),
    }
}

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
            QueryNode::FillNa { .. } => {
                return Err(PlanError::todo("fill na"));
            }
            QueryNode::DropNa { .. } => {
                return Err(PlanError::todo("drop na"));
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
        };
        self.verify_query_plan(&plan, state)?;
        self.register_schema_with_plan_id(&plan, plan_id, state)?;
        Ok(plan)
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
                database: database.map(build_schema_reference).transpose()?,
                table_pattern,
            }),
            CommandNode::ListFunctions {
                database,
                function_pattern,
            } => self.resolve_catalog_command(CatalogCommand::ListFunctions {
                database: database.map(build_schema_reference).transpose()?,
                function_pattern,
            }),
            CommandNode::ListColumns { table } => {
                self.resolve_catalog_command(CatalogCommand::ListColumns {
                    table: build_table_reference(table)?,
                })
            }
            CommandNode::GetDatabase { database } => {
                self.resolve_catalog_command(CatalogCommand::GetDatabase {
                    database: build_schema_reference(database)?,
                })
            }
            CommandNode::GetTable { table } => {
                self.resolve_catalog_command(CatalogCommand::GetTable {
                    table: build_table_reference(table)?,
                })
            }
            CommandNode::GetFunction { function } => {
                self.resolve_catalog_command(CatalogCommand::GetFunction {
                    function: build_table_reference(function)?,
                })
            }
            CommandNode::DatabaseExists { database } => {
                self.resolve_catalog_command(CatalogCommand::DatabaseExists {
                    database: build_schema_reference(database)?,
                })
            }
            CommandNode::TableExists { table } => {
                self.resolve_catalog_command(CatalogCommand::TableExists {
                    table: build_table_reference(table)?,
                })
            }
            CommandNode::FunctionExists { function } => {
                self.resolve_catalog_command(CatalogCommand::FunctionExists {
                    function: build_table_reference(function)?,
                })
            }
            CommandNode::CreateTable { table, definition } => {
                self.resolve_catalog_create_table(table, definition, state)
                    .await
            }
            CommandNode::DropTemporaryView {
                view,
                is_global,
                if_exists,
            } => self.resolve_catalog_command(CatalogCommand::DropTemporaryView {
                view: build_table_reference(view)?,
                is_global,
                if_exists,
            }),
            CommandNode::DropDatabase {
                database,
                if_exists,
                cascade,
            } => self.resolve_catalog_command(CatalogCommand::DropDatabase {
                database: build_schema_reference(database)?,
                if_exists,
                cascade,
            }),
            CommandNode::DropFunction {
                function,
                if_exists,
                is_temporary,
            } => self.resolve_catalog_command(CatalogCommand::DropFunction {
                function: build_table_reference(function)?,
                if_exists,
                is_temporary,
            }),
            CommandNode::DropTable {
                table,
                if_exists,
                purge,
            } => self.resolve_catalog_command(CatalogCommand::DropTable {
                table: build_table_reference(table)?,
                if_exists,
                purge,
            }),
            CommandNode::DropView { view, if_exists } => {
                self.resolve_catalog_command(CatalogCommand::DropView {
                    view: build_table_reference(view)?,
                    if_exists,
                })
            }
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
            CommandNode::RegisterFunction(_) => Err(PlanError::todo("register function")),
            CommandNode::RegisterTableFunction(_) => {
                Err(PlanError::todo("register table function"))
            }
            CommandNode::CreateTemporaryView { view, definition } => {
                self.resolve_catalog_create_temp_view(view, definition, state)
                    .await
            }
            CommandNode::Write { .. } => Err(PlanError::todo("write")),
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
        }
    }

    async fn resolve_query_read_named_table(
        &self,
        table: spec::ReadNamedTable,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::ReadNamedTable { name, options } = table;
        if !options.is_empty() {
            return Err(PlanError::todo("table options"));
        }
        let table_reference = build_table_reference(name)?;
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
        let function_name = build_table_reference(name)?;
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
            schema: _,
            options: _,
            paths,
            predicates: _,
        } = source;
        let urls = paths.to_urls()?;
        if urls.is_empty() {
            return Err(PlanError::invalid("empty data source paths"));
        }
        let (format, extension): (Arc<dyn FileFormat>, _) = match format.as_deref() {
            Some("json") => (Arc::new(JsonFormat::default()), ".json"),
            Some("csv") => (Arc::new(CsvFormat::default()), ".csv"),
            Some("parquet") => (Arc::new(ParquetFormat::new()), ".parquet"),
            other => {
                return Err(PlanError::unsupported(format!(
                    "unsupported data source format: {:?}",
                    other
                )))
            }
        };
        let options = ListingOptions::new(format).with_file_extension(extension);
        // TODO: use provided schema if available
        let schema = options.infer_schema(&self.ctx.state(), &urls[0]).await?;
        let config = ListingTableConfig::new_with_multi_paths(urls)
            .with_listing_options(options)
            .with_schema(schema);
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
            self.rewrite_aggregate(input, expr, vec![], None, state)
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
        let schema = Arc::new(build_join_schema(
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
                schema,
            }));
        }
        // FIXME: resolve using columns
        // TODO: add more validation logic here and in the plan optimizer
        // See `LogicalPlanBuilder` for details about such logic.
        let (on, filter, join_constraint) = if join_condition.is_some() && using_columns.is_empty()
        {
            let condition = match join_condition {
                Some(condition) => Some(self.resolve_expression(condition, &schema, state).await?),
                None => None,
            };
            (vec![], condition, plan::JoinConstraint::On)
        } else if join_condition.is_none() && !using_columns.is_empty() {
            let on = using_columns
                .into_iter()
                .map(|name| {
                    let column = Expr::Column(Column::new_unqualified(name));
                    (column.clone(), column)
                })
                .collect();
            (on, None, plan::JoinConstraint::Using)
        } else {
            return Err(PlanError::invalid(
                "expecting either join condition or using columns",
            ));
        };
        Ok(LogicalPlan::Join(plan::Join {
            left: Arc::new(left),
            right: Arc::new(right),
            on,
            filter,
            join_type,
            join_constraint,
            schema,
            null_equals_null: false,
        }))
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
            by_name: _,
            allow_missing_columns: _,
        } = op;
        // TODO: support set operation by name
        let left = self.resolve_query_plan(*left, state).await?;
        let right = self.resolve_query_plan(*right, state).await?;
        match set_op_type {
            SetOpType::Intersect => Ok(LogicalPlanBuilder::intersect(left, right, is_all)?),
            SetOpType::Union => {
                if is_all {
                    Ok(LogicalPlanBuilder::from(left).union(right)?.build()?)
                } else {
                    Ok(LogicalPlanBuilder::from(left)
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
        let input = self.resolve_query_plan(input, state).await?;
        let schema = input.schema();
        let expr = self.resolve_sort_orders(order, schema, state).await?;
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
        } = aggregate;
        let input = self.resolve_query_plan(*input, state).await?;
        let schema = input.schema();
        let grouping = self.resolve_expressions(grouping, schema, state).await?;
        let projections = self
            .resolve_named_expressions(projections, schema, state)
            .await?;
        let having = match having {
            Some(having) => Some(self.resolve_expression(having, schema, state).await?),
            None => None,
        };
        self.rewrite_aggregate(input, projections, grouping, having, state)
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
            build_table_reference(spec::ObjectName::new_qualified(alias, qualifier))?,
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
        let mut aliases: HashMap<String, (Expr, bool)> = async {
            let mut results: HashMap<String, (Expr, bool)> = HashMap::new();
            for alias in aliases {
                let (name, expr) = match alias {
                    // TODO: handle alias metadata
                    spec::Expr::Alias {
                        name,
                        expr,
                        metadata: _,
                    } => {
                        let name = name
                            .one()
                            .map_err(|_| PlanError::invalid("multi-alias for column"))?;
                        (name, *expr)
                    }
                    _ => return Err(PlanError::invalid("alias expression expected for column")),
                };
                let expr = self.resolve_expression(expr, schema, state).await?;
                results.insert(name.into(), (expr, false));
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
                    Some((e, exists)) => {
                        *exists = true;
                        Ok(NamedExpr::new(vec![name.to_string()], e.clone()))
                    }
                    None => Ok(NamedExpr::new(vec![name.to_string()], Expr::Column(column))),
                }
            })
            .collect::<PlanResult<Vec<_>>>()?;
        for (name, (e, exists)) in &aliases {
            if !exists {
                expr.push(NamedExpr::new(vec![name.clone()], e.clone()));
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
            table_partition_cols,
            file_sort_order,
            if_not_exists,
            or_replace,
            unbounded,
            options,
            query: _, // TODO: handle query
            definition,
        } = definition;
        // TODO: handle query
        //  1. Resolve query to get schema
        //  2. (optional) if columns are specified in the definition, validate the schema
        //  3. create external table
        //  4. fill external table from query table (copy to)
        let fields = self.resolve_fields(schema.fields)?;
        let schema = Arc::new(DFSchema::from_unqualified_fields(fields, HashMap::new())?);
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
            self.config.default_warehouse_directory.clone()
        };
        let file_format = if let Some(file_format) = file_format {
            file_format
        } else if unbounded {
            self.config.default_unbounded_table_file_format.clone()
        } else {
            self.config.default_bounded_table_file_format.clone()
        };
        let table_partition_cols: Vec<String> =
            table_partition_cols.into_iter().map(String::from).collect();
        let file_sort_order: Vec<Vec<Expr>> = async {
            let mut results: Vec<Vec<Expr>> = Vec::with_capacity(file_sort_order.len());
            for order in file_sort_order {
                let order = self.resolve_expressions(order, &schema, state).await?;
                results.push(order);
            }
            Ok(results) as PlanResult<_>
        }
        .await?;
        let options: Vec<(String, String)> = options
            .into_iter()
            .map(|(k, v)| Ok((k, v)))
            .collect::<PlanResult<Vec<(String, String)>>>()?;
        let command = CatalogCommand::CreateTable {
            table: build_table_reference(table)?,
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
            database: build_schema_reference(database)?,
            if_not_exists,
            comment,
            location,
            properties,
        };
        self.resolve_catalog_command(command)
    }

    async fn resolve_catalog_create_temp_view(
        &self,
        view: spec::ObjectName,
        view_definition: spec::TemporaryViewDefinition,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::TemporaryViewDefinition {
            input,
            columns,
            is_global,
            replace,
            definition,
        } = view_definition;
        let columns: Vec<String> = columns.into_iter().map(String::from).collect();
        let input = self.resolve_query_plan(*input, state).await?;
        let input = if !columns.is_empty() {
            // Not sure if we need to do this but this is what datafusion does
            let fields = input.schema().fields().clone();
            if columns.len() != fields.len() {
                return Err(PlanError::invalid(format!(
                    "Source table contains {} columns but only {} names given as column alias",
                    fields.len(),
                    columns.len()
                )));
            }
            LogicalPlanBuilder::from(input)
                .project(
                    fields
                        .iter()
                        .zip(columns.into_iter())
                        .map(|(field, column)| col(field.name()).alias(column)),
                )?
                .build()?
        } else {
            input
        };
        let command = CatalogCommand::CreateTemporaryView {
            input: Arc::new(input),
            view: build_table_reference(view)?,
            is_global,
            replace,
            definition,
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
        let table_reference = build_table_reference(table)?;
        let manager = CatalogManager::new(self.ctx, self.config.clone());
        let table_schema = manager
            .get_table_schema(table_reference.clone())
            .await?
            .ok_or_else(|| PlanError::invalid(format!("Table {} not found", table_reference)))?;
        let columns: Vec<String> = columns.into_iter().map(String::from).collect();
        let arrow_schema = if columns.is_empty() {
            &table_schema
        } else {
            let fields = columns
                .into_iter()
                .map(|c| {
                    let df_schema = DFSchema::try_from_qualified_schema(
                        table_reference.clone(),
                        table_schema.as_ref(),
                    )?;
                    let column_index = df_schema
                        .index_of_column_by_name(None, &c)
                        .ok_or_else(|| PlanError::invalid(format!("Column {} not found", c)))?;
                    Ok(table_schema.field(column_index).clone())
                })
                .collect::<PlanResult<Vec<_>>>()?;
            &adt::Schema::new(adt::Fields::from(fields))
        };
        let plan =
            LogicalPlanBuilder::insert_into(input, table_reference, arrow_schema, overwrite)?
                .build()?;
        Ok(plan)
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
            Err(PlanError::internal(format!(
                "a plan resolver bug has produced invalid fields: {:?}",
                invalid,
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

    fn rewrite_aggregate(
        &self,
        input: LogicalPlan,
        projections: Vec<NamedExpr>,
        grouping: Vec<Expr>,
        having: Option<Expr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let mut aggregate_candidates = projections
            .iter()
            .map(|x| x.expr.clone())
            .collect::<Vec<_>>();
        if let Some(having) = having.as_ref() {
            aggregate_candidates.push(having.clone());
        }
        let aggregate = find_aggregate_exprs(&aggregate_candidates);
        let plan = LogicalPlanBuilder::from(input)
            .aggregate(grouping, aggregate.clone())?
            .build()?;
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
                    expr: rebase_expression(expr, &aggregate, &plan)?,
                    metadata,
                })
            })
            .collect::<PlanResult<_>>()?;
        let having = match having {
            Some(having) => Some(rebase_expression(having.clone(), &aggregate, &plan)?),
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
                Expr::Wildcard { qualifier: None } => {
                    for e in expand_wildcard(schema, &input, None)? {
                        projected.push(NamedExpr::try_from_column_expr(e, state)?)
                    }
                }
                Expr::Wildcard {
                    qualifier: Some(qualifier),
                } => {
                    for e in expand_qualified_wildcard(&qualifier, schema, None)? {
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
                let name = name
                    .one()
                    .map_err(|_| PlanError::invalid("one name expected for expression"))?;
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
