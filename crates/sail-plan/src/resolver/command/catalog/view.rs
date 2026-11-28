// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::sync::Arc;

use datafusion_common::TableReference;
use datafusion_expr::{LogicalPlan, SubqueryAlias};
use sail_catalog::command::CatalogCommand;
use sail_catalog::manager::CatalogManager;
use sail_catalog::manager::scope::ViewDefinitionScope;
use sail_catalog::manager::tracker::CatalogLogicalPlanId;
use sail_catalog::provider::{
    CreateTemporaryViewColumnOptions, CreateTemporaryViewOptions, CreateViewColumnOptions,
    CreateViewOptions, DropTemporaryViewOptions, DropViewOptions,
};
use sail_common::spec;
use sail_common_datafusion::catalog::{TemporaryViewDefinition, TemporaryViewSource};
use sail_common_datafusion::datasource::DataSourceRegistry;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::rename::logical_plan::rename_logical_plan;
use sail_common_datafusion::session::config::SessionConfigService;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;
use crate::shared_cte::inline_deterministic_references;
impl PlanResolver<'_> {
    pub(in super::super) async fn resolve_catalog_create_view(
        &self,
        view: spec::ObjectName,
        definition: spec::ViewDefinition,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::ViewDefinition {
            definition,
            input,
            columns,
            if_not_exists,
            replace,
            comment,
            properties,
        } = definition;
        // Resolve the query plan to register fields in state and extract column types.
        let resolved_input = self.resolve_query_plan(*input, state).await?;
        let schema = resolved_input.schema();
        let columns = if let Some(columns) = columns {
            let field_count = schema.fields().len();
            if columns.len() != field_count {
                return Err(PlanError::AnalysisError(format!(
                    "CREATE VIEW column list has {} columns, but the query produces {} columns",
                    columns.len(),
                    field_count
                )));
            }
            columns
                .into_iter()
                .zip(schema.fields().iter())
                .map(|(x, field)| {
                    let spec::ViewColumnDefinition { name, comment } = x;
                    CreateViewColumnOptions {
                        name,
                        data_type: field.data_type().clone(),
                        nullable: field.is_nullable(),
                        comment,
                    }
                })
                .collect()
        } else {
            Self::get_field_names(schema, state)?
                .into_iter()
                .zip(schema.fields().iter())
                .map(|(name, field)| CreateViewColumnOptions {
                    name,
                    data_type: field.data_type().clone(),
                    nullable: field.is_nullable(),
                    comment: None,
                })
                .collect()
        };
        let command = CatalogCommand::CreateView {
            view: view.into(),
            options: CreateViewOptions {
                definition,
                columns,
                comment,
                if_not_exists,
                replace,
                properties,
            },
        };
        self.resolve_catalog_command(command)
    }

    pub(in super::super) async fn resolve_catalog_create_temporary_view(
        &self,
        view: spec::Identifier,
        is_global: bool,
        definition: spec::TemporaryViewDefinition,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::TemporaryViewDefinition {
            input,
            columns,
            if_not_exists,
            replace,
            comment,
            properties,
        } = definition;
        // A view created with `USING` is backed by a data source. We keep the
        // format and options (which carry the `path`) so that `INSERT INTO` the
        // view can write to the same location, like Spark treats it as a table.
        let source = match &input.node {
            spec::QueryNode::Read {
                read_type: spec::ReadType::DataSource(data_source),
                ..
            } => data_source
                .format
                .clone()
                .map(|format| TemporaryViewSource {
                    format,
                    options: data_source.options.clone(),
                }),
            _ => None,
        };
        // The view resolves the query again when its plan no longer fits the tables that it
        // reads, in the scope that records what the query resolves now.
        let query = input.as_ref().clone();
        let manager = self.ctx.extension::<CatalogManager>()?;
        // The query resolves its names, and resolves them again later, in the catalog and the
        // database that are current now, even if the session changes them meanwhile.
        let namespace = manager.current_namespace()?;
        // The query builds the sources of its tables, and builds them again later, with the
        // configuration and the data sources of the session as they are now.
        let mut session = self.ctx.state();
        let config = session.config_mut();
        if let Some(service) = config.get_extension::<SessionConfigService>() {
            config.set_extension(Arc::new(service.snapshot()?));
        }
        let data_sources = self.ctx.extension::<DataSourceRegistry>()?.snapshot()?;
        config.set_extension(Arc::new(data_sources));
        let session = Arc::new(session) as Arc<dyn Any + Send + Sync>;
        let scope = Arc::new(ViewDefinitionScope::create(
            namespace.clone(),
            Arc::clone(&session),
        ));
        state.begin_view_definition();
        let input = Arc::clone(&scope)
            .run(self.resolve_query_plan(*input, state))
            .await;
        let reads = state.end_view_definition();
        let input = input?;
        // The view outlives the query, so its CTEs are not shared with other plans, except
        // that the references to a volatile CTE read one evaluation for each read of the view.
        let input = inline_deterministic_references(input)?;
        let input = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
            Arc::new(input),
            TableReference::Bare {
                table: String::from(view.clone()).into(),
            },
        )?);

        let output_names = Self::get_field_names(input.schema(), state)?;
        let (fields, columns) = match columns {
            Some(columns) => {
                let fields = columns.iter().map(|x| x.name.clone()).collect();
                let columns = columns
                    .into_iter()
                    .map(|x| {
                        let spec::ViewColumnDefinition { name: _, comment } = x;
                        CreateTemporaryViewColumnOptions { comment }
                    })
                    .collect::<Vec<_>>();
                (fields, columns)
            }
            None => (output_names.clone(), vec![]),
        };
        let input = rename_logical_plan(input, &fields)?;
        // The view reads its tables again at each read, so its plan does not keep the
        // table sources that the definition was resolved with.
        let (input, resolves_at_each_read) = Self::store_view_plan(input, &reads)?;
        let (catalog, database) = namespace;
        // A view that reads no table and no persistent view has a plan that always fits, and
        // keeps no query, which can hold the rows of a local relation.
        let definition = if reads.sources().next().is_some() || resolves_at_each_read {
            Some(Arc::new(TemporaryViewDefinition {
                query,
                output_names,
                catalog: catalog.to_string(),
                database: database.into(),
                objects: scope.recorded()?,
                config: Arc::clone(&self.config) as Arc<dyn Any + Send + Sync>,
                catalogs: manager.session_local_catalogs()?,
                session,
                resolves_at_each_read,
            }))
        } else {
            None
        };
        let input: CatalogLogicalPlanId =
            manager.track_logical_plan((Arc::new(input), definition))?;
        let command = CatalogCommand::CreateTemporaryView {
            view: view.into(),
            is_global,
            options: CreateTemporaryViewOptions {
                input,
                columns,
                if_not_exists,
                replace,
                comment,
                properties,
                source,
            },
        };
        self.resolve_catalog_command(command)
    }

    pub(in super::super) async fn resolve_catalog_drop_view(
        &self,
        view: spec::ObjectName,
        if_exists: bool,
    ) -> PlanResult<LogicalPlan> {
        let command = CatalogCommand::DropView {
            view: view.into(),
            options: DropViewOptions { if_exists },
        };
        self.resolve_catalog_command(command)
    }

    pub(in super::super) async fn resolve_catalog_drop_temporary_view(
        &self,
        view: spec::Identifier,
        is_global: bool,
        if_exists: bool,
    ) -> PlanResult<LogicalPlan> {
        let command = CatalogCommand::DropTemporaryView {
            view: view.into(),
            is_global,
            options: DropTemporaryViewOptions { if_exists },
        };
        self.resolve_catalog_command(command)
    }
}
