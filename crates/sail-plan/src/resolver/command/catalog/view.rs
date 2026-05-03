use std::sync::Arc;

use datafusion_common::TableReference;
use datafusion_expr::{LogicalPlan, SubqueryAlias};
use sail_catalog::command::CatalogCommand;
use sail_catalog::manager::tracker::CatalogLogicalPlanId;
use sail_catalog::manager::CatalogManager;
use sail_catalog::provider::{
    CreateTemporaryViewColumnOptions, CreateTemporaryViewOptions, CreateViewColumnOptions,
    CreateViewOptions, DropTemporaryViewOptions, DropViewOptions,
};
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::rename::logical_plan::rename_logical_plan;

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

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
        let input = self.resolve_query_plan(*input, state).await?;
        let input = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
            Arc::new(input),
            TableReference::Bare {
                table: String::from(view.clone()).into(),
            },
        )?);

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
            None => (Self::get_field_names(input.schema(), state)?, vec![]),
        };
        let input = rename_logical_plan(input, &fields)?;
        let manager = self.ctx.extension::<CatalogManager>()?;
        let input: CatalogLogicalPlanId = manager.track_logical_plan(Arc::new(input))?;
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
