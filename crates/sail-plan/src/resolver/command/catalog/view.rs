use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion_common::TableReference;
use datafusion_expr::{LogicalPlan, SubqueryAlias};
use sail_catalog::command::CatalogCommand;
use sail_catalog::provider::{
    CreateTemporaryViewColumnOptions, CreateTemporaryViewOptions, CreateViewColumnOptions,
    CreateViewOptions, DropTemporaryViewOptions, DropViewOptions,
};
use sail_common::spec;
use sail_common_datafusion::rename::logical_plan::rename_logical_plan;

use crate::error::PlanResult;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(in super::super) async fn resolve_catalog_create_view(
        &self,
        view: spec::ObjectName,
        definition: spec::ViewDefinition,
        _state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::ViewDefinition {
            definition,
            columns,
            if_not_exists,
            replace,
            comment,
            properties,
        } = definition;
        let columns = columns
            .into_iter()
            .flatten()
            .map(|x| {
                let spec::ViewColumnDefinition { name, comment } = x;
                // TODO: get the correct data type from the SQL query
                CreateViewColumnOptions {
                    name,
                    data_type: DataType::Null,
                    nullable: true,
                    comment,
                }
            })
            .collect();
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
        let command = CatalogCommand::CreateTemporaryView {
            view: view.into(),
            is_global,
            options: CreateTemporaryViewOptions {
                input: Arc::new(input),
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
