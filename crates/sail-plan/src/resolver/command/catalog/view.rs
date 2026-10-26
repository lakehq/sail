use std::sync::Arc;

use sail_catalog::command::CatalogCommand;
use sail_catalog::provider::{CreateViewColumnOptions, CreateViewOptions};
use sail_common::spec;

use crate::analyzer::model::*;
use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(in crate::resolver) async fn resolve_command_create_temporary_view(
        &mut self,
        view: spec::Identifier,
        is_global: bool,
        definition: spec::TemporaryViewDefinition,
    ) -> PlanResult<CommandKind> {
        use sail_catalog::provider::{
            CreateTemporaryViewColumnOptions, CreateTemporaryViewOptions,
        };
        use sail_common_datafusion::catalog::TemporaryViewSource;
        let source = match &definition.input.node {
            spec::QueryNode::Read {
                read_type: spec::ReadType::DataSource(source),
                ..
            } => source.format.clone().map(|format| TemporaryViewSource {
                format,
                options: source.options.clone(),
            }),
            _ => None,
        };
        let input = self.command_query(*definition.input).await?;
        let mut input = self.visible_output(input)?;
        let columns = if let Some(columns) = definition.columns {
            input = self.rename(
                input,
                columns.iter().map(|c| c.name.clone().into()).collect(),
            )?;
            columns
                .into_iter()
                .map(|c| CreateTemporaryViewColumnOptions { comment: c.comment })
                .collect()
        } else {
            vec![]
        };
        Ok(CommandKind::CreateTemporaryView {
            view: view.into(),
            is_global,
            options: CreateTemporaryViewOptions {
                input: Arc::new(input),
                columns,
                source,
                if_not_exists: definition.if_not_exists,
                replace: definition.replace,
                comment: definition.comment,
                properties: definition.properties,
            },
        })
    }
}

impl PlanResolver<'_> {
    pub(in crate::resolver) async fn resolve_command_create_view(
        &mut self,
        view: spec::ObjectName,
        definition: spec::ViewDefinition,
    ) -> PlanResult<CommandKind> {
        let input = self.command_query(*definition.input).await?;
        let schema = input.schema.resolved("view schema")?;
        if definition
            .columns
            .as_ref()
            .is_some_and(|columns| columns.len() != schema.attributes.len())
        {
            return Err(PlanError::analysis(
                "CREATE VIEW column count does not match query",
            ));
        }
        let mut requested = definition.columns.map(Vec::into_iter);
        let columns = schema
            .attributes
            .iter()
            .map(|attribute| {
                let field = attribute.result_type.resolved("view field")?;
                let column = requested.as_mut().and_then(Iterator::next);
                Ok(CreateViewColumnOptions {
                    name: column
                        .as_ref()
                        .map(|c| c.name.clone())
                        .unwrap_or_else(|| attribute.name.clone()),
                    data_type: field.data_type.clone(),
                    nullable: field.nullable,
                    comment: column.and_then(|c| c.comment),
                })
            })
            .collect::<PlanResult<_>>()?;
        self.resolve_catalog_command(CatalogCommand::CreateView {
            view: view.into(),
            options: CreateViewOptions {
                definition: definition.definition,
                columns,
                comment: definition.comment,
                if_not_exists: definition.if_not_exists,
                replace: definition.replace,
                properties: definition.properties,
            },
        })
    }
}
