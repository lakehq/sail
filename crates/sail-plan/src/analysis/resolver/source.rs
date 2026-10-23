use arrow_schema::Schema;
use sail_catalog::error::CatalogError;
use sail_catalog::lakehouse::{
    BeginTableAccessRequest, ResolveLakehouseTableRequest, TableAccessPurpose,
};
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::catalog::{
    CatalogTableConstraint, LakehouseFormat, LakehouseOperation, TableKind,
};
use sail_common_datafusion::datasource::{DataSourceRegistry, OptionLayer, SourceInfo};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::lakerelation::{
    LakeRelationAccess, LakeRelationResolution, LakeRelationTimeTravel,
};

use super::Resolver;
use crate::analysis::deferred::Deferred;
use crate::analysis::ir::*;
use crate::error::{PlanError, PlanResult};

impl Resolver<'_, '_> {
    #[async_recursion::async_recursion]
    pub(super) async fn data_source(
        &mut self,
        source: spec::ReadDataSource,
    ) -> PlanResult<LogicalPlan> {
        let spec::ReadDataSource {
            format,
            schema,
            options,
            paths,
            predicates,
        } = source;
        if !predicates.is_empty() {
            return Err(PlanError::unsupported("data source predicates"));
        }
        let format =
            format.unwrap_or_else(|| self.context.config.default_table_file_format.clone());
        let schema = schema
            .map(|schema| self.types().resolve_schema(schema))
            .transpose()?;
        let registry = self.context.session.extension::<DataSourceRegistry>()?;
        let source = registry
            .get_data_source(&format)?
            .create_source(
                &self.context.session.state(),
                SourceInfo {
                    paths,
                    schema,
                    lakehouse_table: None,
                    constraints: Default::default(),
                    partition_by: vec![],
                    bucket_by: None,
                    sort_order: vec![],
                    options: vec![OptionLayer::OptionList {
                        items: options.into_iter().collect(),
                    }],
                    read_case_sensitive: self.context.config.case_sensitive,
                },
            )
            .await?;
        self.scan("source".into(), source, vec![])
    }

    #[async_recursion::async_recursion]
    pub(super) async fn named_table(
        &mut self,
        table: spec::ReadNamedTable,
    ) -> PlanResult<LogicalPlan> {
        let spec::ReadNamedTable {
            name,
            temporal,
            sample,
            options,
        } = table;
        let registry = self.context.session.extension::<DataSourceRegistry>()?;
        if let [format, path] = name.parts()
            && registry
                .get_data_source(&format.as_ref().to_ascii_lowercase())
                .is_ok()
        {
            let temporal_options = self.time_travel_options(format.as_ref(), temporal).await?;
            let plan = self
                .data_source(spec::ReadDataSource {
                    format: Some(format.as_ref().to_ascii_lowercase()),
                    schema: None,
                    paths: vec![path.as_ref().to_string()],
                    options: options.into_iter().chain(temporal_options).collect(),
                    predicates: vec![],
                })
                .await?;
            return if let Some(sample) = sample {
                self.table_sample(plan, sample).await
            } else {
                Ok(plan)
            };
        }
        let name: Vec<String> = name.into();
        let manager = self.context.session.extension::<CatalogManager>()?;
        let (status, catalog_reference, lake_relation) =
            match manager.get_table_or_view(&name).await {
                Ok(status) => (status, name.clone(), None),
                Err(error @ CatalogError::NotFound(_, _))
                | Err(error @ CatalogError::NotSupported(_)) => {
                    let Some((candidate_name, base_reference)) = name.split_last() else {
                        return Err(error.into());
                    };
                    if base_reference.is_empty() {
                        return Err(error.into());
                    }
                    let base_status = match manager.get_table_or_view(base_reference).await {
                        Ok(status) => status,
                        // A base that names no table, such as `catalog.database` left by
                        // `catalog.database.table`, is not a lake relation either: the
                        // table is what is missing.
                        Err(
                            CatalogError::NotFound(_, _)
                            | CatalogError::NotSupported(_)
                            | CatalogError::InvalidArgument(_),
                        ) => {
                            return Err(error.into());
                        }
                        Err(base_error) => return Err(base_error.into()),
                    };
                    let TableKind::Table { format, .. } = &base_status.kind else {
                        return Err(error.into());
                    };
                    let Some(lake_source) = registry.get_lake_source_if_supported(format)? else {
                        return Err(error.into());
                    };
                    let Some(relation_provider) = lake_source.relation_provider() else {
                        return Err(error.into());
                    };
                    match relation_provider.resolve_relation(candidate_name) {
                        LakeRelationResolution::Unrecognized => return Err(error.into()),
                        LakeRelationResolution::Unsupported { reason } => {
                            return Err(PlanError::unsupported(reason));
                        }
                        LakeRelationResolution::Supported(relation) => (
                            base_status,
                            base_reference.to_vec(),
                            Some((relation_provider, relation)),
                        ),
                    }
                }
                Err(error) => return Err(error.into()),
            };
        let mut plan = match status.kind {
            TableKind::Table {
                columns,
                constraints,
                format,
                location,
                partition_by,
                sort_by,
                bucket_by,
                properties,
                ..
            } => {
                if let Some((_, relation)) = &lake_relation
                    && relation.time_travel() == LakeRelationTimeTravel::Unsupported
                    && temporal.is_some()
                {
                    return Err(PlanError::unsupported(format!(
                        "SQL time travel is not supported for lake relation '{}'",
                        relation.name()
                    )));
                }
                let temporal_options = self.time_travel_options(&format, temporal).await?;
                let schema = Schema::new(columns.iter().map(|c| c.field()).collect::<Vec<_>>());
                let constraints = constraints_for_schema(constraints, &schema)?;
                let resolved = manager
                    .resolve_lakehouse_table(
                        &catalog_reference,
                        ResolveLakehouseTableRequest {
                            catalog_table: catalog_reference.clone(),
                            operation: LakehouseOperation::Read,
                            requested_format: Some(LakehouseFormat::from_format_name(&format)),
                            options: vec![],
                        },
                    )
                    .await?;
                let context = match manager
                    .begin_table_access(
                        &catalog_reference,
                        BeginTableAccessRequest {
                            context: resolved.execution.clone(),
                            purpose: match lake_relation.as_ref().map(|(_, r)| r.access()) {
                                Some(LakeRelationAccess::MetadataRead) => {
                                    TableAccessPurpose::MetadataRead
                                }
                                _ => TableAccessPurpose::DataRead,
                            },
                        },
                    )
                    .await
                {
                    Ok(session) => session.context,
                    Err(CatalogError::NotSupported(_) | CatalogError::UnsupportedCapability(_)) => {
                        resolved.execution
                    }
                    Err(error) => return Err(error.into()),
                };
                let info = SourceInfo {
                    paths: location.into_iter().collect(),
                    lakehouse_table: Some(context),
                    schema: Some(schema),
                    constraints,
                    partition_by: partition_by.into_iter().map(|field| field.column).collect(),
                    bucket_by: bucket_by.map(Into::into),
                    sort_order: sort_by.into_iter().map(Into::into).collect(),
                    options: vec![
                        OptionLayer::TablePropertyList { items: properties },
                        OptionLayer::OptionList { items: options },
                        OptionLayer::OptionList {
                            items: temporal_options,
                        },
                    ],
                    read_case_sensitive: self.context.config.case_sensitive,
                };
                let source = match lake_relation {
                    Some((provider, relation)) => {
                        provider
                            .create_relation(&self.context.session.state(), info, relation)
                            .await?
                    }
                    None => {
                        registry
                            .get_data_source(&format)?
                            .create_source(&self.context.session.state(), info)
                            .await?
                    }
                };
                self.scan(name.join("."), source, name.clone())?
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
                let statement = sail_sql_analyzer::parser::parse_one_statement(&definition)?;
                let spec::Plan::Query(request) =
                    sail_sql_analyzer::statement::from_ast_statement(statement)?
                else {
                    return Err(PlanError::analysis("view definition must be a query"));
                };
                // A persisted view's names bind in its definition, never in a
                // CTE or correlated scope introduced by its reader.
                let ctes = std::mem::take(&mut self.ctes);
                let outer = std::mem::take(&mut self.outer);
                let result = self.query(request).await;
                self.ctes = ctes;
                self.outer = outer;
                let input = self.visible_output(result?)?;
                let input = if columns.is_empty() {
                    input
                } else {
                    self.rename(input, columns.into_iter().map(|c| c.name.into()).collect())?
                };
                LogicalPlan {
                    schema: input.schema.clone(),
                    node: Deferred::Resolved(PlanNode::Scope {
                        input: Box::new(input),
                    }),
                }
            }
            TableKind::TemporaryView { plan, .. } | TableKind::GlobalTemporaryView { plan, .. } => {
                if temporal.is_some() {
                    return Err(PlanError::unsupported(
                        "SQL time travel is not supported for temporary views",
                    ));
                }
                let input = plan.instantiate(&mut self.context.ids)?;
                LogicalPlan {
                    schema: input.schema.clone(),
                    node: Deferred::Resolved(PlanNode::Scope {
                        input: Box::new(input),
                    }),
                }
            }
        };
        let mut schema = plan.schema.into_resolved("table output")?;
        for (position, attribute) in schema.attributes.iter_mut().enumerate() {
            attribute.qualifier = name.clone();
            attribute.qualified_position = position;
        }
        plan.schema = Deferred::Resolved(schema);
        if let Some(sample) = sample {
            self.table_sample(plan, sample).await
        } else {
            Ok(plan)
        }
    }
}

fn constraints_for_schema(
    constraints: Vec<CatalogTableConstraint>,
    schema: &Schema,
) -> PlanResult<datafusion_common::Constraints> {
    let constraints = constraints
        .into_iter()
        .map(|constraint| {
            let (primary, columns) = match constraint {
                CatalogTableConstraint::Unique { columns, .. } => (false, columns),
                CatalogTableConstraint::PrimaryKey { columns, .. } => (true, columns),
            };
            let positions = columns
                .iter()
                .map(|name| {
                    let mut matches = schema
                        .fields()
                        .iter()
                        .enumerate()
                        .filter(|(_, f)| f.name().eq_ignore_ascii_case(name));
                    let Some((index, _)) = matches.next() else {
                        return Err(PlanError::analysis(format!(
                            "constraint column not found: {name}"
                        )));
                    };
                    if matches.next().is_some() {
                        return Err(PlanError::analysis(format!(
                            "ambiguous constraint column: {name}"
                        )));
                    }
                    Ok(index)
                })
                .collect::<PlanResult<_>>()?;
            Ok(if primary {
                datafusion_common::Constraint::PrimaryKey(positions)
            } else {
                datafusion_common::Constraint::Unique(positions)
            })
        })
        .collect::<PlanResult<_>>()?;
    Ok(datafusion_common::Constraints::new_unverified(constraints))
}
