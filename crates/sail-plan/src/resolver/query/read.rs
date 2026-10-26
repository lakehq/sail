use std::sync::Arc;

use arrow_schema::Schema;
use datafusion::catalog::MemTable;
use datafusion::datasource::provider_as_source;
use datafusion_expr::TableSource;
use sail_catalog::error::CatalogError;
use sail_catalog::lakehouse::{
    BeginTableAccessRequest, ResolveLakehouseTableRequest, TableAccessPurpose,
};
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::array::record_batch::{
    cast_record_batch_positionally, read_record_batches,
};
use sail_common_datafusion::catalog::{
    CatalogTableConstraint, LakehouseFormat, LakehouseOperation, TableKind,
};
use sail_common_datafusion::datasource::{DataSourceRegistry, OptionLayer, SourceInfo};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::lakerelation::{
    LakeRelationAccess, LakeRelationResolution, LakeRelationTimeTravel,
};

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn resolve_query_read_data_source(
        &mut self,
        source: spec::ReadDataSource,
    ) -> PlanResult<Query> {
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
    pub(in crate::resolver) async fn resolve_query_read_named_table(
        &mut self,
        table: spec::ReadNamedTable,
    ) -> PlanResult<Query> {
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
                .resolve_query_read_data_source(spec::ReadDataSource {
                    format: Some(format.as_ref().to_ascii_lowercase()),
                    schema: None,
                    paths: vec![path.as_ref().to_string()],
                    options: options.into_iter().chain(temporal_options).collect(),
                    predicates: vec![],
                })
                .await?;
            return if let Some(sample) = sample {
                self.resolve_query_table_sample(plan, sample).await
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
                let ctes = std::mem::take(&mut self.state.scope.ctes);
                let outer = std::mem::take(&mut self.outer);
                let result = self.resolve_query_plan(request).await;
                let scope = Arc::new(sail_common_plan::scope::PlanScope {
                    isolated: true,
                    ..self.state.scope.clone()
                });
                self.state.scope.ctes = ctes;
                self.outer = outer;
                let input = self.visible_output(result?)?;
                let input = if columns.is_empty() {
                    input
                } else {
                    self.rename(input, columns.into_iter().map(|c| c.name.into()).collect())?
                };
                Query {
                    schema: input.schema.clone(),
                    kind: Deferred::Resolved(QueryKind::Scope {
                        scope: Some(scope),
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
                let input = plan.instantiate(&mut self.state.ids)?;
                Query {
                    schema: input.schema.clone(),
                    kind: Deferred::Resolved(QueryKind::Scope {
                        scope: Some(Arc::new(sail_common_plan::scope::PlanScope {
                            isolated: true,
                            ..Default::default()
                        })),
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
            self.resolve_query_table_sample(plan, sample).await
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

impl PlanResolver<'_> {
    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn query_source(
        &mut self,
        node: spec::QueryNode,
    ) -> PlanResult<Query> {
        Ok(match node {
            spec::QueryNode::Empty { produce_one_row } => Self::empty(produce_one_row),
            spec::QueryNode::Values(rows) => self.resolve_query_values(rows).await?,
            spec::QueryNode::Range(range) => crate::function::table::range::resolve_range(
                &mut self.context.function_context(&mut self.state.ids),
                range,
            )?,
            spec::QueryNode::LocalRelation { data, schema } => {
                let batches = data
                    .map(|data| read_record_batches(&data))
                    .transpose()?
                    .unwrap_or_default();
                let (schema, batches) = if let Some(schema) = schema {
                    let schema = Arc::new(self.types().resolve_schema(schema)?);
                    let batches = batches
                        .into_iter()
                        .map(|batch| cast_record_batch_positionally(batch, Arc::clone(&schema)))
                        .collect::<Result<Vec<_>, _>>()?;
                    (schema, batches)
                } else if let Some(batch) = batches.first() {
                    (batch.schema(), batches)
                } else {
                    return Err(PlanError::analysis("missing schema for local relation"));
                };
                let provider = Arc::new(MemTable::try_new(schema, vec![batches])?);
                self.scan(
                    "local_relation".into(),
                    provider_as_source(provider),
                    vec![],
                )?
            }
            spec::QueryNode::Read {
                read_type: spec::ReadType::DataSource(source),
                is_streaming: _,
            } => self.resolve_query_read_data_source(*source).await?,
            spec::QueryNode::Read {
                read_type: spec::ReadType::NamedTable(table),
                is_streaming: _,
            } => {
                let name: Vec<String> = table.name.clone().into();
                if let Some(request) = self
                    .state
                    .scope
                    .ctes
                    .get(&self.name_key(&name.join(".")))
                    .cloned()
                {
                    if table.temporal.is_some() || !table.options.is_empty() {
                        return Err(PlanError::analysis(
                            "CTE references do not accept table options",
                        ));
                    }
                    let origins = if request.parameter {
                        &mut self.state.parameter_origins
                    } else {
                        &mut self.state.cte_origins
                    };
                    let output = request.plan.schema.resolved("CTE definition")?;
                    let renew = output
                        .attributes
                        .iter()
                        .any(|a| origins.contains(&a.origin));
                    if !renew {
                        origins.extend(output.attributes.iter().map(|a| a.origin));
                    }
                    let mut plan = request.plan.instantiate_reference(
                        &mut self.state.ids,
                        renew,
                        request.parameter || !renew,
                    )?;
                    let mut schema = plan.schema.into_resolved("CTE schema")?;
                    for (position, attribute) in schema.attributes.iter_mut().enumerate() {
                        attribute.qualifier = name.clone();
                        attribute.qualified_position = position;
                    }
                    plan.schema = Deferred::Resolved(schema);
                    let plan = Query {
                        schema: plan.schema.clone(),
                        kind: Deferred::Resolved(QueryKind::Scope {
                            scope: None,
                            input: Box::new(plan),
                        }),
                    };
                    if let Some(sample) = table.sample {
                        self.resolve_query_table_sample(plan, sample).await?
                    } else {
                        plan
                    }
                } else {
                    self.resolve_query_read_named_table(*table).await?
                }
            }
            spec::QueryNode::Read {
                read_type: spec::ReadType::DynamicTable(table),
                is_streaming: _,
            } => {
                let name = self.identifier(table.name).await?;
                self.resolve_query_plan(spec::QueryPlan {
                    plan_id: None,
                    node: spec::QueryNode::Read {
                        read_type: spec::ReadType::NamedTable(Box::new(spec::ReadNamedTable {
                            name,
                            temporal: None,
                            sample: table.sample,
                            options: table.options,
                        })),
                        is_streaming: false,
                    },
                })
                .await?
            }
            spec::QueryNode::Read {
                read_type: spec::ReadType::Udtf(function),
                is_streaming: _,
            } => {
                if !function.options.is_empty() {
                    return Err(PlanError::todo("ReadType::UDTF options"));
                }
                let name = Vec::<String>::from(function.name.clone())
                    .join(".")
                    .to_ascii_lowercase();
                match (
                    crate::function::get_built_in_table_function(&name),
                    self.registered_function(&name)?,
                ) {
                    (Some(builtin), None) => {
                        if !function.named_arguments.is_empty() {
                            return Err(PlanError::invalid(format!(
                                "{name} does not accept named arguments"
                            )));
                        }
                        let mut arguments = vec![];
                        for argument in function.arguments {
                            arguments.push(
                                self.resolve_named_expression(argument, &QuerySchema::default())
                                    .await?
                                    .expression,
                            );
                        }
                        builtin(crate::function::table::TableFunctionInput {
                            arguments,
                            context: self.context.function_context(&mut self.state.ids),
                        })
                    }
                    _ => {
                        let request = spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                            function_name: function.name,
                            arguments: function.arguments,
                            named_arguments: function.named_arguments,
                            is_distinct: false,
                            is_user_defined_function: false,
                            is_internal: None,
                            ignore_nulls: None,
                            filter: None,
                            order_by: None,
                        });
                        let (input, generated) = self.generator(Self::empty(true), request).await?;
                        self.project(input, generated)
                    }
                }?
            }
            spec::QueryNode::CachedRemoteRelation { relation_id } => {
                use sail_common_datafusion::extension::SessionExtensionAccessor;
                let registry =
                    self.context
                        .session
                        .extension::<sail_cache::remote_checkpoint::RemoteCheckpointRegistry>()?;
                let descriptor = registry.get(&relation_id)?.ok_or_else(|| {
                    PlanError::analysis(format!(
                        "remote checkpoint relation does not exist: {relation_id}"
                    ))
                })?;
                let attributes = descriptor
                    .logical_schema
                    .fields()
                    .iter()
                    .map(|f| {
                        self.context
                            .function_context(&mut self.state.ids)
                            .attribute(f.name().clone(), ExprType::from_field(f))
                    })
                    .collect();
                Query {
                    schema: Deferred::Resolved(QuerySchema { attributes }),
                    kind: Deferred::Resolved(QueryKind::RemoteCheckpoint { relation_id }),
                }
            }
            node => {
                return Err(PlanError::unsupported(format!(
                    "query resolution: {node:?}"
                )));
            }
        })
    }
}

impl PlanResolver<'_> {
    pub(in crate::resolver) fn scan(
        &mut self,
        name: String,
        source: Arc<dyn TableSource>,
        qualifier: Vec<String>,
    ) -> PlanResult<Query> {
        let attributes = source
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(position, field)| {
                let mut attribute = self
                    .context
                    .function_context(&mut self.state.ids)
                    .attribute(field.name().clone(), ExprType::from_field(field));
                attribute.qualifier.clone_from(&qualifier);
                attribute.qualified_position = position;
                attribute
            })
            .collect();
        Ok(Query {
            kind: Deferred::Resolved(QueryKind::Scan {
                name,
                source,
                projection: None,
            }),
            schema: Deferred::Resolved(QuerySchema { attributes }),
        })
    }
}
