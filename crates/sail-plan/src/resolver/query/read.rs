// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;
use std::sync::Arc;

use async_recursion::async_recursion;
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::catalog::TableFunctionArgs;
use datafusion::datasource::{TableProvider, provider_as_source, source_as_provider};
use datafusion::execution::SessionState;
use datafusion_common::{DFSchema, ScalarValue, TableReference};
use datafusion_expr::{
    Expr, LogicalPlan, SubqueryAlias, TableScanBuilder, TableSource, UNNAMED_TABLE,
};
use rand::{RngExt, rng};
use sail_catalog::error::CatalogError;
use sail_catalog::lakehouse::{
    LakehouseResolvedTable, ResolveLakehouseTableRequest, TableAccessPurpose,
};
use sail_catalog::manager::CatalogManager;
use sail_catalog::manager::scope::ViewDefinitionScope;
use sail_catalog::manager::table::SessionCatalogTable;
use sail_common::spec;
use sail_common_datafusion::catalog::{
    LakehouseOperation, TableColumnStatus, TableKind, TableStatus,
};
use sail_common_datafusion::datasource::{DataSourceRegistry, OptionLayer, SourceInfo};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::lakerelation::{
    LakeRelation, LakeRelationAccess, LakeRelationProvider, LakeRelationResolution,
    LakeRelationTimeTravel,
};
use sail_common_datafusion::literal::LiteralEvaluator;
use sail_common_datafusion::rename::logical_plan::{
    rename_logical_plan, rename_logical_plan_reusing_projection,
};
use sail_common_datafusion::rename::table_provider::RenameTableProvider;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_data_source::reuse::is_reusable_source;
use sail_python_udf::udf::pyspark_unresolved_udf::PySparkUnresolvedUDF;
use url::Url;

use super::sample::SAMPLE_ROUNDING_EPSILON;
use super::temp_view::{
    CatalogTableOrigin, LakePathOrigin, TableOrigin, ViewSourceSettings, has_time_travel_option,
    view_definition_source,
};
use crate::error::{PlanError, PlanResult};
use crate::function::{get_built_in_table_function, is_built_in_generator_function};
use crate::resolver::PlanResolver;
use crate::resolver::function::PythonUdtf;
use crate::resolver::state::{
    CatalogTableLookup, LakePathSourceKey, PlanResolverState, TableSourceKey, WrittenLookupKey,
};

impl PlanResolver<'_> {
    // Box the read future so every level of a nested query can poll the resolver
    // without reserving stack space for catalog lookup and partition planning.
    #[async_recursion]
    pub(super) async fn resolve_query_read(
        &self,
        read_type: spec::ReadType,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        match read_type {
            spec::ReadType::NamedTable(table) => {
                self.resolve_query_read_named_table(*table, state).await
            }
            spec::ReadType::Udtf(udtf) => self.resolve_query_read_udtf(*udtf, state).await,
            spec::ReadType::DataSource(source) => {
                self.resolve_query_read_data_source(*source, state).await
            }
            spec::ReadType::DynamicTable(table) => {
                self.resolve_query_read_dynamic_table(*table, state).await
            }
        }
    }

    /// Resolves a named table or view reference into a logical plan node.
    /// Looks up the name in the catalog and produces the appropriate plan
    /// depending on whether it's a table, view, or temporary view.
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

        // Check if the name is in the form `<format>.<path>` where `<format>` is a
        // registered data source. In that case, treat it as a direct data source read.
        if let [format, path] = name.parts() {
            let format = format.as_ref().to_ascii_lowercase();
            let registry = self.data_source_registry()?;
            if registry.get_data_source(&format).is_ok() {
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
                let plan = self
                    .resolve_query_read_data_source_by_path(source, state)
                    .await?;
                return if let Some(table_sample) = sample {
                    self.apply_table_sample(plan, table_sample, state).await
                } else {
                    Ok(plan)
                };
            }
        }

        let table_reference = self.resolve_table_reference(&name);
        if let Ok(table_reference) = &table_reference
            && let Some(cte) = state.get_cte(table_reference)
        {
            if temporal.is_some() {
                return Err(PlanError::unsupported(
                    "SQL time travel is not supported for CTEs",
                ));
            }
            let mut plan = cte.plan.as_ref().clone();
            if let Some(names) = cte.renew_reference(state)? {
                plan = match cte.scan(false, state)? {
                    // The scan is renamed below an alias, as the definition is.
                    Some(scan) => LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
                        Arc::new(rename_logical_plan_reusing_projection(scan, &names)?),
                        table_reference.clone(),
                    )?),
                    None => rename_logical_plan_reusing_projection(plan, &names)?,
                };
                state.register_missing_input_boundary(&plan);
            } else if let Some(scan) = cte.scan(true, state)? {
                // The scan keeps the names of the definition.
                plan = scan;
            }
            return if let Some(table_sample) = sample {
                self.apply_table_sample(plan, table_sample, state).await
            } else {
                Ok(plan)
            };
        }

        self.resolve_query_read_catalog_table(
            spec::ReadNamedTable {
                name,
                temporal,
                sample,
                options,
            },
            None,
            state,
        )
        .await
    }

    /// The request of every catalog table lookup in a query. Reads with different options,
    /// time travel, or required formats share a lookup, so the request has none of these.
    /// Writes share the lookup as well, and set their own operation on the execution
    /// context of the lookup, which is all that the operation of a request decides.
    /// The request names the table by its fully qualified name, which the execution
    /// context carries, so that a commit looks up the same table whichever reference
    /// of the query came first and whatever the current database is by then.
    pub(super) fn catalog_table_lookup_request(table: &[String]) -> ResolveLakehouseTableRequest {
        ResolveLakehouseTableRequest {
            catalog_table: table.to_vec(),
            operation: LakehouseOperation::Read,
            requested_format: None,
            options: vec![],
        }
    }

    /// Looks up a temporary view, a catalog table, or a catalog view, reusing earlier
    /// successful lookups in the query. The catalog is asked once for each table, however
    /// the name of the table is written, so that all the references to the table in the
    /// query read the same table metadata. The targets of writes use the same lookups, so
    /// that a statement writes to the table metadata that it reads.
    pub(in crate::resolver) async fn lookup_catalog_table_or_view(
        manager: &CatalogManager,
        reference: &[String],
        state: &mut PlanResolverState,
    ) -> Result<CatalogTableLookup, CatalogError> {
        let key = WrittenLookupKey::new(reference, false);
        if let Some(lookup) = state.get_written_lookup(&key) {
            return Ok(lookup);
        }
        // A temporary view can have the name of a catalog table, so it is looked up by
        // the name as written, before that name is qualified.
        let lookup = match manager.find_temporary_view(reference).await? {
            Some(status) => (status, None),
            None => {
                let table = manager.resolve_fully_qualified_object_name(reference)?;
                Self::lookup_catalog_object(manager, table, state).await?
            }
        };
        state.insert_written_lookup(key, lookup.clone());
        Ok(lookup)
    }

    /// Returns the table of the fully qualified name `table` in the catalog instance of
    /// another session, for a table that the query of a global temporary view reads in a
    /// catalog of the session that created the view.
    pub(super) fn other_session_catalog_table(
        manager: &CatalogManager,
        table: &[String],
    ) -> Result<Option<SessionCatalogTable>, CatalogError> {
        match manager.session_catalog_table(table)? {
            Some(table) if !manager.has_session_catalog_table(&table)? => Ok(Some(table)),
            _ => Ok(None),
        }
    }

    /// Looks up the catalog table or the catalog view of the fully qualified name `table`,
    /// reusing an earlier successful lookup in the query.
    pub(super) async fn lookup_catalog_object(
        manager: &CatalogManager,
        table: Vec<String>,
        state: &mut PlanResolverState,
    ) -> Result<CatalogTableLookup, CatalogError> {
        let session_catalog =
            Self::other_session_catalog_table(manager, &table)?.map(|x| x.catalog_id());
        if let Some(lookup) = state.get_catalog_table_lookup(session_catalog, &table) {
            return Ok(lookup);
        }
        // The table is loaded by the qualified name that the lookup is stored under, so
        // that the stored lookup is always the one of that table.
        let lookup = manager
            .get_catalog_lakehouse_table_or_view(&table, Self::catalog_table_lookup_request(&table))
            .await?;
        match lookup {
            (status, Some(resolved)) => {
                let (status, resolved) =
                    state.share_catalog_table(session_catalog, table, status, resolved);
                Ok((status, Some(resolved)))
            }
            view => {
                state.insert_catalog_table_lookup(session_catalog, table, view.clone());
                Ok(view)
            }
        }
    }

    /// Looks up a catalog table like [`Self::lookup_catalog_table_or_view`], but without
    /// temporary views and catalog views. Data-source identifier reads, the table targets of
    /// writes (`INSERT` and DataFrame writes, see `resolve_table_info`), and the check of the
    /// generated columns of a Delta Lake table that is replaced use this lookup.
    pub(in crate::resolver) async fn lookup_catalog_table(
        manager: &CatalogManager,
        reference: &[String],
        state: &mut PlanResolverState,
    ) -> Result<(TableStatus, LakehouseResolvedTable), CatalogError> {
        let key = WrittenLookupKey::new(reference, true);
        if let Some((status, Some(resolved))) = state.get_written_lookup(&key) {
            return Ok((status, resolved));
        }
        let table = manager.resolve_fully_qualified_object_name(reference)?;
        let session_catalog =
            Self::other_session_catalog_table(manager, &table)?.map(|x| x.catalog_id());
        // An earlier lookup that found a view is not reused: the catalog is asked for
        // the table, and reports that there is none.
        let (status, resolved) = match state.get_catalog_table_lookup(session_catalog, &table) {
            Some((status, Some(resolved))) => (status, resolved),
            _ => {
                let (status, resolved) = manager
                    .get_lakehouse_table(&table, Self::catalog_table_lookup_request(&table))
                    .await?;
                state.share_catalog_table(session_catalog, table, status, resolved)
            }
        };
        state.insert_written_lookup(key, (status.clone(), Some(resolved.clone())));
        Ok((status, resolved))
    }

    /// Returns the settings of the session that created a temporary view, as they were when
    /// the creation began, while the view is created or resolves its query again.
    pub(super) fn view_settings() -> Option<Arc<ViewSourceSettings>> {
        ViewDefinitionScope::pinned_settings().and_then(|x| x.downcast::<ViewSourceSettings>().ok())
    }

    /// Returns the data sources of the settings, or of this session.
    pub(super) fn session_data_sources(
        &self,
        settings: Option<&ViewSourceSettings>,
    ) -> PlanResult<Arc<DataSourceRegistry>> {
        match settings {
            Some(settings) => Ok(Arc::clone(&settings.data_sources)),
            None => Ok(self.ctx.extension::<DataSourceRegistry>()?),
        }
    }

    /// Returns the data sources that formats are resolved by: those of the session that
    /// created a temporary view, as they were then, while the view is created or resolves its
    /// query again, or else those of this session.
    pub(super) fn data_source_registry(&self) -> PlanResult<Arc<DataSourceRegistry>> {
        self.session_data_sources(Self::view_settings().as_deref())
    }

    /// Returns the settings that build the sources of a read that a temporary view stored with
    /// `settings`, those of the session that created the view as they were then. A view builds
    /// the sources of its tables with the configuration and the data sources that its session
    /// had, as it reads its columns as its session matched them. Returns `None` for the
    /// settings of this session, which builds the sources of a read of a view whose session had
    /// the same settings, so that the view and the direct reads of a table share its sources.
    pub(super) fn source_build_settings(
        &self,
        settings: Option<&Arc<ViewSourceSettings>>,
        state: &mut PlanResolverState,
    ) -> PlanResult<Option<Arc<ViewSourceSettings>>> {
        let Some(settings) = settings else {
            return Ok(None);
        };
        let address = Arc::as_ptr(settings) as usize;
        let same = match state.get_same_source_session(address) {
            Some(same) => same,
            None => {
                let same = settings.is_same_as(&self.ctx.session_config())?;
                state.insert_same_source_session(address, same);
                same
            }
        };
        Ok((!same).then(|| Arc::clone(settings)))
    }

    /// Returns the session state that builds sources with the settings: the state of this
    /// session with the configuration and the data sources of the settings.
    pub(super) fn source_state(&self, settings: Option<&ViewSourceSettings>) -> SessionState {
        let mut state = self.ctx.state();
        if let Some(settings) = settings {
            let config = state.config_mut();
            *config.options_mut() = settings.options.as_ref().clone();
            if let Some(spark) = &settings.spark {
                config.set_extension(Arc::clone(spark));
            }
            config.set_extension(Arc::clone(&settings.data_sources));
        }
        state
    }

    /// Looks up what a read refers to. A read with a required format reads tables only.
    async fn lookup_catalog_read(
        manager: &CatalogManager,
        reference: &[String],
        required_format: Option<&str>,
        state: &mut PlanResolverState,
    ) -> Result<CatalogTableLookup, CatalogError> {
        if required_format.is_some() {
            let (status, resolved) = Self::lookup_catalog_table(manager, reference, state).await?;
            Ok((status, Some(resolved)))
        } else {
            Self::lookup_catalog_table_or_view(manager, reference, state).await
        }
    }

    async fn resolve_query_read_catalog_table(
        &self,
        table: spec::ReadNamedTable,
        required_format: Option<&str>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::ReadNamedTable {
            name,
            temporal,
            sample,
            options,
        } = table;
        let table_reference = self.resolve_table_reference(&name);
        let reference: Vec<String> = name.clone().into();
        let manager = self.ctx.extension::<CatalogManager>()?;
        let data_source_registry = self.data_source_registry()?;
        let lookup = Self::lookup_catalog_read(&manager, &reference, required_format, state).await;
        let ((status, resolved), catalog_reference, lake_relation) = match lookup {
            Ok(lookup) => (lookup, reference.clone(), None),
            Err(error @ CatalogError::NotFound(_, _))
            | Err(error @ CatalogError::NotSupported(_)) => {
                let Some((candidate_name, base_reference)) = reference.split_last() else {
                    return Err(error.into());
                };
                if base_reference.is_empty() {
                    return Err(error.into());
                }
                let base_lookup =
                    Self::lookup_catalog_read(&manager, base_reference, required_format, state)
                        .await;
                let (base_status, base_resolved) = match base_lookup {
                    Ok(lookup) => lookup,
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
                let Some(lake_source) =
                    data_source_registry.get_lake_source_if_supported(format)?
                else {
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
                        (base_status, base_resolved),
                        base_reference.to_vec(),
                        Some((relation_provider, relation)),
                    ),
                }
            }
            Err(error) => return Err(error.into()),
        };
        if let Some(required_format) = required_format
            && !matches!(
                &status.kind,
                TableKind::Table { format, .. } if format.eq_ignore_ascii_case(required_format)
            )
        {
            return Err(PlanError::invalid(format!(
                "{} is not an {required_format} table",
                reference.join(".")
            )));
        }
        let table_reference = match table_reference {
            Ok(table_reference) => table_reference,
            Err(_) if lake_relation.is_some() => state.register_relation_name(reference.clone()),
            Err(error) => return Err(error),
        };
        let plan = match status.kind {
            TableKind::Table { ref format, .. } => {
                if let Some((_, relation)) = lake_relation.as_ref()
                    && relation.time_travel() == LakeRelationTimeTravel::Unsupported
                    && temporal.is_some()
                {
                    return Err(PlanError::unsupported(format!(
                        "SQL time travel is not supported for lake relation '{}'",
                        relation.name()
                    )));
                }
                let temporal_options = self
                    .resolve_time_travel_options(format, temporal, state)
                    .await?;
                let lake_source = data_source_registry.get_lake_source_if_supported(format)?;
                let travels_in_time = !temporal_options.is_empty()
                    || has_time_travel_option(&options)
                    || lake_source
                        .as_ref()
                        .is_some_and(|x| x.selects_table_version(&options));
                let skips_gained_columns = travels_in_time
                    && lake_source.is_some_and(|x| x.reports_table_columns_for_versions());
                let name = manager.resolve_fully_qualified_object_name(&catalog_reference)?;
                let other_session_table = Self::other_session_catalog_table(&manager, &name)?;
                let origin = CatalogTableOrigin {
                    session_table: if state.view_definition_mut().is_some() {
                        manager.session_catalog_table(&name)?
                    } else {
                        None
                    },
                    name,
                    lake_relation: lake_relation
                        .as_ref()
                        .map(|(_, relation)| relation.name().to_string()),
                    format: format.clone(),
                    format_required: required_format.is_some(),
                    options,
                    temporal_options,
                    travels_in_time,
                    skips_gained_columns,
                    read_case_sensitive: self.config.case_sensitive,
                    partition_value_cast: (
                        self.config.ansi_mode,
                        Arc::clone(&self.config.session_timezone),
                    ),
                    settings: Self::view_settings(),
                };
                // The source has a large future, which the future of every read would
                // otherwise hold, also in the recursion of the query resolver.
                let sources = Box::pin(self.resolve_catalog_table_sources(
                    CatalogTableRead {
                        status,
                        resolved,
                        origin: &origin,
                        session_table: other_session_table.as_ref(),
                        lake_relation,
                    },
                    state,
                ))
                .await?;
                self.resolve_catalog_table_scans(
                    sources,
                    table_reference,
                    TableOrigin::CatalogTable(origin),
                    state,
                )?
            }
            TableKind::View {
                definition,
                columns,
                comment: _,
                properties: _,
            } => {
                if temporal.is_some() {
                    return Err(PlanError::unsupported(
                        "SQL time travel is not supported for views",
                    ));
                }
                self.resolve_table_view(definition, columns, table_reference.clone(), state)
                    .await?
            }
            TableKind::TemporaryView { ref plan, .. }
            | TableKind::GlobalTemporaryView { ref plan, .. } => {
                if temporal.is_some() {
                    return Err(PlanError::unsupported(
                        "SQL time travel is not supported for temporary views",
                    ));
                }
                // The future is boxed to keep it out of the recursive resolver futures.
                let plan = Box::pin(self.resolve_stored_view_plan(plan, &status, state)).await?;
                let names = state.register_fields(plan.schema().inner().fields());
                let plan = rename_logical_plan(plan, &names)?;
                // The stored plan's internal field IDs belong to another resolver
                // state. Missing-reference recovery must see only the fresh output.
                state.register_missing_input_boundary(&plan);
                plan
            }
        };

        if let Some(table_sample) = sample {
            self.apply_table_sample(plan, table_sample, state).await
        } else {
            Ok(plan)
        }
    }

    /// Returns the sources that a read of a catalog table scans: one source, or one source
    /// for each group of the registered partitions of a file table that the catalog lists.
    pub(super) async fn resolve_catalog_table_sources(
        &self,
        read: CatalogTableRead<'_>,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<Arc<dyn TableSource>>> {
        let CatalogTableRead {
            status,
            resolved,
            origin,
            session_table,
            lake_relation,
        } = read;
        let TableKind::Table {
            columns,
            comment: _,
            constraints,
            format,
            location,
            partition_by,
            sort_by,
            bucket_by,
            properties,
            is_external: _,
        } = status.kind
        else {
            return Err(PlanError::internal("catalog table read of a view"));
        };
        let schema = Schema::new(columns.iter().map(|x| x.field()).collect::<Vec<_>>());
        let constraints = self.resolve_catalog_table_constraints(constraints, &schema)?;
        // Repeated references to the same table within a query share its source,
        // so that they can share scalar subqueries. Delta Lake and Iceberg sources
        // also pin one table snapshot, while file tables still list files for each scan.
        let settings = self.source_build_settings(origin.settings.as_ref(), state)?;
        let data_source_registry = self.session_data_sources(settings.as_deref())?;
        // Delta Lake and Iceberg sources read their columns by the metadata of the table,
        // whatever the letter case of column names is matched by, and do not cast the
        // partition values that the catalog lists.
        let is_lake_source = data_source_registry
            .get_lake_source_if_supported(&format)?
            .is_some();
        let source_key = lake_relation.is_none().then(|| TableSourceKey {
            catalog: status.catalog,
            database: status.database,
            name: status.name,
            format: format.clone(),
            location: location.clone(),
            // The properties of a table have unique keys, but some catalogs
            // return them in an arbitrary order.
            properties: {
                let mut properties = properties.clone();
                properties.sort_by(|a, b| a.0.cmp(&b.0));
                properties
            },
            options: origin.options.clone(),
            temporal_options: origin.temporal_options.clone(),
            read_case_sensitive: (!is_lake_source).then_some(origin.read_case_sensitive),
            partition_value_cast: (!is_lake_source).then(|| origin.partition_value_cast.clone()),
            settings: settings.as_ref().map(|x| Arc::as_ptr(x) as usize),
            session_catalog: session_table.map(|table| table.catalog_id()),
        });
        if let Some(sources) = source_key
            .as_ref()
            .and_then(|key| state.get_table_sources(key))
        {
            return Ok(sources);
        }
        let access_purpose = match lake_relation.as_ref() {
            Some((_, relation)) => lake_relation_access_purpose(relation.access()),
            None => TableAccessPurpose::DataRead,
        };
        let resolved = resolved
            .ok_or_else(|| PlanError::internal("catalog table without lakehouse resolution"))?;
        let lakehouse_table = self
            .begin_lakehouse_table_access(
                &origin.name,
                session_table,
                resolved.execution,
                access_purpose,
            )
            .await?;
        let info = SourceInfo {
            paths: location.map(|x| vec![x]).unwrap_or_default(),
            lakehouse_table: Some(lakehouse_table),
            schema: Some(schema),
            constraints,
            partition_by: partition_by.into_iter().map(|field| field.column).collect(),
            bucket_by: bucket_by.map(|x| x.into()),
            sort_order: sort_by.into_iter().map(|x| x.into()).collect(),
            // TODO: detect duplicated keys in each set of options
            options: vec![
                OptionLayer::TablePropertyList { items: properties },
                OptionLayer::OptionList {
                    items: origin.options.clone(),
                },
                OptionLayer::OptionList {
                    items: origin.temporal_options.clone(),
                },
            ],
            read_case_sensitive: origin.read_case_sensitive,
            snapshot_pins: lake_relation.is_none().then(|| state.lake_snapshot_pins()),
        };
        let session = self.source_state(settings.as_deref());
        let sources = match lake_relation {
            Some((relation_provider, relation)) => vec![
                relation_provider
                    .create_relation(&session, info, relation)
                    .await?,
            ],
            None => {
                let manager = self.ctx.extension::<CatalogManager>()?;
                self.create_catalog_table_sources(
                    &manager,
                    &session,
                    &data_source_registry,
                    origin,
                    &format,
                    resolved.registered_file_table,
                    info,
                )
                .await?
            }
        };
        if let Some(key) = source_key
            && sources.iter().all(is_reusable_source)
        {
            state.insert_table_sources(key, sources.clone());
        }
        Ok(sources)
    }

    async fn resolve_query_read_dynamic_table(
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

    /// Resolves a persistent view by re-parsing its SQL definition into a logical plan.
    async fn resolve_table_view(
        &self,
        definition: String,
        columns: Vec<TableColumnStatus>,
        table_reference: TableReference,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        if let Some(reads) = state.view_definition_mut() {
            reads.resolves_at_each_read = true;
        }
        let ast = sail_sql_analyzer::parser::parse_one_statement(&definition)?;
        let spec_plan = sail_sql_analyzer::statement::from_ast_statement(ast)?;
        let plan = match spec_plan {
            spec::Plan::Query(query_plan) => self.resolve_query_plan(query_plan, state).await?,
            _ => {
                return Err(PlanError::invalid("view definition must be a query"));
            }
        };
        let plan =
            LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(Arc::new(plan), table_reference)?);
        if columns.is_empty() {
            Ok(plan)
        } else {
            let names = state.register_field_names(columns.iter().map(|c| &c.name));
            let plan = rename_logical_plan(plan, &names)?;
            // Spark renames view columns below the view's alias, so missing-reference
            // recovery must not see the definition's column names under this output.
            state.register_missing_input_boundary(&plan);
            Ok(plan)
        }
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
                if !(-SAMPLE_ROUNDING_EPSILON..=1.0 + SAMPLE_ROUNDING_EPSILON).contains(&fraction) {
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
                let fraction = numerator as f64 / denominator as f64;
                if !(-SAMPLE_ROUNDING_EPSILON..=1.0 + SAMPLE_ROUNDING_EPSILON).contains(&fraction) {
                    return Err(PlanError::invalid(format!(
                        "Sampling fraction ({fraction}) must be on interval [0, 1]"
                    )));
                }
                (0.0, fraction)
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
        let cast_expr = Expr::Cast(datafusion_expr::expr::Cast::new(
            Box::new(resolved),
            DataType::Float64,
        ));
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
                .and_then(|x| x.inner().downcast_ref::<PySparkUnresolvedUDF>())
            {
                if f.eval_type().is_table_function() {
                    let udtf = PythonUdtf {
                        python_version: f.python_version().to_string(),
                        eval_type: f.eval_type(),
                        command: f.command().to_vec(),
                        return_type: f.output_type().cloned(),
                    };
                    let input = self.resolve_query_empty(true)?;
                    // Combine positional arguments with named_arguments (SQL kwargs like a => 10).
                    // Named arguments are appended after positional ones, wrapped as NamedArgument
                    // expressions so that extract_kwargs can process them uniformly.
                    let all_arguments: Vec<spec::Expr> = arguments
                        .into_iter()
                        .chain(named_arguments.into_iter().map(|(key, value)| {
                            spec::Expr::NamedArgument {
                                key: key.into(),
                                value: Box::new(value),
                            }
                        }))
                        .collect();
                    let (positional_args, kwarg_names) = Self::extract_kwargs(all_arguments);
                    // Validate: no duplicate kwarg names and no positional arg after a named arg.
                    {
                        let mut seen_kwarg_names = std::collections::HashSet::new();
                        let mut seen_named = false;
                        for kwarg in &kwarg_names {
                            match kwarg {
                                Some(name) => {
                                    if !seen_kwarg_names.insert(name.as_str()) {
                                        return Err(PlanError::AnalysisError(format!(
                                            "[DUPLICATE_ROUTINE_PARAMETER_ASSIGNMENT.DOUBLE_NAMED_ARGUMENT_REFERENCE] \
                                             Duplicate named argument: '{name}' is assigned more than once."
                                        )));
                                    }
                                    seen_named = true;
                                }
                                None => {
                                    if seen_named {
                                        return Err(PlanError::AnalysisError(
                                            "[UNEXPECTED_POSITIONAL_ARGUMENT] \
                                             Positional argument follows a named (keyword) argument."
                                                .to_string(),
                                        ));
                                    }
                                }
                            }
                        }
                    }
                    let arguments = self
                        .resolve_named_expressions(positional_args, input.schema(), state)
                        .await?;
                    self.resolve_python_udtf_plan(
                        udtf,
                        &function_name,
                        input,
                        arguments,
                        &kwarg_names,
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
                let table_function = match self.ctx.table_function(&canonical_function_name) {
                    Ok(f) => f,
                    _ => match get_built_in_table_function(&canonical_function_name) {
                        Ok(f) => f,
                        _ => {
                            return Err(PlanError::unsupported(format!(
                                "unknown table function: {function_name}"
                            )));
                        }
                    },
                };
                let session_state = self.ctx.state();
                let table_provider = table_function.create_table_provider_with_args(
                    TableFunctionArgs::new(&arguments, &session_state),
                )?;
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

    async fn resolve_query_read_data_source(
        &self,
        mut source: spec::ReadDataSource,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        if !source.predicates.is_empty() {
            return Err(PlanError::todo("data source predicates"));
        }
        let format = source
            .format
            .as_deref()
            .unwrap_or(&self.config.default_table_file_format);
        if format.eq_ignore_ascii_case("iceberg") {
            if source.schema.is_some() {
                return Err(PlanError::unsupported(
                    "iceberg does not support user specified schema",
                ));
            }
            // Connect options have no stable ordering, so conflicting aliases cannot use
            // last-option-wins semantics without choosing an unpredictable table.
            let mut paths = source
                .options
                .iter()
                .filter(|(key, _)| key.eq_ignore_ascii_case("path"))
                .map(|(_, value)| value);
            if let Some(path) = paths.next().cloned() {
                if paths.any(|value| value != &path) {
                    return Err(PlanError::invalid(
                        "Conflicting case-insensitive path options",
                    ));
                }
                if !source.paths.is_empty() {
                    return Err(PlanError::invalid(
                        "Cannot specify both a path option and load paths",
                    ));
                }
                source.paths.push(path);
                source
                    .options
                    .retain(|(key, _)| !key.eq_ignore_ascii_case("path"));
            }
            // Iceberg's Spark source treats locations containing '/' as paths and
            // parses other inputs as identifiers. Also retain Windows paths and URLs.
            if let [path] = source.paths.as_slice()
                && !path.contains(['/', '\\'])
                && !Url::parse(path).is_ok_and(|url| url.scheme().len() > 1)
            {
                let name = sail_sql_analyzer::expression::from_ast_object_name(
                    sail_sql_analyzer::parser::parse_object_name(path)?,
                )?;
                // Catalog-only resolution excludes CTEs and format.`path` rewrites.
                return self
                    .resolve_query_read_catalog_table(
                        spec::ReadNamedTable {
                            name,
                            temporal: None,
                            sample: None,
                            options: source.options,
                        },
                        Some("iceberg"),
                        state,
                    )
                    .await;
            }
        }
        self.resolve_query_read_data_source_by_path(source, state)
            .await
    }

    async fn resolve_query_read_data_source_by_path(
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
        let format = format.unwrap_or_else(|| self.config.default_table_file_format.clone());
        let schema = match schema {
            Some(schema) => Some(self.resolve_schema(schema, state)?),
            None => None,
        };
        let lake_source = self
            .data_source_registry()?
            .get_lake_source_if_supported(&format)?;
        let is_lake = lake_source.is_some();
        let origin = LakePathOrigin {
            skips_gained_columns: lake_source.is_some_and(|x| {
                x.reports_table_columns_for_versions()
                    && (has_time_travel_option(&options) || x.selects_table_version(&options))
            }),
            format,
            schema,
            options,
            paths,
            read_case_sensitive: self.config.case_sensitive,
            settings: Self::view_settings(),
        };
        let table_source = self.create_path_source(&origin, is_lake, state).await?;
        self.resolve_table_source_with_rename(
            table_source,
            UNNAMED_TABLE,
            None,
            vec![],
            None,
            is_lake.then_some(TableOrigin::LakePath(origin)),
            state,
        )
    }

    /// Returns the source that a read of paths scans. The reads of a Delta Lake or Iceberg
    /// table (`is_lake`) with the same paths, options, and schema share one source in a
    /// query, so that they read one version of the table.
    pub(super) async fn create_path_source(
        &self,
        read: &LakePathOrigin,
        is_lake: bool,
        state: &mut PlanResolverState,
    ) -> PlanResult<Arc<dyn TableSource>> {
        let settings = self.source_build_settings(read.settings.as_ref(), state)?;
        let key = is_lake.then(|| {
            let mut options = read.options.clone();
            options.sort_by(|a, b| a.0.cmp(&b.0));
            LakePathSourceKey {
                format: read.format.to_lowercase(),
                paths: read.paths.clone(),
                options,
                schema: read.schema.clone(),
                settings: settings.as_ref().map(|x| Arc::as_ptr(x) as usize),
            }
        });
        if let Some(source) = key.as_ref().and_then(|x| state.get_lake_path_source(x)) {
            return Ok(source);
        }
        let info = SourceInfo {
            paths: read.paths.clone(),
            lakehouse_table: None,
            schema: read.schema.clone(),
            constraints: Default::default(),
            partition_by: vec![],
            bucket_by: None,
            sort_order: vec![],
            // TODO: detect duplicated keys in the set of options
            options: vec![OptionLayer::OptionList {
                items: read.options.clone(),
            }],
            read_case_sensitive: read.read_case_sensitive,
            snapshot_pins: Some(state.lake_snapshot_pins()),
        };
        let registry = self.session_data_sources(settings.as_deref())?;
        let source = registry
            .get_data_source(&read.format)?
            .create_source(&self.source_state(settings.as_deref()), info)
            .await?;
        if let Some(key) = key
            && is_reusable_source(&source)
        {
            state.insert_lake_path_source(key, Arc::clone(&source));
        }
        Ok(source)
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
            None,
            state,
        )
    }

    /// Resolves a scan of the table source. `origin` is what the source reads, for a
    /// source that a temporary view obtains again at each read.
    pub(super) fn resolve_table_source_with_rename(
        &self,
        table_source: Arc<dyn TableSource>,
        table_reference: impl Into<TableReference>,
        projection: Option<Vec<usize>>,
        filters: Vec<datafusion_expr::expr::Expr>,
        fetch: Option<usize>,
        origin: Option<TableOrigin>,
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
            // TODO: support duplicate column names for other `TableSource` implementations
            let names = state.register_fields(schema.fields());
            rename_source(&table_source, names)?
        } else {
            table_source
        };
        let table_source = match (origin, state.view_definition_mut()) {
            (Some(origin), Some(reads)) => {
                let table_source = view_definition_source(table_source);
                reads.record_table_origin(&table_source, Arc::new(origin));
                table_source
            }
            _ => table_source,
        };

        let table_scan = LogicalPlan::TableScan(
            TableScanBuilder::new(table_reference, table_source)
                .with_projection(projection)
                .with_filters(filters)
                .with_fetch(fetch)
                .build()?,
        );

        if !has_duplicates {
            let names = state.register_fields(table_scan.schema().fields());
            let plan = rename_logical_plan(table_scan, &names)?;
            // Physical column names (including names such as "#0") are not
            // resolver field IDs. Only the renamed output is a resolution input.
            state.register_missing_input_boundary(&plan);
            Ok(plan)
        } else {
            Ok(table_scan)
        }
    }
}

/// Returns the source that reads the columns of a source under other names, for a source
/// with duplicate column names.
pub(super) fn rename_source(
    source: &Arc<dyn TableSource>,
    names: Vec<String>,
) -> PlanResult<Arc<dyn TableSource>> {
    let provider = source_as_provider(source).map_err(|e| {
        PlanError::unsupported(format!(
            "duplicate column names require DefaultTableSource-backed TableProvider: {e}"
        ))
    })?;
    Ok(provider_as_source(Arc::new(RenameTableProvider::try_new(
        provider, names,
    )?)))
}

/// A read of a catalog table: the lookup of the table, and what the read reads.
pub(super) struct CatalogTableRead<'a> {
    pub status: TableStatus,
    pub resolved: Option<LakehouseResolvedTable>,
    pub origin: &'a CatalogTableOrigin,
    /// The table in the catalog of another session, for a table that a global temporary
    /// view reads in the session that created the view.
    pub session_table: Option<&'a SessionCatalogTable>,
    pub lake_relation: Option<(Arc<dyn LakeRelationProvider>, LakeRelation)>,
}

fn lake_relation_access_purpose(access: LakeRelationAccess) -> TableAccessPurpose {
    match access {
        LakeRelationAccess::MetadataRead => TableAccessPurpose::MetadataRead,
        LakeRelationAccess::DataRead => TableAccessPurpose::DataRead,
    }
}
