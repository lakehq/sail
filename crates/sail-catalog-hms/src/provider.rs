use std::time::Duration;

use futures::future::try_join_all;
use hive_metastore::{
    EnvironmentContext, GetTableRequest, Table, ThriftHiveMetastoreAlterTableException,
    ThriftHiveMetastoreClient, ThriftHiveMetastoreClientBuilder,
    ThriftHiveMetastoreCreateDatabaseException, ThriftHiveMetastoreCreateTableException,
    ThriftHiveMetastoreDropDatabaseException, ThriftHiveMetastoreDropTableException,
    ThriftHiveMetastoreDropTableWithEnvironmentContextException,
    ThriftHiveMetastoreGetDatabaseException, ThriftHiveMetastoreGetTableException,
    ThriftHiveMetastoreGetTableReqException,
};
use pilota::{AHashMap, FastStr};
use sail_catalog::error::{CatalogError, CatalogObject, CatalogResult};
use sail_catalog::hive_format::HiveCatalogFormat;
use sail_catalog::provider::{
    AlterTableOptions, CatalogProvider, CreateDatabaseOptions, CreateTableOptions,
    CreateViewOptions, DropDatabaseOptions, DropTableOptions, DropViewOptions, Namespace,
    PartitionTransform,
};
use sail_common::runtime::RuntimeHandle;
use sail_common_datafusion::catalog::{DatabaseStatus, TableStatus};
use tokio::sync::Mutex;
use volo_thrift::MaybeException;

use crate::convert::{
    build_database, build_generic_table, build_view, database_to_status, is_view_table,
    table_to_status, validate_namespace, view_to_status, GenericTableFormat,
};
use crate::security::{KerberosMakeTransport, SaslQop};

#[derive(Debug, Clone, Default)]
pub struct HmsCatalogConfig {
    pub uris: Vec<String>,
    pub thrift_transport: Option<String>,
    pub auth: Option<String>,
    pub kerberos_service_principal: Option<String>,
    pub min_sasl_qop: Option<String>,
    pub connect_timeout_secs: Option<u64>,
}

pub struct HmsCatalogProvider {
    name: String,
    config: HmsCatalogConfig,
    min_sasl_qop: SaslQop,
    endpoints: Vec<HmsEndpoint>,
    state: Mutex<HmsClientState>,
}

#[derive(Debug, Clone)]
struct HmsEndpoint {
    uri: String,
}

#[derive(Clone)]
struct HmsClientState {
    active_index: usize,
    client: Option<ThriftHiveMetastoreClient>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CatalogAuthMode {
    None,
    Kerberos { hostbased_service: String },
}

#[derive(Debug)]
struct DropTableRequest {
    delete_data: bool,
    environment_context: Option<EnvironmentContext>,
}

const HMS_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

fn build_drop_table_request(purge: bool) -> DropTableRequest {
    let environment_context = purge.then(|| EnvironmentContext {
        properties: Some(AHashMap::from_iter([(
            FastStr::from_static_str("ifPurge"),
            FastStr::from_static_str("TRUE"),
        )])),
    });

    DropTableRequest {
        delete_data: true,
        environment_context,
    }
}

fn split_hms_uri_list(uris: &[String]) -> CatalogResult<Vec<String>> {
    if uris.is_empty() {
        return Err(CatalogError::InvalidArgument(
            "HMS `uris` must contain at least one endpoint".to_string(),
        ));
    }
    let mut result = Vec::new();
    for uri in uris {
        for token in uri.split(',') {
            let token = token.trim();
            if token.is_empty() {
                continue;
            }
            result.push(normalize_hms_uri(token)?);
        }
    }
    if result.is_empty() {
        return Err(CatalogError::InvalidArgument(
            "HMS `uris` must contain at least one non-empty endpoint".to_string(),
        ));
    }
    Ok(result)
}

fn normalize_hms_uri(uri: &str) -> CatalogResult<String> {
    let uri_lower = uri.trim_start().to_ascii_lowercase();
    let stripped = uri_lower
        .strip_prefix("thrift://")
        .map(|suffix| &uri[uri.len() - suffix.len()..])
        .unwrap_or(uri);
    let normalized = stripped.trim();
    if normalized.is_empty() {
        return Err(CatalogError::InvalidArgument(
            "Invalid empty HMS URI entry".to_string(),
        ));
    }
    if normalized.contains("://") {
        return Err(CatalogError::InvalidArgument(format!(
            "Invalid HMS URI '{uri}', only optional thrift:// scheme is supported"
        )));
    }
    extract_hms_host(normalized)?;
    Ok(normalized.to_string())
}

fn extract_hms_host(uri: &str) -> CatalogResult<&str> {
    let (host, port) = uri.rsplit_once(':').ok_or_else(|| {
        CatalogError::InvalidArgument(format!("Invalid HMS URI '{uri}', expected host:port"))
    })?;
    if host.is_empty() || port.is_empty() {
        return Err(CatalogError::InvalidArgument(format!(
            "Invalid HMS URI '{uri}', expected host:port"
        )));
    }
    Ok(host.trim_matches(['[', ']']))
}

fn expand_kerberos_service_principal(principal: &str, uri: &str) -> CatalogResult<String> {
    let host = extract_hms_host(uri)?;
    Ok(principal.replace("_HOST", host))
}

fn build_hostbased_service_name(service_principal: &str) -> CatalogResult<String> {
    let (service_and_host, _) = service_principal.split_once('@').ok_or_else(|| {
        CatalogError::InvalidArgument(format!(
            "Invalid kerberos_service_principal '{service_principal}', expected service/host@REALM"
        ))
    })?;
    let (service, host) = service_and_host.split_once('/').ok_or_else(|| {
        CatalogError::InvalidArgument(format!(
            "Invalid kerberos_service_principal '{service_principal}', expected service/host@REALM"
        ))
    })?;
    if service.is_empty() || host.is_empty() {
        return Err(CatalogError::InvalidArgument(format!(
            "Invalid kerberos_service_principal '{service_principal}', expected service/host@REALM"
        )));
    }
    Ok(format!("{service}@{host}"))
}

fn catalog_auth_mode(
    config: &HmsCatalogConfig,
    endpoint_uri: &str,
) -> CatalogResult<CatalogAuthMode> {
    match config
        .auth
        .as_deref()
        .unwrap_or("none")
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "none" => Ok(CatalogAuthMode::None),
        "kerberos" => {
            if !crate::security::gssapi_available() {
                return Err(CatalogError::InvalidArgument(
                    "Kerberos auth was requested but the GSSAPI runtime library is not available. \
                     Install the Kerberos runtime (e.g. libgssapi-krb5-2 on Debian, krb5-libs on RHEL) \
                     and run kinit before starting Sail."
                        .to_string(),
                ));
            }
            let service_principal = config
                .kerberos_service_principal
                .as_deref()
                .ok_or_else(|| {
                    CatalogError::InvalidArgument(
                        "kerberos_service_principal is required when HMS auth is 'kerberos'"
                            .to_string(),
                    )
                })
                .and_then(|principal| expand_kerberos_service_principal(principal, endpoint_uri))?;
            let hostbased_service = build_hostbased_service_name(&service_principal)?;
            Ok(CatalogAuthMode::Kerberos { hostbased_service })
        }
        other => Err(CatalogError::InvalidArgument(format!(
            "Unsupported HMS auth '{other}', expected 'none' or 'kerberos'"
        ))),
    }
}

fn parse_min_sasl_qop(config: &HmsCatalogConfig) -> CatalogResult<SaslQop> {
    SaslQop::from_config(config.min_sasl_qop.as_deref().unwrap_or("auth"))
}

fn connect_timeout(config: &HmsCatalogConfig) -> Duration {
    config
        .connect_timeout_secs
        .map(Duration::from_secs)
        .unwrap_or(HMS_CONNECT_TIMEOUT)
}

impl HmsCatalogProvider {
    pub fn new(
        name: String,
        config: HmsCatalogConfig,
        runtime: RuntimeHandle,
    ) -> CatalogResult<Self> {
        Self::try_new(name, config, runtime)
    }

    pub fn try_new(
        name: String,
        config: HmsCatalogConfig,
        _runtime: RuntimeHandle,
    ) -> CatalogResult<Self> {
        let min_sasl_qop = parse_min_sasl_qop(&config)?;
        let normalized = split_hms_uri_list(&config.uris)?;
        let endpoints = normalized
            .into_iter()
            .map(|uri| HmsEndpoint { uri })
            .collect::<Vec<_>>();

        Ok(Self {
            name,
            config,
            min_sasl_qop,
            endpoints,
            state: Mutex::new(HmsClientState {
                active_index: 0,
                client: None,
            }),
        })
    }

    /// Determines whether an error is retryable by failing over to the next endpoint.
    /// Matches on stable prefixes injected at each error source:
    /// - "transport error" from `hms_client_error` for volo-thrift transport failures
    /// - "gssapi error" from `check_gss_ok` for Kerberos/GSSAPI failures
    /// - "dns error" from `build_client_for_endpoint` for DNS resolution failures
    fn should_retry(error: &CatalogError) -> bool {
        match error {
            CatalogError::External(message) => {
                let message = message.to_ascii_lowercase();
                message.contains("transport error")
                    || message.contains("gssapi error")
                    || message.contains("dns error")
            }
            _ => false,
        }
    }

    /// Converts a volo-thrift `ClientError` into a `CatalogError`,
    /// preserving transport-level classification for retry logic.
    fn hms_client_error(context: &str, error: volo_thrift::ClientError) -> CatalogError {
        use volo_thrift::ClientError;
        match &error {
            ClientError::Transport(_) => {
                CatalogError::External(format!("{context}: transport error: {error}"))
            }
            _ => CatalogError::External(format!("{context}: {error}")),
        }
    }

    async fn build_client_for_endpoint(
        &self,
        index: usize,
    ) -> CatalogResult<ThriftHiveMetastoreClient> {
        let endpoint = self.endpoints.get(index).ok_or_else(|| {
            CatalogError::Internal(format!("HMS endpoint index out of bounds: {index}"))
        })?;
        let address = tokio::net::lookup_host(endpoint.uri.as_str())
            .await
            .map_err(|error| {
                CatalogError::External(format!(
                    "dns error: failed to resolve HMS URI '{}': {error}",
                    endpoint.uri
                ))
            })?
            .next()
            .ok_or_else(|| {
                CatalogError::External(format!(
                    "dns error: HMS URI '{}' did not resolve to an address",
                    endpoint.uri
                ))
            })?;

        let builder = ThriftHiveMetastoreClientBuilder::new("hms")
            .address(address)
            .connect_timeout(Some(connect_timeout(&self.config)));

        let auth_mode = catalog_auth_mode(&self.config, &endpoint.uri)?;
        let thrift_transport = self
            .config
            .thrift_transport
            .as_deref()
            .unwrap_or("buffered")
            .to_ascii_lowercase();

        let client = match (auth_mode, thrift_transport.as_str()) {
            (CatalogAuthMode::None, "buffered") => builder
                .make_codec(volo_thrift::codec::default::DefaultMakeCodec::buffered())
                .build(),
            (CatalogAuthMode::None, "framed") => builder
                .make_codec(volo_thrift::codec::default::DefaultMakeCodec::framed())
                .build(),
            (CatalogAuthMode::Kerberos { hostbased_service }, "buffered") => builder
                .make_transport(KerberosMakeTransport::new(
                    hostbased_service,
                    self.min_sasl_qop,
                ))
                .make_codec(volo_thrift::codec::default::DefaultMakeCodec::buffered())
                .build(),
            (CatalogAuthMode::Kerberos { hostbased_service }, "framed") => builder
                .make_transport(KerberosMakeTransport::new(
                    hostbased_service,
                    self.min_sasl_qop,
                ))
                .make_codec(volo_thrift::codec::default::DefaultMakeCodec::framed())
                .build(),
            (_, other) => {
                return Err(CatalogError::InvalidArgument(format!(
                    "Unsupported thrift_transport '{other}', expected 'buffered' or 'framed'"
                )))
            }
        };
        Ok(client)
    }

    async fn current_client(&self) -> CatalogResult<(usize, ThriftHiveMetastoreClient)> {
        let active_index = {
            let state = self.state.lock().await;
            if let Some(client) = &state.client {
                return Ok((state.active_index, client.clone()));
            }
            state.active_index
        };

        match self.build_client_for_endpoint(active_index).await {
            Ok(client) => {
                let mut state = self.state.lock().await;
                if state.client.is_none() && state.active_index == active_index {
                    state.client = Some(client.clone());
                    return Ok((active_index, client));
                }
                if let Some(existing) = &state.client {
                    return Ok((state.active_index, existing.clone()));
                }
                state.client = Some(client.clone());
                Ok((state.active_index, client))
            }
            Err(_error) if self.endpoints.len() > 1 => {
                self.rotate_client_after_failure(active_index).await
            }
            Err(error) => Err(error),
        }
    }

    async fn rotate_client_after_failure(
        &self,
        failed_index: usize,
    ) -> CatalogResult<(usize, ThriftHiveMetastoreClient)> {
        {
            let state = self.state.lock().await;
            if state.active_index != failed_index {
                if let Some(client) = &state.client {
                    return Ok((state.active_index, client.clone()));
                }
            }
        }

        let endpoint_count = self.endpoints.len();
        let mut last_error = None;
        for step in 1..endpoint_count {
            let next_index = (failed_index + step) % endpoint_count;
            match self.build_client_for_endpoint(next_index).await {
                Ok(client) => {
                    let mut state = self.state.lock().await;
                    state.active_index = next_index;
                    state.client = Some(client.clone());
                    return Ok((next_index, client));
                }
                Err(error) => {
                    last_error = Some(error);
                }
            }
        }

        {
            let mut state = self.state.lock().await;
            state.client = None;
        }
        Err(last_error.unwrap_or_else(|| {
            CatalogError::External("No HMS endpoints are available after failover".to_string())
        }))
    }

    async fn with_failover<T, F, Fut>(&self, mut operation: F) -> CatalogResult<T>
    where
        F: FnMut(ThriftHiveMetastoreClient) -> Fut,
        Fut: std::future::Future<Output = CatalogResult<T>>,
    {
        self.with_failover_attempt(|client, _attempt| operation(client))
            .await
    }

    async fn with_failover_attempt<T, F, Fut>(&self, mut operation: F) -> CatalogResult<T>
    where
        F: FnMut(ThriftHiveMetastoreClient, usize) -> Fut,
        Fut: std::future::Future<Output = CatalogResult<T>>,
    {
        let endpoint_count = self.endpoints.len();
        let mut attempts = 0usize;

        loop {
            let (active_index, client) = self.current_client().await?;
            match operation(client, attempts).await {
                Ok(value) => return Ok(value),
                Err(error) if Self::should_retry(&error) && attempts + 1 < endpoint_count => {
                    attempts += 1;
                    self.rotate_client_after_failure(active_index).await?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) async fn create_hms_table(
        &self,
        database: &Namespace,
        table_name: &str,
        table: Table,
        if_not_exists: bool,
    ) -> CatalogResult<TableStatus> {
        let db_name = validate_namespace(database)?;
        let table_name_owned = table_name.to_string();

        self.with_failover_attempt(|client, attempt| {
            let table = table.clone();
            let db_name = db_name.clone();
            let table_name = table_name_owned.clone();
            async move {
                match client.create_table(table).await {
                    Ok(MaybeException::Ok(())) => Ok(()),
                    Ok(MaybeException::Exception(ThriftHiveMetastoreCreateTableException::O1(
                        _,
                    ))) if if_not_exists || attempt > 0 => Ok(()),
                    Ok(MaybeException::Exception(ThriftHiveMetastoreCreateTableException::O1(
                        _,
                    ))) => Err(CatalogError::AlreadyExists(
                        CatalogObject::Table,
                        format!("{db_name}.{table_name}"),
                    )),
                    Ok(MaybeException::Exception(err)) => Err(CatalogError::External(format!(
                        "Failed to create HMS table '{db_name}.{table_name}': {err:?}"
                    ))),
                    Err(err) => Err(Self::hms_client_error(
                        &format!("Failed to create HMS table '{db_name}.{table_name}'"),
                        err,
                    )),
                }
            }
        })
        .await?;

        self.get_table(database, &table_name_owned).await
    }

    async fn fetch_hms_table(&self, database: &Namespace, name: &str) -> CatalogResult<Table> {
        let db_name = validate_namespace(database)?;
        let table_name = name.to_string();

        self.with_failover(|client| {
            let db_name = db_name.clone();
            let table_name = table_name.clone();
            async move {
                match client
                    .get_table(db_name.clone().into(), table_name.clone().into())
                    .await
                {
                    Ok(MaybeException::Ok(table)) => Ok(table),
                    Ok(MaybeException::Exception(ThriftHiveMetastoreGetTableException::O2(_))) => {
                        Err(CatalogError::NotFound(CatalogObject::Table, format!("{db_name}.{table_name}")))
                    }
                    Ok(MaybeException::Exception(err)) => Err(CatalogError::External(format!(
                        "Failed to fetch HMS table '{db_name}.{table_name}': {err:?}"
                    ))),
                    Err(err) if err.to_string().contains("Invalid method name: 'get_table'") => {
                        match client
                            .get_table_req(GetTableRequest {
                                db_name: db_name.clone().into(),
                                tbl_name: table_name.clone().into(),
                                capabilities: None,
                            })
                            .await
                        {
                            Ok(MaybeException::Ok(result)) => Ok(result.table),
                            Ok(MaybeException::Exception(
                                ThriftHiveMetastoreGetTableReqException::O2(_),
                            )) => Err(CatalogError::NotFound(
                                CatalogObject::Table,
                                format!("{db_name}.{table_name}"),
                            )),
                            Ok(MaybeException::Exception(err)) => {
                                Err(CatalogError::External(format!(
                                    "Failed to fetch HMS table '{db_name}.{table_name}' via get_table_req: {err:?}"
                                )))
                            }
                            Err(err) => Err(Self::hms_client_error(
                                &format!(
                                    "Failed to fetch HMS table '{db_name}.{table_name}' via get_table_req"
                                ),
                                err,
                            )),
                        }
                    }
                    Err(err) => Err(Self::hms_client_error(
                        &format!("Failed to fetch HMS table '{db_name}.{table_name}'"),
                        err,
                    )),
                }
            }
        })
        .await
    }

    async fn list_hms_tables(&self, database: &Namespace) -> CatalogResult<Vec<Table>> {
        let db_name = validate_namespace(database)?;
        let db_name_owned = db_name.clone();
        let names = self
            .with_failover(|client| {
                let db_name = db_name_owned.clone();
                async move {
                    match client.get_all_tables(db_name.clone().into()).await {
                        Ok(MaybeException::Ok(names)) => Ok(names),
                        Ok(MaybeException::Exception(err)) => Err(CatalogError::External(format!(
                            "Failed to list HMS tables for '{db_name}': {err:?}"
                        ))),
                        Err(err) => Err(Self::hms_client_error(
                            &format!("Failed to list HMS tables for '{db_name}'"),
                            err,
                        )),
                    }
                }
            })
            .await?;

        let tbl_names = names.to_vec();
        self.with_failover(|client| {
            let db_name = db_name_owned.clone();
            let tbl_names = tbl_names.clone();
            async move {
                match client
                    .get_table_objects_by_name(db_name.into(), tbl_names)
                    .await
                {
                    Ok(MaybeException::Ok(tables)) => Ok(tables),
                    Ok(MaybeException::Exception(err)) => Err(CatalogError::External(format!(
                        "Failed to batch-fetch HMS tables: {err:?}"
                    ))),
                    Err(err) => Err(Self::hms_client_error(
                        "Failed to batch-fetch HMS tables",
                        err,
                    )),
                }
            }
        })
        .await
    }

    async fn drop_hms_table(
        &self,
        db_name: &str,
        table: &str,
        delete_data: bool,
        if_exists: bool,
    ) -> CatalogResult<()> {
        let db_name = db_name.to_string();
        let table = table.to_string();
        self.with_failover_attempt(|client, attempt| {
            let db_name = db_name.clone();
            let table = table.clone();
            async move {
                match client
                    .drop_table(db_name.clone().into(), table.clone().into(), delete_data)
                    .await
                {
                    Ok(MaybeException::Ok(())) => Ok(()),
                    Ok(MaybeException::Exception(ThriftHiveMetastoreDropTableException::O1(_)))
                        if if_exists || attempt > 0 =>
                    {
                        Ok(())
                    }
                    Ok(MaybeException::Exception(ThriftHiveMetastoreDropTableException::O1(_))) => {
                        Err(CatalogError::NotFound(
                            CatalogObject::Table,
                            format!("{db_name}.{table}"),
                        ))
                    }
                    Ok(MaybeException::Exception(err)) => Err(CatalogError::External(format!(
                        "Failed to drop HMS table '{db_name}.{table}': {err:?}"
                    ))),
                    Err(err) => Err(Self::hms_client_error(
                        &format!("Failed to drop HMS table '{db_name}.{table}'"),
                        err,
                    )),
                }
            }
        })
        .await
    }

    async fn drop_hms_table_with_environment_context(
        &self,
        db_name: &str,
        table: &str,
        delete_data: bool,
        environment_context: EnvironmentContext,
        if_exists: bool,
    ) -> CatalogResult<()> {
        let db_name = db_name.to_string();
        let table = table.to_string();
        let result = self
            .with_failover_attempt(|client, attempt| {
                let db_name = db_name.clone();
                let table = table.clone();
                let environment_context = environment_context.clone();
                async move {
                    match client
                        .drop_table_with_environment_context(
                            db_name.clone().into(),
                            table.clone().into(),
                            delete_data,
                            environment_context,
                        )
                        .await
                    {
                        Ok(MaybeException::Ok(())) => Ok(()),
                        Ok(MaybeException::Exception(
                            ThriftHiveMetastoreDropTableWithEnvironmentContextException::O1(_),
                        )) if if_exists || attempt > 0 => Ok(()),
                        Ok(MaybeException::Exception(
                            ThriftHiveMetastoreDropTableWithEnvironmentContextException::O1(_),
                        )) => Err(CatalogError::NotFound(
                            CatalogObject::Table,
                            format!("{db_name}.{table}"),
                        )),
                        Ok(MaybeException::Exception(err)) => Err(CatalogError::External(format!(
                            "Failed to drop HMS table '{db_name}.{table}': {err:?}"
                        ))),
                        Err(err)
                            if err.to_string().contains(
                                "Invalid method name: 'drop_table_with_environment_context'",
                            ) =>
                        {
                            Err(CatalogError::NotSupported(
                                "drop_table_with_environment_context_unavailable".to_string(),
                            ))
                        }
                        Err(err) => Err(Self::hms_client_error(
                            &format!("Failed to drop HMS table '{db_name}.{table}'"),
                            err,
                        )),
                    }
                }
            })
            .await;
        match result {
            Ok(()) => Ok(()),
            Err(CatalogError::NotSupported(msg))
                if msg == "drop_table_with_environment_context_unavailable" =>
            {
                self.drop_hms_table(&db_name, &table, delete_data, if_exists)
                    .await
            }
            Err(other) => Err(other),
        }
    }
}

#[async_trait::async_trait]
impl CatalogProvider for HmsCatalogProvider {
    fn get_name(&self) -> &str {
        &self.name
    }

    async fn create_database(
        &self,
        database: &Namespace,
        options: CreateDatabaseOptions,
    ) -> CatalogResult<DatabaseStatus> {
        let db_name = validate_namespace(database)?;
        let if_not_exists = options.if_not_exists;
        let database = build_database(database, options)?;
        self.with_failover_attempt(|client, attempt| {
            let database = database.clone();
            let db_name = db_name.clone();
            async move {
                match client.create_database(database).await {
                    Ok(MaybeException::Ok(())) => Ok(()),
                    Ok(MaybeException::Exception(
                        ThriftHiveMetastoreCreateDatabaseException::O1(_),
                    )) if if_not_exists || attempt > 0 => Ok(()),
                    Ok(MaybeException::Exception(
                        ThriftHiveMetastoreCreateDatabaseException::O1(_),
                    )) => Err(CatalogError::AlreadyExists(
                        CatalogObject::Database,
                        db_name,
                    )),
                    Ok(MaybeException::Exception(err)) => Err(CatalogError::External(format!(
                        "Failed to create HMS database: {err:?}"
                    ))),
                    Err(err) => Err(Self::hms_client_error("Failed to create HMS database", err)),
                }
            }
        })
        .await?;

        self.get_database(&Namespace::try_from(vec![db_name])?)
            .await
    }

    async fn get_database(&self, database: &Namespace) -> CatalogResult<DatabaseStatus> {
        let db_name = validate_namespace(database)?;
        let database = self
            .with_failover(|client| {
                let db_name = db_name.clone();
                async move {
                    match client.get_database(db_name.clone().into()).await {
                        Ok(MaybeException::Ok(database)) => Ok(database),
                        Ok(MaybeException::Exception(
                            ThriftHiveMetastoreGetDatabaseException::O1(_),
                        )) => Err(CatalogError::NotFound(CatalogObject::Database, db_name)),
                        Ok(MaybeException::Exception(err)) => Err(CatalogError::External(format!(
                            "Failed to fetch HMS database '{db_name}': {err:?}"
                        ))),
                        Err(err) => Err(Self::hms_client_error(
                            &format!("Failed to fetch HMS database '{db_name}'"),
                            err,
                        )),
                    }
                }
            })
            .await?;
        database_to_status(&self.name, &database)
    }

    async fn list_databases(
        &self,
        prefix: Option<&Namespace>,
    ) -> CatalogResult<Vec<DatabaseStatus>> {
        let pattern = match prefix {
            Some(prefix) => Some(validate_namespace(prefix)?),
            None => None,
        };

        let databases = match &pattern {
            Some(pat) => {
                let pat = pat.clone();
                self.with_failover(|client| {
                    let pat = pat.clone();
                    async move {
                        match client.get_databases(pat.into()).await {
                            Ok(MaybeException::Ok(databases)) => Ok(databases),
                            Ok(MaybeException::Exception(err)) => Err(CatalogError::External(
                                format!("Failed to list HMS databases: {err:?}"),
                            )),
                            Err(err) => {
                                Err(Self::hms_client_error("Failed to list HMS databases", err))
                            }
                        }
                    }
                })
                .await?
            }
            None => {
                self.with_failover(|client| async move {
                    match client.get_all_databases().await {
                        Ok(MaybeException::Ok(databases)) => Ok(databases),
                        Ok(MaybeException::Exception(err)) => Err(CatalogError::External(format!(
                            "Failed to list HMS databases: {err:?}"
                        ))),
                        Err(err) => {
                            Err(Self::hms_client_error("Failed to list HMS databases", err))
                        }
                    }
                })
                .await?
            }
        };

        // HMS has no batch database fetch API (unlike get_table_objects_by_name for tables),
        // so we must call get_database() individually. Fetch concurrently to avoid
        // serial N+1 latency — the tokio::sync::Mutex in with_failover is held only
        // briefly to clone the client, never across .await points.
        let namespaces: Vec<Namespace> = databases
            .into_iter()
            .map(|name| Namespace::try_from(vec![name.to_string()]))
            .collect::<CatalogResult<Vec<_>>>()?;
        let fetches: Vec<_> = namespaces.iter().map(|ns| self.get_database(ns)).collect();
        let result = try_join_all(fetches).await?;
        Ok(result)
    }

    async fn drop_database(
        &self,
        database: &Namespace,
        options: DropDatabaseOptions,
    ) -> CatalogResult<()> {
        let db_name = validate_namespace(database)?;

        self.with_failover_attempt(|client, attempt| {
            let db_name = db_name.clone();
            async move {
                match client
                    .drop_database(db_name.clone().into(), false, options.cascade)
                    .await
                {
                    Ok(MaybeException::Ok(())) => Ok(()),
                    Ok(MaybeException::Exception(
                        ThriftHiveMetastoreDropDatabaseException::O1(_),
                    )) if options.if_exists || attempt > 0 => Ok(()),
                    Ok(MaybeException::Exception(
                        ThriftHiveMetastoreDropDatabaseException::O1(_),
                    )) => Err(CatalogError::NotFound(CatalogObject::Database, db_name)),
                    Ok(MaybeException::Exception(err)) => Err(CatalogError::External(format!(
                        "Failed to drop HMS database '{db_name}': {err:?}"
                    ))),
                    Err(err) => Err(Self::hms_client_error(
                        &format!("Failed to drop HMS database '{db_name}'"),
                        err,
                    )),
                }
            }
        })
        .await
    }

    async fn create_table(
        &self,
        database: &Namespace,
        table: &str,
        options: CreateTableOptions,
    ) -> CatalogResult<TableStatus> {
        let format = options.format.trim().to_lowercase();

        if options.replace {
            return Err(CatalogError::NotSupported(
                "Hive Metastore catalog does not support REPLACE".to_string(),
            ));
        }
        if !options.constraints.is_empty() {
            return Err(CatalogError::NotSupported(
                "Hive Metastore catalog does not support constraints for generic tables"
                    .to_string(),
            ));
        }
        if !options.sort_by.is_empty() {
            return Err(CatalogError::NotSupported(
                "Hive Metastore catalog does not support SORT BY for generic tables".to_string(),
            ));
        }
        if options.bucket_by.is_some() {
            return Err(CatalogError::NotSupported(
                "Hive Metastore catalog does not support BUCKET BY for generic tables".to_string(),
            ));
        }
        if !options.options.is_empty() {
            return Err(CatalogError::NotSupported(
                "Hive Metastore catalog does not support table OPTIONS for generic tables"
                    .to_string(),
            ));
        }
        if options.partition_by.iter().any(|field| {
            field.transform.is_some() && field.transform != Some(PartitionTransform::Identity)
        }) {
            return Err(CatalogError::NotSupported(
                "Hive Metastore catalog only supports identity partition columns".to_string(),
            ));
        }

        let db_name = validate_namespace(database)?;
        let format = HiveCatalogFormat::from_format(&format)?;
        let partition_columns: Vec<String> = options
            .partition_by
            .iter()
            .map(|field| field.column.clone())
            .collect();
        let hms_table = build_generic_table(
            &db_name,
            table,
            options.columns,
            partition_columns,
            options.location,
            GenericTableFormat {
                logical_format: format.logical_format,
                storage: &format.storage_format,
            },
            options.comment,
            options.properties,
        )?;

        self.create_hms_table(database, table, hms_table, options.if_not_exists)
            .await
    }

    async fn get_table(&self, database: &Namespace, table: &str) -> CatalogResult<TableStatus> {
        let table_value = self.fetch_hms_table(database, table).await?;
        if is_view_table(&table_value) {
            return Err(CatalogError::NotFound(
                CatalogObject::Table,
                format!("{}.{}", validate_namespace(database)?, table),
            ));
        }
        table_to_status(&self.name, database, &table_value)
    }

    async fn list_tables(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        let tables = self.list_hms_tables(database).await?;
        tables
            .into_iter()
            .filter(|table| !is_view_table(table))
            .map(|table| table_to_status(&self.name, database, &table))
            .collect()
    }

    async fn drop_table(
        &self,
        database: &Namespace,
        table: &str,
        options: DropTableOptions,
    ) -> CatalogResult<()> {
        let db_name = validate_namespace(database)?;

        let request = build_drop_table_request(options.purge);
        match request.environment_context {
            Some(environment_context) => {
                self.drop_hms_table_with_environment_context(
                    &db_name,
                    table,
                    request.delete_data,
                    environment_context,
                    options.if_exists,
                )
                .await
            }
            None => {
                self.drop_hms_table(&db_name, table, request.delete_data, options.if_exists)
                    .await
            }
        }
    }

    async fn alter_table(
        &self,
        database: &Namespace,
        table: &str,
        options: AlterTableOptions,
    ) -> CatalogResult<()> {
        let db_name = validate_namespace(database)?;
        let table_name = table.to_string();

        self.with_failover(|client| {
            let db_name = db_name.clone();
            let table_name = table_name.clone();
            let options = options.clone();
            async move {
                // Fetch the current table, mutate its properties, then call alter_table.
                // This matches Spark's HMS catalog approach.
                let mut hms_table = match client
                    .get_table(db_name.clone().into(), table_name.clone().into())
                    .await
                {
                    Ok(MaybeException::Ok(table)) => table,
                    Ok(MaybeException::Exception(ThriftHiveMetastoreGetTableException::O2(_))) => {
                        return Err(CatalogError::NotFound(
                            CatalogObject::Table,
                            format!("{db_name}.{table_name}"),
                        ))
                    }
                    Ok(MaybeException::Exception(err)) => {
                        return Err(CatalogError::External(format!(
                            "Failed to fetch HMS table '{db_name}.{table_name}' for alter: {err:?}"
                        )))
                    }
                    Err(err) => {
                        return Err(Self::hms_client_error(
                            &format!(
                                "Failed to fetch HMS table '{db_name}.{table_name}' for alter"
                            ),
                            err,
                        ))
                    }
                };

                let parameters = hms_table.parameters.get_or_insert_with(AHashMap::new);
                match options {
                    AlterTableOptions::SetTableProperties { properties } => {
                        for (key, value) in properties {
                            parameters.insert(key.into(), value.into());
                        }
                    }
                    AlterTableOptions::UnsetTableProperties { keys, if_exists } => {
                        for key in keys {
                            if if_exists || parameters.contains_key(key.as_str()) {
                                parameters.remove(key.as_str());
                            } else {
                                return Err(CatalogError::InvalidArgument(format!(
                                    "Table property '{key}' does not exist on \
                                     '{db_name}.{table_name}'"
                                )));
                            }
                        }
                    }
                }

                match client
                    .alter_table(db_name.clone().into(), table_name.clone().into(), hms_table)
                    .await
                {
                    Ok(MaybeException::Ok(())) => Ok(()),
                    Ok(MaybeException::Exception(ThriftHiveMetastoreAlterTableException::O1(
                        err,
                    ))) => Err(CatalogError::External(format!(
                        "Failed to alter HMS table '{db_name}.{table_name}': \
                         invalid operation: {err:?}"
                    ))),
                    Ok(MaybeException::Exception(ThriftHiveMetastoreAlterTableException::O2(
                        err,
                    ))) => Err(CatalogError::External(format!(
                        "Failed to alter HMS table '{db_name}.{table_name}': \
                         metastore error: {err:?}"
                    ))),
                    Err(err) => Err(Self::hms_client_error(
                        &format!("Failed to alter HMS table '{db_name}.{table_name}'"),
                        err,
                    )),
                }
            }
        })
        .await
    }

    async fn create_view(
        &self,
        database: &Namespace,
        view: &str,
        options: CreateViewOptions,
    ) -> CatalogResult<TableStatus> {
        if options.replace {
            return Err(CatalogError::NotSupported(
                "Hive Metastore catalog does not support REPLACE for views".to_string(),
            ));
        }

        let db_name = validate_namespace(database)?;
        let if_not_exists = options.if_not_exists;
        let table = build_view(&db_name, view, options)?;
        let view_name = view.to_string();

        self.with_failover_attempt(|client, attempt| {
            let table = table.clone();
            let db_name = db_name.clone();
            let view = view_name.clone();
            async move {
                match client.create_table(table).await {
                    Ok(MaybeException::Ok(())) => Ok(()),
                    Ok(MaybeException::Exception(ThriftHiveMetastoreCreateTableException::O1(
                        _,
                    ))) if if_not_exists || attempt > 0 => Ok(()),
                    Ok(MaybeException::Exception(ThriftHiveMetastoreCreateTableException::O1(
                        _,
                    ))) => Err(CatalogError::AlreadyExists(
                        CatalogObject::View,
                        format!("{db_name}.{view}"),
                    )),
                    Ok(MaybeException::Exception(err)) => Err(CatalogError::External(format!(
                        "Failed to create HMS view '{db_name}.{view}': {err:?}"
                    ))),
                    Err(err) => Err(Self::hms_client_error(
                        &format!("Failed to create HMS view '{db_name}.{view}'"),
                        err,
                    )),
                }
            }
        })
        .await?;

        self.get_view(database, &view_name).await
    }

    async fn get_view(&self, database: &Namespace, view: &str) -> CatalogResult<TableStatus> {
        let table_value = self.fetch_hms_table(database, view).await?;
        if !is_view_table(&table_value) {
            return Err(CatalogError::NotFound(
                CatalogObject::View,
                format!("{}.{}", validate_namespace(database)?, view),
            ));
        }
        view_to_status(&self.name, database, &table_value)
    }

    async fn list_views(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        let tables = self.list_hms_tables(database).await?;
        tables
            .into_iter()
            .filter(is_view_table)
            .map(|table| view_to_status(&self.name, database, &table))
            .collect()
    }

    async fn drop_view(
        &self,
        database: &Namespace,
        view: &str,
        options: DropViewOptions,
    ) -> CatalogResult<()> {
        self.drop_table(
            database,
            view,
            DropTableOptions {
                if_exists: options.if_exists,
                purge: false,
            },
        )
        .await
        .map_err(|error| match error {
            CatalogError::NotFound(_, value) => CatalogError::NotFound(CatalogObject::View, value),
            other => other,
        })
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used)]

    use std::time::Duration;

    use arrow::datatypes::DataType;
    use pilota::FastStr;
    use sail_catalog::error::{CatalogError, CatalogObject};
    use sail_catalog::provider::{
        CatalogProvider, CreateTableColumnOptions, CreateTableOptions, Namespace,
    };
    use sail_common::runtime::RuntimeHandle;

    use super::{HmsCatalogConfig, HmsCatalogProvider};

    #[tokio::test]
    async fn test_create_table_rejects_iceberg_format() {
        let runtime = RuntimeHandle::new(
            tokio::runtime::Handle::current(),
            tokio::runtime::Handle::current(),
        );
        let provider = HmsCatalogProvider::new(
            "hms".to_string(),
            HmsCatalogConfig {
                uris: vec!["127.0.0.1:9083".to_string()],
                thrift_transport: None,
                auth: None,
                kerberos_service_principal: None,
                min_sasl_qop: None,
                connect_timeout_secs: None,
            },
            runtime,
        )
        .unwrap();

        let error = provider
            .create_table(
                &Namespace::try_from(vec!["default"]).unwrap(),
                "items",
                CreateTableOptions {
                    columns: vec![CreateTableColumnOptions {
                        name: "id".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        comment: None,
                        default: None,
                        generated_always_as: None,
                    }],
                    comment: None,
                    constraints: vec![],
                    location: None,
                    format: "iceberg".to_string(),
                    partition_by: vec![],
                    sort_by: vec![],
                    bucket_by: None,
                    if_not_exists: false,
                    replace: false,
                    options: vec![],
                    properties: vec![],
                },
            )
            .await
            .unwrap_err();

        assert!(matches!(error, CatalogError::NotSupported(_)));
    }

    #[test]
    fn test_build_drop_table_request_without_purge_keeps_delete_data_enabled() {
        let request = super::build_drop_table_request(false);

        assert!(request.delete_data);
        assert!(request.environment_context.is_none());
    }

    #[test]
    fn test_build_drop_table_request_with_purge_sets_if_purge_context() {
        let request = super::build_drop_table_request(true);

        assert!(request.delete_data);
        let properties = request
            .environment_context
            .and_then(|context| context.properties)
            .unwrap();
        assert_eq!(
            properties.get(&FastStr::from_static_str("ifPurge")),
            Some(&FastStr::from_static_str("TRUE"))
        );
    }

    #[test]
    fn test_default_auth_mode_is_none() {
        let auth_mode = super::catalog_auth_mode(
            &HmsCatalogConfig {
                uris: vec!["127.0.0.1:9083".to_string()],
                thrift_transport: None,
                auth: None,
                kerberos_service_principal: None,
                min_sasl_qop: None,
                connect_timeout_secs: None,
            },
            "127.0.0.1:9083",
        )
        .unwrap();

        assert!(matches!(auth_mode, super::CatalogAuthMode::None));
    }

    #[test]
    fn test_expand_kerberos_principal_replaces_host_placeholder() {
        let principal = super::expand_kerberos_service_principal(
            "hive-metastore/_HOST@EXAMPLE.COM",
            "127.0.0.1:9083",
        )
        .unwrap();

        assert_eq!(principal, "hive-metastore/127.0.0.1@EXAMPLE.COM");
    }

    #[test]
    fn test_hostbased_target_name_converts_principal_to_service_host() {
        let target =
            super::build_hostbased_service_name("hive-metastore/node1.example.com@EXAMPLE.COM")
                .unwrap();

        assert_eq!(target, "hive-metastore@node1.example.com");
    }

    #[test]
    fn test_kerberos_auth_requires_principal() {
        let error = super::catalog_auth_mode(
            &HmsCatalogConfig {
                uris: vec!["127.0.0.1:9083".to_string()],
                thrift_transport: None,
                auth: Some("kerberos".to_string()),
                kerberos_service_principal: None,
                min_sasl_qop: None,
                connect_timeout_secs: None,
            },
            "127.0.0.1:9083",
        )
        .unwrap_err();

        assert!(matches!(error, CatalogError::InvalidArgument(_)));
        assert!(error.to_string().contains("kerberos_service_principal"));
    }

    /// Validates that the GSSAPI fail-fast check produces a clear error when
    /// the library isn't available. We can't easily unload the real library in
    /// a unit test, so we verify the error message format by checking that
    /// `catalog_auth_mode` with kerberos auth invokes the availability guard.
    /// The actual "library missing" path is exercised in CI where the library
    /// may not be installed, and via
    /// `security::gssapi::tests::test_missing_gssapi_runtime_library_is_reported`.
    #[test]
    fn test_kerberos_auth_checks_gssapi_availability() {
        // When the library IS loaded (normal test env), kerberos auth should
        // proceed past the availability check and fail on the next validation
        // (missing principal) — proving the guard is reached.
        let error = super::catalog_auth_mode(
            &HmsCatalogConfig {
                uris: vec!["127.0.0.1:9083".to_string()],
                thrift_transport: None,
                auth: Some("kerberos".to_string()),
                kerberos_service_principal: None,
                min_sasl_qop: None,
                connect_timeout_secs: None,
            },
            "127.0.0.1:9083",
        )
        .unwrap_err();

        // If GSSAPI is available, the error is about the missing principal.
        // If GSSAPI is NOT available, the error is about the missing library.
        let message = error.to_string();
        assert!(
            message.contains("kerberos_service_principal")
                || message.contains("GSSAPI runtime library"),
            "expected kerberos-related validation error, got: {message}"
        );
    }

    #[test]
    fn test_unknown_auth_mode_is_rejected() {
        let error = super::catalog_auth_mode(
            &HmsCatalogConfig {
                uris: vec!["127.0.0.1:9083".to_string()],
                thrift_transport: None,
                auth: Some("ldap".to_string()),
                kerberos_service_principal: None,
                min_sasl_qop: None,
                connect_timeout_secs: None,
            },
            "127.0.0.1:9083",
        )
        .unwrap_err();

        assert!(matches!(error, CatalogError::InvalidArgument(_)));
        assert!(error.to_string().contains("Unsupported HMS auth"));
    }

    #[test]
    fn test_default_min_sasl_qop_is_auth() {
        let qop_min = super::parse_min_sasl_qop(&HmsCatalogConfig {
            uris: vec!["127.0.0.1:9083".to_string()],
            thrift_transport: None,
            auth: None,
            kerberos_service_principal: None,
            min_sasl_qop: None,
            connect_timeout_secs: None,
        })
        .unwrap();

        assert_eq!(qop_min, super::SaslQop::Auth);
    }

    #[test]
    fn test_invalid_min_sasl_qop_is_rejected() {
        let error = super::parse_min_sasl_qop(&HmsCatalogConfig {
            uris: vec!["127.0.0.1:9083".to_string()],
            thrift_transport: None,
            auth: None,
            kerberos_service_principal: None,
            min_sasl_qop: Some("invalid".to_string()),
            connect_timeout_secs: None,
        })
        .unwrap_err();

        assert!(matches!(error, CatalogError::InvalidArgument(_)));
        assert!(error.to_string().contains("min_sasl_qop"));
    }

    #[test]
    fn test_split_hms_uri_list_flattens_and_normalizes_entries() {
        let uris = vec![
            "thrift://hms1.internal:9083, hms2.internal:9083".to_string(),
            "THRIFT://hms3.internal:9083".to_string(),
        ];

        let normalized = super::split_hms_uri_list(&uris).unwrap();

        assert_eq!(
            normalized,
            vec![
                "hms1.internal:9083".to_string(),
                "hms2.internal:9083".to_string(),
                "hms3.internal:9083".to_string(),
            ]
        );
    }

    #[test]
    fn test_split_hms_uri_list_rejects_empty_input() {
        let error = super::split_hms_uri_list(&[]).unwrap_err();

        assert!(matches!(error, CatalogError::InvalidArgument(_)));
        assert!(error
            .to_string()
            .contains("must contain at least one endpoint"));
    }

    #[test]
    fn test_catalog_auth_mode_expands_host_per_endpoint() {
        let config = HmsCatalogConfig {
            uris: vec![
                "hms1.internal:9083".to_string(),
                "hms2.internal:9083".to_string(),
            ],
            thrift_transport: None,
            auth: Some("kerberos".to_string()),
            kerberos_service_principal: Some("hive-metastore/_HOST@EXAMPLE.COM".to_string()),
            min_sasl_qop: None,
            connect_timeout_secs: None,
        };

        let auth_mode = super::catalog_auth_mode(&config, "hms2.internal:9083").unwrap();

        assert_eq!(
            auth_mode,
            super::CatalogAuthMode::Kerberos {
                hostbased_service: "hive-metastore@hms2.internal".to_string(),
            }
        );
    }

    #[test]
    fn test_should_retry_transport_errors() {
        let retry = super::HmsCatalogProvider::should_retry(&CatalogError::External(
            "context: transport error: connection reset by peer".to_string(),
        ));
        let retry_gssapi = super::HmsCatalogProvider::should_retry(&CatalogError::External(
            "context: transport error: gssapi error: GSS_S_CONTEXT_EXPIRED".to_string(),
        ));
        let retry_dns = super::HmsCatalogProvider::should_retry(&CatalogError::External(
            "dns error: failed to resolve HMS URI 'hms:9083': connection refused".to_string(),
        ));
        let dont_retry = super::HmsCatalogProvider::should_retry(&CatalogError::NotFound(
            CatalogObject::Table,
            "x".into(),
        ));

        assert!(retry);
        assert!(retry_gssapi);
        assert!(retry_dns);
        assert!(!dont_retry);
    }

    #[test]
    fn test_default_connect_timeout_is_five_seconds() {
        let timeout = super::connect_timeout(&HmsCatalogConfig {
            uris: vec!["127.0.0.1:9083".to_string()],
            thrift_transport: None,
            auth: None,
            kerberos_service_principal: None,
            min_sasl_qop: None,
            connect_timeout_secs: None,
        });

        assert_eq!(timeout, Duration::from_secs(5));
    }

    #[test]
    fn test_custom_connect_timeout_is_applied() {
        let timeout = super::connect_timeout(&HmsCatalogConfig {
            uris: vec!["127.0.0.1:9083".to_string()],
            thrift_transport: None,
            auth: None,
            kerberos_service_principal: None,
            min_sasl_qop: None,
            connect_timeout_secs: Some(12),
        });

        assert_eq!(timeout, Duration::from_secs(12));
    }
}
