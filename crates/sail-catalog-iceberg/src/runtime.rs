use std::sync::Arc;

use async_trait::async_trait;
use tokio::runtime::Handle;

use crate::apis::catalog_api_api::{
    CancelPlanningError, CommitTransactionError, CreateNamespaceError, CreateTableError,
    CreateViewError, DropNamespaceError, DropTableError, DropViewError, FetchPlanningResultError,
    FetchScanTasksError, ListNamespacesError, ListTablesError, ListViewsError,
    LoadCredentialsError, LoadNamespaceMetadataError, LoadTableError, LoadViewError,
    NamespaceExistsError, PlanTableScanError, RegisterTableError, RenameTableError,
    RenameViewError, ReplaceViewError, ReportMetricsError, TableExistsError, UpdatePropertiesError,
    UpdateTableError, ViewExistsError,
};
use crate::apis::configuration_api_api::GetConfigError;
use crate::apis::o_auth2_api_api::GetTokenError;
use crate::apis::{self, Api, ApiClient, Error};
use crate::models;

pub struct RuntimeAwareApiClient {
    inner: Arc<ApiClient>,
    handle: Handle,
    catalog_api: RuntimeAwareCatalogApiApi,
    configuration_api: RuntimeAwareConfigurationApiApi,
    oauth2_api: RuntimeAwareOAuth2ApiApi,
}

impl RuntimeAwareApiClient {
    pub fn try_new<F>(initializer: F, handle: Handle) -> Result<Self, std::io::Error>
    where
        F: FnOnce() -> Result<ApiClient, std::io::Error>,
    {
        let _guard = handle.enter();
        let inner = Arc::new(initializer()?);
        Ok(Self {
            catalog_api: RuntimeAwareCatalogApiApi {
                inner: inner.clone(),
                handle: handle.clone(),
            },
            configuration_api: RuntimeAwareConfigurationApiApi {
                inner: inner.clone(),
                handle: handle.clone(),
            },
            oauth2_api: RuntimeAwareOAuth2ApiApi {
                inner: inner.clone(),
                handle: handle.clone(),
            },
            inner,
            handle,
        })
    }
}

impl Api for RuntimeAwareApiClient {
    fn catalog_api_api(&self) -> &dyn apis::catalog_api_api::CatalogApiApi {
        &self.catalog_api
    }

    fn configuration_api_api(&self) -> &dyn apis::configuration_api_api::ConfigurationApiApi {
        &self.configuration_api
    }

    fn o_auth2_api_api(&self) -> &dyn apis::o_auth2_api_api::OAuth2ApiApi {
        &self.oauth2_api
    }
}

#[derive(Clone)]
struct RuntimeAwareCatalogApiApi {
    inner: Arc<ApiClient>,
    handle: Handle,
}

#[async_trait]
impl apis::catalog_api_api::CatalogApiApi for RuntimeAwareCatalogApiApi {
    async fn cancel_planning<'namespace, 'table, 'plan_id, 'prefix>(
        &self,
        namespace: &'namespace str,
        table: &'table str,
        plan_id: &'plan_id str,
        prefix: Option<&'prefix str>,
    ) -> Result<(), Error<CancelPlanningError>> {
        let inner = self.inner.clone();
        let namespace = namespace.to_string();
        let table = table.to_string();
        let plan_id = plan_id.to_string();
        let prefix = prefix.map(|s| s.to_string());
        self.handle
            .spawn(async move {
                inner
                    .catalog_api_api()
                    .cancel_planning(&namespace, &table, &plan_id, prefix.as_deref())
                    .await
            })
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))?
    }

    async fn commit_transaction<'commit_transaction_request, 'prefix>(
        &self,
        commit_transaction_request: models::CommitTransactionRequest,
        prefix: Option<&'prefix str>,
    ) -> Result<(), Error<CommitTransactionError>> {
        let inner = self.inner.clone();
        let prefix = prefix.map(|s| s.to_string());
        self.handle
            .spawn(async move {
                inner
                    .catalog_api_api()
                    .commit_transaction(commit_transaction_request, prefix.as_deref())
                    .await
            })
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))?
    }

    async fn create_namespace<'create_namespace_request, 'prefix>(
        &self,
        create_namespace_request: models::CreateNamespaceRequest,
        prefix: Option<&'prefix str>,
    ) -> Result<models::CreateNamespaceResponse, Error<CreateNamespaceError>> {
        let inner = self.inner.clone();
        let prefix = prefix.map(|s| s.to_string());
        self.handle
            .spawn(async move {
                inner
                    .catalog_api_api()
                    .create_namespace(create_namespace_request, prefix.as_deref())
                    .await
            })
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))?
    }

    async fn create_table<
        'namespace,
        'create_table_request,
        'x_iceberg_access_delegation,
        'prefix,
    >(
        &self,
        namespace: &'namespace str,
        create_table_request: models::CreateTableRequest,
        x_iceberg_access_delegation: Option<&'x_iceberg_access_delegation str>,
        prefix: Option<&'prefix str>,
    ) -> Result<models::LoadTableResult, Error<CreateTableError>> {
        let inner = self.inner.clone();
        let namespace = namespace.to_string();
        let x_iceberg_access_delegation = x_iceberg_access_delegation.map(|s| s.to_string());
        let prefix = prefix.map(|s| s.to_string());
        self.handle
            .spawn(async move {
                inner
                    .catalog_api_api()
                    .create_table(
                        &namespace,
                        create_table_request,
                        x_iceberg_access_delegation.as_deref(),
                        prefix.as_deref(),
                    )
                    .await
            })
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))?
    }

    async fn create_view<'namespace, 'create_view_request, 'prefix>(
        &self,
        namespace: &'namespace str,
        create_view_request: models::CreateViewRequest,
        prefix: Option<&'prefix str>,
    ) -> Result<models::LoadViewResult, Error<CreateViewError>> {
        let inner = self.inner.clone();
        let namespace = namespace.to_string();
        let prefix = prefix.map(|s| s.to_string());
        self.handle
            .spawn(async move {
                inner
                    .catalog_api_api()
                    .create_view(&namespace, create_view_request, prefix.as_deref())
                    .await
            })
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))?
    }

    async fn drop_namespace<'namespace, 'prefix>(
        &self,
        namespace: &'namespace str,
        prefix: Option<&'prefix str>,
    ) -> Result<(), Error<DropNamespaceError>> {
        let inner = self.inner.clone();
        let namespace = namespace.to_string();
        let prefix = prefix.map(|s| s.to_string());
        self.handle
            .spawn(async move {
                inner
                    .catalog_api_api()
                    .drop_namespace(&namespace, prefix.as_deref())
                    .await
            })
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))?
    }

    async fn drop_table<'namespace, 'table, 'purge_requested, 'prefix>(
        &self,
        namespace: &'namespace str,
        table: &'table str,
        purge_requested: Option<bool>,
        prefix: Option<&'prefix str>,
    ) -> Result<(), Error<DropTableError>> {
        let inner = self.inner.clone();
        let namespace = namespace.to_string();
        let table = table.to_string();
        let prefix = prefix.map(|s| s.to_string());
        self.handle
            .spawn(async move {
                inner
                    .catalog_api_api()
                    .drop_table(&namespace, &table, purge_requested, prefix.as_deref())
                    .await
            })
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))?
    }

    async fn drop_view<'namespace, 'view, 'prefix>(
        &self,
        namespace: &'namespace str,
        view: &'view str,
        prefix: Option<&'prefix str>,
    ) -> Result<(), Error<DropViewError>> {
        let inner = self.inner.clone();
        let namespace = namespace.to_string();
        let view = view.to_string();
        let prefix = prefix.map(|s| s.to_string());
        self.handle
            .spawn(async move {
                inner
                    .catalog_api_api()
                    .drop_view(&namespace, &view, prefix.as_deref())
                    .await
            })
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))?
    }

    async fn fetch_planning_result<'namespace, 'table, 'plan_id, 'prefix>(
        &self,
        namespace: &'namespace str,
        table: &'table str,
        plan_id: &'plan_id str,
        prefix: Option<&'prefix str>,
    ) -> Result<models::FetchPlanningResult, Error<FetchPlanningResultError>> {
        let inner = self.inner.clone();
        let namespace = namespace.to_string();
        let table = table.to_string();
        let plan_id = plan_id.to_string();
        let prefix = prefix.map(|s| s.to_string());
        self.handle
            .spawn(async move {
                inner
                    .catalog_api_api()
                    .fetch_planning_result(&namespace, &table, &plan_id, prefix.as_deref())
                    .await
            })
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))?
    }

    async fn fetch_scan_tasks<'namespace, 'table, 'prefix, 'fetch_scan_tasks_request>(
        &self,
        namespace: &'namespace str,
        table: &'table str,
        prefix: Option<&'prefix str>,
        fetch_scan_tasks_request: Option<models::FetchScanTasksRequest>,
    ) -> Result<models::FetchScanTasksResult, Error<FetchScanTasksError>> {
        let inner = self.inner.clone();
        let namespace = namespace.to_string();
        let table = table.to_string();
        let prefix = prefix.map(|s| s.to_string());
        self.handle
            .spawn(async move {
                inner
                    .catalog_api_api()
                    .fetch_scan_tasks(
                        &namespace,
                        &table,
                        prefix.as_deref(),
                        fetch_scan_tasks_request,
                    )
                    .await
            })
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))?
    }

    async fn list_namespaces<'page_token, 'page_size, 'parent, 'prefix>(
        &self,
        page_token: Option<&'page_token str>,
        page_size: Option<i32>,
        parent: Option<&'parent str>,
        prefix: Option<&'prefix str>,
    ) -> Result<models::ListNamespacesResponse, Error<ListNamespacesError>> {
        let inner = self.inner.clone();
        let page_token = page_token.map(|s| s.to_string());
        let parent = parent.map(|s| s.to_string());
        let prefix = prefix.map(|s| s.to_string());
        self.handle
            .spawn(async move {
                inner
                    .catalog_api_api()
                    .list_namespaces(
                        page_token.as_deref(),
                        page_size,
                        parent.as_deref(),
                        prefix.as_deref(),
                    )
                    .await
            })
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))?
    }

    async fn list_tables<'namespace, 'page_token, 'page_size, 'prefix>(
        &self,
        namespace: &'namespace str,
        page_token: Option<&'page_token str>,
        page_size: Option<i32>,
        prefix: Option<&'prefix str>,
    ) -> Result<models::ListTablesResponse, Error<ListTablesError>> {
        let inner = self.inner.clone();
        let namespace = namespace.to_string();
        let page_token = page_token.map(|s| s.to_string());
        let prefix = prefix.map(|s| s.to_string());
        self.handle
            .spawn(async move {
                inner
                    .catalog_api_api()
                    .list_tables(
                        &namespace,
                        page_token.as_deref(),
                        page_size,
                        prefix.as_deref(),
                    )
                    .await
            })
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))?
    }

    async fn list_views<'namespace, 'page_token, 'page_size, 'prefix>(
        &self,
        namespace: &'namespace str,
        page_token: Option<&'page_token str>,
        page_size: Option<i32>,
        prefix: Option<&'prefix str>,
    ) -> Result<models::ListTablesResponse, Error<ListViewsError>> {
        let inner = self.inner.clone();
        let namespace = namespace.to_string();
        let page_token = page_token.map(|s| s.to_string());
        let prefix = prefix.map(|s| s.to_string());
        self.handle
            .spawn(async move {
                inner
                    .catalog_api_api()
                    .list_views(
                        &namespace,
                        page_token.as_deref(),
                        page_size,
                        prefix.as_deref(),
                    )
                    .await
            })
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))?
    }

    async fn load_credentials<'namespace, 'table, 'prefix>(
        &self,
        namespace: &'namespace str,
        table: &'table str,
        prefix: Option<&'prefix str>,
    ) -> Result<models::LoadCredentialsResponse, Error<LoadCredentialsError>> {
        let inner = self.inner.clone();
        let namespace = namespace.to_string();
        let table = table.to_string();
        let prefix = prefix.map(|s| s.to_string());
        self.handle
            .spawn(async move {
                inner
                    .catalog_api_api()
                    .load_credentials(&namespace, &table, prefix.as_deref())
                    .await
            })
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))?
    }

    async fn load_namespace_metadata<'namespace, 'prefix>(
        &self,
        namespace: &'namespace str,
        prefix: Option<&'prefix str>,
    ) -> Result<models::GetNamespaceResponse, Error<LoadNamespaceMetadataError>> {
        let inner = self.inner.clone();
        let namespace = namespace.to_string();
        let prefix = prefix.map(|s| s.to_string());
        self.handle
            .spawn(async move {
                inner
                    .catalog_api_api()
                    .load_namespace_metadata(&namespace, prefix.as_deref())
                    .await
            })
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))?
    }

    async fn load_table<
        'namespace,
        'table,
        'x_iceberg_access_delegation,
        'if_none_match,
        'snapshots,
        'prefix,
    >(
        &self,
        namespace: &'namespace str,
        table: &'table str,
        x_iceberg_access_delegation: Option<&'x_iceberg_access_delegation str>,
        if_none_match: Option<&'if_none_match str>,
        snapshots: Option<&'snapshots str>,
        prefix: Option<&'prefix str>,
    ) -> Result<models::LoadTableResult, Error<LoadTableError>> {
        let inner = self.inner.clone();
        let namespace = namespace.to_string();
        let table = table.to_string();
        let x_iceberg_access_delegation = x_iceberg_access_delegation.map(|s| s.to_string());
        let if_none_match = if_none_match.map(|s| s.to_string());
        let snapshots = snapshots.map(|s| s.to_string());
        let prefix = prefix.map(|s| s.to_string());
        self.handle
            .spawn(async move {
                inner
                    .catalog_api_api()
                    .load_table(
                        &namespace,
                        &table,
                        x_iceberg_access_delegation.as_deref(),
                        if_none_match.as_deref(),
                        snapshots.as_deref(),
                        prefix.as_deref(),
                    )
                    .await
            })
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))?
    }

    async fn load_view<'namespace, 'view, 'prefix>(
        &self,
        namespace: &'namespace str,
        view: &'view str,
        prefix: Option<&'prefix str>,
    ) -> Result<models::LoadViewResult, Error<LoadViewError>> {
        let inner = self.inner.clone();
        let namespace = namespace.to_string();
        let view = view.to_string();
        let prefix = prefix.map(|s| s.to_string());
        self.handle
            .spawn(async move {
                inner
                    .catalog_api_api()
                    .load_view(&namespace, &view, prefix.as_deref())
                    .await
            })
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))?
    }

    async fn namespace_exists<'namespace, 'prefix>(
        &self,
        namespace: &'namespace str,
        prefix: Option<&'prefix str>,
    ) -> Result<(), Error<NamespaceExistsError>> {
        let inner = self.inner.clone();
        let namespace = namespace.to_string();
        let prefix = prefix.map(|s| s.to_string());
        self.handle
            .spawn(async move {
                inner
                    .catalog_api_api()
                    .namespace_exists(&namespace, prefix.as_deref())
                    .await
            })
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))?
    }

    async fn plan_table_scan<'namespace, 'table, 'prefix, 'plan_table_scan_request>(
        &self,
        namespace: &'namespace str,
        table: &'table str,
        prefix: Option<&'prefix str>,
        plan_table_scan_request: Option<models::PlanTableScanRequest>,
    ) -> Result<models::PlanTableScanResult, Error<PlanTableScanError>> {
        let inner = self.inner.clone();
        let namespace = namespace.to_string();
        let table = table.to_string();
        let prefix = prefix.map(|s| s.to_string());
        self.handle
            .spawn(async move {
                inner
                    .catalog_api_api()
                    .plan_table_scan(
                        &namespace,
                        &table,
                        prefix.as_deref(),
                        plan_table_scan_request,
                    )
                    .await
            })
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))?
    }

    async fn register_table<'namespace, 'register_table_request, 'prefix>(
        &self,
        namespace: &'namespace str,
        register_table_request: models::RegisterTableRequest,
        prefix: Option<&'prefix str>,
    ) -> Result<models::LoadTableResult, Error<RegisterTableError>> {
        let inner = self.inner.clone();
        let namespace = namespace.to_string();
        let prefix = prefix.map(|s| s.to_string());
        self.handle
            .spawn(async move {
                inner
                    .catalog_api_api()
                    .register_table(&namespace, register_table_request, prefix.as_deref())
                    .await
            })
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))?
    }

    async fn rename_table<'rename_table_request, 'prefix>(
        &self,
        rename_table_request: models::RenameTableRequest,
        prefix: Option<&'prefix str>,
    ) -> Result<(), Error<RenameTableError>> {
        let inner = self.inner.clone();
        let prefix = prefix.map(|s| s.to_string());
        self.handle
            .spawn(async move {
                inner
                    .catalog_api_api()
                    .rename_table(rename_table_request, prefix.as_deref())
                    .await
            })
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))?
    }

    async fn rename_view<'rename_table_request, 'prefix>(
        &self,
        rename_table_request: models::RenameTableRequest,
        prefix: Option<&'prefix str>,
    ) -> Result<(), Error<RenameViewError>> {
        let inner = self.inner.clone();
        let prefix = prefix.map(|s| s.to_string());
        self.handle
            .spawn(async move {
                inner
                    .catalog_api_api()
                    .rename_view(rename_table_request, prefix.as_deref())
                    .await
            })
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))?
    }

    async fn replace_view<'namespace, 'view, 'commit_view_request, 'prefix>(
        &self,
        namespace: &'namespace str,
        view: &'view str,
        commit_view_request: models::CommitViewRequest,
        prefix: Option<&'prefix str>,
    ) -> Result<models::LoadViewResult, Error<ReplaceViewError>> {
        let inner = self.inner.clone();
        let namespace = namespace.to_string();
        let view = view.to_string();
        let prefix = prefix.map(|s| s.to_string());
        self.handle
            .spawn(async move {
                inner
                    .catalog_api_api()
                    .replace_view(&namespace, &view, commit_view_request, prefix.as_deref())
                    .await
            })
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))?
    }

    async fn report_metrics<'namespace, 'table, 'report_metrics_request, 'prefix>(
        &self,
        namespace: &'namespace str,
        table: &'table str,
        report_metrics_request: models::ReportMetricsRequest,
        prefix: Option<&'prefix str>,
    ) -> Result<(), Error<ReportMetricsError>> {
        let inner = self.inner.clone();
        let namespace = namespace.to_string();
        let table = table.to_string();
        let prefix = prefix.map(|s| s.to_string());
        self.handle
            .spawn(async move {
                inner
                    .catalog_api_api()
                    .report_metrics(
                        &namespace,
                        &table,
                        report_metrics_request,
                        prefix.as_deref(),
                    )
                    .await
            })
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))?
    }

    async fn table_exists<'namespace, 'table, 'prefix>(
        &self,
        namespace: &'namespace str,
        table: &'table str,
        prefix: Option<&'prefix str>,
    ) -> Result<(), Error<TableExistsError>> {
        let inner = self.inner.clone();
        let namespace = namespace.to_string();
        let table = table.to_string();
        let prefix = prefix.map(|s| s.to_string());
        self.handle
            .spawn(async move {
                inner
                    .catalog_api_api()
                    .table_exists(&namespace, &table, prefix.as_deref())
                    .await
            })
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))?
    }

    async fn update_properties<'namespace, 'update_namespace_properties_request, 'prefix>(
        &self,
        namespace: &'namespace str,
        update_namespace_properties_request: models::UpdateNamespacePropertiesRequest,
        prefix: Option<&'prefix str>,
    ) -> Result<models::UpdateNamespacePropertiesResponse, Error<UpdatePropertiesError>> {
        let inner = self.inner.clone();
        let namespace = namespace.to_string();
        let prefix = prefix.map(|s| s.to_string());
        self.handle
            .spawn(async move {
                inner
                    .catalog_api_api()
                    .update_properties(
                        &namespace,
                        update_namespace_properties_request,
                        prefix.as_deref(),
                    )
                    .await
            })
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))?
    }

    async fn update_table<'namespace, 'table, 'commit_table_request, 'prefix>(
        &self,
        namespace: &'namespace str,
        table: &'table str,
        commit_table_request: models::CommitTableRequest,
        prefix: Option<&'prefix str>,
    ) -> Result<models::CommitTableResponse, Error<UpdateTableError>> {
        let inner = self.inner.clone();
        let namespace = namespace.to_string();
        let table = table.to_string();
        let prefix = prefix.map(|s| s.to_string());
        self.handle
            .spawn(async move {
                inner
                    .catalog_api_api()
                    .update_table(&namespace, &table, commit_table_request, prefix.as_deref())
                    .await
            })
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))?
    }

    async fn view_exists<'namespace, 'view, 'prefix>(
        &self,
        namespace: &'namespace str,
        view: &'view str,
        prefix: Option<&'prefix str>,
    ) -> Result<(), Error<ViewExistsError>> {
        let inner = self.inner.clone();
        let namespace = namespace.to_string();
        let view = view.to_string();
        let prefix = prefix.map(|s| s.to_string());
        self.handle
            .spawn(async move {
                inner
                    .catalog_api_api()
                    .view_exists(&namespace, &view, prefix.as_deref())
                    .await
            })
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))?
    }
}

#[derive(Clone)]
struct RuntimeAwareConfigurationApiApi {
    inner: Arc<ApiClient>,
    handle: Handle,
}

#[async_trait]
impl apis::configuration_api_api::ConfigurationApiApi for RuntimeAwareConfigurationApiApi {
    async fn get_config<'warehouse>(
        &self,
        warehouse: Option<&'warehouse str>,
    ) -> Result<models::CatalogConfig, Error<GetConfigError>> {
        let inner = self.inner.clone();
        let warehouse = warehouse.map(|s| s.to_string());
        self.handle
            .spawn(async move {
                inner
                    .configuration_api_api()
                    .get_config(warehouse.as_deref())
                    .await
            })
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))?
    }
}

#[derive(Clone)]
struct RuntimeAwareOAuth2ApiApi {
    inner: Arc<ApiClient>,
    handle: Handle,
}

#[async_trait]
impl apis::o_auth2_api_api::OAuth2ApiApi for RuntimeAwareOAuth2ApiApi {
    async fn get_token<
        'grant_type,
        'scope,
        'client_id,
        'client_secret,
        'requested_token_type,
        'subject_token,
        'subject_token_type,
        'actor_token,
        'actor_token_type,
    >(
        &self,
        grant_type: Option<&'grant_type str>,
        scope: Option<&'scope str>,
        client_id: Option<&'client_id str>,
        client_secret: Option<&'client_secret str>,
        requested_token_type: Option<models::TokenType>,
        subject_token: Option<&'subject_token str>,
        subject_token_type: Option<models::TokenType>,
        actor_token: Option<&'actor_token str>,
        actor_token_type: Option<models::TokenType>,
    ) -> Result<models::OAuthTokenResponse, Error<GetTokenError>> {
        let inner = self.inner.clone();
        let grant_type = grant_type.map(|s| s.to_string());
        let scope = scope.map(|s| s.to_string());
        let client_id = client_id.map(|s| s.to_string());
        let client_secret = client_secret.map(|s| s.to_string());
        let subject_token = subject_token.map(|s| s.to_string());
        let actor_token = actor_token.map(|s| s.to_string());
        self.handle
            .spawn(async move {
                inner
                    .o_auth2_api_api()
                    .get_token(
                        grant_type.as_deref(),
                        scope.as_deref(),
                        client_id.as_deref(),
                        client_secret.as_deref(),
                        requested_token_type,
                        subject_token.as_deref(),
                        subject_token_type,
                        actor_token.as_deref(),
                        actor_token_type,
                    )
                    .await
            })
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))?
    }
}
