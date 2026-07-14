use std::fs::{self, OpenOptions};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use datafusion::common::{Result, internal_datafusion_err, resources_datafusion_err};
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::SessionConfig;
use object_store::ObjectStoreScheme;
use sail_common::config::{AppConfig, ExecutionMode};
use sail_common::runtime::RuntimeHandle;
use sail_common_datafusion::catalog::display::DefaultCatalogDisplay;
use sail_common_datafusion::session::plan::PlanService;
use sail_plan::catalog::SparkCatalogObjectDisplay;
use sail_plan::formatter::SparkPlanFormatter;
use sail_server::actor::ActorSystem;
use sail_session::session_factory::{
    ServerSessionFactory, ServerSessionInfo, ServerSessionMutator, SessionFactory,
};
use sail_session::session_manager::{SessionManager, SessionManagerOptions};
use url::Url;

use crate::artifact::{
    SparkArtifactOptions, SparkArtifactProcessBudget, SparkArtifactRegistry,
    initialize_artifact_cleanup,
};
use crate::error::{SparkError, SparkResult};
use crate::session::{SparkSession, SparkSessionOptions};
use crate::session_admission::{SparkSessionProcessAdmission, resolve_session_process_admission};

#[derive(Clone)]
struct SparkSessionManagerConfig {
    config: Arc<AppConfig>,
    automatic_artifact_store: Option<Arc<tempfile::TempDir>>,
    artifact_process_budget: Arc<SparkArtifactProcessBudget>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SparkArtifactProcessLimits {
    max_artifacts: usize,
    max_bytes: usize,
    inline_max_bytes: usize,
}

impl SparkArtifactProcessLimits {
    fn from_config(config: &AppConfig) -> Self {
        Self {
            max_artifacts: config.spark.artifact_process_max_artifacts,
            max_bytes: config.spark.artifact_process_max_bytes,
            inline_max_bytes: config.spark.artifact_process_inline_max_bytes,
        }
    }
}

static SPARK_ARTIFACT_PROCESS_BUDGET: OnceLock<Arc<SparkArtifactProcessBudget>> = OnceLock::new();
static SPARK_SESSION_PROCESS_ADMISSION: OnceLock<Arc<SparkSessionProcessAdmission>> =
    OnceLock::new();
static ARTIFACT_CLEANUP_JOURNAL_ROOT: OnceLock<PathBuf> = OnceLock::new();

const DEFAULT_ARTIFACT_CLEANUP_JOURNAL_DIRECTORY: &str = "sail-artifact-cleanup-journals";

struct SparkSessionFactory<I> {
    session_factory: Box<dyn SessionFactory<I>>,
    session_process_admission: Arc<SparkSessionProcessAdmission>,
    _automatic_artifact_store: Option<Arc<tempfile::TempDir>>,
}

impl<I> SessionFactory<I> for SparkSessionFactory<I> {
    fn create(&mut self, info: I) -> Result<datafusion::prelude::SessionContext> {
        crate::artifact::ensure_artifact_cleanup_capacity()
            .map_err(|error| internal_datafusion_err!("{error}"))?;
        let reservation = self
            .session_process_admission
            .reserve()
            .map_err(|error| resources_datafusion_err!("{error}"))?;
        let context = self.session_factory.create(info)?;
        context
            .state_ref()
            .write()
            .config_mut()
            .set_extension(Arc::new(reservation));
        Ok(context)
    }
}

pub struct SparkSessionMutator {
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
    artifact_store_namespace: String,
    artifact_process_budget: Arc<SparkArtifactProcessBudget>,
}

impl ServerSessionMutator for SparkSessionMutator {
    fn mutate_config(
        &self,
        config: SessionConfig,
        info: &ServerSessionInfo,
    ) -> Result<SessionConfig> {
        let plan_service = PlanService::new(
            Box::new(DefaultCatalogDisplay::<SparkCatalogObjectDisplay>::default()),
            Box::new(SparkPlanFormatter),
        );
        let spark = SparkSession::try_new(
            info.session_id.clone(),
            info.user_id.clone(),
            SparkSessionOptions {
                execution_heartbeat_interval: Duration::from_secs(
                    self.config.spark.execution_heartbeat_interval_secs,
                ),
                local_relation_batch_of_chunks_max_bytes: self.config.spark.artifact_rpc_max_bytes,
            },
        )
        .map_err(|e| internal_datafusion_err!("{e}"))?;
        let artifacts = SparkArtifactRegistry::new(
            info.session_id.clone(),
            info.user_id.clone(),
            SparkArtifactOptions {
                root: if self.config.spark.artifact_root.is_empty() {
                    None
                } else {
                    Some(self.config.spark.artifact_root.clone().into())
                },
                inline_max_bytes: self.config.spark.artifact_inline_max_bytes,
                max_bytes: self.config.spark.artifact_max_bytes,
                max_chunks: self.config.spark.artifact_max_chunks,
                rpc_max_artifacts: self.config.spark.artifact_rpc_max_artifacts,
                rpc_max_chunks: self.config.spark.artifact_rpc_max_chunks,
                rpc_max_bytes: self.config.spark.artifact_rpc_max_bytes,
                session_max_artifacts: self.config.spark.artifact_session_max_artifacts,
                session_max_bytes: self.config.spark.artifact_session_max_bytes,
                chunk_timeout: Duration::from_secs(self.config.spark.artifact_chunk_timeout_secs),
                transfer_timeout: Duration::from_secs(
                    self.config.spark.artifact_transfer_timeout_secs,
                ),
                store_uri: if self.config.spark.artifact_store_uri.is_empty() {
                    None
                } else {
                    Some(self.config.spark.artifact_store_uri.clone())
                },
                store_namespace: self.artifact_store_namespace.clone(),
                runtime: self.runtime.clone(),
            },
            Arc::clone(&self.artifact_process_budget),
        );
        Ok(config
            .with_extension(Arc::new(plan_service))
            .with_extension(Arc::new(artifacts))
            .with_extension(Arc::new(spark)))
    }

    fn mutate_state(
        &self,
        builder: SessionStateBuilder,
        _info: &ServerSessionInfo,
    ) -> Result<SessionStateBuilder> {
        Ok(builder)
    }

    fn mutate_runtime_env(
        &self,
        builder: RuntimeEnvBuilder,
        _info: &ServerSessionInfo,
    ) -> Result<RuntimeEnvBuilder> {
        Ok(builder)
    }
}

fn create_spark_session_factory(
    manager_config: SparkSessionManagerConfig,
    runtime: RuntimeHandle,
    system: Arc<Mutex<ActorSystem>>,
    session_process_admission: Arc<SparkSessionProcessAdmission>,
) -> Box<dyn SessionFactory<ServerSessionInfo>> {
    let config = manager_config.config;
    let mutator = Box::new(SparkSessionMutator {
        config: config.clone(),
        runtime: runtime.clone(),
        artifact_store_namespace: uuid::Uuid::new_v4().to_string(),
        artifact_process_budget: manager_config.artifact_process_budget,
    });
    Box::new(SparkSessionFactory {
        session_factory: Box::new(ServerSessionFactory::new(config, runtime, system, mutator)),
        session_process_admission,
        _automatic_artifact_store: manager_config.automatic_artifact_store,
    })
}

pub fn create_spark_session_manager(
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
) -> SparkResult<SessionManager> {
    let manager_config = resolve_artifact_store_config(config)?;
    initialize_artifact_cleanup_journals(&manager_config.config)?;
    let session_process_admission = resolve_session_process_admission(
        manager_config.config.spark.session_max_count,
        &SPARK_SESSION_PROCESS_ADMISSION,
    )?;
    let system = Arc::new(Mutex::new(ActorSystem::new()));
    let factory = {
        let manager_config = manager_config.clone();
        let runtime = runtime.clone();
        let system = system.clone();
        let session_process_admission = Arc::clone(&session_process_admission);
        Box::new(move || {
            create_spark_session_factory(
                manager_config.clone(),
                runtime.clone(),
                system.clone(),
                Arc::clone(&session_process_admission),
            )
        })
    };
    let options = SessionManagerOptions::new(runtime.clone(), system, factory)
        .with_session_timeout(Duration::from_secs(
            manager_config.config.spark.session_timeout_secs,
        ))
        .session_limit(manager_config.config.spark.session_max_count)
        .with_options(manager_config.config.raw().map_err(SparkError::from)?);
    Ok(SessionManager::try_new(options)?)
}

fn initialize_artifact_cleanup_journals(config: &AppConfig) -> SparkResult<()> {
    let configured_root = config.spark.artifact_cleanup_journal_root.trim();
    let configured_root = if configured_root.is_empty() {
        None
    } else {
        Some(Path::new(configured_root))
    };
    let require_configured_root = matches!(config.mode, ExecutionMode::KubernetesCluster)
        && !config.spark.artifact_store_uri.is_empty();
    let journal_root = resolve_artifact_cleanup_journal_root(
        configured_root,
        require_configured_root,
        &ARTIFACT_CLEANUP_JOURNAL_ROOT,
    )?;
    initialize_artifact_cleanup(&journal_root.join("server"))?;
    sail_execution::initialize_task_resource_cleanup_journal(&journal_root.join("execution"))?;
    Ok(())
}

fn resolve_artifact_cleanup_journal_root(
    configured_root: Option<&Path>,
    require_configured_root: bool,
    journal_root_slot: &OnceLock<PathBuf>,
) -> SparkResult<PathBuf> {
    if require_configured_root && configured_root.is_none() {
        return Err(SparkError::invalid(
            "spark.artifact_cleanup_journal_root must be configured when Kubernetes mode uses spark.artifact_store_uri",
        ));
    }
    if configured_root.is_none()
        && let Some(journal_root) = journal_root_slot.get()
    {
        return Ok(journal_root.clone());
    }
    let requested_root = configured_root.map_or_else(
        || std::env::temp_dir().join(DEFAULT_ARTIFACT_CLEANUP_JOURNAL_DIRECTORY),
        Path::to_path_buf,
    );
    if !requested_root.is_absolute() {
        return Err(SparkError::invalid(
            "spark.artifact_cleanup_journal_root must be an absolute path",
        ));
    }
    let requested_root = prepare_artifact_cleanup_journal_root(&requested_root)?;
    match journal_root_slot.set(requested_root.clone()) {
        Ok(()) => Ok(requested_root),
        Err(requested_root) => {
            let configured_root = journal_root_slot.get().ok_or_else(|| {
                SparkError::internal("artifact cleanup journal root was not initialized")
            })?;
            if configured_root == &requested_root {
                Ok(requested_root)
            } else {
                Err(SparkError::invalid(format!(
                    "spark.artifact_cleanup_journal_root conflicts with the process-wide configuration: configured {}, requested {}",
                    configured_root.display(),
                    requested_root.display()
                )))
            }
        }
    }
}

fn prepare_artifact_cleanup_journal_root(root: &Path) -> SparkResult<PathBuf> {
    match fs::symlink_metadata(root) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(SparkError::invalid(format!(
                "spark.artifact_cleanup_journal_root is not a directory: {}",
                root.display()
            )));
        }
        Ok(_) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => {
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;

                let mut builder = fs::DirBuilder::new();
                builder.mode(0o700).recursive(true).create(root)?;
            }
            #[cfg(not(unix))]
            fs::create_dir_all(root)?;
        }
        Err(error) => return Err(error.into()),
    }
    let metadata = fs::symlink_metadata(root)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(SparkError::invalid(format!(
            "spark.artifact_cleanup_journal_root is not a directory: {}",
            root.display()
        )));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

        let probe_path = root.join(format!(
            ".owner-probe-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let probe = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&probe_path)?;
        let process_uid = probe.metadata()?.uid();
        drop(probe);
        fs::remove_file(&probe_path)?;
        if metadata.uid() != process_uid {
            return Err(SparkError::invalid(format!(
                "spark.artifact_cleanup_journal_root has an unexpected owner: {}",
                root.display()
            )));
        }
        fs::set_permissions(root, fs::Permissions::from_mode(0o700))?;
    }
    Ok(fs::canonicalize(root)?)
}

fn resolve_artifact_store_config(config: Arc<AppConfig>) -> SparkResult<SparkSessionManagerConfig> {
    resolve_artifact_store_config_from_process_budget_slot(config, &SPARK_ARTIFACT_PROCESS_BUDGET)
}

fn resolve_artifact_store_config_from_process_budget_slot(
    config: Arc<AppConfig>,
    process_budget_slot: &OnceLock<Arc<SparkArtifactProcessBudget>>,
) -> SparkResult<SparkSessionManagerConfig> {
    validate_artifact_store_uri(&config.spark.artifact_store_uri)?;
    if !config.spark.artifact_store_uri.is_empty()
        || matches!(config.mode, ExecutionMode::KubernetesCluster)
    {
        let artifact_process_budget =
            resolve_artifact_process_budget(&config, process_budget_slot)?;
        return Ok(SparkSessionManagerConfig {
            config,
            automatic_artifact_store: None,
            artifact_process_budget,
        });
    }

    let artifact_store = tempfile::Builder::new()
        .prefix("sail-artifact-store-")
        .tempdir()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        std::fs::set_permissions(
            artifact_store.path(),
            std::fs::Permissions::from_mode(0o700),
        )?;
    }
    let store_uri = Url::from_directory_path(artifact_store.path())
        .map_err(|()| SparkError::internal("failed to create local artifact store URI"))?
        .to_string();
    let mut effective_config = (*config).clone();
    effective_config.spark.artifact_store_uri = store_uri;
    let effective_config = Arc::new(effective_config);
    let artifact_process_budget =
        resolve_artifact_process_budget(&effective_config, process_budget_slot)?;
    Ok(SparkSessionManagerConfig {
        config: effective_config,
        automatic_artifact_store: Some(Arc::new(artifact_store)),
        artifact_process_budget,
    })
}

fn resolve_artifact_process_budget(
    config: &AppConfig,
    process_budget_slot: &OnceLock<Arc<SparkArtifactProcessBudget>>,
) -> SparkResult<Arc<SparkArtifactProcessBudget>> {
    let requested = SparkArtifactProcessLimits::from_config(config);
    let configured = process_budget_slot.get_or_init(|| {
        Arc::new(SparkArtifactProcessBudget::new(
            requested.max_artifacts,
            requested.max_bytes,
            requested.inline_max_bytes,
        ))
    });
    let (max_artifacts, max_bytes, inline_max_bytes) = configured.configured_limits();
    let configured_limits = SparkArtifactProcessLimits {
        max_artifacts,
        max_bytes,
        inline_max_bytes,
    };
    if configured_limits != requested {
        return Err(SparkError::invalid(format!(
            "Spark artifact process limits conflict with the process-wide configuration: configured spark.artifact_process_max_artifacts={}, spark.artifact_process_max_bytes={}, spark.artifact_process_inline_max_bytes={}; requested spark.artifact_process_max_artifacts={}, spark.artifact_process_max_bytes={}, spark.artifact_process_inline_max_bytes={}",
            configured_limits.max_artifacts,
            configured_limits.max_bytes,
            configured_limits.inline_max_bytes,
            requested.max_artifacts,
            requested.max_bytes,
            requested.inline_max_bytes,
        )));
    }
    Ok(Arc::clone(configured))
}

fn validate_artifact_store_uri(value: &str) -> SparkResult<()> {
    if value.is_empty() {
        return Ok(());
    }
    let url = Url::parse(value)
        .map_err(|_| SparkError::invalid("invalid spark.artifact_store_uri value"))?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(SparkError::invalid(
            "spark.artifact_store_uri must not contain userinfo, query parameters, or a fragment",
        ));
    }
    let (scheme, _) = ObjectStoreScheme::parse(&url)
        .map_err(|_| SparkError::invalid("unsupported spark.artifact_store_uri scheme"))?;
    match scheme {
        ObjectStoreScheme::Local
        | ObjectStoreScheme::AmazonS3
        | ObjectStoreScheme::MicrosoftAzure
        | ObjectStoreScheme::GoogleCloudStorage => Ok(()),
        _ => Err(SparkError::invalid(
            "spark.artifact_store_uri must use a writable shared object store",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct EmptySessionFactory;

    impl SessionFactory<()> for EmptySessionFactory {
        fn create(&mut self, _info: ()) -> Result<datafusion::prelude::SessionContext> {
            Ok(datafusion::prelude::SessionContext::new())
        }
    }

    fn admitted_test_factory(
        session_process_admission: Arc<SparkSessionProcessAdmission>,
    ) -> SparkSessionFactory<()> {
        SparkSessionFactory {
            session_factory: Box::new(EmptySessionFactory),
            session_process_admission,
            _automatic_artifact_store: None,
        }
    }

    fn app_config(mode: ExecutionMode, artifact_store_uri: &str) -> SparkResult<Arc<AppConfig>> {
        let mut config = AppConfig::load().map_err(SparkError::from)?;
        config.mode = mode;
        config.spark.artifact_store_uri = artifact_store_uri.to_string();
        Ok(Arc::new(config))
    }

    fn app_config_with_process_limits(
        limits: SparkArtifactProcessLimits,
    ) -> SparkResult<Arc<AppConfig>> {
        let mut config = (*app_config(
            ExecutionMode::LocalCluster,
            "s3://bucket/process-budget-tests",
        )?)
        .clone();
        config.spark.artifact_process_max_artifacts = limits.max_artifacts;
        config.spark.artifact_process_max_bytes = limits.max_bytes;
        config.spark.artifact_process_inline_max_bytes = limits.inline_max_bytes;
        Ok(Arc::new(config))
    }

    fn artifact_registry(
        session_id: &str,
        runtime: RuntimeHandle,
        process_budget: Arc<SparkArtifactProcessBudget>,
    ) -> SparkArtifactRegistry {
        SparkArtifactRegistry::new(
            session_id.to_string(),
            "user".to_string(),
            SparkArtifactOptions {
                root: None,
                inline_max_bytes: usize::MAX,
                max_bytes: usize::MAX,
                max_chunks: usize::MAX,
                rpc_max_artifacts: usize::MAX,
                rpc_max_chunks: usize::MAX,
                rpc_max_bytes: usize::MAX,
                session_max_artifacts: usize::MAX,
                session_max_bytes: usize::MAX,
                chunk_timeout: Duration::from_secs(1),
                transfer_timeout: Duration::from_secs(1),
                store_uri: None,
                store_namespace: "process-budget-tests".to_string(),
                runtime,
            },
            process_budget,
        )
    }

    #[tokio::test]
    async fn separate_server_configs_share_process_artifact_usage() -> SparkResult<()> {
        let process_budget_slot = OnceLock::new();
        let limits = SparkArtifactProcessLimits {
            max_artifacts: 1,
            max_bytes: 16,
            inline_max_bytes: 16,
        };
        let config = app_config_with_process_limits(limits)?;
        let first_server = resolve_artifact_store_config_from_process_budget_slot(
            Arc::clone(&config),
            &process_budget_slot,
        )?;
        let second_server =
            resolve_artifact_store_config_from_process_budget_slot(config, &process_budget_slot)?;
        assert!(Arc::ptr_eq(
            &first_server.artifact_process_budget,
            &second_server.artifact_process_budget
        ));

        let runtime = RuntimeHandle::new(
            tokio::runtime::Handle::current(),
            tokio::runtime::Handle::current(),
        );
        let first_registry = artifact_registry(
            "first",
            runtime.clone(),
            Arc::clone(&first_server.artifact_process_budget),
        );
        let second_registry = artifact_registry(
            "second",
            runtime,
            Arc::clone(&second_server.artifact_process_budget),
        );
        first_registry.register_artifact("files/first".to_string(), 1, None, None)?;
        let error = second_registry
            .register_artifact("files/second".to_string(), 1, None, None)
            .err()
            .ok_or_else(|| {
                SparkError::internal("separate servers did not share process artifact usage")
            })?;
        assert!(error.to_string().contains("process exceeded the limit"));

        drop(first_registry);
        second_registry.register_artifact("files/second".to_string(), 1, None, None)?;
        Ok(())
    }

    #[test]
    fn separate_factories_share_process_session_limit_until_context_drop() -> SparkResult<()> {
        let journal_root = resolve_artifact_cleanup_journal_root(None, false, &OnceLock::new())?;
        initialize_artifact_cleanup(&journal_root.join("server"))?;
        let admission_slot = OnceLock::new();
        let first_admission = resolve_session_process_admission(1, &admission_slot)?;
        let second_admission = resolve_session_process_admission(1, &admission_slot)?;
        let mut first_factory = admitted_test_factory(first_admission);
        let mut second_factory = admitted_test_factory(second_admission);

        let first_context = first_factory.create(())?;
        let retained_context = first_context.clone();
        let error = second_factory
            .create(())
            .err()
            .ok_or_else(|| SparkError::internal("process session limit was not shared"))?;
        assert!(
            error
                .to_string()
                .contains("process-wide maximum number of sessions (1) reached")
        );

        drop(first_context);
        assert!(second_factory.create(()).is_err());

        drop(retained_context);
        let second_context = second_factory.create(())?;
        drop(second_context);
        Ok(())
    }

    #[test]
    fn process_artifact_limit_mismatch_is_rejected() -> SparkResult<()> {
        let process_budget_slot = OnceLock::new();
        let configured_limits = SparkArtifactProcessLimits {
            max_artifacts: 1,
            max_bytes: 16,
            inline_max_bytes: 8,
        };
        let configured = resolve_artifact_store_config_from_process_budget_slot(
            app_config_with_process_limits(configured_limits)?,
            &process_budget_slot,
        )?;
        let requested_limits = SparkArtifactProcessLimits {
            max_artifacts: 2,
            ..configured_limits
        };
        let error = match resolve_artifact_store_config_from_process_budget_slot(
            app_config_with_process_limits(requested_limits)?,
            &process_budget_slot,
        ) {
            Ok(_) => {
                return Err(SparkError::internal(
                    "conflicting process artifact limits were accepted",
                ));
            }
            Err(error) => error,
        };
        assert!(error.to_string().contains("process-wide configuration"));

        let configured_again = resolve_artifact_store_config_from_process_budget_slot(
            app_config_with_process_limits(configured_limits)?,
            &process_budget_slot,
        )?;
        assert!(Arc::ptr_eq(
            &configured.artifact_process_budget,
            &configured_again.artifact_process_budget
        ));
        Ok(())
    }

    #[test]
    fn local_modes_create_automatic_artifact_store() -> SparkResult<()> {
        for mode in [ExecutionMode::Local, ExecutionMode::LocalCluster] {
            let manager_config = resolve_artifact_store_config(app_config(mode, "")?)?;
            let artifact_store = manager_config
                .automatic_artifact_store
                .as_ref()
                .ok_or_else(|| SparkError::internal("automatic artifact store is missing"))?;
            let uri = Url::parse(&manager_config.config.spark.artifact_store_uri)
                .map_err(|error| SparkError::internal(error.to_string()))?;

            assert_eq!(uri.scheme(), "file");
            assert_eq!(
                uri.to_file_path().ok().as_deref(),
                Some(artifact_store.path())
            );
            assert!(artifact_store.path().is_dir());
        }
        Ok(())
    }

    #[test]
    fn kubernetes_mode_does_not_create_automatic_artifact_store() -> SparkResult<()> {
        let manager_config =
            resolve_artifact_store_config(app_config(ExecutionMode::KubernetesCluster, "")?)?;

        assert!(manager_config.config.spark.artifact_store_uri.is_empty());
        assert!(manager_config.automatic_artifact_store.is_none());
        Ok(())
    }

    #[test]
    fn explicit_artifact_store_is_not_overwritten() -> SparkResult<()> {
        let config = app_config(ExecutionMode::LocalCluster, "s3://bucket/prefix")?;
        let manager_config = resolve_artifact_store_config(config.clone())?;

        assert!(Arc::ptr_eq(&manager_config.config, &config));
        assert_eq!(
            manager_config.config.spark.artifact_store_uri,
            "s3://bucket/prefix"
        );
        assert!(manager_config.automatic_artifact_store.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn automatic_artifact_store_is_private_and_removed_with_factory() -> SparkResult<()> {
        let manager_config = resolve_artifact_store_config(app_config(ExecutionMode::Local, "")?)?;
        let path = manager_config
            .automatic_artifact_store
            .as_ref()
            .ok_or_else(|| SparkError::internal("automatic artifact store is missing"))?
            .path()
            .to_path_buf();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            assert_eq!(
                std::fs::metadata(&path)?.permissions().mode() & 0o777,
                0o700
            );
        }
        assert!(path.is_dir());
        let runtime = RuntimeHandle::new(
            tokio::runtime::Handle::current(),
            tokio::runtime::Handle::current(),
        );
        let factory = create_spark_session_factory(
            manager_config,
            runtime,
            Arc::new(Mutex::new(ActorSystem::new())),
            resolve_session_process_admission(1, &OnceLock::new())?,
        );
        assert!(path.is_dir());
        drop(factory);
        assert!(!path.exists());
        Ok(())
    }

    #[test]
    fn artifact_store_uri_accepts_writable_shared_stores() {
        assert!(validate_artifact_store_uri("").is_ok());
        assert!(validate_artifact_store_uri("file:///tmp/sail-artifacts").is_ok());
        assert!(validate_artifact_store_uri("s3://bucket/prefix").is_ok());
    }

    #[test]
    fn artifact_store_uri_rejects_non_shared_or_read_only_stores() {
        assert!(validate_artifact_store_uri("memory:///artifacts").is_err());
        assert!(validate_artifact_store_uri("https://example.com/artifacts").is_err());
    }

    #[test]
    fn artifact_store_uri_rejects_embedded_credentials_or_parameters() {
        assert!(validate_artifact_store_uri("s3://user@bucket/prefix").is_err());
        assert!(validate_artifact_store_uri("s3://bucket/prefix?token=secret").is_err());
    }

    #[test]
    #[expect(
        clippy::expect_used,
        reason = "success would violate fail-closed behavior"
    )]
    fn kubernetes_remote_store_requires_cleanup_journal_root() {
        let error = resolve_artifact_cleanup_journal_root(None, true, &OnceLock::new())
            .expect_err("Kubernetes remote artifact cleanup must fail closed");

        assert!(error.to_string().contains("artifact_cleanup_journal_root"));
        assert!(error.to_string().contains("Kubernetes"));
    }

    #[test]
    #[expect(
        clippy::expect_used,
        reason = "accepting a conflicting process-wide path is a test failure"
    )]
    fn cleanup_journal_root_is_private_and_rejects_process_conflicts() -> SparkResult<()> {
        let temp = tempfile::tempdir()?;
        let first = temp.path().join("first");
        let second = temp.path().join("second");
        let journal_root_slot = OnceLock::new();

        let configured =
            resolve_artifact_cleanup_journal_root(Some(&first), true, &journal_root_slot)?;
        assert_eq!(configured, first.canonicalize()?);
        assert_eq!(
            resolve_artifact_cleanup_journal_root(Some(&first), true, &journal_root_slot,)?,
            configured
        );
        assert_eq!(
            resolve_artifact_cleanup_journal_root(None, false, &journal_root_slot)?,
            configured
        );
        let error = resolve_artifact_cleanup_journal_root(Some(&second), true, &journal_root_slot)
            .expect_err("conflicting process cleanup roots must be rejected");
        assert!(error.to_string().contains("process-wide"));

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            assert_eq!(
                std::fs::metadata(&configured)?.permissions().mode() & 0o777,
                0o700
            );
        }
        Ok(())
    }
}
