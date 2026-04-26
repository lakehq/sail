#![allow(dead_code)]
#![expect(clippy::expect_used, clippy::panic)]

use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::datatypes::DataType;
use sail_catalog::provider::{
    CatalogProvider, CreateDatabaseOptions, CreateTableColumnOptions, CreateTableOptions, Namespace,
};
use sail_catalog_hms::{HmsCatalogConfig, HmsCatalogProvider};
use sail_common::runtime::RuntimeHandle;
use tempfile::TempDir;
use testcontainers::core::{ContainerPort, ExecCommand, WaitFor};
use testcontainers::runners::{AsyncBuilder, AsyncRunner};
use testcontainers::{ContainerAsync, GenericBuildableImage, GenericImage, Image, ImageExt};
use tokio::net::TcpStream;
use tokio::process::Command;
use tokio::sync::{Mutex, OwnedMutexGuard};

const KERBEROS_REALM: &str = "SAIL.TEST";
const KDC_HOSTNAME: &str = "sail-kerberos-kdc";
const HMS_HOSTNAME: &str = "sail-kerberos-hms";
const HMS_KEYTAB_PATH: &str = "/opt/hive/conf/hms.service.keytab";
const CONTAINER_KRB5_CONFIG_PATH: &str = "/etc/krb5.conf";
const HIVE_SITE_XML_PATH: &str = "/opt/hive/conf/hive-site.xml";
const CORE_SITE_XML_PATH: &str = "/opt/hadoop/etc/hadoop/core-site.xml";
const HIVE_METASTORE_PORT: u16 = 9083;
const KDC_PORT: u16 = 88;

pub struct HmsTestContext {
    pub catalog: HmsCatalogProvider,
    pub host: String,
    pub port: u16,
}

pub struct HmsDatabaseContext {
    pub catalog: HmsCatalogProvider,
    pub namespace: Namespace,
}

pub struct KerberosHmsTestContext {
    pub catalog: HmsCatalogProvider,
    _env_guard: ProcessEnvGuard,
    _lock: OwnedMutexGuard<()>,
}

struct ProcessEnvGuard {
    previous: Vec<(String, Option<OsString>)>,
}

impl ProcessEnvGuard {
    fn new() -> Self {
        Self {
            previous: Vec::new(),
        }
    }

    fn set(&mut self, name: &str, value: impl Into<OsString>) {
        if !self.previous.iter().any(|(key, _)| key == name) {
            self.previous
                .push((name.to_string(), std::env::var_os(name)));
        }
        std::env::set_var(name, value.into());
    }
}

impl Drop for ProcessEnvGuard {
    fn drop(&mut self) {
        for (name, value) in self.previous.drain(..).rev() {
            match value {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }
    }
}

pub fn simple_database_options() -> CreateDatabaseOptions {
    CreateDatabaseOptions {
        if_not_exists: false,
        comment: None,
        location: None,
        properties: vec![],
    }
}

pub fn col(name: &str, data_type: DataType) -> CreateTableColumnOptions {
    CreateTableColumnOptions {
        name: name.to_string(),
        data_type,
        nullable: true,
        comment: None,
        default: None,
        generated_always_as: None,
    }
}

pub fn simple_table_options(
    test_name: &str,
    columns: Vec<CreateTableColumnOptions>,
) -> CreateTableOptions {
    simple_table_options_with_format(test_name, columns, "parquet")
}

pub fn simple_table_options_with_format(
    test_name: &str,
    columns: Vec<CreateTableColumnOptions>,
    format: &str,
) -> CreateTableOptions {
    CreateTableOptions {
        columns,
        comment: None,
        constraints: vec![],
        location: Some(format!("/tmp/{test_name}")),
        format: format.to_string(),
        partition_by: vec![],
        sort_by: vec![],
        bucket_by: None,
        if_not_exists: false,
        replace: false,
        options: vec![],
        properties: vec![],
    }
}

pub async fn setup_hms_catalog(test_name: &str) -> HmsTestContext {
    let shared = shared_hms_container().await;
    let provider = HmsCatalogProvider::new(
        test_name.to_string(),
        HmsCatalogConfig {
            uris: vec![format!("{}:{}", shared.host, shared.port)],
            thrift_transport: None,
            auth: None,
            kerberos_service_principal: None,
            min_sasl_qop: None,
            connect_timeout_secs: None,
        },
        runtime_handle(),
    )
    .expect("create HMS provider");

    HmsTestContext {
        catalog: provider,
        host: shared.host.clone(),
        port: shared.port,
    }
}

pub async fn setup_with_database(test_name: &str) -> HmsDatabaseContext {
    let HmsTestContext { catalog, .. } = setup_hms_catalog(test_name).await;
    let namespace = Namespace::try_from(vec![format!("{test_name}_db")]).unwrap();
    catalog
        .create_database(&namespace, simple_database_options())
        .await
        .unwrap();
    HmsDatabaseContext { catalog, namespace }
}

#[derive(Debug)]
struct SharedHmsContainer {
    host: String,
    port: u16,
    _container: ContainerAsync<GenericImage>,
}

static SHARED_HMS: OnceLock<SharedHmsContainer> = OnceLock::new();

static HMS_INIT: std::sync::OnceLock<Mutex<()>> = std::sync::OnceLock::new();

async fn shared_hms_container() -> &'static SharedHmsContainer {
    if let Some(shared) = SHARED_HMS.get() {
        return shared;
    }

    // Serialize async initialization so only one caller creates the container.
    let mutex = HMS_INIT.get_or_init(|| Mutex::new(()));
    let _guard = mutex.lock().await;
    // Double-check after acquiring the lock.
    if let Some(shared) = SHARED_HMS.get() {
        return shared;
    }

    let container = GenericImage::new("apache/hive", "3.1.3")
        .with_wait_for(WaitFor::seconds(1))
        .with_exposed_port(ContainerPort::Tcp(HIVE_METASTORE_PORT))
        .with_env_var("SERVICE_NAME", "metastore")
        .with_env_var("VERBOSE", "true")
        .with_startup_timeout(Duration::from_secs(180))
        .start()
        .await
        .expect("Failed to start Hive Metastore");

    let host = container.get_host().await.expect("get host").to_string();
    let port = container
        .get_host_port_ipv4(HIVE_METASTORE_PORT)
        .await
        .expect("get thrift port");

    let provider = HmsCatalogProvider::new(
        "shared_hms readiness check".to_string(),
        HmsCatalogConfig {
            uris: vec![format!("{host}:{port}")],
            thrift_transport: None,
            auth: None,
            kerberos_service_principal: None,
            min_sasl_qop: None,
            connect_timeout_secs: None,
        },
        runtime_handle(),
    )
    .expect("create HMS provider");

    wait_until_ready(&provider, 60, "Hive Metastore").await;

    SHARED_HMS
        .set(SharedHmsContainer {
            host,
            port,
            _container: container,
        })
        .expect("shared HMS container was already initialized");
    SHARED_HMS.get().unwrap()
}

pub async fn setup_kerberos_hms_catalog(test_name: &str) -> KerberosHmsTestContext {
    setup_kerberos_hms_catalog_inner(test_name, true).await
}

pub async fn setup_kerberos_hms_catalog_without_kinit(test_name: &str) -> KerberosHmsTestContext {
    setup_kerberos_hms_catalog_inner(test_name, false).await
}

async fn setup_kerberos_hms_catalog_inner(
    test_name: &str,
    perform_kinit: bool,
) -> KerberosHmsTestContext {
    let lock = kerberos_test_lock().lock_owned().await;
    let shared = shared_kerberos_infrastructure().await;

    let mut env_guard = ProcessEnvGuard::new();
    let ticket_cache_path = shared.temp_dir.path().join(format!("krb5cc-{test_name}"));
    let ticket_cache_str = format!("FILE:{}", ticket_cache_path.display());
    env_guard.set("KRB5CCNAME", ticket_cache_str);

    if perform_kinit {
        run_kinit(&shared.client_keytab_path, &shared.client_principal).await;
    }

    let catalog = HmsCatalogProvider::new(
        test_name.to_string(),
        HmsCatalogConfig {
            uris: vec![format!("{}:{}", shared.canonical_host, shared.hms_port)],
            thrift_transport: None,
            auth: Some("kerberos".to_string()),
            kerberos_service_principal: Some(shared.service_principal.clone()),
            min_sasl_qop: None,
            connect_timeout_secs: None,
        },
        runtime_handle(),
    )
    .expect("create HMS provider");

    if perform_kinit {
        wait_until_ready(&catalog, 240, "Kerberos Hive Metastore").await;
    }

    KerberosHmsTestContext {
        catalog,
        _env_guard: env_guard,
        _lock: lock,
    }
}

struct SharedKerberosInfrastructure {
    canonical_host: String,
    hms_port: u16,
    service_principal: String,
    client_principal: String,
    client_keytab_path: PathBuf,
    host_krb5_conf_path: PathBuf,
    temp_dir: TempDir,
    _kdc_container: ContainerAsync<GenericImage>,
    _hms_container: ContainerAsync<GenericImage>,
}

static SHARED_KRB: OnceLock<SharedKerberosInfrastructure> = OnceLock::new();
static KRB_INIT: std::sync::OnceLock<Mutex<()>> = std::sync::OnceLock::new();

async fn shared_kerberos_infrastructure() -> &'static SharedKerberosInfrastructure {
    if let Some(shared) = SHARED_KRB.get() {
        return shared;
    }

    let mutex = KRB_INIT.get_or_init(|| Mutex::new(()));
    let _guard = mutex.lock().await;
    if let Some(shared) = SHARED_KRB.get() {
        return shared;
    }

    let suffix = unique_suffix();
    let temp_dir = tempfile::Builder::new()
        .prefix("sail-kerberos-hms-")
        .tempdir()
        .unwrap_or_else(|error| panic!("create temp dir: {error}"));
    let network_name = format!("sail-krb-net-{suffix}");
    let image = build_kerberos_kdc_image().await;

    let kdc_container = image
        .with_exposed_port(ContainerPort::Tcp(KDC_PORT))
        .with_exposed_port(ContainerPort::Udp(KDC_PORT))
        .with_wait_for(WaitFor::message_on_stdout("KDC ready"))
        .with_network(&network_name)
        .with_container_name(format!("sail-krb-kdc-{suffix}"))
        .with_hostname(KDC_HOSTNAME)
        .with_env_var("KERBEROS_REALM", KERBEROS_REALM)
        .with_env_var("KDC_HOSTNAME", KDC_HOSTNAME)
        .with_startup_timeout(Duration::from_secs(120))
        .start()
        .await
        .unwrap_or_else(|error| panic!("start kerberos KDC container: {error}"));

    wait_for_kdc_ready(&kdc_container).await;

    let canonical_host = kdc_container
        .get_host()
        .await
        .unwrap_or_else(|error| panic!("get docker host for kerberos KDC: {error}"))
        .to_string()
        .to_ascii_lowercase();
    let kdc_port = kdc_container
        .get_host_port_ipv4(KDC_PORT)
        .await
        .unwrap_or_else(|error| panic!("get KDC host port: {error}"));
    let kdc_udp_port = kdc_container
        .get_host_port_ipv4(ContainerPort::Udp(KDC_PORT))
        .await
        .unwrap_or_else(|error| panic!("get KDC UDP host port: {error}"));
    wait_for_tcp_port(&canonical_host, kdc_port, 60, "Kerberos KDC").await;

    let service_principal = format!("hive-metastore/localhost@{KERBEROS_REALM}");
    let client_principal = format!("sail-test-client@{KERBEROS_REALM}");

    exec_checked(
        &kdc_container,
        [
            "/usr/local/bin/bootstrap-kdc.sh",
            &service_principal,
            &client_principal,
            "/artifacts/hms.service.keytab",
            "/artifacts/client.keytab",
        ],
        "bootstrap Kerberos principals",
    )
    .await;

    wait_for_nonempty_container_file(&kdc_container, "/artifacts/hms.service.keytab").await;
    wait_for_nonempty_container_file(&kdc_container, "/artifacts/client.keytab").await;
    wait_for_nonempty_container_file(&kdc_container, "/artifacts/krb5.conf").await;

    let hms_keytab_path = temp_dir.path().join("hms.service.keytab");
    let client_keytab_path = temp_dir.path().join("client.keytab");
    let hms_krb5_conf_path = temp_dir.path().join("krb5-hms.conf");
    copy_container_file(
        &kdc_container,
        "/artifacts/hms.service.keytab",
        &hms_keytab_path,
    )
    .await;
    copy_container_file(
        &kdc_container,
        "/artifacts/client.keytab",
        &client_keytab_path,
    )
    .await;
    copy_container_file(&kdc_container, "/artifacts/krb5.conf", &hms_krb5_conf_path).await;

    let host_krb5_conf_path = temp_dir.path().join("krb5-host.conf");
    fs::write(
        &host_krb5_conf_path,
        host_krb5_conf(&canonical_host, kdc_port, kdc_udp_port),
    )
    .unwrap_or_else(|error| panic!("write host krb5.conf: {error}"));

    let hive_site_path = temp_dir.path().join("hive-site.xml");
    fs::write(&hive_site_path, hive_site_xml(&service_principal))
        .unwrap_or_else(|error| panic!("write hive-site.xml: {error}"));
    let core_site_path = temp_dir.path().join("core-site.xml");
    fs::write(&core_site_path, core_site_xml())
        .unwrap_or_else(|error| panic!("write core-site.xml: {error}"));

    let hms_container = GenericImage::new("apache/hive", "3.1.3")
        .with_exposed_port(ContainerPort::Tcp(HIVE_METASTORE_PORT))
        .with_network(&network_name)
        .with_container_name(format!("sail-krb-hms-{suffix}"))
        .with_hostname(HMS_HOSTNAME)
        .with_env_var("SERVICE_NAME", "metastore")
        .with_env_var("VERBOSE", "true")
        .with_env_var("HIVE_CONF_DIR", "/opt/hive/conf")
        .with_env_var("HADOOP_CONF_DIR", "/opt/hadoop/etc/hadoop")
        .with_env_var(
            "SERVICE_OPTS",
            format!(
                "-Djava.security.krb5.conf={CONTAINER_KRB5_CONFIG_PATH} -Dsun.security.krb5.debug=true -Dsun.security.jgss.debug=true"
            ),
        )
        .with_copy_to(CONTAINER_KRB5_CONFIG_PATH, hms_krb5_conf_path.as_path())
        .with_copy_to(HIVE_SITE_XML_PATH, hive_site_path.as_path())
        .with_copy_to(CORE_SITE_XML_PATH, core_site_path.as_path())
        .with_copy_to(HMS_KEYTAB_PATH, hms_keytab_path.as_path())
        .with_startup_timeout(Duration::from_secs(240))
        .start()
        .await
        .unwrap_or_else(|error| panic!("start Kerberos Hive Metastore container: {error}"));

    let hms_port = hms_container
        .get_host_port_ipv4(HIVE_METASTORE_PORT)
        .await
        .unwrap_or_else(|error| panic!("get HMS host port: {error}"));
    wait_for_tcp_port(&canonical_host, hms_port, 240, "Kerberos Hive Metastore").await;

    // Set Kerberos env vars permanently for the process (not per-test).
    // KRB5_CONFIG must be set before any GSSAPI call so the library can
    // locate the KDC. SAIL_HMS_KRB_TRACE enables readiness diagnostics.
    std::env::set_var("KRB5_CONFIG", &host_krb5_conf_path);
    std::env::set_var("SAIL_HMS_KRB_TRACE", "1");

    let _ = SHARED_KRB.set(SharedKerberosInfrastructure {
        canonical_host,
        hms_port,
        service_principal,
        client_principal,
        client_keytab_path,
        host_krb5_conf_path,
        temp_dir,
        _kdc_container: kdc_container,
        _hms_container: hms_container,
    });
    SHARED_KRB.get().unwrap()
}

fn runtime_handle() -> RuntimeHandle {
    RuntimeHandle::new(
        tokio::runtime::Handle::current(),
        tokio::runtime::Handle::current(),
    )
}

fn kerberos_test_lock() -> Arc<tokio::sync::Mutex<()>> {
    static LOCK: OnceLock<Arc<tokio::sync::Mutex<()>>> = OnceLock::new();
    LOCK.get_or_init(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone()
}

async fn wait_until_ready(catalog: &HmsCatalogProvider, attempts: usize, service_name: &str) {
    let mut last_error = None;
    for attempt in 0..attempts {
        match tokio::time::timeout(Duration::from_secs(3), catalog.list_databases(None)).await {
            Ok(Ok(_)) => return,
            Ok(Err(error)) => {
                if std::env::var_os("SAIL_HMS_KRB_TRACE").is_some() {
                    eprintln!(
                        "{service_name} readiness attempt {} failed: {}",
                        attempt + 1,
                        error
                    );
                }
                last_error = Some(error.to_string());
            }
            Err(_) => {
                if std::env::var_os("SAIL_HMS_KRB_TRACE").is_some() {
                    eprintln!(
                        "{service_name} readiness attempt {} timed out waiting for Thrift response",
                        attempt + 1
                    );
                }
                last_error = Some(format!(
                    "timed out waiting for {service_name} Thrift response"
                ));
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    panic!("{service_name} did not become ready in time: {last_error:?}");
}

async fn wait_for_tcp_port(host: &str, port: u16, attempts: usize, service_name: &str) {
    let mut last_error = None;
    for _ in 0..attempts {
        match tokio::time::timeout(Duration::from_secs(3), TcpStream::connect((host, port))).await {
            Ok(Ok(_)) => return,
            Ok(Err(error)) => last_error = Some(error.to_string()),
            Err(_) => {
                last_error = Some(format!(
                    "timed out connecting to {service_name} on {host}:{port}"
                ))
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    panic!("{service_name} did not open {host}:{port} in time: {last_error:?}");
}

async fn build_kerberos_kdc_image() -> GenericImage {
    GenericBuildableImage::new("sail-kerberos-kdc", format!("test-{}", std::process::id()))
        .with_file(kerberos_kdc_fixture_dir(), ".")
        .build_image()
        .await
        .unwrap_or_else(|error| panic!("build kerberos KDC image: {error}"))
}

fn kerberos_kdc_fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("kerberos-kdc")
}

async fn wait_for_kdc_ready(container: &ContainerAsync<GenericImage>) {
    let mut last_error = None;
    for _ in 0..60 {
        let (exit_code, stdout, stderr) =
            exec_output(container, ["kadmin.local", "-q", "listprincs"]).await;
        if exit_code == 0 {
            return;
        }
        last_error = Some(format!(
            "exit_code={exit_code}, stdout={stdout:?}, stderr={stderr:?}"
        ));
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    panic!("Kerberos KDC was not ready in time: {last_error:?}");
}

async fn wait_for_nonempty_container_file(container: &ContainerAsync<GenericImage>, path: &str) {
    let mut last_error = None;
    for _ in 0..30 {
        let (exit_code, stdout, stderr) = exec_output(
            container,
            ["sh", "-lc", &format!("test -s {path} && echo ready")],
        )
        .await;
        if exit_code == 0 && stdout.contains("ready") {
            return;
        }
        last_error = Some(format!(
            "exit_code={exit_code}, stdout={stdout:?}, stderr={stderr:?}"
        ));
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    panic!("Container file {path} was not ready in time: {last_error:?}");
}

async fn copy_container_file(
    container: &ContainerAsync<GenericImage>,
    source: &str,
    target: &Path,
) {
    container
        .copy_file_from(source, target)
        .await
        .unwrap_or_else(|error| {
            panic!(
                "copy {source} from container to {}: {error}",
                target.display()
            )
        });
}

async fn run_kinit(keytab: &Path, principal: &str) {
    let output = Command::new("kinit")
        .env("KRB5_TRACE", "/dev/stderr")
        .arg("-kt")
        .arg(keytab)
        .arg(principal)
        .output()
        .await
        .unwrap_or_else(|error| panic!("run kinit: {error}"));

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let platform_hint = if cfg!(target_os = "macos") {
            "\nHint: if this still fails on macOS, rerun from the repo devcontainer or CI, where the supported Kerberos client/runtime is Linux-based."
        } else {
            ""
        };
        panic!(
            "kinit failed: status={:?}, stdout={}, stderr={}{}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            stderr,
            platform_hint,
        );
    }
}

async fn exec_checked<I, S>(
    container: &ContainerAsync<I>,
    cmd: impl IntoIterator<Item = S>,
    what: &str,
) where
    I: Image,
    S: Into<String>,
{
    let (exit_code, stdout, stderr) = exec_output(container, cmd).await;
    if exit_code != 0 {
        panic!("{what} failed with exit code {exit_code}: stdout={stdout:?}, stderr={stderr:?}");
    }
}

async fn exec_output<I, S>(
    container: &ContainerAsync<I>,
    cmd: impl IntoIterator<Item = S>,
) -> (i64, String, String)
where
    I: Image,
    S: Into<String>,
{
    let command: Vec<String> = cmd.into_iter().map(Into::into).collect();
    let mut exec = container
        .exec(ExecCommand::new(command.clone()))
        .await
        .unwrap_or_else(|error| panic!("exec {:?}: {error}", command));
    let stdout = String::from_utf8(
        exec.stdout_to_vec()
            .await
            .unwrap_or_else(|error| panic!("read stdout for {:?}: {error}", command)),
    )
    .unwrap_or_else(|error| panic!("stdout for {:?} is not utf8: {error}", command));
    let stderr = String::from_utf8(
        exec.stderr_to_vec()
            .await
            .unwrap_or_else(|error| panic!("read stderr for {:?}: {error}", command)),
    )
    .unwrap_or_else(|error| panic!("stderr for {:?} is not utf8: {error}", command));
    let exit_code = exec
        .exit_code()
        .await
        .unwrap_or_else(|error| panic!("inspect exec exit code for {:?}: {error}", command))
        .unwrap_or_else(|| panic!("exec {:?} did not report an exit code", command));
    (exit_code, stdout, stderr)
}

fn host_krb5_conf(kdc_host: &str, kdc_tcp_port: u16, kdc_udp_port: u16) -> String {
    format!(
        r#"[libdefaults]
 default_realm = {realm}
 dns_lookup_kdc = false
 dns_lookup_realm = false
 rdns = false
 dns_canonicalize_hostname = false
 qualify_shortname = ""
 ignore_acceptor_hostname = true
 ticket_lifetime = 24h
 forwardable = true

[realms]
 {realm} = {{
  kdc = {kdc_host}:{kdc_udp_port}
  kdc = {kdc_host}:{kdc_tcp_port}
 }}

[domain_realm]
 localhost = {realm}
 .localhost = {realm}
 .local = {realm}
 .internal.cloudapp.net = {realm}
 .cloudapp.net = {realm}
"#,
        realm = KERBEROS_REALM,
    )
}

fn hive_site_xml(service_principal: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:derby:;databaseName=/tmp/sail-kerberos-metastore/metastore_db;create=true</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.apache.derby.jdbc.EmbeddedDriver</value>
  </property>
  <property>
    <name>datanucleus.autoCreateSchema</name>
    <value>true</value>
  </property>
  <property>
    <name>datanucleus.schema.autoCreateTables</name>
    <value>true</value>
  </property>
  <property>
    <name>datanucleus.schema.autoCreateColumns</name>
    <value>true</value>
  </property>
  <property>
    <name>datanucleus.schema.autoCreateConstraints</name>
    <value>true</value>
  </property>
  <property>
    <name>hive.metastore.schema.verification</name>
    <value>false</value>
  </property>
  <property>
    <name>hive.metastore.sasl.enabled</name>
    <value>true</value>
  </property>
  <property>
    <name>hive.metastore.kerberos.principal</name>
    <value>{service_principal}</value>
  </property>
  <property>
    <name>hive.metastore.kerberos.keytab.file</name>
    <value>{HMS_KEYTAB_PATH}</value>
  </property>
  <property>
    <name>hadoop.rpc.protection</name>
    <value>authentication</value>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>/tmp/sail-kerberos-metastore/warehouse</value>
  </property>
</configuration>
"#
    )
}

fn core_site_xml() -> &'static str {
    r#"<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>hadoop.security.authentication</name>
    <value>kerberos</value>
  </property>
  <property>
    <name>hadoop.security.authorization</name>
    <value>true</value>
  </property>
</configuration>
"#
}

fn unique_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|error| panic!("read system time: {error}"))
        .as_millis()
}
