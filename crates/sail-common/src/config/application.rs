use figment::providers::{Env, Format, Toml};
use figment::value::{Dict, Empty, Map, Tag, Value};
use figment::{Error, Figment, Metadata, Profile, Provider};
use serde::{Deserialize, Serialize};

use crate::error::{CommonError, CommonResult};

const DEFAULT_CONFIG: &str = include_str!("default.toml");

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub mode: ExecutionMode,
    pub runtime: RuntimeConfig,
    pub cluster: ClusterConfig,
    pub execution: ExecutionConfig,
    pub kubernetes: KubernetesConfig,
    pub parquet: ParquetConfig,
    pub spark: SparkConfig,
    pub catalog: CatalogConfig,
    /// Reserved for internal use.
    /// This field ensures that environment variables with prefix `SAIL_INTERNAL_`
    /// can only be used for internal configuration.
    /// Such environment variables are ignored by application configuration.
    pub internal: (),
}

/// A configuration provider that injects placeholder internal configuration.
struct InternalConfigPlaceholder;

impl Provider for InternalConfigPlaceholder {
    fn metadata(&self) -> Metadata {
        Metadata::named("Internal")
    }

    fn data(&self) -> Result<Map<Profile, Dict>, Error> {
        Ok(Map::from([(
            Profile::Default,
            Dict::from([(
                "internal".to_string(),
                Value::Empty(Tag::Default, Empty::Unit),
            )]),
        )]))
    }
}

impl AppConfig {
    pub fn load() -> CommonResult<Self> {
        Figment::from(Toml::string(DEFAULT_CONFIG))
            .merge(InternalConfigPlaceholder)
            .merge(Env::prefixed("SAIL_").map(|p| p.as_str().replace("__", ".").into()))
            .extract()
            .map_err(|e| CommonError::InvalidArgument(e.to_string()))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    Local,
    #[serde(alias = "local-cluster")]
    LocalCluster,
    #[serde(
        alias = "kubernetes-cluster",
        alias = "k8s-cluster",
        alias = "k8s_cluster",
        alias = "kube-cluster",
        alias = "kube_cluster"
    )]
    KubernetesCluster,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeConfig {
    pub stack_size: usize,
    pub enable_secondary: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub enable_tls: bool,
    pub driver_listen_host: String,
    pub driver_listen_port: u16,
    pub driver_external_host: String,
    pub driver_external_port: u16,
    pub worker_id: u64,
    pub worker_listen_host: String,
    pub worker_listen_port: u16,
    pub worker_external_host: String,
    pub worker_external_port: u16,
    pub worker_initial_count: usize,
    pub worker_max_count: usize,
    pub worker_max_idle_time_secs: u64,
    pub worker_heartbeat_interval_secs: u64,
    pub worker_heartbeat_timeout_secs: u64,
    pub worker_launch_timeout_secs: u64,
    pub worker_task_slots: usize,
    pub worker_stream_buffer: usize,
    pub task_launch_timeout_secs: u64,
    pub job_output_buffer: usize,
    pub rpc_retry_strategy: RetryStrategy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryStrategy {
    Fixed {
        max_count: usize,
        delay_secs: u64,
    },
    ExponentialBackoff {
        max_count: usize,
        initial_delay_secs: u64,
        max_delay_secs: u64,
        factor: u32,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionConfig {
    pub batch_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KubernetesConfig {
    pub image: String,
    pub image_pull_policy: String,
    pub namespace: String,
    /// The name of the pod that runs the driver,
    /// or empty if the driver pod name is not known.
    /// This is used to set owner references for worker pods.
    pub driver_pod_name: String,
    /// The prefix of the name of worker pods.
    /// This should usually end with a hyphen (`-`).
    pub worker_pod_name_prefix: String,
    pub worker_service_account_name: String,
}

// Doc Comments [Credit]: <https://github.com/apache/datafusion/blob/555fc2e24dd669e44ac23a9a1d8406f4ac58a9ed/datafusion/common/src/config.rs#L423-L592>
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParquetConfig {
    /// (reading) If true, reads the Parquet data page level metadata (the
    /// Page Index), if present, to reduce the I/O and number of
    /// rows decoded.
    pub enable_page_index: bool,

    /// (reading) If true, the parquet reader attempts to skip entire row groups based
    /// on the predicate in the query and the metadata (min/max values) stored in
    /// the parquet file
    pub pruning: bool,

    /// (reading) If true, the parquet reader skip the optional embedded metadata that may be in
    /// the file Schema. This setting can help avoid schema conflicts when querying
    /// multiple parquet files with schemas containing compatible types but different metadata
    pub skip_metadata: bool,

    /// (reading) If specified, the parquet reader will try and fetch the last `size_hint`
    /// bytes of the parquet file optimistically. If not specified, two reads are required:
    /// One read to fetch the 8-byte parquet footer and
    /// another to fetch the metadata length encoded in the footer
    pub metadata_size_hint: Option<usize>,

    /// (reading) If true, filter expressions are be applied during the parquet decoding operation to
    /// reduce the number of rows decoded. This optimization is sometimes called "late materialization".
    pub pushdown_filters: bool,

    /// (reading) If true, filter expressions evaluated during the parquet decoding operation
    /// will be reordered heuristically to minimize the cost of evaluation. If false,
    /// the filters are applied in the same order as written in the query
    pub reorder_filters: bool,

    /// (reading) If true, parquet reader will read columns of `Utf8/Utf8Large` with `Utf8View`,
    /// and `Binary/BinaryLarge` with `BinaryView`.
    pub schema_force_view_types: bool,

    /// (reading) If true, parquet reader will read columns of
    /// `Binary/LargeBinary` with `Utf8`, and `BinaryView` with `Utf8View`.
    ///
    /// Parquet files generated by some legacy writers do not correctly set
    /// the UTF8 flag for strings, causing string columns to be loaded as
    /// BLOB instead.
    pub binary_as_string: bool,

    /// (writing) Sets best effort maximum size of data page in bytes
    pub data_pagesize_limit: usize,

    /// (writing) Sets write_batch_size in bytes
    pub write_batch_size: usize,

    /// (writing) Sets parquet writer version
    /// valid values are "1.0" and "2.0"
    pub writer_version: String,

    /// (writing) Skip encoding the embedded arrow metadata in the KV_meta
    ///
    /// This is analogous to the `ArrowWriterOptions::with_skip_arrow_metadata`.
    /// Refer to <https://docs.rs/parquet/53.3.0/parquet/arrow/arrow_writer/struct.ArrowWriterOptions.html#method.with_skip_arrow_metadata>
    pub skip_arrow_metadata: bool,

    /// (writing) Sets default parquet compression codec.
    /// Valid values are: uncompressed, snappy, gzip(level),
    /// lzo, brotli(level), lz4, zstd(level), and lz4_raw.
    /// These values are not case-sensitive. If NULL, uses
    /// default parquet writer setting
    ///
    /// Note that this default setting is not the same as
    /// the default parquet writer setting.
    pub compression: Option<String>,

    /// (writing) Sets if dictionary encoding is enabled. If NULL, uses
    /// default parquet writer setting
    pub dictionary_enabled: Option<bool>,

    /// (writing) Sets best effort maximum dictionary page size, in bytes
    pub dictionary_page_size_limit: usize,

    /// (writing) Sets if statistics are enabled for any column
    /// Valid values are: "none", "chunk", and "page"
    /// These values are not case-sensitive. If NULL, uses
    /// default parquet writer setting
    pub statistics_enabled: Option<String>,

    /// (writing) Target maximum number of rows in each row group (defaults to 1M
    /// rows). Writing larger row groups requires more memory to write, but
    /// can get better compression and be faster to read.
    pub max_row_group_size: usize,

    /// (writing) Sets column index truncate length
    pub column_index_truncate_length: Option<usize>,

    /// (writing) Sets statictics truncate length. If NULL, uses
    /// default parquet writer setting
    pub statistics_truncate_length: Option<usize>,

    /// (writing) Sets best effort maximum number of rows in data page
    pub data_page_row_count_limit: usize,

    /// (writing)  Sets default encoding for any column.
    /// Valid values are: plain, plain_dictionary, rle,
    /// bit_packed, delta_binary_packed, delta_length_byte_array,
    /// delta_byte_array, rle_dictionary, and byte_stream_split.
    /// These values are not case sensitive. If NULL, uses
    /// default parquet writer setting
    pub encoding: Option<String>,

    /// (writing) Use any available bloom filters when reading parquet files
    pub bloom_filter_on_read: bool,

    /// (writing) Write bloom filters for all columns when creating parquet files
    pub bloom_filter_on_write: bool,

    /// (writing) Sets bloom filter false positive probability. If NULL, uses
    /// default parquet writer setting
    pub bloom_filter_fpp: Option<f64>,

    /// (writing) Sets bloom filter number of distinct values. If NULL, uses
    /// default parquet writer setting
    pub bloom_filter_ndv: Option<u64>,

    /// (writing) Controls whether DataFusion will attempt to speed up writing
    /// parquet files by serializing them in parallel. Each column
    /// in each row group in each output file are serialized in parallel
    /// leveraging a maximum possible core count of n_files*n_row_groups*n_columns.
    pub allow_single_file_parallelism: bool,

    /// (writing) By default parallel parquet writer is tuned for minimum
    /// memory usage in a streaming execution plan. You may see
    /// a performance benefit when writing large parquet files
    /// by increasing maximum_parallel_row_group_writers and
    /// maximum_buffered_record_batches_per_stream if your system
    /// has idle cores and can tolerate additional memory usage.
    /// Boosting these values is likely worthwhile when
    /// writing out already in-memory data, such as from a cached
    /// data frame.
    pub maximum_parallel_row_group_writers: usize,

    /// (writing) By default parallel parquet writer is tuned for minimum
    /// memory usage in a streaming execution plan. You may see
    /// a performance benefit when writing large parquet files
    /// by increasing maximum_parallel_row_group_writers and
    /// maximum_buffered_record_batches_per_stream if your system
    /// has idle cores and can tolerate additional memory usage.
    /// Boosting these values is likely worthwhile when
    /// writing out already in-memory data, such as from a cached
    /// data frame.
    pub maximum_buffered_record_batches_per_stream: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogConfig {
    pub has_header: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SparkConfig {
    pub session_timeout_secs: u64,
}

/// Environment variables for application cluster configuration.
pub struct ClusterConfigEnv;

macro_rules! define_cluster_config_env {
    ($($name:ident),* $(,)?) => {
        $(pub const $name: &'static str = concat!("SAIL_CLUSTER__", stringify!($name));)*
    };
}

impl ClusterConfigEnv {
    define_cluster_config_env! {
        ENABLE_TLS,
        DRIVER_EXTERNAL_HOST,
        DRIVER_EXTERNAL_PORT,
        WORKER_ID,
        WORKER_LISTEN_HOST,
        WORKER_EXTERNAL_HOST,
        WORKER_HEARTBEAT_INTERVAL_SECS,
        WORKER_STREAM_BUFFER,
        RPC_RETRY_STRATEGY,
    }
}
