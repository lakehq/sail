use std::borrow::Cow;

/// The environment variables used for context propagation.
/// This follows the OpenTelemetry specification.
///
/// Reference: <https://opentelemetry.io/docs/specs/otel/context/env-carriers/>
pub struct ContextPropagationEnv;

impl ContextPropagationEnv {
    pub const TRACEPARENT: &'static str = "TRACEPARENT";
}

/// W3C Trace Context header names used for context propagation
/// among services.
pub struct ContextPropagationHeader;

impl ContextPropagationHeader {
    pub const TRACEPARENT: &'static str = "traceparent";
}

/// Common span attribute names.
/// The name either follow the OpenTelemetry semantic conventions,
/// or is specific to Sail.
pub struct SpanAttribute;

impl SpanAttribute {
    pub const SPAN_KIND: &'static str = "span.kind";
    pub const SPAN_STATUS_DESCRIPTION: &'static str = "span.status_description";
    pub const SPAN_STATUS_CODE: &'static str = "span.status_code";
    pub const OBJECT_STORE_INSTANCE: &'static str = "object_store.instance";
    pub const OBJECT_STORE_LOCATION: &'static str = "object_store.location";
    pub const OBJECT_STORE_FROM: &'static str = "object_store.from";
    pub const OBJECT_STORE_TO: &'static str = "object_store.to";
    pub const OBJECT_STORE_PREFIX: &'static str = "object_store.prefix";
    pub const OBJECT_STORE_OFFSET: &'static str = "object_store.offset";
    pub const OBJECT_STORE_OPTIONS: &'static str = "object_store.options";
    pub const OBJECT_STORE_RANGE: &'static str = "object_store.range";
    pub const OBJECT_STORE_RANGES: &'static str = "object_store.ranges";
    pub const OBJECT_STORE_SIZE: &'static str = "object_store.size";
    pub const OBJECT_STORE_SIZES: &'static str = "object_store.sizes";
    pub const EXCEPTION_MESSAGE: &'static str = "exception.message";
    pub const EXCEPTION_TYPE: &'static str = "exception.type";
    pub const RETRY_ATTEMPT: &'static str = "retry.attempt";
    pub const CLUSTER_DRIVER_PORT: &'static str = "cluster.driver.port";
    pub const CLUSTER_WORKER_ID: &'static str = "cluster.worker.id";
    pub const CLUSTER_WORKER_HOST: &'static str = "cluster.worker.host";
    pub const CLUSTER_WORKER_PORT: &'static str = "cluster.worker.port";
    pub const EXECUTION_JOB_ID: &'static str = "execution.job.id";
    pub const EXECUTION_STAGE: &'static str = "execution.stage";
    pub const EXECUTION_PARTITION: &'static str = "execution.partition";
    pub const EXECUTION_ATTEMPT: &'static str = "execution.attempt";
    pub const EXECUTION_CHANNEL: &'static str = "execution.channel";
    pub const EXECUTION_TASK_STATUS: &'static str = "execution.task.status";
    pub const EXECUTION_TASK_MESSAGE: &'static str = "execution.task.message";
    pub const EXECUTION_TASK_ERROR_CAUSE: &'static str = "execution.task.error_cause";
    pub const EXECUTION_STREAM_LOCAL_STORAGE: &'static str = "execution.stream.local.storage";
    pub const EXECUTION_STREAM_REMOTE_URI: &'static str = "execution.stream.remote.uri";
    pub const SESSION_ID: &'static str = "session.id";
}

/// The OpenTelemetry span kinds.
pub struct SpanKind;

impl SpanKind {
    pub const CLIENT: &'static str = "client";
    pub const SERVER: &'static str = "server";
    pub const PRODUCER: &'static str = "producer";
    pub const CONSUMER: &'static str = "consumer";
    pub const INTERNAL: &'static str = "internal";
}

/// The OpenTelemetry span status codes.
pub struct SpanStatusCode;

impl SpanStatusCode {
    pub const UNSET: &'static str = "unset";
    pub const OK: &'static str = "ok";
    pub const ERROR: &'static str = "error";
}

/// A trait for associating an object with a span.
/// The extracted information from the associated object
/// can be used for span names and properties.
pub trait SpanAssociation {
    /// The name of the object associated with the span.
    fn name(&self) -> Cow<'static, str>;

    /// The properties of the object associated with the span.
    fn properties(&self) -> impl IntoIterator<Item = (Cow<'static, str>, Cow<'static, str>)>;
}

/// A key-value pair for metric attributes.
/// This is a restriction over [`opentelemetry::KeyValue`] to only allow
/// static string keys and clone-on-write string values.
pub type KeyValue = (&'static str, Cow<'static, str>);
