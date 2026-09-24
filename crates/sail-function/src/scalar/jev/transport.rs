//! Bounded, invocation-owned System One requests. No detached tasks or global row cache.

use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime};

use datafusion_common::{Result, exec_datafusion_err, exec_err};
use futures::{TryStreamExt, stream};
use serde::Serialize;
use serde_json::{Map, Value, json};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

use super::JevKind;
use super::contract::{Options, retry_after, validate_request, validate_response};

pub(crate) struct RequestRow {
    pub state: Value,
    pub questions: Map<String, Value>,
    pub options: Options,
}

#[derive(Debug)]
struct Limits {
    concurrency: usize,
    pending: usize,
    pending_bytes: usize,
    target_questions: usize,
    target_bytes: usize,
    hard_bytes: usize,
}

impl Limits {
    fn from_env() -> Result<Self> {
        fn read(name: &str, default: usize) -> Result<usize> {
            match std::env::var(name) {
                Ok(value) => value
                    .parse()
                    .map_err(|_| exec_datafusion_err!("Invalid positive integer for {name}")),
                Err(std::env::VarError::NotPresent) => Ok(default),
                Err(_) => exec_err!("Invalid environment value for {name}"),
            }
        }
        let value = Self {
            concurrency: read("SAIL_JEV_MAX_CONCURRENCY", 8)?,
            pending: read("SAIL_JEV_MAX_PENDING_REQUESTS", 16)?,
            pending_bytes: read("SAIL_JEV_MAX_PENDING_BYTES", 16 * 1024 * 1024)?,
            target_questions: read("SAIL_JEV_BATCH_TARGET_QUESTIONS", 64)?,
            target_bytes: read("SAIL_JEV_BATCH_TARGET_BYTES", 256 * 1024)?,
            hard_bytes: read("SAIL_JEV_MAX_REQUEST_BYTES", 1024 * 1024)?,
        };
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<()> {
        if [
            self.concurrency,
            self.pending,
            self.pending_bytes,
            self.target_questions,
            self.target_bytes,
            self.hard_bytes,
        ]
        .iter()
        .any(|&v| v == 0 || v > u32::MAX as usize || v > Semaphore::MAX_PERMITS)
        {
            return exec_err!(
                "Jev resource limits must be positive and fit semaphore permit counts"
            );
        }
        if self.pending_bytes < self.hard_bytes {
            return exec_err!(
                "SAIL_JEV_MAX_PENDING_BYTES must be at least SAIL_JEV_MAX_REQUEST_BYTES for forward progress"
            );
        }
        Ok(())
    }
}

#[derive(Default)]
struct Gauge {
    current: AtomicUsize,
    peak: AtomicUsize,
}

impl Gauge {
    fn add(&self, count: usize) {
        let current = self.current.fetch_add(count, Ordering::Relaxed) + count;
        self.peak.fetch_max(current, Ordering::Relaxed);
    }
    fn remove(&self, count: usize) {
        self.current.fetch_sub(count, Ordering::Relaxed);
    }
}

struct Runtime {
    limits: Limits,
    client: reqwest::Client,
    base_url: String,
    active: Arc<Semaphore>,
    pending: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    active_gauge: Gauge,
    pending_gauge: Gauge,
    bytes_gauge: Gauge,
}

impl Runtime {
    fn new(limits: Limits, base_url: String) -> Result<Self> {
        limits.validate()?;
        let url = reqwest::Url::parse(&base_url)
            .map_err(|_| exec_datafusion_err!("Invalid TYPESAFE_BASE_URL"))?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return exec_err!(
                "TYPESAFE_BASE_URL must be an HTTP(S) base URL without credentials, query, or fragment"
            );
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| exec_datafusion_err!("Could not initialize Jev HTTP client"))?;
        Ok(Self {
            active: Arc::new(Semaphore::new(limits.concurrency)),
            pending: Arc::new(Semaphore::new(limits.pending)),
            bytes: Arc::new(Semaphore::new(limits.pending_bytes)),
            limits,
            client,
            base_url: base_url.trim_end_matches('/').to_owned(),
            active_gauge: Gauge::default(),
            pending_gauge: Gauge::default(),
            bytes_gauge: Gauge::default(),
        })
    }

    fn global() -> Result<Arc<Self>> {
        static INSTANCE: OnceLock<std::result::Result<Arc<Runtime>, String>> = OnceLock::new();
        INSTANCE
            .get_or_init(|| {
                let url = std::env::var("TYPESAFE_BASE_URL")
                    .ok()
                    .map(|v| v.trim().to_owned())
                    .filter(|v| !v.is_empty())
                    .unwrap_or_else(|| "https://api.typesafe.ai".to_owned());
                Limits::from_env()
                    .and_then(|limits| Self::new(limits, url))
                    .map(Arc::new)
                    .map_err(|e| e.to_string())
            })
            .as_ref()
            .map(Arc::clone)
            .map_err(|message| exec_datafusion_err!("{message}"))
    }

    async fn reserve(self: &Arc<Self>) -> Result<Reservation> {
        let groups = Arc::clone(&self.pending)
            .acquire_owned()
            .await
            .map_err(|_| exec_datafusion_err!("Jev request queue closed"))?;
        self.pending_gauge.add(1);
        let mut reservation = Reservation {
            runtime: Arc::clone(self),
            _groups: groups,
            bytes: None,
        };
        // Reserve the hard bound before parsing/encoding a group. Never acquire a second
        // byte reservation while holding the first: that can deadlock multiple partitions.
        let bytes = Arc::clone(&self.bytes)
            .acquire_many_owned(self.limits.hard_bytes as u32)
            .await
            .map_err(|_| exec_datafusion_err!("Jev byte queue closed"))?;
        self.bytes_gauge.add(self.limits.hard_bytes);
        reservation.bytes = Some(bytes);
        Ok(reservation)
    }
}

struct Reservation {
    runtime: Arc<Runtime>,
    _groups: OwnedSemaphorePermit,
    bytes: Option<OwnedSemaphorePermit>,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.runtime.pending_gauge.remove(1);
        if self.bytes.is_some() {
            self.runtime
                .bytes_gauge
                .remove(self.runtime.limits.hard_bytes);
        }
    }
}

struct ActiveAttempt<'a> {
    runtime: &'a Runtime,
    _permit: OwnedSemaphorePermit,
}

impl Drop for ActiveAttempt<'_> {
    fn drop(&mut self) {
        self.runtime.active_gauge.remove(1);
    }
}

#[derive(Serialize)]
struct Body<'a> {
    state: &'a Value,
    model: &'a str,
    questions: &'a Map<String, Value>,
}

/// Check actual bytes including escaping and generated IDs, without allocating an
/// oversized encoded payload just to discover that it exceeds the hard limit.
struct BodySize {
    size: usize,
    limit: usize,
}

impl Write for BodySize {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if buf.len() > self.limit.saturating_sub(self.size) {
            return Err(std::io::Error::other(
                "Jev hard request byte limit exceeded",
            ));
        }
        self.size += buf.len();
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn encoded_size(
    state: &Value,
    options: &Options,
    questions: &Map<String, Value>,
    limit: usize,
) -> Result<usize> {
    let mut out = BodySize { size: 0, limit };
    serde_json::to_writer(
        &mut out,
        &Body {
            state,
            model: &options.model,
            questions,
        },
    )
    .map_err(|_| exec_datafusion_err!("Jev request exceeds SAIL_JEV_MAX_REQUEST_BYTES"))?;
    Ok(out.size)
}

struct Group {
    options: Options,
    questions: Map<String, Value>,
    rows: Vec<(usize, Vec<(String, String)>)>,
    body: bytes::Bytes,
    _reservation: Reservation,
}

struct CompareState<'a> {
    expected: &'a [u8],
    offset: usize,
}

impl Write for CompareState<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self
            .expected
            .get(self.offset..self.offset.saturating_add(bytes.len()))
            != Some(bytes)
        {
            return Err(std::io::Error::other("different Jev state"));
        }
        self.offset += bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn same_state(value: &Value, expected: &[u8]) -> bool {
    let mut writer = CompareState {
        expected,
        offset: 0,
    };
    serde_json::to_writer(&mut writer, value).is_ok() && writer.offset == expected.len()
}

fn build_group<F>(
    models: bool,
    next: &mut usize,
    count: usize,
    row_at: &mut F,
    reservation: Reservation,
) -> Result<Option<Group>>
where
    F: FnMut(usize) -> Result<Option<RequestRow>>,
{
    let limits = &reservation.runtime.limits;
    let first = loop {
        if *next == count {
            return Ok(None);
        }
        if let Some(row) = row_at(*next)? {
            break row;
        }
        *next += 1;
    };
    let state = first.state;
    let options = first.options;
    // Structural JSON equality ignores object order and signed zero. Compare the
    // actual serialization, without allocating another candidate-state buffer.
    let mut state_size = BodySize {
        size: 0,
        limit: limits.hard_bytes,
    };
    if !models {
        serde_json::to_writer(&mut state_size, &state)
            .map_err(|_| exec_datafusion_err!("Jev request exceeds SAIL_JEV_MAX_REQUEST_BYTES"))?;
    }
    let mut state_bytes = Vec::with_capacity(state_size.size);
    if !models {
        serde_json::to_writer(&mut state_bytes, &state)
            .map_err(|_| exec_datafusion_err!("Could not encode Jev state"))?;
    }
    let mut candidate = Some(first.questions);
    let mut questions = Map::new();
    let mut rows = Vec::new();
    let mut body_size = 0;
    while *next < count {
        let row_questions = if let Some(questions) = candidate.take() {
            questions
        } else {
            let Some(row) = row_at(*next)? else {
                *next += 1;
                continue;
            };
            if row.options != options || (!models && !same_state(&row.state, &state_bytes)) {
                break;
            }
            row.questions
        };
        if !models {
            validate_request(&state, &row_questions)?;
        }
        if !rows.is_empty()
            && questions.len().saturating_add(row_questions.len()) > limits.target_questions
        {
            break;
        }
        let mut mapping = Vec::with_capacity(row_questions.len());
        for (index, (id, question)) in row_questions.into_iter().enumerate() {
            let wire_id = format!("r{}_q{index}", *next);
            questions.insert(wire_id.clone(), question);
            mapping.push((id, wire_id));
        }
        let encoded = if models {
            Ok(0)
        } else {
            encoded_size(&state, &options, &questions, limits.hard_bytes)
        };
        if !rows.is_empty()
            && encoded
                .as_ref()
                .map_or(true, |&size| size > limits.target_bytes)
        {
            for (_, wire_id) in mapping {
                questions.remove(&wire_id);
            }
            break;
        }
        body_size = encoded?;
        rows.push((*next, mapping));
        *next += 1;
        if !models
            && (questions.len() >= limits.target_questions || body_size >= limits.target_bytes)
        {
            break;
        }
        // Discovery has no questions: bound its row-index bookkeeping as well.
        if models && rows.len() >= limits.target_questions {
            break;
        }
    }
    // A counting pass includes escaping/IDs without keeping two encoded bodies alive.
    // Allocate the exact measured length so Vec growth cannot exceed the reservation.
    drop(state_bytes);
    let mut body = Vec::with_capacity(body_size);
    if !models {
        serde_json::to_writer(
            &mut body,
            &Body {
                state: &state,
                model: &options.model,
                questions: &questions,
            },
        )
        .map_err(|_| exec_datafusion_err!("Could not encode Jev request"))?;
    }
    Ok(Some(Group {
        options,
        questions,
        rows,
        body: body.into(),
        _reservation: reservation,
    }))
}

struct AttemptFailure {
    retry: bool,
    delay: Option<Duration>,
    message: String,
}

async fn send(
    runtime: &Runtime,
    models: bool,
    group: &Group,
) -> std::result::Result<(Value, Option<String>), AttemptFailure> {
    let endpoint = if models { "models" } else { "systemone" };
    let url = format!("{}/v1/{endpoint}", runtime.base_url);
    let request = if models {
        runtime.client.get(url)
    } else {
        // reqwest's Bytes body is cheaply cloned for retries; serialize only once.
        runtime
            .client
            .post(url)
            .header("content-type", "application/json")
            .body(group.body.clone())
    };
    let response = request
        .bearer_auth(&*group.options.api_key)
        .send()
        .await
        .map_err(|e| AttemptFailure {
            retry: e.is_connect() || e.is_timeout() || e.is_body() || e.is_request(),
            delay: None,
            message: "Jev HTTP connection failed".to_owned(),
        })?;
    let status = response.status();
    let request_id = response
        .headers()
        .get("x-typesafe-request-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let delay = retry_after(response.headers(), SystemTime::now());
    let bytes = response.bytes().await.map_err(|_| AttemptFailure {
        retry: true,
        delay: None,
        message: "Jev response body could not be read".to_owned(),
    })?;
    if !status.is_success() {
        let body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        let detail = body
            .get("detail")
            .or_else(|| body.get("message"))
            .or_else(|| body.get("error"));
        let detail = detail.map(Value::to_string).unwrap_or_default();
        // Redact the JSON-escaped spelling too (keys may contain quotes/backslashes).
        let quoted_key = Value::String(group.options.api_key.to_string()).to_string();
        let detail = detail
            .replace(&quoted_key[1..quoted_key.len() - 1], "[REDACTED]")
            .replace(&*group.options.api_key, "[REDACTED]");
        return Err(AttemptFailure {
            retry: status.as_u16() == 408 || status.as_u16() == 429 || status.is_server_error(),
            delay,
            message: format!(
                "Jev HTTP {}: {}",
                status.as_u16(),
                detail.chars().take(512).collect::<String>()
            ),
        });
    }
    let mut value = serde_json::from_slice(&bytes).map_err(|_| AttemptFailure {
        retry: false,
        delay: None,
        message: "Invalid Jev response JSON".to_owned(),
    })?;
    validate_response(&mut value, &group.questions, models).map_err(|e| AttemptFailure {
        retry: false,
        delay: None,
        message: e.to_string(),
    })?;
    Ok((value, request_id))
}

async fn execute(runtime: Arc<Runtime>, models: bool, group: Group) -> Result<Vec<(usize, Value)>> {
    let mut started = None;
    let mut retries = 0usize;
    let mut last_error = String::new();
    let (response, request_id) = loop {
        let permit = Arc::clone(&runtime.active)
            .acquire_owned()
            .await
            .map_err(|_| exec_datafusion_err!("Jev HTTP semaphore closed"))?;
        if started.is_some_and(|start: Instant| start.elapsed() >= group.options.retry_budget) {
            return exec_err!("{last_error}");
        }
        let start = *started.get_or_insert_with(Instant::now);
        runtime.active_gauge.add(1);
        let active = ActiveAttempt {
            runtime: &runtime,
            _permit: permit,
        };
        let result = tokio::time::timeout(group.options.timeout, send(&runtime, models, &group))
            .await
            .unwrap_or_else(|_| {
                Err(AttemptFailure {
                    retry: true,
                    delay: None,
                    message: "Jev HTTP attempt timed out".to_owned(),
                })
            });
        drop(active);
        match result {
            Ok(value) => break value,
            Err(failure) => {
                last_error = failure.message;
                if !failure.retry || retries >= group.options.max_retries {
                    return exec_err!("{last_error}");
                }
                let delay = failure.delay.unwrap_or_else(|| {
                    let base = (0.5 * 2f64.powi(retries.min(4) as i32)).min(5.0);
                    Duration::from_millis(
                        (base * (1.0 - rand::random::<f64>() * 0.25) * 1000.0).round() as u64,
                    )
                });
                if start.elapsed().saturating_add(delay) >= group.options.retry_budget {
                    return exec_err!("{last_error}");
                }
                tokio::time::sleep(delay).await;
                retries += 1;
            }
        }
    };
    let batch_id = uuid::Uuid::new_v4().to_string();
    let mut outputs = Vec::with_capacity(group.rows.len());
    for (row, mapping) in &group.rows {
        let output = if models {
            json!({"models":response["models"],"request_id":request_id})
        } else {
            let mut answers = Map::new();
            for (original, wire) in mapping {
                answers.insert(original.clone(), response["answers"][wire].clone());
            }
            json!({"answers":answers,"model":response["model"],"usage":response["usage"],"request_id":request_id,"batch_id":batch_id})
        };
        outputs.push((*row, output));
    }
    Ok(outputs)
}

pub(crate) async fn evaluate<F>(
    kind: JevKind,
    number_rows: usize,
    row: F,
) -> Result<Vec<Option<Value>>>
where
    F: FnMut(usize) -> Result<Option<RequestRow>> + Send,
{
    if number_rows == 0 {
        return Ok(Vec::new());
    }
    let runtime = Runtime::global()?;
    let models = kind == JevKind::Models;
    let generator_runtime = Arc::clone(&runtime);
    let groups = stream::try_unfold((0usize, row), move |(mut next, mut row)| {
        let runtime = Arc::clone(&generator_runtime);
        async move {
            if next == number_rows {
                return Ok(None);
            }
            let reservation = runtime.reserve().await?;
            let group = build_group(models, &mut next, number_rows, &mut row, reservation)?;
            Ok(group.map(|group| (group, (next, row))))
        }
    });
    let results = groups
        .map_ok(|group| execute(Arc::clone(&runtime), models, group))
        .try_buffer_unordered(runtime.limits.pending);
    futures::pin_mut!(results);
    let mut output = vec![None; number_rows];
    while let Some(rows) = results.try_next().await? {
        for (index, value) in rows {
            output[index] = Some(value);
        }
    }
    log::debug!(target: "sail_function::scalar::jev", "Jev resource peaks: active_attempts={} pending_requests={} pending_bytes={}",
        runtime.active_gauge.peak.load(Ordering::Relaxed), runtime.pending_gauge.peak.load(Ordering::Relaxed), runtime.bytes_gauge.peak.load(Ordering::Relaxed));
    Ok(output)
}

#[cfg(test)]
#[expect(
    clippy::expect_used,
    reason = "test setup and successful reservations must succeed"
)]
mod tests {
    use super::*;

    fn limits() -> Limits {
        Limits {
            concurrency: 2,
            pending: 2,
            pending_bytes: 2048,
            hard_bytes: 1024,
            target_bytes: 512,
            target_questions: 4,
        }
    }

    #[test]
    fn resource_limits_must_admit_every_allowed_request() {
        let mut value = limits();
        assert!(value.validate().is_ok());
        value.pending_bytes = value.hard_bytes - 1;
        assert!(value.validate().is_err());
        value = limits();
        value.concurrency = 0;
        assert!(value.validate().is_err());
    }

    #[tokio::test]
    async fn dropping_reservations_and_waiters_restores_capacity() {
        let runtime =
            Arc::new(Runtime::new(limits(), "http://localhost".to_owned()).expect("runtime"));
        let a = runtime.reserve().await.expect("reserve");
        let b = runtime.reserve().await.expect("reserve");
        assert_eq!(runtime.bytes.available_permits(), 0);
        assert!(
            tokio::time::timeout(Duration::from_millis(1), runtime.reserve())
                .await
                .is_err()
        );
        drop(a);
        drop(b);
        assert_eq!(runtime.bytes.available_permits(), 2048);
        assert_eq!(runtime.pending.available_permits(), 2);
        assert_eq!(runtime.pending_gauge.current.load(Ordering::Relaxed), 0);
        assert_eq!(runtime.bytes_gauge.current.load(Ordering::Relaxed), 0);
        let _next = runtime.reserve().await.expect("capacity restored");
    }

    #[tokio::test]
    async fn cancellation_after_group_admission_releases_partial_reservation() {
        let mut config = limits();
        config.pending_bytes = config.hard_bytes;
        let runtime =
            Arc::new(Runtime::new(config, "http://localhost".to_owned()).expect("runtime"));
        let held = runtime.reserve().await.expect("reserve");
        assert!(
            tokio::time::timeout(Duration::from_millis(1), runtime.reserve())
                .await
                .is_err()
        );
        assert_eq!(runtime.pending_gauge.peak.load(Ordering::Relaxed), 2);
        assert_eq!(runtime.pending_gauge.current.load(Ordering::Relaxed), 1);
        assert_eq!(runtime.pending.available_permits(), 1);
        drop(held);
        assert_eq!(runtime.bytes.available_permits(), 1024);
        assert_eq!(runtime.pending.available_permits(), 2);
        assert_eq!(runtime.pending_gauge.current.load(Ordering::Relaxed), 0);
        assert_eq!(runtime.bytes_gauge.current.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn batching_compares_serialized_state_not_json_equality() {
        let negative: Value = serde_json::from_str("[-0.0]").expect("json");
        let positive: Value = serde_json::from_str("[0.0]").expect("json");
        assert_eq!(negative, positive);
        let bytes = serde_json::to_vec(&negative).expect("encode");
        assert!(same_state(&negative, &bytes));
        assert!(!same_state(&positive, &bytes));
    }
}
