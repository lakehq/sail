//! Bounded, invocation-owned System One requests. No detached tasks or global row cache.

use std::collections::BTreeMap;
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

use super::contract::{Options, retry_after, validate_request, validate_response};
use super::output::ResponseRow;
use super::{InputValue, JevKind};

pub(crate) struct RequestRow {
    pub state: InputValue,
    pub questions: BTreeMap<String, InputValue>,
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
        // Keep these limits configurable because API capacity, worker resources, and workload sizes vary.
        // Fixed limits would require a rebuild to tune throughput, memory use, or accepted request sizes.
        let value = Self {
            // Controls pressure on the API. A lower value can reduce throttling; a higher value can improve throughput when capacity permits.
            concurrency: read("TYPESAFE_JEV_MAX_CONCURRENCY", 8)?,
            // Bounds admitted work, including active requests. Different workers can support different amounts of pending work.
            pending: read("TYPESAFE_JEV_MAX_PENDING_REQUESTS", 16)?,
            // Bounds memory reserved for request bodies. A small worker and a large worker need different budgets.
            pending_bytes: read("TYPESAFE_JEV_MAX_PENDING_BYTES", 16 * 1024 * 1024)?,
            // Controls how many questions Sail tries to combine. The useful batch size depends on question complexity and service behavior.
            target_questions: read("TYPESAFE_JEV_BATCH_TARGET_QUESTIONS", 64)?,
            // Controls the preferred request size. Small text and large structured inputs have different batching needs.
            target_bytes: read("TYPESAFE_JEV_BATCH_TARGET_BYTES", 256 * 1024)?,
            // Sets Sail’s hard request-size limit. A fixed value could reject a legitimate workload even when the provider accepts it.
            hard_bytes: read("TYPESAFE_JEV_MAX_REQUEST_BYTES", 1024 * 1024)?,
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
                "TYPESAFE_JEV_MAX_PENDING_BYTES must be at least TYPESAFE_JEV_MAX_REQUEST_BYTES for forward progress"
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
    state: &'a InputValue,
    model: &'a str,
    questions: &'a BTreeMap<String, InputValue>,
}

// Check actual bytes including escaping and generated IDs, without allocating an
// oversized encoded payload just to discover that it exceeds the hard limit.
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
    state: &InputValue,
    options: &Options,
    questions: &BTreeMap<String, InputValue>,
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
    .map_err(|_| exec_datafusion_err!("Jev request exceeds TYPESAFE_JEV_MAX_REQUEST_BYTES"))?;
    Ok(out.size)
}

struct Group {
    options: Options,
    questions: Map<String, Value>,
    rows: Vec<(usize, Vec<(String, String)>)>,
    body: bytes::Bytes,
    _reservation: Reservation,
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
    if !models && state.json.get().len() > limits.hard_bytes {
        return exec_err!("Jev request exceeds TYPESAFE_JEV_MAX_REQUEST_BYTES");
    }
    let mut candidate = Some(first.questions);
    let mut questions = BTreeMap::new();
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
            if row.options != options || (!models && row.state.json.get() != state.json.get()) {
                break;
            }
            row.questions
        };
        if !models {
            validate_request(
                &state.parsed,
                row_questions
                    .values()
                    .map(|question| question.parsed.as_ref()),
            )?;
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
        questions: questions
            .into_iter()
            .map(|(id, question)| (id, Arc::unwrap_or_clone(question.parsed)))
            .collect(),
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
) -> std::result::Result<(Value, Option<String>, bytes::Bytes), AttemptFailure> {
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
    let retry_headers = response.headers().clone();
    let bytes = response.bytes().await.map_err(|_| AttemptFailure {
        retry: true,
        delay: None,
        message: "Jev response body could not be read".to_owned(),
    })?;
    if !status.is_success() {
        let detail = serde_json::from_slice::<Value>(&bytes)
            .map(|body| body.to_string())
            .unwrap_or_else(|_| String::from_utf8_lossy(&bytes).into_owned());
        // Redact the JSON-escaped spelling too (keys may contain quotes/backslashes).
        let quoted_key = Value::String(group.options.api_key.to_string()).to_string();
        let detail = detail
            .replace(&quoted_key[1..quoted_key.len() - 1], "[REDACTED]")
            .replace(&*group.options.api_key, "[REDACTED]");
        return Err(AttemptFailure {
            retry: status.as_u16() == 408 || status.as_u16() == 429 || status.is_server_error(),
            delay: retry_after(&retry_headers, SystemTime::now()),
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
    Ok((value, request_id, bytes))
}

async fn execute(
    runtime: Arc<Runtime>,
    kind: JevKind,
    group: Group,
) -> Result<Vec<(usize, ResponseRow)>> {
    let models = kind == JevKind::Models;
    let mut started = None;
    let mut retries = 0usize;
    let mut last_error = String::new();
    let (response, request_id, response_bytes) = loop {
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
    // Keep structured answer values in their original JSON form until VARIANT encoding.
    let raw_answers: BTreeMap<String, &serde_json::value::RawValue> =
        if matches!(kind, JevKind::Score | JevKind::SystemOne) {
            let raw: BTreeMap<String, &serde_json::value::RawValue> =
                serde_json::from_slice(&response_bytes)
                    .map_err(|_| exec_datafusion_err!("Invalid Jev response JSON"))?;
            let answers = raw
                .get("answers")
                .ok_or_else(|| exec_datafusion_err!("Invalid Jev response at answers"))?;
            serde_json::from_str(answers.get())
                .map_err(|_| exec_datafusion_err!("Invalid Jev response at answers"))?
        } else {
            BTreeMap::new()
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
        let variants = match kind {
            JevKind::SystemOne => mapping
                .iter()
                .map(|(original, wire)| {
                    let answer = raw_answers
                        .get(wire)
                        .ok_or_else(|| exec_datafusion_err!("Invalid Jev response at answers"))?;
                    Ok((original.clone(), (*answer).to_owned()))
                })
                .collect::<Result<_>>()?,
            JevKind::Score => {
                let answer = mapping
                    .first()
                    .and_then(|(_, wire)| raw_answers.get(wire))
                    .ok_or_else(|| exec_datafusion_err!("Invalid Jev response at answers"))?;
                let fields: BTreeMap<String, &serde_json::value::RawValue> =
                    serde_json::from_str(answer.get())
                        .map_err(|_| exec_datafusion_err!("Invalid Jev response at answers"))?;
                let legend = fields.get("legend").ok_or_else(|| {
                    exec_datafusion_err!("Invalid Jev response at answers.legend")
                })?;
                serde_json::from_str(legend.get())
                    .map_err(|_| exec_datafusion_err!("Invalid Jev response at answers.legend"))?
            }
            _ => BTreeMap::new(),
        };
        let mut output = output;
        if kind == JevKind::SystemOne {
            if let Some(object) = output.as_object_mut() {
                object.remove("answers");
            }
        } else if kind == JevKind::Score
            && let Some(answer) = output["answers"]["result"].as_object_mut()
        {
            answer.remove("legend");
        }
        outputs.push((
            *row,
            ResponseRow {
                value: output,
                variants,
            },
        ));
    }
    Ok(outputs)
}

pub(crate) async fn evaluate<F>(
    kind: JevKind,
    number_rows: usize,
    row: F,
) -> Result<Vec<Option<ResponseRow>>>
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
        .map_ok(|group| execute(Arc::clone(&runtime), kind, group))
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
        let negative = InputValue::from_json("[-0.0]".to_owned()).expect("json");
        let positive = InputValue::from_json("[0.0]".to_owned()).expect("json");
        assert_eq!(negative.parsed, positive.parsed);
        assert_ne!(negative.json.get(), positive.json.get());
    }
}
