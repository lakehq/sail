//! System One's wire contract. See the pinned Python SDK and OpenAPI test fixtures.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use datafusion_common::{Result, exec_err};
use reqwest::header::HeaderMap;
use serde_json::{Map, Value};

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Options {
    pub api_key: Arc<str>,
    pub model: Arc<str>,
    pub timeout: Duration,
    pub retry_budget: Duration,
    pub max_retries: usize,
}

impl std::fmt::Debug for Options {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JevOptions").finish_non_exhaustive()
    }
}

fn env_default(name: &str, default: &str) -> String {
    std::env::var(name)
        .ok()
        .map(|x| x.trim().to_owned())
        .filter(|x| !x.is_empty())
        .unwrap_or_else(|| default.to_owned())
}

impl Options {
    pub fn parse(value: Option<&Value>, models: bool) -> Result<Self> {
        let empty = Map::new();
        let values = match value {
            None | Some(Value::Null) => &empty,
            Some(Value::Object(values)) => values,
            _ => return exec_err!("Jev options must be a string map"),
        };
        for (key, value) in values {
            if !matches!(
                key.as_str(),
                "api_key" | "model" | "timeout_ms" | "retry_budget_ms" | "max_retries"
            ) {
                return exec_err!(
                    "Unknown Jev option (allowed: api_key, model, timeout_ms, retry_budget_ms, max_retries)"
                );
            }
            if !value.is_string() {
                return exec_err!("Jev option values must be non-null strings");
            }
        }
        if models && values.contains_key("model") {
            return exec_err!("jev_models does not accept the model option");
        }
        let api_key = values
            .get("api_key")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| env_default("TYPESAFE_API_KEY", ""));
        let api_key = api_key.trim();
        if api_key.is_empty() || !api_key.bytes().all(|x| (b'!'..=b'~').contains(&x)) {
            return exec_err!(
                "Jev requires a nonempty API key containing printable ASCII without whitespace; set TYPESAFE_API_KEY or api_key"
            );
        }
        let integer = |key: &str, default: u64, positive: bool| -> Result<u64> {
            let value = match values.get(key).and_then(Value::as_str) {
                Some(value) => value.parse::<u64>().map_err(|_| {
                    datafusion_common::exec_datafusion_err!(
                        "Invalid Jev {key}: expected an integer"
                    )
                })?,
                None => default,
            };
            if positive && value == 0 {
                return exec_err!("Jev {key} must be positive");
            }
            Ok(value)
        };
        Ok(Self {
            api_key: Arc::from(api_key),
            model: Arc::from(
                values
                    .get("model")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .unwrap_or_else(|| env_default("TYPESAFE_DEFAULT_MODEL", "jev-latest")),
            ),
            timeout: Duration::from_millis(integer("timeout_ms", 10_000, true)?),
            retry_budget: Duration::from_millis(integer("retry_budget_ms", 30_000, true)?),
            max_retries: usize::try_from(integer("max_retries", 2, false)?).map_err(|_| {
                datafusion_common::exec_datafusion_err!("Jev max_retries is too large")
            })?,
        })
    }
}

fn content(value: &Value) -> bool {
    matches!(value, Value::String(_) | Value::Object(_) | Value::Array(_))
}

pub(crate) fn validate_request(state: &Value, questions: &Map<String, Value>) -> Result<()> {
    if !content(state) {
        return exec_err!(
            "Jev state must be a JSON string, object, or array; JSON null is not SQL NULL"
        );
    }
    if questions.is_empty() {
        return exec_err!("Jev questions must be a nonempty object");
    }
    for question in questions.values() {
        let Some(question) = question.as_object() else {
            return exec_err!("Each Jev question must be an object");
        };
        if let Some(instructions) = question.get("instructions")
            && !instructions.is_null()
            && !content(instructions)
        {
            return exec_err!("Jev instructions must be a string, object, array, or null");
        }
        match question.get("type").and_then(Value::as_str) {
            Some("noul") => {
                if let Some(criteria) = question.get("criteria").filter(|v| !v.is_null()) {
                    let Some(criteria) = criteria.as_object() else {
                        return exec_err!("Jev Noul criteria must be an object or null");
                    };
                    for (key, value) in criteria {
                        if !matches!(key.as_str(), "true" | "false")
                            || (!value.is_null() && !content(value))
                        {
                            return exec_err!(
                                "Jev Noul criteria accept only true/false descriptions (string, object, array, or null)"
                            );
                        }
                    }
                }
            }
            Some("choice") => {
                let Some(criteria) = question.get("criteria").and_then(Value::as_object) else {
                    return exec_err!("Jev Choice criteria must be an object");
                };
                if criteria.values().any(|v| !v.is_null() && !content(v)) {
                    return exec_err!(
                        "Jev Choice descriptions must be strings, objects, arrays, or null"
                    );
                }
            }
            Some("score") => {
                let Some(criteria) = question.get("criteria").and_then(Value::as_array) else {
                    return exec_err!("Jev Score criteria must be a nonempty array");
                };
                if criteria.is_empty() || criteria.iter().any(|v| !content(v)) {
                    return exec_err!(
                        "Jev Score requires at least one non-null string, object, or array level"
                    );
                }
            }
            _ => return exec_err!("Jev question type must be noul, choice, or score"),
        }
    }
    Ok(())
}

fn invalid(path: &str) -> datafusion_common::DataFusionError {
    datafusion_common::exec_datafusion_err!("Invalid Jev response at {path}")
}

fn probability(value: Option<&Value>) -> bool {
    value
        .and_then(Value::as_f64)
        .is_some_and(|v| v.is_finite() && (0.0..=1.0).contains(&v))
}

pub(crate) fn validate_response(
    value: &mut Value,
    questions: &Map<String, Value>,
    models: bool,
) -> Result<()> {
    if models {
        let items = value
            .get("models")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("models"))?;
        for item in items {
            for field in ["name", "description", "release_date"] {
                if !item.get(field).is_some_and(Value::is_string) {
                    return Err(invalid(&format!("models.{field}")));
                }
            }
        }
        return Ok(());
    }
    if !value.get("model").is_some_and(Value::is_string) {
        return Err(invalid("model"));
    }
    let usage = value
        .get_mut("usage")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| invalid("usage"))?;
    for name in ["input_tokens", "output_tokens"] {
        let count = usage.entry(name).or_insert(Value::Null);
        if !count.is_null() && !count.as_i64().is_some_and(|v| v >= 0) {
            return Err(invalid(&format!("usage.{name}")));
        }
    }
    let answers = value
        .get("answers")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("answers"))?;
    for (id, question) in questions {
        let answer = answers
            .get(id)
            .and_then(Value::as_object)
            .ok_or_else(|| invalid("answers (missing requested question)"))?;
        if answer.get("type") != question.get("type") {
            return Err(invalid("answers.type"));
        }
        if question["type"] == "noul" {
            if !probability(answer.get("noul")) {
                return Err(invalid("answers.noul"));
            }
            continue;
        }
        if !probability(answer.get("confidence")) {
            return Err(invalid("answers.confidence"));
        }
        let probabilities = answer
            .get("probabilities")
            .and_then(Value::as_object)
            .ok_or_else(|| invalid("answers.probabilities"))?;
        let expected: Vec<String> = if question["type"] == "choice" {
            let criteria = question["criteria"]
                .as_object()
                .ok_or_else(|| invalid("choice criteria"))?;
            if !answer
                .get("choice")
                .and_then(Value::as_str)
                .is_some_and(|v| criteria.contains_key(v))
            {
                return Err(invalid("answers.choice"));
            }
            criteria.keys().cloned().collect()
        } else {
            let criteria = question["criteria"]
                .as_array()
                .ok_or_else(|| invalid("score criteria"))?;
            if !answer
                .get("score")
                .and_then(Value::as_f64)
                .is_some_and(|v| v.is_finite() && v >= 0.0 && v <= (criteria.len() - 1) as f64)
            {
                return Err(invalid("answers.score"));
            }
            let legend = answer
                .get("legend")
                .and_then(Value::as_object)
                .ok_or_else(|| invalid("answers.legend"))?;
            if legend.len() != criteria.len()
                || (0..criteria.len()).any(|i| !legend.get(&i.to_string()).is_some_and(content))
            {
                return Err(invalid("answers.legend"));
            }
            (0..criteria.len()).map(|i| i.to_string()).collect()
        };
        if probabilities.len() != expected.len()
            || expected.iter().any(|k| !probability(probabilities.get(k)))
        {
            return Err(invalid("answers.probabilities"));
        }
    }
    Ok(())
}

/// Python's parse_retry_after: milliseconds first, empty numeric headers mean zero,
/// invalid milliseconds fall through, and HTTP-date values are clamped at zero.
pub(crate) fn retry_after(headers: &HeaderMap, now: SystemTime) -> Option<Duration> {
    for (name, multiplier) in [("retry-after-ms", 1.0), ("retry-after", 1000.0)] {
        let Some(raw) = headers.get(name).and_then(|v| v.to_str().ok()) else {
            continue;
        };
        let raw = raw.trim();
        match python_header_number(raw) {
            Some(value) if value.is_finite() && value >= 0.0 => {
                // Python validates the millisecond conversion before returning the delay.
                // Overflow here is invalid, whereas a finite value beyond Rust's Duration
                // range still represents a valid server delay that exhausts the budget.
                let milliseconds = value * multiplier;
                if milliseconds.is_finite() {
                    if let Ok(delay) = Duration::try_from_secs_f64(milliseconds / 1000.0) {
                        return Some(delay);
                    }
                    return Some(Duration::MAX);
                }
            }
            Some(value) if value.is_finite() && value < 0.0 && name == "retry-after" => {
                return None;
            }
            None if name == "retry-after" => {
                let date = chrono::DateTime::parse_from_rfc2822(raw)
                    .ok()
                    .map(|d| d.timestamp())
                    .or_else(|| {
                        let mut parsed = chrono::format::Parsed::new();
                        chrono::format::parse(
                            &mut parsed,
                            raw,
                            chrono::format::StrftimeItems::new("%A, %d-%b-%y %H:%M:%S GMT"),
                        )
                        .ok()?;
                        // email.utils uses 1969..2068 for two-digit years; chrono's
                        // default window starts at 1970. Python also ignores weekdays.
                        if let Some(year) = parsed.year_mod_100 {
                            parsed.year_div_100 = Some(if year >= 69 { 19 } else { 20 });
                        }
                        parsed.weekday = None;
                        parsed
                            .to_naive_datetime_with_offset(0)
                            .ok()
                            .map(|d| d.and_utc().timestamp())
                    })
                    .or_else(|| {
                        // Python parsedate_to_datetime returns a naive datetime for
                        // asctime values, whose timestamp uses the worker's timezone.
                        chrono::NaiveDateTime::parse_from_str(raw, "%a %b %e %H:%M:%S %Y")
                            .ok()?
                            .and_local_timezone(chrono::Local)
                            .earliest()
                            .map(|d| d.timestamp())
                    });
                if let Some(timestamp) = date {
                    let seconds = timestamp as f64
                        - now
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs_f64();
                    return Duration::try_from_secs_f64(seconds.max(0.0)).ok();
                }
            }
            _ => {}
        }
    }
    None
}

fn python_header_number(raw: &str) -> Option<f64> {
    let raw = if raw.is_empty() { "0" } else { raw };
    if !raw.contains('_') {
        return raw.parse().ok();
    }
    // Python float() permits single underscores between digits, including exponents.
    let bytes = raw.as_bytes();
    if bytes.iter().enumerate().any(|(i, &byte)| {
        byte == b'_'
            && (i == 0
                || i + 1 == bytes.len()
                || !bytes[i - 1].is_ascii_digit()
                || !bytes[i + 1].is_ascii_digit())
    }) {
        return None;
    }
    raw.replace('_', "").parse().ok()
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "literal test fixtures must be valid")]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn headers_follow_python_precedence() {
        let mut headers = HeaderMap::new();
        headers.insert("retry-after", "10".parse().expect("header"));
        headers.insert("retry-after-ms", "12000".parse().expect("header"));
        assert_eq!(
            retry_after(&headers, SystemTime::UNIX_EPOCH),
            Some(Duration::from_secs(12))
        );
        headers.insert("retry-after-ms", "-1".parse().expect("header"));
        assert_eq!(
            retry_after(&headers, SystemTime::UNIX_EPOCH),
            Some(Duration::from_secs(10))
        );
        headers.insert("retry-after-ms", "".parse().expect("header"));
        assert_eq!(
            retry_after(&headers, SystemTime::UNIX_EPOCH),
            Some(Duration::ZERO)
        );
        headers.remove("retry-after-ms");
        headers.insert(
            "retry-after",
            "Thu, 01 Jan 1970 00:00:05 GMT".parse().expect("header"),
        );
        assert_eq!(
            retry_after(&headers, SystemTime::UNIX_EPOCH),
            Some(Duration::from_secs(5))
        );
    }

    #[test]
    fn retry_headers_match_python_numeric_edges() {
        for (header, raw, expected) in [
            ("retry-after", "1e306", None),
            ("retry-after-ms", "1e308", Some(Duration::MAX)),
            ("retry-after-ms", "1_0", Some(Duration::from_millis(10))),
            (
                "retry-after",
                "1e1_0",
                Some(Duration::from_secs(10_000_000_000)),
            ),
            ("retry-after-ms", "1__0", None),
            ("retry-after-ms", "_10", None),
            ("retry-after-ms", "10_", None),
            ("retry-after-ms", "1_e1", None),
            ("retry-after", "NaN", None),
            ("retry-after", "inf", None),
            ("retry-after", "-1", None),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(header, raw.parse().expect("header"));
            assert_eq!(
                retry_after(&headers, SystemTime::UNIX_EPOCH),
                expected,
                "{header}: {raw}"
            );
        }
        let mut headers = HeaderMap::new();
        headers.insert("retry-after-ms", "1e309".parse().expect("header"));
        headers.insert("retry-after", "2".parse().expect("header"));
        assert_eq!(
            retry_after(&headers, SystemTime::UNIX_EPOCH),
            Some(Duration::from_secs(2))
        );
    }

    #[test]
    fn retry_headers_accept_http_dates_and_clamp_the_past() {
        for raw in [
            "Thu, 01 Jan 1970 00:00:05 GMT",
            "Thursday, 01-Jan-70 00:00:05 GMT",
        ] {
            let mut headers = HeaderMap::new();
            headers.insert("retry-after", raw.parse().expect("header"));
            assert_eq!(
                retry_after(&headers, SystemTime::UNIX_EPOCH),
                Some(Duration::from_secs(5)),
                "{raw}"
            );
            assert_eq!(
                retry_after(&headers, SystemTime::UNIX_EPOCH + Duration::from_secs(10)),
                Some(Duration::ZERO),
                "{raw}"
            );
        }
        let mut headers = HeaderMap::new();
        headers.insert(
            "retry-after",
            "Wednesday, 01-Jan-69 00:00:05 GMT".parse().expect("header"),
        );
        assert_eq!(
            retry_after(&headers, SystemTime::UNIX_EPOCH),
            Some(Duration::ZERO)
        );
        let local_date = chrono::DateTime::<chrono::Local>::from(
            SystemTime::UNIX_EPOCH + Duration::from_secs(5),
        )
        .format("%a %b %e %H:%M:%S %Y")
        .to_string();
        headers.insert("retry-after", local_date.parse().expect("header"));
        assert_eq!(
            retry_after(&headers, SystemTime::UNIX_EPOCH),
            Some(Duration::from_secs(5)),
        );
    }

    #[test]
    fn counts_are_nullable_but_usage_and_answers_are_required() {
        let questions = json!({"q":{"type":"noul"}})
            .as_object()
            .expect("object")
            .clone();
        let mut response =
            json!({"model":"jev", "usage":{}, "answers":{"q":{"type":"noul","noul":0.7}}});
        validate_response(&mut response, &questions, false).expect("valid");
        assert!(response["usage"]["input_tokens"].is_null());
        response.as_object_mut().expect("object").remove("usage");
        assert!(validate_response(&mut response, &questions, false).is_err());
    }
}
