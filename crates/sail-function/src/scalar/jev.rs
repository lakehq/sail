//! SQL functions for TypeSafe AI's Jev System One API.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use arrow::array::{
    Array, ArrayRef, Float64Array, Float64Builder, MapBuilder, StringArray, StringBuilder,
    StructArray,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field};
use datafusion_common::{Result, exec_err, plan_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use reqwest::StatusCode;
use reqwest::blocking::Client;
use serde_json::{Value, json};

use crate::scalar::json::json_value_from_array;

const ENDPOINT: &str = "https://api.typesafe.ai/v1/systemone";
const DEFAULT_MODEL: &str = "jev-latest";
const MAX_BATCH_ROWS: usize = 16;
const MAX_BATCH_QUESTIONS: usize = 64;
const MAX_BATCH_BYTES: usize = 32_000;

struct RowRequest {
    row: usize,
    request: Value,
    key: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JevKind {
    Noul,
    Choice,
    Score,
    Evaluate,
}

impl JevKind {
    /// Returns the SQL function name for this Jev operation.
    fn name(self) -> &'static str {
        match self {
            Self::Noul => "jev_noul",
            Self::Choice => "jev_choice",
            Self::Score => "jev_score",
            Self::Evaluate => "jev_evaluate",
        }
    }

    /// Returns the valid argument counts, with the optional model last.
    fn arities(self) -> (usize, usize) {
        match self {
            Self::Choice | Self::Score => (4, 5),
            Self::Noul | Self::Evaluate => (3, 4),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct JevFunction {
    kind: JevKind,
    signature: Signature,
}

impl JevFunction {
    /// Creates one of the four Jev SQL functions.
    pub fn new(kind: JevKind) -> Self {
        Self {
            kind,
            signature: Signature::variadic_any(Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for JevFunction {
    fn name(&self) -> &str {
        self.kind.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let (min, max) = self.kind.arities();
        if !(min..=max).contains(&arg_types.len()) {
            return plan_err!("{} expects {min} or {max} arguments", self.name());
        }
        let text_positions: &[usize] = match self.kind {
            JevKind::Noul | JevKind::Evaluate => &[2],
            JevKind::Choice | JevKind::Score => &[3],
        };
        for &position in text_positions {
            if !is_string(&arg_types[position]) {
                return plan_err!("{} expects a string API key", self.name());
            }
        }
        if arg_types.len() == max && !is_string(&arg_types[max - 1]) {
            return plan_err!("{} expects a string model name", self.name());
        }
        if !is_string(&arg_types[1]) && !matches!(self.kind, JevKind::Evaluate) {
            return plan_err!("{} expects string instructions", self.name());
        }
        Ok(match self.kind {
            JevKind::Noul => DataType::Float64,
            JevKind::Evaluate => DataType::Utf8,
            JevKind::Choice => DataType::Struct(
                vec![
                    Arc::new(Field::new("choice", DataType::Utf8, true)),
                    Arc::new(Field::new("confidence", DataType::Float64, true)),
                    Arc::new(Field::new(
                        "probabilities",
                        map_type(DataType::Float64),
                        true,
                    )),
                ]
                .into(),
            ),
            JevKind::Score => DataType::Struct(
                vec![
                    Arc::new(Field::new("score", DataType::Float64, true)),
                    Arc::new(Field::new("confidence", DataType::Float64, true)),
                    Arc::new(Field::new(
                        "probabilities",
                        map_type(DataType::Float64),
                        true,
                    )),
                    Arc::new(Field::new("legend", map_type(DataType::Utf8), true)),
                ]
                .into(),
            ),
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let is_scalar = args
            .args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let (min, max) = self.kind.arities();
        if !(min..=max).contains(&arrays.len()) {
            return exec_err!("{} expects {min} or {max} arguments", self.name());
        }
        let rows = arrays.first().map_or(0, |array| array.len());
        if arrays.iter().any(|array| array.len() != rows) {
            return exec_err!("{} arguments have different row counts", self.name());
        }

        let results = std::thread::scope(|scope| {
            scope
                .spawn(|| evaluate_rows(self.kind, ENDPOINT, &arrays, rows))
                .join()
                .map_err(|_| {
                    datafusion_common::DataFusionError::Execution(
                        "Jev request worker panicked".to_string(),
                    )
                })?
        })?;
        let result = build_result(self.kind, results)?;
        if is_scalar {
            Ok(ColumnarValue::Scalar(
                datafusion_common::ScalarValue::try_from_array(&result, 0)?,
            ))
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }
}

/// Evaluates non-null rows in bounded multi-question requests outside the query runtime.
fn evaluate_rows(
    kind: JevKind,
    endpoint: &str,
    arrays: &[ArrayRef],
    rows: usize,
) -> Result<Vec<Option<Value>>> {
    let mut results = vec![None; rows];
    let mut pending: Vec<RowRequest> = Vec::new();
    let mut pending_questions = 0;
    let mut pending_bytes = 0;
    for row in 0..rows {
        if arrays.iter().any(|array| array.is_null(row)) {
            continue;
        }
        let (request, key) = request_for_row(kind, arrays, row)?;
        let question_count = request["questions"]
            .as_object()
            .map_or(0, serde_json::Map::len);
        let request_bytes = request.to_string().len();
        if !pending.is_empty()
            && (pending.len() >= MAX_BATCH_ROWS
                || pending_questions + question_count > MAX_BATCH_QUESTIONS
                || pending_bytes + request_bytes > MAX_BATCH_BYTES
                || pending[0].key != key
                || pending[0].request["model"] != request["model"])
        {
            flush_batch(endpoint, &mut pending, &mut results)?;
            pending_questions = 0;
            pending_bytes = 0;
        }
        pending.push(RowRequest { row, request, key });
        pending_questions += question_count;
        pending_bytes += request_bytes;
    }
    flush_batch(endpoint, &mut pending, &mut results)?;
    Ok(results)
}

/// Sends one batch and maps its named answers back to the original rows.
fn flush_batch(
    endpoint: &str,
    pending: &mut Vec<RowRequest>,
    results: &mut [Option<Value>],
) -> Result<()> {
    if pending.is_empty() {
        return Ok(());
    }
    if pending.len() == 1 {
        let row = pending.pop().expect("batch has one row");
        results[row.row] = Some(call_jev(endpoint, &row.key, &row.request)?);
        return Ok(());
    }

    let model = pending[0].request["model"].clone();
    let key = &pending[0].key;
    let states = pending
        .iter()
        .map(|row| row.request["state"].clone())
        .collect::<Vec<_>>();
    let mut questions = serde_json::Map::new();
    let mut names = Vec::new();
    for (item_index, row) in pending.iter().enumerate() {
        let row_questions = row.request["questions"].as_object().ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "Jev questions must be an object".to_string(),
            )
        })?;
        for (question_index, (name, question)) in row_questions.iter().enumerate() {
            let mut question = question.clone();
            let instructions = question.get_mut("instructions").ok_or_else(|| {
                datafusion_common::DataFusionError::Execution(
                    "Jev question is missing instructions".to_string(),
                )
            })?;
            *instructions = scoped_instructions(instructions.clone(), item_index);
            let batch_name = format!("row_{item_index}_question_{question_index}");
            questions.insert(batch_name.clone(), question);
            names.push((row.row, name.clone(), batch_name));
        }
    }
    let request = json!({"state": {"items": states}, "model": model, "questions": questions});
    let response = call_jev(endpoint, key, &request)?;
    let answers = response["answers"].as_object().ok_or_else(|| {
        datafusion_common::DataFusionError::Execution(
            "Jev batch response has no answers".to_string(),
        )
    })?;
    let mut row_answers = vec![serde_json::Map::new(); results.len()];
    for (row, name, batch_name) in names {
        let answer = answers.get(&batch_name).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "Jev batch response is missing a question answer".to_string(),
            )
        })?;
        row_answers[row].insert(name, answer.clone());
    }
    for row in pending.drain(..) {
        results[row.row] = Some(json!({
            "model": response["model"],
            "answers": row_answers[row.row],
            "usage": response["usage"],
            "usage_scope": "batch",
            "batch_size": states.len(),
        }));
    }
    Ok(())
}

/// Restricts one batched question to its corresponding state item.
fn scoped_instructions(instructions: Value, item_index: usize) -> Value {
    let focus = format!("Use only `items[{item_index}]` in `state` for this question.");
    match instructions {
        Value::String(text) => Value::String(format!("{focus} {text}")),
        other => json!({"focus": focus, "question": other}),
    }
}

/// Checks whether an argument is a SQL string type.
fn is_string(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

/// Builds an Arrow map type with string keys and the specified value type.
fn map_type(value_type: DataType) -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("keys", DataType::Utf8, false)),
                    Arc::new(Field::new("values", value_type, true)),
                ]
                .into(),
            ),
            false,
        )),
        false,
    )
}

/// Extracts a required string argument without putting its value in errors.
fn string_arg(array: &ArrayRef, row: usize, label: &str) -> Result<String> {
    match json_value_from_array(array, row)? {
        Value::String(value) if !value.is_empty() => Ok(value),
        _ => exec_err!("{label} must be a nonempty string"),
    }
}

/// Parses JSON text or converts a SQL map, array, or struct to a Jev value.
fn structured_arg(array: &ArrayRef, row: usize, label: &str) -> Result<Value> {
    let value = json_value_from_array(array, row)?;
    match value {
        Value::String(text) => serde_json::from_str(&text).map_err(|_| {
            datafusion_common::DataFusionError::Execution(format!("{label} must be valid JSON"))
        }),
        other => Ok(other),
    }
}

/// Builds and validates one row's API request.
fn request_for_row(kind: JevKind, args: &[ArrayRef], row: usize) -> Result<(Value, String)> {
    let state = json_value_from_array(&args[0], row)?;
    if !matches!(state, Value::String(_) | Value::Array(_) | Value::Object(_)) {
        return exec_err!("Jev state must be text, an array, a map, or a struct");
    }
    let (questions, key_position) = match kind {
        JevKind::Noul => {
            let instructions = string_arg(&args[1], row, "instructions")?;
            (
                json!({"answer": {"type": "noul", "instructions": instructions}}),
                2,
            )
        }
        JevKind::Choice | JevKind::Score => {
            let instructions = string_arg(&args[1], row, "instructions")?;
            let criteria = structured_arg(&args[2], row, "criteria")?;
            match kind {
                JevKind::Choice if !criteria.is_object() => {
                    return exec_err!("choice criteria must be a JSON object or SQL map");
                }
                JevKind::Score if !criteria.is_array() => {
                    return exec_err!("score criteria must be a JSON array or SQL array");
                }
                _ => {}
            }
            (
                json!({"answer": {"type": if kind == JevKind::Choice { "choice" } else { "score" }, "instructions": instructions, "criteria": criteria}}),
                3,
            )
        }
        JevKind::Evaluate => {
            let questions = structured_arg(&args[1], row, "questions")?;
            if !questions.is_object()
                || questions.as_object().is_some_and(serde_json::Map::is_empty)
            {
                return exec_err!("questions must be a nonempty JSON object or SQL map");
            }
            (questions, 2)
        }
    };
    let key = string_arg(&args[key_position], row, "API key")?;
    let model = if args.len() > key_position + 1 {
        string_arg(&args[key_position + 1], row, "model")?
    } else {
        DEFAULT_MODEL.to_string()
    };
    Ok((
        json!({"state": state, "model": model, "questions": questions}),
        key,
    ))
}

/// Reuses a bounded HTTP client across Jev evaluations.
fn client() -> Result<&'static Client> {
    static CLIENT: OnceLock<std::result::Result<Client, String>> = OnceLock::new();
    CLIENT
        .get_or_init(|| {
            Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .map_err(|error| error.to_string())
        })
        .as_ref()
        .map_err(|error| {
            datafusion_common::DataFusionError::Execution(format!("Jev HTTP client: {error}"))
        })
}

/// Calls Jev with bounded backoff for rate limits and temporary overloads.
fn call_jev(endpoint: &str, key: &str, request: &Value) -> Result<Value> {
    let client = client()?;
    for attempt in 0..3 {
        let response = client
            .post(endpoint)
            .bearer_auth(key)
            .json(request)
            .send()
            .map_err(|error| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Jev request failed: {error}"
                ))
            })?;
        let status = response.status();
        if status.is_success() {
            return response.json().map_err(|error| {
                datafusion_common::DataFusionError::Execution(format!(
                    "invalid Jev response: {error}"
                ))
            });
        }
        if (status == StatusCode::TOO_MANY_REQUESTS || status.as_u16() == 529) && attempt < 2 {
            std::thread::sleep(Duration::from_millis(200 * (1 << attempt)));
            continue;
        }
        return exec_err!("Jev API returned HTTP {status}");
    }
    exec_err!("Jev request exhausted retries")
}

/// Extracts the one answer returned for a typed Jev function.
fn answer<'a>(response: &'a Value, expected: &str) -> Result<&'a Value> {
    let answer = response.pointer("/answers/answer").ok_or_else(|| {
        datafusion_common::DataFusionError::Execution("Jev response has no answer".to_string())
    })?;
    if answer.get("type").and_then(Value::as_str) != Some(expected) {
        return exec_err!("Jev response has the wrong answer type");
    }
    Ok(answer)
}

/// Builds the SQL column for a batch of Jev responses.
fn build_result(kind: JevKind, responses: Vec<Option<Value>>) -> Result<ArrayRef> {
    match kind {
        JevKind::Noul => {
            let values = responses
                .iter()
                .map(|response| {
                    response
                        .as_ref()
                        .map(|response| {
                            answer(response, "noul")?
                                .get("noul")
                                .and_then(Value::as_f64)
                                .ok_or_else(|| {
                                    datafusion_common::DataFusionError::Execution(
                                        "Jev response has no noul probability".to_string(),
                                    )
                                })
                        })
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(Float64Array::from(values)))
        }
        JevKind::Evaluate => {
            let values = responses
                .into_iter()
                .map(|response| response.map(|response| response.to_string()))
                .collect::<Vec<_>>();
            Ok(Arc::new(StringArray::from(values)))
        }
        JevKind::Choice | JevKind::Score => build_struct_result(kind, &responses),
    }
}

/// Converts one Jev probability or legend object into an Arrow map row.
fn append_map<V: arrow::array::builder::ArrayBuilder>(
    builder: &mut MapBuilder<StringBuilder, V>,
    value: Option<&Value>,
    mut append_value: impl FnMut(&mut V, &Value) -> Result<()>,
) -> Result<()> {
    if let Some(entries) = value.and_then(Value::as_object) {
        for (key, value) in entries {
            builder.keys().append_value(key);
            append_value(builder.values(), value)?;
        }
        builder.append(true)?;
    } else {
        builder.append(false)?;
    }
    Ok(())
}

/// Builds typed Choice or Score structs from Jev's answer objects.
fn build_struct_result(kind: JevKind, responses: &[Option<Value>]) -> Result<ArrayRef> {
    let mut choices = StringBuilder::new();
    let mut scores = Float64Builder::new();
    let mut confidences = Float64Builder::new();
    let mut probabilities = MapBuilder::new(None, StringBuilder::new(), Float64Builder::new());
    let mut legends = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    let valid = responses.iter().map(Option::is_some).collect::<Vec<_>>();
    for response in responses {
        let answer = response
            .as_ref()
            .map(|response| {
                answer(
                    response,
                    if kind == JevKind::Choice {
                        "choice"
                    } else {
                        "score"
                    },
                )
            })
            .transpose()?;
        if let Some(answer) = answer {
            let confidence = answer
                .get("confidence")
                .and_then(Value::as_f64)
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Execution(
                        "Jev response has no confidence".to_string(),
                    )
                })?;
            confidences.append_value(confidence);
            append_map(
                &mut probabilities,
                answer.get("probabilities"),
                |builder, value| {
                    builder.append_value(value.as_f64().ok_or_else(|| {
                        datafusion_common::DataFusionError::Execution(
                            "Jev probability is not numeric".to_string(),
                        )
                    })?);
                    Ok(())
                },
            )?;
            if kind == JevKind::Choice {
                choices.append_value(answer.get("choice").and_then(Value::as_str).ok_or_else(
                    || {
                        datafusion_common::DataFusionError::Execution(
                            "Jev response has no choice".to_string(),
                        )
                    },
                )?);
            } else {
                scores.append_value(answer.get("score").and_then(Value::as_f64).ok_or_else(
                    || {
                        datafusion_common::DataFusionError::Execution(
                            "Jev response has no score".to_string(),
                        )
                    },
                )?);
                append_map(&mut legends, answer.get("legend"), |builder, value| {
                    builder.append_value(value.as_str().ok_or_else(|| {
                        datafusion_common::DataFusionError::Execution(
                            "Jev legend value is not text".to_string(),
                        )
                    })?);
                    Ok(())
                })?;
            }
        } else {
            confidences.append_null();
            probabilities.append(false)?;
            if kind == JevKind::Choice {
                choices.append_null();
            } else {
                scores.append_null();
                legends.append(false)?;
            }
        }
    }
    let fields = if kind == JevKind::Choice {
        vec![
            (
                Arc::new(Field::new("choice", DataType::Utf8, true)),
                Arc::new(choices.finish()) as ArrayRef,
            ),
            (
                Arc::new(Field::new("confidence", DataType::Float64, true)),
                Arc::new(confidences.finish()) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "probabilities",
                    map_type(DataType::Float64),
                    true,
                )),
                Arc::new(probabilities.finish()) as ArrayRef,
            ),
        ]
    } else {
        vec![
            (
                Arc::new(Field::new("score", DataType::Float64, true)),
                Arc::new(scores.finish()) as ArrayRef,
            ),
            (
                Arc::new(Field::new("confidence", DataType::Float64, true)),
                Arc::new(confidences.finish()) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "probabilities",
                    map_type(DataType::Float64),
                    true,
                )),
                Arc::new(probabilities.finish()) as ArrayRef,
            ),
            (
                Arc::new(Field::new("legend", map_type(DataType::Utf8), true)),
                Arc::new(legends.finish()) as ArrayRef,
            ),
        ]
    };
    let (fields, columns): (Vec<_>, Vec<_>) = fields.into_iter().unzip();
    Ok(Arc::new(StructArray::try_new(
        fields.into(),
        columns,
        Some(NullBuffer::from(valid)),
    )?))
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::{TcpListener, TcpStream};

    use arrow::array::{ListBuilder, MapArray};
    use datafusion_common::ScalarValue;

    use super::*;

    /// Verifies all four SQL return types match their Arrow results.
    #[test]
    fn typed_results_match_declared_types() -> Result<()> {
        let cases = [
            (
                JevKind::Noul,
                json!({"answers":{"answer":{"type":"noul","noul":0.8}}}),
            ),
            (
                JevKind::Choice,
                json!({"answers":{"answer":{"type":"choice","choice":"billing","confidence":0.7,"probabilities":{"billing":0.8,"technical":0.2}}}}),
            ),
            (
                JevKind::Score,
                json!({"answers":{"answer":{"type":"score","score":1.1,"confidence":0.6,"probabilities":{"0":0.1,"1":0.8,"2":0.1},"legend":{"0":"Calm","1":"Frustrated","2":"Angry"}}}}),
            ),
            (
                JevKind::Evaluate,
                json!({"model":"jev-1.13.0","answers":{"a":{"type":"noul","noul":0.8}},"usage":{"input_tokens":10,"output_tokens":2}}),
            ),
        ];
        for (kind, response) in cases {
            let result = build_result(kind, vec![Some(response), None])?;
            let arguments = if matches!(kind, JevKind::Choice | JevKind::Score) {
                vec![DataType::Utf8; 4]
            } else {
                vec![DataType::Utf8; 3]
            };
            assert_eq!(
                result.data_type(),
                &JevFunction::new(kind).return_type(&arguments)?
            );
            assert!(result.is_null(1));
            ScalarValue::try_from_array(&result, 0)?;
        }
        Ok(())
    }

    /// Verifies Choice maps and Score arrays form valid Jev question definitions.
    #[test]
    fn sql_criteria_are_serialized() -> Result<()> {
        let mut choices = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        choices.keys().append_value("billing");
        choices.values().append_value("Payments");
        choices.append(true)?;
        let choice_args: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["Help with payment"])),
            Arc::new(StringArray::from(vec!["Which team?"])),
            Arc::new(choices.finish()) as Arc<MapArray>,
            Arc::new(StringArray::from(vec!["test-key"])),
        ];
        let (choice, key) = request_for_row(JevKind::Choice, &choice_args, 0)?;
        assert_eq!(key, "test-key");
        assert_eq!(
            choice["questions"]["answer"]["criteria"]["billing"],
            "Payments"
        );
        assert_eq!(choice["model"], DEFAULT_MODEL);

        let mut levels = ListBuilder::new(StringBuilder::new());
        levels.values().append_value("Calm");
        levels.values().append_value("Angry");
        levels.append(true);
        let score_args: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["Help with payment"])),
            Arc::new(StringArray::from(vec!["How frustrated?"])),
            Arc::new(levels.finish()),
            Arc::new(StringArray::from(vec!["test-key"])),
            Arc::new(StringArray::from(vec!["jev-1.13.0"])),
        ];
        let (score, _) = request_for_row(JevKind::Score, &score_args, 0)?;
        assert_eq!(
            score["questions"]["answer"]["criteria"],
            json!(["Calm", "Angry"])
        );
        assert_eq!(score["model"], "jev-1.13.0");
        Ok(())
    }

    /// Verifies multi-question fan-out uses one HTTP request and a bearer token.
    #[test]
    fn evaluate_sends_one_authenticated_request() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let endpoint = format!("http://{}/v1/systemone", listener.local_addr()?);
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut bytes = Vec::new();
            let (body_start, body_len) = loop {
                let mut buffer = [0; 4096];
                let count = stream.read(&mut buffer).unwrap();
                assert!(count > 0);
                bytes.extend_from_slice(&buffer[..count]);
                if let Some(start) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&bytes[..start]).to_ascii_lowercase();
                    assert!(headers.contains("authorization: bearer test-key"));
                    let body_len = headers
                        .lines()
                        .find_map(|line| {
                            line.strip_prefix("content-length: ")
                                .and_then(|value| value.parse::<usize>().ok())
                        })
                        .unwrap();
                    let body_start = start + 4;
                    if bytes.len() >= body_start + body_len {
                        break (body_start, body_len);
                    }
                }
            };
            let request: Value =
                serde_json::from_slice(&bytes[body_start..body_start + body_len]).unwrap();
            assert_eq!(request["state"], "Customer wants a refund now");
            assert_eq!(request["questions"]["urgent"]["type"], "noul");
            assert_eq!(request["questions"]["team"]["type"], "choice");
            let body = json!({"model":"jev-1.13.0","answers":{"urgent":{"type":"noul","noul":0.9},"team":{"type":"choice","choice":"billing","probabilities":{"billing":1.0},"confidence":1.0}},"usage":{"input_tokens":20,"output_tokens":5}}).to_string();
            write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
        });
        let args: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["Customer wants a refund now"])),
            Arc::new(StringArray::from(vec![
                r#"{"urgent":{"type":"noul","instructions":"Is this urgent?"},"team":{"type":"choice","instructions":"Which team?","criteria":{"billing":"Payments"}}}"#,
            ])),
            Arc::new(StringArray::from(vec!["test-key"])),
        ];
        let responses = evaluate_rows(JevKind::Evaluate, &endpoint, &args, 1)?;
        server.join().unwrap();
        assert_eq!(
            responses[0].as_ref().unwrap()["answers"]
                .as_object()
                .unwrap()
                .len(),
            2
        );
        Ok(())
    }

    /// Verifies a null input row does not make an external request.
    #[test]
    fn null_row_skips_request() -> Result<()> {
        let args: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![Some("Is this urgent?")])),
            Arc::new(StringArray::from(vec![Some("test-key")])),
        ];
        assert_eq!(
            evaluate_rows(JevKind::Noul, "http://127.0.0.1:1", &args, 1)?,
            vec![None]
        );
        Ok(())
    }

    /// Reads one JSON request from a local HTTP test connection.
    fn read_http_json(stream: &TcpStream) -> Value {
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut content_length = 0;
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            if line == "\r\n" {
                break;
            }
            if let Some(length) = line.to_ascii_lowercase().strip_prefix("content-length: ") {
                content_length = length.trim().parse().unwrap();
            }
        }
        let mut body = vec![0; content_length];
        reader.read_exact(&mut body).unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    /// Verifies multiple input rows and questions share one request and retain row order.
    #[test]
    fn batches_rows_and_demultiplexes_answers() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let endpoint = format!("http://{}/v1/systemone", listener.local_addr()?);
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let request = read_http_json(&stream);
            assert_eq!(
                request["state"]["items"],
                json!(["urgent ticket", "routine ticket"])
            );
            assert_eq!(request["questions"].as_object().unwrap().len(), 4);
            assert!(
                request["questions"]["row_0_question_0"]["instructions"]
                    .as_str()
                    .unwrap()
                    .contains("items[0]")
            );
            assert!(
                request["questions"]["row_1_question_0"]["instructions"]
                    .as_str()
                    .unwrap()
                    .contains("items[1]")
            );
            let body = json!({
                "model": "jev-1.13.0",
                "answers": {
                    "row_0_question_0": {"type":"noul","noul":0.9},
                    "row_0_question_1": {"type":"choice","choice":"urgent","probabilities":{"urgent":1.0},"confidence":1.0},
                    "row_1_question_0": {"type":"noul","noul":0.1},
                    "row_1_question_1": {"type":"choice","choice":"routine","probabilities":{"routine":1.0},"confidence":1.0}
                },
                "usage": {"input_tokens":50,"output_tokens":10}
            }).to_string();
            write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
        });
        let questions = r#"{"a_urgency":{"type":"noul","instructions":"Is it urgent?"},"b_class":{"type":"choice","instructions":"Which class?","criteria":{"urgent":"Urgent","routine":"Routine"}}}"#;
        let args: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![
                Some("urgent ticket"),
                None,
                Some("routine ticket"),
            ])),
            Arc::new(StringArray::from(vec![questions, questions, questions])),
            Arc::new(StringArray::from(vec!["test-key", "test-key", "test-key"])),
        ];
        let results = evaluate_rows(JevKind::Evaluate, &endpoint, &args, 3)?;
        server.join().unwrap();
        assert!(results[1].is_none());
        assert_eq!(
            results[0].as_ref().unwrap()["answers"]["a_urgency"]["noul"],
            0.9
        );
        assert_eq!(
            results[2].as_ref().unwrap()["answers"]["a_urgency"]["noul"],
            0.1
        );
        assert_eq!(
            results[2].as_ref().unwrap()["answers"]["b_class"]["choice"],
            "routine"
        );
        assert_eq!(results[0].as_ref().unwrap()["usage_scope"], "batch");
        assert_eq!(results[0].as_ref().unwrap()["batch_size"], 2);
        Ok(())
    }
}
