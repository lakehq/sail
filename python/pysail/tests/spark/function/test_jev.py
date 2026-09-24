"""Observable SQL and HTTP contract tests for Sail's built-in async Jev functions.

The mock and the server run in this process. These tests intentionally use Python
rather than snapshots so they can assert request isolation, retries and concurrency.
Local-cluster execution also exercises physical UDF serialization.
"""

import json
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from email.utils import format_datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pysail.testing.jev import JevMock
from pysail.testing.spark.session import spark_connect_server, spark_session_factory
from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = [pytest.mark.skipif(is_jvm_spark(), reason="Jev functions are Sail extensions"), pytest.mark.timeout(60)]

EXPECTED_NOUL = 0.75
EXPECTED_CONFIDENCE = 0.8
EXPECTED_INPUT_TOKENS = 17
EXPECTED_OUTPUT_TOKENS = 3
DEFAULT_ATTEMPTS = 3
MAX_CONCURRENCY = 4
BATCH_TARGET_QUESTIONS = 4
BATCH_TARGET_BYTES = 4096
MAX_REQUEST_BYTES = 32768
DOUBLE_FALLBACK_NUMBER = "1" + "0" * 255


@pytest.fixture(scope="module")
def jev_service():
    with JevMock() as service:
        yield service


@pytest.fixture(scope="module")
def remote(jev_service):
    envs = {
        "SAIL_MODE": "local-cluster",
        # Isolate the HTTP retry contract from cluster retries of a failed task.
        "SAIL_CLUSTER__TASK_MAX_ATTEMPTS": "1",
        "TYPESAFE_BASE_URL": jev_service.url,
        "TYPESAFE_API_KEY": "mock-default-key",
        "TYPESAFE_DEFAULT_MODEL": "jev-test",
        "TYPESAFE_JEV_MAX_CONCURRENCY": str(MAX_CONCURRENCY),
        "TYPESAFE_JEV_MAX_PENDING_REQUESTS": "8",
        "TYPESAFE_JEV_MAX_PENDING_BYTES": "262144",
        "TYPESAFE_JEV_BATCH_TARGET_QUESTIONS": str(BATCH_TARGET_QUESTIONS),
        "TYPESAFE_JEV_BATCH_TARGET_BYTES": str(BATCH_TARGET_BYTES),
        "TYPESAFE_JEV_MAX_REQUEST_BYTES": str(MAX_REQUEST_BYTES),
    }
    with spark_connect_server(envs=envs) as server:
        yield server.remote


@pytest.fixture
def jev(jev_service):
    jev_service.reset()
    jev_service.delay = 0
    yield jev_service
    # Timeout tests can leave the *mock's* handler asleep after the client is gone.
    deadline = time.monotonic() + 5
    while jev_service.active and time.monotonic() < deadline:
        time.sleep(0.01)
    assert jev_service.active == 0


def test_noul_metadata_and_options_position(spark, jev):
    result = spark.sql("SELECT jev_noul('text', 'yes?', NULL, map('model', 'jev-pinned')) AS j").first().j
    assert result.noul == EXPECTED_NOUL
    assert result.model == "jev-pinned"
    assert result.request_id == "mock-1"
    assert result.batch_id
    assert result.usage.input_tokens == EXPECTED_INPUT_TOKENS
    assert result.usage.output_tokens == EXPECTED_OUTPUT_TOKENS
    assert jev.requests[0]["authorization"] == "Bearer mock-default-key"
    assert jev.requests[0]["path"] == "/v1/systemone"
    assert next(iter(jev.requests[0]["body"]["questions"].values()))["instructions"] == "yes?"


def test_choice_keeps_complete_distribution(spark, jev):
    result = spark.sql("SELECT jev_choice('text', NULL, map('a', 'first', 'b', NULL)) AS j").first().j
    assert result.choice == "a"
    assert result.probabilities == {"a": 0.5, "b": 0.5}
    assert result.confidence == EXPECTED_CONFIDENCE
    question = next(iter(jev.requests[0]["body"]["questions"].values()))
    assert question["criteria"] == {"a": "first", "b": None}
    assert "instructions" not in question


def test_nullable_noul_descriptions(spark, jev):
    result = spark.sql("SELECT jev_noul('text', 'yes?', map('true', NULL, 'false', 'no')) AS j").first().j
    assert result.noul == EXPECTED_NOUL
    question = next(iter(jev.requests[0]["body"]["questions"].values()))
    assert question["criteria"] == {"true": None, "false": "no"}


@pytest.mark.parametrize("levels", [1, 11])
def test_score_operating_guidance_is_not_local_schema_validation(spark, jev, levels):
    # Spark 3.5 clients cannot decode the VARIANT legend; collect only the fields under test.
    result = (
        spark.sql(f"SELECT jev_score('text', 'rate', array_repeat('level', {levels})) AS j")
        .selectExpr("j.score", "j.probabilities")
        .first()
    )
    assert result.score == (levels - 1) / 2
    assert len(result.probabilities) == levels
    assert jev.request_count == 1


def test_string_state_is_literal_and_json_null_instructions_are_preserved(spark, jev):
    assert spark.sql("SELECT jev_noul('{\"text\": 1}', parse_json('null')) AS j").first().j.noul == EXPECTED_NOUL
    body = jev.requests[0]["body"]
    assert body["state"] == '{"text": 1}'
    question = next(iter(body["questions"].values()))
    assert "instructions" in question
    assert question["instructions"] is None


@pytest.mark.parametrize(
    ("expression", "location"),
    [
        ("jev_noul(to_variant_object({context}), 'compare')", "state"),
        ("jev_noul('text', to_variant_object({context}))", "instructions"),
        (
            "jev_noul('text', 'compare', to_variant_object(named_struct('true', {context})))",
            "criteria",
        ),
        (
            "jev_choice('text', 'compare', to_variant_object(named_struct('a', {context})))",
            "criteria",
        ),
        ("jev_score('text', 'compare', to_variant_object(array({context})))", "criteria"),
        (
            "jev_system_one('text', to_variant_object(named_struct("
            "'q', named_struct('type', 'noul', 'instructions', {context}))))",
            "instructions",
        ),
    ],
)
def test_structured_request_numbers_are_preserved(spark, jev, expression, location):
    wide = "12345678901234567890123456789012345678"
    fraction = "12345678901234567890.123456789012345678"
    context = f"named_struct('wide', CAST('{wide}' AS DECIMAL(38,0)), 'fraction', CAST('{fraction}' AS DECIMAL(38,18)))"
    expected = {"wide": Decimal(wide), "fraction": Decimal(fraction)}
    original = spark.sql(f"SELECT to_json(to_variant_object({context})) AS j").first().j
    assert json.loads(original, parse_int=Decimal, parse_float=Decimal) == expected

    spark.sql(f"SELECT to_json({expression.format(context=context)}) AS j").collect()
    assert jev.request_count == 1
    body = json.loads(jev.requests[0]["encoded"], parse_int=Decimal, parse_float=Decimal)
    actual = body["state"] if location == "state" else next(iter(body["questions"].values()))[location]
    if location == "criteria":
        actual = actual[0] if isinstance(actual, list) else next(iter(actual.values()))
    assert actual == expected


def test_distinct_decimal_states_do_not_batch_together(spark, jev):
    account = "12345678901234567890123456789012345678"
    expected_rows = 2
    rows = spark.sql(
        "SELECT jev_noul(to_variant_object(named_struct('account', "
        "CAST('12345678901234567890123456789012345678' AS DECIMAL(38,0)) "
        "+ CAST(id AS DECIMAL(38,0)))), 'compare') AS j "
        "FROM range(0, 2, 1, 1)"
    ).collect()
    assert len(rows) == expected_rows
    assert jev.request_count == expected_rows
    states = [
        json.loads(request["encoded"], parse_int=Decimal, parse_float=Decimal)["state"]["account"]
        for request in jev.requests
    ]
    assert sorted(states) == [Decimal(account), Decimal(int(account) + 1)]


def test_score_preserves_structured_legend(spark, jev):
    expected_score = 0.5
    row = spark.sql(
        "SELECT j.score, j.probabilities, to_json(j.legend['0']) AS low, to_json(j.legend['1']) AS high "
        'FROM (SELECT jev_score(parse_json(\'{"text": ["nested"]}\'), parse_json(\'["rate"]\'), '
        'parse_json(\'[{"label": "low"}, ["high"]]\')) AS j)'
    ).first()
    assert row.score == expected_score
    assert row.probabilities == {"0": 0.5, "1": 0.5}
    assert json.loads(row.low) == {"label": "low"}
    assert json.loads(row.high) == ["high"]
    assert jev.requests[0]["body"]["state"] == {"text": ["nested"]}


def test_mixed_questions_preserve_ids_and_additional_answer_fields(spark, jev):
    questions = {
        "q/0": {"type": "noul", "criteria": {"true": None, "false": {"label": "no"}}},
        "q.1": {"type": "choice", "criteria": {"a": None, "b": ["description"]}},
        "q:2": {"type": "score", "criteria": ["low", {"label": "high"}]},
    }
    encoded = json.dumps(questions)
    row = spark.sql(
        f"SELECT to_json(j.answers['q/0']) AS noul, to_json(j.answers['q.1']) AS choice, "  # noqa: S608 -- fixed test JSON
        f"to_json(j.answers['q:2']) AS score FROM (SELECT jev_system_one('text', parse_json('{encoded}')) AS j)"
    ).first()
    answers = [json.loads(row.noul), json.loads(row.choice), json.loads(row.score)]
    assert [a["type"] for a in answers] == ["noul", "choice", "score"]
    assert all(a["provider_extra"] == {"preserved": True} for a in answers)
    assert jev.request_count == 1


@pytest.mark.parametrize("expression", ["jev_noul('text', 'yes?')", "jev_models()"])
def test_all_scalar_and_zero_argument_row_cardinality(spark, jev, expression):
    expected_rows = 17
    rows = spark.sql(f"SELECT {expression} AS j FROM range(0, 17, 1, 1)").collect()  # noqa: S608 -- fixed test expressions
    assert len(rows) == expected_rows
    assert all(row.j is not None for row in rows)
    if expression.startswith("jev_models"):
        assert all(row.j.models[0].name == "jev-test" for row in rows)
        assert all(request["path"] == "/v1/models" for request in jev.requests)
    else:
        assert all(row.j.noul == EXPECTED_NOUL for row in rows)
        assert jev.request_count < len(rows)
        assert all(len(request["body"]["questions"]) <= BATCH_TARGET_QUESTIONS for request in jev.requests)
        assert sum(len(request["body"]["questions"]) for request in jev.requests) == expected_rows


@pytest.mark.parametrize("expression", ["jev_noul('text', 'yes?')", "jev_models()"])
def test_zero_rows_issue_no_requests(spark, jev, expression):
    assert spark.sql(f"SELECT {expression} FROM range(0, 0, 1, 1)").collect() == []  # noqa: S608 -- fixed test expressions
    assert jev.request_count == 0


def test_null_skipping_is_row_specific(spark, jev):
    expected_rows = 9
    expected_questions = 4
    rows = spark.sql(
        "SELECT id, jev_noul(CASE WHEN id % 2 = 0 THEN CAST(NULL AS STRING) ELSE 'text' END, 'yes?') AS j "
        "FROM range(0, 9, 1, 1)"
    ).collect()
    assert len(rows) == expected_rows
    assert all((row.j is None) == (row.id % 2 == 0) for row in rows)
    assert sum(len(request["body"]["questions"]) for request in jev.requests) == expected_questions


def test_entirely_null_state_issues_no_requests(spark, jev):
    expected_rows = 9
    rows = spark.sql("SELECT jev_noul(CAST(NULL AS STRING), 'yes?') AS j FROM range(0, 9, 1, 1)").collect()
    assert len(rows) == expected_rows
    assert all(row.j is None for row in rows)
    assert jev.request_count == 0


def test_batch_usage_is_shared_without_losing_rows(spark, jev):
    expected_rows = 9
    rows = spark.sql("SELECT id, jev_noul('text', concat('question ', id)) AS j FROM range(0, 9, 1, 1)").collect()
    batches = {row.j.batch_id for row in rows}
    assert len(rows) == expected_rows
    assert len(batches) == jev.request_count < expected_rows
    for batch_id in batches:
        batch = [row.j for row in rows if row.j.batch_id == batch_id]
        assert len({row.request_id for row in batch}) == 1
        assert all(row.usage.input_tokens == EXPECTED_INPUT_TOKENS for row in batch)
    assert sum(row.j.usage.input_tokens for row in rows) == expected_rows * EXPECTED_INPUT_TOKENS
    assert len(batches) * EXPECTED_INPUT_TOKENS < expected_rows * EXPECTED_INPUT_TOKENS


def test_separate_expressions_do_not_merge_requests(spark, jev):
    expected_rows = 9
    rows = spark.sql(
        "SELECT jev_noul('text', 'yes?') AS n, jev_choice('text', 'which?', map('a', 'A', 'b', 'B')) AS c "
        "FROM range(0, 9, 1, 1)"
    ).collect()
    assert len(rows) == expected_rows
    assert all(row.n.noul == EXPECTED_NOUL and row.c.choice == "a" for row in rows)
    assert all(len({q["type"] for q in request["body"]["questions"].values()}) == 1 for request in jev.requests)
    assert {q["type"] for request in jev.requests for q in request["body"]["questions"].values()} == {"noul", "choice"}


def test_different_states_overlap_and_return_to_original_rows(spark, jev):
    expected_rows = 24
    jev.delay = lambda body, _ordinal: 0.02 * (4 - int(body["state"]) % 4)
    rows = spark.sql("SELECT id, jev_noul(CAST(id AS STRING), 'yes?') AS j FROM range(0, 24, 1, 4)").collect()
    assert len(rows) == expected_rows
    assert all(row.j.noul == pytest.approx(row.id / 100) for row in rows)
    assert jev.request_count == expected_rows
    assert 1 < jev.peak_active <= MAX_CONCURRENCY
    assert all(len(request["body"]["questions"]) == 1 for request in jev.requests)


def test_model_options_split_batches(spark, jev):
    expected_rows = 12
    rows = spark.sql(
        "SELECT id, jev_noul('text', 'yes?', NULL, map('model', concat('model-', id % 2))) AS j FROM range(0, 12, 1, 1)"
    ).collect()
    assert len(rows) == expected_rows
    assert all(row.j.model == f"model-{row.id % 2}" for row in rows)
    assert {request["authorization"] for request in jev.requests} == {"Bearer mock-default-key"}


@pytest.mark.parametrize("usage", [{}, {"input_tokens": None, "output_tokens": None}])
def test_nullable_token_counts(spark, jev, usage):
    jev.transform = lambda response, _body: {**response, "usage": usage}
    result = spark.sql("SELECT jev_noul('text', 'yes?') AS j").first().j
    assert result.usage.input_tokens is None
    assert result.usage.output_tokens is None


@pytest.mark.parametrize("field", ["model", "usage", "answers"])
def test_missing_required_response_fields_are_errors(spark, jev, field):
    def remove(response, _body):
        response.pop(field)
        return response

    jev.transform = remove
    with pytest.raises(Exception, match=rf"(?i){field}"):
        spark.sql("SELECT jev_noul('text', 'yes?')").collect()
    assert jev.request_count == 1


@pytest.mark.parametrize("corruption", ["missing", "wrong_type", "missing_probability", "string_probability"])
def test_invalid_answers_are_errors_without_retry(spark, jev, corruption):
    def corrupt(response, _body):
        key = next(iter(response["answers"]))
        answer = response["answers"][key]
        if corruption == "missing":
            del response["answers"][key]
        elif corruption == "wrong_type":
            answer["type"] = "choice"
        elif corruption == "missing_probability":
            del answer["noul"]
        else:
            answer["noul"] = "0.75"
        return response

    jev.transform = corrupt
    with pytest.raises(Exception, match=r"(?i)(answer|noul|type|probability)"):
        spark.sql("SELECT jev_noul('text', 'yes?')").collect()
    assert jev.request_count == 1


@pytest.mark.parametrize(
    ("expression", "field"),
    [
        ("jev_choice('text', 'which?', map('a', 'A', 'b', 'B'))", "probabilities"),
        ("jev_score('text', 'rate', array('low', 'high'))", "legend"),
    ],
)
def test_missing_choice_and_score_fields_are_errors(spark, jev, expression, field):
    def corrupt(response, _body):
        next(iter(response["answers"].values())).pop(field)
        return response

    jev.transform = corrupt
    with pytest.raises(Exception, match=rf"(?i){field}"):
        spark.sql(f"SELECT to_json({expression})").collect()
    assert jev.request_count == 1


def test_model_discovery_validates_required_fields(spark, jev):
    def corrupt(response, _body):
        response["models"][0].pop("description")
        return response

    jev.transform = corrupt
    with pytest.raises(Exception, match=r"(?i)(description|model)"):
        spark.sql("SELECT jev_models()").collect()
    assert jev.request_count == 1


def test_answer_extra_fields_cannot_overwrite_request_metadata(spark, jev):
    def poison(response, _body):
        for answer in response["answers"].values():
            answer.update(model="untrusted-model", usage={"input_tokens": 999}, batch_id="fake", request_id="fake")
        return response

    jev.transform = poison
    result = spark.sql("SELECT jev_noul('text', 'yes?') AS j").first().j
    assert result.model == "jev-test"
    assert result.usage.input_tokens == EXPECTED_INPUT_TOKENS
    assert result.batch_id != "fake"
    assert result.request_id == "mock-1"
    row = spark.sql(
        "SELECT j.model, j.usage.input_tokens AS tokens, to_json(j.answers['q']) AS answer "
        'FROM (SELECT jev_system_one(\'text\', parse_json(\'{"q": {"type": "noul"}}\')) AS j)'
    ).first()
    assert row.model == "jev-test"
    assert row.tokens == EXPECTED_INPUT_TOKENS
    assert json.loads(row.answer)["model"] == "untrusted-model"
    assert json.loads(row.answer)["usage"] == {"input_tokens": 999}


def test_error_detail_redacts_raw_and_json_escaped_api_keys(spark, jev, monkeypatch):
    key = 'quote"\\backslash-key'
    monkeypatch.setenv("TYPESAFE_API_KEY", key)
    jev.error_response = {"detail": f"invalid credential: {key}"}
    jev.statuses.append((401, {}))
    with pytest.raises(Exception, match="401") as error:
        spark.sql("SELECT jev_noul('text', 'yes?')").collect()
    message = str(error.value)
    assert key not in message
    assert json.dumps(key)[1:-1] not in message
    assert jev.request_count == 1
    assert jev.requests[0]["authorization"] == f"Bearer {key}"


@pytest.mark.parametrize("key", [None, "", "bad key", "nonascii-\u00e9"])
@pytest.mark.parametrize("expression", ["jev_noul('text', 'yes?')", "jev_models()"])
def test_environment_api_key_is_required_and_validated(spark, jev, monkeypatch, key, expression):
    if key is None:
        monkeypatch.delenv("TYPESAFE_API_KEY", raising=False)
    else:
        monkeypatch.setenv("TYPESAFE_API_KEY", key)
    with pytest.raises(Exception, match=r"(?i)(API key|TYPESAFE_API_KEY)"):
        spark.sql(f"SELECT {expression}").collect()
    assert jev.request_count == 0


@pytest.mark.parametrize("padding", ["  ", "\t\r\n", "\x1c", "\x1d", "\x1e", "\x1f"])
def test_environment_api_key_is_trimmed(spark, jev, monkeypatch, padding):
    monkeypatch.setenv("TYPESAFE_API_KEY", f"{padding}mock-trimmed-key{padding}")
    assert spark.sql("SELECT jev_noul('text', 'yes?') AS j").first().j.noul == EXPECTED_NOUL
    assert jev.requests[0]["authorization"] == "Bearer mock-trimmed-key"


@pytest.mark.parametrize("status", [408, 429, 500, 529])
def test_transient_status_retries_reuse_request_bytes(spark, jev, status):
    jev.statuses.extend([(status, {"retry-after-ms": "1"}), (status, {"retry-after-ms": "1"})])
    assert spark.sql("SELECT jev_noul('text', 'yes?') AS j").first().j.noul == EXPECTED_NOUL
    assert jev.request_count == DEFAULT_ATTEMPTS
    assert len({request["encoded"] for request in jev.requests}) == 1


@pytest.mark.parametrize("status", [400, 401, 403, 422])
def test_permanent_status_is_not_retried(spark, jev, status):
    jev.statuses.append((status, {}))
    with pytest.raises(Exception, match=str(status)):
        spark.sql("SELECT jev_noul('text', 'yes?')").collect()
    assert jev.request_count == 1


@pytest.mark.parametrize(
    "body",
    [
        b"provider rejection marker: mock-default-key",
        b'"provider rejection marker: mock-default-key"',
        b'{"errors":[{"message":"provider rejection marker: mock-default-key"}]}',
        b'{"detail":[],"message":"provider rejection marker: mock-default-key"}',
        b'{"detail":[],"errors":[{"message":"provider rejection marker: mock-default-key"}]}',
        b'{"context":"' + b"x" * 600 + b'","message":"provider rejection marker: mock-default-key"}',
    ],
)
def test_http_errors_keep_provider_diagnostics(spark, jev, body):
    jev.statuses.append((422, {}))
    jev.encode_response = lambda _response: body
    with pytest.raises(Exception, match="provider rejection marker") as error:
        spark.sql("SELECT jev_noul('text', 'yes?')").collect()
    assert "mock-default-key" not in str(error.value)
    assert jev.request_count == 1


def test_retry_header_precedence_and_budget(spark, jev):
    expected_attempts = 2
    jev.statuses.append((429, {"retry-after-ms": "1", "Retry-After": "60"}))
    assert (
        spark.sql("SELECT jev_noul('text', 'yes?', NULL, map('retry_budget_ms', '1000')) AS j").first().j.noul
        == EXPECTED_NOUL
    )
    assert jev.request_count == expected_attempts
    jev.reset()
    jev.statuses.append((429, {"retry-after-ms": "60000"}))
    with pytest.raises(Exception, match=r"(?i)(429|retry|budget)"):
        spark.sql("SELECT jev_noul('text', 'yes?', NULL, map('retry_budget_ms', '10'))").collect()
    assert jev.request_count == 1


def test_attempt_timeout_releases_capacity_for_following_query(spark, jev):
    expected_rows = 16
    jev.delay = 0.2
    with pytest.raises(Exception, match=r"(?i)(timeout|timed out|deadline)"):
        spark.sql("SELECT jev_noul('text', 'yes?', NULL, map('timeout_ms', '10', 'max_retries', '0'))").collect()
    jev.delay = 0
    assert len(spark.sql("SELECT jev_noul('text', 'yes?') AS j FROM range(0, 16, 1, 4)").collect()) == expected_rows


@pytest.mark.parametrize(
    ("expression", "message"),
    [
        ("jev_noul(parse_json('null'), 'yes?')", "(?i)state"),
        ("jev_noul(parse_json('true'), 'yes?')", "(?i)state"),
        ("jev_noul('text', 'yes?', NULL, map('unknown', 'x'))", "(?i)(unknown|option)"),
        ("jev_noul('text', 'yes?', NULL, map('timeout_ms', '0'))", "(?i)timeout"),
        ("jev_noul('text', 'yes?', NULL, map('max_retries', '-1'))", "(?i)retr"),
        ("jev_score('text', 'rate', array())", "(?i)(criteria|level|empty)"),
        ("jev_score('text', 'rate', parse_json('[null]'))", "(?i)(criteria|level|null)"),
        ("jev_system_one('text', parse_json('{}'))", "(?i)(question|empty)"),
        ("jev_models(map('model', 'jev-test'))", "(?i)model"),
    ],
)
def test_invalid_inputs_never_reach_service(spark, jev, expression, message):
    with pytest.raises(Exception, match=message):
        spark.sql(f"SELECT to_json({expression})").collect()
    assert jev.request_count == 0


@pytest.mark.parametrize("option", ["api_key", "base_url"])
@pytest.mark.parametrize(
    "expression",
    [
        "jev_noul('text', 'yes?', NULL, {options})",
        "jev_choice('text', 'which?', map('a', 'A'), {options})",
        "jev_score('text', 'rate', array('low', 'high'), {options})",
        'jev_system_one(\'text\', parse_json(\'{"q":{"type":"noul"}}\'), {options})',
        "jev_models({options})",
    ],
)
def test_credentials_and_endpoint_are_not_sql_options(spark, jev, option, expression):
    call = expression.replace("{options}", f"map('{option}', 'unsupported-option-value')")
    with pytest.raises(Exception, match=r"(?i)(unknown|unsupported).*option"):
        spark.sql(f"SELECT to_json({call})").collect()
    assert jev.request_count == 0


def test_preferred_batch_size_is_not_a_hard_row_limit(spark, jev):
    expected_rows = 3
    rows = spark.sql("SELECT jev_noul(repeat('x', 6000), 'yes?') AS j FROM range(0, 3, 1, 1)").collect()
    assert len(rows) == expected_rows
    assert jev.request_count == expected_rows
    assert all(BATCH_TARGET_BYTES < len(request["encoded"]) < MAX_REQUEST_BYTES for request in jev.requests)


def test_hard_limit_rejects_without_request_and_releases_reservations(spark, jev):
    with pytest.raises(Exception, match=r"(?i)(limit|size|bytes|large)"):
        spark.sql("SELECT jev_noul(repeat('x', 40000), 'yes?')").collect()
    assert jev.request_count == 0
    assert spark.sql("SELECT jev_noul('text', 'yes?') AS j").first().j.noul == EXPECTED_NOUL


def test_request_at_hard_byte_limit_can_make_progress(spark, jev):
    spark.sql("SELECT jev_noul('x', 'yes?') AS j").collect()
    body_overhead = len(jev.requests[0]["encoded"]) - 1
    # A single-row call consistently namespaces its question as row zero.
    payload_length = MAX_REQUEST_BYTES - body_overhead
    rows = spark.sql(f"SELECT jev_noul(repeat('x', {payload_length}), 'yes?') AS j").collect()
    assert len(rows) == 1
    assert rows[0].j.noul == EXPECTED_NOUL
    assert len(jev.requests[-1]["encoded"]) == MAX_REQUEST_BYTES


def test_explain_and_schema_do_not_make_requests(spark, jev):
    spark.sql("EXPLAIN SELECT jev_noul('text', 'yes?')").collect()
    assert spark.sql("SELECT jev_models() AS j").schema["j"].dataType.typeName() == "struct"
    assert jev.request_count == 0


@pytest.mark.parametrize("mode", ["", "EXTENDED", "FORMATTED"])
def test_explain_preserves_nonsecret_model_options(spark, jev, mode):
    rows = spark.sql(f"EXPLAIN {mode} SELECT jev_noul('text', 'yes?', NULL, map('model', 'visible-model'))").collect()
    plan = "\n".join(str(row[0]) for row in rows)
    assert "visible-model" in plan
    assert "mock-default-key" not in plan
    assert jev.request_count == 0


@pytest.mark.parametrize("mode", ["", "EXTENDED", "FORMATTED"])
def test_environment_api_key_does_not_enter_plan(spark, jev, mode):
    rows = spark.sql(f"EXPLAIN {mode} SELECT jev_noul('text', 'yes?')").collect()
    assert "mock-default-key" not in "\n".join(str(row[0]) for row in rows)
    assert jev.request_count == 0


def test_invalid_argument_diagnostics_do_not_include_environment_key(spark, jev):
    with pytest.raises(Exception, match=r"(?i)unsupported type") as error:
        spark.sql("SELECT jev_noul(12, 'yes?')").collect()
    assert "mock-default-key" not in str(error.value)
    assert jev.request_count == 0


def test_generated_column_name_redacts_options(spark, jev):
    query = spark.sql("SELECT jev_noul('text', 'yes?', NULL, map('model', 'column-model'))")
    assert "<redacted options>" in query.columns[0]
    assert "column-model" not in query.columns[0]
    assert jev.request_count == 0


def test_registered_python_function_named_like_jev_is_not_intercepted(remote, jev):
    from pyspark.sql.types import StringType

    # Registering this override in the shared session would hide the real built-in.
    with spark_session_factory(remote) as sessions:
        session = sessions.create()
        session.udf.register("jev_models", lambda value: value, StringType())
        query = session.sql("SELECT jev_models('visible-value')")
        assert "visible-value" in query.columns[0]
        assert query.collect()[0][0] == "visible-value"
        plan = session.sql("EXPLAIN SELECT jev_models('visible-value')").collect()
        assert "visible-value" in "\n".join(str(row[0]) for row in plan)
        assert jev.request_count == 0


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("SELECT round(jev_noul('text', 'yes?').noul, 1) AS x", [0.8]),
        ("SELECT id AS x FROM range(0, 4, 1, 1) WHERE jev_noul(CAST(id AS STRING), 'yes?').noul >= 0.02", [2, 3]),
        ("SELECT CASE WHEN id = 0 THEN 0.0 ELSE jev_noul('text', 'yes?').noul END AS x FROM range(0, 2)", [0, 0.75]),
        ("SELECT id > 0 AND jev_noul('text', 'yes?').noul > 0.5 AS x FROM range(0, 2)", [False, True]),
        ("SELECT id = 0 OR jev_noul('text', 'yes?').noul < 0.5 AS x FROM range(0, 2)", [True, False]),
    ],
)
@pytest.mark.usefixtures("jev")
def test_surrounding_expressions(spark, query, expected):
    assert [row.x for row in spark.sql(query).collect()] == expected


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        (
            "SELECT a.id AS a, b.id AS b FROM range(0, 4, 1, 1) a "
            "JOIN range(0, 4, 1, 1) b ON a.id = b.id "
            "WHERE jev_noul(concat(CAST(a.id AS STRING), CAST(b.id AS STRING)), 'yes?').noul > 0.1 "
            "ORDER BY a, b",
            [(1, 1), (2, 2), (3, 3)],
        ),
        (
            "SELECT a.id AS a, b.id AS b FROM range(0, 4, 1, 1) a "
            "CROSS JOIN range(0, 4, 1, 1) b "
            "WHERE a.id < b.id AND "
            "jev_noul(concat(CAST(a.id AS STRING), CAST(b.id AS STRING)), 'yes?').noul > 0.01 "
            "ORDER BY a, b",
            [(0, 2), (0, 3), (1, 2), (1, 3), (2, 3)],
        ),
        (
            "SELECT a.id AS a, b.id AS b FROM range(0, 4, 1, 1) a "
            "JOIN range(0, 4, 1, 1) b ON a.id = b.id "
            "WHERE jev_noul(CASE WHEN a.id = 0 THEN CAST(NULL AS STRING) "
            "ELSE concat(CAST(a.id AS STRING), CAST(b.id AS STRING)) END, 'yes?').noul > 0.1 "
            "ORDER BY a, b",
            [(1, 1), (2, 2), (3, 3)],
        ),
    ],
)
def test_jev_where_filters_over_joins(spark, jev, query, expected):
    assert [tuple(row) for row in spark.sql(query).collect()] == expected
    assert jev.request_count > 0


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        (
            "SELECT id FROM range(0, 4, 1, 1) ORDER BY jev_noul(CAST(id AS STRING), 'yes?').noul DESC LIMIT 2",
            [3, 2],
        ),
        (
            "SELECT id FROM range(0, 4, 1, 1) ORDER BY jev_noul(CAST(id % 2 AS STRING), 'yes?').noul DESC, id DESC",
            [3, 1, 2, 0],
        ),
        (
            "SELECT id FROM range(0, 4, 1, 1) SORT BY jev_noul(CAST(id AS STRING), 'yes?').noul DESC",
            [3, 2, 1, 0],
        ),
    ],
)
def test_jev_direct_order_by(spark, jev, query, expected):
    result = spark.sql(query)
    assert result.columns == ["id"]
    assert [row.id for row in result.collect()] == expected
    assert jev.request_count > 0


@pytest.mark.parametrize("order", ["ORDER BY", "SORT BY"])
def test_jev_required_sort_for_order_sensitive_aggregate(spark, jev, order):
    query = (
        "SELECT collect_list(id) AS ids FROM (SELECT id FROM range(0, 4, 1, 1) "  # noqa: S608 -- fixed test sort clauses
        f"{order} jev_noul(CAST(id AS STRING), 'yes?').noul DESC)"
    )
    row = spark.sql(query).first()
    assert row.ids == [3, 2, 1, 0]
    assert jev.request_count > 0


@pytest.mark.parametrize(
    "query",
    [
        "SELECT jev_noul(CAST(jev_noul('text', 'first?').noul AS STRING), 'second?')",
        "SELECT sum(jev_noul(CAST(jev_noul('text', 'first?').noul AS STRING), 'second?').noul) FROM range(0, 2, 1, 1)",
        "SELECT id FROM range(0, 2, 1, 1) "
        "ORDER BY jev_noul(CAST(jev_noul(CAST(id AS STRING), 'first?').noul AS STRING), 'second?').noul",
    ],
)
def test_direct_async_nesting_is_diagnosed(spark, jev, query):
    with pytest.raises(Exception, match="Jev async calls cannot be nested"):
        spark.sql(query).collect()
    assert jev.request_count == 0


def test_cancellation_releases_reservations(spark, jev):
    expected_rows = 24
    jev.delay = 0.5
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(
            lambda: spark.sql("SELECT jev_noul(CAST(id AS STRING), 'yes?') FROM range(0, 80, 1, 4)").collect()
        )
        assert jev.started.wait(timeout=10)
        spark.interruptAll()
        assert future.exception(timeout=10) is not None
    # A subsequent query needs all reservations again; leaked permits would hang.
    jev.delay = 0
    rows = spark.sql("SELECT jev_noul(CAST(id AS STRING), 'yes?') AS j FROM range(0, 24, 1, 4)").collect()
    assert len(rows) == expected_rows


def test_retry_after_http_date_obeys_budget(spark, jev):
    retry_at = format_datetime(datetime.now(timezone.utc) + timedelta(minutes=1), usegmt=True)
    jev.statuses.append((429, {"Retry-After": retry_at}))
    with pytest.raises(Exception, match=r"(?i)(429|retry|budget)"):
        spark.sql("SELECT jev_noul('text', 'yes?', NULL, map('retry_budget_ms', '10'))").collect()
    assert jev.request_count == 1


def test_retry_after_http_date_uses_time_after_body_read(spark, jev):
    expected_attempts = 2
    retry_headers = {}

    def set_retry_date(_body, ordinal):
        if ordinal == 1:
            retry_headers["Retry-After"] = format_datetime(
                datetime.now(timezone.utc) + timedelta(seconds=3), usegmt=True
            )
        return 0

    jev.delay = set_retry_date
    jev.body_delay = lambda _body, ordinal: 3 if ordinal == 1 else 0
    jev.statuses.append((429, retry_headers))
    result = (
        spark.sql("SELECT jev_noul('text', 'yes?', NULL, map('retry_budget_ms', '4500', 'max_retries', '1')) AS j")
        .first()
        .j
    )
    assert result.noul == EXPECTED_NOUL
    assert jev.request_count == expected_attempts


def test_default_retry_count_is_two(spark, jev):
    jev.statuses.extend([(503, {"retry-after-ms": "1"})] * 4)
    with pytest.raises(Exception, match="503"):
        spark.sql("SELECT jev_noul('text', 'yes?')").collect()
    assert jev.request_count == DEFAULT_ATTEMPTS


def test_cancellation_during_backoff_releases_reservations(spark, jev):
    jev.statuses.append((429, {"retry-after-ms": "60000"}))
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(
            lambda: spark.sql("SELECT jev_noul('text', 'yes?', NULL, map('retry_budget_ms', '90000'))").collect()
        )
        assert jev.started.wait(timeout=10)
        # The completed response puts this invocation into its owned retry sleep.
        time.sleep(0.1)
        spark.interruptAll()
        assert future.exception(timeout=10) is not None
    assert spark.sql("SELECT jev_noul('text', 'yes?') AS j").first().j.noul == EXPECTED_NOUL


@pytest.mark.parametrize(
    "number",
    [
        "9223372036854775807",
        "9223372036854775808",
        "9223372036854775809",
        "18446744073709551615",
        "12345678901234567890123456789012345678",
        "12345678901234567890.123456789012345678",
        "-12345678901234567890.123456789012345678",
        "1.2345678901234567890123456789012345678e20",
        "1.000e-38",
        "-0.0",
        "123456789012345678901234567890123456780.0e-1",
        DOUBLE_FALLBACK_NUMBER,
    ],
)
@pytest.mark.parametrize("kind", ["score", "system_one"])
def test_structured_response_numbers_are_preserved(spark, jev, number, kind):
    marker = "jev-numeric-response-fixture"
    description = {"nested": [marker, None, True, "text"]}

    def transform(response, _body):
        for answer in response["answers"].values():
            if kind == "score":
                answer["legend"]["0"] = description
            else:
                answer["provider_extra"] = description
        return response

    jev.transform = transform
    jev.encode_response = lambda response: json.dumps(response).replace(json.dumps(marker), number).encode()
    if kind == "score":
        expression = "jev_score('text', 'rate', array('low', 'high'))"
        selected = "j.legend['0']"
    else:
        expression = 'jev_system_one(\'text\', parse_json(\'{"q":{"type":"noul"}}\'))'
        selected = "j.answers['q']"
    row = spark.sql(
        f"SELECT to_json({selected}) AS value, "  # noqa: S608 -- fixed test expressions
        "j.model, j.request_id, j.usage.input_tokens AS tokens "
        f"FROM (SELECT {expression} AS j)"
    ).first()
    value = json.loads(row.value, parse_int=Decimal, parse_float=Decimal)
    if kind == "system_one":
        assert value["type"] == "noul"
        assert value["noul"] == Decimal("0.75")
        value = value["provider_extra"]
    actual, *other = value["nested"]
    if number == DOUBLE_FALLBACK_NUMBER:
        assert float(actual) == float(number)
    else:
        assert actual == Decimal(number)
        if number == "-0.0":
            assert actual.is_signed()
    assert other == [None, True, "text"]
    assert row.model == "jev-test"
    assert row.request_id == "mock-1"
    assert row.tokens == EXPECTED_INPUT_TOKENS
    assert jev.request_count == 1


@pytest.mark.parametrize("kind", ["score", "system_one"])
def test_variant_result_maps_keep_sql_null_rows(spark, jev, kind):
    state = "CASE WHEN id = 0 THEN CAST(NULL AS STRING) ELSE 'text' END"
    if kind == "score":
        expression = f"jev_score({state}, 'rate', array('low', 'high'))"
        selected = "j.legend['0']"
    else:
        questions = json.dumps({"q": {"type": "noul"}})
        expression = f"jev_system_one({state}, parse_json('{questions}'))"
        selected = "j.answers['q']"
    rows = spark.sql(
        f"SELECT id, j IS NULL AS skipped, to_json({selected}) AS value "  # noqa: S608 -- fixed test expressions
        f"FROM (SELECT id, {expression} AS j FROM range(0, 2, 1, 1)) ORDER BY id"
    ).collect()
    assert rows[0].skipped
    assert rows[0].value is None
    assert not rows[1].skipped
    assert rows[1].value is not None
    assert jev.request_count == 1


def test_repeated_volatile_calls_keep_their_own_answers(spark, jev):
    expected_requests = 2
    probabilities = []

    def vary(response, _body):
        with jev.lock:
            probability = (len(probabilities) + 1) / 10
            probabilities.append(probability)
        for answer in response["answers"].values():
            answer["noul"] = probability
        return response

    jev.transform = vary
    row = spark.sql("SELECT jev_noul('text', 'yes?') AS a, jev_noul('text', 'yes?') AS b").first()
    assert jev.request_count == expected_requests
    assert sorted([row.a.noul, row.b.noul]) == probabilities == [0.1, 0.2]
    assert {row.a.request_id, row.b.request_id} == {"mock-1", "mock-2"}
    assert row.a.batch_id != row.b.batch_id


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        (
            "SELECT round(sum(jev_noul(CAST(id AS STRING), 'first?').noul), 2) AS a, "
            "round(sum(jev_noul(CAST(id + 10 AS STRING), 'second?').noul), 2) AS b "
            "FROM range(0, 4, 1, 1)",
            [{"a": 0.06, "b": 0.46}],
        ),
        (
            "WITH judged AS (SELECT id, jev_noul(CAST(id AS STRING), 'first?').noul AS a, "
            "jev_noul(CAST(id + 10 AS STRING), 'second?').noul AS b FROM range(0, 4, 1, 2)) "
            "SELECT id % 2 AS group_id, round(sum(a), 2) AS a, round(avg(b), 2) AS b, sum(id) AS c "
            "FROM judged GROUP BY id % 2 HAVING b > 0.1 ORDER BY group_id",
            [{"group_id": 0, "a": 0.02, "b": 0.11, "c": 2}, {"group_id": 1, "a": 0.04, "b": 0.12, "c": 4}],
        ),
        (
            "SELECT round(sum(jev_noul(CAST(id AS STRING), 'first?').noul) "
            "FILTER (WHERE id < 2), 2) AS a, "
            "round(sum(jev_noul(CAST(id + 10 AS STRING), 'second?').noul) "
            "FILTER (WHERE id >= 2), 2) AS b FROM range(0, 4, 1, 1)",
            [{"a": 0.01, "b": 0.25}],
        ),
        (
            "SELECT round(sum(DISTINCT jev_noul(CAST(id % 2 AS STRING), 'first?').noul), 2) AS a, "
            "round(sum(DISTINCT jev_noul(CAST(id % 2 + 10 AS STRING), 'second?').noul), 2) AS b "
            "FROM range(0, 4, 1, 1)",
            [{"a": 0.01, "b": 0.21}],
        ),
        (
            "WITH judged AS (SELECT id, jev_noul(CAST(id AS STRING), 'first?').noul AS a, "
            "jev_noul(CAST(id + 10 AS STRING), 'second?').noul AS b FROM range(0, 4, 1, 2)) "
            "SELECT id % 2 AS group_id, round(sum(a), 2) AS a, round(sum(b), 2) AS b "
            "FROM judged GROUP BY GROUPING SETS ((id % 2), ()) ORDER BY group_id NULLS LAST",
            [
                {"group_id": 0, "a": 0.02, "b": 0.22},
                {"group_id": 1, "a": 0.04, "b": 0.24},
                {"group_id": None, "a": 0.06, "b": 0.46},
            ],
        ),
        (
            "SELECT round(sum(jev_noul(CASE WHEN id % 2 = 0 THEN NULL "
            "ELSE CAST(id AS STRING) END, 'first?').noul), 2) AS a, "
            "round(avg(jev_noul('20', 'second?').noul), 2) AS b FROM range(0, 4, 1, 1)",
            [{"a": 0.04, "b": 0.2}],
        ),
        (
            "SELECT round(sum(jev_noul('10', 'first?').noul), 2) AS a, "
            "round(avg(jev_noul('20', 'second?').noul), 2) AS b FROM range(0, 4, 1, 1)",
            [{"a": 0.4, "b": 0.2}],
        ),
        (
            "SELECT (SELECT round(sum(jev_noul(CAST(id AS STRING), 'first?').noul) + "
            "sum(jev_noul(CAST(id + 10 AS STRING), 'second?').noul), 2) FROM range(0, 4, 1, 1)) AS a",
            [{"a": 0.52}],
        ),
    ],
)
@pytest.mark.usefixtures("jev")
def test_jev_aggregate_arguments_keep_their_own_result_columns(spark, query, expected):
    assert [row.asDict() for row in spark.sql(query).collect()] == expected


def test_jev_aggregates_over_empty_input_make_no_requests(spark, jev):
    row = spark.sql(
        "SELECT sum(jev_noul('10', 'first?').noul) AS a, avg(jev_noul('20', 'second?').noul) AS b FROM range(0)"
    ).first()
    assert row.a is None
    assert row.b is None
    assert jev.request_count == 0


@pytest.mark.parametrize(
    ("expression", "location"),
    [
        ("jev_noul(payload, 'yes?')", "state"),
        ("jev_noul('text', payload)", "instructions"),
        ("jev_noul('text', 'yes?', payload)", "criteria"),
    ],
)
def test_shredded_variant_arguments_keep_object_content(spark, jev, tmp_path, expression, location):
    amount_type = pa.struct(
        [
            pa.field("value", pa.binary(), nullable=True),
            pa.field("typed_value", pa.decimal128(9, 2), nullable=True),
        ]
    )
    description_type = pa.struct([pa.field("amount", amount_type, nullable=True)])
    description_variant_type = pa.struct(
        [
            pa.field("value", pa.binary(), nullable=True),
            pa.field("typed_value", description_type, nullable=True),
        ]
    )
    typed_value_type = pa.struct([pa.field("true", description_variant_type, nullable=True)])
    payload_type = pa.struct(
        [
            pa.field("metadata", pa.binary(), nullable=False, metadata={"variant": "true"}),
            pa.field("value", pa.binary(), nullable=True),
            pa.field("typed_value", typed_value_type, nullable=True),
        ]
    )
    amounts = [Decimal("1.23"), Decimal("4.56")]
    payload = pa.array(
        [
            None
            if amount is None
            else {
                "metadata": b"\x01\x02\x00\x06\x0aamounttrue",
                "value": None,
                "typed_value": {
                    "true": {
                        "value": None,
                        "typed_value": {"amount": {"value": None, "typed_value": amount}},
                    }
                },
            }
            for amount in [amounts[0], None, amounts[1]]
        ],
        type=payload_type,
    )
    field = pa.field(
        "payload",
        payload_type,
        nullable=True,
        metadata={"ARROW:extension:name": "arrow.parquet.variant", "ARROW:extension:metadata": "{}"},
    )
    path = tmp_path / "shredded_decimal.parquet"
    pq.write_table(pa.Table.from_arrays([payload], schema=pa.schema([field])), path)
    frame = spark.read.parquet(str(path))
    observed = frame.selectExpr("variant_get(payload, '$.true.amount', 'decimal(9, 2)') AS amount").collect()
    assert [row.amount for row in observed] == [amounts[0], None, amounts[1]]
    rows = frame.selectExpr(f"({expression}).noul AS probability").collect()
    middle = None if location == "state" else EXPECTED_NOUL
    assert [row.probability for row in rows] == [EXPECTED_NOUL, middle, EXPECTED_NOUL]

    actual = []
    for request in jev.requests:
        body = json.loads(request["encoded"], parse_float=Decimal)
        if location == "state":
            actual.append(body["state"])
        else:
            actual.extend(question[location] for question in body["questions"].values() if location in question)
    assert sorted(value["true"]["amount"] for value in actual) == amounts
    assert all(set(value) == {"true"} and set(value["true"]) == {"amount"} for value in actual)
