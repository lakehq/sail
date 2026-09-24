---
title: Jev / AI Functions
rank: 6
---

# Jev / AI Functions

Sail's built-in Jev functions evaluate text or structured data using [TypeSafe's System One API](https://docs.typesafe.ai/api).
They run asynchronously in the Rust execution engine and are available without registering a UDF.
These functions are Sail extensions; Apache Spark has no equivalent functions.

## Functions and Results

Square brackets below indicate optional trailing arguments, not SQL syntax.
Every function returns one struct per input row, including calls with only literal arguments and `jev_models()`.

| Function                                                 | Result fields, in addition to inference metadata                                                                             |
| -------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `jev_noul(state, instructions [, criteria [, options]])` | `noul DOUBLE`: probability of yes, between 0 and 1.                                                                          |
| `jev_choice(state, instructions, criteria [, options])`  | `choice STRING`, `probabilities MAP<STRING, DOUBLE>`, `confidence DOUBLE`.                                                   |
| `jev_score(state, instructions, criteria [, options])`   | `score DOUBLE`, `probabilities MAP<STRING, DOUBLE>`, `confidence DOUBLE`, `legend MAP<STRING, VARIANT>`.                     |
| `jev_system_one(state, questions [, options])`           | `answers MAP<STRING, VARIANT>`, keyed by your original question IDs.                                                         |
| `jev_models([options])`                                  | `models ARRAY<STRUCT<name: STRING, description: STRING, release_date: STRING>>`, `request_id STRING`; no inference metadata. |

Noul preserves the probability rather than choosing a Boolean threshold for you.
Choice preserves the probabilities for every option, including unselected ones.
Score is the probability-weighted score over zero-based rubric positions and can be fractional.
Its legend preserves structured descriptions.
System One preserves each complete answer object, including its `type` and additional response fields.
Model discovery lists models and aliases available to your account; you can also request an explicit model version that is absent from that list.

All four inference functions also return these fields:

| Field        | SQL type                                              | Meaning                                                                              |
| ------------ | ----------------------------------------------------- | ------------------------------------------------------------------------------------ |
| `model`      | `STRING`                                              | Model returned by the service, which can differ from your requested alias.           |
| `request_id` | `STRING`                                              | The `x-typesafe-request-id` response header, or null if absent.                      |
| `batch_id`   | `STRING`                                              | Sail's identifier for the shared request; see [usage accounting](#usage-accounting). |
| `usage`      | `STRUCT<input_tokens: BIGINT, output_tokens: BIGINT>` | Request-level token counts. Missing or null counts remain null.                      |

## Inputs

`state` and `instructions` accept `STRING` or `VARIANT`.
A SQL string is literal text, even when it looks like JSON. Use `parse_json(...)` to supply a JSON object or array.
Nested JSON values are preserved without flattening or double encoding.
State accepts JSON strings, objects, and arrays. Instructions also accept JSON null.

| Input                | Accepted values                                                                                                                                                       |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Noul criteria        | `MAP<STRING, STRING>` or a VARIANT object with optional `true` and `false` descriptions. Each description can be a string, object, array, or null.                    |
| Choice criteria      | `MAP<STRING, STRING>` or a VARIANT object mapping choice names to descriptions. Each description can be a string, object, array, or null.                             |
| Score criteria       | `ARRAY<STRING>` or a VARIANT array containing one or more descriptions. Each entry must be a string, object, or array; null entries are invalid.                      |
| System One questions | A nonempty VARIANT object mapping your question IDs to objects with `type` (`noul`, `choice`, or `score`), optional `instructions`, and the corresponding `criteria`. |

TypeSafe recommends 2–10 Score levels and documents a 255-option Choice operating limit.
These are provider operating guidance, separate from local schema validation and Sail's resource limits.
Provider validation failures are returned as query errors.

SQL-null state produces SQL null for that row and contributes no question to an outgoing request.
An entirely null-state batch issues no requests. A zero-row invocation also issues no requests.
JSON-null state, such as `parse_json('null')`, is invalid rather than skipped.
SQL-null instructions are omitted from the request; JSON-null instructions remain null.
SQL-null or JSON-null Noul criteria are omitted. Choice criteria, Score criteria, and System One questions must be non-null.

Criteria and options have fixed positions; Sail never guesses their meaning from map contents.
To supply Noul options without criteria, pass an explicit null placeholder:

```sql
SELECT jev_noul(
  'The customer requested a refund.',
  'Is this about billing?',
  NULL,
  map('model', 'jev-latest')
);
```

Responses must contain a model, a usage object, and a correctly typed answer with the required fields for every requested question.
Missing answers, mismatched answer types, and malformed fields fail the query.
Sail never fabricates probabilities or substitutes null for missing answers.
Missing or null token counts are accepted for Python SDK compatibility; a missing or null usage object is an error.

## Credentials and Options

Provision `TYPESAFE_API_KEY` in every process executing Jev calls, including each worker in a distributed deployment.
The client machine's environment is not automatically copied to workers.
You may override it per row with the `api_key` option. An explicitly empty key is an error, not a request to use the environment.
Keys are trimmed and must contain only printable ASCII characters without whitespace.
Sail redacts keys from Jev transport diagnostics. Prefer worker environment configuration to putting secrets in SQL literals, which can be visible in query text and plans.

`options` is a `MAP<STRING, STRING>`. Omitted or SQL-null options use defaults.
Unknown names, null values, nonpositive timeouts or budgets, and negative retry counts are errors.

| Option            | Default                                     | Behavior                                               |
| ----------------- | ------------------------------------------- | ------------------------------------------------------ |
| `api_key`         | `TYPESAFE_API_KEY`                          | Authentication key for this row.                       |
| `model`           | `TYPESAFE_DEFAULT_MODEL`, then `jev-latest` | Requested model or alias. Rejected by `jev_models`.    |
| `timeout_ms`      | `10000`                                     | Deadline for one complete HTTP attempt.                |
| `retry_budget_ms` | `30000`                                     | Budget controlling whether another retry can start.    |
| `max_retries`     | `2`                                         | Retries after the first attempt; `0` disables retries. |

The server-side `TYPESAFE_BASE_URL` selects the endpoint and defaults to `https://api.typesafe.ai`.
Configure environment variables before starting the executing processes.
Inference uses `POST /v1/systemone`; model discovery uses `GET /v1/models`.

### Timeouts and Retries

The attempt timeout starts after Sail acquires an active-request permit and covers sending the request through reading the response body.
This is a Sail whole-attempt deadline, distinct from the Python SDK's HTTP-operation timeout.
The retry budget begins when the first attempt starts and includes later backoff and permit waits.
A retry is not started if elapsed time plus the next delay reaches the budget; Sail checks the budget again after obtaining an active-request permit.
The budget does not cancel an attempt already running. When retries stop, Sail reports the last error.

Sail retries HTTP 408, 429, all 5xx statuses, connection failures, and timeouts.
Authentication errors, ordinary validation errors, and response-contract errors are not retried.
These rules apply to HTTP retries within one function invocation.
Sail's cluster task retries are separate: a failed task can be executed again, issuing new requests even after a nonretryable HTTP error.
The cluster's `task_max_attempts` setting defaults to `3`; set `SAIL_CLUSTER__TASK_MAX_ATTEMPTS=1` before starting Sail to disable automatic task retries when that is appropriate for your workload.
Default exponential backoff starts at 500 ms, caps at 5 seconds, and subtracts up to 25% jitter.
Valid `retry-after-ms` takes precedence over `Retry-After`; the latter supports numeric seconds and HTTP dates.
As in the Python SDK, a date without a timezone (the older asctime format) uses the executing worker's local timezone; dates with `GMT` use UTC.
A valid server delay replaces backoff without the five-second cap, but may exhaust the retry budget.
This policy follows the pinned Python SDK, not the JavaScript SDK.

## Batching and Execution

Sail batches nearby rows within one function invocation when their effective key, endpoint, model, request options, and serialized state match.
All questions in a System One request see the same state, so unrelated states are never merged.
Question IDs are namespaced internally and restored in each row's result.
Independent requests run concurrently, and results retain input row order even when requests finish out of order.
Scalar options, instructions, and criteria are parsed once per invocation and reused.

Batching does not combine separate SQL expressions or different invocations.
Use `jev_system_one` when you need several judgments about the same state.
There is no cross-query result cache or request deduplication guarantee.

### Worker Resource Limits

Set these environment variables before starting each worker. Values are positive integers; byte values are raw byte counts.
Limits are shared across Jev functions and concurrent partitions in the same worker.

| Variable                          | Default             | Meaning                                                   |
| --------------------------------- | ------------------- | --------------------------------------------------------- |
| `SAIL_JEV_MAX_CONCURRENCY`        | `8`                 | Maximum active HTTP attempts per worker.                  |
| `SAIL_JEV_MAX_PENDING_REQUESTS`   | `16`                | Maximum admitted request groups, including active groups. |
| `SAIL_JEV_MAX_PENDING_BYTES`      | `16777216` (16 MiB) | Budget for retained encoded request bodies.               |
| `SAIL_JEV_BATCH_TARGET_QUESTIONS` | `64`                | Preferred questions per request.                          |
| `SAIL_JEV_BATCH_TARGET_BYTES`     | `262144` (256 KiB)  | Preferred encoded request size.                           |
| `SAIL_JEV_MAX_REQUEST_BYTES`      | `1048576` (1 MiB)   | Hard encoded request size limit.                          |

Preferred targets flush a batch. A valid logical row exceeding a preferred target is sent alone.
A row exceeding the hard size limit fails rather than being split, truncated, or rewritten.
Size accounting includes the actual JSON body, generated question IDs, and JSON encoding overhead.
These are Sail resource controls, not additional provider schema restrictions.

Configurations are validated so a request allowed by the hard limit can acquire the required reservations.
Invalid combinations fail instead of waiting indefinitely.
Each admitted group reserves the hard request byte limit before encoding, even when its eventual body is smaller.
Consequently, the byte budget also limits simultaneous admitted groups to `floor(MAX_PENDING_BYTES / MAX_REQUEST_BYTES)`.
To make full configured HTTP concurrency possible, allow at least that many groups and their byte reservations.
Pending work and encoded payloads are bounded in addition to active HTTP attempts; the input and output Arrow arrays still require memory proportional to their batch size.
Retries reuse serialized request bytes and release active-attempt permits during backoff.
Cancellation and terminal failures drop owned request and retry futures and release their reservations.

### Expressions and Cancellation

Jev calls work in projections, filters, struct-field access, and surrounding ordinary scalar expressions.
They are volatile: planning and `EXPLAIN` do not issue requests.
The async evaluator can extract calls before evaluating a surrounding `CASE`, `AND`, or `OR`, so those expressions do not guarantee a call is skipped.
Use a SQL-null state for rows that must not contribute a question.

Direct nesting of a Jev call inside another async call is unsupported.
Materialize intermediate results, for example by writing and reading a table, before a second Jev evaluation.
A view or common table expression alone does not guarantee materialization.
Repeated calls and query re-execution can issue new requests.

### Usage Accounting

Usage belongs to the shared request, not an individual input row or question.
Rows answered by the same request share `batch_id` and its usage totals.
Count each batch once; summing every row's usage overcounts it.
Materialize results before accounting so a second query cannot re-evaluate volatile calls:

```sql
-- evaluated_messages is a table containing previously stored Jev results.
SELECT
  sum(input_tokens) AS input_tokens,
  sum(output_tokens) AS output_tokens
FROM (
  SELECT
    result.batch_id,
    max(result.usage.input_tokens) AS input_tokens,
    max(result.usage.output_tokens) AS output_tokens
  FROM evaluated_messages
  WHERE result IS NOT NULL
  GROUP BY result.batch_id
);
```

Missing token counts remain unknown. Retries can reach the provider even when Sail does not receive their responses.
The returned usage is not a guarantee of exactly-once billing or complete billing totals across failed attempts and query re-execution.

## Examples

These examples assume the worker has an API key and `messages` contains `id` and `body` columns.

### Moderation and Probability Filtering

```sql
SELECT id, jev_noul(body, 'Does this message contain a threat?').noul AS threat_probability
FROM messages;

SELECT id, body
FROM messages
WHERE jev_noul(body, 'Is this message spam?').noul >= 0.9;
```

### Classification

```sql
SELECT
  id,
  jev_choice(
    body,
    'Which team should handle this message?',
    map('billing', 'Payments and invoices', 'support', 'Technical help', 'sales', 'Purchasing')
  ) AS result
FROM messages;
```

### Relevance Scoring and Reranking

```sql
SELECT
  id,
  jev_score(
    body,
    'How relevant is this document to reducing database latency?',
    array('Unrelated', 'Some useful information', 'Directly answers the question')
  ).score AS relevance
FROM messages
ORDER BY relevance DESC;
```

### Structured State and Mixed Questions

```sql
SELECT jev_system_one(
  parse_json('{"subject":"Duplicate charge","message":"Please refund the second payment."}'),
  parse_json('{
    "billing": {"type":"noul", "instructions":"Is this about billing?"},
    "team": {"type":"choice", "criteria":{"billing":"Payments","support":"Technical help"}},
    "urgency": {"type":"score", "instructions":"How urgent is this?", "criteria":["Can wait", {"deadline":"today"}]}
  }')
) AS result;
```

Access a mixed answer with `result.answers['billing']`; it is a VARIANT containing the complete Noul answer object.

### Model Discovery

```sql
SELECT jev_models().models;
```

## Contract Reference

The implementation pins TypeSafe Python SDK **0.7.1**, commit [`0ffd094c72ed9445223060b24ffd7a56aa781fb4`](https://github.com/typesafe-ai/typesafe-sdk-python/tree/0ffd094c72ed9445223060b24ffd7a56aa781fb4), and an OpenAPI 3.1.0 snapshot for API version 0.2.0 captured on **2026-09-24 UTC**.
Exact source artifacts, schema bytes, hashes, and attribution are recorded in the repository's `python/pysail/tests/spark/jev/references/manifest.json`.
Request shapes follow that schema. Missing/null token counts follow the Python SDK's more tolerant response types.
Sail additionally validates completeness against requested question IDs, preserves additional mixed-answer fields, and exposes Score map keys as SQL strings.
The request deadline and worker resource controls above are Sail-specific behavior.
