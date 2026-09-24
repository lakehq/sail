---
title: Jev / AI Functions
rank: 6
---

# Jev / AI Functions

Jev functions use [TypeSafe's System One API](https://docs.typesafe.ai/api) to evaluate text and JSON data.
These functions are built into Sail.
Apache Spark does not include these functions.
Sail sends asynchronous HTTP requests from Rust.
No Python SDK installation is necessary.

## Set Up Jev

An API key is the only value that you must supply.
For normal use, supply the key through `TYPESAFE_API_KEY`.
All other Jev environment variables have defaults.

1. Set `TYPESAFE_API_KEY` in the environment of the Sail server.
2. For a distributed deployment, set `TYPESAFE_API_KEY` in every worker that executes Jev calls.
3. Start the server and workers with these environment settings.

For a local deployment, this shell command sets the key before you start Sail:

```sh
export TYPESAFE_API_KEY='<your-api-key>'
```

The environment of a remote SQL client does not configure the Sail server or its workers.
An explicit SQL `api_key` option can supply a key instead of `TYPESAFE_API_KEY`.
Keys in SQL text can appear in query records and debug logs.

### Environment Variables

This table lists all environment variables that configure the Jev functions.
Only `TYPESAFE_API_KEY` is necessary when the SQL call does not supply `api_key`.
The six `SAIL_JEV_*` variables control optional [worker resource limits](#worker-resource-limits).

| Environment variable              | Required                           | Default                   | Function                                                            |
| --------------------------------- | ---------------------------------- | ------------------------- | ------------------------------------------------------------------- |
| `TYPESAFE_API_KEY`                | Yes, unless SQL supplies `api_key` | No default                | API key for requests from this process.                             |
| `TYPESAFE_DEFAULT_MODEL`          | No                                 | `jev-latest`              | Model for inference when SQL does not supply `model`.               |
| `TYPESAFE_BASE_URL`               | No                                 | `https://api.typesafe.ai` | API root URL for this process.                                      |
| `SAIL_JEV_MAX_CONCURRENCY`        | No                                 | `8`                       | Maximum number of active HTTP attempts per worker.                  |
| `SAIL_JEV_MAX_PENDING_REQUESTS`   | No                                 | `16`                      | Maximum number of admitted request groups, including active groups. |
| `SAIL_JEV_MAX_PENDING_BYTES`      | No                                 | `16777216`                | Maximum reserved bytes for encoded request bodies.                  |
| `SAIL_JEV_BATCH_TARGET_QUESTIONS` | No                                 | `64`                      | Preferred number of questions in one request.                       |
| `SAIL_JEV_BATCH_TARGET_BYTES`     | No                                 | `262144`                  | Preferred size of one encoded request, in bytes.                    |
| `SAIL_JEV_MAX_REQUEST_BYTES`      | No                                 | `1048576`                 | Maximum permitted size of one encoded request, in bytes.            |

Set these variables before you start the processes that execute Jev calls.
Restart the affected processes after you change the API URL or Jev resource limits.

Empty or whitespace-only `TYPESAFE_*` values use their defaults.
`TYPESAFE_API_KEY` has no default, so a request without another key fails.
The `SAIL_JEV_*` values must be positive integers without whitespace.
Empty `SAIL_JEV_*` values cause an error.

Sail does not read the Python SDK variable `TYPESAFE_LOG_LEVEL` for these functions.

### API URL

Sail reads `TYPESAFE_BASE_URL` directly.
This variable works without the Python SDK.
Its default URL is sufficient for the public TypeSafe service.

For a different service address, set the variable in every process that executes Jev calls:

```sh
export TYPESAFE_BASE_URL='https://api.example.com'
```

Supply the API root URL.
Do not add `/v1`, `/v1/systemone`, or `/v1/models` to the root URL.
Sail adds these paths to the root URL:

| Operation                | HTTP request         |
| ------------------------ | -------------------- |
| Evaluate questions       | `POST /v1/systemone` |
| Get the available models | `GET /v1/models`     |

The URL must use HTTP or HTTPS and must include a host.
The URL must not contain credentials, query parameters, or a fragment.
Sail does not follow HTTP redirects.
There is no SQL `base_url` option.

### API Key and Model Selection

An explicit SQL option has priority over the related environment variable.
For the model, Sail uses `jev-latest` when neither source supplies a model.
The `jev_models` function does not accept a `model` option.

Sail removes leading and trailing whitespace from API keys.
The remaining characters must be printable ASCII characters without whitespace.
An empty SQL `api_key` value causes an error.
It does not select the environment key.

## SQL Options

The optional `options` argument has type `MAP<STRING, STRING>`.
Each value must be a non-null string, including numeric settings.
If `options` is absent or SQL NULL, Sail uses the defaults.
An unknown option name causes an error.

| Option            | Default                                     | Function                                                                       |
| ----------------- | ------------------------------------------- | ------------------------------------------------------------------------------ |
| `api_key`         | `TYPESAFE_API_KEY`                          | API key for this row.                                                          |
| `model`           | `TYPESAFE_DEFAULT_MODEL`, then `jev-latest` | Model or model alias for inference.                                            |
| `timeout_ms`      | `10000`                                     | Time limit for one full HTTP attempt, in milliseconds.                         |
| `retry_budget_ms` | `30000`                                     | Time budget that controls the start of retries, in milliseconds.               |
| `max_retries`     | `2`                                         | Maximum retries after the first attempt. A value of `0` disables HTTP retries. |

`timeout_ms` and `retry_budget_ms` must be positive integers.
`max_retries` must be a nonnegative integer.
SQL NULL and an option value of `0` do not disable the time limits.
SQL NULL for the full options argument still selects the defaults.

Criteria and options have fixed argument positions.
For Noul options without criteria, put SQL NULL in the criteria position:

```sql
SELECT jev_noul(
  'The customer requested a refund.',
  'Is this about billing?',
  NULL,
  map('model', 'jev-latest', 'timeout_ms', '15000', 'max_retries', '0')
);
```

### Python SDK Configuration Differences

Sail supplies a smaller configuration interface than the Python SDK.
The SQL options do not reproduce all `AsyncTypeSafeClient` or `RetryPolicy` parameters.
Sail uses its own HTTP client and fixed result types.

The following table maps the [Python client settings](https://docs.typesafe.ai/sdk/python/api/clients/async) to Sail settings.

| Python SDK setting         | Sail setting or behavior                                                                                  |
| -------------------------- | --------------------------------------------------------------------------------------------------------- |
| `api_key`, `model`         | SQL options or the related `TYPESAFE_*` variables.                                                        |
| `base_url`                 | `TYPESAFE_BASE_URL` only.                                                                                 |
| Client `timeout`           | `timeout_ms` controls a full attempt. Separate connect, read, write, and pool timeouts are not available. |
| `retry`                    | Only `max_retries` and `retry_budget_ms` are configurable. The next table lists the fixed settings.       |
| `headers`, `extra_headers` | Custom request headers are not available.                                                                 |
| `transport`, `http_client` | Sail does not accept Python transport or client objects.                                                  |
| `extra_body`               | Extra fields in the request body are not available as SQL options.                                        |
| `response_model`           | Sail does not accept custom Python response models.                                                       |

The following table gives the Sail equivalents of the [Python retry settings](https://docs.typesafe.ai/sdk/python/api/retries).

| Python `RetryPolicy` setting | Sail setting or fixed value                                                         |
| ---------------------------- | ----------------------------------------------------------------------------------- |
| `max_retries`                | SQL `max_retries`. Default: `2`, for at most three attempts.                        |
| `timeout`                    | SQL `retry_budget_ms`. Default: `30000`. An unlimited budget is not available.      |
| `backoff_initial`            | Fixed at `0.5` seconds.                                                             |
| `backoff_max`                | Fixed at `5.0` seconds for the calculated delay.                                    |
| `backoff_jitter`             | Fixed at `0.25`. Sail randomly subtracts up to 25% from the calculated delay.       |
| `http_statuses`              | Fixed at HTTP `408`, `429`, and `500` through `599`.                                |
| `respect_retry_after`        | Always enabled for valid retry headers.                                             |
| `api_connection_error`       | Retries are enabled for connection failures and failures to read the response body. |
| `api_timeout_error`          | Retries are enabled for timeouts.                                                   |
| `exceptions`, `predicate`    | Custom exception classes and retry callbacks are not available.                     |

The fixed settings have no SQL option or Jev environment variable.
A value of `max_retries = 0` disables all HTTP retries, including retries for connection failures and timeouts.
[Cluster task retries](#cluster-task-retries) are separate.

## Functions and Results

Square brackets in this table identify optional arguments.
Do not include these brackets in a SQL call.
Each function returns one result row for each input row.
This rule also applies to calls with literal arguments and to `jev_models()`.

| Function                                                 | Result fields                                                                                                                     |
| -------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `jev_noul(state, instructions [, criteria [, options]])` | `noul DOUBLE`, plus inference metadata.                                                                                           |
| `jev_choice(state, instructions, criteria [, options])`  | `choice STRING`, `probabilities MAP<STRING, DOUBLE>`, `confidence DOUBLE`, plus inference metadata.                               |
| `jev_score(state, instructions, criteria [, options])`   | `score DOUBLE`, `probabilities MAP<STRING, DOUBLE>`, `confidence DOUBLE`, `legend MAP<STRING, VARIANT>`, plus inference metadata. |
| `jev_system_one(state, questions [, options])`           | `answers MAP<STRING, VARIANT>`, plus inference metadata.                                                                          |
| `jev_models([options])`                                  | `models ARRAY<STRUCT<name: STRING, description: STRING, release_date: STRING>>`, `request_id STRING`.                             |

Each result is a struct, except when a SQL NULL state produces a SQL NULL result.

Noul returns the probability of yes, from 0 to 1.
Noul does not convert this probability to a Boolean value.
Choice returns the selected answer and the probabilities for all choices.
Score returns a probability-weighted value over rubric positions, with the first position at zero.
The score can contain a fractional part.
The legend retains each structured description.

System One returns answers under the original question IDs.
Each answer retains its `type` and additional response fields.
Model discovery returns the available models and aliases for the account.
An explicit inference model can identify a version absent from this list.

### Inference Metadata

All four inference functions return these additional fields.
The `jev_models` function does not return inference metadata.

| Field        | SQL type                                              | Description                                                                               |
| ------------ | ----------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| `model`      | `STRING`                                              | Model returned by the service. This value can differ from the requested alias.            |
| `request_id` | `STRING`                                              | Value of the `x-typesafe-request-id` response header. An absent header produces SQL NULL. |
| `batch_id`   | `STRING`                                              | Sail identifier for the request shared by these rows.                                     |
| `usage`      | `STRUCT<input_tokens: BIGINT, output_tokens: BIGINT>` | Token counts for the full request. An absent or null count remains SQL NULL.              |

## Inputs

The state is the data that the model evaluates.
Instructions describe the question that the model must answer.
Criteria describe the possible answers.

`state` and `instructions` accept SQL `STRING` or `VARIANT` values.
A SQL string remains literal text, even if it contains JSON syntax.
Use `parse_json(...)` for a JSON object or array.
Sail retains nested JSON values without a change to their structure.

The state can be a JSON string, object, or array.
Instructions can also be JSON null.

| Input                | Accepted values                                                                                                                                                                            |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Noul criteria        | `MAP<STRING, STRING>` or a VARIANT object with optional `true` and `false` descriptions. Each description can be a string, object, array, or null.                                         |
| Choice criteria      | `MAP<STRING, STRING>` or a VARIANT object. Each choice name maps to a string, object, array, or null description.                                                                          |
| Score criteria       | `ARRAY<STRING>` or a VARIANT array with one or more descriptions. Each description must be a string, object, or array. Null entries are invalid.                                           |
| System One questions | A VARIANT object with at least one question. Each question has an ID and a `type` of `noul`, `choice`, or `score`. Instructions are optional. Choice and Score questions require criteria. |

TypeSafe recommends 2 to 10 Score levels and gives a 255-choice operating limit.
Sail does not use these values as local schema limits.
The service can reject values for a specified model.
Sail returns these service errors as query errors.

### Null Values

| Input                                                         | Sail behavior                                                                         |
| ------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| SQL NULL state                                                | Sail returns SQL NULL for this row. Sail adds no question from this row to a request. |
| JSON null state, such as `parse_json('null')`                 | Sail returns an error.                                                                |
| SQL NULL instructions                                         | Sail omits instructions from the request.                                             |
| JSON null instructions                                        | Sail sends JSON null instructions.                                                    |
| SQL NULL or JSON null `jev_noul` criteria argument            | Sail omits the criteria.                                                              |
| Null Choice criteria, Score criteria, or System One questions | Sail returns an error.                                                                |

A batch with only SQL NULL states sends no requests.
A batch with no rows also sends no requests.
Other rows in a mixed batch can still cause requests.
The `jev_system_one` function retains explicit JSON null fields inside question objects when the schema permits null.

### Response Validation

Each inference response must contain a model, a usage object, and an answer for every requested question.
Each answer must have the correct type and all required fields.
A missing answer or an invalid field causes a query error.
Sail does not create replacement probabilities or null answers.

Missing or null token counts remain SQL NULL.
This behavior follows the Python SDK response types.
A missing or null usage object causes an error.

## Batching and Worker Resource Limits

Sail processes input rows in batches.
Within one function invocation, nearby rows can share a request when these values are equal:

- API key
- API root URL
- Model
- Request options
- Encoded state.

A function invocation is one execution of a function on a batch of rows.
Every question in one System One request sees the same state.
Sail does not combine different states in one request.
Sail replaces question IDs for transmission and restores the original IDs in each result.

Independent requests run concurrently.
Results retain the input row order when responses arrive in a different order.
Sail parses constant options, instructions, and criteria once for each function invocation.

Separate SQL expressions and separate function invocations do not share requests.
Use `jev_system_one` for several questions about the same state.
Sail does not cache results between queries.
Sail can send duplicate requests.

### Worker Resource Limits

The [environment-variable table](#environment-variables) gives all resource settings and defaults.
All Jev functions and query partitions in one worker share these limits.
Enter byte values as numbers of bytes.

A request group contains the rows assigned to one API request.
An admitted group has permission to use the worker's request queue.
The pending limits include active groups and groups that wait to run.
The active limit controls the number of HTTP attempts that run at the same time.

The preferred targets control batch size.
When another row would exceed a target, Sail puts that row in a new request group.
A valid row larger than a target can use one request by itself.
A row larger than the hard request limit causes an error.
Sail does not split or shorten that row.

Size calculations include JSON encoding and the generated question IDs.
These limits control Sail resources.
They do not define the provider's schema limits.

Set `SAIL_JEV_MAX_PENDING_BYTES` to at least `SAIL_JEV_MAX_REQUEST_BYTES`.
An invalid combination causes an error instead of an indefinite wait.
Each admitted group reserves the maximum request size before Sail encodes the request.
A small request therefore uses the same byte reservation as a large request.

The byte budget permits this maximum number of simultaneous reservations:

```text
floor(SAIL_JEV_MAX_PENDING_BYTES / SAIL_JEV_MAX_REQUEST_BYTES)
```

For full HTTP concurrency, supply enough group slots and byte reservations for the configured number of active attempts.
Input and output arrays require additional memory that increases with the row count.
Retries reuse the encoded request body.
A retry releases its active-attempt slot during the delay.
Cancellation and terminal errors release request reservations.

## Timeouts and Retries

An attempt is one transmission of an API request, including the response read.
A retry is another attempt after a failure.

### Attempt Timeout

`timeout_ms` limits one full attempt.
Its default is 10000 milliseconds.
The timer starts after Sail obtains an active-attempt slot.
The timer includes the request transmission and the response read.

The Python SDK can apply separate HTTP-operation timeouts.
Sail uses one deadline for the full attempt.

### Retry Budget

`retry_budget_ms` controls whether another attempt can start.
Its default is 30000 milliseconds.
The budget starts with the first attempt.
It includes subsequent delays and waits for an active-attempt slot.

If elapsed time plus the next delay reaches the budget, Sail stops without another attempt.
Sail tests the remaining budget again after it obtains an active-attempt slot.
The budget does not cancel an attempt that already runs.
Sail returns the last error when retries stop.

### Retry Delays and Errors

The default `max_retries` value permits two retries after the first attempt.
The [retry-settings table](#python-sdk-configuration-differences) lists all fixed retry conditions.
Authentication errors, ordinary validation errors, and invalid response data do not cause HTTP retries.

The calculated delay starts at 500 milliseconds and doubles up to 5 seconds.
Sail randomly subtracts up to 25% from this delay.

A valid `retry-after-ms` header has priority over `Retry-After`.
`Retry-After` can contain seconds or an HTTP date.
A server delay replaces the calculated delay and can exceed 5 seconds.
The retry budget can prevent a retry with that delay.

An HTTP date with `GMT` uses UTC.
An older asctime date without a timezone uses the worker's local timezone.

### Cluster Task Retries

A Sail cluster can execute a failed task again.
That task can send more API requests, even when `max_retries` is `0`.
A task can also send a request again after an HTTP error that does not permit HTTP retries.

The separate `SAIL_CLUSTER__TASK_MAX_ATTEMPTS` setting has a default of `3`.
This setting applies to cluster tasks, not individual HTTP requests.
It is not a required Jev setting.

To disable automatic task retries, set this variable in the Sail server configuration before startup:

```sh
export SAIL_CLUSTER__TASK_MAX_ATTEMPTS=1
```

## SQL Expressions

Jev calls work in projections, filters, struct-field access, and ordinary scalar expressions.
The functions are volatile, so repeated evaluation can send new requests.
Query planning and ordinary `EXPLAIN` do not send requests.
`EXPLAIN ANALYZE` executes the query and can send requests.

`CASE`, `AND`, and `OR` do not guarantee that Sail skips a Jev call.
The async evaluator can evaluate the Jev call before the surrounding expression.
For a row that must not send a question, supply a SQL NULL state.

A Jev call cannot contain another async call.
Another async call cannot contain a Jev call.
Save the inner result in a table before you evaluate the outer call.
A view or common table expression does not guarantee this separation.

## Usage Accounting

Token usage applies to the full request.
It does not apply separately to each row or question.
Rows from the same request share a `batch_id` and the same usage counts.

Count each `batch_id` once.
A sum across all rows counts shared usage more than once.
Save the results before you calculate usage, because another query can evaluate volatile functions again.

This example uses a table of previously saved results:

```sql
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

Missing token counts remain unknown.
The provider can receive an attempt even when Sail receives no response.
Returned usage therefore does not give full billing totals for failed attempts or repeated queries.

## Examples

These examples use an API key from the worker environment.
The `messages` table has `id` and `body` columns.

### Detect Threats or Spam

```sql
SELECT id, jev_noul(body, 'Does this message contain a threat?').noul AS threat_probability
FROM messages;

SELECT id, body
FROM messages
WHERE jev_noul(body, 'Is this message spam?').noul >= 0.9;
```

### Select a Team

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

### Score Relevance

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

### Ask Different Questions About One State

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

Use `result.answers['billing']` to read the billing answer.
This VARIANT contains the full Noul answer object.

### Get the Available Models

```sql
SELECT jev_models().models;
```
