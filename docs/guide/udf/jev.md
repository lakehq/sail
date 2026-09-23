---
title: Jev Functions
rank: 3
---

# Jev Functions

Sail provides four SQL functions for [TypeSafe AI's Jev](https://docs.typesafe.ai/api) decision model.
Each function combines nearby non-null rows into one request to `POST /v1/systemone` when they use the same API key and model.
Sail places the row states in an `items` array and scopes each named question to one item, following [TypeSafe's multi-item question pattern](https://docs.typesafe.ai/model-jaggedness/jev-1.13#counting).
Each request includes at most 16 rows and 64 questions, with a conservative size threshold of roughly 32 KB; a larger single row is sent on its own.
Batching happens within each Sail execution batch, so a query may make several requests even when it has fewer than 16 rows overall.
The functions are volatile because their answers depend on an external service.

| Function | Inputs | Result |
| --- | --- | --- |
| `jev_noul` | `state, instructions, api_key[, model]` | Yes probability as `DOUBLE` |
| `jev_choice` | `state, instructions, criteria, api_key[, model]` | Struct with `choice`, `confidence`, and `probabilities` |
| `jev_score` | `state, instructions, criteria, api_key[, model]` | Struct with `score`, `confidence`, `probabilities`, and `legend` |
| `jev_evaluate` | `state, questions, api_key[, model]` | Full JSON response with named answers, resolved model, and token usage |

`state` can be text or a SQL struct, map, or array.
For `jev_choice`, `criteria` is a SQL map or a JSON object string mapping option names to descriptions.
For `jev_score`, `criteria` is an ordered SQL array or a JSON array string of level descriptions.
For `jev_evaluate`, `questions` is a SQL map or JSON object string of named Jev question definitions.
The default model is `jev-latest`.
Pass a versioned model ID to pin behavior.

```sql
SELECT jev_noul(
  message,
  'Does this convey urgency?',
  api_key
) AS urgency_probability
FROM tickets;

SELECT jev_choice(
  message,
  'Which team should handle this?',
  map('billing', 'Payments and refunds', 'technical', 'Bugs and outages'),
  api_key
) AS routing
FROM tickets;

SELECT jev_score(
  message,
  'How frustrated is the customer?',
  array('Calm', 'Frustrated', 'Very angry'),
  api_key
) AS frustration
FROM tickets;

SELECT jev_evaluate(
  message,
  '{"urgent":{"type":"noul","instructions":"Is this urgent?"},"team":{"type":"choice","instructions":"Which team should handle this?","criteria":{"billing":"Payments","technical":"Bugs"}}}',
  api_key
) AS evaluation
FROM tickets;
```

The `api_key` argument is sent in the HTTP `Authorization: Bearer` header.
Use a protected column or client-supplied value and avoid writing a literal key into shared SQL text, query logs, or saved plans.
In a distributed Sail deployment, the key travels with the query data to the worker that executes the function.
The functions skip rows with a null argument and return null for those rows.
For a multi-row `jev_evaluate` call, the JSON result for each row includes the shared request's `usage`, plus `usage_scope: "batch"` and `batch_size`.
The shared token usage should be counted once per batch, not once per output row.
Since Jev sees all batched states together, compare answer quality with single-row calls for your workload, especially when states contain unrelated detail.
Rate-limit (`429`) and overload (`529`) responses are retried up to two times with backoff.
