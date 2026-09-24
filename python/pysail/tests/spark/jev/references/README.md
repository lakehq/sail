# Pinned TypeSafe contract references

These files are immutable reference fixtures, not imported dependencies or an installed SDK.

- Python package: `typesafe-sdk` version **0.7.1**.
- Repository: <https://github.com/typesafe-ai/typesafe-sdk-python>.
- Commit: `0ffd094c72ed9445223060b24ffd7a56aa781fb4`.
- OpenAPI source: <https://api.typesafe.ai/openapi.json>, OpenAPI **3.1.0**, API **0.2.0**.
- Capture time: **2026-09-24T04:01:38.422538+00:00**. The SDK files were downloaded from immutable commit URLs during this capture session.
- OpenAPI SHA-256: `a191f8a7df6bd6fedced8120dd0fd106f88575d1d1c8360d08900a6c7c0360d5`.

`manifest.json` records each artifact's origin and SHA-256 over the original bytes.
Python and TOML source files have an extra `.txt` suffix so project formatters cannot rewrite upstream fixtures.
The SDK's MIT license is included at `python-sdk/LICENSE`; preserve it with these source extracts.
Do not reformat `openapi.json` or source extracts. Updating the reference contract requires deliberately capturing new artifacts and reviewing the behavior differences.

The pinned Python SDK is authoritative for credential normalization, defaults, retry eligibility, retry headers, and nullable/missing token counts.
The pinned OpenAPI specifies request shapes and the required answer fields.
Sail deliberately differs in these ways:

- SQL-null state skips one row; JSON-null state fails validation.
- `timeout_ms` bounds a complete HTTP attempt, rather than an individual HTTP operation.
- The retry budget is checked again after waiting for a worker attempt permit.
- Required answers must match every requested question ID and type. The SDK can default a missing answer map to empty and ignore future answer types; Sail fails a requested answer with either problem.
- `jev_system_one` preserves additional answer fields; the Python models ignore extra fields.
- Score maps use string keys matching JSON, instead of the SDK's integer-keyed Python maps.
- Batching targets, hard resource limits, worker admission, and shared accounting are Sail controls.

The source was reviewed against Spark's `opt/spark` built-in function registry; Spark has no Jev functions to match.
Deterministic fixture validation does not establish live-service behavior.
