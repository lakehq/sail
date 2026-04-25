---
title: Python Artifact Upload and Runtime Activation
rank: 25
---

# Python Artifact Upload and Runtime Activation

Sail's Spark Connect server supports uploading session-scoped Python artifacts and activating them on workers so that PySpark UDFs can import uploaded modules and archives.

## Supported prefixes

Only the following artifact name prefixes are accepted (Python-focused scope):

| Prefix     | Use |
|-----------|-----|
| `pyfiles/`  | Python source files (e.g. `pyfiles/mymod.py`). The directory containing the file is added to worker `PYTHONPATH`. |
| `archives/` | Zip/egg/wheel archives (e.g. `archives/deps.zip`). The archive path is added to worker `PYTHONPATH` for `zipimport`. |
| `files/`    | Generic files. Stored and queryable via `ArtifactStatus`; not used for Python import path. |
| `cache/`    | Execution cache payloads. Not part of Python runtime activation. |

Other prefixes (e.g. `jars/`) are rejected with a clear error.

## Runtime activation

- **Session worker `PYTHONPATH`**: When you add artifacts under `pyfiles/` or Python-importable archives under `archives/`, the server builds a snapshot and propagates it to worker launch options. Workers started or refreshed after that receive the corresponding `PYTHONPATH` (and optional Python executable override) so that `import mymod` and `zipimport` work as expected.
- **Python executable override**: You can set `spark.sql.execution.pyspark.python` (or `spark.pyspark.python` as fallback) to choose the worker Python binary. This is propagated to workers as the `PYSPARK_PYTHON` environment variable.

## Safety and validation

- **Path traversal**: Artifact names must be relative; `..` and absolute paths are rejected.
- **CRC**: Each artifact chunk is verified with CRC-32. If verification fails, the artifact is not persisted and the response reports `is_crc_successful: false`.

## Kubernetes deployment

- Workers receive `PYTHONPATH` and `PYSPARK_PYTHON` via pod environment variables when the session has uploaded Python-path artifacts or set the Python executable config.
- This milestone assumes a **shared artifact path** visible to both driver and worker pods (e.g. a shared volume). The server writes artifacts under a per-session directory; workers must have the same path available so that the entries in `PYTHONPATH` are valid on the worker node. Automated artifact distribution to workers without a shared filesystem is out of scope for this release.

## Refresh behavior

- When Python path artifacts or the Python executable config change, the session’s runtime revision is bumped and the new snapshot is pushed to the job runner. Newly launched workers use the updated snapshot. Existing workers are **not** mutated in-flight; best-effort refresh (e.g. recycle idle workers) is in scope, but zero-restart in-flight runtime mutation is not.
