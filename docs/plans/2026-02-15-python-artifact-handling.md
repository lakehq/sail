# Python Artifact Handling (Spark Connect) Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement Spark Connect artifact upload/status handling in Sail for Python-focused artifacts (`pyfiles/`, `archives/`, `files/`, `cache/`) so `pyspark`/`pysail` clients can successfully upload per-session artifacts and activate them in session worker runtime.

**Architecture:** Add a per-session artifact registry to `SparkSession`, implement streamed artifact assembly with CRC validation in `artifact_manager`, and persist accepted artifacts into a session-scoped local directory. Build a `PythonRuntimeActivation` snapshot in `SparkSession` from uploaded artifacts + session config, then propagate it to cluster worker launch options so workers start with session-specific `PYTHONPATH` and optional Python executable override. For Kubernetes, activation assumes a shared artifact path visible to driver and worker pods (no new artifact distribution service in this milestone).

**Tech Stack:** Rust (`tonic`, `tokio`, `datafusion` session extensions), Spark Connect protobuf-generated types, PySpark Connect client behavior, `pytest` for Python integration checks.

---

## Scope and Non-Goals

- In scope:
  - `AddArtifacts` RPC end-to-end behavior for batch and chunked payloads.
  - CRC verification and summary reporting.
  - `ArtifactStatus` RPC backed by session registry.
  - Safe on-disk storage for session artifacts.
  - Session-scoped Python runtime activation metadata (artifact-derived `PYTHONPATH` + optional Python executable override).
  - Worker launch/runtime plumbing so activation applies to newly launched workers and refreshed workers.
  - Basic refresh policy on runtime revision change (mark stale workers and recycle them when safe, e.g., idle/before next task wave).
  - Python integration tests that validate upload + status semantics.
- Out of scope (follow-up milestone):
  - Full per-session environment build/sync without shared filesystem (e.g., uploading once then broadcasting to all pods automatically).
  - Zero-restart, in-flight runtime mutation for already running Python tasks.
  - Hard isolation guarantees for local-cluster in-process workers across simultaneously active sessions.
  - Full JVM Spark parity for classloader/jar behaviors.

## Implementation Notes

- Follow `@test-driven-development` for every task.
- Use `@verification-before-completion` before claiming completion.
- Keep changes DRY and YAGNI.

### Runtime Refresh Trigger Matrix

| Change source | Example | Bump runtime revision | Refresh/recycle workers | Notes |
| --- | --- | --- | --- | --- |
| Python path artifact add/update | `pyfiles/mod.py`, `pyfiles/lib.zip`, `archives/env.tar.gz#environment` | Yes | Yes (best-effort when safe) | Recompute session `PYTHONPATH` entries. |
| Non-python artifact add/update | `files/config.yaml` | No | No | Stored and queryable via `ArtifactStatus`, but not part of Python runtime activation. |
| Cache artifact add/update | `cache/<sha256>` | No | No | Execution cache payload only; no interpreter path change. |
| Python executable config set/unset | `spark.sql.execution.pyspark.python`, fallback `spark.pyspark.python` | Yes | Yes (best-effort when safe) | Changes worker Python executable selection. |
| Other config changes | `spark.sql.shuffle.partitions` | No | No | Outside Python runtime activation scope. |
| Failed upload / CRC failure | bad CRC summary | No | No | Artifact is not activated if upload is not accepted. |

### Task 1: Add Failing Rust Tests for Artifact RPC Semantics

**Files:**
- Modify: `crates/sail-spark-connect/src/service/artifact_manager.rs`
- Test: `crates/sail-spark-connect/src/service/artifact_manager.rs` (new `#[cfg(test)]` module)

**Step 1: Write failing tests for upload/state/status behavior**

```rust
#[tokio::test]
async fn add_artifacts_batch_marks_status_exists() {
    // Build a stream with Payload::Batch containing pyfiles/mod.py
    // Assert returned summary has is_crc_successful=true
    // Assert handle_artifact_statuses(..., ["pyfiles/mod.py"]) => exists=true
}

#[tokio::test]
async fn add_artifacts_chunked_reassembles_file() {
    // BeginChunk + Chunk flow for archives/env.tar.gz
    // Assert CRC success + exists=true
}

#[tokio::test]
async fn add_artifacts_rejects_invalid_paths() {
    // Try ../escape.py and absolute path
    // Assert invalid argument error
}

#[tokio::test]
async fn add_artifacts_crc_failure_reports_unsuccessful_and_not_persisted() {
    // Send bad CRC
    // Assert summary.is_crc_successful=false and status exists=false
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p sail-spark-connect add_artifacts -- --nocapture`
Expected: FAIL with `not implemented: handle add artifacts`.

**Step 3: Commit test scaffolding only**

```bash
git add crates/sail-spark-connect/src/service/artifact_manager.rs
git commit -m "test(spark-connect): add failing tests for artifact upload and status semantics"
```

### Task 2: Add Session-Scoped Artifact Registry to SparkSession

**Files:**
- Modify: `crates/sail-spark-connect/src/session.rs`
- Test: `crates/sail-spark-connect/src/session.rs` (new unit tests)

**Step 1: Write failing tests for registry operations**

```rust
#[test]
fn artifact_registry_tracks_exists_and_lookup() {
    // register("pyfiles/mod.py", "/tmp/.../mod.py")
    // assert exists("pyfiles/mod.py")
    // assert !exists("pyfiles/missing.py")
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p sail-spark-connect artifact_registry_tracks_exists_and_lookup -- --nocapture`
Expected: FAIL because registry APIs do not exist.

**Step 3: Implement minimal registry in `SparkSessionState`**

```rust
struct SparkSessionState {
    config: SparkRuntimeConfig,
    executors: HashMap<String, Arc<Executor>>,
    streaming_queries: StreamingQueryManager,
    artifacts: HashMap<String, std::path::PathBuf>,
}

pub(crate) fn set_artifact_path(&self, name: String, path: PathBuf) -> SparkResult<()> { ... }
pub(crate) fn has_artifact(&self, name: &str) -> SparkResult<bool> { ... }
```

**Step 4: Run tests to verify pass**

Run: `cargo test -p sail-spark-connect artifact_registry_tracks_exists_and_lookup -- --nocapture`
Expected: PASS.

**Step 5: Commit**

```bash
git add crates/sail-spark-connect/src/session.rs
git commit -m "feat(spark-connect): add session artifact registry in SparkSession state"
```

### Task 3: Implement Safe Artifact Stream Assembly and Persistence

**Files:**
- Modify: `crates/sail-spark-connect/src/service/artifact_manager.rs`
- Modify: `crates/sail-spark-connect/src/error.rs` (only if new helper errors needed)

**Step 1: Write failing tests for path normalization and staged writes**

```rust
#[test]
fn normalize_artifact_path_rejects_parent_and_absolute() {
    assert!(normalize_artifact_path("pyfiles/mod.py").is_ok());
    assert!(normalize_artifact_path("../x.py").is_err());
    assert!(normalize_artifact_path("/tmp/x.py").is_err());
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p sail-spark-connect normalize_artifact_path_rejects_parent_and_absolute -- --nocapture`
Expected: FAIL because helper does not exist.

**Step 3: Implement upload pipeline**

```rust
pub(crate) async fn handle_add_artifacts(...) -> SparkResult<Vec<ArtifactSummary>> {
    // 1) Resolve SparkSession extension from ctx.
    // 2) Consume Payload stream state machine:
    //    - Batch => immediate single-chunk artifact processing.
    //    - BeginChunk => create active chunked artifact state.
    //    - Chunk => append; finalize when expected chunk count reached.
    // 3) CRC check each chunk and overall artifact result.
    // 4) Persist accepted artifacts to per-session directory:
    //    <temp>/sail-spark-connect/<session-id>/artifacts/<relative-name>
    // 5) Register persisted paths in SparkSession artifact registry.
    // 6) Return summaries with per-artifact CRC status.
}
```

**Step 4: Run focused Rust tests**

Run: `cargo test -p sail-spark-connect add_artifacts -- --nocapture`
Expected: PASS for new upload tests.

**Step 5: Commit**

```bash
git add crates/sail-spark-connect/src/service/artifact_manager.rs crates/sail-spark-connect/src/error.rs
git commit -m "feat(spark-connect): implement streamed artifact upload with CRC and safe persistence"
```

### Task 4: Implement Artifact Status RPC Backed by Session Registry

**Files:**
- Modify: `crates/sail-spark-connect/src/service/artifact_manager.rs`
- Test: `crates/sail-spark-connect/src/service/artifact_manager.rs` (extend tests)

**Step 1: Write failing test for mixed status lookup**

```rust
#[tokio::test]
async fn artifact_status_returns_exists_for_present_and_false_for_missing() {
    // add pyfiles/a.py
    // query ["pyfiles/a.py", "pyfiles/missing.py"]
    // assert {true, false}
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p sail-spark-connect artifact_status_returns_exists_for_present_and_false_for_missing -- --nocapture`
Expected: FAIL with `not implemented: handle artifact statuses`.

**Step 3: Implement `handle_artifact_statuses`**

```rust
pub(crate) async fn handle_artifact_statuses(
    ctx: &SessionContext,
    names: Vec<String>,
) -> SparkResult<HashMap<String, ArtifactStatus>> {
    // Resolve SparkSession extension
    // Return exists flag for each name from registry
}
```

**Step 4: Run focused tests**

Run: `cargo test -p sail-spark-connect artifact_status -- --nocapture`
Expected: PASS.

**Step 5: Commit**

```bash
git add crates/sail-spark-connect/src/service/artifact_manager.rs
git commit -m "feat(spark-connect): implement artifact status lookup"
```

### Task 5: Enforce Python-Focused Prefix Policy in This Milestone

**Files:**
- Modify: `crates/sail-spark-connect/src/service/artifact_manager.rs`
- Test: `crates/sail-spark-connect/src/service/artifact_manager.rs`

**Step 1: Write failing tests for prefix allowlist**

```rust
#[test]
fn validate_artifact_prefix_allows_python_related_prefixes() {
    assert!(validate_artifact_name("pyfiles/a.py").is_ok());
    assert!(validate_artifact_name("archives/env.tar.gz").is_ok());
    assert!(validate_artifact_name("files/config.yaml").is_ok());
    assert!(validate_artifact_name("cache/abc123").is_ok());
    assert!(validate_artifact_name("jars/a.jar").is_err());
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p sail-spark-connect validate_artifact_prefix_allows_python_related_prefixes -- --nocapture`
Expected: FAIL because validation helper is missing.

**Step 3: Implement allowlist validation**

```rust
fn validate_artifact_name(name: &str) -> SparkResult<PathBuf> {
    // normalize + reject absolute/parent traversal
    // accept prefixes: pyfiles/, archives/, files/, cache/
    // reject everything else for now with clear NotSupported error
}
```

**Step 4: Run targeted tests**

Run: `cargo test -p sail-spark-connect validate_artifact_prefix -- --nocapture`
Expected: PASS.

**Step 5: Commit**

```bash
git add crates/sail-spark-connect/src/service/artifact_manager.rs
git commit -m "feat(spark-connect): add python-focused artifact prefix validation"
```

### Task 6: Add Python Integration Tests for AddArtifacts + ArtifactStatus

**Files:**
- Create: `python/pysail/tests/spark/test_artifact_upload.py`
- Modify: `python/pysail/tests/spark/conftest.py` (only if helper fixture needed)

**Step 1: Write failing integration tests**

```python
import uuid
from pathlib import Path

import pyspark.sql.connect.proto as proto

def test_add_artifact_pyfile_and_status_exists(spark, tmp_path):
    mod = tmp_path / "mod_a.py"
    mod.write_text("VALUE = 1\n", encoding="utf-8")

    spark.addArtifact(str(mod), pyfile=True)

    artifact_name = f"pyfiles/{mod.name}"
    req = proto.ArtifactStatusesRequest(session_id=spark._client._session_id, names=[artifact_name])
    resp = spark._client._artifact_manager._stub.ArtifactStatus(req, metadata=spark._client._builder.metadata())
    assert resp.statuses[artifact_name].exists is True
```

**Step 2: Run test to verify it fails**

Run: `hatch run test.spark-4.1.1:pytest python/pysail/tests/spark/test_artifact_upload.py -v`
Expected: FAIL due to unimplemented server artifact handlers.

**Step 3: Adjust test for session-safe naming and cleanup**

```python
def _unique_name(base: str) -> str:
    return f"{uuid.uuid4().hex}_{base}"
```

**Step 4: Run test to verify it passes**

Run: `hatch run test.spark-4.1.1:pytest python/pysail/tests/spark/test_artifact_upload.py -v`
Expected: PASS.

**Step 5: Commit**

```bash
git add python/pysail/tests/spark/test_artifact_upload.py python/pysail/tests/spark/conftest.py
git commit -m "test(pysail): cover python artifact upload and status via Spark Connect"
```

### Task 7: Add Session-Level Python Runtime Activation State

**Files:**
- Modify: `crates/sail-spark-connect/src/session.rs`
- Modify: `crates/sail-spark-connect/src/service/artifact_manager.rs`
- Modify: `crates/sail-spark-connect/src/service/config_manager.rs`

**Step 1: Write failing unit tests for runtime activation state**

```rust
#[test]
fn python_runtime_activation_tracks_pythonpath_entries_from_artifacts() {
    // register pyfiles/mod.py and archives/deps.zip
    // assert runtime snapshot includes deterministic python_path entries
}

#[test]
fn python_runtime_activation_honors_python_exec_config() {
    // set spark.sql.execution.pyspark.python
    // assert runtime snapshot.python_executable is updated
}

#[test]
fn python_runtime_activation_revision_increments_on_change() {
    // capture revision, mutate artifacts/config, verify monotonic increment
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p sail-spark-connect python_runtime_activation -- --nocapture`
Expected: FAIL because runtime activation state APIs do not exist.

**Step 3: Implement activation snapshot APIs in `SparkSession`**

```rust
struct PythonRuntimeActivation {
    revision: u64,
    python_executable: Option<String>,
    python_path_entries: Vec<std::path::PathBuf>,
}

pub(crate) fn update_python_runtime_from_artifact(...) -> SparkResult<()>;
pub(crate) fn update_python_runtime_from_config(...) -> SparkResult<()>;
pub(crate) fn get_python_runtime_activation(&self) -> SparkResult<PythonRuntimeActivation>;
```

Implementation details:
- Include uploaded artifact paths from `pyfiles/` and Python-importable archives (`.zip`, `.egg`, `.whl`) in `python_path_entries`.
- Read executable override from `spark.sql.execution.pyspark.python` (fallback `spark.pyspark.python` if set).
- Keep deterministic ordering and dedupe for stable snapshots.

**Step 4: Run tests to verify pass**

Run: `cargo test -p sail-spark-connect python_runtime_activation -- --nocapture`
Expected: PASS.

**Step 5: Commit**

```bash
git add crates/sail-spark-connect/src/session.rs crates/sail-spark-connect/src/service/artifact_manager.rs crates/sail-spark-connect/src/service/config_manager.rs
git commit -m "feat(spark-connect): add session python runtime activation state"
```

### Task 8: Plumb Runtime Activation to Worker Launch and Refresh

**Files:**
- Modify: `crates/sail-common-datafusion/src/session/job.rs`
- Modify: `crates/sail-execution/src/job_runner.rs`
- Modify: `crates/sail-execution/src/driver/event.rs`
- Modify: `crates/sail-execution/src/driver/actor/core.rs`
- Modify: `crates/sail-execution/src/driver/actor/handler.rs`
- Modify: `crates/sail-execution/src/driver/worker_pool/options.rs`
- Modify: `crates/sail-execution/src/driver/worker_pool/core.rs`
- Modify: `crates/sail-execution/src/worker_manager/options.rs`
- Modify: `crates/sail-execution/src/worker_manager/kubernetes.rs`
- Modify: `crates/sail-execution/src/worker/options.rs`
- Modify: `crates/sail-common/src/config/application.rs` (if new cluster env keys needed)

**Step 1: Write failing tests for runtime plumbing**

```rust
#[test]
fn worker_launch_options_include_python_runtime_fields() {
    // assert python_executable/python_path are copied from worker pool options
}

#[test]
fn kubernetes_worker_env_includes_python_runtime() {
    // assert PYSPARK_PYTHON and PYTHONPATH are emitted when runtime is present
}
```

**Step 2: Run focused tests to verify failure**

Run:
- `cargo test -p sail-execution worker_launch_options_include_python_runtime_fields -- --nocapture`
- `cargo test -p sail-execution kubernetes_worker_env_includes_python_runtime -- --nocapture`
Expected: FAIL because launch options and env mapping do not include runtime activation fields.

**Step 3: Implement runtime propagation and refresh API**

```rust
// job.rs
async fn update_worker_python_runtime(&self, runtime: WorkerPythonRuntime) -> Result<()>;

// driver/event.rs
DriverEvent::UpdateWorkerPythonRuntime { runtime, force_refresh, result }
```

Implementation details:
- Add `WorkerPythonRuntime` to execution plumbing (job runner -> driver -> worker pool -> launch options).
- Update `WorkerPool::start_worker` to attach runtime activation fields.
- Kubernetes: export `PYTHONPATH` and optional `PYSPARK_PYTHON` from runtime snapshot.
- Add a refresh path so runtime updates can recycle existing workers for that session.
- Refresh behavior for this milestone: best-effort worker recycle when safe; no attempt to mutate in-flight Python task runtime.

**Step 4: Run tests to verify pass**

Run:
- `cargo test -p sail-execution worker_launch_options_include_python_runtime_fields -- --nocapture`
- `cargo test -p sail-execution kubernetes_worker_env_includes_python_runtime -- --nocapture`
Expected: PASS.

**Step 5: Commit**

```bash
git add crates/sail-common-datafusion/src/session/job.rs crates/sail-execution/src/job_runner.rs crates/sail-execution/src/driver/event.rs crates/sail-execution/src/driver/actor/core.rs crates/sail-execution/src/driver/actor/handler.rs crates/sail-execution/src/driver/worker_pool/options.rs crates/sail-execution/src/driver/worker_pool/core.rs crates/sail-execution/src/worker_manager/options.rs crates/sail-execution/src/worker_manager/kubernetes.rs crates/sail-execution/src/worker/options.rs crates/sail-common/src/config/application.rs
git commit -m "feat(execution): propagate session python runtime activation to worker launch"
```

### Task 9: Wire Artifact/Config Changes to Runtime Activation + Integration Coverage

**Files:**
- Modify: `crates/sail-spark-connect/src/service/artifact_manager.rs`
- Modify: `crates/sail-spark-connect/src/service/config_manager.rs`
- Create: `python/pysail/tests/spark/test_python_artifact_runtime_activation.py`

**Step 1: Add failing integration test for import path activation**

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

def test_add_artifact_pyfile_is_importable_in_python_udf(spark, tmp_path):
    mod = tmp_path / "dep_mod.py"
    mod.write_text("def plus_one(x):\n    return x + 1\n", encoding="utf-8")
    spark.addArtifact(str(mod), pyfile=True)

    f = udf(lambda x: __import__("dep_mod").plus_one(x), IntegerType())
    rows = spark.sql("select 1 as x").select(f("x").alias("y")).collect()
    assert rows[0].y == 2
```

**Step 2: Run integration test to verify failure**

Run: `hatch run test.spark-4.1.1:pytest python/pysail/tests/spark/test_python_artifact_runtime_activation.py -v`
Expected: FAIL before runtime activation wiring is complete.

**Step 3: Wire runtime updates from artifact/config mutation points**

Implementation details:
- After successful artifact persistence/registration, recompute session runtime snapshot.
- After `Config.Set`/`Config.Unset` touching Python executable keys, recompute runtime snapshot.
- Push snapshot to `JobService::runner().update_worker_python_runtime(...)`.

**Step 4: Run integration and targeted Rust tests**

Run:
- `cargo test -p sail-spark-connect add_artifacts -- --nocapture`
- `hatch run test.spark-4.1.1:pytest python/pysail/tests/spark/test_python_artifact_runtime_activation.py -v`

Expected: PASS.

**Step 5: Commit**

```bash
git add crates/sail-spark-connect/src/service/artifact_manager.rs crates/sail-spark-connect/src/service/config_manager.rs python/pysail/tests/spark/test_python_artifact_runtime_activation.py
git commit -m "feat(spark-connect): activate session python artifacts in worker runtime"
```

### Task 10: Full Verification and Guardrail Documentation

**Files:**
- Create: `docs/development/recipes/python-artifacts.md`
- Modify: `docs/development/recipes/index.md` (or nearest recipe index)

**Step 1: Add/update operator docs for activation behavior**

```markdown
- Supported prefixes: `pyfiles/`, `archives/`, `files/`, `cache/`
- Runtime activation: session worker `PYTHONPATH` + optional `spark.sql.execution.pyspark.python`
- Kubernetes note: requires shared artifact mount between driver and worker pods
- Safety: path traversal rejected; CRC required
```

**Step 2: Run verification suite**

Run:
- `cargo test -p sail-spark-connect -- --nocapture`
- `cargo test -p sail-execution -- --nocapture`
- `hatch run test.spark-4.1.1:pytest python/pysail/tests/spark/test_artifact_upload.py python/pysail/tests/spark/test_python_artifact_runtime_activation.py -v`

Expected: PASS for all modified scopes.

**Step 3: Run formatting/lint if required by repo policy**

Run: `cargo fmt --check` and project Rust lints for touched crates.
Expected: PASS.

**Step 4: Commit**

```bash
git add docs/development/recipes/python-artifacts.md docs/development/recipes/index.md
git commit -m "docs: document python artifact upload and runtime activation"
```

## Final Verification Checklist

- [ ] `AddArtifacts` supports batch + chunked payloads.
- [ ] CRC failures return `is_crc_successful=false` and do not persist artifacts.
- [ ] `ArtifactStatus` reports accurate existence per requested name.
- [ ] Invalid paths (`..`, absolute) are rejected.
- [ ] Python-focused prefix policy enforced.
- [ ] Session runtime activation snapshot is computed from artifact + config state.
- [ ] Worker launch path receives runtime activation (`PYTHONPATH`, optional Python executable).
- [ ] Runtime updates can refresh workers so activation is effective for the session.
- [ ] Python integration tests pass for upload/status + runtime activation behavior.
- [ ] Documentation added for activation behavior and deployment caveats.

## Follow-up Plan (Next Iteration)

- Add automated artifact distribution for Kubernetes when shared filesystem is unavailable.
- Extend activation to archive extraction workflows (`tar.gz`/venv bundles) with explicit interpreter discovery policy.
- Add cache eviction / disk quota management for session artifact directories.
