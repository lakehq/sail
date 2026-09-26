//! Callback-scoped adapter over the existing RuntimeEnv object-store registry.
use std::collections::BTreeMap;
use std::future::Future;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use datafusion::execution::context::SessionConfig;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion_common::config::ConfigOptions;
use datafusion_common::{DataFusionError, Result};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectMeta, ObjectStoreExt};
use pyo3::exceptions::{
    PyFileNotFoundError, PyInterruptedError, PyOSError, PyPermissionError, PyRuntimeError,
    PyValueError,
};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use sail_object_store::{qualify_object_store_path, resolve_object_store_location};
use tokio::runtime::Handle;
use tokio_util::sync::{CancellationToken, DropGuard};

use super::error::py_err;
use crate::listing::utils::list_all_files;
use crate::url::resolve_listing_urls;
const PYTHON_OBJECT_STORE_MODULE: &str = "pysail.spark.datasource.object_store";
type PythonObjectMeta = (String, u64, String, Option<String>, Option<String>);

#[derive(Clone, Debug)]
pub(crate) struct PythonObjectStoreContext {
    runtime_env: Arc<RuntimeEnv>,
    runtime_handle: Handle,
    session_config: SessionConfig,
    canceled: CancellationToken,
}
impl PythonObjectStoreContext {
    pub(crate) fn try_new(runtime_env: Arc<RuntimeEnv>, options: &ConfigOptions) -> Result<Self> {
        let session_config = SessionConfig::from(options.clone());
        let runtime_handle = Handle::try_current().map_err(|e| {
            DataFusionError::Execution(format!("Python storage requires a Tokio runtime: {e}"))
        })?;
        Ok(Self {
            runtime_env,
            runtime_handle,
            session_config,
            canceled: CancellationToken::new(),
        })
    }
    pub(crate) fn child(&self) -> Self {
        Self {
            canceled: self.canceled.child_token(),
            ..self.clone()
        }
    }
    pub(crate) fn cancel_on_drop(&self) -> DropGuard {
        self.canceled.clone().drop_guard()
    }
    fn run<T: Send>(
        &self,
        py: Python<'_>,
        operation: impl Future<Output = PyResult<T>> + Send,
    ) -> PyResult<T> {
        py.detach(|| self.runtime_handle.block_on(async {
            tokio::select! {
                biased;
                _ = self.canceled.cancelled() => Err(PyInterruptedError::new_err("Sail storage operation canceled")),
                result = operation => result,
            }
        }))
    }
}
fn storage_error(error: object_store::Error) -> PyErr {
    match error {
        object_store::Error::NotFound { .. } => PyFileNotFoundError::new_err(error.to_string()),
        object_store::Error::PermissionDenied { .. } => {
            PyPermissionError::new_err(error.to_string())
        }
        _ => PyOSError::new_err(error.to_string()),
    }
}
fn discovery_error(error: DataFusionError) -> PyErr {
    match error {
        DataFusionError::ObjectStore(error) => storage_error(*error),
        DataFusionError::External(error) => match error.downcast::<object_store::Error>() {
            Ok(error) => storage_error(*error),
            Err(error) => PyOSError::new_err(error.to_string()),
        },
        DataFusionError::Plan(message) => PyValueError::new_err(message),
        error => PyOSError::new_err(error.to_string()),
    }
}
fn metadata(base: &ObjectStoreUrl, meta: ObjectMeta) -> PyResult<PythonObjectMeta> {
    Ok((
        qualify_object_store_path(base, &meta.location)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?,
        meta.size,
        meta.last_modified.to_rfc3339(),
        meta.e_tag,
        meta.version,
    ))
}
fn range(start: u64, end: u64, max_bytes: u64) -> PyResult<Range<u64>> {
    if end < start || end - start > max_bytes {
        return Err(PyValueError::new_err("invalid or oversized byte range"));
    }
    Ok(start..end)
}
async fn collect_bounded(
    mut stream: BoxStream<'static, object_store::Result<Bytes>>,
    max_bytes: usize,
) -> PyResult<Vec<u8>> {
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.try_next().await.map_err(storage_error)? {
        if chunk.len() > max_bytes.saturating_sub(bytes.len()) {
            return Err(PyValueError::new_err(
                "object exceeds max_bytes; use iter_bytes",
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

#[pyclass(name = "_SailObjectStore")]
struct PySailObjectStore {
    context: PythonObjectStoreContext,
}
impl PySailObjectStore {
    fn resolve(&self, location: &str) -> PyResult<sail_object_store::ResolvedObjectStorePath> {
        if self.context.canceled.is_cancelled() {
            return Err(PyInterruptedError::new_err(
                "Sail storage callback has ended",
            ));
        }
        resolve_object_store_location(self.context.runtime_env.as_ref(), location)
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }
}
#[pymethods]
impl PySailObjectStore {
    fn close(&self) {
        self.context.canceled.cancel();
    }
    fn read(&self, py: Python<'_>, location: &str, max_bytes: usize) -> PyResult<Py<PyBytes>> {
        let resolved = self.resolve(location)?;
        let bytes = self.context.run(py, async {
            let result = resolved
                .store()
                .get(resolved.prefix())
                .await
                .map_err(storage_error)?;
            if result.meta.size > max_bytes as u64 {
                return Err(PyValueError::new_err(
                    "object exceeds max_bytes; use iter_bytes",
                ));
            }
            collect_bounded(result.into_stream(), max_bytes).await
        })?;
        Ok(PyBytes::new(py, &bytes).unbind())
    }
    fn read_range(
        &self,
        py: Python<'_>,
        location: &str,
        start: u64,
        end: u64,
        max_bytes: u64,
    ) -> PyResult<Py<PyBytes>> {
        let range = range(start, end, max_bytes)?;
        let resolved = self.resolve(location)?;
        let bytes = self.context.run(py, async {
            if range.is_empty() {
                return Ok(Bytes::new());
            }
            resolved
                .store()
                .get_range(resolved.prefix(), range)
                .await
                .map_err(storage_error)
        })?;
        Ok(PyBytes::new(py, &bytes).unbind())
    }
    fn read_ranges(
        &self,
        py: Python<'_>,
        location: &str,
        ranges: Vec<(u64, u64)>,
        max_bytes: u64,
        max_ranges: usize,
    ) -> PyResult<Vec<Py<PyBytes>>> {
        if ranges.len() > max_ranges {
            return Err(PyValueError::new_err("too many byte ranges"));
        }
        let mut remaining = max_bytes;
        let ranges = ranges
            .into_iter()
            .map(|(start, end)| {
                let range = range(start, end, remaining)?;
                remaining -= end - start;
                Ok(range)
            })
            .collect::<PyResult<Vec<_>>>()?;
        let resolved = self.resolve(location)?;
        let nonempty = ranges
            .iter()
            .filter(|r| !r.is_empty())
            .cloned()
            .collect::<Vec<_>>();
        let chunks = self.context.run(py, async {
            if nonempty.is_empty() {
                return Ok(vec![]);
            }
            resolved
                .store()
                .get_ranges(resolved.prefix(), &nonempty)
                .await
                .map_err(storage_error)
        })?;
        let mut chunks = chunks.into_iter();
        ranges
            .iter()
            .map(|range| {
                let bytes = if range.is_empty() {
                    Bytes::new()
                } else {
                    chunks
                        .next()
                        .ok_or_else(|| PyOSError::new_err("object store returned too few ranges"))?
                };
                Ok(PyBytes::new(py, &bytes).unbind())
            })
            .collect()
    }
    fn write(&self, py: Python<'_>, location: &str, data: &Bound<'_, PyBytes>) -> PyResult<()> {
        let resolved = self.resolve(location)?;
        let payload = Bytes::copy_from_slice(data.as_bytes()).into();
        self.context.run(py, async {
            resolved
                .store()
                .put(resolved.prefix(), payload)
                .await
                .map_err(storage_error)?;
            Ok(())
        })
    }
    fn delete(&self, py: Python<'_>, location: &str) -> PyResult<()> {
        let resolved = self.resolve(location)?;
        self.context.run(py, async {
            resolved
                .store()
                .delete(resolved.prefix())
                .await
                .map_err(storage_error)
        })
    }
    fn head(&self, py: Python<'_>, location: &str) -> PyResult<PythonObjectMeta> {
        let resolved = self.resolve(location)?;
        let meta = self.context.run(py, async {
            resolved
                .store()
                .head(resolved.prefix())
                .await
                .map_err(storage_error)
        })?;
        metadata(resolved.object_store_url(), meta)
    }
    /// File discovery shares the native parser, filtering, and listing cache.
    fn glob(
        &self,
        py: Python<'_>,
        pattern: &str,
        max_entries: usize,
    ) -> PyResult<Vec<PythonObjectMeta>> {
        self.context.run(py, async {
            // Deferred optimization: lazily reuse this discovery setup within one callback's
            // native store, releasing it on close. Keep it out of the cloned context so it
            // cannot accidentally span callbacks. This only saves setup for repeated glob
            // calls; it does not cache matches, Parquet footers, row groups, or byte reads.
            // Retain per-call construction unless reuse fits that existing ownership boundary.
            let state = SessionStateBuilder::new()
                .with_config(self.context.session_config.clone())
                .with_runtime_env(self.context.runtime_env.clone())
                .build();
            let urls = resolve_listing_urls(&state, vec![pattern.to_owned()])
                .await
                .map_err(discovery_error)?;
            let mut matches = BTreeMap::new();
            for url in urls {
                let store = self
                    .context
                    .runtime_env
                    .object_store(&url)
                    .map_err(discovery_error)?;
                let mut files = list_all_files(&url, &state, store.as_ref(), None)
                    .await
                    .map_err(discovery_error)?;
                while let Some(meta) = files.try_next().await.map_err(discovery_error)? {
                    let meta = metadata(&url.object_store(), meta)?;
                    if !matches.contains_key(&meta.0) && matches.len() >= max_entries {
                        return Err(PyValueError::new_err("glob exceeds max_entries"));
                    }
                    matches.insert(meta.0.clone(), meta);
                }
            }
            Ok(matches.into_values().collect())
        })
    }
    fn iter_objects(&self, location: &str, batch_size: usize) -> PyResult<PyObjectMetaIterator> {
        if batch_size == 0 || batch_size > 4096 {
            return Err(PyValueError::new_err(
                "batch_size must be between 1 and 4096",
            ));
        }
        let resolved = self.resolve(location)?;
        Ok(PyObjectMetaIterator {
            context: self.context.child(),
            base: resolved.object_store_url().clone(),
            batch_size,
            stream: Mutex::new(Some(resolved.store().list(Some(resolved.prefix())))),
        })
    }
    fn iter_bytes(
        &self,
        py: Python<'_>,
        location: &str,
        chunk_size: usize,
    ) -> PyResult<PyByteIterator> {
        if chunk_size == 0 || chunk_size > 64 * 1024 * 1024 {
            return Err(PyValueError::new_err(
                "chunk_size must be between 1 and 64 MiB",
            ));
        }
        let resolved = self.resolve(location)?;
        let stream = self.context.run(py, async {
            Ok(resolved
                .store()
                .get(resolved.prefix())
                .await
                .map_err(storage_error)?
                .into_stream())
        })?;
        Ok(PyByteIterator {
            context: self.context.child(),
            chunk_size,
            state: Mutex::new(Some((stream, Bytes::new()))),
        })
    }
}
#[pyclass]
struct PyObjectMetaIterator {
    context: PythonObjectStoreContext,
    base: ObjectStoreUrl,
    batch_size: usize,
    stream: Mutex<Option<BoxStream<'static, object_store::Result<ObjectMeta>>>>,
}
#[pymethods]
impl PyObjectMetaIterator {
    fn next_batch(&self, py: Python<'_>) -> PyResult<Vec<PythonObjectMeta>> {
        // No synchronous mutex is held across I/O.
        let mut stream = self
            .stream
            .lock()
            .map_err(|_| PyRuntimeError::new_err("listing lock poisoned"))?
            .take()
            .ok_or_else(|| {
                PyRuntimeError::new_err("listing is closed or already being consumed")
            })?;
        let batch = self.context.run(py, async {
            let mut batch = Vec::new();
            while batch.len() < self.batch_size {
                let Some(meta) = stream.try_next().await.map_err(storage_error)? else {
                    break;
                };
                batch.push(metadata(&self.base, meta)?);
            }
            Ok(batch)
        })?;
        let mut state = self
            .stream
            .lock()
            .map_err(|_| PyRuntimeError::new_err("listing lock poisoned"))?;
        if !batch.is_empty() && !self.context.canceled.is_cancelled() {
            *state = Some(stream);
        }
        Ok(batch)
    }
    fn close(&self) {
        self.context.canceled.cancel();
        if let Ok(mut stream) = self.stream.lock() {
            *stream = None;
        }
    }
}
type ByteStreamState = (BoxStream<'static, object_store::Result<Bytes>>, Bytes);
#[pyclass]
struct PyByteIterator {
    context: PythonObjectStoreContext,
    chunk_size: usize,
    state: Mutex<Option<ByteStreamState>>,
}
#[pymethods]
impl PyByteIterator {
    fn next_chunk(&self, py: Python<'_>) -> PyResult<Option<Py<PyBytes>>> {
        let (mut stream, mut pending) = self
            .state
            .lock()
            .map_err(|_| PyRuntimeError::new_err("reader lock poisoned"))?
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("reader is closed or already being consumed"))?;
        let chunk = self.context.run(py, async {
            while pending.is_empty() {
                match stream.next().await {
                    Some(chunk) => pending = chunk.map_err(storage_error)?,
                    None => return Ok(None),
                }
            }
            Ok(Some(pending.split_to(pending.len().min(self.chunk_size))))
        })?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| PyRuntimeError::new_err("reader lock poisoned"))?;
        if chunk.is_some() && !self.context.canceled.is_cancelled() {
            *state = Some((stream, pending));
        }
        Ok(chunk.map(|chunk| PyBytes::new(py, &chunk).unbind()))
    }
    fn close(&self) {
        self.context.canceled.cancel();
        if let Ok(mut state) = self.state.lock() {
            *state = None;
        }
    }
}

/// Restores the outer callback and closes this callback's resources.
pub(crate) struct PythonObjectStoreGuard {
    previous: Option<Py<PyAny>>,
}
impl Drop for PythonObjectStoreGuard {
    fn drop(&mut self) {
        let Some(previous) = self.previous.take() else {
            return;
        };
        Python::attach(|py| {
            let result = py
                .import(PYTHON_OBJECT_STORE_MODULE)
                .and_then(|module| module.call_method1("_reset_current", (previous,)));
            if let Err(error) = result {
                log::warn!("Failed to reset Python storage context: {error}");
            }
        });
    }
}
pub(crate) fn install_object_store_context(
    py: Python<'_>,
    context: Option<&PythonObjectStoreContext>,
) -> Result<PythonObjectStoreGuard> {
    let Some(context) = context else {
        return Ok(PythonObjectStoreGuard { previous: None });
    };
    let module = py.import(PYTHON_OBJECT_STORE_MODULE).map_err(py_err)?;
    let store = Py::new(
        py,
        PySailObjectStore {
            context: context.child(),
        },
    )
    .map_err(py_err)?;
    let previous = module
        .call_method1("_set_current", (store,))
        .map_err(py_err)?
        .unbind();
    Ok(PythonObjectStoreGuard {
        previous: Some(previous),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use object_store::memory::InMemory;
    use object_store::path::Path;
    use tokio::sync::Notify;
    use url::Url;

    use super::*;

    async fn context(payload: &'static [u8]) -> Result<PythonObjectStoreContext> {
        let runtime = Arc::new(RuntimeEnv::default());
        let store = Arc::new(InMemory::new());
        store
            .put(&Path::from("data"), Bytes::from_static(payload).into())
            .await?;
        runtime.register_object_store(
            &Url::parse("memory:///").map_err(|e| DataFusionError::External(Box::new(e)))?,
            store,
        );
        PythonObjectStoreContext::try_new(runtime, &ConfigOptions::default())
    }
    #[tokio::test]
    async fn glob_reuses_native_discovery_and_preserves_literal_reads() -> Result<()> {
        Python::initialize();
        let mut context = context(b"unused").await?;
        context
            .session_config
            .options_mut()
            .execution
            .listing_table_ignore_subdirectory = false;
        let store = context
            .runtime_env
            .object_store(ObjectStoreUrl::parse("memory:///")?)?;
        for key in [
            "files/part-1.txt",
            "files/part-2.json",
            "files/other.csv",
            "files/.hidden.txt",
            "files/_hidden.txt",
            "files/_temporary/part-3.txt",
            "files/nested/part-4.txt",
            "files/literal*.txt",
            "files/hash#%.txt",
        ] {
            store
                .put(&Path::parse(key)?, Bytes::from_static(b"value").into())
                .await?;
        }
        let mut shallow = context.child();
        shallow
            .session_config
            .options_mut()
            .execution
            .listing_table_ignore_subdirectory = true;
        tokio::task::spawn_blocking(move || {
            Python::attach(|py| -> PyResult<()> {
                let store = PySailObjectStore { context };
                let files = store.glob(py, "memory:///files/part-?.{txt,json,txt}", 2)?;
                assert_eq!(
                    files.iter().map(|m| m.0.as_str()).collect::<Vec<_>>(),
                    vec!["memory:///files/part-1.txt", "memory:///files/part-2.json"]
                );
                assert!(
                    store
                        .glob(py, "memory:///files/part-?.*", 1)
                        .is_err_and(|e| e.is_instance_of::<PyValueError>(py))
                );
                assert!(
                    store
                        .glob(py, "memory:///files/missing-*.txt", 0)?
                        .is_empty()
                );
                assert!(
                    store
                        .glob(py, "memory:///files/[", 10)
                        .is_err_and(|e| e.is_instance_of::<PyValueError>(py))
                );
                let nested = store.glob(py, "memory:///files/*/part-?.txt", 10)?;
                assert_eq!(nested.len(), 1);
                assert_eq!(nested[0].0, "memory:///files/nested/part-4.txt");
                let shallow = PySailObjectStore { context: shallow };
                assert!(
                    shallow
                        .glob(py, "memory:///files/*/part-?.txt", 10)?
                        .is_empty()
                );
                let files = store.glob(py, "memory:///files", 10)?;
                assert!(
                    files
                        .iter()
                        .all(|m| !m.0.contains("hidden") && !m.0.contains("temporary"))
                );
                assert!(
                    files
                        .iter()
                        .any(|m| m.0 == "memory:///files/hash%23%25.txt"),
                    "{files:?}"
                );
                for meta in files {
                    assert_eq!(store.read(py, &meta.0, 10)?.bind(py).as_bytes(), b"value");
                }
                assert_eq!(
                    store
                        .read(py, "memory:///files/literal*.txt", 10)?
                        .bind(py)
                        .as_bytes(),
                    b"value"
                );
                store.close();
                assert!(
                    store
                        .glob(py, "memory:///files/*", 10)
                        .is_err_and(|e| e.is_instance_of::<PyInterruptedError>(py))
                );
                Ok(())
            })
        })
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .map_err(py_err)
    }
    #[tokio::test]
    async fn native_reads_use_bound_registry_and_enforce_limits() -> Result<()> {
        Python::initialize();
        let a = context(b"first").await?;
        let b = context(b"second").await?;
        tokio::task::spawn_blocking(move || {
            Python::attach(|py| -> PyResult<()> {
                let a = PySailObjectStore { context: a };
                let b = PySailObjectStore { context: b };
                assert_eq!(
                    a.read(py, "memory:///data", 16)?.bind(py).as_bytes(),
                    b"first"
                );
                assert_eq!(
                    b.read(py, "memory:///data", 16)?.bind(py).as_bytes(),
                    b"second"
                );
                assert!(a.read(py, "memory:///data", 4).is_err());
                assert!(
                    a.read_ranges(py, "memory:///data", vec![(0, 3), (0, 3)], 5, 2)
                        .is_err()
                );
                assert!(
                    a.read_ranges(py, "memory:///data", vec![(0, 1), (1, 2)], 8, 1)
                        .is_err()
                );
                assert!(a.read_range(py, "memory:///data", 3, 2, 8).is_err());
                assert_eq!(
                    a.read_range(py, "memory:///data", 2, 2, 0)?
                        .bind(py)
                        .as_bytes(),
                    b""
                );
                let error = a
                    .head(py, "memory:///missing")
                    .err()
                    .ok_or_else(|| PyRuntimeError::new_err("expected NotFound"))?;
                assert!(error.is_instance_of::<PyFileNotFoundError>(py));
                let error = storage_error(object_store::Error::PermissionDenied {
                    path: "data".into(),
                    source: "denied".into(),
                });
                assert!(error.is_instance_of::<PyPermissionError>(py));
                a.close();
                assert!(a.read(py, "memory:///data", 16).is_err());
                assert!(b.read(py, "memory:///data", 16).is_ok());
                Ok(())
            })
        })
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .map_err(py_err)
    }
    #[tokio::test]
    async fn listing_pulls_only_one_batch_and_byte_chunks_are_bounded() -> Result<()> {
        Python::initialize();
        let context = context(b"unused").await?;
        let pulled = Arc::new(AtomicUsize::new(0));
        let count = pulled.clone();
        let stream = futures::stream::iter(0..1_000_000)
            .map(move |i| {
                count.fetch_add(1, Ordering::Relaxed);
                Ok(ObjectMeta {
                    location: Path::from(format!("data/{i}")),
                    size: 1,
                    last_modified: chrono::Utc::now(),
                    e_tag: None,
                    version: None,
                })
            })
            .boxed();
        tokio::task::spawn_blocking(move || {
            Python::attach(|py| -> PyResult<()> {
                let listing = PyObjectMetaIterator {
                    context: context.child(),
                    base: ObjectStoreUrl::parse("memory:///")
                        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?,
                    batch_size: 7,
                    stream: Mutex::new(Some(stream)),
                };
                assert_eq!(listing.next_batch(py)?.len(), 7);
                assert_eq!(pulled.load(Ordering::Relaxed), 7);
                listing.close();
                assert!(listing.next_batch(py).is_err());
                assert_eq!(pulled.load(Ordering::Relaxed), 7);
                let stream = futures::stream::iter([Ok(Bytes::from_static(b"abcdefghij"))]).boxed();
                let reader = PyByteIterator {
                    context,
                    chunk_size: 3,
                    state: Mutex::new(Some((stream, Bytes::new()))),
                };
                let mut output = Vec::new();
                while let Some(chunk) = reader.next_chunk(py)? {
                    assert!(chunk.bind(py).as_bytes().len() <= 3);
                    output.extend_from_slice(chunk.bind(py).as_bytes());
                }
                assert_eq!(output, b"abcdefghij");
                Ok(())
            })
        })
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .map_err(py_err)
    }
    #[tokio::test]
    async fn eager_limit_checks_actual_response_bytes() {
        Python::initialize();
        let pulled = Arc::new(AtomicUsize::new(0));
        let count = pulled.clone();
        let stream = futures::stream::iter(0..1000)
            .map(move |_| {
                count.fetch_add(1, Ordering::Relaxed);
                Ok(Bytes::from_static(b"abcd"))
            })
            .boxed();
        assert!(collect_bounded(stream, 7).await.is_err());
        assert_eq!(pulled.load(Ordering::Relaxed), 2);
    }
    struct NotifyOnDrop(Arc<Notify>);
    impl Drop for NotifyOnDrop {
        fn drop(&mut self) {
            self.0.notify_one();
        }
    }
    #[tokio::test]
    async fn cancellation_drops_pending_io_and_returns_to_python() -> Result<()> {
        Python::initialize();
        let context = context(b"unused").await?;
        let token = context.canceled.clone();
        let started = Arc::new(Notify::new());
        let dropped = Arc::new(Notify::new());
        let start = started.clone();
        let drop = dropped.clone();
        let task = tokio::task::spawn_blocking(move || {
            Python::attach(|py| {
                let error = context
                    .run(py, async move {
                        let _guard = NotifyOnDrop(drop);
                        start.notify_one();
                        futures::future::pending::<PyResult<()>>().await
                    })
                    .err();
                assert!(error.is_some_and(|e| e.is_instance_of::<PyInterruptedError>(py)));
            })
        });
        tokio::time::timeout(Duration::from_secs(5), started.notified())
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        token.cancel();
        tokio::time::timeout(Duration::from_secs(5), dropped.notified())
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        task.await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(())
    }
}
