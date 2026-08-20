use std::cell::Cell;

use pyo3::{Python, ffi};

thread_local! {
    static THREAD_STATE_PINNED: Cell<bool> = const { Cell::new(false) };
}

/// Attaches to the interpreter and runs `f`, keeping this OS thread's
/// [`ffi::PyThreadState`] alive for the rest of the thread's life.
///
/// # Why this is needed
///
/// [`Python::attach`] uses `PyGILState_Ensure`/`PyGILState_Release`. On a
/// thread that is not otherwise attached, the outermost `PyGILState_Release`
/// **destroys** the thread state that `Ensure` created, so a runtime worker
/// thread observes a fresh, short-lived `PyThreadState` for every call into
/// Python.
///
/// That breaks Python libraries that key native resources by thread: values
/// stored in `threading.local` live on the thread state and are finalized when
/// it is destroyed, while companion pointers stored in OS-level thread storage
/// (`PyThread_tss_*`) survive with the OS thread. A library that frees a
/// native handle from a `threading.local` finalizer but caches the raw pointer
/// in thread storage (pyproj's per-thread `PJ_CONTEXT*` is one example) is left
/// with a dangling pointer that the *next* call on the same worker thread
/// dereferences — a use-after-free that surfaces as heap corruption far from
/// the cause.
///
/// Pinning the thread state gives Python code invoked by the engine the same
/// stable thread identity that a plain `threading.Thread` provides, which is
/// what those libraries assume. It also lets per-thread caches survive between
/// calls instead of being rebuilt on every invocation.
///
/// # Where to use it
///
/// Every entry point that may make the *first* Python call on a thread the
/// engine owns. Pinning is idempotent and costs one thread-local read after
/// the first call, so preferring it over [`Python::attach`] everywhere on
/// those paths is cheaper than auditing which call happens first.
///
/// The one place that should keep using [`Python::attach`] directly is code
/// that owns its thread and holds a single attachment for the whole life of
/// that thread, such as `PyMapStream`: the attachment already spans every call
/// and the thread state is destroyed at thread exit, which is exactly the
/// behavior this helper recreates for pooled threads.
///
/// # Lifetime and cost
///
/// The pinned thread state is deliberately never released, so it outlives not
/// only individual calls but the OS thread itself: when a pooled thread exits
/// (Tokio reaps idle `spawn_blocking` threads after an idle timeout), its
/// thread state stays registered with the interpreter until the interpreter
/// shuts down. The retained memory is the thread state plus whatever that
/// thread's `threading.local` values hold.
///
/// Unpinning from a thread-exit hook is possible — `PyGILState_Ensure`, then
/// `PyGILState_Release` with `PyGILState_LOCKED` and again with
/// `PyGILState_UNLOCKED` — but it would run arbitrary Python finalizers from a
/// Rust thread-local destructor, where a panic aborts the process, and it
/// races interpreter finalization when Sail is embedded in a host Python
/// process. Retaining the state is the safer trade.
pub fn attach_persistent<F, R>(f: F) -> R
where
    F: for<'py> FnOnce(Python<'py>) -> R,
{
    Python::attach(|py| {
        pin_thread_state(py);
        f(py)
    })
}

/// Takes one permanent reference to the current thread's Python thread state.
///
/// Running inside [`Python::attach`] rather than before it is deliberate: the
/// `py` token proves the interpreter is initialized and that this thread is
/// attached, so the raw `PyGILState_Ensure` below cannot be reached during
/// startup or interpreter finalization.
fn pin_thread_state(_py: Python<'_>) {
    THREAD_STATE_PINNED.with(|pinned| {
        if pinned.get() {
            return;
        }
        // SAFETY: `_py` guarantees the interpreter is initialized and that
        // this thread holds the GIL with its thread state current, which is
        // the precondition of `PyGILState_Ensure`. Because the thread is
        // already attached, the call cannot block or create a thread state; it
        // only increments the gilstate counter and returns `PyGILState_LOCKED`.
        //
        // The matching `PyGILState_Release` is intentionally never made. That
        // leaves the counter permanently at one or more, so the release that
        // ends the enclosing `Python::attach` detaches the thread and drops
        // the GIL without destroying the thread state. Later `Ensure` calls
        // find the same state in thread storage and reuse it.
        let _pin = unsafe { ffi::PyGILState_Ensure() };
        pinned.set(true);
    });
}

#[cfg(test)]
mod tests {
    use pyo3::types::PyAnyMethods;
    use pyo3::{Py, PyAny, Python};

    use super::attach_persistent;

    /// A value stored in a `threading.local` during one attachment must still
    /// be visible on the next attachment from the same thread.
    ///
    /// With a plain `Python::attach`, the thread state — and with it the
    /// thread's `threading.local` storage — is destroyed when the first
    /// attachment ends, so the second attachment sees nothing. See
    /// <https://github.com/lakehq/sail/issues/2456>.
    #[test]
    #[expect(clippy::unwrap_used)]
    fn thread_local_value_survives_between_attachments() {
        Python::initialize();

        let local: Py<PyAny> = Python::attach(|py| {
            py.import("threading")
                .unwrap()
                .getattr("local")
                .unwrap()
                .call0()
                .unwrap()
                .unbind()
        });

        // A thread of our own, so that the state under test is the one this
        // helper creates rather than one the test harness already pinned.
        let survived = std::thread::spawn(move || {
            attach_persistent(|py| local.bind(py).setattr("marker", 1).unwrap());
            attach_persistent(|py| local.bind(py).hasattr("marker").unwrap())
        })
        .join()
        .unwrap();

        assert!(
            survived,
            "the thread state was destroyed between attachments"
        );
    }
}
