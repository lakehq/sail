# MREs (Minimal Reproducible Examples)

This folder contains small, self-contained reproducers for specific Sail bugs / risks.

## Python pre-init C-API calls (UB + leaks)

The Sail CLI has a code path that turns the binary into a Python interpreter (used by
Python `multiprocessing` child processes). The problematic pattern is calling Python C-API
functions to build `wchar_t** argv` before the interpreter is initialized, and not freeing
the resulting allocations.

### Reproducer

Run:

```bash
./scripts/mre/python-preinit-argv-ub.sh --iters 200000 --arg-bytes 4096 --print-every 10000
```

Expected:
- The program calls `PyUnicode_FromString` and `PyUnicode_AsWideCharString` **before**
  initializing Python.
- The program intentionally does **not** `Py_DECREF` the returned `PyObject*` and does **not**
  free the returned `wchar_t*`, so RSS should generally increase over time on Linux.

### Reference (production path)

This is the corresponding production code path:
- `/home/runner/work/sail/sail/crates/sail-cli/src/main.rs`

You can also exercise that path directly (it may not crash on all Python versions):

```bash
SAIL_INTERNAL__RUN_PYTHON=true cargo run -p sail-cli -- -c "import sys; print(sys.version)"
```

