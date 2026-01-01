# Python DataSource RFC - Validation Checklist

**RFC:** [Python DataSource Support](./python-datasource-rfc-v2.md)
**Status:** Pre-Comment Review - Claims Requiring Validation
**Purpose:** This document enumerates all claims in the RFC that require thorough investigation before the RFC is opened for public comments.
**Total Claims Requiring Validation:** 42

---

## Table of Contents

1. [Performance Claims](#1-performance-claims-high-priority)
2. [PySpark Compatibility Claims](#2-pyspark-compatibility-claims-critical)
3. [Library Ecosystem Claims](#3-library-ecosystem-claims-medium-priority)
4. [Implementation Scope Claims](#4-implementation-scope-claims-medium-priority)
5. [Architecture Claims](#5-architecture-claims-high-priority)
6. [Security Claims](#6-security-claims-medium-priority)
7. [Risk Assessment Claims](#7-risk-assessment-claims-low-priority)
8. [Free Threading Timeline Claims](#8-free-threading-timeline-claims-low-priority)
9. [Error Message Parity Claims](#9-error-message-parity-claims-medium-priority)
10. [Subprocess Isolation Architecture Claims](#10-subprocess-isolation-architecture-claims-medium-priority)

---

## Summary by Priority

| Priority | Category | Count | Status |
|----------|----------|-------|--------|
| **CRITICAL** | PySpark Compatibility | 8 | 🔴 Not Started |
| **HIGH** | Performance | 6 | 🟡 In Progress |
| **HIGH** | Architecture | 5 | 🔴 Not Started |
| **MEDIUM** | Library Ecosystem | 5 | 🔴 Not Started |
| **MEDIUM** | Implementation Scope | 4 | 🔴 Not Started |
| **MEDIUM** | Security | 3 | 🔴 Not Started |
| **MEDIUM** | Error Message Parity | 1 | 🔴 Not Started |
| **MEDIUM** | Subprocess Architecture | 4 | 🔴 Not Started |
| **LOW** | Risk Assessment | 4 | 🔴 Not Started |
| **LOW** | Free Threading | 2 | 🔴 Not Started |

**Legend:**
- 🔴 Not Started
- 🟡 In Progress
- 🟢 Complete
- ⚠️ Blocked
- ❌ Failed Validation

---

## 1. Performance Claims (HIGH PRIORITY)

### 1.1 Overhead Analysis Numbers

**RFC Location:** Lines 739-743, 1295-1299

| Claim | Source | Validation Status | Evidence Required |
|-------|--------|-------------------|-------------------|
| "Control plane: ~10-50ms per query" | Line 739 | 🔴 Not Started | [ ] Benchmark with real DataSource |
| "Data plane: Zero-copy, sub-microsecond overhead" | Line 740 | 🔴 Not Started | [ ] Measure actual Arrow transfer latency |
| "Schema discovery: ~10-50ms" | Line 1296 | 🔴 Not Started | [ ] Compare to native Parquet (~1ms baseline) |
| "Partition enumeration: ~10-50ms" | Line 1297 | 🔴 Not Started | [ ] Measure reader.partitions() call overhead |
| "Batch read (Arrow): Zero-copy" | Line 1298 | 🔴 Not Started | [ ] Verify no data copying occurs |
| "Batch read (tuples): ~100μs per 8K rows" | Line 1299 | 🔴 Not Started | [ ] Actual measurement with tuple-yielding reader |

**Validation Steps:**
1. Create microbenchmark DataSource that yields data
2. Measure time from query start to first RecordBatch
3. Profile GIL acquisition/release overhead
4. Compare against native Parquet datasource
5. Document results in RFC or link to benchmark repo

**Acceptance Criteria:**
- [ ] Overhead measurements documented with methodology
- [ ] Comparison to native formats provided
- [ ] Worst-case (GIL contention) scenario tested
- [ ] Results reproducible via documented benchmark suite

---

### 1.2 Database Connector Performance Claims

**RFC Location:** Lines 1752-1759

| Claim | Validation Status | Evidence Required |
|-------|-------------------|-------------------|
| "1M rows fetch: ~5s (GIL-bound) vs ~0.5s (GIL-free)" | 🔴 Not Started | [ ] Run benchmark with PostgreSQL/MySQL |
| "10x faster" with connector-x | 🔴 Not Started | [ ] Test connector-x in Sail environment |
| "Arrow conversion: Required vs Not needed" | 🔴 Not Started | [ ] Verify connector-x returns Arrow directly |
| "Memory overhead: 2x vs 1x" | 🔴 Not Started | [ ] Memory profiling with heap profiler |
| "Parallel partitions: GIL contention vs Minimal" | 🔴 Not Started | [ ] Concurrent partition read test with 8+ partitions |

**Validation Steps:**
1. Install connector-x and DuckDB in test environment
2. Create test database with 1M+ rows
3. Implement DataSource using connector-x
4. Measure: wall time, memory usage, CPU utilization
5. Compare pure Python (psycopg3) vs connector-x
6. Test with 1, 4, 8, 16 parallel partitions

**Acceptance Criteria:**
- [ ] Benchmark numbers match RFC claims within 2x factor
- [ ] If numbers don't match, update RFC with actual measurements
- [ ] Document test environment (hardware, database size, network)
- [ ] Include both single-partition and multi-partition results

---

### 1.3 GIL Contention Mitigation Claims

**RFC Location:** Lines 246-256, 778-780, 1045-1051

| Claim | Validation Status | Evidence Required |
|-------|-------------------|-------------------|
| "Arrow/NumPy release GIL during compute" | 🔴 Not Started | [ ] Profile with Python GIL profiler |
| "Most data operations use Arrow/NumPy" | 🟡 In Progress | [ ] Survey common DataSource libraries |
| "Minimal Python in hot path" | 🔴 Not Started | [ ] Trace actual execution path |
| "Subprocess isolation eliminates GIL limitation" | 🔴 Not Started | [ ] Prototype and measure subprocess mode |

**Validation Steps:**
1. Instrument Python with GIL profiling (`sys._get_frame()` or specialized profiler)
2. Create test DataSource with varying GIL hold times
3. Measure throughput with 1, 2, 4, 8 parallel partitions
4. Plot throughput vs partitions to show contention curve
5. Repeat with NumPy-heavy operations (should show better scaling)

**Acceptance Criteria:**
- [ ] GIL contention demonstrated with measurements
- [ ] Mitigation strategies tested and quantified
- [ ] Clear guidance on when to use subprocess mode

---

## 2. PySpark Compatibility Claims (CRITICAL)

### 2.1 API Compatibility Assertion

**RFC Location:** Lines 11, 224

**Claim:** "100% API compatibility with PySpark 3.5+/4.0+ DataSource API"

| Validation Aspect | Status | Evidence Required |
|-------------------|--------|-------------------|
| DataSource API methods match | 🔴 Not Started | [ ] Side-by-side API comparison |
| DataSourceReader API methods match | 🔴 Not Started | [ ] Side-by-side API comparison |
| Filter classes match | 🔴 Not Started | [ ] Verify all filter types |
| InputPartition behavior matches | 🔴 Not Started | [ ] Test pickling/unpickling |
| Optional methods work correctly | 🔴 Not Started | [ ] Test pushFilters default behavior |

**Validation Steps:**
1. Extract PySpark DataSource API from PySpark 3.5.0, 4.0.0 source
2. Create comparison table: PySpark vs Sail APIs
3. For each API surface, create test case
4. Document any deviations (even minor ones)
5. Test with real PySpark DataSource implementations

**Acceptance Criteria:**
- [ ] All required methods present in Sail API
- [ ] All optional methods present or explicitly documented as unsupported
- [ ] Method signatures match (parameter names, types, defaults)
- [ ] Return types match
- [ ] Error behaviors match

---

### 2.2 sys.modules Shim Implementation

**RFC Location:** Lines 119-154, 709-728

**Claim:** "Zero-code-change compatibility via sys.modules shim"

| Validation Aspect | Status | Evidence Required |
|-------------------|--------|-------------------|
| Shim code works | 🔴 Not Started | [ ] Verify code compiles and runs |
| Unpickling PySpark classes succeeds | 🔴 Not Started | [ ] Test with pickled PySpark DataSource |
| Works with PySpark 3.5.x | 🔴 Not Started | [ ] Test with PySpark 3.5.0, 3.5.1, 3.5.2 |
| Works with PySpark 3.6.x | 🔴 Not Started | [ ] Test with PySpark 3.6.0+ |
| Works with PySpark 4.0.x | 🔴 Not Started | [ ] Test with PySpark 4.0.0+ |
| Handles nested classes | 🔴 Not Started | [ ] Test DataSource with inner classes |
| Handles lambdas/closures | 🔴 Not Started | [ ] Test DataSource with lambda in options |

**Critical Risk:** If cloudpickle serializes absolute module paths (e.g., `pyspark.sql.datasource:DataSource`), the shim may not work because unpickling will try to import from `pyspark` which doesn't exist in Sail environment.

**Validation Steps:**
1. Implement shim as described in RFC
2. Create test DataSource using PySpark base class
3. Pickle with cloudpickle on PySpark client
4. Unpickle in Sail environment
5. Test all edge cases above
6. If any fail, document limitation or fix shim

**Acceptance Criteria:**
- [ ] All test cases pass
- [ ] Or: Clear documentation of limitations
- [ ] Shim initialization happens before any DataSource unpickling
- [ ] Shim persists for entire Sail session

---

### 2.3 Version Support Matrix

**RFC Location:** Lines 773-774

**Claim:** "PySpark 3.5+, 4.0+; Python 3.9, 3.10, 3.11, 3.12"

| Version Combo | Status | Test Date | Result |
|---------------|--------|-----------|--------|
| PySpark 3.5.0 + Python 3.9 | 🔴 Not Started | - | - |
| PySpark 3.5.0 + Python 3.10 | 🔴 Not Started | - | - |
| PySpark 3.5.0 + Python 3.11 | 🔴 Not Started | - | - |
| PySpark 3.5.0 + Python 3.12 | 🔴 Not Started | - | - |
| PySpark 3.6.0 + Python 3.9 | 🔴 Not Started | - | - |
| PySpark 3.6.0 + Python 3.10 | 🔴 Not Started | - | - |
| PySpark 3.6.0 + Python 3.11 | 🔴 Not Started | - | - |
| PySpark 3.6.0 + Python 3.12 | 🔴 Not Started | - | - |
| PySpark 4.0.0 + Python 3.9 | 🔴 Not Started | - | - |
| PySpark 4.0.0 + Python 3.10 | 🔴 Not Started | - | - |
| PySpark 4.0.0 + Python 3.11 | 🔴 Not Started | - | - |
| PySpark 4.0.0 + Python 3.12 | 🔴 Not Started | - | - |

**Validation Steps:**
1. Set up test matrix: 3 PySpark versions × 4 Python versions = 12 combinations
2. For each combo: create environment, run compatibility tests
3. Document any version-specific issues
4. Check PySpark changelog for API changes between 3.5 and 4.0

**Acceptance Criteria:**
- [ ] All 12 combinations tested
- [ ] Any failures documented with root cause
- [ ] Minimum supported versions clearly documented
- [ ] Known issues section added if any combos fail

---

### 2.4 Error Message Parity

**RFC Location:** Lines 272-280

**Claim:** "Application-level errors MUST match PySpark behavior 1:1"

| Error Scenario | PySpark Message | Sail Message | Status |
|----------------|-----------------|--------------|--------|
| Column not found | (document) | (test) | 🔴 Not Started |
| Invalid date format | (document) | (test) | 🔴 Not Started |
| Type mismatch | (document) | (test) | 🔴 Not Started |
| Connection failure | (document) | (test) | 🔴 Not Started |
| Permission denied | (document) | (test) | 🔴 Not Started |
| Invalid partition | (document) | (test) | 🔴 Not Started |
| Schema mismatch | (document) | (test) | 🔴 Not Started |

**Validation Steps:**
1. Document PySpark error messages for each scenario
2. Create test DataSource that triggers each error
3. Capture Sail error messages
4. Compare: exact string match or semantic equivalence?
5. Document gaps

**Acceptance Criteria:**
- [ ] All common error scenarios tested
- [ ] Parity criteria defined (exact match or semantic)
- [ ] Any deviations documented with justification
- [ ] Or: RFC updated to reflect actual parity level

---

## 3. Library Ecosystem Claims (MEDIUM PRIORITY)

### 3.1 GIL Release Behavior - connector-x

**RFC Location:** Lines 817-826, 828-896

**Claim:** "connector-x releases GIL during query (Rust backend)"

| Validation Aspect | Status | Evidence Required |
|-------------------|--------|-------------------|
| Rust backend releases GIL | 🔴 Not Started | [ ] Check connector-x source code |
| Arrow output is zero-copy | 🔴 Not Started | [ ] Verify memory layout |
| Native Arrow output works | 🔴 Not Started | [ ] Test in Sail environment |
| Partitioning works | 🔴 Not Started | [ ] Test partition_on parameter |

**Validation Steps:**
1. Inspect connector-x Rust source for GIL release points
2. Profile with Python GIL tracer during query execution
3. Test example DataSource from RFC (lines 828-896)
4. Verify Arrow batches are zero-copy (check memory addresses)

**Acceptance Criteria:**
- [ ] GIL release confirmed via profiling
- [ ] Example code works as documented
- [ ] Zero-copy transfer verified

---

### 3.2 GIL Release Behavior - DuckDB

**RFC Location:** Lines 817-826, 900-939

**Claim:** "DuckDB releases GIL during query (C++ backend)"

| Validation Aspect | Status | Evidence Required |
|-------------------|--------|-------------------|
| C++ backend releases GIL | 🔴 Not Started | [ ] Check DuckDB Python bindings source |
| fetch_arrow_table() returns Arrow | 🔴 Not Started | [ ] Test in Sail environment |
| No conversion needed | 🔴 Not Started | [ ] Verify type mapping |
| Read-only mode works | 🔴 Not Started | [ ] Test with connection parameter |

**Validation Steps:**
1. Inspect DuckDB Python bindings for GIL management
2. Profile with Python GIL tracer
3. Test example DataSource from RFC (lines 900-939)
4. Verify Arrow batches integrate correctly

**Acceptance Criteria:**
- [ ] GIL release confirmed
- [ ] Example code works
- [ ] Zero-copy integration verified

---

### 3.3 NumPy/PyArrow/Polars GIL Behavior

**RFC Location:** Lines 823-825, 1089-1090

**Claim:** "NumPy.sum(), numpy.dot(), pyarrow.compute.*, polars.DataFrame.filter() release GIL"

| Library | Operation | Status | Evidence |
|---------|-----------|--------|----------|
| NumPy | sum() | 🔴 Not Started | [ ] Profile with GIL tracer |
| NumPy | dot() | 🔴 Not Started | [ ] Profile with GIL tracer |
| PyArrow | compute.* | 🔴 Not Started | [ ] Profile with GIL tracer |
| Polars | filter() | 🔴 Not Started | [ ] Profile with GIL tracer |

**Validation Steps:**
1. Create test script calling each operation
2. Profile with Python GIL tracer (e.g., `py-spy`)
3. Document GIL hold time for each operation
4. Identify operations that DON'T release GIL

**Acceptance Criteria:**
- [ ] Each operation profiled
- [ ] Results documented with GIL hold times
- [ ] Warning added for operations that hold GIL

---

### 3.4 Arrow Zero-Copy Transfer

**RFC Location:** Lines 1081-1086, 1089-1102

**Claim:** "RecordBatch::from_pyarrow_bound() provides zero-copy via Arrow C Data Interface"

| Validation Aspect | Status | Evidence Required |
|-------------------|--------|-------------------|
| Method exists in arrow-pyarrow | 🔴 Not Started | [ ] Check crate documentation |
| Zero-copy actually works | 🔴 Not Started | [ ] Memory address verification |
| Reference counting correct | 🔴 Not Started | [ ] Test for double-free or leaks |
| Works with large batches | 🔴 Not Started | [ ] Test with >1M rows |

**Critical Risk:** If PyArrow and Rust Arrow use different memory allocators (e.g., jemalloc vs mimalloc), zero-copy may not work even if C Data Interface is used.

**Validation Steps:**
1. Check arrow-pyarrow crate version and documentation
2. Create test: allocate PyArrow batch, convert to Rust, check memory addresses
3. Run with address sanitizer to detect memory issues
4. Test with various batch sizes (1K, 10K, 100K, 1M rows)
5. Run memory profiler to detect leaks

**Acceptance Criteria:**
- [ ] Memory addresses verified to be shared
- [ ] No memory leaks detected
- [ ] No crashes with sanitizers
- [ ] Large batch performance validates zero-copy (constant time regardless of size)

---

## 4. Implementation Scope Claims (MEDIUM PRIORITY)

### 4.1 Lines of Code Verification

**RFC Location:** Lines 656-680, 2044-2068

| Component | Claimed LOC | Actual LOC | Status | Verification Method |
|-----------|-------------|------------|--------|---------------------|
| **Rust - mod.rs** | 55 | - | 🔴 Not Started | [ ] Run `cloc` on file |
| **Rust - discovery.rs** | 331 | - | 🔴 Not Started | [ ] Run `cloc` on file |
| **Rust - filter.rs** | 457 | - | 🔴 Not Started | [ ] Run `cloc` on file |
| **Rust - arrow_utils.rs** | 342 | - | 🔴 Not Started | [ ] Run `cloc` on file |
| **Rust - executor.rs** | 191 | - | 🔴 Not Started | [ ] Run `cloc` on file |
| **Rust - python_datasource.rs** | 384 | - | 🔴 Not Started | [ ] Run `cloc` on file |
| **Rust - python_table_provider.rs** | 115 | - | 🔴 Not Started | [ ] Run `cloc` on file |
| **Rust - table_format.rs** | 213 | - | 🔴 Not Started | [ ] Run `cloc` on file |
| **Rust - exec.rs** | 197 | - | 🔴 Not Started | [ ] Run `cloc` on file |
| **Rust - stream.rs** | 289 | - | 🔴 Not Started | [ ] Run `cloc` on file |
| **Rust - error.rs** | 52 | - | 🔴 Not Started | [ ] Run `cloc` on file |
| **Rust Total** | ~2,626 | - | 🔴 Not Started | [ ] Run `cloc` on directory |
| **Python - base.py** | 444 | - | 🔴 Not Started | [ ] Run `cloc` on file |
| **Python - examples.py** | 379 | - | 🔴 Not Started | [ ] Run `cloc` on file |
| **Python Total** | ~843 | - | 🔴 Not Started | [ ] Run `cloc` on directory |

**Validation Steps:**
1. Install `cloc` (Count Lines of Code) tool
2. Run: `cloc crates/sail-data-source/src/python_datasource/`
3. Run: `cloc python/pysail/spark/datasource/`
4. Compare results to RFC claims
5. If significant deviation (>20%), update RFC

**Acceptance Criteria:**
- [ ] All files counted
- [ ] Total LOC within 20% of claim
- [ ] Or RFC updated with actual counts

---

### 4.2 Phase 1 Implementation Status

**RFC Location:** Lines 435-461

**Claim:** "Phase 1 (MVP) Complete - 9 components"

| Component | File Location | Status | Verified |
|-----------|---------------|--------|----------|
| Core Infrastructure | `python_datasource/` | 🔴 Not Started | [ ] Check files exist |
| Discovery & Registry | `discovery.rs` | 🔴 Not Started | [ ] Check file exists |
| Filter Pushdown | `filter.rs` | 🔴 Not Started | [ ] Check file exists |
| Arrow Utilities | `arrow_utils.rs` | 🔴 Not Started | [ ] Check file exists |
| Execution Plan | `exec.rs` | 🔴 Not Started | [ ] Check file exists |
| Stream with RAII | `stream.rs` | 🔴 Not Started | [ ] Check file exists |
| TableProvider | `python_table_provider.rs` | 🔴 Not Started | [ ] Check file exists |
| TableFormat | `table_format.rs` | 🔴 Not Started | [ ] Check file exists |
| Python API | `base.py` | 🔴 Not Started | [ ] Check file exists |
| Example DataSources | `examples.py` | 🔴 Not Started | [ ] Check file exists |

**Validation Steps:**
1. Check each file exists at claimed location
2. Verify file compiles (Rust) or imports (Python)
3. Run existing tests to confirm functionality
4. Update status table in RFC

**Acceptance Criteria:**
- [ ] All 9 components verified to exist
- [ ] All components functional
- [ ] Status table accurate

---

### 4.3 Future Phase LOC Estimates

**RFC Location:** Lines 1366-1377

| Phase | Claimed LOC | Breakdown Available | Validation Status |
|-------|-------------|---------------------|-------------------|
| **PR #2: Batch Write** | ~550 | No | 🔴 Not Started |
| **PR #3: Subprocess** | ~800 | No | 🔴 Not Started |
| **PR #4: Streaming Read** | ~650 | No | 🔴 Not Started |
| **PR #5: Streaming Write** | ~600 | No | 🔴 Not Started |
| **PR #6: Polish** | ~250 | No | 🔴 Not Started |
| **Total** | ~2,850 | Partial | 🔴 Not Started |

**Validation Steps:**
1. For each phase, create detailed breakdown:
   - New files needed
   - Lines per file (estimated)
   - Modifications to existing files
2. Compare to similar features in codebase (e.g., existing streaming sources)
3. Update RFC with detailed breakdowns

**Acceptance Criteria:**
- [ ] Each phase has detailed LOC breakdown
- [ ] Estimates are based on similar features
- [ ] Or estimates marked as "rough estimates"

---

## 5. Architecture Claims (HIGH PRIORITY)

### 5.1 DataFusion Trait Implementations

**RFC Location:** Lines 742-895

**Claim:** Rust code shows correct implementations of TableProvider, ExecutionPlan, RecordBatchStream

| Implementation | Status | Evidence Required |
|----------------|--------|-------------------|
| TableProvider trait | 🔴 Not Started | [ ] Compile check |
| ExecutionPlan trait | 🔴 Not Started | [ ] Compile check |
| RecordBatchStream trait | 🔴 Not Started | [ ] Compile check |
| Filter pushdown logic | 🔴 Not Started | [ ] Integration test |

**Critical Question:** Is the code in RFC (lines 742-895) actual implementation or illustrative pseudo-code?

**Validation Steps:**
1. If actual code: Compile in current codebase
2. If pseudo-code: Label as "illustrative" in RFC
3. Test with actual DataSource query
4. Verify DataFusion accepts implementations

**Acceptance Criteria:**
- [ ] Code compiles without errors
- [ ] Code is tested and working
- [ ] Or code is labeled as illustrative

---

### 5.2 Filter Pushdown Classification

**RFC Location:** Lines 798-801, 896-1014

| Classification | Meaning | Test Required | Status |
|----------------|---------|---------------|--------|
| **Exact** | Datasource fully handles filter; DataFusion skips | 🔴 Not Started | [ ] Verify DataFusion skips re-filter |
| **Inexact** | Datasource partially handles; DataFusion re-evaluates | 🔴 Not Started | [ ] Verify DataFusion re-checks |
| **Unsupported** | Datasource can't handle; DataFusion applies post-read | 🔴 Not Started | [ ] Verify post-read filtering |

**Current Status (RFC Line 998-1002):** Filter infrastructure exists but is NOT ACTIVE in MVP. All filters return `Unsupported`.

**Validation Steps:**
1. Create test DataSource that implements pushFilters
2. Test each classification path
3. Verify DataFusion behavior matches classification
4. Document in RFC which path is active in Phase 1

**Acceptance Criteria:**
- [ ] Each classification path tested
- [ ] DataFusion behavior verified
- [ ] RFC clearly states current status (infrastructure exists, not active)

---

### 5.3 PythonExecutor Trait Abstraction

**RFC Location:** Lines 592-609, 1038-1058

**Claim:** "Trait abstracts execution model, enables seamless switching between in-process and subprocess"

| Validation Aspect | Status | Evidence Required |
|-------------------|--------|-------------------|
| Trait methods sufficient for both modes | 🔴 Not Started | [ ] Review trait methods |
| InProcessExecutor implements trait | 🔴 Not Started | [ ] Compile check |
| RemoteExecutor can be implemented | 🔴 Not Started | [ ] Design review |
| Switching executors doesn't break queries | 🔴 Not Started | [ ] Integration test |

**Validation Steps:**
1. Review PythonExecutor trait methods:
   - `get_schema()`
   - `get_partitions()`
   - `execute_read()`
2. Confirm sufficient for both in-process and subprocess
3. Implement InProcessExecutor (if not already done)
4. Design RemoteExecutor interface
5. Test switching between executors

**Acceptance Criteria:**
- [ ] Trait is abstraction boundary, not implementation detail
- [ ] Both execution models can implement trait
- [ ] Switching mechanism documented

---

### 5.4 RAII Cleanup Guarantees

**RFC Location:** Lines 631-652, 1152-1158

**Claim:** "RAII cleanup ensures resources are freed even on errors"

| Validation Aspect | Status | Evidence Required |
|-------------------|--------|-------------------|
| Drop trait implemented | 🔴 Not Started | [ ] Code review |
| Cleanup happens on error path | 🔴 Not Started | [ ] Test with panic |
| Python thread always joined | 🔴 Not Started | [ ] Test with early return |
| No resource leaks | 🔴 Not Started | [ ] Long-running test with leak detector |

**Validation Steps:**
1. Review Drop implementation in PythonDataSourceStream
2. Create test that triggers error mid-stream
3. Verify cleanup still happens
4. Run with leak detector (Valgrind, Instruments)
5. Test with 1000s of queries to detect cumulative leaks

**Acceptance Criteria:**
- [ ] Drop trait correctly implemented
- [ ] Error paths tested
- [ ] No leaks detected in long-running tests

---

### 5.5 Discovery System Correctness

**RFC Location:** Lines 1017-1036

**Claim:** "Discovers DataSources via entry points and @register decorator"

| Validation Aspect | Status | Evidence Required |
|-------------------|--------|-------------------|
| Entry points discovery works | 🔴 Not Started | [ ] Test with installed package |
| @register decorator works | 🔴 Not Started | [ ] Test with manual import |
| Registry persists across sessions | 🔴 Not Started | [ ] Test session lifecycle |
| No duplicate registrations | 🔴 Not Started | [ ] Test duplicate handling |

**Validation Steps:**
1. Create test package with entry point
2. Install package, verify discovery
3. Test @register decorator
4. Test multiple Sail sessions
5. Test duplicate name handling

**Acceptance Criteria:**
- [ ] Both discovery mechanisms work
- [ ] Registry lifecycle correct
- [ ] Duplicate handling documented

---

## 6. Security Claims (MEDIUM PRIORITY)

### 6.1 Cloudpickle Trust Model

**RFC Location:** Lines 1313-1327, 1249-1257

**Claim:** "Same trust model as Spark - datasources are trusted code"

| Validation Aspect | Status | Evidence Required |
|-------------------|--------|-------------------|
| Trust model clearly defined | 🔴 Not Started | [ ] Document what "trusted" means |
| Comparison to Spark accurate | 🔴 Not Started | [ ] Review Spark security model |
| @register vs entry points security | 🔴 Not Started | [ ] Document threat model for each |
| Uninstalled package risks | 🔴 Not Started | [ ] Document attack scenarios |

**Ambiguity in RFC:** Section says "Only load from trusted packages" but @register works without package installation. Need to clarify threat model.

**Validation Steps:**
1. Define "trusted code" in Sail context
2. Compare to Spark's security model (document differences)
3. Analyze attack vectors:
   - Malicious DataSource in pip package
   - Malicious @register in imported script
   - Unpickling untrusted DataSource
4. Document which scenarios are secure/unsafe

**Acceptance Criteria:**
- [ ] Trust model clearly documented
- [ ] Threat model defined
- [ ] User guidance on safe DataSource usage
- [ ] Any mitigations implemented (e.g., validation)

---

### 6.2 Validation Code

**RFC Location:** Lines 1323-1327

**Claim:** "`validate_datasource_class()` checks required methods"

| Validation Aspect | Status | Evidence Required |
|-------------------|--------|-------------------|
| Function exists | 🔴 Not Started | [ ] Locate in codebase |
| Checks required methods | 🔴 Not Started | [ ] Code review |
| Prevents malicious classes | 🔴 Not Started | [ ] Test with exploit |
| Called before unpickling | 🔴 Not Started | [ ] Trace execution path |

**Validation Steps:**
1. Locate `validate_datasource_class()` in codebase
2. Review what it validates
3. Test against malicious classes (e.g., with `__reduce__` exploits)
4. Verify it's called in all code paths

**Acceptance Criteria:**
- [ ] Function exists and is called
- [ ] Validation checks are documented
- [ ] Limitations documented (can't prevent all attacks)

---

### 6.3 Subprocess Isolation Security

**RFC Location:** Lines 1492-1506

**Claim:** "Subprocess isolation provides security sandboxing for untrusted datasources"

| Validation Aspect | Status | Evidence Required |
|-------------------|--------|-------------------|
| Worker process isolation works | 🔴 Not Started | [ ] Prototype subprocess mode |
| Resource limits enforced | 🔴 Not Started | [ ] Test RLIMIT_AS |
| Crash isolation works | 🔴 Not Started | [ ] Test worker kill |
| No escape from sandbox | 🔴 Not Started | [ ] Security audit |

**Validation Steps:**
1. Prototype subprocess mode (Phase 3)
2. Test resource limits (memory, CPU, time)
3. Test crash isolation (kill worker, verify Sail continues)
4. Security audit: can DataSource escape to host?

**Acceptance Criteria:**
- [ ] Subprocess mode implemented
- [ ] Isolation properties tested
- [ ] Limitations documented

---

## 7. Risk Assessment Claims (LOW PRIORITY)

### 7.1 Risk Likelihood and Impact Ratings

**RFC Location:** Lines 424-434

| Risk | Claimed Likelihood | Claimed Impact | Basis for Rating | Status |
|------|-------------------|----------------|------------------|--------|
| **GIL becomes bottleneck** | Medium | High | (unspecified) | 🔴 Not Started |
| **Python crash kills Sail** | Low | Critical | (unspecified) | 🔴 Not Started |
| **Memory leak at boundary** | Low | Medium | (unspecified) | 🔴 Not Started |
| **Arrow version mismatch** | Medium | Medium | (unspecified) | 🔴 Not Started |
| **PySpark API divergence** | Low | High | (unspecified) | 🔴 Not Started |
| **Free threading makes subprocess obsolete** | Low | Medium | (unspecified) | 🔴 Not Started |

**Validation Steps for Each Risk:**
1. Document basis for likelihood rating (historical data, expert judgment, testing)
2. Document basis for impact rating (user impact, recovery difficulty)
3. Test where possible (e.g., memory leak detection)
4. Update RFC with rationale

**Acceptance Criteria:**
- [ ] Each risk has documented rationale
- [ ] Ratings can be defended in review
- [ ] Mitigations are tested

---

## 8. Free Threading Timeline Claims (LOW PRIORITY)

### 8.1 PEP 703 Timeline and Ecosystem Support

**RFC Location:** Lines 1010-1015, 1509-1520

**Claim:** "Free threading experimental in 3.13... ecosystem maturing... relevant in 2026+"

| Validation Aspect | Status | Evidence Required |
|-------------------|--------|-------------------|
| Python 3.13 release date accurate | 🟢 Complete | [ ] Verified: Oct 2024 |
| Free threading status in 3.13 | 🔴 Not Started | [ ] Check Python 3.13 release notes |
| PyO3 free threading support | 🔴 Not Started | [ ] Check PyO3 GitHub issues/roadmap |
| NumPy free threading support | 🔴 Not Started | [ ] Check NumPy GitHub |
| PyArrow free threading support | 🔴 Not Started | [ ] Check PyArrow GitHub |
| "Production-ready 2026+" prediction | 🔴 Not Started | [ ] Document basis for timeline |

**Validation Steps:**
1. Verify Python 3.13 release status
2. Search PyO3 repo for "free threading" or "nogil"
3. Search NumPy repo for "free threading" or "nogil"
4. Search PyArrow repo for "free threading" or "nogil"
5. Document current ecosystem status
6. Update RFC with citations

**Acceptance Criteria:**
- [ ] All ecosystem claims cited with sources
- [ ] Timeline prediction has documented rationale
- [ ] Or claim softened to "may be relevant 2026+"

---

### 8.2 Adaptive Executor Code

**RFC Location:** Lines 1525-1563

**Claim:** Code shows runtime detection of free threading

| Validation Aspect | Status | Evidence Required |
|-------------------|--------|-------------------|
| `sys._is_gil_enabled` exists in Python 3.13 | 🔴 Not Started | [ ] Check Python 3.13 docs |
| Code compiles | 🔴 Not Started | [ ] Compile check |
| Code works with --disable-gil build | 🔴 Not Started | [ ] Test with Python 3.13 nogil |
| Fallback works for older Python | 🔴 Not Started | [ ] Test with Python 3.11 |

**Validation Steps:**
1. Check Python 3.13 documentation for `sys._is_gil_enabled`
2. Compile code snippet
3. If Python 3.13 available, build with --disable-gil and test
4. Test fallback behavior on Python 3.9-3.12

**Acceptance Criteria:**
- [ ] Code is accurate (or labeled as pseudo-code)
- [ ] Works on supported Python versions
- [ ] Or labeled as "future implementation"

---

## 9. Error Message Parity Claims (MEDIUM PRIORITY)

**RFC Location:** Lines 272-280

**Claim:** "Application-level errors MUST match PySpark behavior 1:1 to minimize migration friction"

**Note:** This overlaps with Section 2.4, but from a design perspective rather than compatibility perspective.

| Design Decision | Status | Evidence Required |
|-----------------|--------|-------------------|
| "Application-level" defined | 🔴 Not Started | [ ] Document which errors are in scope |
| Parity criteria defined | 🔴 Not Started | [ ] Exact match or semantic? |
| Test suite exists | 🔴 Not Started | [ ] List of error scenarios |
| Current gap documented | 🔴 Not Started | [ ] Measure current state |

**Validation Steps:**
1. Define "application-level errors" (user-facing, not internal)
2. Define parity criteria (exact string, error type, or semantics)
3. Create test matrix of error scenarios
4. Test Sail vs PySpark for each scenario
5. Document gaps

**Acceptance Criteria:**
- [ ] Scope and criteria defined
- [ ] Test matrix created
- [ ] Current state measured
- [ ] Or RFC updated to reflect actual priority

---

## 10. Subprocess Isolation Architecture Claims (MEDIUM PRIORITY)

### 10.1 Protobuf Service Design

**RFC Location:** Lines 1381-1490

**Claim:** gRPC service definition with shared memory data plane

| Validation Aspect | Status | Evidence Required |
|-------------------|--------|-------------------|
| Protobuf compiles | 🔴 Not Started | [ ] Run protoc on .proto file |
| Rust generates client from .proto | 🔴 Not Started | [ ] Test with rust-grpc |
| Python generates server from .proto | 🔴 Not Started | [ ] Test with python-grpc |
| Message definitions sufficient | 🔴 Not Started | [ ] Design review |

**Validation Steps:**
1. Create .proto file with definitions from RFC
2. Run `protoc` to compile
3. Generate Rust code (tonic or gRPC-rs)
4. Generate Python code (grpcio)
5. Review message definitions for completeness

**Acceptance Criteria:**
- [ ] .proto definitions compile
- [ ] Generated code is usable
- [ ] Or design marked as "proposed, not implemented"

---

### 10.2 Shared Memory Data Plane

**RFC Location:** Lines 1442-1473

**Claim:** "Zero-copy across process boundary via mmap"

| Validation Aspect | Status | Evidence Required |
|-------------------|--------|-------------------|
| mmap works cross-platform | 🔴 Not Started | [ ] Test Linux/macOS/Windows |
| Arrow IPC compatible with mmap | 🔴 Not Started | [ ] Test Arrow IPC over mmap |
| No data copying | 🔴 Not Started | [ ] Verify memory addresses |
| Cleanup handles mmap correctly | 🔴 Not Started | [ ] Test for memory leaks |

**Validation Steps:**
1. Prototype mmap-based shared memory
2. Write Arrow IPC to mmap region
3. Read from other process
4. Verify zero-copy (memory addresses)
5. Test cleanup

**Acceptance Criteria:**
- [ ] Prototype works
- [ ] Zero-copy verified
- [ ] Or design marked as "proposed, needs prototyping"

---

### 10.3 Multi-Faceted Benefits Validation

**RFC Location:** Lines 1492-1506

| Benefit | Claimed Value | Test Required | Status |
|---------|---------------|---------------|--------|
| **GIL Parallelism** | N workers = N GILs | Benchmark with 8 workers | 🔴 Not Started |
| **Crash Isolation** | Worker crash only | Kill worker, verify Sail continues | 🔴 Not Started |
| **Memory Isolation** | Per-worker limits | Test RLIMIT_AS | 🔴 Not Started |
| **Version Isolation** | Different Python versions | Test Python 3.9 + 3.12 workers | 🔴 Not Started |
| **Security Sandboxing** | Untrusted code isolated | Security audit | 🔴 Not Started |

**Validation Steps:**
1. Prototype subprocess mode (Phase 3)
2. For each benefit: design test, execute, document results
3. Update RFC with actual measurements

**Acceptance Criteria:**
- [ ] Each benefit tested and validated
- [ ] Or marked as "claimed benefit, to be validated in Phase 3"

---

## Validation Progress Tracking

### Overall Progress

```
Total Claims: 42
Validated: 0 (0%)
In Progress: 2 (5%)
Not Started: 40 (95%)
```

### By Category

| Category | Complete | In Progress | Not Started | Total |
|----------|----------|-------------|-------------|-------|
| Performance | 0 | 0 | 17 | 17 |
| PySpark Compatibility | 0 | 0 | 8 | 8 |
| Library Ecosystem | 0 | 0 | 7 | 7 |
| Implementation Scope | 0 | 0 | 3 | 3 |
| Architecture | 0 | 0 | 5 | 5 |
| Security | 0 | 0 | 3 | 3 |
| Risk Assessment | 0 | 0 | 6 | 6 |
| Free Threading | 1 | 0 | 1 | 2 |
| Error Parity | 0 | 0 | 1 | 1 |
| Subprocess Architecture | 0 | 0 | 4 | 4 |

---

## Validation Phase Plan

### Phase 1: Critical Path (Before Opening for Comments)

**Goal:** Validate claims that would block RFC approval if false

| Claim | Priority | Owner | Target Date |
|-------|----------|-------|-------------|
| PySpark API compatibility | CRITICAL | - | - |
| sys.modules shim works | CRITICAL | - | - |
| Version support matrix | CRITICAL | - | - |
| Overhead numbers | HIGH | - | - |
| LOC verification | MEDIUM | - | - |
| Arrow zero-copy | HIGH | - | - |

**Exit Criteria:**
- [ ] All CRITICAL claims validated
- [ ] HIGH claims have initial measurements
- [ ] MEDIUM claims have validation plan

---

### Phase 2: Important (Should Complete Before Merge)

**Goal:** Validate claims that affect user experience or implementation quality

| Claim | Priority | Owner | Target Date |
|-------|----------|-------|-------------|
| Error message parity | MEDIUM | - | - |
| GIL release behavior | MEDIUM | - | - |
| Security model | MEDIUM | - | - |
| DataFusion integration | HIGH | - | - |

**Exit Criteria:**
- [ ] All user-facing claims validated
- [ ] Security model documented
- [ ] Code examples tested

---

### Phase 3: Nice to Have (Before Future Phases)

**Goal:** Validate claims for future phases or nice-to-have features

| Claim | Priority | Owner | Target Date |
|-------|----------|-------|-------------|
| Free threading ecosystem | LOW | - | - |
| Risk assessment ratings | LOW | - | - |
| Subprocess isolation | MEDIUM | - | - |

**Exit Criteria:**
- [ ] Future work claims documented
- [ ] Risk ratings have rationale

---

## Appendices

### A. Validation Tools and Techniques

**Performance Measurement:**
- `cloc` - Lines of code counting
- `py-spy` - Python GIL profiling
- `flamegraph` - Performance visualization
- Valgrind/Instruments - Memory leak detection
- `hyperfine` - Benchmarking

**Compatibility Testing:**
- `tox` - Multi-version Python testing
- Virtual environments for isolated testing
- Docker containers for clean environments

**Code Verification:**
- Compiler checks (rustc, mypy)
- Linters (clippy, pylint, ruff)
- Static analysis

---

### B. Test Data Sources

**For Performance Testing:**
- Synthetic data generation (range, constant sources)
- Real data sources (PostgreSQL, MySQL via connector-x)
- Large datasets (1M+ rows)

**For Compatibility Testing:**
- Real PySpark DataSource implementations
- Custom test DataSources covering edge cases

---

### C. Documentation Standards

**When documenting validation results:**

1. **Methodology:** How was the claim tested?
2. **Environment:** Hardware, OS, versions
3. **Results:** Actual measurements, pass/fail
4. **Artifacts:** Links to benchmark code, test suites
5. **Conclusion:** Does the claim hold? Any caveats?

---

## Change Log

| Date | Change | Author |
|------|--------|--------|
| 2025-12-31 | Initial document creation | - |
| - | - | - |

---

**Next Steps:**
1. Assign owners to each validation item
2. Set target dates for Phase 1 (Critical) validations
3. Begin validation work
4. Update this document as validations complete
5. Open RFC for comments when Phase 1 complete

---

**Document Status:** 🔴 Pre-Comment Review - Validation in Progress
**Last Updated:** 2025-12-31
