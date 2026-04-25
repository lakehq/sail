# Arrow Columnar Format Expert

Expert in Apache Arrow for zero-copy, vectorized operations in Sail. Guides efficient array operations, SIMD utilization, and Python-Arrow integration.

## Context

Sail uses Apache Arrow (version 57.1.0) as its in-memory columnar format. Arrow provides:
- Zero-copy data sharing between Rust and Python
- SIMD-optimized operations
- Efficient memory layout for analytical workloads
- Language-independent format

## Key Components

### Arrow Arrays
Arrow arrays are the fundamental data structures:

```rust
use arrow::array::*;
use arrow::datatypes::*;

// Primitive arrays (contiguous memory)
// - Int8Array, Int16Array, Int32Array, Int64Array
// - UInt8Array, UInt16Array, UInt32Array, UInt64Array
// - Float32Array, Float64Array
// - BooleanArray

// Variable-length arrays
// - StringArray, LargeStringArray
// - BinaryArray, LargeBinaryArray
// - ListArray, LargeListArray
```

### RecordBatch
A RecordBatch is a table-like structure:

```rust
use arrow::record_batch::RecordBatch;

let batch = RecordBatch::try_new(
    schema,
    vec![Arc::new(array1), Arc::new(array2)]
)?;
```

### Schema
Defines the structure of RecordBatches:

```rust
use arrow::datatypes::{Schema, Field, DataType};

let schema = Schema::new(vec![
    Field::new("id", DataType::Int32, false),
    Field::new("name", DataType::Utf8, true),
]);
```

## Memory Layout

### Primitive Arrays
Contiguous memory with validity bitmap:

```
[Int32Array with values [1, null, 3]]
┌─────────────────────┬──────────────────┐
│ Validity Bitmap     │ Values           │
├─────────────────────┼──────────────────┤
│ 0b00000101          │ 0x01 0x00 0x03   │
│ (1, null, 3)        │ (little-endian)   │
└─────────────────────┴──────────────────┘
```

### String Arrays
Separate validity, offsets, and data:

```
[StringArray with values ["hello", null, "world"]]
┌─────────────────────┬─────────────────────┬───────────────────────┐
│ Validity Bitmap     │ Offsets             │ Data                  │
├─────────────────────┼─────────────────────┼───────────────────────┤
│ 0b00000101          │ 0, 5, 5, 10        │ "helloworld"          │
│                     │ (lengths: 5, 0, 5)  │                       │
└─────────────────────┴─────────────────────┴───────────────────────┘
```

### Dictionary Encoding
Compressed representation for repeated strings:

```rust
use arrow::array::{DictionaryArray, StringArray};
use arrow::datatypes::Int32Type;

type StringDictionary = DictionaryArray<Int32Type>;

// Compresses ["a", "b", "a", "c", "b"] to:
// - Keys: [0, 1, 0, 2, 1]
// - Values: ["a", "b", "c"]
```

## Zero-Copy with Python

### PyO3 + Arrow Integration
Sail uses PyO3 and PyArrow for zero-copy:

```rust
use pyo3::prelude::*;
use arrow::array::RecordBatch;

#[pyclass]
pub struct PyRecordBatch {
    batch: RecordBatch,
}

#[pymethods]
impl PyRecordBatch {
    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        // Zero-copy conversion to Python
        let pyarrow = py.import("pyarrow")?;
        let batch_c_ptr = // Get C ABI pointer
        pyarrow.call_method1("import_record_batch_from_c", (batch_c_ptr,))
    }
}
```

### Python UDFs with Arrow
Sail passes Arrow arrays to Python UDFs:

```python
import pyarrow

# Sail passes Arrow arrays directly to Python
def my_udf(batch: pyarrow.RecordBatch) -> pyarrow.RecordBatch:
    # Zero-copy access to data
    arr = batch.column("data")
    # Process with Arrow
    result = arr.to_pylist()
    # Return Arrow array
    return pyarrow.RecordBatch.from_arrays(...)

# Register with Sail
spark.udf.register("my_udf", my_udf)
```

### mapInArrow for Maximum Performance
Use Arrow UDFs to avoid serialization:

```python
from pyspark.sql.functions import mapInArrow

# Process entire batches with Arrow
@mapInArrow
def process_batch(batch):
    # batch is an Arrow RecordBatch
    # Yield Arrow arrays
    yield pyarrow.RecordBatch.from_arrays(...)

df.select(process_batch("col"))
```

## SIMD Operations

### Vectorized Operations
Arrow enables SIMD-optimized operations:

```rust
use arrow::compute::*;

// SIMD-accelerated operations
let result = add(&array1, &array2)?;           // Element-wise add
let result = filter(&array, &mask)?;            // Boolean mask
let result = sort(&array, None)?;               // Sort
let result = take(&array, &indices)?;           // Indexed access
let result = cast(&array, &DataType::Int64)?;   // Type cast
```

### SIMD Benefits
- Process multiple values per CPU instruction
- Cache-friendly memory access
- No branching overhead

## Common Patterns

### Array Construction
Build arrays efficiently:

```rust
use arrow::array::{Int32Array, StringBuilder};

// From Vec
let array = Int32Array::from(vec![1, 2, 3]);

// With nulls
let array = Int32Array::from(vec![
    Some(1),
    None,
    Some(3),
]);

// Using builder
let mut builder = Int32Builder::new();
builder.append_value(1);
builder.append_null();
builder.append_value(3);
let array = builder.finish();
```

### Array Iteration
Iterate over array values:

```rust
use arrow::array::Array;

// Iterate with Option for nulls
for i in 0..array.len() {
    match array.is_valid(i) {
        true => {
            let value = array.value(i);
            // Process value
        }
        false => {
            // Handle null
        }
    }
}
```

### Type Coercion
Convert between Arrow types:

```rust
use arrow::compute::cast;

let array = Int32Array::from(vec![1, 2, 3]);
let float_array = cast(&array, &DataType::Float64)?;
```

## Memory Management

### Memory Pools
Arrow uses memory pools for allocation:

```rust
use arrow::memory::MemoryPool;

// Track allocations
let pool = MemoryPool::new(&Default::default());

// Or use Arena for temporary allocations
use arrow::memory::Arena;
let arena = Arena::new();
let ptr = arena.alloc(1024);
// Automatically freed when arena drops
```

### Reference Counting
Arrow uses Arc for shared ownership:

```rust
use std::sync::Arc;

let array = Arc::new(Int32Array::from(vec![1, 2, 3]));
// Clone is cheap (just increments ref count)
let array_clone = Arc::clone(&array);
```

## Critical Files

| File | Purpose |
|------|---------|
| `crates/sail-python-udf/src/` | Python-Arrow bridge |
| `crates/sail-common-datafusion/src/expr/` | Expression handling |
| `python/pysail/src/lib.rs` | PyO3 bindings |

## Arrow Types

| Arrow Type | Rust Type | Description |
|------------|-----------|-------------|
| Boolean | BooleanArray | True/False values |
| Int8/16/32/64 | Int*Array | Signed integers |
| UInt8/16/32/64 | UInt*Array | Unsigned integers |
| Float32/64 | Float*Array | IEEE-754 floats |
| Utf8 | StringArray | Variable-length strings (32-bit offsets) |
| LargeUtf8 | LargeStringArray | Variable-length strings (64-bit offsets) |
| Binary | BinaryArray | Variable-length bytes (32-bit offsets) |
| LargeBinary | LargeBinaryArray | Variable-length bytes (64-bit offsets) |
| List | ListArray | Nested lists (32-bit offsets) |
| LargeList | LargeListArray | Nested lists (64-bit offsets) |
| Struct | StructArray | Struct/column-wise data |
| Dictionary | DictionaryArray | Dictionary-encoded values |

## Debugging Tips

### Inspect Array Contents
```rust
println!("{:?}", array);              // Debug format
println!("{}", array);                 // Pretty print
```

### Check Null Count
```rust
let null_count = array.null_count();
```

### Get Buffer Pointers
```rust
let values = array.values();           // Get underlying slice
let validity = array.validity();       // Get validity bitmap
```

## Performance Tips

1. **Use Large* variants** for >2GB data (LargeString, LargeBinary, LargeList)
2. **Prefer Dictionary encoding** for low-cardinality strings
3. **Batch operations** - process entire arrays, not row-by-row
4. **Avoid unnecessary conversions** - stay in Arrow format
5. **Use builders** for constructing arrays efficiently

## References

- Arrow docs: https://arrow.apache.org/docs/
- Arrow Rust: https://github.com/apache/arrow-rs
- PyArrow: https://arrow.apache.org/docs/python/
