---
name: sail-arrow
description: Expert guidance for Apache Arrow columnar format in Sail. Use when working with zero-copy data sharing, SIMD operations, Python-Arrow integration, or optimizing memory layout. Triggers include implementing Arrow UDFs with mapInArrow, understanding Arrow memory layout (validity bitmaps, offsets), debugging type conversions between Rust/Python, or optimizing array operations for vectorized execution.
---

# Arrow Columnar Format

Sail uses Apache Arrow 57.1.0 for zero-copy, SIMD-accelerated columnar operations.

## Quick Reference

| Concept | Description |
|---------|-------------|
| Array | Fundamental data structure (Int32Array, StringArray, etc.) |
| RecordBatch | Table-like structure with schema |
| Schema | Defines data types |
| Zero-Copy | Shared memory between Rust and Python via PyArrow |

## Common Patterns

### Array Construction

```rust
// From Vec
let array = Int32Array::from(vec![1, 2, 3]);

// With nulls
let array = Int32Array::from(vec![Some(1), None, Some(3)]);

// Using builder
let mut builder = Int32Builder::new();
builder.append_value(1);
builder.append_null();
let array = builder.finish();
```

### Zero-Copy Python Integration

```python
import pyarrow

# Sail passes Arrow arrays directly
def my_udf(batch: pyarrow.RecordBatch) -> pyarrow.RecordBatch:
    arr = batch.column("data")  # Zero-copy access
    result = process(arr)
    return pyarrow.RecordBatch.from_arrays(...)
```

### SIMD Operations

```rust
use arrow::compute::*;

let result = add(&array1, &array2)?;      // SIMD-accelerated
let filtered = filter(&array, &mask)?;
let sorted = sort(&array, None)?;
```

## Memory Layout

**Primitive arrays**: Contiguous memory + validity bitmap
```
[Int32Array: 1, null, 3]
Validity: 0b101 | Values: 0x01 0x00 0x03
```

**String arrays**: Validity + offsets + data
```
["hello", null, "world"]
Validity: 0b101 | Offsets: [0, 5, 5, 10] | Data: "helloworld"
```

## Python UDF Performance

| UDF Type | Performance | Use Case |
|----------|-------------|----------|
| Scalar UDF | Slow (row-by-row) | Simple transformations |
| Pandas UDF | Medium | Batch processing |
| Arrow UDF (mapInArrow) | Fast (zero-copy) | Large data processing |

## Resources

- **[arrow.md](references/arrow.md)**: Comprehensive Arrow guide with memory layout, SIMD operations, and Python integration
- **[datafusion.md](../sail-datafusion/references/datafusion.md)**: DataFusion integration (uses Arrow)
- **[tokio.md](../sail-tokio/references/tokio.md)**: Async data streaming with Arrow
