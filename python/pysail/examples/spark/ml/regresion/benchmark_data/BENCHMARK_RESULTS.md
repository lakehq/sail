# LinearRegression Benchmark: Sail OLS vs Spark L-BFGS

**Date:** 2026-02-04
**Machine:** MacBook (12 cores)
**Mode:** Local single-node (both)

## Configuration

| Parameter | Sail | Spark JVM |
|-----------|------|-----------|
| Connection | `sc://localhost:50051` | `local` |
| Engine | DataFusion (Rust) | Spark 4.x (JVM) |
| Parallelism | 12 partitions | 12 partitions |
| Driver Memory | N/A | 16GB |
| Solver | OLS (closed-form) | L-BFGS (iterative) |

## Final Results (After Optimization)

| Dataset | Rows | Features | Sail Optimized | Spark L-BFGS | Speedup |
|---------|------|----------|----------------|--------------|---------|
| tiny | 10,000 | 20 | **0.07s** | 5.16s | **72.4x** |
| small | 100,000 | 50 | **0.82s** | 4.80s | **5.9x** |
| medium | 500,000 | 100 | **10.25s** | 19.04s | **1.9x** |
| large | 1,000,000 | 100 | **20.50s** | 35.42s | **1.7x** |
| large2 | 1,000,000 | 500 | 426.62s | **169.07s** | 0.4x (Spark wins) |
| xlarge | 2,000,000 | 100 | **40.29s** | 70.00s | **1.7x** |
| xxlarge | 5,000,000 | 100 | **99.23s** | 172.68s | **1.7x** |

**Sail wins for p≤100. Spark wins for p=500 (cache issue).**

## Optimization Impact (Sail Original vs Optimized)

| Dataset | Sail Original | Sail Optimized | Improvement |
|---------|---------------|----------------|-------------|
| tiny | 0.07s | 0.07s | - |
| small | 1.41s | 0.82s | **1.7x** |
| medium | 20.18s | 10.25s | **2.0x** |
| large | 39.17s | 20.50s | **1.9x** |
| xlarge | 78.53s | 40.29s | **1.9x** |
| xxlarge | 197.16s | 99.23s | **2.0x** |

**SIMD optimization achieved ~2x speedup on medium-to-large datasets.**

## Three-Way Comparison

| Dataset | Sail Original | Sail Optimized | Spark L-BFGS | Best |
|---------|---------------|----------------|--------------|------|
| tiny | 0.07s | **0.07s** | 5.16s | Sail |
| small | 1.41s | **0.82s** | 4.80s | Sail |
| medium | 20.18s | **10.25s** | 19.04s | Sail |
| large | 39.17s | **20.50s** | 35.42s | Sail |
| large2 (p=500) | - | 426.62s | **169.07s** | Spark |
| xlarge | 78.53s | **40.29s** | 70.00s | Sail |
| xxlarge | 197.16s | **99.23s** | 172.68s | Sail |

## Accuracy (Mean Absolute Error)

| Dataset | Sail OLS | Spark L-BFGS |
|---------|----------|--------------|
| tiny | 0.000782 | 0.000781 |
| small | 0.000236 | 0.000236 |
| medium | 0.000107 | 0.000107 |
| large | 0.000088 | 0.000088 |
| large2 | 0.000078 | 0.000078 |
| xlarge | 0.000051 | 0.000051 |
| xxlarge | 0.000035 | 0.000035 |

**Both solvers achieve identical accuracy.**

## Optimization Details

### Before: Naive Scalar Loops

```rust
// O(n * p²) scalar operations - no SIMD
for (i, &xi) in features.iter().enumerate().take(p) {
    for (j, &xj) in features.iter().enumerate().take(p) {
        self.xtx[i * p + j] += xi * xj;
    }
}
for (i, &xi) in features.iter().enumerate().take(p) {
    self.xty[i] += label * xi;
}
```

### After: SIMD-Friendly Iterator Patterns

```rust
#[inline]
fn update_one(&mut self, features: &[f64], label: f64) {
    self.initialize(features.len());
    let p = self.num_features;

    // Outer product: xtx += x * x^T (SIMD-vectorized inner loop)
    for i in 0..p {
        let xi = features[i];
        let row = &mut self.xtx[i * p..(i + 1) * p];
        row.iter_mut()
            .zip(features.iter())
            .for_each(|(xtx_ij, &xj)| *xtx_ij += xi * xj);
    }

    // axpy: xty += label * x (vectorized)
    self.xty
        .iter_mut()
        .zip(features.iter())
        .for_each(|(xty_i, &xi)| *xty_i += label * xi);

    self.count += 1;
}
```

**Key changes:**
1. Use `iter_mut().zip().for_each()` pattern for LLVM auto-vectorization
2. Extract row slice before inner loop to help compiler optimize
3. Add `#[inline]` hint for hot path

**File:** `crates/sail-function/src/aggregate/ols_sufficient_stats.rs`

## Key Findings

### Sail Advantages
- **72x faster on small datasets** - No JVM startup overhead
- **1.7-2x faster on large datasets** - SIMD-optimized Rust
- **Native Arrow/Parquet** - Zero-copy data loading
- **Pure Rust** - No external BLAS dependency required

### Why Sail Wins

| Aspect | Sail OLS (Optimized) | Spark L-BFGS |
|--------|---------------------|--------------|
| Linear Algebra | LLVM auto-vectorized | Breeze + Native BLAS |
| SIMD | Yes (compiler-generated) | Yes (via BLAS) |
| Algorithm | O(n) closed-form | O(iterations * n) iterative |
| JVM Overhead | None | Significant |

## High Feature Count Limitation (p=500)

For large2 (1M rows × 500 features), Spark wins by 2.5x. The reason:

| Metric | p=100 | p=500 |
|--------|-------|-------|
| X^T X matrix size | 80KB | **2MB** |
| Fits in L2 cache? | Yes | **No** |
| Cache misses | Low | **High** |

**Problem:** With p=500, the X^T X matrix (500×500 = 2MB) exceeds L2 cache (~256KB). Each row update causes cache misses.

**Solution:** Batch GEMM optimization (future work):
- Instead of rank-1 updates per row, accumulate batches
- Use BLAS GEMM with cache blocking
- Would achieve similar performance to Spark's BLAS-optimized L-BFGS

## Conclusion

With SIMD-friendly code patterns, **Sail OLS beats Spark L-BFGS for typical feature counts (p≤100)**:
- Small datasets: **72x faster** (JVM overhead dominates for Spark)
- Large datasets (p≤100): **1.7x faster** (efficient vectorized Rust)
- High feature counts (p=500): Spark wins 2.5x (needs BLAS batch optimization)

The optimization achieved **~2x speedup** over the original naive implementation by enabling LLVM auto-vectorization through idiomatic Rust iterator patterns.

## Raw Data

- `results_sail.json` - Original Sail implementation
- `results_sail_optimized.json` - SIMD-optimized Sail implementation
- `results_spark.json` - Spark L-BFGS baseline
