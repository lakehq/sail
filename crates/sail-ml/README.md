# sail-ml

Machine Learning library for Sail, compatible with Spark ML.

## Current Status

### Implemented

- **LinearRegression**
  - OLS solver (Normal Equation) - exact closed-form solution
  - SGD solver - iterative gradient descent
  - VectorUDT support for Spark ML compatibility

- **Distributed Aggregate Functions**
  - `ols_sufficient_stats` - Computes X^T X and X^T y for OLS
  - `sgd_gradient_sum` - Computes gradient sums for SGD

## Next Steps

### Phase 1: Integration
- [ ] Update `ml_handler.rs` to use `sail-ml` crate instead of inline code
- [ ] Add `sail-ml` as dependency to `sail-spark-connect`

### Phase 2: More Estimators
- [ ] **LogisticRegression** - binary/multiclass classification
  - Sigmoid function for probabilities
  - Cross-entropy loss
  - OLS-like closed form or gradient descent
- [ ] **DecisionTreeRegressor** - tree-based regression
- [ ] **DecisionTreeClassifier** - tree-based classification
- [ ] **RandomForest** - ensemble methods

### Phase 3: Transformers
- [ ] **VectorAssembler** - combine columns into feature vector
- [ ] **StandardScaler** - normalize features (mean=0, std=1)
- [ ] **MinMaxScaler** - scale to [0, 1] range
- [ ] **StringIndexer** - encode strings as integers
- [ ] **OneHotEncoder** - one-hot encoding for categorical

### Phase 4: Pipeline
- [ ] **Pipeline** - chain estimators and transformers
- [ ] **CrossValidator** - k-fold cross-validation
- [ ] **TrainValidationSplit** - train/validation split

### Phase 5: Model Persistence
- [ ] Save/load models to disk
- [ ] Parquet format for model storage
- [ ] Compatibility with Spark ML model format

## Architecture

```
sail-ml/
├── src/
│   ├── lib.rs              # Public API
│   ├── model.rs            # Trained model types
│   ├── estimator/          # Trainable estimators
│   │   ├── mod.rs
│   │   └── linear_regression.rs
│   ├── solver/             # Training algorithms
│   │   ├── mod.rs
│   │   ├── ols.rs          # Normal Equation
│   │   └── sgd.rs          # Gradient Descent
│   ├── transformer/        # Feature transformers (TODO)
│   └── pipeline/           # Pipeline support (TODO)
```

## Usage

```rust
use sail_ml::estimator::LinearRegression;
use sail_ml::solver::Solver;

let lr = LinearRegression::new()
    .with_solver(Solver::OLS);

let model = lr.fit(&features, &labels)?;
println!("Coefficients: {:?}", model.coefficients());
```

## Distributed Training

For very large datasets, use the SQL aggregate functions:

### OLS (Single-pass, exact solution)

```sql
SELECT ols_sufficient_stats(features, label) FROM training_data
```

Computes X^T X and X^T y in parallel across partitions. The driver merges
the results and solves β = (X^T X)^-1 X^T y.

### SGD (Iterative)

```sql
SELECT sgd_gradient_sum(features, label, coefficients) FROM training_data
```

Computes gradient sums in parallel for each epoch. The driver updates
coefficients and iterates until convergence.

Both functions support distributed execution - workers compute partial
statistics, driver merges and updates the model.

## Examples

### Demo: Train Linear Regression step by step

```bash
# Start Sail server
RUST_LOG='sail=debug' cargo run -p sail-cli -- spark server --port 50051

# Run demo (shows SGD training step by step)
export SPARK_REMOTE="sc://localhost:50051"
hatch run python python/pysail/examples/spark/ml/regresion/train_linear_regression.py
```

### Benchmark: Sail vs Spark

```bash
# 1. Generate test data (once)
hatch run python python/pysail/examples/spark/ml/regresion/benchmark_solvers.py --generate

# 2. Run against Sail
SPARK_REMOTE="sc://localhost:50051" hatch run python python/pysail/examples/spark/ml/regresion/benchmark_solvers.py

# 3. Run against Spark JVM (requires Java 17)
SPARK_REMOTE="local" hatch run python python/pysail/examples/spark/ml/regresion/benchmark_solvers.py

# 4. Compare results
hatch run python python/pysail/examples/spark/ml/regresion/benchmark_solvers.py --compare
```
