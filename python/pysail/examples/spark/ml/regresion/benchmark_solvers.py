import ast
import json
import os
import sys
import time
from pathlib import Path
from tempfile import gettempdir

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf

"""
Benchmark: Sail (OLS, SGD) vs Spark (L-BFGS)

Generates synthetic data and compares:
- Training time
- Coefficient accuracy (MAE vs true coefficients)

Usage:
  # Step 1: Generate test data (only once, uses PyArrow)
  hatch run python python/pysail/examples/spark/ml/regresion/benchmark_solvers.py --generate

  # Step 2: Run benchmark against Sail
  SPARK_REMOTE="sc://localhost:50051" hatch run python python/pysail/examples/spark/ml/regresion/benchmark_solvers.py

  # Step 3: Run benchmark against Spark JVM (requires Java 17)
  SPARK_REMOTE="local" hatch run python python/pysail/examples/spark/ml/regresion/benchmark_solvers.py

  # Step 4: Compare results (after running both)
  hatch run python python/pysail/examples/spark/ml/regresion/benchmark_solvers.py --compare
"""


# Data directory (in temp to avoid polluting the repo)
DATA_DIR = Path(gettempdir()) / "sail_benchmark"

# Configuration - can be large since we read from Parquet
DATASETS = [
    {"name": "tiny", "n": 10_000, "p": 20},
    {"name": "small", "n": 100_000, "p": 50},
    {"name": "medium", "n": 500_000, "p": 100},
    {"name": "large", "n": 1_000_000, "p": 100},
]


# True coefficients for validation
def generate_true_coefficients(p):
    """Generate known coefficients: [1, 2, 3, ..., p]"""
    return np.arange(1, p + 1, dtype=np.float64)


def generate_and_save_data(dataset, seed=42):
    """Generate synthetic data and save to Parquet using PyArrow (no Spark needed)"""

    n, p, name = dataset["n"], dataset["p"], dataset["name"]

    np.random.seed(seed)
    true_coefs = generate_true_coefficients(p)

    print(f"  Generating {n:,} samples with {p} features...")
    start = time.time()

    # Generate all data at once (more efficient)
    x_big = np.random.randn(n, p)
    y = x_big @ true_coefs + np.random.randn(n) * 0.1  # Small noise

    gen_time = time.time() - start
    print(f"  Data generated in {gen_time:.1f}s")

    # Create Arrow table with VectorUDT-compatible structure
    # VectorUDT: struct{type: int8, size: int32, indices: list<int32>, values: list<float64>}
    print("  Converting to Arrow format...")

    # For dense vectors: type=1, size=0, indices=null, values=features
    types = np.ones(n, dtype=np.int8)  # 1 = dense
    sizes = np.zeros(n, dtype=np.int32)  # 0 for dense

    # Create list arrays for values (each row is a list of floats)
    values_list = [x_big[i].tolist() for i in range(n)]

    # Build the struct array for features
    features_struct = pa.StructArray.from_arrays(
        [
            pa.array(types),
            pa.array(sizes),
            pa.array([None] * n, type=pa.list_(pa.int32())),  # indices (null for dense)
            pa.array(values_list, type=pa.list_(pa.float64())),  # values
        ],
        names=["type", "size", "indices", "values"],
    )

    table = pa.table(
        {
            "label": pa.array(y),
            "features": features_struct,
        }
    )

    # Save to Parquet
    output_path = DATA_DIR / name
    print(f"  Saving to {output_path}...")
    output_path.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, output_path / "data.parquet")

    # Save coefficients for validation
    coef_path = DATA_DIR / f"{name}_coefs.npy"
    np.save(coef_path, true_coefs)

    print(f"  Saved {n:,} rows to {output_path}")


def load_data(spark, dataset):
    """Load data from Parquet (no gRPC transfer)"""
    name = dataset["name"]
    data_path = DATA_DIR / name
    coef_path = DATA_DIR / f"{name}_coefs.npy"

    if not data_path.exists():
        msg = f"Data not found at {data_path}. Run with --generate first."
        raise FileNotFoundError(msg)

    df = spark.read.parquet(str(data_path))
    true_coefs = np.load(coef_path)

    return df, true_coefs


def parse_coefficients(coefs):
    """Parse coefficients - handles both array and string format"""
    if isinstance(coefs, str):
        # Sail returns string like "[1.0, 2.0, 3.0]"
        return ast.literal_eval(coefs)
    # Spark returns DenseVector
    return list(coefs)


def coefficient_error(predicted, true_coefs):
    """Calculate mean absolute error of coefficients"""
    pred = np.array(parse_coefficients(predicted))
    true = np.array(true_coefs)
    return np.mean(np.abs(pred - true))


def run_benchmark(spark, dataset):
    """Run benchmark for a single dataset size"""
    n, p = dataset["n"], dataset["p"]
    name = dataset["name"]

    print(f"\n{'=' * 60}")
    print(f"DATASET: {name} ({n:,} rows x {p} features)")
    print(f"{'=' * 60}")

    # Load data from Parquet (no gRPC transfer!)
    try:
        df, true_coefs = load_data(spark, dataset)
        print(f"  Loaded from Parquet: {DATA_DIR / name}")
    except FileNotFoundError as e:
        print(f"  ERROR: {e}")
        return {}

    results = {}

    # Check if we're on Sail or Spark
    is_sail = "localhost:50051" in os.environ.get("SPARK_REMOTE", "")

    if is_sail:
        # Sail: OLS now uses distributed by default (auto/normal/distributed all use it)
        solvers = [
            ("OLS (Sail)", {"solver": "auto", "regParam": 0.0}),  # Uses distributed OLS
            ("SGD (Sail)", {"solver": "sgd", "maxIter": 1000, "regParam": 0.0}),
        ]
    else:
        # Spark JVM: convert struct to proper Vector using Python UDF

        @udf(returnType=VectorUDT())
        def to_vector(struct):
            if struct is None:
                return None
            return Vectors.dense(struct["values"])

        df = df.withColumn("features", to_vector(df["features"]))

        solvers = [
            ("L-BFGS (Spark)", {"maxIter": 100, "regParam": 0.0}),
        ]

    for solver_name, params in solvers:
        print(f"\n{solver_name}")
        print("-" * 40)

        try:
            lr = LinearRegression(**params)

            start = time.time()
            model = lr.fit(df)
            train_time = time.time() - start

            # Get coefficients (Sail returns string, Spark returns DenseVector)
            raw_coefs = model.coefficients
            coefs = parse_coefficients(raw_coefs)
            error = coefficient_error(raw_coefs, true_coefs)

            print(f"  Time: {train_time:.2f}s")
            print(f"  Coef Error (MAE): {error:.6f}")
            print(f"  First 5 coefs: {coefs[:5]}")
            print(f"  Expected:       {list(true_coefs[:5])}")

            results[solver_name] = {
                "time": train_time,
                "error": error,
                "coefficients": coefs,
            }

        except Exception as e:  # noqa: BLE001
            print(f"  ERROR: {e}")
            results[solver_name] = {"error": str(e)}

    return results


def generate_all_data():
    """Generate all benchmark datasets (run once)"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Generating benchmark data in {DATA_DIR}")
    print("This may take a few minutes...\n")

    for dataset in DATASETS:
        print(f"\n{dataset['name'].upper()}: {dataset['n']:,} rows x {dataset['p']} features")
        generate_and_save_data(dataset)

    print("\n" + "=" * 60)
    print("Data generation complete!")
    print(f"Files saved in: {DATA_DIR}")
    print("=" * 60)


def run_benchmarks():
    """Run benchmarks against Sail or Spark"""

    remote = os.environ.get("SPARK_REMOTE", "sc://localhost:50051")
    os.environ.setdefault("SPARK_REMOTE", remote)

    print(f"Connecting to: {remote}")
    spark = SparkSession.builder.getOrCreate()

    all_results = {}

    for dataset in DATASETS:
        results = run_benchmark(spark, dataset)
        if results:
            all_results[dataset["name"]] = results

    # Summary table
    print(f"\n{'=' * 70}")
    print("BENCHMARK SUMMARY")
    print(f"{'=' * 70}")

    # Header
    print(f"\n{'Dataset':<10} {'Rows':>10} {'Features':>8} {'Solver':<15} {'Time (s)':>10} {'MAE':>12}")
    print("-" * 70)

    # Data rows
    for ds in DATASETS:
        ds_name = ds["name"]
        if ds_name not in all_results:
            continue
        for solver, data in all_results[ds_name].items():
            if "time" in data:
                print(
                    f"{ds_name:<10} {ds['n']:>10,} {ds['p']:>8} {solver:<15} {data['time']:>10.2f} {data['error']:>12.6f}"
                )
            else:
                print(f"{ds_name:<10} {ds['n']:>10,} {ds['p']:>8} {solver:<15} {'FAILED':>10} {'-':>12}")

    print("-" * 70)

    # Save results to JSON for comparison
    backend = "sail" if "localhost:50051" in remote else "spark"
    results_file = DATA_DIR / f"results_{backend}.json"
    with open(results_file, "w") as f:
        json.dump(
            {
                "backend": backend,
                "remote": remote,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "results": {
                    k: {sk: {kk: vv for kk, vv in sv.items() if kk != "coefficients"} for sk, sv in v.items()}
                    for k, v in all_results.items()
                },
            },
            f,
            indent=2,
        )
    print(f"\nResults saved to: {results_file}")

    spark.stop()
    print("Done!")


def compare_results():
    """Compare Sail vs Spark results if both exist"""

    sail_file = DATA_DIR / "results_sail.json"
    spark_file = DATA_DIR / "results_spark.json"

    if not sail_file.exists() or not spark_file.exists():
        print("Need both results_sail.json and results_spark.json to compare")
        print(f"  Sail results: {'EXISTS' if sail_file.exists() else 'MISSING'}")
        print(f"  Spark results: {'EXISTS' if spark_file.exists() else 'MISSING'}")
        return

    with open(sail_file) as f:
        sail = json.load(f)
    with open(spark_file) as f:
        spark = json.load(f)

    print(f"\n{'=' * 80}")
    print("SAIL vs SPARK COMPARISON")
    print(f"{'=' * 80}")
    print(f"Sail run:  {sail['timestamp']}")
    print(f"Spark run: {spark['timestamp']}")

    print(f"\n{'Dataset':<10} {'Rows':>10} {'Sail OLS':>12} {'Sail SGD':>12} {'Spark L-BFGS':>14} {'Speedup':>10}")
    print("-" * 80)

    for ds in DATASETS:
        name = ds["name"]
        sail_ols = sail["results"].get(name, {}).get("OLS (Sail)", {}).get("time", "-")
        sail_sgd = sail["results"].get(name, {}).get("SGD (Sail)", {}).get("time", "-")
        spark_lbfgs = spark["results"].get(name, {}).get("L-BFGS (Spark)", {}).get("time", "-")

        # Calculate speedup (Spark time / Sail OLS time)
        if isinstance(sail_ols, (int, float)) and isinstance(spark_lbfgs, (int, float)) and sail_ols > 0:
            speedup = f"{spark_lbfgs / sail_ols:.1f}x"
        else:
            speedup = "-"

        sail_ols_str = f"{sail_ols:.2f}s" if isinstance(sail_ols, (int, float)) else sail_ols
        sail_sgd_str = f"{sail_sgd:.2f}s" if isinstance(sail_sgd, (int, float)) else sail_sgd
        spark_str = f"{spark_lbfgs:.2f}s" if isinstance(spark_lbfgs, (int, float)) else spark_lbfgs

        print(f"{name:<10} {ds['n']:>10,} {sail_ols_str:>12} {sail_sgd_str:>12} {spark_str:>14} {speedup:>10}")

    print("-" * 80)


def main():
    if "--generate" in sys.argv:
        generate_all_data()
    elif "--compare" in sys.argv:
        compare_results()
    else:
        run_benchmarks()


if __name__ == "__main__":
    main()
