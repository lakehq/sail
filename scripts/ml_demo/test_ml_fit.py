#!/usr/bin/env python3
"""
Test script for MlCommand::Fit handler.

This tests that the ML Fit command is properly routed and handled.
"""

import os
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.linalg import Vectors

os.environ.setdefault("SPARK_REMOTE", "sc://localhost:50051")

spark = SparkSession.builder.getOrCreate()

# Create simple training data using Vectors (compatible with both Sail and Spark ML)
# Solution: y = 1*x1 + 2*x2, so coefficients should be [1.0, 2.0]
data = [
    (1.0, Vectors.dense([1.0, 0.0])),
    (2.0, Vectors.dense([0.0, 1.0])),
    (3.0, Vectors.dense([1.0, 1.0])),
]
df = spark.createDataFrame(data, ["label", "features"])

print("Training data:")
df.show()

# Create LinearRegression estimator with more iterations and low tolerance for SGD convergence
lr = LinearRegression(maxIter=1000, tol=1e-10, regParam=0.0)

print("Attempting to fit LinearRegression...")
try:
    model = lr.fit(df)
    print(f"Model trained successfully!")
    print(f"  UID: {model.uid}")

    # Access model properties
    # Note: In this POC, coefficients returns a string representation
    print(f"  Coefficients: {model.coefficients}")
    print(f"  Intercept: {model.intercept}")
    print("\nMlCommand::Fit and MlCommand::Fetch are working!")

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

spark.stop()
print("\nDone!")
