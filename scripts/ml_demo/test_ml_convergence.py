#!/usr/bin/env python3
"""
Test that LinearRegression converges to correct coefficients.

Dataset: y = 1*x1 + 2*x2
Expected coefficients: [1.0, 2.0]
"""

import os
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression

os.environ.setdefault("SPARK_REMOTE", "sc://localhost:50051")

spark = SparkSession.builder.getOrCreate()

# Create training data where y = 1*x1 + 2*x2
# Solution: β = [1.0, 2.0]
from pyspark.ml.linalg import Vectors

data = [
    (1.0, Vectors.dense([1.0, 0.0])),  # 1*1 + 2*0 = 1
    (2.0, Vectors.dense([0.0, 1.0])),  # 1*0 + 2*1 = 2
    (3.0, Vectors.dense([1.0, 1.0])),  # 1*1 + 2*1 = 3
]
df = spark.createDataFrame(data, ["label", "features"])

print("Training data (y = x1 + 2*x2):")
df.show()

# Use more iterations and low tolerance
lr = LinearRegression(
    maxIter=10000,
    tol=1e-10,     # Very low tolerance to not stop early
    regParam=0.0,  # No regularization
)

print(f"Training with maxIter=10000, stepSize=0.1...")
try:
    model = lr.fit(df)
    print(f"\nModel trained!")
    print(f"  Coefficients: {model.coefficients}")
    print(f"  Intercept: {model.intercept}")
    print(f"\nExpected: [1.0, 2.0], intercept=0.0")

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

spark.stop()
