#!/usr/bin/env python3
"""
Test script to compare OLS (Normal Equation) vs SGD solvers.

Dataset: y = 1*x1 + 2*x2
Expected coefficients: [1.0, 2.0]
"""

import os
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.linalg import Vectors

os.environ.setdefault("SPARK_REMOTE", "sc://localhost:50051")

spark = SparkSession.builder.getOrCreate()

# Create training data where y = 1*x1 + 2*x2
data = [
    (1.0, Vectors.dense([1.0, 0.0])),  # 1*1 + 2*0 = 1
    (2.0, Vectors.dense([0.0, 1.0])),  # 1*0 + 2*1 = 2
    (3.0, Vectors.dense([1.0, 1.0])),  # 1*1 + 2*1 = 3
]
df = spark.createDataFrame(data, ["label", "features"])

print("Training data (y = x1 + 2*x2):")
df.show()

print("\n" + "="*60)
print("COMPARING SOLVERS")
print("Expected: coefficients=[1.0, 2.0], intercept=0.0")
print("="*60)

# Test 1: Normal Equation (OLS) - default, exact solution
print("\n1. NORMAL EQUATION (solver='normal') - Exact OLS")
print("-"*50)
lr_normal = LinearRegression(solver="normal", regParam=0.0)
try:
    model_normal = lr_normal.fit(df)
    print(f"   Coefficients: {model_normal.coefficients}")
    print(f"   Intercept: {model_normal.intercept}")
except Exception as e:
    print(f"   Error: {e}")

# Test 2: SGD solver
print("\n2. SGD (solver='sgd') - Iterative gradient descent")
print("-"*50)
lr_sgd = LinearRegression(
    solver="sgd",
    maxIter=10000,
    tol=1e-10,
    regParam=0.0,
)
try:
    model_sgd = lr_sgd.fit(df)
    print(f"   Coefficients: {model_sgd.coefficients}")
    print(f"   Intercept: {model_sgd.intercept}")
except Exception as e:
    print(f"   Error: {e}")

# Test 3: Auto solver (should use Normal by default)
print("\n3. AUTO (solver='auto') - Should use Normal Equation")
print("-"*50)
lr_auto = LinearRegression(solver="auto", regParam=0.0)
try:
    model_auto = lr_auto.fit(df)
    print(f"   Coefficients: {model_auto.coefficients}")
    print(f"   Intercept: {model_auto.intercept}")
except Exception as e:
    print(f"   Error: {e}")

print("\n" + "="*60)
print("SUMMARY")
print("="*60)
print("- Normal Equation: Exact closed-form solution, O(n*p² + p³)")
print("- SGD: Iterative approximation, O(n*p*iterations)")
print("- For exact results: use solver='normal'")
print("- For huge sparse data: use solver='sgd'")

spark.stop()
print("\nDone!")
