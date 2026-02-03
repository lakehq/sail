#!/usr/bin/env python3
"""
Demo: Distributed Linear Regression Training with Sail

This script demonstrates how to train a linear regression model using Sail's
distributed sgd_gradient_sum function. The training happens in a distributed
manner across workers, making it suitable for large datasets.

Usage:
    1. Start Sail server:
       RUST_LOG='sail=debug' cargo run -p sail-cli -- spark server --port 50051

    2. Run this script:
       export SPARK_REMOTE="sc://localhost:50051"
       hatch run python scripts/ml_demo/train_linear_regression.py

Algorithm:
    For y = X*beta, we minimize MSE using batch gradient descent:

    Each epoch:
        gradient = (2/n) * X^T * (X*beta - y)
        beta = beta - learning_rate * gradient

    The sgd_gradient_sum function computes the gradient sum in a distributed
    manner across all partitions.
"""

from dataclasses import dataclass
from typing import Optional

from pyspark.sql import SparkSession


@dataclass
class LinearRegressionModel:
    """Trained linear regression model."""
    coefficients: list[float]
    intercept: float
    num_epochs: int
    final_mse: float


@dataclass
class TrainingConfig:
    """Configuration for training."""
    learning_rate: float = 0.01
    max_epochs: int = 100
    tolerance: float = 1e-6
    verbose: bool = True


def train_linear_regression(
    spark: SparkSession,
    table_name: str,
    feature_cols: list[str],
    label_col: str,
    config: Optional[TrainingConfig] = None,
) -> LinearRegressionModel:
    """
    Train a linear regression model using distributed gradient descent.

    Args:
        spark: SparkSession connected to Sail
        table_name: Name of the table/view with training data
        feature_cols: List of feature column names
        label_col: Name of the label column
        config: Training configuration

    Returns:
        Trained LinearRegressionModel
    """
    if config is None:
        config = TrainingConfig()

    num_features = len(feature_cols)
    beta = [0.0] * num_features

    if config.verbose:
        print(f"Training Linear Regression")
        print(f"  Features: {feature_cols}")
        print(f"  Label: {label_col}")
        print(f"  Learning rate: {config.learning_rate}")
        print(f"  Max epochs: {config.max_epochs}")
        print()

    prev_mse = float('inf')

    for epoch in range(config.max_epochs):
        # Build feature array expression
        features_expr = ", ".join([f"CAST({col} AS DOUBLE)" for col in feature_cols])
        beta_expr = ", ".join([f"CAST({b} AS DOUBLE)" for b in beta])

        # Execute distributed gradient computation
        query = f"""
            SELECT sgd_gradient_sum(
                ARRAY({features_expr}),
                CAST({label_col} AS DOUBLE),
                ARRAY({beta_expr})
            ) AS result
            FROM {table_name}
        """

        result = spark.sql(query).collect()[0]['result']

        gradient_sum = result['gradient_sum']
        count = result['count']
        loss_sum = result['loss_sum']
        mse = loss_sum / count

        # Update coefficients: beta = beta - lr * (2/n) * gradient_sum
        for i in range(num_features):
            avg_gradient = (2.0 / count) * gradient_sum[i]
            beta[i] = beta[i] - config.learning_rate * avg_gradient

        # Check convergence
        if abs(prev_mse - mse) < config.tolerance:
            if config.verbose:
                print(f"  Converged at epoch {epoch}")
            break

        prev_mse = mse

        # Print progress
        if config.verbose and (epoch % 20 == 0 or epoch == config.max_epochs - 1):
            beta_str = ", ".join([f"{b:.4f}" for b in beta])
            print(f"  Epoch {epoch:3d}: MSE = {mse:.6f}, beta = [{beta_str}]")

    if config.verbose:
        print()
        print(f"Training complete!")
        print(f"  Final coefficients: {beta}")
        print(f"  Final MSE: {mse:.6f}")

    return LinearRegressionModel(
        coefficients=beta,
        intercept=0.0,  # No intercept in this version
        num_epochs=epoch + 1,
        final_mse=mse,
    )


def predict(model: LinearRegressionModel, features: list[float]) -> float:
    """Make a prediction using the trained model."""
    return sum(c * f for c, f in zip(model.coefficients, features)) + model.intercept


def main():
    # Connect to Sail server
    spark = SparkSession.builder \
        .remote("sc://localhost:50051") \
        .getOrCreate()

    print("=" * 60)
    print("Sail Linear Regression Demo")
    print("=" * 60)
    print()

    # Create training data: y = 2*x1 + 3*x2 + noise
    print("Creating training data (y = 2*x1 + 3*x2 + noise)...")
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW training_data AS
        SELECT
            CAST(x1 AS DOUBLE) as x1,
            CAST(x2 AS DOUBLE) as x2,
            CAST(y AS DOUBLE) as y
        FROM VALUES
            (1.0, 1.0, 5.1),
            (2.0, 1.0, 7.2),
            (1.0, 2.0, 8.0),
            (2.0, 2.0, 10.1),
            (3.0, 1.0, 9.0),
            (1.0, 3.0, 10.9),
            (3.0, 2.0, 12.1),
            (2.0, 3.0, 12.9),
            (3.0, 3.0, 15.0),
            (4.0, 2.0, 14.2)
        AS t(x1, x2, y)
    """)
    print()

    # Show the data
    print("Training data:")
    spark.sql("SELECT * FROM training_data").show()

    # Train the model
    model = train_linear_regression(
        spark=spark,
        table_name="training_data",
        feature_cols=["x1", "x2"],
        label_col="y",
        config=TrainingConfig(
            learning_rate=0.01,
            max_epochs=200,
            verbose=True,
        ),
    )

    print()
    print("=" * 60)
    print("Making predictions")
    print("=" * 60)
    print()

    # Make predictions
    test_cases = [
        [1.0, 1.0],  # Expected: ~5 (2*1 + 3*1)
        [2.0, 2.0],  # Expected: ~10 (2*2 + 3*2)
        [5.0, 5.0],  # Expected: ~25 (2*5 + 3*5)
    ]

    print("Model: y = {:.4f}*x1 + {:.4f}*x2".format(
        model.coefficients[0], model.coefficients[1]
    ))
    print("Expected: y = 2*x1 + 3*x2")
    print()

    for features in test_cases:
        pred = predict(model, features)
        expected = 2 * features[0] + 3 * features[1]
        print(f"  x1={features[0]}, x2={features[1]} -> "
              f"pred={pred:.2f}, expected={expected:.2f}")


if __name__ == "__main__":
    main()
