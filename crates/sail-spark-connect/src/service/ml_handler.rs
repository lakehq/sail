use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use datafusion::arrow::array::{Array, Float64Array, Int32Array, Int8Array, ListArray, StructArray};
use datafusion::arrow::compute::concat_batches;
use datafusion::prelude::SessionContext;
use futures::stream;
use lazy_static::lazy_static;
use log::{debug, info};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::job::JobService;
use sail_plan::resolve_and_execute_plan;
use uuid::Uuid;

use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::executor::{ExecutorBatch, ExecutorMetadata, ExecutorOutput};
use crate::service::plan_executor::ExecutePlanResponseStream;
use crate::session::SparkSession;
use crate::spark::connect::expression::literal::LiteralType;
use crate::spark::connect::expression::Literal;
use crate::spark::connect::ml_command::Command;
use crate::spark::connect::ml_command_result::{MlOperatorInfo, ResultType};
use crate::spark::connect::{Fetch, MlCommand, MlCommandResult, MlParams, ObjectRef, Relation};

lazy_static! {
    /// Simple in-memory cache for trained ML models.
    ///
    /// WARNING: This cache has known limitations:
    /// - No eviction policy (models stay in memory until explicitly deleted)
    /// - No persistence (models are lost on server restart)
    /// - No size limits (could cause OOM with many large models)
    ///
    /// For production use, consider:
    /// - LRU eviction based on model size
    /// - Disk offloading (see MlCommand::Delete.evict_only)
    /// - Per-session isolation
    static ref MODEL_CACHE: RwLock<HashMap<String, TrainedModel>> = RwLock::new(HashMap::new());
}

/// Represents a trained LinearRegression model.
#[derive(Debug, Clone)]
struct TrainedModel {
    coefficients: Vec<f64>,
    intercept: f64,
    num_features: usize,
}

/// Handle ML command execution.
pub(crate) async fn handle_execute_ml_command(
    ctx: &SessionContext,
    ml_command: MlCommand,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    debug!("Received ML command: {:?}", ml_command);
    let spark = ctx.extension::<SparkSession>()?;
    let command = ml_command.command.required("ML command")?;

    let result = match command {
        Command::Fit(fit) => handle_ml_fit(ctx, &spark, fit).await?,
        Command::Fetch(fetch) => handle_ml_fetch(fetch)?,
        Command::Delete(_) => return Err(SparkError::todo("ML delete command")),
        Command::Write(_) => return Err(SparkError::todo("ML write command")),
        Command::Read(_) => return Err(SparkError::todo("ML read command")),
        Command::Evaluate(_) => return Err(SparkError::todo("ML evaluate command")),
        Command::CleanCache(_) => return Err(SparkError::todo("ML clean cache command")),
        Command::GetCacheInfo(_) => return Err(SparkError::todo("ML get cache info command")),
        Command::CreateSummary(_) => return Err(SparkError::todo("ML create summary command")),
        Command::GetModelSize(_) => return Err(SparkError::todo("ML get model size command")),
    };

    let output = vec![
        ExecutorOutput::new(ExecutorBatch::MlCommandResult(Box::new(result))),
        ExecutorOutput::complete(),
    ];

    Ok(ExecutePlanResponseStream::new(
        spark.session_id().to_string(),
        metadata.operation_id,
        Box::pin(stream::iter(output)),
    ))
}

/// Handle ML Fit command - trains a LinearRegression model using SGD.
async fn handle_ml_fit(
    ctx: &SessionContext,
    spark: &SparkSession,
    fit: crate::spark::connect::ml_command::Fit,
) -> SparkResult<MlCommandResult> {
    let estimator = fit.estimator.required("estimator")?;
    let params = fit.params;
    let dataset = fit.dataset.required("dataset")?;

    debug!("ML Fit: estimator={:?}, params={:?}", estimator.name, params);

    // Check if this is a LinearRegression estimator
    if !estimator.name.contains("LinearRegression") {
        return Err(SparkError::unsupported(format!(
            "ML estimator '{}' is not supported yet. Only LinearRegression is implemented.",
            estimator.name
        )));
    }

    // Extract parameters with defaults
    let params_map = extract_params(&params);
    let max_iter = get_param_i64(&params_map, "maxIter", 1000) as usize;
    let learning_rate = get_param_f64(&params_map, "stepSize", 0.1);
    let tolerance = get_param_f64(&params_map, "tol", 1e-6);
    let features_col = get_param_string(&params_map, "featuresCol", "features");
    let label_col = get_param_string(&params_map, "labelCol", "label");
    // Solver: "normal" (OLS, exact), "sgd" (gradient descent), "auto" (chooses normal)
    let solver = get_param_string(&params_map, "solver", "auto");

    info!(
        "Training LinearRegression: solver={}, maxIter={}, stepSize={}, tol={}, featuresCol={}, labelCol={}",
        solver, max_iter, learning_rate, tolerance, features_col, label_col
    );

    // Step 1: Execute dataset and extract training data
    let (features, labels) =
        extract_training_data(ctx, spark, &dataset, &features_col, &label_col).await?;

    let num_samples = labels.len();
    let num_features = if features.is_empty() {
        0
    } else {
        features[0].len()
    };

    info!(
        "Training data extracted: {} samples, {} features",
        num_samples, num_features
    );

    if num_samples == 0 {
        return Err(SparkError::invalid("Training dataset is empty"));
    }

    // Step 2: Train model using selected solver
    let trained_model = if solver == "sgd" {
        info!("Using SGD solver (iterative gradient descent)");
        let mut coefficients = vec![0.0; num_features];
        run_sgd_training(
            &features,
            &labels,
            &mut coefficients,
            max_iter,
            learning_rate,
            tolerance,
        )?
    } else {
        // Default to Normal Equation (OLS) - gives exact solution
        // Handles: "normal", "auto", and any other value
        info!("Using Normal Equation solver (exact OLS)");
        run_ols_training(&features, &labels)?
    };

    // Step 4: Store model in cache
    let model_id = Uuid::new_v4().to_string();
    {
        let mut cache = MODEL_CACHE.write().map_err(|e| {
            SparkError::internal(format!("Failed to acquire model cache lock: {e}"))
        })?;
        cache.insert(model_id.clone(), trained_model.clone());
    }

    info!(
        "Model trained and cached: id={}, coefficients={:?}, intercept={}",
        model_id, trained_model.coefficients, trained_model.intercept
    );

    // Step 5: Return result
    let mut result_params = HashMap::new();
    let coef_str = format!("{:?}", trained_model.coefficients);
    result_params.insert(
        "coefficients".to_string(),
        Literal {
            data_type: None,
            literal_type: Some(LiteralType::String(coef_str)),
        },
    );

    Ok(MlCommandResult {
        result_type: Some(ResultType::OperatorInfo(MlOperatorInfo {
            r#type: Some(
                crate::spark::connect::ml_command_result::ml_operator_info::Type::ObjRef(
                    ObjectRef { id: model_id },
                ),
            ),
            uid: Some(estimator.uid),
            params: Some(MlParams {
                params: result_params,
            }),
            warning_message: None,
        })),
    })
}

/// Extract training data (features and labels) from the dataset.
async fn extract_training_data(
    ctx: &SessionContext,
    spark: &SparkSession,
    dataset: &Relation,
    features_col: &str,
    label_col: &str,
) -> SparkResult<(Vec<Vec<f64>>, Vec<f64>)> {
    // Execute the dataset plan
    let plan: sail_common::spec::Plan = dataset.clone().try_into()?;
    let config = spark.plan_config()?;
    let (physical_plan, _) = resolve_and_execute_plan(ctx, config, plan).await?;

    // Execute to get data
    let job_service = ctx.extension::<JobService>()?;
    let stream = job_service.runner().execute(ctx, physical_plan).await?;
    let schema = stream.schema();

    // Collect all batches
    use futures::StreamExt;
    let batches: Vec<_> = stream.collect().await;
    let batches: Result<Vec<_>, _> = batches.into_iter().collect();
    let batches =
        batches.map_err(|e| SparkError::internal(format!("Failed to collect data: {e}")))?;

    if batches.is_empty() {
        return Ok((vec![], vec![]));
    }

    let data = concat_batches(&schema, batches.iter())
        .map_err(|e| SparkError::internal(format!("Failed to concat batches: {e}")))?;

    // Find column indices
    let features_idx = schema
        .fields()
        .iter()
        .position(|f| f.name() == features_col)
        .ok_or_else(|| {
            SparkError::invalid(format!("Features column '{}' not found", features_col))
        })?;

    let label_idx = schema
        .fields()
        .iter()
        .position(|f| f.name() == label_col)
        .ok_or_else(|| SparkError::invalid(format!("Label column '{}' not found", label_col)))?;

    // Extract features (array of arrays)
    let features_array = data.column(features_idx);
    let features = extract_features(features_array)?;

    // Extract labels
    let label_array = data.column(label_idx);
    let labels = extract_labels(label_array)?;

    Ok((features, labels))
}

/// Extract features from an array column.
///
/// Supports two formats:
/// - ListArray: Plain array of floats (e.g., from Python lists)
/// - StructArray: Spark ML VectorUDT (type, size, indices, values)
fn extract_features(array: &Arc<dyn Array>) -> SparkResult<Vec<Vec<f64>>> {
    // Try ListArray first (plain arrays)
    if let Some(list_array) = array.as_any().downcast_ref::<ListArray>() {
        return extract_features_from_list(list_array);
    }

    // Try StructArray (VectorUDT)
    if let Some(struct_array) = array.as_any().downcast_ref::<StructArray>() {
        return extract_features_from_vector_udt(struct_array);
    }

    Err(SparkError::invalid(
        "Features column must be an array type or VectorUDT",
    ))
}

/// Extract features from a ListArray (plain Python lists).
fn extract_features_from_list(list_array: &ListArray) -> SparkResult<Vec<Vec<f64>>> {
    let mut features = Vec::with_capacity(list_array.len());

    for i in 0..list_array.len() {
        if list_array.is_null(i) {
            continue;
        }
        let inner = list_array.value(i);
        let float_array = inner
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| SparkError::invalid("Feature values must be float64"))?;

        let row: Vec<f64> = float_array.iter().flatten().collect();
        features.push(row);
    }

    Ok(features)
}

/// Extract features from a VectorUDT (Spark ML vector format).
///
/// VectorUDT struct fields:
/// - type: Int8 (0=sparse, 1=dense)
/// - size: Int32 (vector size, used for sparse)
/// - indices: List<Int32> (non-zero indices for sparse)
/// - values: List<Float64> (values)
fn extract_features_from_vector_udt(struct_array: &StructArray) -> SparkResult<Vec<Vec<f64>>> {
    let type_col = struct_array
        .column_by_name("type")
        .ok_or_else(|| SparkError::invalid("VectorUDT missing 'type' field"))?;
    let type_array = type_col
        .as_any()
        .downcast_ref::<Int8Array>()
        .ok_or_else(|| SparkError::invalid("VectorUDT 'type' must be Int8"))?;

    let size_col = struct_array
        .column_by_name("size")
        .ok_or_else(|| SparkError::invalid("VectorUDT missing 'size' field"))?;
    let size_array = size_col
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| SparkError::invalid("VectorUDT 'size' must be Int32"))?;

    let indices_col = struct_array
        .column_by_name("indices")
        .ok_or_else(|| SparkError::invalid("VectorUDT missing 'indices' field"))?;
    let indices_array = indices_col
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| SparkError::invalid("VectorUDT 'indices' must be List"))?;

    let values_col = struct_array
        .column_by_name("values")
        .ok_or_else(|| SparkError::invalid("VectorUDT missing 'values' field"))?;
    let values_array = values_col
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| SparkError::invalid("VectorUDT 'values' must be List"))?;

    let mut features = Vec::with_capacity(struct_array.len());

    for i in 0..struct_array.len() {
        if struct_array.is_null(i) {
            continue;
        }

        let vec_type = type_array.value(i);
        let values = values_array.value(i);
        let float_values = values
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| SparkError::invalid("VectorUDT values must be Float64"))?;

        let row = if vec_type == 1 {
            // Dense vector: values array contains all elements
            float_values.iter().flatten().collect()
        } else {
            // Sparse vector: need to expand using indices
            let size = size_array.value(i) as usize;
            let indices = indices_array.value(i);
            let int_indices = indices
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| SparkError::invalid("VectorUDT indices must be Int32"))?;

            let mut dense = vec![0.0; size];
            for (j, idx) in int_indices.iter().enumerate() {
                if let Some(idx) = idx {
                    if let Some(val) = float_values.value(j).into() {
                        dense[idx as usize] = val;
                    }
                }
            }
            dense
        };

        features.push(row);
    }

    Ok(features)
}

/// Extract labels from a column.
fn extract_labels(array: &Arc<dyn Array>) -> SparkResult<Vec<f64>> {
    let float_array = array
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| SparkError::invalid("Label column must be float64"))?;

    Ok(float_array.iter().flatten().collect())
}

/// Run SGD training loop in pure Rust.
///
/// Uses batch gradient descent to minimize MSE:
/// For y = Xβ, the gradient is: ∂MSE/∂β = (2/n) * X^T * (Xβ - y)
fn run_sgd_training(
    features: &[Vec<f64>],
    labels: &[f64],
    coefficients: &mut [f64],
    max_iter: usize,
    learning_rate: f64,
    tolerance: f64,
) -> SparkResult<TrainedModel> {
    let n = labels.len() as f64;
    let mut prev_loss = f64::MAX;

    for epoch in 0..max_iter {
        // Compute predictions and gradients
        let mut gradient = vec![0.0; coefficients.len()];
        let mut loss_sum = 0.0;

        for (x, &y) in features.iter().zip(labels.iter()) {
            // Compute prediction: y_pred = x · β
            let y_pred: f64 = x
                .iter()
                .zip(coefficients.iter())
                .map(|(xi, bi)| xi * bi)
                .sum();

            // Compute error
            let error = y_pred - y;
            loss_sum += error * error;

            // Accumulate gradient: ∂MSE/∂β_j = 2 * error * x_j
            for (j, xj) in x.iter().enumerate() {
                gradient[j] += error * xj;
            }
        }

        // Update coefficients: β = β - learning_rate * (2/n) * gradient
        let scale = 2.0 / n;
        for (coef, grad) in coefficients.iter_mut().zip(gradient.iter()) {
            *coef -= learning_rate * scale * grad;
        }

        // Compute average loss (MSE)
        let avg_loss = loss_sum / n;

        debug!(
            "Epoch {}: loss={:.6}, coefficients={:?}",
            epoch, avg_loss, coefficients
        );

        // Check convergence
        if (prev_loss - avg_loss).abs() < tolerance {
            info!("Converged at epoch {} with loss {:.6}", epoch, avg_loss);
            break;
        }
        prev_loss = avg_loss;
    }

    Ok(TrainedModel {
        coefficients: coefficients.to_vec(),
        intercept: 0.0, // No intercept in this simple implementation
        num_features: coefficients.len(),
    })
}

/// Run OLS training using the Normal Equation (exact closed-form solution).
///
/// Computes: β = (X^T X)^-1 X^T y
///
/// This gives the exact least squares solution in a single pass.
/// Complexity: O(n * p^2) for matrix computation + O(p^3) for solving.
/// Best for: datasets where p (features) is moderate and exact solution is needed.
fn run_ols_training(features: &[Vec<f64>], labels: &[f64]) -> SparkResult<TrainedModel> {
    if features.is_empty() {
        return Err(SparkError::invalid("No training samples provided"));
    }

    let n = features.len();
    let p = features[0].len();

    // Step 1: Compute X^T X (p x p matrix)
    let mut xtx = vec![vec![0.0; p]; p];
    for row in features {
        for i in 0..p {
            for j in 0..p {
                xtx[i][j] += row[i] * row[j];
            }
        }
    }

    // Step 2: Compute X^T y (p vector)
    let mut xty = vec![0.0; p];
    for (row, &label) in features.iter().zip(labels.iter()) {
        for (i, &xi) in row.iter().enumerate() {
            xty[i] += xi * label;
        }
    }

    debug!("OLS: n={}, p={}", n, p);
    debug!("OLS: X^T X = {:?}", xtx);
    debug!("OLS: X^T y = {:?}", xty);

    // Step 3: Solve (X^T X) β = X^T y using Gaussian elimination with partial pivoting
    let coefficients = solve_linear_system(&mut xtx, &mut xty)?;

    info!("OLS solution: coefficients={:?}", coefficients);

    Ok(TrainedModel {
        coefficients,
        intercept: 0.0, // No intercept term in this implementation
        num_features: p,
    })
}

/// Solve a linear system Ax = b using Gaussian elimination with partial pivoting.
///
/// Modifies A and b in place. Returns the solution vector x.
fn solve_linear_system(a: &mut [Vec<f64>], b: &mut [f64]) -> SparkResult<Vec<f64>> {
    let n = b.len();

    if n == 0 {
        return Ok(vec![]);
    }

    // Forward elimination with partial pivoting
    for col in 0..n {
        // Find pivot (largest absolute value in column)
        let (max_row, max_val) = a
            .iter()
            .enumerate()
            .skip(col)
            .map(|(i, row)| (i, row[col].abs()))
            .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
            .unwrap_or((col, 0.0));

        // Check for singular matrix
        if max_val < 1e-10 {
            return Err(SparkError::invalid(
                "Matrix is singular or nearly singular - cannot solve OLS",
            ));
        }

        // Swap rows if needed
        if max_row != col {
            a.swap(col, max_row);
            b.swap(col, max_row);
        }

        // Eliminate column entries below pivot
        let pivot = a[col][col];
        let pivot_row = a[col].clone();
        let pivot_b = b[col];

        for (row_idx, (a_row, b_val)) in a.iter_mut().zip(b.iter_mut()).enumerate() {
            if row_idx > col {
                let factor = a_row[col] / pivot;
                for (a_elem, pivot_elem) in a_row.iter_mut().zip(pivot_row.iter()) {
                    *a_elem -= factor * pivot_elem;
                }
                *b_val -= factor * pivot_b;
            }
        }
    }

    // Back substitution
    let mut x = vec![0.0; n];
    for i in (0..n).rev() {
        let mut sum = b[i];
        for (j, &xj) in x.iter().enumerate().skip(i + 1) {
            sum -= a[i][j] * xj;
        }
        x[i] = sum / a[i][i];
    }

    Ok(x)
}

/// Handle ML Fetch command - retrieves model attributes from cache.
fn handle_ml_fetch(fetch: Fetch) -> SparkResult<MlCommandResult> {
    let obj_ref = fetch.obj_ref.required("object reference")?;
    let methods = fetch.methods;

    debug!("ML Fetch: obj_ref={:?}, methods={:?}", obj_ref.id, methods);

    if methods.is_empty() {
        return Err(SparkError::invalid("Fetch requires at least one method"));
    }

    // Look up model in cache
    let model = {
        let cache = MODEL_CACHE
            .read()
            .map_err(|e| SparkError::internal(format!("Failed to read model cache: {e}")))?;
        cache.get(&obj_ref.id).cloned()
    };

    let model = model
        .ok_or_else(|| SparkError::invalid(format!("Model not found in cache: {}", obj_ref.id)))?;

    let method_name = &methods[0].method;

    let result_literal = match method_name.as_str() {
        "coefficients" => {
            debug!("Returning coefficients: {:?}", model.coefficients);
            let coef_str = format!("{:?}", model.coefficients);
            Literal {
                data_type: None,
                literal_type: Some(LiteralType::String(coef_str)),
            }
        }
        "intercept" => {
            debug!("Returning intercept: {}", model.intercept);
            Literal {
                data_type: None,
                literal_type: Some(LiteralType::Double(model.intercept)),
            }
        }
        "numFeatures" => {
            debug!("Returning numFeatures: {}", model.num_features);
            Literal {
                data_type: None,
                literal_type: Some(LiteralType::Integer(model.num_features as i32)),
            }
        }
        _ => {
            return Err(SparkError::unsupported(format!(
                "ML Fetch method '{}' is not supported",
                method_name
            )));
        }
    };

    Ok(MlCommandResult {
        result_type: Some(ResultType::Param(result_literal)),
    })
}

/// Extract parameters from MlParams into a HashMap.
fn extract_params(params: &Option<MlParams>) -> HashMap<String, ParamValue> {
    let mut result = HashMap::new();
    if let Some(ml_params) = params {
        for (key, literal) in &ml_params.params {
            if let Some(literal_type) = &literal.literal_type {
                let value = match literal_type {
                    LiteralType::Integer(v) => ParamValue::Int(*v),
                    LiteralType::Long(v) => ParamValue::Long(*v),
                    LiteralType::Float(v) => ParamValue::Float(*v as f64),
                    LiteralType::Double(v) => ParamValue::Float(*v),
                    LiteralType::String(v) => ParamValue::String(v.clone()),
                    LiteralType::Boolean(v) => ParamValue::Bool(*v),
                    _ => continue,
                };
                result.insert(key.clone(), value);
            }
        }
    }
    result
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
enum ParamValue {
    Int(i32),
    Long(i64),
    Float(f64),
    String(String),
    Bool(bool),
}

fn get_param_i64(params: &HashMap<String, ParamValue>, key: &str, default: i64) -> i64 {
    params.get(key).map_or(default, |v| match v {
        ParamValue::Int(i) => *i as i64,
        ParamValue::Long(l) => *l,
        _ => default,
    })
}

fn get_param_f64(params: &HashMap<String, ParamValue>, key: &str, default: f64) -> f64 {
    params.get(key).map_or(default, |v| match v {
        ParamValue::Float(f) => *f,
        ParamValue::Int(i) => *i as f64,
        ParamValue::Long(l) => *l as f64,
        _ => default,
    })
}

fn get_param_string(params: &HashMap<String, ParamValue>, key: &str, default: &str) -> String {
    params.get(key).map_or(default.to_string(), |v| match v {
        ParamValue::String(s) => s.clone(),
        _ => default.to_string(),
    })
}
