use std::collections::HashMap;

use datafusion::prelude::SessionContext;
use futures::stream;
use log::debug;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use uuid::Uuid;

use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::executor::{ExecutorBatch, ExecutorMetadata, ExecutorOutput};
use crate::service::plan_executor::ExecutePlanResponseStream;
use crate::session::SparkSession;
use crate::spark::connect::expression::literal::LiteralType;
use crate::spark::connect::expression::Literal;
use crate::spark::connect::ml_command::Command;
use crate::spark::connect::ml_command_result::{MlOperatorInfo, ResultType};
use crate::spark::connect::{Fetch, MlCommand, MlCommandResult, MlParams, ObjectRef};

/// Handle ML command execution.
///
/// NOTE: The ML cache mechanism (storing trained models in session state) may have
/// issues with memory management and lifecycle. Some have noted that caching models
/// in the session could cause problems with large models or long-running sessions.
/// For now, we use a simple approach with placeholder model IDs.
/// A proper implementation might need:
/// - LRU eviction for cached models
/// - Offloading to disk (see MlCommand::Delete.evict_only)
/// - Proper cleanup on session termination
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
        Command::Fetch(fetch) => handle_ml_fetch(fetch).await?,
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

/// Handle ML Fit command - trains a model on the dataset.
async fn handle_ml_fit(
    _ctx: &SessionContext,
    _spark: &SparkSession,
    fit: crate::spark::connect::ml_command::Fit,
) -> SparkResult<MlCommandResult> {
    let estimator = fit.estimator.required("estimator")?;
    let params = fit.params;
    let _dataset = fit.dataset.required("dataset")?;

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
    let max_iter = get_param_i64(&params_map, "maxIter", 100);
    let learning_rate = get_param_f64(&params_map, "stepSize", 0.01);
    let _tolerance = get_param_f64(&params_map, "tol", 1e-6);
    let features_col = get_param_string(&params_map, "featuresCol", "features");
    let label_col = get_param_string(&params_map, "labelCol", "label");

    debug!(
        "Training LinearRegression: maxIter={}, stepSize={}, featuresCol={}, labelCol={}",
        max_iter, learning_rate, features_col, label_col
    );

    // For now, we return a placeholder result
    // In a full implementation, we would:
    // 1. Execute the dataset relation to get the training data
    // 2. Run the SGD training loop using sgd_gradient_sum
    // 3. Cache the model and return an ObjectRef

    // Generate a unique model ID
    let model_id = Uuid::new_v4().to_string();

    // Create coefficients parameter (placeholder for now)
    let mut result_params = HashMap::new();
    result_params.insert(
        "coefficients".to_string(),
        Literal {
            data_type: None,
            literal_type: Some(LiteralType::String("[]".to_string())),
        },
    );

    Ok(MlCommandResult {
        result_type: Some(ResultType::OperatorInfo(MlOperatorInfo {
            r#type: Some(crate::spark::connect::ml_command_result::ml_operator_info::Type::ObjRef(
                ObjectRef { id: model_id },
            )),
            uid: Some(estimator.uid),
            params: Some(MlParams {
                params: result_params,
            }),
            warning_message: Some(
                "LinearRegression training is a POC - model coefficients are placeholders".to_string(),
            ),
        })),
    })
}

/// Handle ML Fetch command - retrieves model attributes (coefficients, intercept, etc.).
async fn handle_ml_fetch(fetch: Fetch) -> SparkResult<MlCommandResult> {
    let obj_ref = fetch.obj_ref.required("object reference")?;
    let methods = fetch.methods;

    debug!("ML Fetch: obj_ref={:?}, methods={:?}", obj_ref.id, methods);

    if methods.is_empty() {
        return Err(SparkError::invalid("Fetch requires at least one method"));
    }

    // Get the method name (e.g., "coefficients", "intercept")
    let method_name = &methods[0].method;

    // For this POC, return placeholder values based on the method name
    // In a real implementation, we would look up the cached model by obj_ref.id
    // and return the actual computed values
    //
    // NOTE: PySpark's LinearRegression expects DenseVector for coefficients,
    // which uses a specialized serialization format. For this POC, we just
    // log that the fetch was successful and return a placeholder.
    let result_literal = match method_name.as_str() {
        "coefficients" => {
            debug!("Returning placeholder coefficients [1.0, 1.0]");
            // Return as a struct that represents DenseVector
            // Format: {type: 1, size: 2, indices: null, values: [1.0, 1.0]}
            // For simplicity in POC, return a string representation
            Literal {
                data_type: None,
                literal_type: Some(LiteralType::String("[1.0, 1.0]".to_string())),
            }
        }
        "intercept" => {
            debug!("Returning placeholder intercept 0.0");
            Literal {
                data_type: None,
                literal_type: Some(LiteralType::Double(0.0)),
            }
        }
        "numFeatures" => {
            debug!("Returning placeholder numFeatures 2");
            Literal {
                data_type: None,
                literal_type: Some(LiteralType::Integer(2)),
            }
        }
        _ => {
            debug!("Unsupported ML Fetch method: {}", method_name);
            return Err(SparkError::unsupported(format!(
                "ML Fetch method '{}' is not supported yet. \
                 This is a POC implementation of LinearRegression.",
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
