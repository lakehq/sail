use crate::error::{SparkError, SparkResult};
use crate::executor::execute_plan;
use crate::plan::from_spark_relation;
use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use glob;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use tonic::codegen::tokio_stream::StreamExt;

pub(crate) const UPDATE_GOLD_DATA_ENV_VAR: &str = "SPARK_UPDATE_GOLD_DATA";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TestData<S, T> {
    tests: Vec<TestCase<S, T>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TestCase<S, T> {
    input: S,
    output: Option<TestOutput<T>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) enum TestOutput<T> {
    Success(T),
    Failure(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct InputOnlyTestData<S> {
    tests: Vec<InputOnlyTestCase<S>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct InputOnlyTestCase<S> {
    input: S,
}

impl<S, T> TestData<S, T>
where
    S: Clone,
{
    pub(crate) fn map<F>(self, f: &F) -> Self
    where
        F: Fn(S) -> SparkResult<T>,
    {
        let tests = self
            .tests
            .into_iter()
            .map(|x| {
                let output = match f(x.input.clone()) {
                    Ok(v) => TestOutput::Success(v),
                    Err(e) => TestOutput::Failure(e.to_string()),
                };
                TestCase {
                    input: x.input,
                    output: Some(output),
                }
            })
            .collect();
        TestData { tests }
    }
}

impl<S> InputOnlyTestData<S>
where
    S: Clone,
{
    pub(crate) fn map<F, T>(self, f: &F) -> TestData<S, T>
    where
        F: Fn(S) -> SparkResult<T>,
    {
        let tests = self
            .tests
            .into_iter()
            .map(|x| TestCase {
                input: x.input,
                output: None,
            })
            .collect();
        TestData { tests }.map(f)
    }
}

pub(crate) fn test_gold_set<S, T, F>(path: &str, f: F) -> SparkResult<()>
where
    S: Clone + Serialize + for<'de> Deserialize<'de> + PartialEq,
    T: Clone + Serialize + for<'de> Deserialize<'de> + PartialEq,
    F: Fn(S) -> SparkResult<T>,
{
    let paths = glob::glob(path).map_err(|e| SparkError::internal(e.to_string()))?;
    for entry in paths {
        let path = entry.map_err(|e| SparkError::internal(e.to_string()))?;
        let content = fs::read_to_string(path.clone())?;
        if std::env::var(UPDATE_GOLD_DATA_ENV_VAR).is_ok_and(|v| !v.is_empty()) {
            let expected: InputOnlyTestData<S> = serde_json::from_str(&content).map_err(|e| {
                SparkError::internal(format!(
                    "failed to deserialize inputs in test data file {}: {}",
                    path.display(),
                    e
                ))
            })?;
            let actual = expected.clone().map(&f);
            let output = serde_json::to_string_pretty(&actual).map_err(|e| {
                SparkError::internal(format!(
                    "failed to serialize test data file {}: {}",
                    path.display(),
                    e
                ))
            })?;
            fs::write(path, output + "\n")?;
        } else {
            let expected: TestData<S, T> = serde_json::from_str(&content).map_err(|e| {
                SparkError::internal(format!(
                    "failed to deserialize test data file {}: {}",
                    path.display(),
                    e
                ))
            })?;
            let actual = expected.clone().map(&f);
            if expected != actual {
                Err(SparkError::internal(format!(
                    "The test data from {} is not up-to-date. Please run 'env {}=1 cargo test' to save the updates.",
                    path.display(),
                    UPDATE_GOLD_DATA_ENV_VAR
                )))?;
            }
        }
    }
    Ok(())
}

pub(crate) async fn execute_query(
    ctx: &SessionContext,
    query: &str,
) -> SparkResult<Vec<RecordBatch>> {
    let relation = crate::spark::connect::Relation {
        common: None,
        rel_type: Some(crate::spark::connect::relation::RelType::Sql(
            crate::spark::connect::Sql {
                query: query.to_string(),
                args: HashMap::new(),
                pos_args: vec![],
            },
        )),
    };
    let plan = from_spark_relation(&ctx, relation.try_into()?).await?;
    let mut stream = execute_plan(&ctx, &plan).await?;
    let mut output = vec![];
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        output.push(batch);
    }
    Ok(output)
}
