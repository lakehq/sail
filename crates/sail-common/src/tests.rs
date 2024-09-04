use std::fs;

use glob;
use serde::{Deserialize, Serialize};

pub const UPDATE_GOLD_DATA_ENV_VAR: &str = "SAIL_UPDATE_GOLD_DATA";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TestData<S, T> {
    tests: Vec<TestCase<S, T>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TestCase<S, T> {
    input: S,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    exception: Option<String>,
    output: Option<TestOutput<T>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub enum TestOutput<T> {
    Success(T),
    Failure(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InputOnlyTestData<S> {
    tests: Vec<InputOnlyTestCase<S>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InputOnlyTestCase<S> {
    input: S,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    exception: Option<String>,
}

impl<S, T> TestData<S, T>
where
    S: Clone,
{
    pub fn map<F, E>(self, f: &F) -> Self
    where
        F: Fn(S) -> Result<T, E>,
        E: std::fmt::Display,
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
                    exception: x.exception,
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
    pub fn map<F, T, E>(self, f: &F) -> TestData<S, T>
    where
        F: Fn(S) -> Result<T, E>,
        E: std::fmt::Display,
    {
        let tests = self
            .tests
            .into_iter()
            .map(|x| TestCase {
                input: x.input,
                exception: x.exception,
                output: None,
            })
            .collect();
        TestData { tests }.map(f)
    }
}

pub fn test_gold_set<S, T, E, F, H>(path: &str, f: F, error: H) -> Result<(), E>
where
    S: Clone + Serialize + for<'de> Deserialize<'de> + PartialEq,
    T: Clone + Serialize + for<'de> Deserialize<'de> + PartialEq,
    E: std::fmt::Display,
    F: Fn(S) -> Result<T, E>,
    H: Fn(String) -> E,
{
    let paths = glob::glob(path).map_err(|e| error(e.to_string()))?;
    for entry in paths {
        let path = entry.map_err(|e| error(e.to_string()))?;
        let content = fs::read_to_string(path.clone()).map_err(|e| error(e.to_string()))?;
        if std::env::var(UPDATE_GOLD_DATA_ENV_VAR).is_ok_and(|v| !v.is_empty()) {
            let expected: InputOnlyTestData<S> = serde_json::from_str(&content).map_err(|e| {
                error(format!(
                    "failed to deserialize inputs in test data file {}: {}",
                    path.display(),
                    e
                ))
            })?;
            let actual = expected.clone().map(&f);
            let output = serde_json::to_string_pretty(&actual).map_err(|e| {
                error(format!(
                    "failed to serialize test data file {}: {}",
                    path.display(),
                    e
                ))
            })?;
            fs::write(path, output + "\n").map_err(|e| error(e.to_string()))?;
        } else {
            let expected: TestData<S, T> = serde_json::from_str(&content).map_err(|e| {
                error(format!(
                    "failed to deserialize test data file {}: {}",
                    path.display(),
                    e
                ))
            })?;
            let actual = expected.clone().map(&f);
            if expected != actual {
                Err(error(format!(
                    "The test data from {} is not up-to-date. Please run 'env {}=1 cargo test' to save the updates.",
                    path.display(),
                    UPDATE_GOLD_DATA_ENV_VAR
                )))?;
            }
        }
    }
    Ok(())
}
