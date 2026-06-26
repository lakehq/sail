// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use sail_catalog::error::{CatalogError, CatalogResult};
use serde_json::Value;

use crate::models;

pub fn completed_planning_result_from_values(
    plan_tasks: Vec<String>,
    file_scan_tasks: Vec<Value>,
    delete_files: Vec<Value>,
) -> CatalogResult<models::CompletedPlanningResult> {
    let mut result = models::CompletedPlanningResult::new(models::PlanStatus::Completed);
    result.plan_tasks = non_empty(plan_tasks);
    result.file_scan_tasks = parse_optional_values(file_scan_tasks, "file-scan-tasks")?;
    result.delete_files = parse_delete_files(delete_files)?;
    Ok(result)
}

pub fn completed_planning_with_id_result_from_values(
    plan_id: Option<String>,
    plan_tasks: Vec<String>,
    file_scan_tasks: Vec<Value>,
    delete_files: Vec<Value>,
) -> CatalogResult<models::CompletedPlanningWithIdResult> {
    let mut result = models::CompletedPlanningWithIdResult::new(models::PlanStatus::Completed);
    result.plan_id = plan_id;
    result.plan_tasks = non_empty(plan_tasks);
    result.file_scan_tasks = parse_optional_values(file_scan_tasks, "file-scan-tasks")?;
    result.delete_files = parse_delete_files(delete_files)?;
    Ok(result)
}

pub fn fetch_scan_tasks_result_from_values(
    plan_tasks: Vec<String>,
    file_scan_tasks: Vec<Value>,
    delete_files: Vec<Value>,
) -> CatalogResult<models::FetchScanTasksResult> {
    let mut result = models::FetchScanTasksResult::new();
    result.plan_tasks = non_empty(plan_tasks);
    result.file_scan_tasks = parse_optional_values(file_scan_tasks, "file-scan-tasks")?;
    result.delete_files = parse_delete_files(delete_files)?;
    Ok(result)
}

fn non_empty<T>(values: Vec<T>) -> Option<Vec<T>> {
    (!values.is_empty()).then_some(values)
}

fn parse_optional_values<T: serde::de::DeserializeOwned>(
    values: Vec<Value>,
    label: &str,
) -> CatalogResult<Option<Vec<T>>> {
    if values.is_empty() {
        return Ok(None);
    }
    serde_json::from_value(Value::Array(values))
        .map(Some)
        .map_err(|err| {
            CatalogError::External(format!("invalid Iceberg REST {label} payload: {err}"))
        })
}

fn parse_delete_files(values: Vec<Value>) -> CatalogResult<Option<Vec<models::DeleteFile>>> {
    if values.is_empty() {
        return Ok(None);
    }
    values
        .into_iter()
        .map(parse_delete_file)
        .collect::<CatalogResult<Vec<_>>>()
        .map(Some)
}

fn parse_delete_file(value: Value) -> CatalogResult<models::DeleteFile> {
    match value.get("content").and_then(Value::as_str) {
        Some("position-deletes") => serde_json::from_value(value)
            .map(|file| models::DeleteFile::PositionDeletes(Box::new(file)))
            .map_err(delete_file_error),
        Some("equality-deletes") => serde_json::from_value(value)
            .map(|file| models::DeleteFile::EqualityDeletes(Box::new(file)))
            .map_err(delete_file_error),
        Some(content) => Err(CatalogError::External(format!(
            "invalid Iceberg REST delete-files payload: unsupported content {content}"
        ))),
        None => Err(CatalogError::External(
            "invalid Iceberg REST delete-files payload: missing content".to_string(),
        )),
    }
}

fn delete_file_error(err: serde_json::Error) -> CatalogError {
    CatalogError::External(format!("invalid Iceberg REST delete-files payload: {err}"))
}

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn builds_completed_planning_result_from_plan_tokens() {
        let result = completed_planning_result_from_values(
            vec!["task-1".to_string(), "task-2".to_string()],
            Vec::new(),
            Vec::new(),
        )
        .expect("planning result");

        assert_eq!(result.status, models::PlanStatus::Completed);
        assert_eq!(
            result.plan_tasks,
            Some(vec!["task-1".to_string(), "task-2".to_string()])
        );
        assert!(result.file_scan_tasks.is_none());
        assert!(result.delete_files.is_none());
    }

    #[test]
    fn builds_fetch_scan_tasks_result_from_concrete_delete_files() {
        let data_file = json!({
            "content": "data",
            "file-path": "file:///tmp/events/data-1.parquet",
            "file-format": "parquet",
            "spec-id": 0,
            "partition": [],
            "file-size-in-bytes": 100,
            "record-count": 10
        });
        let delete_file = json!({
            "content": "position-deletes",
            "file-path": "file:///tmp/events/delete-1.parquet",
            "file-format": "parquet",
            "spec-id": 0,
            "partition": [],
            "file-size-in-bytes": 50,
            "record-count": 1
        });

        let result = fetch_scan_tasks_result_from_values(
            vec!["child-task".to_string()],
            vec![json!({
                "data-file": data_file,
                "delete-file-references": [0]
            })],
            vec![delete_file],
        )
        .expect("fetch scan tasks result");

        assert_eq!(result.plan_tasks, Some(vec!["child-task".to_string()]));
        assert_eq!(result.file_scan_tasks.as_ref().unwrap().len(), 1);
        assert_eq!(
            result.file_scan_tasks.as_ref().unwrap()[0].delete_file_references,
            Some(vec![0])
        );
        assert!(matches!(
            result.delete_files.as_ref().unwrap()[0],
            models::DeleteFile::PositionDeletes(_)
        ));
    }
}
