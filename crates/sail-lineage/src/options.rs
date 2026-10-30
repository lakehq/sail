use log::warn;
use uuid::Uuid;

/// A run that the lineage run is part of, e.g. an Airflow task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunReference {
    pub namespace: String,
    pub name: String,
    pub run_id: Uuid,
}

/// A tag of an OpenLineage job or run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineageTag {
    pub key: String,
    pub value: String,
    pub source: String,
}

/// The tag source reported for tags set via Spark configuration,
/// which matches the OpenLineage Spark integration.
const CONFIG_TAG_SOURCE: &str = "CONFIG";

/// Lineage options provided by the client session.
#[derive(Debug, Clone, Default)]
pub struct LineageSessionOptions {
    /// The prefix of the job name.
    pub app_name: Option<String>,
    /// The job namespace, which overrides the server configuration.
    pub namespace: Option<String>,
    pub parent: Option<RunReference>,
    pub root_parent: Option<RunReference>,
    pub job_tags: Vec<LineageTag>,
    pub run_tags: Vec<LineageTag>,
}

impl LineageSessionOptions {
    /// Reads the options from Spark configuration, using the same keys as
    /// the OpenLineage Spark integration, so that orchestrators (e.g. Airflow)
    /// that inject these keys can link their runs to Sail runs.
    pub fn from_spark_config(get: impl Fn(&str) -> Option<String>) -> Self {
        let get = |key: &str| get(key).filter(|x| !x.is_empty());
        let run = |prefix: &str| {
            let namespace = get(&format!("spark.openlineage.{prefix}JobNamespace"))?;
            let name = get(&format!("spark.openlineage.{prefix}JobName"))?;
            let run_id = get(&format!("spark.openlineage.{prefix}RunId"))?;
            match Uuid::parse_str(&run_id) {
                Ok(run_id) => Some(RunReference {
                    namespace,
                    name,
                    run_id,
                }),
                Err(e) => {
                    warn!("ignored invalid OpenLineage {prefix} run ID {run_id}: {e}");
                    None
                }
            }
        };
        Self {
            app_name: get("spark.app.name"),
            namespace: get("spark.openlineage.namespace"),
            parent: run("parent"),
            root_parent: run("rootParent"),
            job_tags: parse_tags(get("spark.openlineage.job.tags").as_deref()),
            run_tags: parse_tags(get("spark.openlineage.run.tags").as_deref()),
        }
    }
}

/// Parses tags in the format of the OpenLineage Spark integration,
/// where tags are separated by `;` and each tag has the form `key:value[:source]`.
fn parse_tags(value: Option<&str>) -> Vec<LineageTag> {
    let Some(value) = value else {
        return vec![];
    };
    value
        .split(';')
        .map(str::trim)
        .filter(|x| !x.is_empty())
        .filter_map(|tag| {
            let mut parts = tag.splitn(3, ':').map(str::trim);
            match (parts.next(), parts.next(), parts.next()) {
                (Some(key), Some(value), source) if !key.is_empty() => Some(LineageTag {
                    key: key.to_string(),
                    value: value.to_string(),
                    source: source
                        .filter(|x| !x.is_empty())
                        .unwrap_or(CONFIG_TAG_SOURCE)
                        .to_string(),
                }),
                _ => {
                    warn!("ignored invalid OpenLineage tag {tag}: expected key:value[:source]");
                    None
                }
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn options(items: &[(&str, &str)]) -> LineageSessionOptions {
        let config: HashMap<String, String> = items
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        LineageSessionOptions::from_spark_config(|key| config.get(key).cloned())
    }

    #[test]
    fn test_from_spark_config() {
        let run_id = "01890cb8-f7a4-7c8d-b1a5-2b6f6d8e9a10";
        let options = options(&[
            ("spark.app.name", "app"),
            ("spark.openlineage.namespace", "ns"),
            ("spark.openlineage.parentJobNamespace", "airflow"),
            ("spark.openlineage.parentJobName", "dag.task"),
            ("spark.openlineage.parentRunId", run_id),
            // The root parent is incomplete, so it is ignored.
            ("spark.openlineage.rootParentJobName", "dag"),
        ]);
        assert_eq!(options.app_name.as_deref(), Some("app"));
        assert_eq!(options.namespace.as_deref(), Some("ns"));
        assert_eq!(
            options.parent,
            Some(RunReference {
                namespace: "airflow".to_string(),
                name: "dag.task".to_string(),
                run_id: Uuid::parse_str(run_id).unwrap_or_default(),
            })
        );
        assert_eq!(options.root_parent, None);
    }

    #[test]
    fn test_tags() {
        let options = options(&[
            ("spark.openlineage.job.tags", "dag_id:my_dag; task_id:load"),
            (
                "spark.openlineage.run.tags",
                "try_number:1;invalid;:no_key;user:etl:AIRFLOW;",
            ),
        ]);
        let tag = |key: &str, value: &str, source: &str| LineageTag {
            key: key.to_string(),
            value: value.to_string(),
            source: source.to_string(),
        };
        assert_eq!(
            options.job_tags,
            vec![
                tag("dag_id", "my_dag", "CONFIG"),
                tag("task_id", "load", "CONFIG")
            ]
        );
        assert_eq!(
            options.run_tags,
            vec![
                tag("try_number", "1", "CONFIG"),
                tag("user", "etl", "AIRFLOW")
            ]
        );
        assert!(
            LineageSessionOptions::from_spark_config(|_| None)
                .job_tags
                .is_empty()
        );
    }

    #[test]
    fn test_invalid_run_id() {
        let options = options(&[
            ("spark.openlineage.parentJobNamespace", "airflow"),
            ("spark.openlineage.parentJobName", "dag.task"),
            ("spark.openlineage.parentRunId", "not-a-uuid"),
        ]);
        assert_eq!(options.parent, None);
    }
}
