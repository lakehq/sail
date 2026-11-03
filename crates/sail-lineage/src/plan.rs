use std::collections::HashSet;

use crate::dataset::LineageDataset;
use crate::event::{InputDataset, OutputDataset};

/// The kind of operation performed by a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LineageOperation {
    #[default]
    Query,
    Write,
    Delete,
    Merge,
}

impl LineageOperation {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            LineageOperation::Query => "query",
            LineageOperation::Write => "write",
            LineageOperation::Delete => "delete",
            LineageOperation::Merge => "merge",
        }
    }
}

/// The lineage information of a resolved plan.
#[derive(Debug, Clone, Default)]
pub struct LineagePlan {
    pub operation: LineageOperation,
    pub inputs: Vec<LineageDataset>,
    pub outputs: Vec<LineageDataset>,
    pub streaming: bool,
}

impl LineagePlan {
    pub fn is_empty(&self) -> bool {
        self.inputs.is_empty() && self.outputs.is_empty()
    }

    /// Returns the deduplicated input and output datasets for OpenLineage events.
    pub(crate) fn datasets(&self, namespace: &str) -> (Vec<InputDataset>, Vec<OutputDataset>) {
        let inputs = dedupe(&self.inputs, namespace)
            .filter_map(|x| x.to_input(namespace))
            .collect();
        let outputs = dedupe(&self.outputs, namespace)
            .filter_map(|x| x.to_output(namespace))
            .collect();
        (inputs, outputs)
    }

    /// Returns a stable job name, so that runs of the same query in an application
    /// are grouped under the same OpenLineage job.
    /// The name of the first output is used as the suffix of the job name.
    /// The table name is preferred over the location, since the location of a managed
    /// table may change when the table is recreated.
    pub(crate) fn job_name(&self, prefix: &str, namespace: &str) -> String {
        let mut name = format!("{prefix}.{}", self.operation.as_str());
        let output = self.outputs.first().and_then(|x| match &x.table {
            Some(table) if !table.is_empty() => Some(table.join(".")),
            _ => x.identifier(namespace).map(|x| x.name),
        });
        if let Some(output) = output {
            let suffix = sanitize(&output);
            if !suffix.is_empty() {
                name.push('.');
                name.push_str(&suffix);
            }
        }
        name
    }
}

/// Removes datasets with duplicate identifiers, keeping the first occurrence.
fn dedupe<'a>(
    datasets: &'a [LineageDataset],
    namespace: &str,
) -> impl Iterator<Item = &'a LineageDataset> {
    let mut seen = HashSet::new();
    datasets
        .iter()
        .filter(move |x| x.identifier(namespace).is_some_and(|x| seen.insert(x)))
}

fn sanitize(name: &str) -> String {
    let name = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>();
    name.split('_')
        .filter(|x| !x.is_empty())
        .collect::<Vec<_>>()
        .join("_")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(location: &str) -> LineageDataset {
        LineageDataset {
            location: Some(location.to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn test_job_name() {
        let plan = LineagePlan {
            operation: LineageOperation::Write,
            inputs: vec![path("/data/in")],
            outputs: vec![path("s3://bucket/warehouse/Out-Table/")],
            streaming: false,
        };
        assert_eq!(
            plan.job_name("my_app", "sail"),
            "my_app.write.warehouse_out_table"
        );

        let plan = LineagePlan {
            inputs: vec![path("/data/in")],
            ..Default::default()
        };
        assert_eq!(plan.job_name("my_app", "sail"), "my_app.query");

        let plan = LineagePlan {
            operation: LineageOperation::Merge,
            outputs: vec![LineageDataset {
                location: Some("/warehouse/t-0f8e2c".to_string()),
                table: Some(vec!["spark_catalog".into(), "db".into(), "t".into()]),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert_eq!(
            plan.job_name("my_app", "sail"),
            "my_app.merge.spark_catalog_db_t"
        );
    }

    #[test]
    fn test_dedupe_datasets() {
        let plan = LineagePlan {
            inputs: vec![path("/data/a"), path("/data/b"), path("file:///data/a/")],
            ..Default::default()
        };
        let (inputs, outputs) = plan.datasets("sail");
        assert!(outputs.is_empty());
        let names = inputs.iter().map(|x| x.name.as_str()).collect::<Vec<_>>();
        assert_eq!(names, vec!["/data/a", "/data/b"]);
    }
}
