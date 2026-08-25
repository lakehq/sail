use std::collections::BTreeMap;
use std::fmt::Write;

use serde::{Deserialize, Serialize};

pub const DISTRIBUTED_PLAN_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExplainFormat {
    Text,
    Json,
    Graphviz,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DistributedExecutionMode {
    LocalCluster,
    KubernetesCluster,
}

impl DistributedExecutionMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::LocalCluster => "local-cluster",
            Self::KubernetesCluster => "kubernetes-cluster",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DistributedPlanV1 {
    pub schema_version: u32,
    pub execution_mode: DistributedExecutionMode,
    pub executed: bool,
    pub stages: Vec<DistributedStageV1>,
    pub edges: Vec<DistributedEdgeV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution: Option<DistributedExecutionV1>,
}

impl DistributedPlanV1 {
    pub fn new(
        execution_mode: DistributedExecutionMode,
        stages: Vec<DistributedStageV1>,
        edges: Vec<DistributedEdgeV1>,
    ) -> Self {
        Self {
            schema_version: DISTRIBUTED_PLAN_SCHEMA_VERSION,
            execution_mode,
            executed: false,
            stages,
            edges,
            execution: None,
        }
    }

    pub fn mark_executed(&mut self, job_id: u64, metrics: BTreeMap<String, u64>) {
        self.executed = true;
        self.execution = Some(DistributedExecutionV1 { job_id, metrics });
    }

    pub fn render(&self, format: ExplainFormat, verbose: bool) -> serde_json::Result<String> {
        match format {
            ExplainFormat::Text => Ok(self.render_text(verbose)),
            ExplainFormat::Json => serde_json::to_string_pretty(self),
            ExplainFormat::Graphviz => Ok(self.render_graphviz(verbose)),
        }
    }

    fn render_text(&self, verbose: bool) -> String {
        let mut output = String::new();
        let _ = writeln!(output, "Distributed Plan");
        let _ = writeln!(output, "execution_mode={}", self.execution_mode.as_str());
        let _ = writeln!(output, "executed={}", self.executed);
        if let Some(execution) = &self.execution {
            let _ = writeln!(output, "job_id={}", execution.job_id);
        }

        for stage in &self.stages {
            let _ = writeln!(output);
            let _ = writeln!(output, "=== stage {} ===", stage.stage_id);
            let inputs = self
                .edges
                .iter()
                .filter(|edge| edge.to_stage == stage.stage_id)
                .map(|edge| {
                    format!(
                        "StageInput(stage={}, mode={})",
                        edge.from_stage,
                        edge.exchange_kind.stage_input_mode_name(),
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            let _ = writeln!(output, "inputs=[{inputs}]");
            let _ = writeln!(output, "placement={}", stage.placement.as_str());
            let _ = writeln!(output, "partitions={}", stage.partition_count);
            let _ = writeln!(output, "output_mode={}", stage.output_mode.as_str());
            let _ = writeln!(output, "distribution={}", stage.distribution);
            if verbose {
                let _ = writeln!(output, "{}", stage.operator_tree.trim_end());
            }
        }

        if !self.edges.is_empty() {
            let _ = writeln!(output);
            let _ = writeln!(output, "=== exchanges ===");
            for edge in &self.edges {
                let _ = writeln!(
                    output,
                    "stage {} -> stage {}: kind={}, distribution={}, channels={}",
                    edge.from_stage,
                    edge.to_stage,
                    edge.exchange_kind.as_str(),
                    edge.distribution,
                    edge.channel_count,
                );
            }
        }
        output.trim_end().to_string()
    }

    fn render_graphviz(&self, verbose: bool) -> String {
        fn escape(value: &str) -> String {
            value
                .replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace('\n', "\\l")
        }

        let mut output = String::from("digraph distributed_plan {\n  rankdir=LR;\n");
        for stage in &self.stages {
            let mut label = format!(
                "stage {}\nplacement={}\npartitions={}\noutput_mode={}\ndistribution={}",
                stage.stage_id,
                stage.placement.as_str(),
                stage.partition_count,
                stage.output_mode.as_str(),
                stage.distribution,
            );
            if verbose {
                label.push('\n');
                label.push_str(stage.operator_tree.trim_end());
            }
            let _ = writeln!(
                output,
                "  stage_{} [shape=box, label=\"{}\"];",
                stage.stage_id,
                escape(&label),
            );
        }
        for edge in &self.edges {
            let label = format!(
                "{} / {} / {} channels",
                edge.exchange_kind.as_str(),
                edge.distribution,
                edge.channel_count,
            );
            let _ = writeln!(
                output,
                "  stage_{} -> stage_{} [label=\"{}\"];",
                edge.from_stage,
                edge.to_stage,
                escape(&label),
            );
        }
        output.push('}');
        output
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DistributedStageV1 {
    pub stage_id: usize,
    pub placement: DistributedPlacement,
    pub partition_count: usize,
    pub output_mode: DistributedOutputMode,
    pub distribution: DistributedDistributionV1,
    pub operator_tree: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DistributedPlacement {
    Driver,
    Worker,
}

impl DistributedPlacement {
    fn as_str(self) -> &'static str {
        match self {
            Self::Driver => "driver",
            Self::Worker => "worker",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DistributedOutputMode {
    Pipelined,
    Blocking,
}

impl DistributedOutputMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pipelined => "pipelined",
            Self::Blocking => "blocking",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DistributedEdgeV1 {
    pub from_stage: usize,
    pub to_stage: usize,
    pub exchange_kind: DistributedExchangeKind,
    pub distribution: DistributedDistributionV1,
    pub channel_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DistributedExchangeKind {
    Forward,
    Merge,
    Shuffle,
    Broadcast,
    Rescale,
}

impl DistributedExchangeKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Forward => "forward",
            Self::Merge => "merge",
            Self::Shuffle => "shuffle",
            Self::Broadcast => "broadcast",
            Self::Rescale => "rescale",
        }
    }

    fn stage_input_mode_name(self) -> &'static str {
        match self {
            Self::Forward => "Forward",
            Self::Merge => "Merge",
            Self::Shuffle => "Shuffle",
            Self::Broadcast => "Broadcast",
            Self::Rescale => "Rescale",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DistributedDistributionV1 {
    Hash { keys: Vec<String> },
    RoundRobinBatch,
    RoundRobinRow,
}

impl std::fmt::Display for DistributedDistributionV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Hash { keys } => write!(f, "hash({})", keys.join(", ")),
            Self::RoundRobinBatch => write!(f, "round_robin_batch"),
            Self::RoundRobinRow => write!(f, "round_robin_row"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DistributedExecutionV1 {
    pub job_id: u64,
    pub metrics: BTreeMap<String, u64>,
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn plan() -> DistributedPlanV1 {
        DistributedPlanV1::new(
            DistributedExecutionMode::LocalCluster,
            vec![DistributedStageV1 {
                stage_id: 0,
                placement: DistributedPlacement::Worker,
                partition_count: 2,
                output_mode: DistributedOutputMode::Pipelined,
                distribution: DistributedDistributionV1::RoundRobinBatch,
                operator_tree: "ProjectionExec\n  DataSourceExec".to_string(),
            }],
            vec![],
        )
    }

    #[test]
    fn renders_versioned_json() {
        let output = plan().render(ExplainFormat::Json, false).unwrap();
        let value: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["execution_mode"], "local_cluster");
        assert_eq!(value["stages"][0]["stage_id"], 0);
        assert_eq!(
            value["stages"][0]["distribution"]["kind"],
            "round_robin_batch"
        );
    }

    #[test]
    fn verbose_renderers_include_operator_tree() {
        let plan = plan();
        assert!(
            !plan
                .render(ExplainFormat::Text, false)
                .unwrap()
                .contains("ProjectionExec")
        );
        assert!(
            plan.render(ExplainFormat::Text, true)
                .unwrap()
                .contains("ProjectionExec")
        );
        assert!(
            plan.render(ExplainFormat::Graphviz, true)
                .unwrap()
                .contains("ProjectionExec")
        );
    }

    #[test]
    fn text_renderer_preserves_stage_inputs_without_versioned_title() {
        let mut plan = plan();
        let mut downstream = plan.stages[0].clone();
        downstream.stage_id = 1;
        plan.stages.push(downstream);
        plan.edges.push(DistributedEdgeV1 {
            from_stage: 0,
            to_stage: 1,
            exchange_kind: DistributedExchangeKind::Shuffle,
            distribution: DistributedDistributionV1::Hash {
                keys: vec!["#1@0".to_string()],
            },
            channel_count: 2,
        });

        let output = plan.render(ExplainFormat::Text, false).unwrap();

        assert!(output.starts_with("Distributed Plan\n"));
        assert!(!output.contains("Distributed Plan V1"));
        assert!(output.contains("=== stage 0 ===\ninputs=[]\n"));
        assert!(output.contains("=== stage 1 ===\ninputs=[StageInput(stage=0, mode=Shuffle)]\n"));
        assert!(output.contains("distribution=round_robin_batch"));
    }
}
