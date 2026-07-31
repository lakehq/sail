use std::sync::Arc;

use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use serde::Serialize;

#[derive(Default, Serialize)]
struct MetricSummary {
    output_rows: u64,
    output_bytes: u64,
    output_batches: u64,
    input_rows: u64,
    input_batches: u64,
    spilled_bytes: u64,
    spilled_rows: u64,
    spill_count: u64,
    peak_memory_bytes: u64,
    elapsed_compute_ns: u64,
    join_time_ns: u64,
    build_time_ns: u64,
    shuffle_read_rows: u64,
    shuffle_read_bytes: u64,
    shuffle_write_rows: u64,
    shuffle_write_bytes: u64,
}

#[derive(Serialize)]
struct OperatorMetrics {
    operator_id: usize,
    name: String,
    partition_count: usize,
    metrics: Vec<MetricSnapshot>,
}

#[derive(Serialize)]
struct MetricSnapshot {
    name: String,
    kind: &'static str,
    value: u64,
    display: String,
}

#[derive(Serialize)]
struct PlanMetrics {
    summary: MetricSummary,
    operators: Vec<OperatorMetrics>,
}

pub fn plan_metrics_json(plan: Arc<dyn ExecutionPlan>) -> Option<String> {
    let mut metrics = PlanMetrics {
        summary: MetricSummary::default(),
        operators: vec![],
    };
    collect_plan_metrics(plan, &mut metrics);
    if metrics.operators.iter().all(|x| x.metrics.is_empty()) {
        None
    } else {
        serde_json::to_string(&metrics).ok()
    }
}

fn collect_plan_metrics(plan: Arc<dyn ExecutionPlan>, output: &mut PlanMetrics) {
    if plan.name() == "TracingExec" {
        let children = plan.children();
        let Some(child) = children.first() else {
            return;
        };
        let operator_id = output.operators.len();
        let mut operator = OperatorMetrics {
            operator_id,
            name: child.name().to_string(),
            partition_count: child.output_partitioning().partition_count(),
            metrics: vec![],
        };
        append_metric_snapshots(&mut operator.metrics, child.metrics());
        append_missing_metric_snapshots(&mut operator.metrics, plan.metrics());
        for snapshot in &operator.metrics {
            update_summary(&mut output.summary, &operator.name, snapshot);
        }
        output.operators.push(operator);
        for grandchild in child.children() {
            collect_plan_metrics(Arc::clone(grandchild), output);
        }
        return;
    }
    let operator_id = output.operators.len();
    let mut operator = OperatorMetrics {
        operator_id,
        name: plan.name().to_string(),
        partition_count: plan.output_partitioning().partition_count(),
        metrics: vec![],
    };
    append_metric_snapshots(&mut operator.metrics, plan.metrics());
    for snapshot in &operator.metrics {
        update_summary(&mut output.summary, &operator.name, snapshot);
    }
    output.operators.push(operator);
    for child in plan.children() {
        collect_plan_metrics(Arc::clone(child), output);
    }
}

fn append_metric_snapshots(
    target: &mut Vec<MetricSnapshot>,
    metrics: Option<datafusion::physical_plan::metrics::MetricsSet>,
) {
    if let Some(metrics) = metrics {
        target.extend(
            metrics
                .aggregate_by_name()
                .sorted_for_display()
                .into_iter()
                .map(|metric| metric_snapshot(metric.value())),
        );
    }
}

fn append_missing_metric_snapshots(
    target: &mut Vec<MetricSnapshot>,
    metrics: Option<datafusion::physical_plan::metrics::MetricsSet>,
) {
    let Some(metrics) = metrics else {
        return;
    };
    for metric in metrics.aggregate_by_name().sorted_for_display() {
        let value = metric.value();
        let name = value.name();
        if !target.iter().any(|snapshot| snapshot.name == name) {
            target.push(metric_snapshot(value));
        }
    }
}

fn metric_snapshot(value: &MetricValue) -> MetricSnapshot {
    let name = value.name().to_string();
    MetricSnapshot {
        name,
        kind: metric_kind(value),
        value: value.as_usize() as u64,
        display: value.to_string(),
    }
}

fn metric_kind(value: &MetricValue) -> &'static str {
    let name = value.name();
    match value {
        MetricValue::OutputRows(_)
        | MetricValue::OutputBatches(_)
        | MetricValue::SpillCount(_)
        | MetricValue::SpilledRows(_) => "count",
        MetricValue::ElapsedCompute(_) | MetricValue::Time { .. } => "time",
        MetricValue::SpilledBytes(_) | MetricValue::OutputBytes(_) => "bytes",
        MetricValue::Count { .. } if metric_name_is_bytes(name) => "bytes",
        MetricValue::Count { .. } if metric_name_is_time(name) => "time",
        MetricValue::Count { .. } => "count",
        MetricValue::CurrentMemoryUsage(_) => "bytes",
        MetricValue::Gauge { .. } if metric_name_is_bytes(name) => "bytes",
        MetricValue::Gauge { .. } => "gauge",
        MetricValue::StartTimestamp(_) | MetricValue::EndTimestamp(_) => "timestamp",
        MetricValue::PruningMetrics { .. } => "pruning",
        MetricValue::Ratio { .. } => "ratio",
        MetricValue::Custom { .. } => "custom",
    }
}

fn metric_name_is_bytes(name: &str) -> bool {
    name == "bytes_scanned"
        || name.ends_with("_bytes")
        || name.ends_with("_size")
        || name.ends_with("_mem_used")
        || name.ends_with("_memory_usage")
        || name.contains("memory")
}

fn metric_name_is_time(name: &str) -> bool {
    name == "elapsed_compute"
        || name.ends_with("_time")
        || name.ends_with("_duration")
        || name.ends_with("_elapsed")
}

fn update_summary(summary: &mut MetricSummary, operator_name: &str, metric: &MetricSnapshot) {
    let value = metric.value;
    match metric.name.as_str() {
        "output_rows" => summary.output_rows = summary.output_rows.saturating_add(value),
        "output_bytes" => summary.output_bytes = summary.output_bytes.saturating_add(value),
        "output_batches" => summary.output_batches = summary.output_batches.saturating_add(value),
        "input_rows" | "build_input_rows" | "left_input_rows" | "right_input_rows" => {
            summary.input_rows = summary.input_rows.saturating_add(value)
        }
        "input_batches" | "build_input_batches" | "left_input_batches" | "right_input_batches" => {
            summary.input_batches = summary.input_batches.saturating_add(value)
        }
        "spilled_bytes" => summary.spilled_bytes = summary.spilled_bytes.saturating_add(value),
        "spilled_rows" => summary.spilled_rows = summary.spilled_rows.saturating_add(value),
        "spill_count" => summary.spill_count = summary.spill_count.saturating_add(value),
        "mem_used" | "build_mem_used" => {
            summary.peak_memory_bytes = summary.peak_memory_bytes.max(value)
        }
        "elapsed_compute" => {
            summary.elapsed_compute_ns = summary.elapsed_compute_ns.saturating_add(value)
        }
        "join_time" => summary.join_time_ns = summary.join_time_ns.saturating_add(value),
        "build_time" => summary.build_time_ns = summary.build_time_ns.saturating_add(value),
        _ => {}
    }
    if operator_name.contains("ShuffleRead") || operator_name.contains("StageInput") {
        if metric.name.ends_with("rows") {
            summary.shuffle_read_rows = summary.shuffle_read_rows.saturating_add(value);
        } else if metric.kind == "bytes" || metric.name.ends_with("bytes") {
            summary.shuffle_read_bytes = summary.shuffle_read_bytes.saturating_add(value);
        }
    }
    if operator_name.contains("ShuffleWrite") {
        if metric.name.ends_with("rows") {
            summary.shuffle_write_rows = summary.shuffle_write_rows.saturating_add(value);
        } else if metric.kind == "bytes" || metric.name.ends_with("bytes") {
            summary.shuffle_write_bytes = summary.shuffle_write_bytes.saturating_add(value);
        }
    }
}
