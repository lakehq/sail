use std::collections::{BTreeMap, HashMap};
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::common::{DFSchema, DFSchemaRef, Result, not_impl_err, plan_err};
use datafusion_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common_datafusion::datasource::{SinkMode, SourceInfo};
use sail_common_datafusion::lakeprocedure::LakeProcedureCall;
use serde::{Deserialize, Serialize};

use super::arguments::{optional_string, optional_string_map};
use crate::datasource::type_converter::iceberg_schema_to_arrow;
use crate::lake_source::{
    IcebergLakeSource, IcebergReadPurpose, IcebergWriteNode, IcebergWriteNodeOptions,
    load_iceberg_read_table,
};
use crate::physical_plan::file_tasks_exec::IcebergFileTask;
use crate::spec::{DataContentType, DataFile, DataFileFormat, FormatVersion, Literal, Snapshot};
use crate::table::files::{balance_by_size, collect_live_files};

const DEFAULT_TARGET_FILE_SIZE: u64 = 512 * 1024 * 1024;
const DEFAULT_MIN_INPUT_FILES: usize = 5;

#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct RewriteDataFilesPlan {
    expected_snapshot_id: Option<i64>,
    removed_data_file_paths: Vec<String>,
    rewritten_data_files_count: i32,
    rewritten_bytes_count: i64,
}

impl RewriteDataFilesPlan {
    pub fn expected_snapshot_id(&self) -> Option<i64> {
        self.expected_snapshot_id
    }

    pub fn removed_data_file_paths(&self) -> &[String] {
        &self.removed_data_file_paths
    }

    pub fn rewritten_data_files_count(&self) -> i32 {
        self.rewritten_data_files_count
    }

    pub fn rewritten_bytes_count(&self) -> i64 {
        self.rewritten_bytes_count
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub(crate) struct RewriteDataFilesScanNode {
    table_url: String,
    groups: Vec<Vec<IcebergFileTask>>,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
}

impl RewriteDataFilesScanNode {
    fn new(table_url: String, groups: Vec<Vec<IcebergFileTask>>, schema: DFSchemaRef) -> Self {
        Self {
            table_url,
            groups,
            schema,
        }
    }

    pub(crate) fn table_url(&self) -> &str {
        &self.table_url
    }
    pub(crate) fn groups(&self) -> &[Vec<IcebergFileTask>] {
        &self.groups
    }

    pub(crate) fn arrow_schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        Arc::new(self.schema.as_arrow().clone())
    }
}

impl UserDefinedLogicalNodeCore for RewriteDataFilesScanNode {
    fn name(&self) -> &str {
        "IcebergRewriteDataFilesScan"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "IcebergRewriteDataFilesScan: groups={}, selected_files={}",
            self.groups.len(),
            self.groups.iter().map(Vec::len).sum::<usize>()
        )
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if !exprs.is_empty() || !inputs.is_empty() {
            return plan_err!("IcebergRewriteDataFilesScan does not accept expressions or inputs");
        }
        Ok(self.clone())
    }
}

pub(super) async fn plan_rewrite_data_files(
    session: &dyn Session,
    info: SourceInfo,
    call: &LakeProcedureCall,
) -> Result<(LogicalPlan, RewriteDataFilesPlan)> {
    validate_arguments(call)?;
    let SourceInfo {
        paths,
        lakehouse_table,
        options,
        ..
    } = info.clone();
    let [path] = paths.as_slice() else {
        return plan_err!(
            "Iceberg table requires exactly one path, got {}",
            paths.len()
        );
    };
    let (table, _) = load_iceberg_read_table(session, info, IcebergReadPurpose::DataScan).await?;
    let metadata = table.metadata();
    let current_snapshot = metadata.current_snapshot();
    let expected_snapshot_id = current_snapshot.map(Snapshot::snapshot_id);
    let live_files = collect_live_files(
        table.store_context(),
        metadata,
        session.config().target_partitions(),
        |file| Ok(Some(RewriteFile::from(file))),
    )
    .await?;
    let active_delete_files = live_files
        .iter()
        .filter(|file| !matches!(file.content, DataContentType::Data))
        .count();

    let rewrite_options = rewrite_options(call, &metadata.properties)?;
    let selected_files = select_files(&live_files, &rewrite_options)?;

    if !selected_files.is_empty() && active_delete_files > 0 {
        return not_impl_err!(
            "rewrite_data_files does not yet support tables with active delete files"
        );
    }
    if !selected_files.is_empty() && matches!(metadata.format_version, FormatVersion::V3) {
        return not_impl_err!(
            "rewrite_data_files does not yet support Iceberg v3 row-lineage preservation"
        );
    }

    let selected_data_file_paths = selected_files
        .iter()
        .map(|file| file.file_path.clone())
        .collect::<Vec<_>>();
    let rewritten_bytes = selected_files.iter().try_fold(0u64, |total, file| {
        total.checked_add(file.file_size_in_bytes).ok_or_else(|| {
            datafusion_common::DataFusionError::Plan(
                "rewrite_data_files byte count overflow".to_string(),
            )
        })
    })?;
    let groups = rewrite_groups(&selected_files, rewrite_options.target_file_size)?;
    let plan = RewriteDataFilesPlan {
        expected_snapshot_id,
        removed_data_file_paths: selected_data_file_paths.clone(),
        rewritten_data_files_count: i32::try_from(selected_files.len()).map_err(|error| {
            datafusion_common::DataFusionError::Plan(format!(
                "rewrite_data_files file count overflow: {error}"
            ))
        })?,
        rewritten_bytes_count: i64::try_from(rewritten_bytes).map_err(|error| {
            datafusion_common::DataFusionError::Plan(format!(
                "rewrite_data_files byte count overflow: {error}"
            ))
        })?,
    };

    let current_schema = metadata.current_schema().ok_or_else(|| {
        datafusion_common::DataFusionError::Plan(
            "Iceberg table metadata is missing the current schema".to_string(),
        )
    })?;
    let arrow_schema = iceberg_schema_to_arrow(current_schema)?;
    let scan_schema = Arc::new(DFSchema::try_from(arrow_schema)?);
    let scan = LogicalPlan::Extension(Extension {
        node: Arc::new(RewriteDataFilesScanNode::new(
            table.table_url().to_string(),
            groups,
            scan_schema,
        )),
    });
    let partition_by = IcebergLakeSource::partition_columns_from_metadata(&table)?;
    let writer = LogicalPlan::Extension(Extension {
        node: Arc::new(IcebergWriteNode::new(
            Arc::new(scan),
            IcebergWriteNodeOptions {
                path: path.clone(),
                mode: SinkMode::Append,
                partition_by,
                bucket_by: None,
                sort_order: vec![],
                options,
                lakehouse_table,
                defer_commit: true,
                target_file_size: Some(rewrite_options.target_file_size),
                preserve_input_partitions: true,
            },
        )),
    });
    Ok((writer, plan))
}

fn validate_arguments(call: &LakeProcedureCall) -> Result<()> {
    let invocation = &call.invocation;
    if optional_string(invocation, "strategy")?
        .is_some_and(|strategy| !strategy.eq_ignore_ascii_case("binpack"))
    {
        return not_impl_err!("rewrite_data_files currently supports only the binpack strategy");
    }
    if optional_string(invocation, "sort_order")?.is_some() {
        return not_impl_err!(
            "rewrite_data_files sort_order is supported only by the sort strategy"
        );
    }
    if optional_string(invocation, "where")?.is_some() {
        return not_impl_err!("rewrite_data_files where predicates are not implemented yet");
    }
    if optional_string(invocation, "branch")?
        .is_some_and(|branch| !branch.eq_ignore_ascii_case("main"))
    {
        return not_impl_err!("rewrite_data_files currently supports only the main branch");
    }
    Ok(())
}

struct RewriteOptions {
    rewrite_all: bool,
    target_file_size: u64,
    min_file_size: u64,
    max_file_size: u64,
    min_input_files: usize,
}

/// Only retain fields needed for grouping; manifest column metrics stay out of the driver plan.
struct RewriteFile {
    content: DataContentType,
    file_path: String,
    file_format: DataFileFormat,
    partition: Vec<Option<Literal>>,
    record_count: u64,
    file_size_in_bytes: u64,
    partition_spec_id: i32,
}

impl From<DataFile> for RewriteFile {
    fn from(file: DataFile) -> Self {
        Self {
            content: file.content,
            file_path: file.file_path,
            file_format: file.file_format,
            partition: file.partition,
            record_count: file.record_count,
            file_size_in_bytes: file.file_size_in_bytes,
            partition_spec_id: file.partition_spec_id,
        }
    }
}

fn rewrite_options(
    call: &LakeProcedureCall,
    table_properties: &std::collections::HashMap<String, String>,
) -> Result<RewriteOptions> {
    let provided = optional_string_map(&call.invocation, "options")?.unwrap_or_default();
    let mut options = BTreeMap::new();
    for (key, value) in provided {
        let normalized = key.to_ascii_lowercase();
        if options.insert(normalized.clone(), value).is_some() {
            return plan_err!("Duplicate rewrite_data_files option '{normalized}'");
        }
    }
    const SUPPORTED: &[&str] = &[
        "rewrite-all",
        "target-file-size-bytes",
        "min-file-size-bytes",
        "max-file-size-bytes",
        "min-input-files",
    ];
    if let Some(unsupported) = options
        .keys()
        .find(|key| !SUPPORTED.contains(&key.as_str()))
    {
        return not_impl_err!("rewrite_data_files option '{unsupported}' is not implemented yet");
    }

    let target_file_size = option_u64(&options, "target-file-size-bytes")?
        .or_else(|| {
            table_properties
                .get("write.target-file-size-bytes")
                .and_then(|value| value.parse().ok())
        })
        .unwrap_or(DEFAULT_TARGET_FILE_SIZE);
    let min_file_size = option_u64(&options, "min-file-size-bytes")?
        .unwrap_or_else(|| target_file_size.saturating_mul(75) / 100);
    let max_file_size = option_u64(&options, "max-file-size-bytes")?
        .unwrap_or_else(|| target_file_size.saturating_mul(180) / 100);
    let min_input_files = option_u64(&options, "min-input-files")?
        .map(usize::try_from)
        .transpose()
        .map_err(|error| {
            datafusion_common::DataFusionError::Plan(format!(
                "Invalid rewrite_data_files min-input-files: {error}"
            ))
        })?
        .unwrap_or(DEFAULT_MIN_INPUT_FILES);
    let rewrite_all = option_bool(&options, "rewrite-all")?.unwrap_or(false);
    if target_file_size == 0
        || min_input_files == 0
        || min_file_size >= target_file_size
        || target_file_size >= max_file_size
    {
        return plan_err!(
            "Invalid rewrite_data_files sizing options: min-file-size-bytes must be less than the positive target-file-size-bytes, target-file-size-bytes must be less than max-file-size-bytes, and min-input-files must be positive"
        );
    }
    Ok(RewriteOptions {
        rewrite_all,
        target_file_size,
        min_file_size,
        max_file_size,
        min_input_files,
    })
}

fn select_files<'a>(
    live_files: &'a [RewriteFile],
    options: &RewriteOptions,
) -> Result<Vec<&'a RewriteFile>> {
    let mut candidate_groups = HashMap::new();
    for file in live_files
        .iter()
        .filter(|file| matches!(file.content, DataContentType::Data))
        .filter(|file| {
            options.rewrite_all
                || file.file_size_in_bytes < options.min_file_size
                || file.file_size_in_bytes > options.max_file_size
        })
    {
        candidate_groups
            .entry((file.partition_spec_id, file.partition.clone()))
            .or_insert_with(Vec::new)
            .push(file);
    }

    let mut selected = Vec::new();
    for group in candidate_groups.into_values() {
        let input_size = group.iter().try_fold(0u64, |total, file| {
            total.checked_add(file.file_size_in_bytes).ok_or_else(|| {
                datafusion_common::DataFusionError::Plan(
                    "rewrite_data_files group byte count overflow".to_string(),
                )
            })
        })?;
        let enough_input_files = group.len() > 1 && group.len() >= options.min_input_files;
        let enough_content = group.len() > 1 && input_size > options.target_file_size;
        let oversized_content = input_size > options.max_file_size;
        if options.rewrite_all || enough_input_files || enough_content || oversized_content {
            selected.extend(group);
        }
    }
    selected.sort_by(|left, right| left.file_path.cmp(&right.file_path));
    Ok(selected)
}

fn rewrite_groups(files: &[&RewriteFile], target_size: u64) -> Result<Vec<Vec<IcebergFileTask>>> {
    let mut partitions = HashMap::new();
    for file in files {
        if file.file_format != DataFileFormat::Parquet {
            return not_impl_err!("rewrite_data_files supports only Parquet data files");
        }
        partitions
            .entry((file.partition_spec_id, file.partition.clone()))
            .or_insert_with(Vec::new)
            .push(IcebergFileTask {
                path: file.file_path.clone(),
                size: file.file_size_in_bytes,
                records: file.record_count,
                spec_id: file.partition_spec_id,
            });
    }
    let mut groups = Vec::new();
    for files in partitions.into_values() {
        let bytes = files.iter().try_fold(0u64, |total, file| {
            total.checked_add(file.size).ok_or_else(|| {
                datafusion_common::plan_datafusion_err!("rewrite_data_files group size overflow")
            })
        })?;
        let count = usize::try_from(bytes.div_ceil(target_size).max(1))
            .unwrap_or(usize::MAX)
            .min(files.len());
        groups.extend(balance_by_size(files, count, |file| file.size));
    }
    for group in &mut groups {
        group.sort_by(|left, right| left.path.cmp(&right.path));
    }
    groups.sort_by(|left, right| {
        left.first()
            .map(|file| &file.path)
            .cmp(&right.first().map(|file| &file.path))
    });
    if groups.is_empty() {
        groups.push(vec![]);
    }
    Ok(groups)
}

fn option_u64(options: &BTreeMap<String, String>, name: &str) -> Result<Option<u64>> {
    options
        .get(name)
        .map(|value| {
            value.parse::<u64>().map_err(|error| {
                datafusion_common::DataFusionError::Plan(format!(
                    "Invalid rewrite_data_files option '{name}={value}': {error}"
                ))
            })
        })
        .transpose()
}

fn option_bool(options: &BTreeMap<String, String>, name: &str) -> Result<Option<bool>> {
    options
        .get(name)
        .map(|value| match value.to_ascii_lowercase().as_str() {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => plan_err!("Invalid rewrite_data_files option '{name}={value}': expected boolean"),
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::PrimitiveLiteral;

    fn data_file(path: &str, size: u64, partition: i32) -> RewriteFile {
        RewriteFile {
            content: DataContentType::Data,
            file_path: path.to_string(),
            file_format: DataFileFormat::Parquet,
            partition: vec![Some(Literal::Primitive(PrimitiveLiteral::Int(partition)))],
            record_count: 1,
            file_size_in_bytes: size,
            partition_spec_id: 0,
        }
    }

    #[test]
    fn binpack_selection_applies_thresholds_per_partition() -> Result<()> {
        let files = vec![
            data_file("p1-a", 10, 1),
            data_file("p1-b", 10, 1),
            data_file("p2-a", 10, 2),
            data_file("p3-large", 200, 3),
        ];
        let options = RewriteOptions {
            rewrite_all: false,
            target_file_size: 100,
            min_file_size: 75,
            max_file_size: 180,
            min_input_files: 2,
        };

        let selected = select_files(&files, &options)?
            .into_iter()
            .map(|file| file.file_path.as_str())
            .collect::<Vec<_>>();

        assert_eq!(selected, vec!["p1-a", "p1-b", "p3-large"]);
        Ok(())
    }

    #[test]
    fn rewrite_groups_scale_and_keep_partitions_separate() -> Result<()> {
        let files = (0..20)
            .map(|index| data_file(&format!("file-{index:02}"), 60, index % 2))
            .collect::<Vec<_>>();
        let selected = files.iter().collect::<Vec<_>>();
        let groups = rewrite_groups(&selected, 100)?;
        assert_eq!(groups.len(), 12);
        assert_eq!(groups.iter().map(Vec::len).sum::<usize>(), files.len());
        for group in groups {
            let partitions = group
                .iter()
                .map(|task| {
                    files
                        .iter()
                        .find(|file| file.file_path == task.path)
                        .map(|file| file.partition.clone())
                })
                .collect::<std::collections::HashSet<_>>();
            assert_eq!(partitions.len(), 1);
        }
        Ok(())
    }
}
