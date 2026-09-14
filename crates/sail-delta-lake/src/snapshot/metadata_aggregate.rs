use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::ScalarValue;
use indexmap::IndexMap;

use super::DeltaSnapshot;
use crate::conversion::{ScalarConverter, parse_optional_partition_value};
use crate::schema::arrow_field_physical_name;
use crate::spec::{Add, Stats};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct GroupedCountMetadataRow {
    pub(crate) group_values: Vec<ScalarValue>,
    pub(crate) count: i64,
}

/// A conservative split between files represented by metadata rows and files that must be read.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct GroupedCountMetadata {
    pub(crate) rows: Vec<GroupedCountMetadataRow>,
    pub(crate) residual_file_indices: Vec<usize>,
    pub(crate) metadata_file_count: usize,
    pub(crate) metadata_bytes: u64,
    pub(crate) residual_bytes: u64,
}

#[derive(Debug)]
struct GroupColumn<'a> {
    logical_name: &'a str,
    physical_name: &'a str,
    field: &'a Field,
    partition: bool,
}

enum FileContribution {
    Metadata {
        group_values: Vec<ScalarValue>,
        logical_rows: i64,
    },
    Empty,
    Residual,
}

impl DeltaSnapshot {
    /// Summarize files whose grouping columns are provably constant and return all other files as
    /// a residual scan. `None` means the optimization cannot be applied safely or within the
    /// requested metadata-group budget.
    pub(crate) fn grouped_count_metadata(
        &self,
        group_columns: &[String],
        max_metadata_groups: usize,
    ) -> Option<GroupedCountMetadata> {
        if !self.load_config().require_files || group_columns.is_empty() || max_metadata_groups == 0
        {
            return None;
        }

        let partition_columns = self.metadata().partition_columns();
        let mapping_mode = self.effective_column_mapping_mode();
        let columns = group_columns
            .iter()
            .map(|name| {
                let field = self.schema().field_with_name(name).ok()?;
                let partition = partition_columns.contains(name);
                if !partition && !supports_data_column(field.data_type()) {
                    return None;
                }
                Some(GroupColumn {
                    logical_name: name,
                    physical_name: arrow_field_physical_name(field, mapping_mode),
                    field,
                    partition,
                })
            })
            .collect::<Option<Vec<_>>>()?;

        let mut grouped_counts = IndexMap::<Vec<ScalarValue>, i64>::new();
        let mut residual_file_indices = Vec::new();
        let mut metadata_file_count = 0usize;
        let mut metadata_bytes = 0u64;
        let mut residual_bytes = 0u64;

        for (index, add) in self.adds().iter().enumerate() {
            let file_size = u64::try_from(add.size).ok()?;
            match classify_file(add, &columns) {
                FileContribution::Metadata {
                    group_values,
                    logical_rows,
                } => {
                    if !grouped_counts.contains_key(&group_values)
                        && grouped_counts.len() == max_metadata_groups
                    {
                        return None;
                    }
                    let count = grouped_counts.entry(group_values).or_default();
                    *count = count.checked_add(logical_rows)?;
                    metadata_file_count = metadata_file_count.checked_add(1)?;
                    metadata_bytes = metadata_bytes.checked_add(file_size)?;
                }
                FileContribution::Empty => {
                    metadata_file_count = metadata_file_count.checked_add(1)?;
                    metadata_bytes = metadata_bytes.checked_add(file_size)?;
                }
                FileContribution::Residual => {
                    residual_file_indices.push(index);
                    residual_bytes = residual_bytes.checked_add(file_size)?;
                }
            }
        }

        Some(GroupedCountMetadata {
            rows: grouped_counts
                .into_iter()
                .map(|(group_values, count)| GroupedCountMetadataRow {
                    group_values,
                    count,
                })
                .collect(),
            residual_file_indices,
            metadata_file_count,
            metadata_bytes,
            residual_bytes,
        })
    }
}

fn classify_file(add: &Add, columns: &[GroupColumn<'_>]) -> FileContribution {
    let Some(stats) = add
        .stats
        .as_deref()
        .and_then(|json| Stats::from_json_str(json).ok())
    else {
        return FileContribution::Residual;
    };
    let Ok(physical_rows) = u64::try_from(stats.num_records) else {
        return FileContribution::Residual;
    };
    let deleted_rows = match &add.deletion_vector {
        Some(vector) => match u64::try_from(vector.cardinality) {
            Ok(value) => value,
            Err(_) => return FileContribution::Residual,
        },
        None => 0,
    };
    let Some(logical_rows) = physical_rows.checked_sub(deleted_rows) else {
        return FileContribution::Residual;
    };
    let Ok(logical_rows) = i64::try_from(logical_rows) else {
        return FileContribution::Residual;
    };
    if logical_rows == 0 {
        return FileContribution::Empty;
    }

    let Some(group_values) = columns
        .iter()
        .map(|column| constant_group_value(add, &stats, column, physical_rows, logical_rows))
        .collect::<Option<Vec<_>>>()
    else {
        return FileContribution::Residual;
    };

    FileContribution::Metadata {
        group_values,
        logical_rows,
    }
}

fn constant_group_value(
    add: &Add,
    stats: &Stats,
    column: &GroupColumn<'_>,
    physical_rows: u64,
    logical_rows: i64,
) -> Option<ScalarValue> {
    if column.partition {
        let raw = add
            .partition_values
            .get(column.physical_name)
            .or_else(|| add.partition_values.get(column.logical_name))
            .and_then(Option::as_deref);
        return parse_optional_partition_value(raw, column.field.data_type()).ok();
    }

    let physical_nulls = u64::try_from(stats.null_count_value(column.physical_name)?).ok()?;
    let logical_rows = u64::try_from(logical_rows).ok()?;
    let maximum_nulls = if stats.tight_bounds {
        logical_rows
    } else {
        physical_rows
    };
    if physical_nulls > maximum_nulls {
        return None;
    }
    let logical_nulls = if stats.tight_bounds {
        physical_nulls
    } else if physical_nulls == 0 {
        0
    } else if physical_nulls == physical_rows {
        logical_rows
    } else {
        return None;
    };

    if logical_nulls == logical_rows {
        return ScalarValue::try_new_null(column.field.data_type()).ok();
    }
    if logical_nulls != 0 {
        // A file containing both NULL and a constant non-null value contributes two groups.
        // Keep it in the residual scan until the metadata representation can express both.
        return None;
    }

    let min = stats.min_value(column.physical_name)?;
    let max = stats.max_value(column.physical_name)?;
    // Delta may truncate string bounds, but truncation cannot make both bounds equal. Equality
    // therefore still proves that every non-null value in the file is the same value.
    if min != max {
        return None;
    }
    ScalarConverter::stat_value_to_arrow_scalar_value(min, column.field.data_type())
        .ok()
        .flatten()
        .filter(|value| !value.is_null())
}

fn supports_data_column(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Date32
            | DataType::Date64
    )
}
