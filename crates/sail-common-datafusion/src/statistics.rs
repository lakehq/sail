use datafusion::arrow::datatypes::Schema;
use datafusion_common::ScalarValue;
use datafusion_common::stats::{ColumnStatistics, Precision, Statistics};

/// Merge bounds only from files that may contribute non-null values.
pub fn aggregate_statistics<'a>(
    schema: &Schema,
    files: impl IntoIterator<Item = &'a Statistics>,
) -> Statistics {
    let files = files.into_iter().collect::<Vec<_>>();
    let sum = |value: fn(&Statistics) -> &Precision<usize>| {
        files
            .iter()
            .try_fold(0usize, |sum, file| match value(file) {
                Precision::Exact(value) => sum.checked_add(*value),
                _ => None,
            })
            .map(Precision::Exact)
            .unwrap_or(Precision::Absent)
    };
    let column_statistics = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let mut stats = ColumnStatistics::new_unknown();
            stats.null_count = files
                .iter()
                .try_fold(0usize, |sum, file| {
                    let Precision::Exact(nulls) = file.column_statistics.get(index)?.null_count
                    else {
                        return None;
                    };
                    sum.checked_add(nulls)
                })
                .map(Precision::Exact)
                .unwrap_or(Precision::Absent);
            let bound = |minimum: bool| {
                let mut result: Option<ScalarValue> = None;
                let mut exact = true;
                for file in &files {
                    let Some(column) = file.column_statistics.get(index) else {
                        return Precision::Absent;
                    };
                    if let (Precision::Exact(rows), Precision::Exact(nulls)) =
                        (&file.num_rows, &column.null_count)
                        && rows == nulls
                    {
                        continue;
                    }
                    let value = if minimum {
                        &column.min_value
                    } else {
                        &column.max_value
                    };
                    let value = match value {
                        Precision::Exact(value) => value,
                        Precision::Inexact(value) => {
                            exact = false;
                            value
                        }
                        Precision::Absent => return Precision::Absent,
                    };
                    if value.is_null() {
                        return Precision::Absent;
                    }
                    if result.as_ref().is_none_or(|current| {
                        if minimum {
                            value < current
                        } else {
                            value > current
                        }
                    }) {
                        result = Some(value.clone());
                    }
                }
                let Some(value) =
                    result.or_else(|| ScalarValue::try_new_null(field.data_type()).ok())
                else {
                    return Precision::Absent;
                };
                if exact {
                    Precision::Exact(value)
                } else {
                    Precision::Inexact(value)
                }
            };
            stats.min_value = bound(true);
            stats.max_value = bound(false);
            stats
        })
        .collect();
    Statistics {
        num_rows: sum(|stats| &stats.num_rows),
        total_byte_size: sum(|stats| &stats.total_byte_size),
        column_statistics,
    }
}
