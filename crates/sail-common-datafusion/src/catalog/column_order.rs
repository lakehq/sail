use std::collections::HashMap;

use datafusion_common::{Result, plan_err};
use sail_common::utils::string::spark_case_insensitive_name_eq;

use super::{CatalogPartitionField, PartitionTransform};

/// Parquet exposes data columns first, then identity partition columns in partition-spec order.
/// Exact partition identities take precedence over case-insensitive fallback, which must be
/// unambiguous. This also supports provider metadata that retains the original reference casing.
// [CREDIT]: https://github.com/apache/spark/blob/v4.3.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/rules.scala#L266-L308
pub fn normalize_parquet_column_order<T>(
    format: &str,
    columns: &mut [T],
    partition_by: &[CatalogPartitionField],
    case_sensitive: bool,
    name: impl Fn(&T) -> &str,
) -> Result<()> {
    if !format.eq_ignore_ascii_case("parquet")
        || columns.is_empty()
        || partition_by.is_empty()
        || partition_by.iter().any(|partition| {
            !matches!(
                partition.transform,
                None | Some(PartitionTransform::Identity)
            )
        })
    {
        return Ok(());
    }
    let mut positions = HashMap::new();
    for (index, partition) in partition_by.iter().enumerate() {
        let exact = columns
            .iter()
            .any(|column| name(column) == partition.column);
        let mut matches = columns.iter().filter(|column| {
            if exact || case_sensitive {
                name(column) == partition.column
            } else {
                spark_case_insensitive_name_eq(name(column), &partition.column)
            }
        });
        let Some(column) = matches.next() else {
            return plan_err!("partition column not found: {}", partition.column);
        };
        if matches.next().is_some() {
            return plan_err!("ambiguous partition column: {}", partition.column);
        }
        if positions
            .insert(name(column).to_string(), index + 1)
            .is_some()
        {
            return plan_err!("duplicate partition column: {}", partition.column);
        }
    }
    // Stable sorting moves whole column records and retains data-column declaration order.
    columns.sort_by_key(|column| positions.get(name(column)).copied().unwrap_or(0));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_partition_identities_preserve_case_distinct_columns() -> Result<()> {
        for case_sensitive in [false, true] {
            let mut columns = ["p", "P", "msg"];
            let partition_by = [CatalogPartitionField {
                column: "p".into(),
                transform: None,
            }];
            normalize_parquet_column_order(
                "parquet",
                &mut columns,
                &partition_by,
                case_sensitive,
                |c| c,
            )?;
            assert_eq!(columns, ["P", "msg", "p"]);
        }
        Ok(())
    }

    #[test]
    fn fallback_requires_one_matching_column() -> Result<()> {
        let partition_by = [CatalogPartitionField {
            column: "part".into(),
            transform: None,
        }];
        let mut columns = ["PART", "msg"];
        normalize_parquet_column_order("parquet", &mut columns, &partition_by, false, |c| c)?;
        assert_eq!(columns, ["msg", "PART"]);
        assert!(
            normalize_parquet_column_order("parquet", &mut columns, &partition_by, true, |c| c)
                .is_err()
        );

        let mut ambiguous = ["PART", "Part", "msg"];
        assert!(
            normalize_parquet_column_order("parquet", &mut ambiguous, &partition_by, false, |c| c)
                .is_err()
        );
        assert_eq!(ambiguous, ["PART", "Part", "msg"]);
        Ok(())
    }

    #[test]
    fn parquet_column_order_preserves_features_and_partition_spec_order() -> Result<()> {
        let original = ["q", "a", "p", "b"].map(|name| (name, format!("metadata for {name}")));
        let partition_by = [
            CatalogPartitionField {
                column: "P".into(),
                transform: Some(PartitionTransform::Identity),
            },
            CatalogPartitionField {
                column: "q".into(),
                transform: None,
            },
        ];
        for format in ["parquet", "PARQUET", "delta", "iceberg", "csv"] {
            let mut columns = original.clone();
            normalize_parquet_column_order(format, &mut columns, &partition_by, false, |c| c.0)?;
            let expected = if format.eq_ignore_ascii_case("parquet") {
                [1, 3, 2, 0].map(|index| original[index].clone())
            } else {
                original.clone()
            };
            assert_eq!(columns, expected);
            normalize_parquet_column_order(format, &mut columns, &partition_by, false, |c| c.0)?;
            assert_eq!(columns, expected);
        }
        let mut columns = original.clone();
        normalize_parquet_column_order("parquet", &mut columns, &[], false, |c| c.0)?;
        assert_eq!(columns, original);
        let transformed = [CatalogPartitionField {
            column: "q".into(),
            transform: Some(PartitionTransform::Bucket(4)),
        }];
        normalize_parquet_column_order("parquet", &mut columns, &transformed, false, |c| c.0)?;
        assert_eq!(columns, original);
        Ok(())
    }
}
