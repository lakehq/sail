// The listing and pruning flow is adapted from DataFusion's listing-table helpers. Sail owns this
// copy because DataFusion's private path parser does not implement Spark/Hive unescaping or the
// default partition sentinel.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::physical_expr::create_physical_expr;
use datafusion_common::{
    Column, DFSchema, Result, ScalarValue, TableReference, assert_or_internal_err,
    exec_datafusion_err,
};
use datafusion_datasource::{ListingTableUrl, PartitionedFile};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::{BinaryExpr, Expr, Operator, lit, utils};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::path::{Path, PathPart};
use object_store::{ObjectMeta, ObjectStore};
use sail_common_datafusion::hive_partition::{
    format_partition_scalar, parse_partition_value, partition_path_segment, unescape_path_name,
};

#[derive(Debug)]
enum PartitionValue {
    Single(ScalarValue),
    Multi,
}

fn populate_partition_values<'a>(
    partition_values: &mut HashMap<&'a str, PartitionValue>,
    filter: &'a Expr,
) {
    if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = filter {
        match op {
            Operator::Eq => {
                let equality = match (left.as_ref(), right.as_ref()) {
                    (Expr::Column(Column { name, .. }), Expr::Literal(value, _))
                    | (Expr::Literal(value, _), Expr::Column(Column { name, .. })) => {
                        Some((name, value))
                    }
                    _ => None,
                };
                if let Some((name, value)) = equality
                    && partition_values
                        .insert(name, PartitionValue::Single(value.clone()))
                        .is_some()
                {
                    partition_values.insert(name, PartitionValue::Multi);
                }
            }
            Operator::And => {
                populate_partition_values(partition_values, left);
                populate_partition_values(partition_values, right);
            }
            _ => {}
        }
    }
}

fn evaluate_partition_prefix(
    partition_cols: &[(String, DataType)],
    filters: &[Expr],
) -> Result<Option<Path>> {
    let mut partition_values = HashMap::new();
    for filter in filters {
        populate_partition_values(&mut partition_values, filter);
    }

    let mut prefix = Path::ROOT;
    let mut count = 0;
    for (column, data_type) in partition_cols {
        let Some(PartitionValue::Single(value)) = partition_values.get(column.as_str()) else {
            break;
        };
        let value = value.cast_to(data_type)?;
        let segment = partition_path_segment(column, &format_partition_scalar(&value)?);
        prefix = prefix.join(PathPart::parse(&segment).map_err(|error| {
            exec_datafusion_err!("invalid Hive partition prefix segment {segment:?}: {error}")
        })?);
        count += 1;
    }

    Ok((count > 0).then_some(prefix))
}

fn filter_partitions(
    file: PartitionedFile,
    filters: &[Expr],
    schema: &DFSchema,
) -> Result<Option<PartitionedFile>> {
    if file.partition_values.is_empty() && !filters.is_empty() {
        return Ok(None);
    }
    if filters.is_empty() {
        return Ok(Some(file));
    }

    let arrays = file
        .partition_values
        .iter()
        .map(ScalarValue::to_array)
        .collect::<Result<Vec<_>>>()?;
    let batch = RecordBatch::try_new(Arc::clone(schema.inner()), arrays)?;
    let filter = utils::conjunction(filters.iter().cloned()).unwrap_or_else(|| lit(true));
    let expression = create_physical_expr(&filter, schema, &ExecutionProps::new())?;
    let matches = expression.evaluate(&batch)?.into_array(1)?;
    let matches = matches.as_boolean();
    Ok((matches.is_valid(0) && matches.value(0)).then_some(file))
}

fn parse_partitions_for_path<'a, I>(
    table_path: &ListingTableUrl,
    file_path: &'a Path,
    partition_columns: I,
) -> Result<Option<Vec<&'a str>>>
where
    I: IntoIterator<Item = &'a str>,
{
    let Some(mut parts) = table_path.strip_prefix(file_path) else {
        return Ok(None);
    };
    let mut values = vec![];
    for expected_column in partition_columns {
        let Some(part) = parts.next() else {
            return Ok(None);
        };
        let Some((encoded_column, encoded_value)) = part.split_once('=') else {
            return Ok(None);
        };
        if unescape_path_name(encoded_column)? != expected_column {
            return Ok(None);
        }
        values.push(encoded_value);
    }
    Ok(Some(values))
}

fn try_into_partitioned_file(
    object_meta: ObjectMeta,
    partition_cols: &[(String, DataType)],
    table_path: &ListingTableUrl,
) -> Result<Option<PartitionedFile>> {
    let columns = partition_cols.iter().map(|(name, _)| name.as_str());
    let Some(parsed) = parse_partitions_for_path(table_path, &object_meta.location, columns)?
    else {
        return Ok(None);
    };
    let partition_values = parsed
        .into_iter()
        .zip(partition_cols)
        .map(|(value, (_, data_type))| parse_partition_value(value, data_type))
        .collect::<Result<Vec<_>>>()?;

    let mut file: PartitionedFile = object_meta.into();
    file.partition_values = partition_values;
    file.table_reference.clone_from(table_path.get_table_ref());
    Ok(Some(file))
}

fn object_meta_to_partitioned_file(
    object_meta: ObjectMeta,
    table_ref: &Option<TableReference>,
) -> PartitionedFile {
    let mut file: PartitionedFile = object_meta.into();
    file.table_reference.clone_from(table_ref);
    file
}

pub async fn pruned_partition_list<'a>(
    context: &'a dyn Session,
    store: &'a dyn ObjectStore,
    table_path: &'a ListingTableUrl,
    filters: &'a [Expr],
    file_extension: &'a str,
    partition_cols: &'a [(String, DataType)],
) -> Result<BoxStream<'a, Result<PartitionedFile>>> {
    let prefix = if partition_cols.is_empty() {
        None
    } else {
        evaluate_partition_prefix(partition_cols, filters)?
    };
    let objects = table_path
        .list_prefixed_files(context, store, prefix, file_extension)
        .await?
        .try_filter(|object_meta| futures::future::ready(object_meta.size > 0));

    if partition_cols.is_empty() {
        assert_or_internal_err!(
            filters.is_empty(),
            "got partition filters for unpartitioned table {table_path}"
        );
        return Ok(objects
            .map_ok(|object_meta| {
                object_meta_to_partitioned_file(object_meta, table_path.get_table_ref())
            })
            .boxed());
    }

    let schema = DFSchema::from_unqualified_fields(
        partition_cols
            .iter()
            .map(|(name, data_type)| Field::new(name, data_type.clone(), true))
            .collect(),
        Default::default(),
    )?;
    Ok(objects
        .try_filter_map(|object_meta| {
            futures::future::ready(try_into_partitioned_file(
                object_meta,
                partition_cols,
                table_path,
            ))
        })
        .try_filter_map(move |file| {
            futures::future::ready(filter_partitions(file, filters, &schema))
        })
        .boxed())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_escaped_partition_names_and_values() -> Result<()> {
        let table_path = ListingTableUrl::parse("memory:///table")?;
        let file_path = Path::parse("table/a%3Ab=a%2Fb/part.parquet")?;
        let values = parse_partitions_for_path(&table_path, &file_path, ["a:b"])?;
        assert_eq!(values, Some(vec!["a%2Fb"]));
        Ok(())
    }

    #[test]
    fn builds_hive_escaped_partition_prefix() -> Result<()> {
        let filters = vec![datafusion_expr::col("part").eq(lit("a=b"))];
        let prefix = evaluate_partition_prefix(&[("part".to_string(), DataType::Utf8)], &filters)?
            .ok_or_else(|| exec_datafusion_err!("partition filter did not produce a prefix"))?;
        assert_eq!(prefix.as_ref(), "part=a%3Db");
        Ok(())
    }
}
