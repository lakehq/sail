use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::catalog::Session;
use datafusion::datasource::listing::helpers::{
    evaluate_partition_prefix, filter_partitioned_file,
};
use datafusion::logical_expr::Expr;
use datafusion_common::{DFSchema, Result, ScalarValue};
use datafusion_datasource::{ListingTableUrl, PartitionedFile};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStore;
use object_store::path::Path;
use percent_encoding::percent_decode_str;

pub(crate) const HIVE_DEFAULT_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

/// DataFusion's listing assumes partition values are never null. Decode Hive's
/// null marker before applying typed partition filters to files written by Spark
/// or Sail.
pub(crate) async fn pruned_partition_list<'a>(
    ctx: &'a dyn Session,
    store: &'a dyn ObjectStore,
    table_path: &'a ListingTableUrl,
    filters: &'a [Expr],
    file_extension: &'a str,
    partition_cols: &'a [(String, DataType)],
) -> Result<BoxStream<'a, Result<PartitionedFile>>> {
    if partition_cols.is_empty() {
        return datafusion::datasource::listing::helpers::pruned_partition_list(
            ctx,
            store,
            table_path,
            filters,
            file_extension,
            partition_cols,
        )
        .await;
    }
    // Floating-point values have multiple equivalent directory spellings, e.g.
    // `3` and `3.0`. Do not narrow listing to one spelling of a typed value.
    let prefix_columns = partition_cols
        .iter()
        .take_while(|(name, data_type)| {
            // Escaped column names cannot be used by DataFusion's prefix builder.
            !name.chars().any(|c| !c.is_alphanumeric() && c != '_')
                && matches!(
                    data_type,
                    DataType::Utf8
                        | DataType::LargeUtf8
                        | DataType::Utf8View
                        | DataType::Boolean
                        | DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::UInt8
                        | DataType::UInt16
                        | DataType::UInt32
                        | DataType::UInt64
                )
        })
        .count();
    let prefix = evaluate_partition_prefix(&partition_cols[..prefix_columns], filters)
        .and_then(unescaped_partition_prefix);
    let objects = table_path
        .list_prefixed_files(ctx, store, prefix, file_extension)
        .await?;
    let schema = DFSchema::from_unqualified_fields(
        partition_cols
            .iter()
            .map(|(name, data_type)| Field::new(name, data_type.clone(), true))
            .collect(),
        Default::default(),
    )?;
    // The upstream helper reads the boolean value without checking validity.
    let filters = filters
        .iter()
        .cloned()
        .map(Expr::is_true)
        .collect::<Vec<_>>();
    Ok(objects
        .try_filter_map(move |object| {
            let result = (|| {
                if object.size == 0 {
                    return Ok(None);
                }
                let Some(mut segments) = table_path.strip_prefix(&object.location) else {
                    return Ok(None);
                };
                let mut values = Vec::with_capacity(partition_cols.len());
                for (expected_name, data_type) in partition_cols {
                    let Some((name, value)) = segments.next().and_then(|s| s.split_once('='))
                    else {
                        return Ok(None);
                    };
                    let name = percent_decode_str(name).decode_utf8_lossy();
                    if name != expected_name.as_str() {
                        return Ok(None);
                    }
                    let value = percent_decode_str(value).decode_utf8_lossy();
                    values.push(if value == HIVE_DEFAULT_PARTITION {
                        ScalarValue::try_from(data_type)?
                    } else {
                        ScalarValue::try_from_string(value.into_owned(), data_type)?
                    });
                }
                drop(segments);
                let file = PartitionedFile::new_from_meta(object)
                    .with_partition_values(values)
                    .with_table_reference(table_path.get_table_ref().clone());
                filter_partitioned_file(file, &filters, &schema)
            })();
            futures::future::ready(result)
        })
        .boxed())
}

fn unescaped_partition_prefix(prefix: Path) -> Option<Path> {
    // Hive, DataFusion and object_store escape different sets of characters.
    // Only retain leading components whose spelling is the same in all three.
    let safe = |value: &str| {
        !value.is_empty()
            && value
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-' | b'.'))
    };
    let parts = prefix
        .as_ref()
        .split('/')
        .take_while(|part| {
            part.split_once('=')
                .is_some_and(|(name, value)| safe(name) && safe(value))
        })
        .collect::<Vec<_>>();
    (!parts.is_empty()).then(|| Path::from_iter(parts))
}
