use std::sync::Arc;

use arrow::array::{Array, AsArray, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, FieldRef, Metadata};
use chrono::NaiveDate;
use datafusion::catalog::Session;
use datafusion::datasource::listing::helpers::{
    evaluate_partition_prefix, parse_partitions_for_path, pruned_partition_list,
};
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::physical_expr::create_physical_expr;
use datafusion_common::{
    DFSchema, Result, ScalarValue, exec_datafusion_err, internal_err, not_impl_err,
};
use datafusion_datasource::{ListingTableUrl, PartitionedFile};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt, future};
use object_store::ObjectStore;
use sail_common_datafusion::utils::items::ItemTaker;

pub const HIVE_DEFAULT_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

/// Directory keys only: the upstream sink removes these columns from file payloads.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct HivePartitionKey {
    signature: Signature,
}

impl Default for HivePartitionKey {
    fn default() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for HivePartitionKey {
    fn name(&self) -> &str {
        "sail_hive_partition_key"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // Keep the types supported by DataFusion's Hive demuxer. In particular,
        // a general string cast would also change its non-null float spelling.
        let [arg_type] = arg_types else {
            return internal_err!("Hive partition key requires one argument");
        };
        match arg_type {
            DataType::Null
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Boolean
            | DataType::Date32
            | DataType::Date64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64 => Ok(DataType::Utf8),
            DataType::Dictionary(_, value) if value.as_ref() == &DataType::Utf8 => {
                Ok(DataType::Utf8)
            }
            other => not_impl_err!(
                "it is not yet supported to write to hive partitions with datatype {other}"
            ),
        }
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [field] = args.arg_fields else {
            return internal_err!("Hive partition key requires one argument");
        };
        let data_type = self.return_type(&[field.data_type().clone()])?;
        Ok(Arc::new(Field::new(self.name(), data_type, false)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match args.args.one()? {
            ColumnarValue::Scalar(value) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                encode_partition_key(&value)?,
            )))),
            ColumnarValue::Array(array) => {
                let values = (0..array.len())
                    .map(|row| encode_partition_key(&ScalarValue::try_from_array(&array, row)?))
                    .collect::<Result<Vec<_>>>()?;
                Ok(ColumnarValue::Array(Arc::new(StringArray::from(values))))
            }
        }
    }
}

fn encode_partition_key(value: &ScalarValue) -> Result<String> {
    if value.is_null() {
        return Ok(HIVE_DEFAULT_PARTITION.to_string());
    }
    match value {
        ScalarValue::Dictionary(_, value) => encode_partition_key(value),
        ScalarValue::Utf8(Some(value))
        | ScalarValue::LargeUtf8(Some(value))
        | ScalarValue::Utf8View(Some(value)) => Ok(if value.is_empty() {
            HIVE_DEFAULT_PARTITION.to_string()
        } else {
            value.clone()
        }),
        ScalarValue::Date32(Some(days)) => format_partition_date(i64::from(*days)),
        ScalarValue::Date64(Some(millis)) => format_partition_date(millis / 86_400_000),
        // ScalarValue's primitive Display uses the same Rust Display as the demuxer.
        _ => Ok(value.to_string()),
    }
}

fn format_partition_date(days: i64) -> Result<String> {
    let date = days
        .checked_add(719_163)
        .and_then(|days| i32::try_from(days).ok())
        .and_then(NaiveDate::from_num_days_from_ce_opt)
        .ok_or_else(|| exec_datafusion_err!("partition date is out of range: {days}"))?;
    Ok(date.format("%Y-%m-%d").to_string())
}

/// Restore Hive NULLs before typed conversion and partition predicate evaluation.
/// Path parsing and prefix pruning retain DataFusion's listing semantics.
pub(super) async fn list_partitioned_files<'a>(
    ctx: &'a dyn Session,
    store: &'a dyn ObjectStore,
    table_path: &'a ListingTableUrl,
    filters: &'a [Expr],
    partition_cols: &'a [(String, DataType)],
) -> Result<BoxStream<'a, Result<PartitionedFile>>> {
    if partition_cols.is_empty() {
        return pruned_partition_list(ctx, store, table_path, filters, "", partition_cols).await;
    }
    let schema = DFSchema::from_unqualified_fields(
        partition_cols
            .iter()
            .map(|(name, data_type)| Field::new(name, data_type.clone(), true))
            .collect(),
        Metadata::new(),
    )?;
    let predicate = conjunction(filters.iter().cloned())
        .map(|expr| {
            create_physical_expr(
                &expr,
                &schema,
                ctx.execution_props(),
                &PhysicalPlanningContext::default(),
            )
        })
        .transpose()?;
    let prefix = evaluate_partition_prefix(partition_cols, filters);
    let files = table_path
        .list_prefixed_files(ctx, store, prefix, "")
        .await?;
    Ok(files
        .try_filter_map(move |meta| {
            future::ready((|| {
                if meta.size == 0 {
                    return Ok(None);
                }
                let Some(values) = parse_partitions_for_path(
                    table_path,
                    &meta.location,
                    partition_cols.iter().map(|(name, _)| name.as_str()),
                ) else {
                    return Ok(None);
                };
                if values.len() != partition_cols.len() {
                    return Ok(None);
                }
                let values = values
                    .into_iter()
                    .zip(partition_cols)
                    .map(|(value, (_, data_type))| {
                        if value == HIVE_DEFAULT_PARTITION {
                            ScalarValue::try_from(data_type)
                        } else {
                            ScalarValue::try_from_string(value.into_owned(), data_type)
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;
                if let Some(predicate) = &predicate {
                    let batch = RecordBatch::try_new(
                        Arc::clone(schema.inner()),
                        values
                            .iter()
                            .map(ScalarValue::to_array)
                            .collect::<Result<_>>()?,
                    )?;
                    let result = predicate.evaluate(&batch)?.into_array(1)?;
                    let result = result.as_boolean();
                    if result.is_null(0) || !result.value(0) {
                        return Ok(None);
                    }
                }
                let mut file: PartitionedFile = meta.into();
                file.partition_values = values;
                file.table_reference.clone_from(table_path.get_table_ref());
                Ok(Some(file))
            })())
        })
        .boxed())
}

#[cfg(test)]
mod tests {
    use arrow::array::{ArrayRef, DictionaryArray, Int32Array};
    use arrow::datatypes::Int32Type;

    use super::*;

    #[tokio::test]
    async fn typed_null_partitions_are_restored_before_filtering() -> Result<()> {
        use datafusion::prelude::{SessionContext, col, lit};
        use object_store::ObjectStoreExt;
        use object_store::memory::InMemory;
        use object_store::path::Path;

        let store = InMemory::new();
        for value in [HIVE_DEFAULT_PARTITION, "0", "1"] {
            store
                .put(
                    &Path::from(format!("table/p={value}/data.arrow")),
                    "data".into(),
                )
                .await?;
        }
        // Unpartitioned and incomplete paths are not rows of this partitioned table.
        store
            .put(&Path::from("table/root.arrow"), "data".into())
            .await?;
        let state = SessionContext::new().state();
        let path = ListingTableUrl::parse("memory:///table/")?;
        let columns = vec![("p".to_string(), DataType::Int32)];
        for (filter, expected) in [
            (col("p").is_null(), vec![ScalarValue::Int32(None)]),
            (
                col("p").is_not_null(),
                vec![ScalarValue::Int32(Some(0)), ScalarValue::Int32(Some(1))],
            ),
            (col("p").not_eq(lit(0)), vec![ScalarValue::Int32(Some(1))]),
            (
                col("p").eq(lit(0)).or(col("p").is_null()),
                vec![ScalarValue::Int32(None), ScalarValue::Int32(Some(0))],
            ),
            (
                col("p").gt(lit(0)).and(lit(ScalarValue::Boolean(None))),
                vec![],
            ),
        ] {
            let filters = [filter];
            let files = list_partitioned_files(&state, &store, &path, &filters, &columns)
                .await?
                .try_collect::<Vec<_>>()
                .await?;
            let mut actual = files
                .into_iter()
                .map(|file| file.partition_values[0].clone())
                .collect::<Vec<_>>();
            actual.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            assert_eq!(actual, expected);
        }
        // Use another listing root so the earlier cached inventory cannot hide the bad file.
        let invalid_path = ListingTableUrl::parse("memory:///invalid/")?;
        store
            .put(
                &Path::from("invalid/p=not-an-int/data.arrow"),
                "data".into(),
            )
            .await?;
        assert!(ScalarValue::try_from_string("not-an-int".into(), &DataType::Int32).is_err());
        assert!(
            list_partitioned_files(&state, &store, &invalid_path, &[], &columns)
                .await?
                .try_collect::<Vec<_>>()
                .await
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn partition_keys_preserve_primitive_spelling() -> Result<()> {
        for (value, expected) in [
            (ScalarValue::Null, HIVE_DEFAULT_PARTITION),
            (ScalarValue::Int32(None), HIVE_DEFAULT_PARTITION),
            (
                ScalarValue::Utf8(Some(String::new())),
                HIVE_DEFAULT_PARTITION,
            ),
            (ScalarValue::Utf8(Some("null".into())), "null"),
            (ScalarValue::Utf8(Some("a/b%=".into())), "a/b%="),
            (ScalarValue::Int32(Some(0)), "0"),
            (ScalarValue::Float32(Some(3.0)), "3"),
            (ScalarValue::Float64(Some(3.0)), "3"),
            (ScalarValue::Float64(Some(-0.0)), "-0"),
            (ScalarValue::Float64(Some(1e-8)), "0.00000001"),
            (ScalarValue::Float64(Some(f64::NAN)), "NaN"),
            (ScalarValue::Float64(Some(f64::INFINITY)), "inf"),
            (ScalarValue::Boolean(Some(false)), "false"),
            (ScalarValue::Date32(Some(0)), "1970-01-01"),
            (ScalarValue::Date64(Some(-86_400_000)), "1969-12-31"),
        ] {
            assert_eq!(encode_partition_key(&value)?, expected, "{value:?}");
        }
        Ok(())
    }

    #[test]
    fn partition_keys_encode_dictionary_logical_nulls() -> Result<()> {
        let array: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(0), None, Some(1), Some(2)]),
            Arc::new(StringArray::from(vec![Some("value"), None, Some("")])),
        )?);
        let udf = HivePartitionKey::default();
        let result = udf.invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::clone(&array))],
            arg_fields: vec![Arc::new(Field::new("p", array.data_type().clone(), true))],
            number_rows: 4,
            return_field: Arc::new(Field::new("key", DataType::Utf8, false)),
            config_options: Arc::new(Default::default()),
        })?;
        let result = result.into_array(4)?;
        assert_eq!(result.null_count(), 0);
        assert_eq!(
            result.as_string::<i32>(),
            &StringArray::from(vec![
                "value",
                HIVE_DEFAULT_PARTITION,
                HIVE_DEFAULT_PARTITION,
                HIVE_DEFAULT_PARTITION,
            ])
        );
        Ok(())
    }
}
