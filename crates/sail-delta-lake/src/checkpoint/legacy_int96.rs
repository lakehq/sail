// SPDX-License-Identifier: Apache-2.0

//! Spark writes INT96 values in its legacy hybrid calendar when the footer has
//! `org.apache.spark.version` and either the version is older than "3.1.0" or the footer also has
//! `org.apache.spark.legacyINT96` (`DataSourceUtils.getRebaseSpec`). That shifts values before 1900
//! by up to about a day after 1582 and by days before. Sail does not rebase, so it reads such
//! `add.stats_parsed` bounds before 1900-01-02 as null, an unknown bound.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, RecordBatch, Scalar, StructArray, TimestampMicrosecondArray,
};
use datafusion::arrow::compute::kernels::cmp::lt;
use datafusion::arrow::compute::{cast, nullif};
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::arrow::error::ArrowError;
use parquet::basic::Type as PhysicalType;
use parquet::file::metadata::ParquetMetaData;

use crate::spec::fields::{
    FIELD_NAME_STATS_PARSED, STATS_FIELD_MAX_VALUES, STATS_FIELD_MIN_VALUES,
};

const SPARK_VERSION_KEY: &str = "org.apache.spark.version";
const SPARK_LEGACY_INT96_KEY: &str = "org.apache.spark.legacyINT96";
/// Files written by Spark before this version always use the legacy calendar for INT96.
const SPARK_INT96_REBASE_MIN_VERSION: &str = "3.1.0";
/// 1900-01-02T00:00:00Z in microseconds. Stored values from here on are never shifted.
const LEGACY_INT96_CUTOFF_MICROS: i64 = -2_208_902_400_000_000;

/// Returns the column paths of the INT96 `add.stats_parsed` min/max values that Spark would rebase,
/// or nothing if the file does not use the legacy calendar for INT96.
pub(super) fn legacy_int96_stats_paths(metadata: &ParquetMetaData) -> Vec<Vec<String>> {
    let file_metadata = metadata.file_metadata();
    let value = |key: &str| {
        file_metadata
            .key_value_metadata()?
            .iter()
            .find(|kv| kv.key == key)?
            .value
            .as_deref()
    };
    // The version is compared as a string, like Spark does.
    let legacy = value(SPARK_VERSION_KEY).is_some_and(|version| {
        version < SPARK_INT96_REBASE_MIN_VERSION || value(SPARK_LEGACY_INT96_KEY).is_some()
    });
    if !legacy {
        return vec![];
    }
    file_metadata
        .schema_descr()
        .columns()
        .iter()
        .filter(|column| column.physical_type() == PhysicalType::INT96)
        .map(|column| column.path().parts().to_vec())
        .filter(|path| {
            matches!(
                path.as_slice(),
                [add, stats, bound, _, ..]
                    if add == "add"
                        && stats == FIELD_NAME_STATS_PARSED
                        && (bound == STATS_FIELD_MIN_VALUES || bound == STATS_FIELD_MAX_VALUES)
            )
        })
        .collect()
}

/// Sets the values before 1900-01-02T00:00:00Z at the given column paths to null.
pub(super) fn null_legacy_int96_stats(
    batch: RecordBatch,
    paths: &[Vec<String>],
) -> Result<RecordBatch, ArrowError> {
    if paths.is_empty() {
        return Ok(batch);
    }
    let metadata = batch.schema().metadata().clone();
    let mut array: ArrayRef = Arc::new(StructArray::from(batch));
    for path in paths {
        array = null_before_cutoff(&array, path)?;
    }
    let array = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| ArrowError::SchemaError("expected a struct array".to_string()))?;
    let schema = Schema::new_with_metadata(array.fields().clone(), metadata);
    RecordBatch::try_new(Arc::new(schema), array.columns().to_vec())
}

fn null_before_cutoff(array: &ArrayRef, path: &[String]) -> Result<ArrayRef, ArrowError> {
    let Some((name, rest)) = path.split_first() else {
        let cutoff = cast(
            &TimestampMicrosecondArray::from(vec![LEGACY_INT96_CUTOFF_MICROS]),
            array.data_type(),
        )?;
        return nullif(array, &lt(array, &Scalar::new(cutoff))?);
    };
    let Some(parent) = array.as_any().downcast_ref::<StructArray>() else {
        return Ok(Arc::clone(array));
    };
    let Some((index, field)) = parent.fields().find(name) else {
        return Ok(Arc::clone(array));
    };
    let mut fields = parent.fields().iter().cloned().collect::<Vec<_>>();
    let mut columns = parent.columns().to_vec();
    columns[index] = null_before_cutoff(&columns[index], rest)?;
    fields[index] = Arc::new(
        Field::clone(field)
            .with_data_type(columns[index].data_type().clone())
            .with_nullable(field.is_nullable() || rest.is_empty()),
    );
    Ok(Arc::new(StructArray::try_new(
        fields.into(),
        columns,
        parent.nulls().cloned(),
    )?))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::array::{AsArray, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Fields, TimeUnit, TimestampMicrosecondType};
    use parquet::file::metadata::{FileMetaData, KeyValue, ParquetMetaData};
    use parquet::schema::parser::parse_message_type;
    use parquet::schema::types::SchemaDescriptor;

    use super::*;

    const SCHEMA: &str = "message spark_schema {
        optional group add {
            optional group partitionValues_parsed { optional int96 ts; }
            optional group stats_parsed {
                optional group minValues { optional int96 ts; optional int64 n; }
                optional group maxValues { optional int96 ts; optional int64 n; }
            }
        }
    }";

    fn paths(footer: &[(&str, Option<&str>)]) -> Vec<String> {
        #[expect(clippy::unwrap_used)]
        let schema = SchemaDescriptor::new(Arc::new(parse_message_type(SCHEMA).unwrap()));
        let footer = footer
            .iter()
            .map(|(key, value)| KeyValue::new(key.to_string(), value.map(str::to_string)))
            .collect();
        let file = FileMetaData::new(1, 0, None, Some(footer), Arc::new(schema), None);
        legacy_int96_stats_paths(&ParquetMetaData::new(file, vec![]))
            .into_iter()
            .map(|path| path.join("."))
            .collect()
    }

    #[test]
    fn test_legacy_int96_stats_paths_follow_spark_footer_rule() {
        let stats = [
            "add.stats_parsed.minValues.ts",
            "add.stats_parsed.maxValues.ts",
        ];
        let version = SPARK_VERSION_KEY;
        let legacy = SPARK_LEGACY_INT96_KEY;
        assert_eq!(paths(&[(version, Some("2.4.8"))]), stats);
        assert_eq!(paths(&[(version, Some("3.0.1"))]), stats);
        assert_eq!(
            paths(&[(version, Some("3.5.0")), (legacy, Some(""))]),
            stats
        );
        assert!(paths(&[(version, Some("3.1.0"))]).is_empty());
        assert!(paths(&[(version, Some("3.5.0")), (legacy, None)]).is_empty());
        assert!(paths(&[(version, Some("3.5.0"))]).is_empty());
        assert!(paths(&[(legacy, Some(""))]).is_empty());
        assert!(paths(&[]).is_empty());
    }

    #[test]
    fn test_null_legacy_int96_stats_before_cutoff() -> Result<(), ArrowError> {
        // 1900-01-01T03:00:00Z, which a legacy write can produce from a value before 1900.
        let shifted = -2_208_978_000_000_000;
        let ts: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
            Some(LEGACY_INT96_CUTOFF_MICROS - 1),
            Some(shifted),
            Some(LEGACY_INT96_CUTOFF_MICROS),
            None,
        ]));
        let n: ArrayRef = Arc::new(Int64Array::from(vec![Some(-1), Some(-2), Some(-3), None]));
        let bound_fields = Fields::from(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("n", DataType::Int64, true),
        ]);
        let bound: ArrayRef = Arc::new(StructArray::try_new(
            bound_fields.clone(),
            vec![Arc::clone(&ts), Arc::clone(&n)],
            None,
        )?);
        let stats_fields = Fields::from(vec![
            Field::new(
                STATS_FIELD_MIN_VALUES,
                DataType::Struct(bound_fields.clone()),
                true,
            ),
            Field::new(STATS_FIELD_MAX_VALUES, DataType::Struct(bound_fields), true),
        ]);
        let stats: ArrayRef = Arc::new(StructArray::try_new(
            stats_fields.clone(),
            vec![Arc::clone(&bound), Arc::clone(&bound)],
            None,
        )?);
        let add_fields = Fields::from(vec![Field::new(
            FIELD_NAME_STATS_PARSED,
            DataType::Struct(stats_fields),
            true,
        )]);
        let add: ArrayRef = Arc::new(StructArray::try_new(add_fields.clone(), vec![stats], None)?);
        let metadata = HashMap::from([("key".to_string(), "value".to_string())]);
        let schema = Schema::new_with_metadata(
            vec![Field::new("add", DataType::Struct(add_fields), true)],
            metadata.clone(),
        );
        let batch = RecordBatch::try_new(Arc::new(schema), vec![add])?;
        let path = ["add", FIELD_NAME_STATS_PARSED, STATS_FIELD_MIN_VALUES, "ts"]
            .map(str::to_string)
            .to_vec();

        let batch = null_legacy_int96_stats(batch, &[path])?;
        assert_eq!(batch.schema().metadata(), &metadata);
        let stats = batch.column(0).as_struct().column(0).as_struct();
        let min = stats.column(0).as_struct();
        assert_eq!(
            min.column(0)
                .as_primitive::<TimestampMicrosecondType>()
                .iter()
                .collect::<Vec<_>>(),
            vec![None, None, Some(LEGACY_INT96_CUTOFF_MICROS), None]
        );
        assert_eq!(min.column(1), &n);
        assert_eq!(stats.column(1), &bound);
        Ok(())
    }
}
