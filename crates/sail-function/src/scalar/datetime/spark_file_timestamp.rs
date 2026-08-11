use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, FixedSizeListArray, GenericListArray, MapArray, StructArray,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, exec_err, plan_err};
use datafusion_expr::function::Hint;
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions::utils::make_scalar_function;

use super::spark_date_format::format_file_timestamp_array;

/// Spark's file-writer default when `spark.sql.files.supportSecondOffsetFormat` is enabled
/// (the default since Spark 4.0).
pub const SPARK_FILE_TIMESTAMP_FORMAT: &str = "yyyy-MM-dd'T'HH:mm:ss[.SSS][XXXXX]";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SparkFileTimestamp {
    session_timezone: Arc<str>,
    timestamp_format: Arc<str>,
    signature: Signature,
}

impl SparkFileTimestamp {
    pub fn new(session_timezone: Arc<str>, timestamp_format: Arc<str>) -> Self {
        Self {
            session_timezone,
            timestamp_format,
            signature: Signature::any(1, Volatility::Immutable),
        }
    }

    pub fn session_timezone(&self) -> &str {
        &self.session_timezone
    }

    pub fn timestamp_format(&self) -> &str {
        &self.timestamp_format
    }

    pub fn output_type(data_type: &DataType) -> Result<DataType> {
        Ok(match data_type {
            DataType::Timestamp(_, Some(_)) => DataType::Utf8,
            DataType::List(field) => DataType::List(output_field(field)?),
            DataType::LargeList(field) => DataType::LargeList(output_field(field)?),
            DataType::FixedSizeList(field, size) => {
                DataType::FixedSizeList(output_field(field)?, *size)
            }
            DataType::Struct(fields) => DataType::Struct(
                fields
                    .iter()
                    .map(output_field)
                    .collect::<Result<Vec<_>>>()?
                    .into(),
            ),
            DataType::Map(field, sorted) => DataType::Map(output_field(field)?, *sorted),
            _ => data_type.clone(),
        })
    }
}

fn output_field(field: &FieldRef) -> Result<FieldRef> {
    Ok(Arc::new(field.as_ref().clone().with_data_type(
        SparkFileTimestamp::output_type(field.data_type())?,
    )))
}

impl ScalarUDFImpl for SparkFileTimestamp {
    fn name(&self) -> &str {
        "spark_file_timestamp"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [data_type] = arg_types else {
            return plan_err!("spark_file_timestamp expects exactly one argument");
        };
        Self::output_type(data_type)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [input] = args.arg_fields else {
            return plan_err!("spark_file_timestamp expects exactly one argument");
        };
        Ok(Arc::new(Field::new(
            self.name(),
            Self::output_type(input.data_type())?,
            input.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(
            |args| {
                let [array] = args else {
                    return exec_err!("spark_file_timestamp expects exactly one argument");
                };
                format_array(array, &self.session_timezone, &self.timestamp_format)
            },
            vec![Hint::AcceptsSingular],
        )(args.args.as_slice())
    }
}

fn format_array(
    array: &ArrayRef,
    session_timezone: &str,
    timestamp_format: &str,
) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Timestamp(_, Some(_)) => {
            format_file_timestamp_array(array, session_timezone, timestamp_format)
        }
        DataType::List(field) => {
            let source = array.as_list::<i32>();
            let target_field = output_field(field)?;
            let values = format_array(source.values(), session_timezone, timestamp_format)?;
            Ok(Arc::new(GenericListArray::<i32>::try_new(
                target_field,
                source.offsets().clone(),
                values,
                source.nulls().cloned(),
            )?))
        }
        DataType::LargeList(field) => {
            let source = array.as_list::<i64>();
            let target_field = output_field(field)?;
            let values = format_array(source.values(), session_timezone, timestamp_format)?;
            Ok(Arc::new(GenericListArray::<i64>::try_new(
                target_field,
                source.offsets().clone(),
                values,
                source.nulls().cloned(),
            )?))
        }
        DataType::FixedSizeList(field, size) => {
            let source = array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Internal(
                        "spark_file_timestamp expected FixedSizeListArray".to_string(),
                    )
                })?;
            let target_field = output_field(field)?;
            let values = format_array(source.values(), session_timezone, timestamp_format)?;
            Ok(Arc::new(FixedSizeListArray::try_new(
                target_field,
                *size,
                values,
                source.nulls().cloned(),
            )?))
        }
        DataType::Struct(fields) => {
            let source = array.as_struct();
            let target_fields = fields
                .iter()
                .map(output_field)
                .collect::<Result<Vec<_>>>()?;
            let columns = source
                .columns()
                .iter()
                .map(|column| format_array(column, session_timezone, timestamp_format))
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(StructArray::try_new(
                target_fields.into(),
                columns,
                source.nulls().cloned(),
            )?))
        }
        DataType::Map(field, sorted) => {
            let source = array.as_map();
            let target_field = output_field(field)?;
            let entries = Arc::new(source.entries().clone()) as ArrayRef;
            let entries = format_array(&entries, session_timezone, timestamp_format)?;
            let entries = entries
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Internal(
                        "spark_file_timestamp map entries must be a struct".to_string(),
                    )
                })?;
            Ok(Arc::new(MapArray::try_new(
                target_field,
                source.offsets().clone(),
                entries.clone(),
                source.nulls().cloned(),
                *sorted,
            )?))
        }
        _ => Ok(Arc::clone(array)),
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{StringArray, TimestampMicrosecondArray};
    use datafusion::arrow::csv::WriterBuilder;
    use datafusion::arrow::datatypes::{Field, Schema, TimeUnit};
    use datafusion::arrow::json::LineDelimitedWriter;
    use datafusion::arrow::record_batch::RecordBatch;

    use super::*;

    #[test]
    fn formats_second_precision_offset_without_arrow_timezone_parser() -> Result<()> {
        let input =
            Arc::new(TimestampMicrosecondArray::from(vec![-3_723_000_000]).with_timezone("UTC"))
                as ArrayRef;
        let output = format_array(&input, "+01:02:03", SPARK_FILE_TIMESTAMP_FORMAT)?;
        assert_eq!(output.data_type(), &DataType::Utf8);
        let output_strings = output
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "formatted timestamp was not a string array".to_string(),
                )
            })?;
        assert_eq!(output_strings.value(0), "1970-01-01T00:00:00.000+01:02:03");
        assert_eq!(
            SparkFileTimestamp::output_type(&DataType::Timestamp(
                TimeUnit::Microsecond,
                Some(Arc::from("UTC")),
            ))?,
            DataType::Utf8
        );

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("t", DataType::Utf8, false)])),
            vec![output],
        )?;
        let mut csv = Vec::new();
        WriterBuilder::new()
            .with_header(false)
            .build(&mut csv)
            .write(&batch)?;
        assert_eq!(csv, b"1970-01-01T00:00:00.000+01:02:03\n");

        let mut json = Vec::new();
        let mut writer = LineDelimitedWriter::new(&mut json);
        writer.write(&batch)?;
        writer.finish()?;
        assert_eq!(json, b"{\"t\":\"1970-01-01T00:00:00.000+01:02:03\"}\n");

        let custom = format_array(&input, "+01:02:03", "yyyy/MM/dd HH:mm:ss XXXXX")?;
        let custom = custom
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "custom-formatted timestamp was not a string array".to_string(),
                )
            })?;
        assert_eq!(custom.value(0), "1970/01/01 00:00:00 +01:02:03");
        Ok(())
    }
}
