use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ListArray, StructArray, TimestampMicrosecondArray};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use datafusion_common::cast::as_timestamp_microsecond_array;
use datafusion_common::{exec_err, plan_err, Result};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

/// Computes Spark `window` candidates at execute time, so the plan stays bounded
/// regardless of the `windowDuration / slideDuration` ratio. For each input
/// timestamp the UDF returns the list of overlapping windows that contain it; a
/// null timestamp produces an empty list, so a downstream `explode` drops the row.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkWindowBuckets {
    signature: Signature,
    window_duration: i64,
    slide_duration: i64,
    start_time: i64,
}

impl SparkWindowBuckets {
    pub fn new(window_duration: i64, slide_duration: i64, start_time: i64) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            window_duration,
            slide_duration,
            start_time,
        }
    }
}

impl ScalarUDFImpl for SparkWindowBuckets {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "window_buckets"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let tz = match arg_types {
            [DataType::Timestamp(TimeUnit::Microsecond, tz)] => tz.clone(),
            other => {
                return plan_err!(
                    "window_buckets expects a single Timestamp(Microsecond, *) argument, got {other:?}"
                )
            }
        };
        let field_type = DataType::Timestamp(TimeUnit::Microsecond, tz);
        let item = DataType::Struct(Fields::from(vec![
            Field::new("start", field_type.clone(), true),
            Field::new("end", field_type, true),
        ]));
        Ok(DataType::List(Arc::new(Field::new("item", item, true))))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args,
            number_rows,
            return_field,
            ..
        } = args;
        let time = match args.as_slice() {
            [t] => t,
            _ => return exec_err!("window_buckets requires 1 argument"),
        };
        let array = time.to_array(number_rows)?;
        let times = as_timestamp_microsecond_array(&array)?;

        let DataType::List(item_field) = return_field.data_type() else {
            return exec_err!("window_buckets return type must be List<Struct>");
        };
        let DataType::Struct(struct_fields) = item_field.data_type() else {
            return exec_err!("window_buckets return type must be List<Struct>");
        };
        let tz = match struct_fields.first().map(|f| f.data_type()) {
            Some(DataType::Timestamp(TimeUnit::Microsecond, tz)) => tz.clone(),
            _ => return exec_err!("window_buckets bucket field type must be Timestamp(us, *)"),
        };

        let n = (self.window_duration + self.slide_duration - 1) / self.slide_duration;
        let mut starts: Vec<i64> = Vec::new();
        let mut ends: Vec<i64> = Vec::new();
        let mut offsets: Vec<i32> = Vec::with_capacity(times.len() + 1);
        offsets.push(0);

        for i in 0..times.len() {
            if !times.is_null(i) {
                let ts = times.value(i);
                let offset_us = (ts - self.start_time).rem_euclid(self.slide_duration);
                let last_start = ts - offset_us;
                for j in 0..n {
                    let ws = last_start - j * self.slide_duration;
                    let we = ws + self.window_duration;
                    if ws <= ts && ts < we {
                        starts.push(ws);
                        ends.push(we);
                    }
                }
            }
            offsets.push(starts.len() as i32);
        }

        let start_arr = TimestampMicrosecondArray::from(starts).with_timezone_opt(tz.clone());
        let end_arr = TimestampMicrosecondArray::from(ends).with_timezone_opt(tz);

        let struct_arr = StructArray::new(
            struct_fields.clone(),
            vec![Arc::new(start_arr), Arc::new(end_arr)],
            None,
        );

        let list = ListArray::new(
            item_field.clone(),
            OffsetBuffer::new(offsets.into()),
            Arc::new(struct_arr),
            None,
        );

        Ok(ColumnarValue::Array(Arc::new(list)))
    }
}
