use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray, Float64Array};
use datafusion::arrow::datatypes::{
    DataType, Decimal128Type, DurationMicrosecondType, DurationMillisecondType,
    DurationNanosecondType, DurationSecondType, Float16Type, Float32Type, Float64Type, Int16Type,
    Int32Type, Int64Type, Int8Type, IntervalDayTimeType, IntervalMonthDayNanoType, IntervalUnit,
    IntervalYearMonthType, TimeUnit, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use half::f16;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkSignum {
    signature: Signature,
}

impl Default for SparkSignum {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSignum {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Numeric(1),
                    TypeSignature::Uniform(1, vec![DataType::Interval(IntervalUnit::YearMonth)]),
                    TypeSignature::Uniform(1, vec![DataType::Interval(IntervalUnit::DayTime)]),
                    TypeSignature::Uniform(1, vec![DataType::Interval(IntervalUnit::MonthDayNano)]),
                    TypeSignature::Uniform(1, vec![DataType::Duration(TimeUnit::Second)]),
                    TypeSignature::Uniform(1, vec![DataType::Duration(TimeUnit::Millisecond)]),
                    TypeSignature::Uniform(1, vec![DataType::Duration(TimeUnit::Microsecond)]),
                    TypeSignature::Uniform(1, vec![DataType::Duration(TimeUnit::Nanosecond)]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkSignum {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_signum"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let [arg] = args.as_slice() else {
            return exec_err!(
                "Spark `signum` function requires 1 argument, got {}",
                args.len()
            );
        };
        match arg {
            ColumnarValue::Scalar(ScalarValue::UInt8(val)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(val.map(|x| {
                    if x == 0_u8 {
                        0_f64
                    } else {
                        (x as f64).signum()
                    }
                }))))
            }
            ColumnarValue::Scalar(ScalarValue::UInt16(val)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(val.map(|x| {
                    if x == 0_u16 {
                        0_f64
                    } else {
                        (x as f64).signum()
                    }
                }))))
            }
            ColumnarValue::Scalar(ScalarValue::UInt32(val)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(val.map(|x| {
                    if x == 0_u32 {
                        0_f64
                    } else {
                        (x as f64).signum()
                    }
                }))))
            }
            ColumnarValue::Scalar(ScalarValue::UInt64(val)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(val.map(|x| {
                    if x == 0_u64 {
                        0_f64
                    } else {
                        (x as f64).signum()
                    }
                }))))
            }
            ColumnarValue::Scalar(ScalarValue::Int8(val)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(val.map(|x| {
                    if x == 0_i8 {
                        0_f64
                    } else {
                        x.signum() as f64
                    }
                }))))
            }
            ColumnarValue::Scalar(ScalarValue::Int16(val)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(val.map(|x| {
                    if x == 0_i16 {
                        0_f64
                    } else {
                        x.signum() as f64
                    }
                }))))
            }
            ColumnarValue::Scalar(ScalarValue::Int32(val)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(val.map(|x| {
                    if x == 0_i32 {
                        0_f64
                    } else {
                        x.signum() as f64
                    }
                }))))
            }
            ColumnarValue::Scalar(ScalarValue::Int64(val)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(val.map(|x| {
                    if x == 0_i64 {
                        0_f64
                    } else {
                        x.signum() as f64
                    }
                }))))
            }
            ColumnarValue::Scalar(ScalarValue::Float16(val)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(val.map(|x| {
                    if x == f16::ZERO {
                        0_f64
                    } else {
                        f64::from(x.signum())
                    }
                }))))
            }
            ColumnarValue::Scalar(ScalarValue::Float32(val)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(val.map(|x| {
                    if x == 0_f32 {
                        0_f64
                    } else {
                        x.signum() as f64
                    }
                }))))
            }
            ColumnarValue::Scalar(ScalarValue::Float64(val)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(val.map(|x| {
                    if x == 0_f64 {
                        0_f64
                    } else {
                        x.signum()
                    }
                }))))
            }
            ColumnarValue::Scalar(ScalarValue::Decimal128(val, _precision, _scale)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(val.map(|x| {
                    if x == 0 {
                        0.0
                    } else if x > 0 {
                        1.0
                    } else {
                        -1.0
                    }
                }))))
            }
            ColumnarValue::Scalar(ScalarValue::IntervalYearMonth(val)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(val.map(|x| {
                    if x == 0_i32 {
                        0_f64
                    } else {
                        x.signum() as f64
                    }
                }))))
            }
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(val)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(val.map(|x| {
                    // IntervalDayTime has days and milliseconds components
                    let days = x.days;
                    let ms = x.milliseconds;
                    if days == 0 && ms == 0 {
                        0_f64
                    } else if days > 0 || (days == 0 && ms > 0) {
                        1_f64
                    } else {
                        -1_f64
                    }
                }))))
            }
            ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(val)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(val.map(|x| {
                    // IntervalMonthDayNano stores months, days, nanoseconds as i128
                    // The sign is determined by the overall value
                    let months = IntervalMonthDayNanoType::to_parts(x).0;
                    let days = IntervalMonthDayNanoType::to_parts(x).1;
                    let nanos = IntervalMonthDayNanoType::to_parts(x).2;
                    if months == 0 && days == 0 && nanos == 0 {
                        0_f64
                    } else if months > 0
                        || (months == 0 && days > 0)
                        || (months == 0 && days == 0 && nanos > 0)
                    {
                        1_f64
                    } else {
                        -1_f64
                    }
                }))))
            }
            ColumnarValue::Scalar(ScalarValue::DurationSecond(val))
            | ColumnarValue::Scalar(ScalarValue::DurationMillisecond(val))
            | ColumnarValue::Scalar(ScalarValue::DurationMicrosecond(val))
            | ColumnarValue::Scalar(ScalarValue::DurationNanosecond(val)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(val.map(|x| {
                    if x == 0_i64 {
                        0_f64
                    } else {
                        x.signum() as f64
                    }
                }))))
            }
            ColumnarValue::Array(array) => {
                let result = match array.data_type() {
                    DataType::UInt8 => {
                        let result: Float64Array = array.as_primitive::<UInt8Type>().unary(|x| {
                            if x == 0_u8 {
                                0_f64
                            } else {
                                (x as f64).signum()
                            }
                        });
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    DataType::UInt16 => {
                        let result: Float64Array = array.as_primitive::<UInt16Type>().unary(|x| {
                            if x == 0_u16 {
                                0_f64
                            } else {
                                (x as f64).signum()
                            }
                        });
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    DataType::UInt32 => {
                        let result: Float64Array = array.as_primitive::<UInt32Type>().unary(|x| {
                            if x == 0_u32 {
                                0_f64
                            } else {
                                (x as f64).signum()
                            }
                        });
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    DataType::UInt64 => {
                        let result: Float64Array = array.as_primitive::<UInt64Type>().unary(|x| {
                            if x == 0_u64 {
                                0_f64
                            } else {
                                (x as f64).signum()
                            }
                        });
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    DataType::Int8 => {
                        let result: Float64Array = array.as_primitive::<Int8Type>().unary(|x| {
                            if x == 0_i8 {
                                0_f64
                            } else {
                                x.signum() as f64
                            }
                        });
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    DataType::Int16 => {
                        let result: Float64Array = array.as_primitive::<Int16Type>().unary(|x| {
                            if x == 0_i16 {
                                0_f64
                            } else {
                                x.signum() as f64
                            }
                        });
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    DataType::Int32 => {
                        let result: Float64Array = array.as_primitive::<Int32Type>().unary(|x| {
                            if x == 0_i32 {
                                0_f64
                            } else {
                                x.signum() as f64
                            }
                        });
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    DataType::Int64 => {
                        let result: Float64Array = array.as_primitive::<Int64Type>().unary(|x| {
                            if x == 0_i64 {
                                0_f64
                            } else {
                                x.signum() as f64
                            }
                        });
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    DataType::Float16 => {
                        let result: Float64Array = array.as_primitive::<Float16Type>().unary(|x| {
                            if x == f16::ZERO {
                                0_f64
                            } else {
                                f64::from(x.signum())
                            }
                        });
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    DataType::Float32 => {
                        let result: Float64Array = array.as_primitive::<Float32Type>().unary(|x| {
                            if x == 0_f32 {
                                0_f64
                            } else {
                                x.signum() as f64
                            }
                        });
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    DataType::Float64 => {
                        let result: Float64Array = array.as_primitive::<Float64Type>().unary(|x| {
                            if x == 0_f64 {
                                0_f64
                            } else {
                                x.signum()
                            }
                        });
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    DataType::Decimal128(_p, _s) => {
                        let result: Float64Array =
                            array.as_primitive::<Decimal128Type>().unary(|x| {
                                if x == 0 {
                                    0.0
                                } else if x > 0 {
                                    1.0
                                } else {
                                    -1.0
                                }
                            });
                        Ok(Arc::new(result) as ArrayRef)
                    }

                    DataType::Interval(IntervalUnit::YearMonth) => {
                        let result: Float64Array = array
                            .as_primitive::<IntervalYearMonthType>()
                            .unary(|x| if x == 0_i32 { 0_f64 } else { x.signum() as f64 });
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    DataType::Interval(IntervalUnit::DayTime) => {
                        let result: Float64Array =
                            array.as_primitive::<IntervalDayTimeType>().unary(|x| {
                                let days = x.days;
                                let ms = x.milliseconds;
                                if days == 0 && ms == 0 {
                                    0_f64
                                } else if days > 0 || (days == 0 && ms > 0) {
                                    1_f64
                                } else {
                                    -1_f64
                                }
                            });
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    DataType::Interval(IntervalUnit::MonthDayNano) => {
                        let result: Float64Array =
                            array.as_primitive::<IntervalMonthDayNanoType>().unary(|x| {
                                let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(x);
                                if months == 0 && days == 0 && nanos == 0 {
                                    0_f64
                                } else if months > 0
                                    || (months == 0 && days > 0)
                                    || (months == 0 && days == 0 && nanos > 0)
                                {
                                    1_f64
                                } else {
                                    -1_f64
                                }
                            });
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    DataType::Duration(TimeUnit::Second) => {
                        let result: Float64Array = array
                            .as_primitive::<DurationSecondType>()
                            .unary(|x| if x == 0 { 0_f64 } else { x.signum() as f64 });
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    DataType::Duration(TimeUnit::Millisecond) => {
                        let result: Float64Array = array
                            .as_primitive::<DurationMillisecondType>()
                            .unary(|x| if x == 0 { 0_f64 } else { x.signum() as f64 });
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    DataType::Duration(TimeUnit::Microsecond) => {
                        let result: Float64Array = array
                            .as_primitive::<DurationMicrosecondType>()
                            .unary(|x| if x == 0 { 0_f64 } else { x.signum() as f64 });
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    DataType::Duration(TimeUnit::Nanosecond) => {
                        let result: Float64Array = array
                            .as_primitive::<DurationNanosecondType>()
                            .unary(|x| if x == 0 { 0_f64 } else { x.signum() as f64 });
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    other => exec_err!("Unsupported data type {other:?} for function signum"),
                }?;
                Ok(ColumnarValue::Array(result))
            }
            other => exec_err!("Unsupported arg {other:?} for function signum"),
        }
    }
}
