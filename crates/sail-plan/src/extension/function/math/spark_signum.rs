use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray, Float64Array};
use datafusion::arrow::datatypes::{
    DataType, Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    IntervalUnit, IntervalYearMonthType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility};
use half::f16;

#[derive(Debug)]
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

    fn invoke_batch(&self, args: &[ColumnarValue], _number_rows: usize) -> Result<ColumnarValue> {
        if args.len() != 1 {
            return exec_err!(
                "Spark `signum` function requires 1 argument, got {}",
                args.len()
            );
        }
        match &args[0] {
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
            ColumnarValue::Scalar(ScalarValue::IntervalYearMonth(val)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(val.map(|x| {
                    if x == 0_i32 {
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
                    DataType::Interval(IntervalUnit::YearMonth) => {
                        let result: Float64Array = array
                            .as_primitive::<IntervalYearMonthType>()
                            .unary(|x| if x == 0_i32 { 0_f64 } else { x.signum() as f64 });
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
