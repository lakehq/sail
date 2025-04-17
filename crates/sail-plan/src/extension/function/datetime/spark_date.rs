use std::any::Any;
use std::sync::Arc;

use chrono::format::{parse_and_remainder, Item, Numeric, Pad, Parsed};
use chrono::{NaiveDate, ParseError};
use datafusion::arrow::array::Date32Array;
use datafusion::arrow::datatypes::{DataType, Date32Type};
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::types::logical_string;
use datafusion_common::{exec_datafusion_err, exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_expr_common::signature::{Coercion, TypeSignatureClass};

use crate::utils::ItemTaker;

#[derive(Debug)]
pub struct SparkDate {
    signature: Signature,
}

impl Default for SparkDate {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkDate {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![Coercion::new_exact(TypeSignatureClass::Native(
                    logical_string(),
                ))],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkDate {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_date"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let arg = args.one()?;
        match arg {
            ColumnarValue::Array(array) => {
                let array = match array.data_type() {
                    DataType::Utf8 => as_string_array(&array)?
                        .iter()
                        .map(|x| x.map(string_to_date).transpose())
                        .collect::<Result<Date32Array>>()?,
                    DataType::LargeUtf8 => as_large_string_array(&array)?
                        .iter()
                        .map(|x| x.map(string_to_date).transpose())
                        .collect::<Result<Date32Array>>()?,
                    DataType::Utf8View => as_string_view_array(&array)?
                        .iter()
                        .map(|x| x.map(string_to_date).transpose())
                        .collect::<Result<Date32Array>>()?,
                    _ => return exec_err!("expected string array for `date`"),
                };
                Ok(ColumnarValue::Array(Arc::new(array)))
            }
            ColumnarValue::Scalar(scalar) => {
                let value = match scalar.try_as_str() {
                    Some(x) => x.map(string_to_date).transpose()?,
                    _ => {
                        return exec_err!("expected string scalar for `date`");
                    }
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Date32(value)))
            }
        }
    }
}

pub fn parse_date(s: &str) -> Result<NaiveDate> {
    const DATE_ITEMS: &[Item<'static>] = &[
        Item::Numeric(Numeric::Year, Pad::Zero),
        Item::Space(""),
        Item::Literal("-"),
        Item::Numeric(Numeric::Month, Pad::Zero),
        Item::Space(""),
        Item::Literal("-"),
        Item::Numeric(Numeric::Day, Pad::Zero),
    ];

    let error = |e: ParseError| exec_datafusion_err!("invalid date: {e}: {s}");
    let mut parsed = Parsed::new();
    let _ = parse_and_remainder(&mut parsed, s, DATE_ITEMS.iter()).map_err(error)?;
    parsed.to_naive_date().map_err(error)
}

fn string_to_date(value: &str) -> Result<i32> {
    let date = parse_date(value)?;
    Ok(Date32Type::from_naive_date(date))
}
