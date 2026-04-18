use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, GenericStringArray, Int64Array, ListArray, ListBuilder, OffsetSizeTrait,
    StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::function::Hint;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::signature::{Signature, Volatility};
use regex::Regex;

use crate::error::{generic_exec_err, generic_internal_err, unsupported_data_types_exec_err};
use crate::functions_nested_utils::opt_downcast_arg;
use crate::functions_utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkRegexpExtractAll {
    signature: Signature,
}

impl Default for SparkRegexpExtractAll {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkRegexpExtractAll {
    pub const NAME: &'static str = "regexp_extract_all";

    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkRegexpExtractAll {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        Self::NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new_list_field(
            DataType::Utf8,
            true,
        ))))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let err = || {
            Err(unsupported_data_types_exec_err(
                Self::NAME,
                "Expected (STRING, STRING) or (STRING, STRING, INT)",
                arg_types,
            ))
        };

        let mut res_types = vec![];
        for i in 0..=1 {
            res_types.push(match arg_types.get(i) {
                Some(DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8) => {
                    Ok(arg_types[i].clone())
                }
                Some(DataType::Null) => Ok(DataType::Utf8),
                _ => err(),
            });
        }
        if arg_types.len() == 3 {
            res_types.push(if arg_types[2].is_null() || arg_types[2].is_integer() {
                Ok(DataType::Int64)
            } else {
                err()
            });
        }
        res_types.into_iter().collect::<Result<Vec<_>>>()
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { mut args, .. } = args;
        if args.len() == 2 {
            args.push(ColumnarValue::Scalar(ScalarValue::Int64(Some(1))));
        }
        make_scalar_function(
            regexp_extract_all_inner,
            vec![Hint::Pad, Hint::AcceptsSingular, Hint::AcceptsSingular],
        )(&args)
    }
}

fn regexp_extract_all_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::LargeUtf8 => regexp_extract_all_downcast::<i64>(args),
        _ => regexp_extract_all_downcast::<i32>(args),
    }
}

fn regexp_extract_all_downcast<O: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [values_arr, pattern_arr, idx_arr] = take_function_args(SparkRegexpExtractAll::NAME, args)?;
    let values = values_arr.as_any().downcast_ref::<GenericStringArray<O>>();
    let pattern = pattern_arr.as_any().downcast_ref::<GenericStringArray<O>>();
    let idx = opt_downcast_arg!(idx_arr, Int64Array);

    // Try the other offset size as fallback for mixed Utf8/LargeUtf8 inputs
    let pattern_i32_storage;
    let pattern_i64_storage;
    let pattern_ref: Option<&dyn StringArrayLike> = match pattern {
        Some(p) => Some(p),
        None => {
            pattern_i32_storage = pattern_arr
                .as_any()
                .downcast_ref::<GenericStringArray<i32>>();
            if let Some(p) = pattern_i32_storage {
                Some(p as &dyn StringArrayLike)
            } else {
                pattern_i64_storage = pattern_arr
                    .as_any()
                    .downcast_ref::<GenericStringArray<i64>>();
                pattern_i64_storage.map(|p| p as &dyn StringArrayLike)
            }
        }
    };

    match (values, pattern_ref, idx.as_ref()) {
        (Some(values), Some(pattern), Some(idx)) => {
            let pattern_len = pattern.len_();
            let idx_len = idx.len();

            let pattern_scalar_opt = (pattern_len == 1 && pattern.is_valid_(0))
                .then(|| parse_regex(pattern.value_(0)))
                .transpose()?;
            let idx_scalar_opt = (idx_len == 1 && idx.is_valid(0)).then(|| idx.value(0));
            let is_pattern_null = pattern_len == 1 && pattern.is_null_(0);
            let is_idx_null = idx_len == 1 && idx.is_null(0);

            let mut builder = ListBuilder::new(StringBuilder::new());
            for i in 0..args[0].len() {
                let pattern_is_null = if pattern_len == 1 {
                    is_pattern_null
                } else {
                    pattern.is_null_(i)
                };
                let idx_is_null = if idx_len == 1 {
                    is_idx_null
                } else {
                    idx.is_null(i)
                };

                if pattern_is_null || idx_is_null || values.is_null(i) {
                    builder.append_null();
                } else {
                    let re = pattern_scalar_opt
                        .as_ref()
                        .map_or_else(|| parse_regex(pattern.value_(i)), |re| Ok(re.clone()))?;
                    let group_idx = idx_scalar_opt.unwrap_or_else(|| idx.value(i));
                    let matches = extract_all_matches(values.value(i), &re, group_idx)?;
                    builder.append_value(matches);
                }
            }
            let array: ListArray = builder.finish();
            Ok(Arc::new(array))
        }
        _ => Err(generic_internal_err(
            SparkRegexpExtractAll::NAME,
            "Could not downcast arguments to arrow arrays",
        )),
    }
}

trait StringArrayLike {
    fn len_(&self) -> usize;
    fn is_valid_(&self, i: usize) -> bool;
    fn is_null_(&self, i: usize) -> bool;
    fn value_(&self, i: usize) -> &str;
}

impl<O: OffsetSizeTrait> StringArrayLike for GenericStringArray<O> {
    fn len_(&self) -> usize {
        self.len()
    }
    fn is_valid_(&self, i: usize) -> bool {
        self.is_valid(i)
    }
    fn is_null_(&self, i: usize) -> bool {
        self.is_null(i)
    }
    fn value_(&self, i: usize) -> &str {
        self.value(i)
    }
}

fn parse_regex(pattern: &str) -> Result<Regex> {
    Regex::new(pattern)
        .map_err(|_| generic_exec_err(SparkRegexpExtractAll::NAME, "Invalid regex pattern"))
}

fn extract_all_matches(value: &str, re: &Regex, group_idx: i64) -> Result<Vec<Option<String>>> {
    if group_idx < 0 {
        return Err(generic_exec_err(
            SparkRegexpExtractAll::NAME,
            &format!("The group index must be non-negative, but got {group_idx}"),
        ));
    }
    let group_idx = group_idx as usize;
    let mut results = Vec::new();
    for caps in re.captures_iter(value) {
        let matched = caps.get(group_idx).map(|m| m.as_str().to_string());
        results.push(matched);
    }
    Ok(results)
}
