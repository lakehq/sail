// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// [CREDIT]: https://github.com/apache/datafusion/blob/55.2.0/datafusion/spark/src/function/array/shuffle.rs

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Capacities, FixedSizeListArray, GenericListArray, MutableArrayData,
    OffsetSizeTrait,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::DataType::{FixedSizeList, LargeList, List, Null};
use arrow::datatypes::{DataType, FieldRef};
use datafusion_common::cast::{as_fixed_size_list_array, as_large_list_array, as_list_array};
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::{
    ArrayFunctionArgument, ArrayFunctionSignature, ColumnarValue, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use rand::{RngExt, rng};

use crate::scalar::math::mersenne_twister::MersenneTwister;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkShuffle {
    signature: Signature,
}

impl Default for SparkShuffle {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkShuffle {
    pub fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::OneOf(vec![
                    // Only array argument
                    TypeSignature::ArraySignature(ArrayFunctionSignature::Array {
                        arguments: vec![ArrayFunctionArgument::Array],
                        array_coercion: None,
                    }),
                    // Array + Index (seed) argument
                    TypeSignature::ArraySignature(ArrayFunctionSignature::Array {
                        arguments: vec![ArrayFunctionArgument::Array, ArrayFunctionArgument::Index],
                        array_coercion: None,
                    }),
                ]),
                volatility: Volatility::Volatile,
                parameter_names: None,
            },
        }
    }
}

impl ScalarUDFImpl for SparkShuffle {
    fn name(&self) -> &str {
        "shuffle"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: datafusion_expr::ReturnFieldArgs) -> Result<FieldRef> {
        // Shuffle returns an array with the same type and nullability as the input
        Ok(Arc::clone(&args.arg_fields[0]))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() || args.args.len() > 2 {
            return exec_err!("shuffle expects 1 or 2 argument(s)");
        }

        // Extract seed from second argument if present
        let seed = if args.args.len() == 2 {
            extract_seed(&args.args[1])?
        } else {
            None
        };

        let array = args.args[0].to_array(args.number_rows)?;
        let mut random = RandomIndicesGenerator::new(seed.unwrap_or_else(|| rng().random::<i64>()));
        array_shuffle(&array, &mut random).map(ColumnarValue::Array)
    }
}

/// Extract seed value from ColumnarValue
fn extract_seed(seed_arg: &ColumnarValue) -> Result<Option<i64>> {
    match seed_arg {
        ColumnarValue::Scalar(scalar) => {
            let seed = match scalar {
                ScalarValue::Int64(Some(v)) => Some(*v),
                ScalarValue::Null | ScalarValue::Int64(None) => None,
                _ => {
                    return exec_err!(
                        "shuffle seed must be Int64 type but got '{}'",
                        scalar.data_type()
                    );
                }
            };
            Ok(seed)
        }
        ColumnarValue::Array(_) => {
            exec_err!("shuffle seed must be a scalar value, not an array")
        }
    }
}

/// Port of Spark's `RandomIndicesGenerator`.
///
/// [CREDIT]: https://github.com/apache/spark/blob/v4.2.0-rc6/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/RandomIndicesGenerator.scala
pub struct RandomIndicesGenerator {
    random: MersenneTwister,
}

impl RandomIndicesGenerator {
    pub fn new(seed: i64) -> Self {
        Self {
            random: MersenneTwister::new(seed),
        }
    }

    fn next_indices(&mut self, length: usize) -> Vec<usize> {
        let mut indices = vec![0; length];
        for i in 0..length {
            let j = self.random.next_int_bounded(i as i32 + 1) as usize;
            if j != i {
                indices[i] = indices[j];
            }
            indices[j] = i;
        }
        indices
    }
}

/// Returns a random permutation of each array, where each non-null array advances the
/// generator as in Spark.
pub fn array_shuffle(
    input_array: &ArrayRef,
    random: &mut RandomIndicesGenerator,
) -> Result<ArrayRef> {
    match &input_array.data_type() {
        List(field) => {
            let array = as_list_array(input_array)?;
            general_array_shuffle::<i32>(array, field, random)
        }
        LargeList(field) => {
            let array = as_large_list_array(input_array)?;
            general_array_shuffle::<i64>(array, field, random)
        }
        FixedSizeList(field, _) => {
            let array = as_fixed_size_list_array(input_array)?;
            fixed_size_array_shuffle(array, field, random)
        }
        Null => Ok(Arc::clone(input_array)),
        array_type => exec_err!(
            "shuffle does not support type '{array_type}'; \
        expected types: List, LargeList, FixedSizeList or Null."
        ),
    }
}

fn general_array_shuffle<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
    field: &FieldRef,
    random: &mut RandomIndicesGenerator,
) -> Result<ArrayRef> {
    let values = array.values();
    let original_data = values.to_data();
    let capacity = Capacities::Array(original_data.len());
    let mut offsets = vec![O::usize_as(0)];
    let mut nulls = vec![];
    let mut mutable = MutableArrayData::with_capacities(vec![&original_data], false, capacity);

    for (row_index, offset_window) in array.offsets().windows(2).enumerate() {
        // skip the null value
        if array.is_null(row_index) {
            nulls.push(false);
            offsets.push(offsets[row_index]);
            continue;
        }
        nulls.push(true);
        let start = offset_window[0];
        let end = offset_window[1];
        let length = (end - start).as_usize();
        let start = start.as_usize();

        // Add shuffled elements
        for index in random.next_indices(length) {
            mutable.try_extend(0, start + index, start + index + 1)?;
        }

        offsets.push(offsets[row_index] + O::usize_as(length));
    }

    let data = mutable.freeze();
    Ok(Arc::new(GenericListArray::<O>::try_new(
        Arc::clone(field),
        OffsetBuffer::<O>::new(offsets.into()),
        arrow::array::make_array(data),
        Some(nulls.into()),
    )?))
}

fn fixed_size_array_shuffle(
    array: &FixedSizeListArray,
    field: &FieldRef,
    random: &mut RandomIndicesGenerator,
) -> Result<ArrayRef> {
    let values = array.values();
    let original_data = values.to_data();
    let capacity = Capacities::Array(original_data.len());
    let mut nulls = vec![];
    let mut mutable = MutableArrayData::with_capacities(vec![&original_data], false, capacity);
    let value_length = array.value_length() as usize;

    for row_index in 0..array.len() {
        // skip the null value
        if array.is_null(row_index) {
            nulls.push(false);
            mutable.try_extend(0, 0, value_length)?;
            continue;
        }
        nulls.push(true);

        let start = row_index * value_length;

        // Add shuffled elements
        for index in random.next_indices(value_length) {
            mutable.try_extend(0, start + index, start + index + 1)?;
        }
    }

    let data = mutable.freeze();
    Ok(Arc::new(FixedSizeListArray::try_new(
        Arc::clone(field),
        array.value_length(),
        arrow::array::make_array(data),
        Some(nulls.into()),
    )?))
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use arrow::datatypes::Field;
    use datafusion_expr::ReturnFieldArgs;

    use super::*;

    #[test]
    fn test_shuffle_nullability() {
        let shuffle = SparkShuffle::new();

        // Test with non-nullable array
        let non_nullable_field = Arc::new(Field::new(
            "arr",
            List(Arc::new(Field::new("item", DataType::Int32, true))),
            false, // not nullable
        ));

        let result = shuffle
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&non_nullable_field)],
                scalar_arguments: &[None],
            })
            .unwrap();

        // The result should not be nullable (same as input)
        assert!(!result.is_nullable());
        assert_eq!(result.data_type(), non_nullable_field.data_type());

        // Test with nullable array
        let nullable_field = Arc::new(Field::new(
            "arr",
            List(Arc::new(Field::new("item", DataType::Int32, true))),
            true, // nullable
        ));

        let result = shuffle
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&nullable_field)],
                scalar_arguments: &[None],
            })
            .unwrap();

        // The result should be nullable (same as input)
        assert!(result.is_nullable());
        assert_eq!(result.data_type(), nullable_field.data_type());
    }
}
