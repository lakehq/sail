use arrow::array::{
    Array, ArrayRef, BinaryArray, Int32Array, Int64Array, LargeStringArray, StringArray,
    StringViewArray,
};
use arrow::datatypes::DataType;
use datafusion_common::{exec_err, DataFusionError, Result};
use datasketches::hash_value::{raw_bytes, sign_extend};
use datasketches::hll::{HllSketch, HllType, HllUnion};

pub(crate) const MIN_LG_CONFIG_K: i32 = 4;
pub(crate) const MAX_LG_CONFIG_K: i32 = 21;
pub const DEFAULT_LG_CONFIG_K: i32 = 12;
const DEFAULT_UNION_LG_CONFIG_K: u8 = 21;

pub(crate) fn validate_lg_config_k(value: i32, function_name: &str) -> Result<u8> {
    if !(MIN_LG_CONFIG_K..=MAX_LG_CONFIG_K).contains(&value) {
        exec_err!(
            "{function_name} requires lgConfigK between {MIN_LG_CONFIG_K} and {MAX_LG_CONFIG_K}, got {value}"
        )
    } else {
        Ok(value as u8)
    }
}

pub(crate) fn new_hll_sketch(lg_config_k: u8) -> HllSketch {
    HllSketch::new(lg_config_k, HllType::Hll8)
}

pub(crate) fn empty_hll_sketch_bytes() -> Vec<u8> {
    new_hll_sketch(DEFAULT_LG_CONFIG_K as u8).serialize()
}

pub(crate) fn empty_hll_union_bytes() -> Vec<u8> {
    new_hll_sketch(DEFAULT_UNION_LG_CONFIG_K).serialize()
}

pub(crate) fn estimate_hll_sketch(bytes: &[u8], function_name: &str) -> Result<i64> {
    let sketch = deserialize_hll_sketch(bytes, function_name)?;
    Ok(sketch.estimate().round() as i64)
}

pub(crate) fn normalize_hll_sketch_bytes(bytes: &[u8], function_name: &str) -> Result<Vec<u8>> {
    Ok(deserialize_hll_sketch(bytes, function_name)?.serialize())
}

pub(crate) fn update_hll_sketch_from_array(
    sketch: &mut HllSketch,
    values: &ArrayRef,
) -> Result<()> {
    for row in 0..values.len() {
        update_hll_sketch_from_array_value(sketch, values.as_ref(), row)?;
    }
    Ok(())
}

pub(crate) fn union_hll_sketch_bytes(
    left: &[u8],
    right: &[u8],
    allow_different_lg_config_k: bool,
    function_name: &str,
) -> Result<Vec<u8>> {
    let left = deserialize_hll_sketch(left, function_name)?;
    let right = deserialize_hll_sketch(right, function_name)?;
    if !allow_different_lg_config_k && left.lg_config_k() != right.lg_config_k() {
        return exec_err!(
            "{function_name} cannot union HLL sketches with different lgConfigK values: {} and {}",
            left.lg_config_k(),
            right.lg_config_k()
        );
    }
    let mut union = HllUnion::new(left.lg_config_k().min(right.lg_config_k()));
    union.update(&left);
    union.update(&right);
    Ok(union.to_sketch(HllType::Hll8).serialize())
}

pub(crate) fn union_hll_sketches<'a, I>(
    sketches: I,
    allow_different_lg_config_k: bool,
    function_name: &str,
) -> Result<Vec<u8>>
where
    I: IntoIterator<Item = &'a [u8]>,
{
    let mut union: Option<(u8, HllUnion)> = None;
    for bytes in sketches {
        let sketch = deserialize_hll_sketch(bytes, function_name)?;
        match &mut union {
            Some((expected_lg_config_k, union)) => {
                if !allow_different_lg_config_k && *expected_lg_config_k != sketch.lg_config_k() {
                    return exec_err!(
                        "{function_name} cannot union HLL sketches with different lgConfigK values: {} and {}",
                        *expected_lg_config_k,
                        sketch.lg_config_k()
                    );
                }
                union.update(&sketch);
                *expected_lg_config_k = union.lg_config_k();
            }
            None => {
                let mut next_union = HllUnion::new(sketch.lg_config_k());
                next_union.update(&sketch);
                union = Some((next_union.lg_config_k(), next_union));
            }
        }
    }

    Ok(union
        .map(|(_, union)| union.to_sketch(HllType::Hll8).serialize())
        .unwrap_or_else(empty_hll_sketch_bytes))
}

fn update_hll_sketch_from_array_value(
    sketch: &mut HllSketch,
    values: &dyn Array,
    row: usize,
) -> Result<()> {
    if values.is_null(row) {
        return Ok(());
    }

    match values.data_type() {
        DataType::Int32 => {
            let values = values
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("HLL sketch expected Int32Array".to_string())
                })?;
            sketch.update(sign_extend::from_i32(values.value(row)));
        }
        DataType::Int64 => {
            let values = values
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("HLL sketch expected Int64Array".to_string())
                })?;
            sketch.update(values.value(row));
        }
        DataType::Binary => {
            let values = values
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("HLL sketch expected BinaryArray".to_string())
                })?;
            let value = values.value(row);
            if !value.is_empty() {
                sketch.update(raw_bytes::from_slice(value));
            }
        }
        DataType::Utf8 => {
            let values = values
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("HLL sketch expected StringArray".to_string())
                })?;
            let value = values.value(row);
            if !value.is_empty() {
                sketch.update(raw_bytes::from_str(value));
            }
        }
        DataType::LargeUtf8 => {
            let values = values
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("HLL sketch expected LargeStringArray".to_string())
                })?;
            let value = values.value(row);
            if !value.is_empty() {
                sketch.update(raw_bytes::from_str(value));
            }
        }
        DataType::Utf8View => {
            let values = values
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("HLL sketch expected StringViewArray".to_string())
                })?;
            let value = values.value(row);
            if !value.is_empty() {
                sketch.update(raw_bytes::from_str(value));
            }
        }
        DataType::Null => {}
        data_type => {
            return exec_err!("hll_sketch_agg does not support input type {data_type}");
        }
    }
    Ok(())
}

fn deserialize_hll_sketch(bytes: &[u8], function_name: &str) -> Result<HllSketch> {
    HllSketch::deserialize(bytes).map_err(|error| {
        DataFusionError::Execution(format!(
            "{function_name} received an invalid HLL sketch: {error}"
        ))
    })
}
