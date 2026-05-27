use std::collections::HashSet;

use arrow::array::{
    Array, ArrayRef, BinaryArray, Float32Array, Float64Array, Int32Array, Int64Array,
    LargeListArray, LargeStringArray, ListArray, StringArray, StringViewArray, UInt32Array,
    UInt64Array,
};
use datafusion_common::{exec_err, DataFusionError, Result};
use datasketches::hash_value::{canonical_float, raw_bytes};
use datasketches::theta::{CompactThetaSketch, ThetaSketch};

pub(crate) const MIN_LG_NOM_ENTRIES: i32 = 4;
pub(crate) const MAX_LG_NOM_ENTRIES: i32 = 26;
pub const DEFAULT_LG_NOM_ENTRIES: i32 = 12;

const MAX_THETA: u64 = i64::MAX as u64;
const COMPACT_FAMILY_ID: u8 = 3;
const SERIAL_VERSION: u8 = 3;
const FLAG_READ_ONLY: u8 = 1 << 1;
const FLAG_EMPTY: u8 = 1 << 2;
const FLAG_COMPACT: u8 = 1 << 3;
const FLAG_ORDERED: u8 = 1 << 4;

pub(crate) fn validate_lg_nom_entries(value: i32, function_name: &str) -> Result<u8> {
    if !(MIN_LG_NOM_ENTRIES..=MAX_LG_NOM_ENTRIES).contains(&value) {
        exec_err!(
            "{function_name} requires lgNomEntries between {MIN_LG_NOM_ENTRIES} and {MAX_LG_NOM_ENTRIES}, got {value}"
        )
    } else {
        Ok(value as u8)
    }
}

pub(crate) fn new_update_sketch(lg_nom_entries: u8) -> ThetaSketch {
    ThetaSketch::builder().lg_k(lg_nom_entries.max(5)).build()
}

pub(crate) fn default_seed_hash() -> u16 {
    ThetaSketch::builder().build().seed_hash()
}

pub(crate) fn empty_compact_sketch_bytes() -> Vec<u8> {
    serialize_compact_sketch(vec![], MAX_THETA, default_seed_hash(), true)
}

pub(crate) fn estimate_sketch(bytes: &[u8], function_name: &str) -> Result<i64> {
    let sketch = deserialize_sketch(bytes, function_name)?;
    Ok(sketch.estimate().round() as i64)
}

pub(crate) fn normalize_sketch_bytes(bytes: &[u8], function_name: &str) -> Result<Vec<u8>> {
    let sketch = deserialize_sketch(bytes, function_name)?;
    Ok(serialize_compact_sketch(
        sketch.iter().collect(),
        sketch.theta64(),
        sketch.seed_hash(),
        sketch.is_empty(),
    ))
}

pub(crate) fn update_sketch_from_array(sketch: &mut ThetaSketch, values: &ArrayRef) -> Result<()> {
    for row in 0..values.len() {
        update_sketch_from_array_value(sketch, values.as_ref(), row)?;
    }
    Ok(())
}

pub(crate) fn compact_update_sketch_bytes(sketch: &ThetaSketch, lg_nom_entries: u8) -> Vec<u8> {
    let compact = sketch.compact(true);
    let mut entries: Vec<u64> = compact.iter().collect();
    let mut theta = compact.theta64();
    trim_entries(&mut entries, &mut theta, lg_nom_entries);
    serialize_compact_sketch(entries, theta, compact.seed_hash(), compact.is_empty())
}

pub(crate) fn union_sketch_bytes(
    left: &[u8],
    right: &[u8],
    lg_nom_entries: u8,
    function_name: &str,
) -> Result<Vec<u8>> {
    union_sketches([left, right], lg_nom_entries, function_name)
}

pub(crate) fn union_sketches<'a, I>(
    sketches: I,
    lg_nom_entries: u8,
    function_name: &str,
) -> Result<Vec<u8>>
where
    I: IntoIterator<Item = &'a [u8]>,
{
    let mut entries = HashSet::new();
    let mut theta = MAX_THETA;
    let mut seed_hash = None;
    let mut saw_non_empty = false;

    for bytes in sketches {
        let sketch = deserialize_sketch(bytes, function_name)?;
        update_seed_hash(&mut seed_hash, &sketch, function_name)?;
        saw_non_empty |= !sketch.is_empty();
        theta = theta.min(sketch.theta64());
        for hash in sketch.iter() {
            entries.insert(hash);
        }
    }

    let mut entries: Vec<u64> = entries.into_iter().filter(|hash| *hash < theta).collect();
    trim_entries(&mut entries, &mut theta, lg_nom_entries);
    let empty = !saw_non_empty && entries.is_empty() && theta == MAX_THETA;
    Ok(serialize_compact_sketch(
        entries,
        theta,
        seed_hash.unwrap_or_else(default_seed_hash),
        empty,
    ))
}

pub(crate) fn intersect_sketch_bytes(
    left: &[u8],
    right: &[u8],
    function_name: &str,
) -> Result<Vec<u8>> {
    let left = deserialize_sketch(left, function_name)?;
    let right = deserialize_sketch(right, function_name)?;
    check_seed_hashes(&left, &right, function_name)?;

    let theta = left.theta64().min(right.theta64());
    let right_entries: HashSet<u64> = right.iter().filter(|hash| *hash < theta).collect();
    let entries: Vec<u64> = left
        .iter()
        .filter(|hash| *hash < theta && right_entries.contains(hash))
        .collect();
    let empty = left.is_empty() || right.is_empty() || (theta == MAX_THETA && entries.is_empty());

    Ok(serialize_compact_sketch(
        entries,
        theta,
        output_seed_hash(&left, &right),
        empty,
    ))
}

pub(crate) fn difference_sketch_bytes(
    left: &[u8],
    right: &[u8],
    function_name: &str,
) -> Result<Vec<u8>> {
    let left = deserialize_sketch(left, function_name)?;
    let right = deserialize_sketch(right, function_name)?;
    check_seed_hashes(&left, &right, function_name)?;

    let theta = left.theta64().min(right.theta64());
    let right_entries: HashSet<u64> = right.iter().filter(|hash| *hash < theta).collect();
    let entries: Vec<u64> = left
        .iter()
        .filter(|hash| *hash < theta && !right_entries.contains(hash))
        .collect();
    let empty = left.is_empty() || (theta == MAX_THETA && entries.is_empty());

    Ok(serialize_compact_sketch(
        entries,
        theta,
        output_seed_hash(&left, &right),
        empty,
    ))
}

fn update_sketch_from_array_value(
    sketch: &mut ThetaSketch,
    values: &dyn Array,
    row: usize,
) -> Result<()> {
    if values.is_null(row) {
        return Ok(());
    }

    match values.data_type() {
        arrow::datatypes::DataType::Int32 => {
            let values = values
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("theta sketch expected Int32Array".to_string())
                })?;
            sketch.update(values.value(row) as i64);
        }
        arrow::datatypes::DataType::Int64 => {
            let values = values
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("theta sketch expected Int64Array".to_string())
                })?;
            sketch.update(values.value(row));
        }
        arrow::datatypes::DataType::UInt32 => {
            let values = values
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("theta sketch expected UInt32Array".to_string())
                })?;
            sketch.update(values.value(row) as u64);
        }
        arrow::datatypes::DataType::UInt64 => {
            let values = values
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("theta sketch expected UInt64Array".to_string())
                })?;
            sketch.update(values.value(row));
        }
        arrow::datatypes::DataType::Float32 => {
            let values = values
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("theta sketch expected Float32Array".to_string())
                })?;
            sketch.update(canonical_float::from_f32(values.value(row)));
        }
        arrow::datatypes::DataType::Float64 => {
            let values = values
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("theta sketch expected Float64Array".to_string())
                })?;
            sketch.update(canonical_float::from_f64(values.value(row)));
        }
        arrow::datatypes::DataType::Binary => {
            let values = values
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("theta sketch expected BinaryArray".to_string())
                })?;
            let value = values.value(row);
            if !value.is_empty() {
                sketch.update(raw_bytes::from_slice(value));
            }
        }
        arrow::datatypes::DataType::Utf8 => {
            let values = values
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("theta sketch expected StringArray".to_string())
                })?;
            let value = values.value(row);
            if !value.is_empty() {
                sketch.update(raw_bytes::from_str(value));
            }
        }
        arrow::datatypes::DataType::LargeUtf8 => {
            let values = values
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("theta sketch expected LargeStringArray".to_string())
                })?;
            let value = values.value(row);
            if !value.is_empty() {
                sketch.update(raw_bytes::from_str(value));
            }
        }
        arrow::datatypes::DataType::Utf8View => {
            let values = values
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("theta sketch expected StringViewArray".to_string())
                })?;
            let value = values.value(row);
            if !value.is_empty() {
                sketch.update(raw_bytes::from_str(value));
            }
        }
        arrow::datatypes::DataType::List(_) => {
            let values = values.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                DataFusionError::Internal("theta sketch expected ListArray".to_string())
            })?;
            update_sketch_from_list(sketch, &values.value(row))?;
        }
        arrow::datatypes::DataType::LargeList(_) => {
            let values = values
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("theta sketch expected LargeListArray".to_string())
                })?;
            update_sketch_from_list(sketch, &values.value(row))?;
        }
        arrow::datatypes::DataType::Null => {}
        data_type => {
            return exec_err!("theta_sketch_agg does not support input type {data_type}");
        }
    }
    Ok(())
}

fn update_sketch_from_list(sketch: &mut ThetaSketch, values: &ArrayRef) -> Result<()> {
    if values.is_empty() {
        return Ok(());
    }

    match values.data_type() {
        arrow::datatypes::DataType::Int32 => {
            let values = values
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "theta sketch expected Int32Array list values".to_string(),
                    )
                })?;
            let mut bytes = Vec::with_capacity(values.len() * std::mem::size_of::<i32>());
            for row in 0..values.len() {
                let value = if values.is_null(row) {
                    0
                } else {
                    values.value(row)
                };
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            sketch.update(raw_bytes::from_slice(&bytes));
        }
        arrow::datatypes::DataType::Int64 => {
            let values = values
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "theta sketch expected Int64Array list values".to_string(),
                    )
                })?;
            let mut bytes = Vec::with_capacity(values.len() * std::mem::size_of::<i64>());
            for row in 0..values.len() {
                let value = if values.is_null(row) {
                    0
                } else {
                    values.value(row)
                };
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            sketch.update(raw_bytes::from_slice(&bytes));
        }
        data_type => {
            return exec_err!("theta_sketch_agg does not support array element type {data_type}");
        }
    }
    Ok(())
}

fn deserialize_sketch(bytes: &[u8], function_name: &str) -> Result<CompactThetaSketch> {
    CompactThetaSketch::deserialize(bytes).map_err(|error| {
        DataFusionError::Execution(format!(
            "{function_name} received an invalid theta sketch: {error}"
        ))
    })
}

fn update_seed_hash(
    seed_hash: &mut Option<u16>,
    sketch: &CompactThetaSketch,
    function_name: &str,
) -> Result<()> {
    if sketch.is_empty() {
        return Ok(());
    }
    match seed_hash {
        Some(expected) if *expected != sketch.seed_hash() => exec_err!(
            "{function_name} received theta sketches with different seed hashes: expected {}, got {}",
            *expected,
            sketch.seed_hash()
        ),
        Some(_) => Ok(()),
        None => {
            *seed_hash = Some(sketch.seed_hash());
            Ok(())
        }
    }
}

fn check_seed_hashes(
    left: &CompactThetaSketch,
    right: &CompactThetaSketch,
    function_name: &str,
) -> Result<()> {
    if !left.is_empty() && !right.is_empty() && left.seed_hash() != right.seed_hash() {
        exec_err!(
            "{function_name} received theta sketches with different seed hashes: expected {}, got {}",
            left.seed_hash(),
            right.seed_hash()
        )
    } else {
        Ok(())
    }
}

fn output_seed_hash(left: &CompactThetaSketch, right: &CompactThetaSketch) -> u16 {
    if !left.is_empty() {
        left.seed_hash()
    } else if !right.is_empty() {
        right.seed_hash()
    } else {
        default_seed_hash()
    }
}

fn trim_entries(entries: &mut Vec<u64>, theta: &mut u64, lg_nom_entries: u8) {
    entries.retain(|hash| *hash != 0 && *hash < *theta);
    entries.sort_unstable();
    entries.dedup();

    let nominal_entries = 1usize << lg_nom_entries;
    if entries.len() > nominal_entries {
        *theta = entries[nominal_entries];
        entries.truncate(nominal_entries);
    }
}

fn serialize_compact_sketch(
    mut entries: Vec<u64>,
    theta: u64,
    seed_hash: u16,
    empty: bool,
) -> Vec<u8> {
    entries.retain(|hash| *hash != 0 && *hash < theta);
    entries.sort_unstable();
    entries.dedup();

    let estimation_mode = theta < MAX_THETA;
    let pre_longs = if estimation_mode {
        3
    } else if empty || entries.len() == 1 {
        1
    } else {
        2
    };

    let mut bytes = Vec::with_capacity((pre_longs as usize * 8) + (entries.len() * 8));
    bytes.push(pre_longs);
    bytes.push(SERIAL_VERSION);
    bytes.push(COMPACT_FAMILY_ID);
    bytes.extend_from_slice(&0u16.to_be_bytes());

    let mut flags = FLAG_READ_ONLY | FLAG_COMPACT | FLAG_ORDERED;
    if empty {
        flags |= FLAG_EMPTY;
    }
    bytes.push(flags);
    bytes.extend_from_slice(&seed_hash.to_le_bytes());

    if pre_longs > 1 {
        bytes.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&0u32.to_be_bytes());
    }
    if estimation_mode {
        bytes.extend_from_slice(&theta.to_le_bytes());
    }
    for hash in entries {
        bytes.extend_from_slice(&hash.to_le_bytes());
    }
    bytes
}
