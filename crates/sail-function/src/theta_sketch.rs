use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::{
    DataType, Float32Type, Float64Type, Int32Type, Int64Type, UInt32Type, UInt64Type,
};
use datafusion_common::{DataFusionError, Result, exec_err};
use datasketches::hash::value::{canonical_float, raw_bytes};
use datasketches::theta::{
    CompactThetaSketch, ThetaANotB, ThetaIntersection, ThetaSketch, ThetaSketchBuilder,
    ThetaUnionBuilder,
};

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
    ThetaSketchBuilder::default()
        .lg_k(lg_nom_entries.max(5))
        .build()
}

pub(crate) fn default_seed_hash() -> u16 {
    ThetaSketchBuilder::default().build().seed_hash()
}

pub(crate) fn empty_compact_sketch_bytes() -> Result<Vec<u8>> {
    serialize_compact_sketch(vec![], MAX_THETA, default_seed_hash(), true)
}

pub(crate) fn estimate_sketch(bytes: &[u8], function_name: &str) -> Result<i64> {
    let sketch = deserialize_sketch(bytes, function_name)?;
    Ok(sketch.estimate().round() as i64)
}

pub(crate) fn normalize_sketch_bytes(bytes: &[u8], function_name: &str) -> Result<Vec<u8>> {
    let sketch = deserialize_sketch(bytes, function_name)?;
    serialize_compact_sketch(
        sketch.iter().map(|entry| entry.hash()).collect(),
        sketch.theta64(),
        sketch.seed_hash(),
        sketch.is_empty(),
    )
}

pub(crate) fn update_sketch_from_array(sketch: &mut ThetaSketch, values: &ArrayRef) -> Result<()> {
    // Downcast once outside the row loop (the data type and concrete array are
    // loop-invariant); `iter().flatten()` skips nulls without a per-row branch.
    match values.data_type() {
        DataType::Int32 => {
            for v in values.as_primitive::<Int32Type>().iter().flatten() {
                sketch.update(v as i64);
            }
        }
        DataType::Int64 => {
            for v in values.as_primitive::<Int64Type>().iter().flatten() {
                sketch.update(v);
            }
        }
        DataType::UInt32 => {
            for v in values.as_primitive::<UInt32Type>().iter().flatten() {
                sketch.update(v as u64);
            }
        }
        DataType::UInt64 => {
            for v in values.as_primitive::<UInt64Type>().iter().flatten() {
                sketch.update(v);
            }
        }
        DataType::Float32 => {
            for v in values.as_primitive::<Float32Type>().iter().flatten() {
                sketch.update(canonical_float::from_f32(v));
            }
        }
        DataType::Float64 => {
            for v in values.as_primitive::<Float64Type>().iter().flatten() {
                sketch.update(canonical_float::from_f64(v));
            }
        }
        DataType::Binary => {
            for v in values.as_binary::<i32>().iter().flatten() {
                if !v.is_empty() {
                    sketch.update(raw_bytes::from_slice(v));
                }
            }
        }
        DataType::Utf8 => {
            for v in values.as_string::<i32>().iter().flatten() {
                if !v.is_empty() {
                    sketch.update(raw_bytes::from_str(v));
                }
            }
        }
        DataType::LargeUtf8 => {
            for v in values.as_string::<i64>().iter().flatten() {
                if !v.is_empty() {
                    sketch.update(raw_bytes::from_str(v));
                }
            }
        }
        DataType::Utf8View => {
            for v in values.as_string_view().iter().flatten() {
                if !v.is_empty() {
                    sketch.update(raw_bytes::from_str(v));
                }
            }
        }
        DataType::List(_) => {
            let mut scratch = Vec::new();
            for cell in values.as_list::<i32>().iter().flatten() {
                update_sketch_from_list(sketch, &cell, &mut scratch)?;
            }
        }
        DataType::LargeList(_) => {
            let mut scratch = Vec::new();
            for cell in values.as_list::<i64>().iter().flatten() {
                update_sketch_from_list(sketch, &cell, &mut scratch)?;
            }
        }
        DataType::Null => {}
        data_type => {
            return exec_err!("theta_sketch_agg does not support input type {data_type}");
        }
    }
    Ok(())
}

pub(crate) fn compact_update_sketch_bytes(
    sketch: &ThetaSketch,
    lg_nom_entries: u8,
) -> Result<Vec<u8>> {
    let compact = sketch.compact(true);
    let mut entries: Vec<u64> = compact.iter().map(|entry| entry.hash()).collect();
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
    let mut union = ThetaUnionBuilder::default()
        .lg_k(lg_nom_entries.max(5))
        .build();
    for bytes in sketches {
        let sketch = deserialize_sketch(bytes, function_name)?;
        union.update(&sketch).map_err(|error| {
            DataFusionError::Execution(format!(
                "{function_name} could not union theta sketches: {error}"
            ))
        })?;
    }

    let sketch = union.to_sketch(true);
    let mut entries: Vec<u64> = sketch.iter().map(|entry| entry.hash()).collect();
    let mut theta = sketch.theta64();
    trim_entries(&mut entries, &mut theta, lg_nom_entries);
    serialize_compact_sketch(entries, theta, sketch.seed_hash(), sketch.is_empty())
}

pub(crate) fn intersect_sketch_bytes(
    left: &[u8],
    right: &[u8],
    function_name: &str,
) -> Result<Vec<u8>> {
    let left = deserialize_sketch(left, function_name)?;
    let right = deserialize_sketch(right, function_name)?;
    let mut intersection = ThetaIntersection::default();
    intersection.update(&left).map_err(|error| {
        DataFusionError::Execution(format!(
            "{function_name} could not intersect theta sketches: {error}"
        ))
    })?;
    intersection.update(&right).map_err(|error| {
        DataFusionError::Execution(format!(
            "{function_name} could not intersect theta sketches: {error}"
        ))
    })?;
    Ok(intersection.to_sketch(true).serialize_compressed())
}

pub(crate) fn difference_sketch_bytes(
    left: &[u8],
    right: &[u8],
    function_name: &str,
) -> Result<Vec<u8>> {
    let left = deserialize_sketch(left, function_name)?;
    let right = deserialize_sketch(right, function_name)?;
    let difference = ThetaANotB::default()
        .compute(&left, &right, true)
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "{function_name} could not subtract theta sketches: {error}"
            ))
        })?;
    Ok(difference.serialize_compressed())
}

fn update_sketch_from_list(
    sketch: &mut ThetaSketch,
    values: &ArrayRef,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    if values.is_empty() {
        return Ok(());
    }

    // Spark hashes the whole array as one entity, so the element bytes must be
    // concatenated into a contiguous buffer. `scratch` is reused across list cells
    // (cleared per cell) to avoid a fresh allocation for every array value.
    match values.data_type() {
        DataType::Int32 => {
            let values = values.as_primitive::<Int32Type>();
            scratch.clear();
            scratch.reserve(values.len() * std::mem::size_of::<i32>());
            for row in 0..values.len() {
                let value = if values.is_null(row) {
                    0
                } else {
                    values.value(row)
                };
                scratch.extend_from_slice(&value.to_le_bytes());
            }
            sketch.update(raw_bytes::from_slice(scratch.as_slice()));
        }
        DataType::Int64 => {
            let values = values.as_primitive::<Int64Type>();
            scratch.clear();
            scratch.reserve(values.len() * std::mem::size_of::<i64>());
            for row in 0..values.len() {
                let value = if values.is_null(row) {
                    0
                } else {
                    values.value(row)
                };
                scratch.extend_from_slice(&value.to_le_bytes());
            }
            sketch.update(raw_bytes::from_slice(scratch.as_slice()));
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
) -> Result<Vec<u8>> {
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
    // The crate builders require lg_k >= 5, while Spark accepts lgNomEntries = 4.
    let sketch = CompactThetaSketch::deserialize(&bytes).map_err(|error| {
        DataFusionError::Internal(format!("generated invalid theta sketch: {error}"))
    })?;
    Ok(sketch.serialize_compressed())
}
