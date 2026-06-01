use std::any::Any;
use std::fmt::Debug;

use arrow::array::{Array, ArrayRef, BinaryArray};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion_common::{exec_err, DataFusionError, Result, ScalarValue};

use super::utils::get_scalar_value;
use crate::theta_sketch::{
    compact_update_sketch_bytes, empty_compact_sketch_bytes, intersect_sketch_bytes,
    new_update_sketch, normalize_sketch_bytes, union_sketches, update_sketch_from_array,
    validate_lg_nom_entries, DEFAULT_LG_NOM_ENTRIES,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ThetaSketchAggFunction {
    signature: Signature,
}

impl Default for ThetaSketchAggFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl ThetaSketchAggFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(1), TypeSignature::Any(2)],
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for ThetaSketchAggFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "theta_sketch_agg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        validate_theta_sketch_agg_types("theta_sketch_agg", arg_types)?;
        Ok(DataType::Binary)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let lg_nom_entries = resolve_lg_nom_entries(&acc_args, 1, "theta_sketch_agg")?;
        Ok(Box::new(ThetaSketchAggAccumulator::new(lg_nom_entries)))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Field::new(
            format_state_name(args.name, "theta_sketch"),
            DataType::Binary,
            true,
        )
        .into()])
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ThetaUnionAggFunction {
    signature: Signature,
}

impl Default for ThetaUnionAggFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl ThetaUnionAggFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(1), TypeSignature::Any(2)],
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for ThetaUnionAggFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "theta_union_agg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        validate_binary_agg_types("theta_union_agg", arg_types, true)?;
        Ok(DataType::Binary)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let lg_nom_entries = resolve_lg_nom_entries(&acc_args, 1, "theta_union_agg")?;
        Ok(Box::new(ThetaUnionAggAccumulator::new(lg_nom_entries)))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Field::new(
            format_state_name(args.name, "theta_union"),
            DataType::Binary,
            true,
        )
        .into()])
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ThetaIntersectionAggFunction {
    signature: Signature,
}

impl Default for ThetaIntersectionAggFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl ThetaIntersectionAggFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(vec![TypeSignature::Any(1)], Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for ThetaIntersectionAggFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "theta_intersection_agg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        validate_binary_agg_types("theta_intersection_agg", arg_types, false)?;
        Ok(DataType::Binary)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ThetaIntersectionAggAccumulator::new()))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Field::new(
            format_state_name(args.name, "theta_intersection"),
            DataType::Binary,
            true,
        )
        .into()])
    }
}

#[derive(Debug)]
struct ThetaSketchAggAccumulator {
    sketch: datasketches::theta::ThetaSketch,
    merged: Option<Vec<u8>>,
    lg_nom_entries: u8,
}

impl ThetaSketchAggAccumulator {
    fn new(lg_nom_entries: u8) -> Self {
        Self {
            sketch: new_update_sketch(lg_nom_entries),
            merged: None,
            lg_nom_entries,
        }
    }

    fn evaluate_bytes(&self) -> Result<Vec<u8>> {
        let update_bytes = compact_update_sketch_bytes(&self.sketch, self.lg_nom_entries)?;
        if let Some(merged) = &self.merged {
            union_sketches(
                [merged.as_slice(), update_bytes.as_slice()],
                self.lg_nom_entries,
                "theta_sketch_agg",
            )
        } else {
            Ok(update_bytes)
        }
    }

    fn merge_one(&mut self, bytes: &[u8]) -> Result<()> {
        self.merged = Some(if let Some(merged) = &self.merged {
            union_sketches(
                [merged.as_slice(), bytes],
                self.lg_nom_entries,
                "theta_sketch_agg",
            )?
        } else {
            union_sketches([bytes], self.lg_nom_entries, "theta_sketch_agg")?
        });
        Ok(())
    }
}

impl Accumulator for ThetaSketchAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        update_sketch_from_array(&mut self.sketch, &values[0])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Binary(Some(self.evaluate_bytes()?)))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self
                .merged
                .as_ref()
                .map(|bytes| bytes.capacity())
                .unwrap_or_default()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(Some(self.evaluate_bytes()?))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let Some(states) = as_binary_array(&states[0], "theta_sketch_agg state")? else {
            return Ok(());
        };
        for row in 0..states.len() {
            if !states.is_null(row) {
                self.merge_one(states.value(row))?;
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct ThetaUnionAggAccumulator {
    sketch: Option<Vec<u8>>,
    lg_nom_entries: u8,
}

impl ThetaUnionAggAccumulator {
    fn new(lg_nom_entries: u8) -> Self {
        Self {
            sketch: None,
            lg_nom_entries,
        }
    }

    fn merge_one(&mut self, bytes: &[u8]) -> Result<()> {
        self.sketch = Some(if let Some(sketch) = &self.sketch {
            union_sketches(
                [sketch.as_slice(), bytes],
                self.lg_nom_entries,
                "theta_union_agg",
            )?
        } else {
            union_sketches([bytes], self.lg_nom_entries, "theta_union_agg")?
        });
        Ok(())
    }

    fn evaluate_bytes(&self) -> Result<Vec<u8>> {
        match &self.sketch {
            Some(sketch) => Ok(sketch.clone()),
            None => empty_compact_sketch_bytes(),
        }
    }
}

impl Accumulator for ThetaUnionAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(values) = as_binary_array(&values[0], "theta_union_agg")? else {
            return Ok(());
        };
        for row in 0..values.len() {
            if !values.is_null(row) {
                self.merge_one(values.value(row))?;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Binary(Some(self.evaluate_bytes()?)))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self
                .sketch
                .as_ref()
                .map(|bytes| bytes.capacity())
                .unwrap_or_default()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(Some(self.evaluate_bytes()?))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let Some(states) = as_binary_array(&states[0], "theta_union_agg state")? else {
            return Ok(());
        };
        for row in 0..states.len() {
            if !states.is_null(row) {
                self.merge_one(states.value(row))?;
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct ThetaIntersectionAggAccumulator {
    sketch: Option<Vec<u8>>,
}

impl ThetaIntersectionAggAccumulator {
    fn new() -> Self {
        Self { sketch: None }
    }

    fn merge_one(&mut self, bytes: &[u8]) -> Result<()> {
        let bytes = normalize_sketch_bytes(bytes, "theta_intersection_agg")?;
        self.sketch = Some(if let Some(sketch) = &self.sketch {
            intersect_sketch_bytes(sketch, &bytes, "theta_intersection_agg")?
        } else {
            bytes
        });
        Ok(())
    }

    fn evaluate_bytes(&self) -> Result<Vec<u8>> {
        match &self.sketch {
            Some(sketch) => Ok(sketch.clone()),
            None => exec_err!(
                "theta_intersection_agg cannot produce a result without any non-null input sketches"
            ),
        }
    }
}

impl Accumulator for ThetaIntersectionAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(values) = as_binary_array(&values[0], "theta_intersection_agg")? else {
            return Ok(());
        };
        for row in 0..values.len() {
            if !values.is_null(row) {
                self.merge_one(values.value(row))?;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Binary(Some(self.evaluate_bytes()?)))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self
                .sketch
                .as_ref()
                .map(|bytes| bytes.capacity())
                .unwrap_or_default()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(self.sketch.clone())])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let Some(states) = as_binary_array(&states[0], "theta_intersection_agg state")? else {
            return Ok(());
        };
        for row in 0..states.len() {
            if !states.is_null(row) {
                self.merge_one(states.value(row))?;
            }
        }
        Ok(())
    }
}

fn resolve_lg_nom_entries(args: &AccumulatorArgs, index: usize, function_name: &str) -> Result<u8> {
    let Some(expr) = args.exprs.get(index) else {
        return validate_lg_nom_entries(DEFAULT_LG_NOM_ENTRIES, function_name);
    };
    let value = get_scalar_value(expr).map_err(|_| {
        DataFusionError::Plan(format!(
            "{function_name} requires lgNomEntries to be a constant integer"
        ))
    })?;
    let value = match value {
        ScalarValue::Int32(Some(value)) => value,
        ScalarValue::Int64(Some(value)) => i32::try_from(value).map_err(|_| {
            DataFusionError::Plan(format!(
                "{function_name} requires lgNomEntries between 4 and 26, got {value}"
            ))
        })?,
        value => {
            return exec_err!(
                "{function_name} requires lgNomEntries to be a non-null integer literal, got {}",
                value.data_type()
            )
        }
    };
    validate_lg_nom_entries(value, function_name)
}

fn validate_theta_sketch_agg_types(function_name: &str, arg_types: &[DataType]) -> Result<()> {
    if !(1..=2).contains(&arg_types.len()) {
        return exec_err!(
            "{function_name} requires 1 or 2 arguments, got {}",
            arg_types.len()
        );
    }
    match &arg_types[0] {
        DataType::Int32
        | DataType::Int64
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::Binary
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Null => {}
        DataType::List(field) | DataType::LargeList(field)
            if matches!(field.data_type(), DataType::Int32 | DataType::Int64) => {}
        data_type => {
            return exec_err!("{function_name} does not support input type {data_type}");
        }
    }
    if arg_types.len() == 2 && !matches!(arg_types[1], DataType::Int32 | DataType::Int64) {
        return exec_err!(
            "{function_name} requires an integer lgNomEntries argument, got {}",
            arg_types[1]
        );
    }
    Ok(())
}

fn validate_binary_agg_types(
    function_name: &str,
    arg_types: &[DataType],
    has_lg_nom_entries: bool,
) -> Result<()> {
    let expected = if has_lg_nom_entries { 1..=2 } else { 1..=1 };
    if !expected.contains(&arg_types.len()) {
        return exec_err!(
            "{function_name} received invalid argument count: {}",
            arg_types.len()
        );
    }
    if !matches!(arg_types[0], DataType::Binary | DataType::Null) {
        return exec_err!(
            "{function_name} requires a binary sketch argument, got {}",
            arg_types[0]
        );
    }
    if arg_types.len() == 2 && !matches!(arg_types[1], DataType::Int32 | DataType::Int64) {
        return exec_err!(
            "{function_name} requires an integer lgNomEntries argument, got {}",
            arg_types[1]
        );
    }
    Ok(())
}

fn as_binary_array<'a>(array: &'a ArrayRef, context: &str) -> Result<Option<&'a BinaryArray>> {
    if matches!(array.data_type(), DataType::Null) {
        return Ok(None);
    }
    array
        .as_any()
        .downcast_ref::<BinaryArray>()
        .map(Some)
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "{context} expected BinaryArray, got {}",
                array.data_type()
            ))
        })
}
