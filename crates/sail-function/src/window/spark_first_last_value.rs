use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::error::Result;
use datafusion::logical_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
use datafusion::logical_expr::{
    LimitEffect, PartitionEvaluator, ReversedUDWF, Signature, TypeSignature, Volatility, WindowUDF,
    WindowUDFImpl,
};
use datafusion::scalar::ScalarValue;

pub fn spark_first_value_udwf(ignore_nulls: bool) -> Arc<WindowUDF> {
    Arc::new(WindowUDF::from(SparkFirstLastValue::first(ignore_nulls)))
}

pub fn spark_last_value_udwf(ignore_nulls: bool) -> Arc<WindowUDF> {
    Arc::new(WindowUDF::from(SparkFirstLastValue::last(ignore_nulls)))
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum SparkFirstLastValueKind {
    First,
    Last,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkFirstLastValue {
    kind: SparkFirstLastValueKind,
    ignore_nulls: bool,
    signature: Signature,
}

impl SparkFirstLastValue {
    pub fn first(ignore_nulls: bool) -> Self {
        Self::new(SparkFirstLastValueKind::First, ignore_nulls)
    }

    pub fn last(ignore_nulls: bool) -> Self {
        Self::new(SparkFirstLastValueKind::Last, ignore_nulls)
    }

    pub fn kind(&self) -> SparkFirstLastValueKind {
        self.kind
    }

    pub fn ignore_nulls(&self) -> bool {
        self.ignore_nulls
    }

    fn new(kind: SparkFirstLastValueKind, ignore_nulls: bool) -> Self {
        Self {
            kind,
            ignore_nulls,
            signature: Signature::one_of(
                vec![TypeSignature::Nullary, TypeSignature::Any(1)],
                Volatility::Immutable,
            ),
        }
    }
}

impl WindowUDFImpl for SparkFirstLastValue {
    fn name(&self) -> &str {
        match self.kind {
            SparkFirstLastValueKind::First => "first_value",
            SparkFirstLastValueKind::Last => "last_value",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(SparkFirstLastValueEvaluator {
            kind: self.kind,
            ignore_nulls: self.ignore_nulls,
        }))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<FieldRef> {
        let input_field = field_args
            .input_fields()
            .first()
            .cloned()
            .unwrap_or_else(|| Arc::new(Field::new(field_args.name(), DataType::Null, true)));
        Ok(input_field
            .as_ref()
            .clone()
            .with_name(field_args.name())
            .with_nullable(true)
            .into())
    }

    fn reverse_expr(&self) -> ReversedUDWF {
        match self.kind {
            SparkFirstLastValueKind::First => {
                ReversedUDWF::Reversed(spark_last_value_udwf(self.ignore_nulls))
            }
            SparkFirstLastValueKind::Last => {
                ReversedUDWF::Reversed(spark_first_value_udwf(self.ignore_nulls))
            }
        }
    }

    fn limit_effect(
        &self,
        _args: &[Arc<dyn datafusion_physical_expr::PhysicalExpr>],
    ) -> LimitEffect {
        LimitEffect::None
    }
}

#[derive(Debug)]
struct SparkFirstLastValueEvaluator {
    kind: SparkFirstLastValueKind,
    ignore_nulls: bool,
}

impl PartitionEvaluator for SparkFirstLastValueEvaluator {
    fn evaluate(&mut self, values: &[ArrayRef], range: &Range<usize>) -> Result<ScalarValue> {
        let array = &values[0];
        if range.start == range.end {
            return ScalarValue::try_from(array.data_type());
        }
        match self.valid_index(array, range) {
            Some(index) => ScalarValue::try_from_array(array, index),
            None => ScalarValue::try_from(array.data_type()),
        }
    }

    fn supports_bounded_execution(&self) -> bool {
        true
    }

    fn uses_window_frame(&self) -> bool {
        true
    }
}

impl SparkFirstLastValueEvaluator {
    fn valid_index(&self, array: &ArrayRef, range: &Range<usize>) -> Option<usize> {
        if self.ignore_nulls {
            let slice = array.slice(range.start, range.end - range.start);
            if let Some(nulls) = slice.nulls() {
                if nulls.null_count() > 0 {
                    return self.valid_index_with_nulls(nulls, range.start);
                }
            }
        }
        match self.kind {
            SparkFirstLastValueKind::First => Some(range.start),
            SparkFirstLastValueKind::Last => Some(range.end - 1),
        }
    }

    fn valid_index_with_nulls(&self, nulls: &NullBuffer, offset: usize) -> Option<usize> {
        match self.kind {
            SparkFirstLastValueKind::First => nulls.valid_indices().next().map(|idx| idx + offset),
            SparkFirstLastValueKind::Last => nulls.valid_indices().last().map(|idx| idx + offset),
        }
    }
}
