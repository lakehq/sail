use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::cast::as_boolean_array;
use datafusion::common::{Result, ScalarValue, internal_err};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::DynamicFilterTracking;

const UNDECIDED: u8 = 0;
const KEEP: u8 = 1;
const BYPASS: u8 = 2;
const SAMPLE_ROWS: usize = 8192;

/// An optional consumer of a native join filter, whose producing join still
/// enforces the condition. A completed filter that retains most sampled rows is
/// bypassed locally, avoiding repeated membership work on unselective inputs.
#[derive(Debug)]
pub struct OptionalFilterExpr {
    predicate: Arc<dyn PhysicalExpr>,
    decision: Arc<AtomicU8>,
}

impl OptionalFilterExpr {
    pub fn new(predicate: Arc<dyn PhysicalExpr>) -> Self {
        Self {
            predicate,
            decision: Arc::new(AtomicU8::new(UNDECIDED)),
        }
    }

    pub fn predicate(&self) -> &Arc<dyn PhysicalExpr> {
        &self.predicate
    }
}

impl PartialEq for OptionalFilterExpr {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.decision, &other.decision) && self.predicate.eq(&other.predicate)
    }
}

impl Eq for OptionalFilterExpr {}

impl Hash for OptionalFilterExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Equality must not change when sampling completes, and consumers make
        // independent decisions even when they share the same native filter.
        Arc::as_ptr(&self.decision).hash(state);
        self.predicate.hash(state);
    }
}

impl Display for OptionalFilterExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "optional_filter({})", self.predicate)
    }
}

impl PhysicalExpr for OptionalFilterExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let pass = || ColumnarValue::Scalar(ScalarValue::Boolean(Some(true)));
        match self.decision.load(Ordering::Relaxed) {
            BYPASS => return Ok(pass()),
            KEEP => return self.predicate.evaluate(batch),
            _ => {}
        }
        // An eager input may arrive before the build finishes. Neither that
        // placeholder predicate nor an empty batch is evidence to bypass.
        if batch.num_rows() == 0
            || !matches!(
                DynamicFilterTracking::classify(&self.predicate),
                DynamicFilterTracking::AllComplete
            )
        {
            return Ok(pass());
        }
        let sample_rows = batch.num_rows().min(SAMPLE_ROWS);
        let sampled = self
            .predicate
            .evaluate(&batch.slice(0, sample_rows))?
            .into_array(sample_rows)?;
        // A few rejected rows do not justify repeated membership work. Require
        // at least a halving of the sampled input before keeping this optional
        // consumer. This is a conservative benefit gate, not a cost estimate.
        let decision = if as_boolean_array(sampled.as_ref())?.true_count() > sample_rows / 2 {
            BYPASS
        } else {
            KEEP
        };
        // Concurrent first batches may sample different rows. Either terminal
        // choice is safe: bypass only admits extra rows to the original join.
        let decision = match self.decision.compare_exchange(
            UNDECIDED,
            decision,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => decision,
            Err(previous) => previous,
        };
        if decision == BYPASS {
            Ok(pass())
        } else if sample_rows == batch.num_rows() {
            Ok(ColumnarValue::Array(sampled))
        } else {
            self.predicate.evaluate(batch)
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.predicate]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let [predicate] = children.as_slice() else {
            return internal_err!("OptionalFilterExpr requires exactly one child");
        };
        Ok(Arc::new(Self::new(Arc::clone(predicate))))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{ArrayRef, BooleanArray, Int64Array};
    use datafusion::arrow::datatypes::Field;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{
        BinaryExpr, Column, DynamicFilterPhysicalExpr, lit,
    };

    use super::*;

    fn batch(values: Vec<Option<i64>>) -> Result<RecordBatch> {
        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(values))],
        )?)
    }

    fn producer() -> Result<Arc<DynamicFilterPhysicalExpr>> {
        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
        let filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::clone(&key)],
            lit(true),
        ));
        filter.update(Arc::new(BinaryExpr::new(key, Operator::Eq, lit(1_i64))))?;
        Ok(filter)
    }

    fn evaluate(expr: &dyn PhysicalExpr, batch: &RecordBatch) -> Result<ArrayRef> {
        expr.evaluate(batch)?.into_array(batch.num_rows())
    }

    #[test]
    fn test_optional_filter_waits_for_ready_nonempty_sample() -> Result<()> {
        let native = producer()?;
        let optional = OptionalFilterExpr::new(Arc::clone(&native) as _);
        let mixed = batch(vec![Some(1), Some(2), None])?;
        assert_eq!(
            evaluate(&optional, &mixed)?.as_ref(),
            &BooleanArray::from(vec![true, true, true])
        );
        native.mark_complete();
        evaluate(&optional, &batch(vec![])?)?;
        assert_eq!(
            evaluate(&optional, &mixed)?.as_ref(),
            &BooleanArray::from(vec![Some(true), Some(false), None])
        );
        assert_eq!(
            evaluate(&optional, &batch(vec![Some(2)])?)?.as_ref(),
            &BooleanArray::from(vec![false])
        );
        Ok(())
    }

    #[test]
    fn test_optional_filter_bypass_is_sampled_and_consumer_local() -> Result<()> {
        let native = producer()?;
        native.mark_complete();
        let optional = OptionalFilterExpr::new(Arc::clone(&native) as _);
        let mut values = vec![Some(1); SAMPLE_ROWS];
        values.push(Some(2));
        let input = batch(values)?;
        assert_eq!(
            as_boolean_array(evaluate(&optional, &input)?.as_ref())?.true_count(),
            SAMPLE_ROWS + 1
        );
        let rejected = batch(vec![Some(2)])?;
        assert_eq!(
            evaluate(&optional, &rejected)?.as_ref(),
            &BooleanArray::from(vec![true])
        );
        let independent = OptionalFilterExpr::new(Arc::clone(&native) as _);
        assert_eq!(
            evaluate(&independent, &rejected)?.as_ref(),
            &BooleanArray::from(vec![false])
        );
        assert_eq!(
            evaluate(native.as_ref(), &rejected)?.as_ref(),
            &BooleanArray::from(vec![false])
        );
        Ok(())
    }

    #[test]
    fn test_optional_filter_rewrite_resets_consumer_decision() -> Result<()> {
        let native = producer()?;
        native.mark_complete();
        let optional = Arc::new(OptionalFilterExpr::new(Arc::clone(&native) as _));
        evaluate(optional.as_ref(), &batch(vec![Some(1)])?)?;
        let remapped =
            Arc::clone(&native).with_new_children(vec![Arc::new(Column::new("stored_key", 1))])?;
        let rewritten = optional.with_new_children(vec![remapped])?;
        assert_eq!(rewritten.expression_id(), None);
        assert_eq!(
            rewritten.children()[0].expression_id(),
            native.expression_id()
        );
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("padding", DataType::Int64, true),
                Field::new("stored_key", DataType::Int64, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1_i64])),
                Arc::new(Int64Array::from(vec![2_i64])),
            ],
        )?;
        assert_eq!(
            evaluate(rewritten.as_ref(), &input)?.as_ref(),
            &BooleanArray::from(vec![false])
        );
        Ok(())
    }

    #[test]
    fn test_optional_filter_bypasses_low_reduction() -> Result<()> {
        let native = producer()?;
        native.mark_complete();
        let optional = OptionalFilterExpr::new(Arc::clone(&native) as _);
        let mut values = vec![Some(1); 99];
        values.push(Some(2));
        let input = batch(values)?;
        assert_eq!(
            as_boolean_array(evaluate(&optional, &input)?.as_ref())?.true_count(),
            100
        );
        assert_eq!(
            as_boolean_array(evaluate(native.as_ref(), &input)?.as_ref())?.true_count(),
            99
        );
        // A different consumer with a useful reduction still keeps filtering.
        let useful = OptionalFilterExpr::new(Arc::clone(&native) as _);
        assert_eq!(
            evaluate(&useful, &batch(vec![Some(1), Some(2)])?)?.as_ref(),
            &BooleanArray::from(vec![true, false])
        );
        assert_eq!(
            evaluate(&useful, &batch(vec![Some(2)])?)?.as_ref(),
            &BooleanArray::from(vec![false])
        );
        for (values, expected) in [
            (vec![Some(1), Some(1), Some(2)], vec![true, true, true]),
            (vec![Some(1), Some(2), Some(2)], vec![true, false, false]),
        ] {
            let optional = OptionalFilterExpr::new(Arc::clone(&native) as _);
            assert_eq!(
                evaluate(&optional, &batch(values)?)?.as_ref(),
                &BooleanArray::from(expected)
            );
        }
        Ok(())
    }
}
