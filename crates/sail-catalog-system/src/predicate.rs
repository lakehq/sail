use std::marker::PhantomData;
use std::mem;
use std::sync::Arc;

use datafusion::arrow::array::{Array, BooleanArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{internal_datafusion_err, Result, ScalarValue};
use datafusion::logical_expr::{Expr, Operator};
use datafusion::physical_expr::expressions::BinaryExpr;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::internal_err;
use sail_common_datafusion::system::predicate::Predicate;

pub struct PredicateExtractor {
    expressions: Vec<Arc<dyn PhysicalExpr>>,
    extracted: Vec<String>,
}

impl PredicateExtractor {
    pub fn new(expressions: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        Self {
            expressions,
            extracted: vec![],
        }
    }

    #[expect(private_bounds)]
    pub fn extract<T: PredicateInput>(&mut self, column: &str) -> Result<Option<Predicate<T>>> {
        self.extracted.push(column.to_string());
        let expressions = mem::take(&mut self.expressions);
        let (selected, remaining) = expressions
            .into_iter()
            .partition(|e| is_column_physical_predicate(e, column).unwrap_or(false));
        self.expressions = remaining;
        let Some(conjunction) = selected.into_iter().fold(None, |acc, expr| {
            if let Some(acc) = acc {
                Some(Arc::new(BinaryExpr::new(acc, Operator::And, expr)) as Arc<dyn PhysicalExpr>)
            } else {
                Some(expr)
            }
        }) else {
            return Ok(None);
        };
        let predicate = ArrowPredicateEvaluator::<T>::try_new(conjunction)?;
        Ok(Some(Arc::new(move |value: &T| predicate.evaluate(value))))
    }

    pub fn finalize(&self) -> Result<()> {
        if self.expressions.is_empty() {
            Ok(())
        } else {
            internal_err!(
                "found {} unhandled predicate(s) after extracting predicates for columns {:?}: {:?}",
                self.expressions.len(),
                self.extracted,
                self.expressions
            )
        }
    }
}

pub fn is_column_logical_predicate(expr: &Expr, column: &str) -> Result<bool> {
    use datafusion::common::Column;

    let mut valid = true;
    expr.apply(|e| {
        if let Expr::Column(Column { name, .. }) = e {
            if name != column {
                valid = false;
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(valid)
}

pub fn is_column_physical_predicate(expr: &Arc<dyn PhysicalExpr>, column: &str) -> Result<bool> {
    use datafusion::physical_expr::expressions::Column;

    let mut valid = true;
    expr.apply(|e| {
        if let Some(col) = e.as_any().downcast_ref::<Column>() {
            if col.name() != column {
                valid = false;
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(valid)
}

pub struct ArrowPredicateEvaluator<T> {
    predicate: Arc<dyn PhysicalExpr>,
    schema: SchemaRef,
    phantom: PhantomData<T>,
}

#[expect(private_bounds)]
impl<T: PredicateInput> ArrowPredicateEvaluator<T> {
    /// An arbitrary field name for the record batch
    /// constructed during predicate evaluation.
    const FIELD_NAME: &'static str = "value";

    pub fn try_new(predicate: Arc<dyn PhysicalExpr>) -> Result<Self> {
        use datafusion::physical_expr::expressions::Column;

        let schema = Arc::new(Schema::new(vec![Field::new(
            Self::FIELD_NAME,
            T::arrow_type(),
            false,
        )]));
        // We need to rewrite column reference in the predicate to refer to
        // the single field in the record batch constructed during
        // predicate evaluation.
        let predicate = predicate
            .transform(|e| {
                if e.as_any().is::<Column>() {
                    Ok(Transformed::yes(Arc::new(Column::new(Self::FIELD_NAME, 0))))
                } else {
                    Ok(Transformed::no(e))
                }
            })?
            .data;
        Ok(Self {
            predicate,
            schema,
            phantom: PhantomData,
        })
    }

    pub fn evaluate(&self, value: &T) -> Result<bool> {
        let array = value.to_scalar()?.to_array()?;
        let batch = RecordBatch::try_new(self.schema.clone(), vec![array])?;
        let result = self.predicate.evaluate(&batch)?.into_array(1)?;
        let result = result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                internal_datafusion_err!("expected boolean array from predicate evaluation")
            })?;
        if result.len() != 1 || !result.is_valid(0) {
            return internal_err!(
                "expected a single non-null boolean result from predicate evaluation"
            );
        }
        Ok(result.value(0))
    }
}

/// A private trait to restrict the types that can be used as
/// predicate inputs.
trait PredicateInput: Send + Sync + 'static {
    fn arrow_type() -> DataType;
    fn to_scalar(&self) -> Result<ScalarValue>;
}

impl PredicateInput for String {
    fn arrow_type() -> DataType {
        DataType::Utf8
    }

    fn to_scalar(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::Utf8(Some(self.clone())))
    }
}

macro_rules! impl_primitive_predicate_input {
    ($ty:ty, $variant:ident) => {
        impl PredicateInput for $ty {
            fn arrow_type() -> DataType {
                DataType::$variant
            }

            fn to_scalar(&self) -> Result<ScalarValue> {
                Ok(ScalarValue::$variant(Some(*self)))
            }
        }
    };
}

impl_primitive_predicate_input!(i8, Int8);
impl_primitive_predicate_input!(i16, Int16);
impl_primitive_predicate_input!(i32, Int32);
impl_primitive_predicate_input!(i64, Int64);
impl_primitive_predicate_input!(u8, UInt8);
impl_primitive_predicate_input!(u16, UInt16);
impl_primitive_predicate_input!(u32, UInt32);
impl_primitive_predicate_input!(u64, UInt64);

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::scalar::ScalarValue;

    use super::ArrowPredicateEvaluator;

    #[test]
    fn test_string_predicate_evaluator() {
        let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new("x", 42));
        let literal: Arc<dyn PhysicalExpr> =
            Arc::new(Literal::new(ScalarValue::Utf8(Some("test".to_string()))));
        let predicate: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(column, Operator::Eq, literal));

        let evaluator = ArrowPredicateEvaluator::<String>::try_new(predicate).unwrap();

        let result = evaluator.evaluate(&"test".to_string()).unwrap();
        assert!(result);

        let result = evaluator.evaluate(&"other".to_string()).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_primitive_predicate_evaluator() {
        let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new("x", 42));
        let literal: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int32(Some(1))));
        let predicate: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(column, Operator::Eq, literal));

        let evaluator = ArrowPredicateEvaluator::<i32>::try_new(predicate).unwrap();

        let result = evaluator.evaluate(&1).unwrap();
        assert!(result);

        let result = evaluator.evaluate(&3).unwrap();
        assert!(!result);
    }
}
