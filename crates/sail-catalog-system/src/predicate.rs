use std::marker::PhantomData;
use std::mem;
use std::ops::Bound;
use std::sync::Arc;

use datafusion::arrow::array::{Array, BooleanArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, ScalarValue, internal_datafusion_err};
use datafusion::logical_expr::{Expr, Operator};
use datafusion::physical_expr::expressions::{BinaryExpr, Column, InListExpr, Literal};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_plan::internal_err;
use sail_system_store::predicate::{
    MapValueFilter, Predicate, TimestampMicros, ValueDomain, ValueFilter,
};

pub struct PredicateExtractor {
    expressions: Vec<Arc<dyn PhysicalExpr>>,
    extracted: Vec<String>,
}

impl PredicateExtractor {
    pub fn new(expressions: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        let mut flattened = vec![];
        for expression in expressions {
            flatten_physical_conjunctions(expression, &mut flattened);
        }
        Self {
            expressions: flattened,
            extracted: vec![],
        }
    }

    #[expect(private_bounds)]
    pub fn extract<T: PredicateInput>(&mut self, column: &str) -> Result<Option<ValueFilter<T>>> {
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
        let domain = value_domain::<T>(&conjunction, column);
        let evaluator = ArrowPredicateEvaluator::<T>::try_new(conjunction)?;
        let predicate: Predicate<T> = Arc::new(move |value: &T| evaluator.evaluate(value));
        Ok(Some(ValueFilter::new(domain, predicate)))
    }

    #[expect(private_bounds)]
    pub fn extract_map_values<T: PredicateInput>(
        &mut self,
        column: &str,
    ) -> Result<Vec<MapValueFilter<String, T>>> {
        self.extracted.push(column.to_string());
        let expressions = mem::take(&mut self.expressions);
        let mut selected = vec![];
        let mut remaining = vec![];
        for expression in expressions {
            if let Some((key, expression)) = map_value_expression(Arc::clone(&expression), column)?
            {
                selected.push((key, expression));
            } else {
                remaining.push(expression);
            }
        }
        self.expressions = remaining;
        selected
            .into_iter()
            .map(|(key, expression)| {
                let domain =
                    value_domain::<T>(&expression, ArrowPredicateEvaluator::<T>::FIELD_NAME);
                let evaluator = ArrowPredicateEvaluator::<T>::try_new(expression)?;
                let predicate: Predicate<T> = Arc::new(move |value: &T| evaluator.evaluate(value));
                Ok(MapValueFilter::new(key, domain, predicate))
            })
            .collect()
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

fn flatten_physical_conjunctions(
    expression: Arc<dyn PhysicalExpr>,
    flattened: &mut Vec<Arc<dyn PhysicalExpr>>,
) {
    if let Some(expression) = expression.downcast_ref::<BinaryExpr>()
        && expression.op() == &Operator::And
    {
        flatten_physical_conjunctions(Arc::clone(expression.left()), flattened);
        flatten_physical_conjunctions(Arc::clone(expression.right()), flattened);
    } else {
        flattened.push(expression);
    }
}

fn map_value_expression(
    expression: Arc<dyn PhysicalExpr>,
    column: &str,
) -> Result<Option<(String, Arc<dyn PhysicalExpr>)>> {
    let mut keys = Vec::new();
    let mut valid = true;
    expression.apply(|expression| {
        if let Some(key) = physical_map_value_key(expression, column) {
            keys.push(key);
            return Ok(TreeNodeRecursion::Jump);
        }
        if expression.is::<Column>() {
            valid = false;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    keys.sort();
    keys.dedup();
    let [key] = keys.as_slice() else {
        return Ok(None);
    };
    if !valid {
        return Ok(None);
    }
    let key = key.clone();
    let expression = expression
        .transform_down(|expression| {
            if physical_map_value_key(&expression, column).as_ref() == Some(&key) {
                Ok(Transformed::new(
                    Arc::new(Column::new(
                        ArrowPredicateEvaluator::<String>::FIELD_NAME,
                        0,
                    )) as Arc<dyn PhysicalExpr>,
                    true,
                    TreeNodeRecursion::Jump,
                ))
            } else {
                Ok(Transformed::no(expression))
            }
        })?
        .data;
    Ok(Some((key, expression)))
}

fn physical_map_value_key(expression: &Arc<dyn PhysicalExpr>, column: &str) -> Option<String> {
    let function = expression.downcast_ref::<ScalarFunctionExpr>()?;
    match function.name() {
        "array_element" => {
            let [map_extract, index] = function.args() else {
                return None;
            };
            if !physical_literal(index).is_some_and(scalar_is_one) {
                return None;
            }
            physical_map_extract_key(map_extract, column)
        }
        "get_field" => physical_map_extract_key(expression, column),
        _ => None,
    }
}

fn scalar_is_one(value: &ScalarValue) -> bool {
    matches!(
        value,
        ScalarValue::Int8(Some(1))
            | ScalarValue::Int16(Some(1))
            | ScalarValue::Int32(Some(1))
            | ScalarValue::Int64(Some(1))
            | ScalarValue::UInt8(Some(1))
            | ScalarValue::UInt16(Some(1))
            | ScalarValue::UInt32(Some(1))
            | ScalarValue::UInt64(Some(1))
    )
}

fn physical_map_extract_key(expression: &Arc<dyn PhysicalExpr>, column: &str) -> Option<String> {
    let function = expression.downcast_ref::<ScalarFunctionExpr>()?;
    if !matches!(function.name(), "map_extract" | "get_field") {
        return None;
    }
    let [map, key] = function.args() else {
        return None;
    };
    if !is_column(map, column) {
        return None;
    }
    string_scalar(physical_literal(key)?).map(ToOwned::to_owned)
}

fn physical_literal(expression: &Arc<dyn PhysicalExpr>) -> Option<&ScalarValue> {
    expression.downcast_ref::<Literal>().map(Literal::value)
}

fn string_scalar(value: &ScalarValue) -> Option<&str> {
    match value {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::LargeUtf8(Some(value))
        | ScalarValue::Utf8View(Some(value)) => Some(value),
        _ => None,
    }
}

fn value_domain<T: PredicateInput>(
    expression: &Arc<dyn PhysicalExpr>,
    column: &str,
) -> ValueDomain<T> {
    if let Some(expression) = expression.downcast_ref::<BinaryExpr>() {
        if expression.op() == &Operator::And {
            return value_domain::<T>(expression.left(), column)
                .intersect(&value_domain::<T>(expression.right(), column));
        }
        if matches!(
            expression.op(),
            Operator::Eq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
        ) {
            if is_column(expression.left(), column) {
                return comparison_domain::<T>(expression.op(), expression.right(), false);
            }
            if is_column(expression.right(), column) {
                return comparison_domain::<T>(expression.op(), expression.left(), true);
            }
        }
    }
    if let Some(expression) = expression.downcast_ref::<InListExpr>()
        && !expression.negated()
        && is_column(expression.expr(), column)
    {
        let mut values = vec![];
        for expression in expression.list() {
            match literal_value::<T>(expression) {
                Some(Some(value)) => values.push(value),
                Some(None) => {}
                None => return ValueDomain::all(),
            }
        }
        return ValueDomain::from_points(values);
    }
    ValueDomain::all()
}

fn is_column(expression: &Arc<dyn PhysicalExpr>, name: &str) -> bool {
    expression
        .downcast_ref::<Column>()
        .is_some_and(|column| column.name() == name)
}

fn comparison_domain<T: PredicateInput>(
    operator: &Operator,
    literal: &Arc<dyn PhysicalExpr>,
    reverse: bool,
) -> ValueDomain<T> {
    let Some(value) = literal_value::<T>(literal) else {
        return ValueDomain::all();
    };
    let Some(value) = value else {
        return ValueDomain::empty();
    };
    match (operator, reverse) {
        (Operator::Eq, _) => ValueDomain::point(value),
        (Operator::Lt, false) | (Operator::Gt, true) => {
            ValueDomain::range(Bound::Unbounded, Bound::Excluded(value))
        }
        (Operator::LtEq, false) | (Operator::GtEq, true) => {
            ValueDomain::range(Bound::Unbounded, Bound::Included(value))
        }
        (Operator::Gt, false) | (Operator::Lt, true) => {
            ValueDomain::range(Bound::Excluded(value), Bound::Unbounded)
        }
        (Operator::GtEq, false) | (Operator::LtEq, true) => {
            ValueDomain::range(Bound::Included(value), Bound::Unbounded)
        }
        _ => ValueDomain::all(),
    }
}

fn literal_value<T: PredicateInput>(expression: &Arc<dyn PhysicalExpr>) -> Option<Option<T>> {
    let literal = expression.downcast_ref::<Literal>()?;
    if literal.value().is_null() {
        Some(None)
    } else {
        T::from_scalar(literal.value()).map(Some)
    }
}

pub fn is_column_logical_predicate(expr: &Expr, column: &str) -> Result<bool> {
    use datafusion::common::Column;

    let mut valid = true;
    expr.apply(|e| {
        if let Expr::Column(Column { name, .. }) = e
            && name != column
        {
            valid = false;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(valid)
}

pub fn is_map_value_logical_predicate(expr: &Expr, column: &str) -> Result<bool> {
    if let Expr::BinaryExpr(expression) = expr
        && expression.op == Operator::And
    {
        return Ok(is_map_value_logical_predicate(&expression.left, column)?
            && is_map_value_logical_predicate(&expression.right, column)?);
    }
    if !matches!(
        expr,
        Expr::BinaryExpr(expression)
            if matches!(
                expression.op,
                Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
            )
    ) && !matches!(expr, Expr::InList(_))
    {
        return Ok(false);
    }
    let mut keys = Vec::new();
    let mut valid = true;
    expr.apply(|expression| {
        if let Some(key) = logical_map_value_key(expression, column) {
            keys.push(key);
            return Ok(TreeNodeRecursion::Jump);
        }
        if let Expr::Column(_) = expression {
            valid = false;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    keys.sort();
    keys.dedup();
    Ok(valid && keys.len() == 1)
}

fn logical_map_value_key(expression: &Expr, column: &str) -> Option<String> {
    let Expr::ScalarFunction(function) = expression else {
        return None;
    };
    match function.name() {
        "array_element" => {
            let [map_extract, index] = function.args.as_slice() else {
                return None;
            };
            if !logical_literal(index).is_some_and(scalar_is_one) {
                return None;
            }
            logical_map_extract_key(map_extract, column)
        }
        "get_field" => logical_map_extract_key(expression, column),
        _ => None,
    }
}

fn logical_map_extract_key(expression: &Expr, column: &str) -> Option<String> {
    let Expr::ScalarFunction(function) = expression else {
        return None;
    };
    if !matches!(function.name(), "map_extract" | "get_field") {
        return None;
    }
    let [map, key] = function.args.as_slice() else {
        return None;
    };
    if !matches!(map, Expr::Column(value) if value.name == column) {
        return None;
    }
    string_scalar(logical_literal(key)?).map(ToOwned::to_owned)
}

fn logical_literal(expression: &Expr) -> Option<&ScalarValue> {
    let Expr::Literal(value, _) = expression else {
        return None;
    };
    Some(value)
}

pub fn is_column_physical_predicate(expr: &Arc<dyn PhysicalExpr>, column: &str) -> Result<bool> {
    use datafusion::physical_expr::expressions::Column;

    let mut valid = true;
    expr.apply(|e| {
        if let Some(col) = e.downcast_ref::<Column>()
            && col.name() != column
        {
            valid = false;
            return Ok(TreeNodeRecursion::Stop);
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
                if e.is::<Column>() {
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
trait PredicateInput: Clone + Ord + Send + Sync + 'static {
    fn arrow_type() -> DataType;
    fn to_scalar(&self) -> Result<ScalarValue>;
    fn from_scalar(value: &ScalarValue) -> Option<Self>
    where
        Self: Sized;
}

impl PredicateInput for String {
    fn arrow_type() -> DataType {
        DataType::Utf8
    }

    fn to_scalar(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::Utf8(Some(self.clone())))
    }

    fn from_scalar(value: &ScalarValue) -> Option<Self> {
        match value {
            ScalarValue::Utf8(Some(value)) => Some(value.clone()),
            _ => None,
        }
    }
}

impl PredicateInput for TimestampMicros {
    fn arrow_type() -> DataType {
        DataType::Timestamp(
            datafusion::arrow::datatypes::TimeUnit::Microsecond,
            Some(Arc::from("UTC")),
        )
    }

    fn to_scalar(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::TimestampMicrosecond(
            Some(self.0),
            Some(Arc::from("UTC")),
        ))
    }

    fn from_scalar(value: &ScalarValue) -> Option<Self> {
        match value {
            ScalarValue::TimestampMicrosecond(Some(value), _) => Some(Self(*value)),
            _ => None,
        }
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

            fn from_scalar(value: &ScalarValue) -> Option<Self> {
                match value {
                    ScalarValue::$variant(Some(value)) => Some(*value),
                    _ => None,
                }
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
#[expect(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal, in_list};
    use datafusion::scalar::ScalarValue;
    use sail_system_store::predicate::{TimestampMicros, ValueRange};

    use super::{ArrowPredicateEvaluator, PredicateExtractor};

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

    #[test]
    fn test_timestamp_predicate_evaluator() {
        let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new("timestamp", 0));
        let literal: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(
            ScalarValue::TimestampMicrosecond(Some(42), Some(Arc::from("UTC"))),
        ));
        let predicate: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(column, Operator::GtEq, literal));

        let evaluator = ArrowPredicateEvaluator::<TimestampMicros>::try_new(predicate).unwrap();

        assert!(evaluator.evaluate(&TimestampMicros(42)).unwrap());
        assert!(!evaluator.evaluate(&TimestampMicros(41)).unwrap());
    }

    #[test]
    fn test_extract_value_domain_from_equal_and_in_list() {
        let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new("x", 0));
        let equal: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::clone(&column),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::UInt64(Some(3)))),
        ));
        let schema = Schema::new(vec![Field::new("x", DataType::UInt64, false)]);
        let in_list = in_list(
            column,
            vec![
                Arc::new(Literal::new(ScalarValue::UInt64(Some(3)))),
                Arc::new(Literal::new(ScalarValue::UInt64(Some(5)))),
            ],
            &false,
            &schema,
        )
        .unwrap();
        let mut extractor = PredicateExtractor::new(vec![equal, in_list]);

        let filter = extractor.extract::<u64>("x").unwrap().unwrap();

        assert_eq!(filter.domain.points(), Some(vec![3]));
    }

    #[test]
    fn test_extract_value_domain_from_comparison() {
        let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new("x", 0));
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            column,
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::UInt64(Some(3)))),
        ));
        let mut extractor = PredicateExtractor::new(vec![predicate]);

        let filter = extractor.extract::<u64>("x").unwrap().unwrap();

        assert_eq!(filter.domain.points(), None);
        assert!(matches!(
            filter.domain.ranges(),
            [ValueRange {
                lower: std::ops::Bound::Excluded(3),
                upper: std::ops::Bound::Unbounded,
            }]
        ));
    }
}
