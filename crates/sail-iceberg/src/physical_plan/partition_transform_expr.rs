use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, new_empty_array};
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_err, plan_datafusion_err};

use crate::datasource::type_converter::{arrow_type_to_iceberg, iceberg_type_to_arrow};
use crate::spec::transform::Transform;
use crate::utils::conversions::{scalar_to_iceberg_literal, to_scalar};
use crate::utils::transform::apply_transform;

/// Evaluates an Iceberg partition transform for physical distribution and ordering.
#[derive(Debug, Clone, Eq)]
pub struct IcebergPartitionTransformExpr {
    input: Arc<dyn PhysicalExpr>,
    transform: Transform,
}

impl IcebergPartitionTransformExpr {
    pub fn new(input: Arc<dyn PhysicalExpr>, transform: Transform) -> Self {
        Self { input, transform }
    }

    pub fn input(&self) -> &Arc<dyn PhysicalExpr> {
        &self.input
    }

    pub const fn transform(&self) -> Transform {
        self.transform
    }

    fn transformed_type(&self, input_schema: &Schema) -> Result<crate::spec::types::Type> {
        let input_type = arrow_type_to_iceberg(&self.input.data_type(input_schema)?)?;
        self.transform
            .result_type(&input_type)
            .map_err(|error| DataFusionError::Plan(error.to_string()))
    }
}

impl PartialEq for IcebergPartitionTransformExpr {
    fn eq(&self, other: &Self) -> bool {
        self.input.eq(&other.input) && self.transform == other.transform
    }
}

impl Hash for IcebergPartitionTransformExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        self.transform.to_string().hash(state);
    }
}

impl Display for IcebergPartitionTransformExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.transform {
            Transform::Year => write!(f, "years({})", self.input),
            Transform::Month => write!(f, "months({})", self.input),
            Transform::Day => write!(f, "days({})", self.input),
            Transform::Hour => write!(f, "hours({})", self.input),
            Transform::Bucket(count) => write!(f, "bucket({count}, {})", self.input),
            Transform::Truncate(width) => write!(f, "truncate({width}, {})", self.input),
            transform => write!(f, "{transform}({})", self.input),
        }
    }
}

impl PhysicalExpr for IcebergPartitionTransformExpr {
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        iceberg_type_to_arrow(&self.transformed_type(input_schema)?)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.input.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let input = self.input.evaluate(batch)?.into_array(batch.num_rows())?;
        let input_type = arrow_type_to_iceberg(input.data_type())?;
        let transformed_type = self
            .transform
            .result_type(&input_type)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let output_type = iceberg_type_to_arrow(&transformed_type)?;
        if input.is_empty() {
            return Ok(ColumnarValue::Array(new_empty_array(&output_type)));
        }

        let values = (0..input.len())
            .map(|index| {
                if input.is_null(index) {
                    return ScalarValue::try_new_null(&output_type);
                }
                let scalar = ScalarValue::try_from_array(input.as_ref(), index)?;
                let literal = scalar_to_iceberg_literal(&scalar, input.data_type())
                    .map_err(|error| plan_datafusion_err!("{error}"))?;
                let transformed = apply_transform(self.transform, &input_type, Some(literal))
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "Iceberg partition transform {} returned null for a non-null value",
                            self.transform
                        ))
                    })?;
                to_scalar(&transformed, &transformed_type)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(ColumnarValue::Array(ScalarValue::iter_to_array(values)?))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if children.len() != 1 {
            return exec_err!(
                "IcebergPartitionTransformExpr requires one child, got {}",
                children.len()
            );
        }
        let input = children.pop().ok_or_else(|| {
            DataFusionError::Internal(
                "IcebergPartitionTransformExpr child disappeared after validation".to_string(),
            )
        })?;
        Ok(Arc::new(Self::new(input, self.transform)))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use datafusion::arrow::array::{Int32Array, TimestampMicrosecondArray};
    use datafusion::arrow::datatypes::Field;
    use datafusion::physical_expr::expressions::Column;

    use super::*;

    #[test]
    fn day_transform_groups_timestamps_by_iceberg_day() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "event_time",
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None),
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(TimestampMicrosecondArray::from(vec![
                Some(0),
                Some(86_399_999_999),
                Some(86_400_000_000),
                None,
            ]))],
        )
        .expect("timestamp batch");
        let expression = IcebergPartitionTransformExpr::new(
            Arc::new(Column::new("event_time", 0)),
            Transform::Day,
        );

        let actual = expression
            .evaluate(&batch)
            .expect("day transform")
            .into_array(batch.num_rows())
            .expect("day array");
        let actual = actual
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32 day values");
        assert_eq!(
            actual,
            &Int32Array::from(vec![Some(0), Some(0), Some(1), None])
        );
    }
}
