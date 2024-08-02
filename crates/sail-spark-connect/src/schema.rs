use datafusion::arrow::datatypes as adt;
use sail_common::spec;
use sail_plan::resolver::PlanResolver;

use crate::error::SparkResult;
use crate::spark::connect as sc;

pub(crate) fn to_spark_schema(schema: adt::SchemaRef) -> SparkResult<sc::DataType> {
    let fields = PlanResolver::unresolve_fields(schema.fields().clone())?;
    spec::DataType::Struct { fields }.try_into()
}
