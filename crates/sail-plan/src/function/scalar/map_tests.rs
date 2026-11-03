use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int32Array, RecordBatch, StructArray};
use datafusion::arrow::datatypes::Field;
use datafusion::functions::core::expr_fn::named_struct;
use datafusion::prelude::SessionContext;
use datafusion_common::DFSchema;
use datafusion_expr::col;
use sail_function::scalar::struct_function::StructFunction;

use super::*;
use crate::config::PlanConfig;
use crate::function::common::FunctionContextInput;

#[test]
fn nested_map_plans_preserve_values_and_nullability() -> PlanResult<()> {
    let context = SessionContext::new();
    let config = Arc::new(PlanConfig::default());
    let value: ArrayRef = Arc::new(StructArray::new(
        vec![Field::new("b", DataType::Int32, false)].into(),
        vec![Arc::new(Int32Array::from(vec![1, 2]))],
        None,
    ));
    let batch = RecordBatch::try_from_iter_with_nullable([("value", value.clone(), false)])?;
    let schema = Arc::new(DFSchema::try_from(batch.schema())?);
    let functions = list_built_in_map_functions();
    let build = |name: &str, arguments| -> PlanResult<expr::Expr> {
        let (_, function) = functions
            .iter()
            .find(|(registered, _)| *registered == name)
            .ok_or_else(|| PlanError::invalid(format!("missing map function {name}")))?;
        function(ScalarFunctionInput {
            arguments,
            function_context: FunctionContextInput {
                argument_display_names: &[],
                plan_config: &config,
                session_context: &context,
                schema: &schema,
            },
        })
    };
    let array = ScalarUDF::from(SparkArray::new());
    for key in [lit("a"), named_struct(vec![lit("a"), lit(1i32)])] {
        let map = build("map", vec![key.clone(), col("value")])?;
        let arrays = build(
            "map_from_arrays",
            vec![
                array.call(vec![key.clone()]),
                array.call(vec![col("value")]),
            ],
        )?;
        let entries = build(
            "map_from_entries",
            vec![array.call(vec![ScalarUDF::from(StructFunction::new(vec![
                "key".into(),
                "value".into(),
            ]))
            .call(vec![key, col("value")])])],
        )?;
        let concat = build("map_concat", vec![map.clone()])?;
        let expected_type = map.get_type(schema.as_ref())?;
        assert!(!map_value_contains_null(&expected_type));
        for expr in [map.clone(), arrays, entries, concat] {
            assert_eq!(expr.get_type(schema.as_ref())?, expected_type);
            let physical = context.create_physical_expr(expr.clone(), schema.as_ref())?;
            eprintln!("logical: {expr}\nphysical: {physical}");
            let result = physical.evaluate(&batch)?.into_array(2)?;
            assert_eq!(result.data_type(), &expected_type);
            assert_eq!(result.as_map().values().to_data(), value.to_data());
        }
        let values = build("map_values", vec![map])?;
        let DataType::List(element) = values.get_type(schema.as_ref())? else {
            return Err(PlanError::invalid("expected list"));
        };
        assert!(element.is_nullable());
        assert_eq!(element.data_type(), value.data_type());
        let result = context
            .create_physical_expr(values, schema.as_ref())?
            .evaluate(&batch)?
            .into_array(2)?;
        assert_eq!(result.as_list::<i32>().values().to_data(), value.to_data());
    }
    Ok(())
}
