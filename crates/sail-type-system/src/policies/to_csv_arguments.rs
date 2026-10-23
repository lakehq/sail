use arrow_schema::DataType;
use datafusion_common::{Result, plan_err};
pub fn coerce_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    match arg_types {
        [DataType::Struct(_)] => Ok(vec![arg_types[0].clone()]),
        [DataType::Struct(_), DataType::Map(_, _)] => {
            Ok(vec![arg_types[0].clone(), arg_types[1].clone()])
        }
        _ => plan_err!(
            "`{}` function requires 1 or 2 arguments: a struct and an optional options map, got {:?}",
            "to_csv",
            arg_types
        ),
    }
}
