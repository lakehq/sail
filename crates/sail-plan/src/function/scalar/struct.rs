use datafusion::functions::expr_fn;

use crate::extension::function::struct_function::StructFunction;
use crate::function::common::Function;

pub(super) fn list_built_in_struct_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("named_struct", F::var_arg(expr_fn::named_struct)),
        (
            "struct",
            F::dynamic_udf(|args| Ok(StructFunction::try_new_from_expressions(args)?)),
        ),
    ]
}
