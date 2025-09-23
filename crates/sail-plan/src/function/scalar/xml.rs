use crate::function::common::ScalarFunction;

pub(super) fn list_built_in_xml_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("from_xml", F::unknown("from_xml")),
        ("schema_of_xml", F::unknown("schema_of_xml")),
        ("to_xml", F::unknown("to_xml")),
        ("xpath", F::unknown("xpath")),
        ("xpath_boolean", F::unknown("xpath_boolean")),
        ("xpath_double", F::unknown("xpath_double")),
        ("xpath_float", F::unknown("xpath_float")),
        ("xpath_int", F::unknown("xpath_int")),
        ("xpath_long", F::unknown("xpath_long")),
        ("xpath_number", F::unknown("xpath_number")),
        ("xpath_short", F::unknown("xpath_short")),
        ("xpath_string", F::unknown("xpath_string")),
    ]
}
