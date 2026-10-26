use sail_function::scalar::xml::xpath_typed::{XpathTyped, XpathTypedKind};

use crate::function::common::ScalarFunctionBuilder as F;

pub(super) fn list_built_in_xml_functions()
-> Vec<(&'static str, crate::function::common::ScalarFunction)> {
    vec![
        ("schema_of_xml", F::kernel("schema_of_xml")),
        (
            "xpath",
            F::udf(sail_function::scalar::xml::xpath::Xpath::new()),
        ),
        (
            "xpath_boolean",
            F::udf(XpathTyped::new(XpathTypedKind::Boolean)),
        ),
        (
            "xpath_double",
            F::udf(XpathTyped::new(XpathTypedKind::Double)),
        ),
        (
            "xpath_float",
            F::udf(XpathTyped::new(XpathTypedKind::Float)),
        ),
        ("xpath_int", F::udf(XpathTyped::new(XpathTypedKind::Int))),
        ("xpath_long", F::udf(XpathTyped::new(XpathTypedKind::Long))),
        (
            "xpath_number",
            F::udf(XpathTyped::new(XpathTypedKind::Number)),
        ),
        (
            "xpath_short",
            F::udf(XpathTyped::new(XpathTypedKind::Short)),
        ),
        (
            "xpath_string",
            F::udf(XpathTyped::new(XpathTypedKind::String)),
        ),
    ]
}
