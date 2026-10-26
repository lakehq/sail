use crate::function::common::ScalarFunction;

pub(crate) mod array;
pub(crate) mod bitwise;
pub(crate) mod collection;
pub(crate) mod conditional;
pub(crate) mod conversion;
pub(crate) mod csv;
pub(crate) mod datetime;
pub(crate) mod geo;
pub(crate) mod hash;
pub(crate) mod json;
pub(crate) mod kernel;
pub(crate) mod lambda;
pub(crate) mod map;
pub(crate) mod math;
pub(crate) mod misc;
pub(crate) mod predicate;
pub(crate) mod sequence;
pub(crate) mod string;
pub(crate) mod r#struct;
pub(crate) mod url;
pub(crate) mod variant;
pub(crate) mod vector;
pub(crate) mod xml;

pub(crate) fn list_built_in_scalar_functions() -> Vec<(&'static str, ScalarFunction)> {
    let mut output = Vec::new();
    output.extend(array::list_built_in_array_functions());
    output.extend(bitwise::list_built_in_bitwise_functions());
    output.extend(collection::list_built_in_collection_functions());
    output.extend(conditional::list_built_in_conditional_functions());
    output.extend(conversion::list_built_in_conversion_functions());
    output.extend(csv::list_built_in_csv_functions());
    output.extend(datetime::list_built_in_datetime_functions());
    output.extend(datetime::list_datetime_helpers());
    output.extend(geo::list_built_in_geo_functions());
    output.extend(hash::list_built_in_hash_functions());
    output.extend(json::list_built_in_json_functions());
    output.extend(map::list_built_in_map_functions());
    output.extend(math::list_built_in_math_functions());
    output.extend(misc::list_built_in_misc_functions());
    output.extend(predicate::list_built_in_operator_functions());
    output.extend(predicate::list_built_in_predicate_functions());
    output.extend(r#struct::list_built_in_struct_functions());
    output.extend(string::list_built_in_string_functions());
    output.extend(url::list_built_in_url_functions());
    output.extend(variant::list_built_in_variant_functions());
    output.extend(vector::list_built_in_vector_functions());
    output.extend(xml::list_built_in_xml_functions());
    output
}
