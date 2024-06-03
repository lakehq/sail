use crate::function::common::Function;

mod array;
mod bitwise;
mod collection;
mod conditional;
mod conversion;
mod csv;
mod datetime;
mod hash;
mod json;
mod lambda;
mod map;
mod math;
mod misc;
mod predicate;
mod string;
mod r#struct;
mod url;
mod xml;

pub(super) fn list_built_in_scalar_functions() -> Vec<(&'static str, Function)> {
    let mut output = Vec::new();
    output.extend(array::list_built_in_array_functions());
    output.extend(bitwise::list_built_in_bitwise_functions());
    output.extend(collection::list_built_in_collection_functions());
    output.extend(conditional::list_built_in_conditional_functions());
    output.extend(conversion::list_built_in_conversion_functions());
    output.extend(csv::list_built_in_csv_functions());
    output.extend(datetime::list_built_in_datetime_functions());
    output.extend(hash::list_built_in_hash_functions());
    output.extend(json::list_built_in_json_functions());
    output.extend(lambda::list_built_in_lambda_functions());
    output.extend(map::list_built_in_map_functions());
    output.extend(math::list_built_in_math_functions());
    output.extend(misc::list_built_in_misc_functions());
    output.extend(predicate::list_built_in_predicate_functions());
    output.extend(string::list_built_in_string_functions());
    output.extend(r#struct::list_built_in_struct_functions());
    output.extend(url::list_built_in_url_functions());
    output.extend(xml::list_built_in_xml_functions());
    output
}
