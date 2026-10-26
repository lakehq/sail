use crate::function::common::ScalarFunctionBuilder as F;
pub(super) fn list_built_in_map_functions()
-> Vec<(&'static str, crate::function::common::ScalarFunction)> {
    vec![("map_keys", F::kernel("map_keys"))]
}
