use sail_function::scalar::geo::st_asbinary::StAsBinary;
use sail_function::scalar::geo::st_geogfromwkb::StGeogFromWKB;
use sail_function::scalar::geo::st_geomfromwkb::StGeomFromWKB;

use crate::function::common::ScalarFunction;

pub(super) fn list_built_in_geo_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("st_asbinary", F::udf(StAsBinary::new())),
        ("st_geomfromwkb", F::udf(StGeomFromWKB::new())),
        ("st_geogfromwkb", F::udf(StGeogFromWKB::new())),
        ("st_setsrid", F::unknown("st_setsrid")),
        ("st_srid", F::unknown("st_srid")),
    ]
}
