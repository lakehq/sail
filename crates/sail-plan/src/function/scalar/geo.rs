use crate::function::common::ScalarFunctionBuilder as F;

use sail_function::scalar::geo::st_asbinary::StAsBinary;
use sail_function::scalar::geo::st_geogfromwkb::StGeoGFromWKB;
use sail_function::scalar::geo::st_geomfromwkb::StGeomFromWKB;
use sail_function::scalar::geo::st_setsrid::StSetSrid;
use sail_function::scalar::geo::st_srid::StSrid;

pub(super) fn list_built_in_geo_functions() -> Vec<(&'static str, ScalarFunction)> {
    vec![
        ("st_asbinary", F::udf(StAsBinary::new())),
        ("st_geogfromwkb", F::udf(StGeoGFromWKB::new())),
        ("st_geomfromwkb", F::udf(StGeomFromWKB::new())),
        ("st_srid", F::udf(StSrid::new())),
        ("st_setsrid", F::udf(StSetSrid::new())),
    ]
}
