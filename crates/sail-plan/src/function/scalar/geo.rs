use crate::function::common::ScalarFunctionBuilder as F;

pub(super) fn list_built_in_geo_functions()
-> Vec<(&'static str, crate::function::common::ScalarFunction)> {
    vec![
        (
            "st_asbinary",
            F::udf(sail_function::scalar::geo::st_asbinary::StAsBinary::new()),
        ),
        (
            "st_geogfromwkb",
            F::udf(sail_function::scalar::geo::st_geogfromwkb::StGeogFromWKB::new()),
        ),
        (
            "st_geomfromwkb",
            F::udf(sail_function::scalar::geo::st_geomfromwkb::StGeomFromWKB::new()),
        ),
        ("st_setsrid", F::kernel("st_setsrid")),
        ("st_srid", F::kernel("st_srid")),
    ]
}
