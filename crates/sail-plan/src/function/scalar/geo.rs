use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::geo::st_asbinary::StAsBinary;
use sail_function::scalar::geo::st_geogfromwkb::StGeoGFromWKB;
use sail_function::scalar::geo::st_geomfromwkb::StGeomFromWKB;

use crate::function::common::{ScalarFunction, ScalarFunctionBuilder as F, ScalarFunctionInput};

pub(super) fn list_built_in_geo_functions() -> Vec<(&'static str, ScalarFunction)> {
    vec![
        ("st_asbinary", F::custom(st_asbinary)),
        ("st_geomfromwkb", F::custom(st_geomfromwkb)),
        ("st_geogfromwkb", F::custom(st_geogfromwkb)),
    ]
}

fn st_geomfromwkb(input: ScalarFunctionInput) -> crate::error::PlanResult<datafusion_expr::Expr> {
    use datafusion_expr::{Expr, ScalarUDF};

    let arg = input.arguments.one()?;

    let func = StGeomFromWKB::new();
    Ok(Expr::ScalarFunction(
        datafusion_expr::expr::ScalarFunction {
            func: std::sync::Arc::new(ScalarUDF::from(func)),
            args: vec![arg],
        },
    ))
}

fn st_geogfromwkb(input: ScalarFunctionInput) -> crate::error::PlanResult<datafusion_expr::Expr> {
    use datafusion_expr::{Expr, ScalarUDF};

    let arg = input.arguments.one()?;

    let func = StGeoGFromWKB::new();
    Ok(Expr::ScalarFunction(
        datafusion_expr::expr::ScalarFunction {
            func: std::sync::Arc::new(ScalarUDF::from(func)),
            args: vec![arg],
        },
    ))
}

fn st_asbinary(input: ScalarFunctionInput) -> crate::error::PlanResult<datafusion_expr::Expr> {
    use datafusion_expr::{Expr, ScalarUDF};

    let arg = input.arguments.one()?;

    let func = StAsBinary::new();
    Ok(Expr::ScalarFunction(
        datafusion_expr::expr::ScalarFunction {
            func: std::sync::Arc::new(ScalarUDF::from(func)),
            args: vec![arg],
        },
    ))
}
