use sail_function::scalar::vector::cosine_similarity::VectorCosineSimilarity;
use sail_function::scalar::vector::inner_product::VectorInnerProduct;

use crate::function::common::ScalarFunction;

pub(super) fn list_built_in_vector_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        (
            "vector_cosine_similarity",
            F::udf(VectorCosineSimilarity::new()),
        ),
        ("vector_inner_product", F::udf(VectorInnerProduct::new())),
        ("vector_l2_distance", F::unknown("vector_l2_distance")),
        ("vector_norm", F::unknown("vector_norm")),
        ("vector_normalize", F::unknown("vector_normalize")),
    ]
}
