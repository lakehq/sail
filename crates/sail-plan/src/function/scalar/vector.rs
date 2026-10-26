use crate::function::common::ScalarFunctionBuilder as F;

pub(super) fn list_built_in_vector_functions()
-> Vec<(&'static str, crate::function::common::ScalarFunction)> {
    vec![
        (
            "vector_cosine_similarity",
            F::udf(sail_function::scalar::vector::cosine_similarity::VectorCosineSimilarity::new()),
        ),
        (
            "vector_inner_product",
            F::udf(sail_function::scalar::vector::inner_product::VectorInnerProduct::new()),
        ),
        (
            "vector_l2_distance",
            F::udf(sail_function::scalar::vector::l2_distance::VectorL2Distance::new()),
        ),
        (
            "vector_norm",
            F::udf(sail_function::scalar::vector::norm::VectorNorm::new()),
        ),
        (
            "vector_normalize",
            F::udf(sail_function::scalar::vector::normalize::VectorNormalize::new()),
        ),
    ]
}
