use datafusion::arrow::datatypes::DataType;

pub mod cosine_similarity;
pub mod inner_product;

fn is_float_vector(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::List(field) | DataType::LargeList(field)
            if field.data_type() == &DataType::Float32
    )
}
