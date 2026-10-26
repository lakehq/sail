use arrow_schema::DataType;

/// Whether a positional relabel can preserve every physical leaf array.
/// Field names, nullability and metadata do not change the array layout.
pub fn same_layout(source: &DataType, target: &DataType) -> bool {
    match (source, target) {
        (DataType::Struct(a), DataType::Struct(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b)
                    .all(|(a, b)| same_layout(a.data_type(), b.data_type()))
        }
        (DataType::List(a), DataType::List(b))
        | (DataType::LargeList(a), DataType::LargeList(b)) => {
            same_layout(a.data_type(), b.data_type())
        }
        (DataType::FixedSizeList(a, n), DataType::FixedSizeList(b, m)) => {
            n == m && same_layout(a.data_type(), b.data_type())
        }
        (DataType::Map(a, n), DataType::Map(b, m)) => {
            n == m && same_layout(a.data_type(), b.data_type())
        }
        _ => source == target,
    }
}
