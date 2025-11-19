pub mod converter;
pub mod manager;
pub mod mapping;

pub use converter::{
    arrow_schema_from_struct_type, kernel_to_logical_arrow, logical_arrow_to_kernel,
};
pub use manager::{annotate_for_column_mapping, evolve_schema, get_physical_schema};
pub use mapping::{
    annotate_new_fields_for_column_mapping, annotate_schema_for_column_mapping,
    compute_max_column_id,
};
